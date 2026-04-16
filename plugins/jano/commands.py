"""
Jano Plugin for DCSServerBot
Manages Discord channel visibility on a configurable schedule or manually.

Ported from jano_bot_v103.py (standalone) to DCSServerBot Plugin architecture.
"""

from __future__ import annotations

import asyncio
import datetime
import json
import logging
import re
from typing import Optional, Type

import discord
from zoneinfo import ZoneInfo
from discord import app_commands
from discord.ext import tasks

import psycopg
import psycopg.rows
from core import Plugin, TEventListener
from services.bot import DCSServerBot

from .version import __version__

log = logging.getLogger(__name__)

# Timezone for schedule calculations — configure in jano.yaml (timezone: "Europe/Madrid")
_DEFAULT_TZ = "Europe/Madrid"
TZ = ZoneInfo(_DEFAULT_TZ)


# ══════════════════════════════════════════════════════════════════════════════
# DATA MODEL — InstanceConfig + InstanceState
# (same logic as standalone, persistence replaced by PostgreSQL)
# ══════════════════════════════════════════════════════════════════════════════

class InstanceConfig:
    """Immutable configuration for one Jano instance (category group)."""

    def __init__(self, name, server_id, category_id, role_id,
                 text_channel_id, voice_channel_id, mention_role_id,
                 active_days, opening_time, closing_time,
                 max_manual_hours, command_role_ids_global,
                 command_role_ids_instance, status_icon: bool = True):
        self.name                    = name
        self.server_id                 = server_id
        self.category_id               = category_id
        self.role_id                   = role_id          # None → @everyone
        self.text_channel_id           = text_channel_id
        self.voice_channel_id          = voice_channel_id
        self.mention_role_id           = mention_role_id
        self.active_days              = active_days
        self.opening_time             = opening_time
        self.closing_time               = closing_time
        self.max_manual_hours          = max_manual_hours
        self.command_role_ids_global    = command_role_ids_global
        self.command_role_ids_instance = command_role_ids_instance
        self.status_icon               = status_icon  # True = rename category with 🟢🔴

    def effective_role_id(self):
        return self.role_id if self.role_id else self.server_id


class InstanceState:
    """Mutable runtime state for one Jano instance. Persisted in PostgreSQL."""

    def __init__(self, cfg: InstanceConfig, plugin: "Jano"):
        self.cfg                    = cfg
        self.plugin                 = plugin        # back-reference for DB access
        self.current_state          = None
        self.category_name_cache = None
        self.last_message_id      = None
        self.manual_override        = None
        self.override_ts     = None
        self.manual_hours_active    = cfg.max_manual_hours
        self.max_hours_override     = None
        self.schedule_override       = None
        self._trimmed_duration    = None
        self._evaluando             = False

    # ── Derived getters ────────────────────────────────────────────────────

    def get_category_id(self):      return self.cfg.category_id
    def get_role_id(self):          return self.cfg.effective_role_id()
    def get_text_channel_id(self):  return self.cfg.text_channel_id
    def get_voice_channel_id(self): return self.cfg.voice_channel_id
    def get_mention_role_id(self):  return self.cfg.mention_role_id

    def active_ceiling(self):
        return self.max_hours_override if self.max_hours_override is not None else self.cfg.max_manual_hours

    def active_schedule(self):
        if self.schedule_override:
            return (
                self.schedule_override.get("days", []),
                self.schedule_override.get("opening", self.cfg.opening_time),
                self.schedule_override.get("closing",   self.cfg.closing_time),
            )
        return self.cfg.active_days, self.cfg.opening_time, self.cfg.closing_time

    def schedule_readable(self):
        day_map = {0:"Mon",1:"Tue",2:"Wed",3:"Thu",4:"Fri",5:"Sat",6:"Sun"}
        days, open_t, close_t = self.active_schedule()
        if not days:
            return {"days": "No schedule (manual)", "opening": "—", "closing": "—"}
        open_h, open_m = map(int, open_t.split(":"))
        close_h, close_m = map(int, close_t.split(":"))
        overnight = open_h * 60 + open_m > close_h * 60 + close_m
        return {
            "days":    ", ".join(day_map[d] for d in days if d in day_map),
            "opening": open_t,
            "closing":   close_t,
        }

    # ── Manual mode ────────────────────────────────────────────────────────

    def manual_expired(self):
        if self.override_ts and self.manual_hours_active > 0:
            elapsed_hours = (datetime.datetime.now(TZ) - self.override_ts).total_seconds() / 3600
            return elapsed_hours >= self.manual_hours_active
        return False

    def activate_manual(self, is_open: bool, hours: float = None):
        self._trimmed_duration = None
        if self.manual_override is None:
            self.override_ts = datetime.datetime.now(TZ)
        self.manual_override = is_open
        if is_open:
            ceiling = self.active_ceiling()
            if hours is None or hours == 0:
                self.manual_hours_active = ceiling
            else:
                if ceiling > 0 and hours > ceiling:
                    self.manual_hours_active = ceiling
                    self._trimmed_duration = (hours, ceiling)
                else:
                    self.manual_hours_active = hours
        self.save()

    def deactivate_manual(self):
        self.manual_override    = None
        self.override_ts = None
        self.save()

    def manual_mode_info(self):
        if self.manual_override is None or not self.override_ts:
            return None
        if self.manual_hours_active <= 0:
            return {"no_limit": True}
        now        = datetime.datetime.now(TZ)
        total        = datetime.timedelta(hours=self.manual_hours_active)
        elapsed = now - self.override_ts
        remaining     = total - elapsed
        if remaining.total_seconds() < 0:
            remaining = datetime.timedelta(seconds=0)
        return {"no_limit": False, "remaining": remaining, "expires_at": now + remaining}

    # ── Desired state ──────────────────────────────────────────────────────

    def compute_desired_state(self):
        if self.manual_override is not None and self.manual_expired():
            return None, "EXPIRED"
        if self.manual_override is not None:
            return self.manual_override, "MANUAL"
        days, open_t, close_t = self.active_schedule()
        if not days:
            return False, "NO_SCHEDULE"
        now        = datetime.datetime.now(TZ)
        current_time  = now.hour * 60 + now.minute
        open_h, open_m       = map(int, open_t.split(":"))
        close_h, close_m  = map(int, close_t.split(":"))
        opening_min = open_h * 60 + open_m
        closing_min   = close_h * 60 + close_m
        today      = now.weekday()
        yesterday     = (today - 1) % 7
        if opening_min < closing_min:
            # Normal schedule (e.g. 18:00 → 22:00)
            in_range = opening_min <= current_time < closing_min
            return (today in days and in_range), "SCHEDULE"
        else:
            # Overnight schedule (e.g. 23:50 → 00:50)
            # Open if: today is a valid day AND past opening time
            # OR yesterday was a valid day AND before closing time
            after_opening  = today in days and current_time >= opening_min
            before_closing = yesterday in days and current_time < closing_min
            return (after_opening or before_closing), "SCHEDULE"

    # ── Persistence ────────────────────────────────────────────────────────

    def save(self):
        """Persist instance config + state to PostgreSQL (fire-and-forget)."""
        asyncio.ensure_future(self._save_async())

    async def _save_async(self):
        try:
            async with self.plugin.apool.connection() as conn:
                async with conn.transaction():
                    # Upsert config
                    await conn.execute("""
                        INSERT INTO jano_instances
                            (name, category_id, role_id, text_channel_id, voice_channel_id,
                             mention_role_id, active_days, opening_time, closing_time,
                             max_manual_hours, command_role_ids_instance, status_icon)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT (name) DO UPDATE SET
                            category_id               = EXCLUDED.category_id,
                            role_id                   = EXCLUDED.role_id,
                            text_channel_id           = EXCLUDED.text_channel_id,
                            voice_channel_id          = EXCLUDED.voice_channel_id,
                            mention_role_id           = EXCLUDED.mention_role_id,
                            active_days              = EXCLUDED.active_days,
                            opening_time             = EXCLUDED.opening_time,
                            closing_time               = EXCLUDED.closing_time,
                            max_manual_hours          = EXCLUDED.max_manual_hours,
                            command_role_ids_instance = EXCLUDED.command_role_ids_instance,
                            status_icon               = EXCLUDED.status_icon
                    """, (
                        self.cfg.name,
                        self.cfg.category_id,
                        self.cfg.role_id,
                        self.cfg.text_channel_id,
                        self.cfg.voice_channel_id,
                        self.cfg.mention_role_id,
                        self.cfg.active_days or [],
                        self.cfg.opening_time,
                        self.cfg.closing_time,
                        self.cfg.max_manual_hours,
                        self.cfg.command_role_ids_instance,
                        self.cfg.status_icon,
                    ))
                    # Upsert state
                    ts = self.override_ts.isoformat() if self.override_ts else None
                    horario_json = json.dumps(self.schedule_override) if self.schedule_override else None
                    await conn.execute("""
                        INSERT INTO jano_state
                            (name, current_state, category_name_cache, last_message_id,
                             manual_override, override_ts, manual_hours_active,
                             max_hours_override, schedule_override)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT (name) DO UPDATE SET
                            current_state           = EXCLUDED.current_state,
                            category_name_cache  = EXCLUDED.category_name_cache,
                            last_message_id       = EXCLUDED.last_message_id,
                            manual_override         = EXCLUDED.manual_override,
                            override_ts      = EXCLUDED.override_ts,
                            manual_hours_active     = EXCLUDED.manual_hours_active,
                            max_hours_override      = EXCLUDED.max_hours_override,
                            schedule_override        = EXCLUDED.schedule_override
                    """, (
                        self.cfg.name,
                        self.current_state,
                        self.category_name_cache,
                        self.last_message_id,
                        self.manual_override,
                        ts,
                        self.manual_hours_active,
                        self.max_hours_override,
                        horario_json,
                    ))
        except Exception as e:
            log.error(f"[Jano/{self.cfg.name}] Error saving state: {e}")

    def from_row(self, cfg_row, state_row):
        """Restore state from DB rows after startup."""
        if state_row is None:
            return
        self.current_state          = state_row["current_state"]
        self.category_name_cache = state_row["category_name_cache"]
        self.last_message_id      = state_row["last_message_id"]
        self.manual_hours_active    = state_row["manual_hours_active"] or self.cfg.max_manual_hours
        self.max_hours_override     = state_row["max_hours_override"]
        raw_horario = state_row["schedule_override"]
        self.schedule_override = json.loads(raw_horario) if isinstance(raw_horario, str) else raw_horario

        manual_override    = state_row["manual_override"]
        override_ts = state_row["override_ts"]
        if manual_override is not None and override_ts:
            if isinstance(override_ts, str):
                ts = datetime.datetime.fromisoformat(override_ts)
            else:
                ts = override_ts
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=TZ)
            self.override_ts = ts
            self.manual_override    = manual_override
            if self.manual_expired():
                log.info(f"[Jano/{self.cfg.name}] ⏳ Manual mode expired offline → resetting")
                self.manual_override    = None
                self.override_ts = None
            else:
                log.info(f"[Jano/{self.cfg.name}] 🔄 Manual mode restored ({'OPEN' if manual_override else 'CLOSED'})")
        else:
            self.manual_override    = None
            self.override_ts = None


# ══════════════════════════════════════════════════════════════════════════════
# PLUGIN CLASS
# ══════════════════════════════════════════════════════════════════════════════

# ── Command groups ────────────────────────────────────────────────────────────
# Defined at module level so they can be used as decorators inside the Jano cog

class ModalCommsDuration(discord.ui.Modal, title="Open comms — set duration"):
    """Modal that asks for duration when opening comms manually."""

    _f_duration = discord.ui.TextInput(
        label="Duration in hours (0 or empty = no limit)",
        placeholder="e.g. 2  or  2.5  or  0 for no limit",
        required=False,
        max_length=10,
        style=discord.TextStyle.short,
    )

    def __init__(self, st: "InstanceState", plugin: "Jano"):
        super().__init__()
        self.st      = st
        self.plugin  = plugin
        self.ceiling = st.active_ceiling()
        if self.ceiling > 0:
            self._f_duration.label = (
                f"Duration in hours — max is {self.ceiling}h"
            )
            self._f_duration.placeholder = (
                f"1 to {self.ceiling}  ·  0 = apply max ({self.ceiling}h)  ·  empty = apply max"
            )
        else:
            self._f_duration.label = "Duration in hours (0 or empty = no limit)"
            self._f_duration.placeholder = "e.g. 2  or  2.5  ·  0 or empty = no limit"
        # Note: _f_duration is a class-level TextInput, discord.py adds it automatically
        # Do NOT call self.add_item() — that would duplicate it

    async def on_submit(self, interaction: discord.Interaction):
        raw = self._f_duration.value.strip().replace(",", ".")
        duration: float | None = None
        if raw and raw != "0":
            try:
                duration = float(raw)
                if duration < 0:
                    raise ValueError
                # If value exceeds ceiling, activate_manual will trim it automatically
                # and set _trimmed_duration so the response shows the adjustment
            except ValueError:
                await interaction.response.send_message(
                    "❌ Invalid duration. Enter a positive number in hours (e.g. 2.5) or 0 for no limit.",
                    ephemeral=True, delete_after=120
                )
                return
        await self.plugin._comms_open(interaction, self.st, str(duration) if duration else None)



class Jano(Plugin):
    """DCSServerBot plugin — manages Discord comms channels by schedule or manually."""

    # ── Slash command groups ───────────────────────────────────────────────
    jano_group = app_commands.Group(
        name="jano",
        description="Jano — manage comms channel access"
    )

    def __init__(self, bot: DCSServerBot, eventlistener: Type[TEventListener] = None):
        super().__init__(bot, eventlistener)
        # In-memory state dict: name → InstanceState
        self.states: dict[str, InstanceState] = {}
        # Global command role IDs (from DB, overrides yaml default)
        self.command_role_ids_global: list[int] = []
        # Server ID (guild ID) — read from bot's guild
        self._server_id: int = 0

    # ── Plugin lifecycle ───────────────────────────────────────────────────

    async def cog_load(self) -> None:
        await super().cog_load()
        self.log.debug("Plugin loading...")
        # Load timezone from config
        global TZ
        cfg = self.get_config() or {}
        tz_name = cfg.get("timezone", _DEFAULT_TZ)
        try:
            TZ = ZoneInfo(tz_name)
        except Exception:
            self.log.warning(f"Invalid timezone '{tz_name}', using default '{_DEFAULT_TZ}'")
            TZ = ZoneInfo(_DEFAULT_TZ)
        # Ensure tables exist before anything else
        await self._ensure_tables()
        # Sync after ready to fix any Discord cache issues
        asyncio.ensure_future(self._sync_after_ready())
        # Store raw values from YAML (can be role names or numeric IDs).
        # Resolution to IDs happens in on_ready() once the guild is available.
        cfg = self.get_config() or {}
        self._command_role_ids_raw = cfg.get("command_role_ids", []) or []

    async def cog_unload(self) -> None:
        if self.scheduler.is_running():
            self.scheduler.cancel()
        await super().cog_unload()

    async def _sync_after_ready(self):
        """Fix CommandSignatureMismatch by clearing global commands and syncing guild.
        Global commands conflict with guild commands causing duplicate entries and
        signature mismatches. Solution: clear globals, keep only guild commands.
        """
        await self.bot.wait_until_ready()
        await asyncio.sleep(2)  # let DCSServerBot finish its own sync first
        try:
            guild_obj = discord.Object(id=self.bot.guilds[0].id)
            # Step 1: Clear global commands (these conflict with guild commands)
            self.bot.tree.clear_commands(guild=None)
            await self.bot.tree.sync()
            # Step 2: Copy all commands to guild and sync (instant propagation)
            self.bot.tree.copy_global_to(guild=guild_obj)
            synced = await self.bot.tree.sync(guild=guild_obj)
            self.log.debug(f"Jano: synced {len(synced)} commands to guild, globals cleared.")
        except Exception as e:
            self.log.warning(f"Jano: sync failed: {e}")


    async def on_ready(self) -> None:
        await super().on_ready()
        guild = self._get_guild()
        if not guild:
            self.log.error("❌ Could not find guild. Plugin disabled.")
            return
        self._server_id = guild.id
        self.log.info(f"Connected to Guild: {guild.name}")
        # Resolve role names/IDs from YAML now that the guild is available.
        # DB values take priority — only use YAML as bootstrap if DB is empty.
        await self._resolve_yaml_roles(guild)
        await self._migrate_db()
        await self._load_state()
        await self._clean_orphan_embeds()
        await self._evaluate_all()
        if not self.scheduler.is_running():
            self.scheduler.start()
        self.log.info(f"Ready - {len(self.states)} instance(s) loaded.")

    async def _resolve_yaml_roles(self, guild: discord.Guild):
        """Resolve role names or numeric IDs from jano.yaml into a list of int IDs.
        Supports both styles used in DCSServerBot (e.g. 'Admin' or 123456789)."""
        resolved = []
        for val in self._command_role_ids_raw:
            # Try numeric ID first
            try:
                resolved.append(int(val))
                continue
            except (ValueError, TypeError):
                pass
            # Try role name
            role = discord.utils.get(guild.roles, name=str(val))
            if role:
                resolved.append(role.id)
                self.log.debug(f"Role '{val}' resolved → ID {role.id}")
            else:
                self.log.warning(f"Role '{val}' not found in guild — skipping")
        # Only apply YAML roles as bootstrap if DB has nothing yet
        if resolved and not self.command_role_ids_global:
            self.command_role_ids_global = resolved
            # YAML bootstrap — no log needed, stored in DB

    # ── Helpers ────────────────────────────────────────────────────────────

    def _get_guild(self) -> discord.Guild | None:
        guilds = self.bot.guilds
        return guilds[0] if guilds else None

    def _get_names(self) -> list[str]:
        return list(self.states.keys())

    def _resolve_instance(self, name: str | None) -> InstanceState | None:
        if name:
            return self.states.get(name)
        if len(self.states) == 1:
            return next(iter(self.states.values()))
        return None

    def _is_authorized(self, interaction: discord.Interaction, st: InstanceState | None = None) -> bool:
        user_roles       = [r.id for r in interaction.user.roles] if hasattr(interaction.user, "roles") else []
        global_roles   = self.command_role_ids_global
        instance_roles  = (st.cfg.command_role_ids_instance if st else None) or []
        if not global_roles and not instance_roles:
            return True
        if global_roles and any(r in user_roles for r in global_roles):
            return True
        if instance_roles and any(r in user_roles for r in instance_roles):
            return True
        return False

    def _tiene_rol_global(self, interaction: discord.Interaction) -> bool:
        return self._is_authorized(interaction, st=None)

    async def _ensure_tables(self):
        """Create Jano tables if they do not exist yet.
        Called early in cog_load so tables are ready before on_ready.
        Safe to call multiple times — uses IF NOT EXISTS.
        """
        try:
            async with self.apool.connection() as conn:
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS jano_instances (
                        name                        TEXT PRIMARY KEY,
                        category_id                 BIGINT NOT NULL,
                        role_id                     BIGINT,
                        text_channel_id             BIGINT,
                        voice_channel_id            BIGINT,
                        mention_role_id             BIGINT,
                        active_days                 INTEGER[],
                        opening_time                TEXT    NOT NULL DEFAULT '19:00',
                        closing_time                TEXT    NOT NULL DEFAULT '22:00',
                        max_manual_hours            FLOAT   NOT NULL DEFAULT 0.0,
                        command_role_ids_instance   BIGINT[],
                        status_icon                 BOOLEAN NOT NULL DEFAULT true
                    )
                """)
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS jano_state (
                        name                    TEXT PRIMARY KEY REFERENCES jano_instances(name) ON DELETE CASCADE ON UPDATE CASCADE,
                        current_state           BOOLEAN,
                        category_name_cache     TEXT,
                        last_message_id         BIGINT,
                        manual_override         BOOLEAN,
                        override_ts             TIMESTAMP WITH TIME ZONE,
                        manual_hours_active     FLOAT   NOT NULL DEFAULT 0.0,
                        max_hours_override      FLOAT,
                        schedule_override       JSONB
                    )
                """)
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS jano_global (
                        id                      INTEGER PRIMARY KEY DEFAULT 1 CHECK (id = 1),
                        command_role_ids_global BIGINT[]
                    )
                """)
                await conn.execute("""
                    INSERT INTO jano_global (id, command_role_ids_global)
                    VALUES (1, ARRAY[]::BIGINT[])
                    ON CONFLICT DO NOTHING
                """)
        except Exception as e:
            self.log.error(f"Error creating tables: {e}")

    async def install(self) -> None:

        """Called by DCSServerBot on first install — creates tables via tables.sql."""
        await super().install()

    async def _migrate_db(self):
        """Apply any missing DB schema changes automatically on startup.
        Add new ALTER TABLE statements here for every future schema change —
        IF NOT EXISTS ensures they are safe to run repeatedly."""
        migrations = [
            # v1.0 → v1.1: status_icon per instance
            "ALTER TABLE jano_instances ADD COLUMN IF NOT EXISTS status_icon BOOLEAN NOT NULL DEFAULT true",

            # v1.1 → v1.2: rename Spanish columns to English (safe — checks existence first)
            "DO $$ BEGIN IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='jano_instances' AND column_name='dias_activos') THEN ALTER TABLE jano_instances RENAME COLUMN dias_activos TO active_days; END IF; END $$",
            "DO $$ BEGIN IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='jano_instances' AND column_name='hora_apertura') THEN ALTER TABLE jano_instances RENAME COLUMN hora_apertura TO opening_time; END IF; END $$",
            "DO $$ BEGIN IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='jano_instances' AND column_name='hora_cierre') THEN ALTER TABLE jano_instances RENAME COLUMN hora_cierre TO closing_time; END IF; END $$",
            "DO $$ BEGIN IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='jano_instances' AND column_name='max_horas_manual') THEN ALTER TABLE jano_instances RENAME COLUMN max_horas_manual TO max_manual_hours; END IF; END $$",
            "DO $$ BEGIN IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='jano_instances' AND column_name='command_role_ids_instancia') THEN ALTER TABLE jano_instances RENAME COLUMN command_role_ids_instancia TO command_role_ids_instance; END IF; END $$",
            "DO $$ BEGIN IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='jano_state' AND column_name='ultimo_mensaje_id') THEN ALTER TABLE jano_state RENAME COLUMN ultimo_mensaje_id TO last_message_id; END IF; END $$",
            "DO $$ BEGIN IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='jano_state' AND column_name='nombre_categoria_cache') THEN ALTER TABLE jano_state RENAME COLUMN nombre_categoria_cache TO category_name_cache; END IF; END $$",
            "DO $$ BEGIN IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='jano_state' AND column_name='override_manual') THEN ALTER TABLE jano_state RENAME COLUMN override_manual TO manual_override; END IF; END $$",
            "DO $$ BEGIN IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='jano_state' AND column_name='override_timestamp') THEN ALTER TABLE jano_state RENAME COLUMN override_timestamp TO override_ts; END IF; END $$",
            "DO $$ BEGIN IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='jano_state' AND column_name='horas_manual_activo') THEN ALTER TABLE jano_state RENAME COLUMN horas_manual_activo TO manual_hours_active; END IF; END $$",
            "DO $$ BEGIN IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='jano_state' AND column_name='max_horas_override') THEN ALTER TABLE jano_state RENAME COLUMN max_horas_override TO max_hours_override; END IF; END $$",
            "DO $$ BEGIN IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='jano_state' AND column_name='horario_override') THEN ALTER TABLE jano_state RENAME COLUMN horario_override TO schedule_override; END IF; END $$",

            # v1.1 → v1.2 (missed): estado_actual → current_state
            "DO $$ BEGIN IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='jano_state' AND column_name='estado_actual') THEN ALTER TABLE jano_state RENAME COLUMN estado_actual TO current_state; END IF; END $$",

            # ── Add future migrations below this line ──────────────────────────
        ]
        try:
            async with self.apool.connection() as conn:
                for sql in migrations:
                    await conn.execute(sql)
        except Exception as e:
            self.log.error(f"Error applying DB migrations: {e}")

    async def _save_global_roles(self):
        try:
            async with self.apool.connection() as conn:
                await conn.execute("""
                    UPDATE jano_global SET command_role_ids_global = %s WHERE id = 1
                """, (self.command_role_ids_global,))
        except Exception as e:
            self.log.error(f"Error saving global roles: {e}")

    # ── DB Load ────────────────────────────────────────────────────────────

    async def _load_state(self):
        """Load all instances and their state from PostgreSQL."""
        try:
            async with self.apool.connection() as conn:
                # Global roles
                async with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
                    await cur.execute("SELECT command_role_ids_global FROM jano_global WHERE id=1")
                    row = await cur.fetchone()
                    if row and row["command_role_ids_global"]:
                        self.command_role_ids_global = list(row["command_role_ids_global"])

                    # Instances
                    await cur.execute("SELECT * FROM jano_instances")
                    inst_rows = await cur.fetchall()

                    await cur.execute("SELECT * FROM jano_state")
                    state_rows = await cur.fetchall()
                    state_map = {r["name"]: r for r in state_rows}

            guild = self._get_guild()
            for r in inst_rows:
                name = r["name"]
                if name in self.states:
                    continue
                cfg = InstanceConfig(
                    name                    = name,
                    server_id                 = self._server_id,
                    category_id               = r["category_id"],
                    role_id                   = r["role_id"],
                    text_channel_id           = r["text_channel_id"],
                    voice_channel_id          = r["voice_channel_id"],
                    mention_role_id           = r["mention_role_id"],
                    active_days              = list(r["active_days"] or []),
                    opening_time             = r["opening_time"],
                    closing_time               = r["closing_time"],
                    max_manual_hours          = r["max_manual_hours"],
                    command_role_ids_global   = self.command_role_ids_global,
                    command_role_ids_instance = list(r["command_role_ids_instance"] or []) or None,
                    status_icon               = r["status_icon"] if r["status_icon"] is not None else True,
                )
                st = InstanceState(cfg, self)
                st.from_row(r, state_map.get(name))
                self.states[name] = st
                self.log.info(f"Instance '{name}' restored from DB")

            if not self.states:
                self.log.info("No instances configured. Use /jano setup to create one.")

        except Exception as e:
            self.log.error(f"Error loading state from DB: {e}")

    async def _create_instance(self, cfg: InstanceConfig) -> InstanceState:
        """Add a new instance to memory and persist config to DB."""
        st = InstanceState(cfg, self)
        self.states[cfg.name] = st
        await st._save_async()
        self.log.info(f"Instance '{cfg.name}' created.")
        return st

    async def _delete_instance(self, name: str):
        """Remove instance from memory and DB."""
        if name in self.states:
            del self.states[name]
        try:
            async with self.apool.connection() as conn:
                # jano_state cascades on delete from jano_instances
                await conn.execute("DELETE FROM jano_instances WHERE name = %s", (name,))
        except Exception as e:
            self.log.error(f"Error deleting instance '{name}': {e}")

    # ── Core scheduling logic ──────────────────────────────────────────────

    @tasks.loop(minutes=1)
    async def scheduler(self):
        await self._evaluate_all()

    async def _evaluate_all(self):
        for st in list(self.states.values()):
            await self._evaluate_instance(st)

    async def _evaluate_instance(self, st: InstanceState):
        if st._evaluando:
            return
        st._evaluando = True
        try:
            is_open, source = st.compute_desired_state()
            if source == "EXPIRED":
                self.log.info(f"[{st.cfg.name}] ⏳ Manual mode expired → returning to schedule")
                st.deactivate_manual()
                is_open, source = st.compute_desired_state()

            guild = self._get_guild()
            if not guild:
                return

            role      = guild.get_role(st.get_role_id())
            category = guild.get_channel(st.get_category_id())
            text_ch   = guild.get_channel(st.get_text_channel_id()) if st.get_text_channel_id() else None

            if not role or not category:
                self.log.warning(f"[{st.cfg.name}] Role or category not found, skipping.")
                return

            await _update_category_name(category, is_open, st)

            actual_perm    = category.permissions_for(role).view_channel
            necesita_cambio = actual_perm != is_open

            if not necesita_cambio:
                st.current_state = is_open
                st.save()
                return

            self.log.info(f"[{st.cfg.name}] 🔄 Applying ({source}) → {'OPEN' if is_open else 'CLOSED'}")

            for attempt in range(3):
                try:
                    overwrite = category.overwrites_for(role)
                    overwrite.view_channel = is_open
                    await category.set_permissions(role, overwrite=overwrite)
                    break
                except discord.HTTPException as e:
                    if e.status == 429:
                        self.log.warning(f"[{st.cfg.name}] Rate limit on permissions")
                        break
                    else:
                        self.log.warning(f"[{st.cfg.name}] Permission error (attempt {attempt+1}/3): {e}")
                        if attempt < 2:
                            await asyncio.sleep(3)
                        else:
                            self.log.error(f"[{st.cfg.name}] ❌ Definitive failure: {e}")
                            return

            if text_ch:
                if is_open:
                    if not st.last_message_id and source == "SCHEDULE":
                        voice_id    = st.get_voice_channel_id()
                        inst_name = st.cfg.name
                        desc = f"The **{inst_name}** channels will remain open until the end of the event.\n\nPlease connect to the channel:\n\n"
                        desc += f"<#{voice_id}>\n\n" if voice_id else "Voice Channel\n\n"
                        desc += f"Before the start of **{inst_name}**."
                        embed = discord.Embed(
                            title=f"🟢   __**{inst_name.upper()} ACCESS CHANNELS ARE OPEN**__",
                            description=desc,
                            color=0x2ECC71
                        )
                        mention_id = st.get_mention_role_id()
                        contenido  = f"<@&{mention_id}>" if mention_id else None
                        msg = await text_ch.send(
                            content=contenido,
                            embed=embed,
                            allowed_mentions=discord.AllowedMentions(roles=True)
                        )
                        st.last_message_id = msg.id
                else:
                    if st.last_message_id:
                        try:
                            msg = await text_ch.fetch_message(st.last_message_id)
                            await msg.delete()
                        except Exception:
                            pass
                        st.last_message_id = None

            st.current_state = is_open
            st.save()
        finally:
            st._evaluando = False

    async def _clean_orphan_embeds(self):
        guild = self._get_guild()
        if not guild:
            return
        for st in self.states.values():
            if st.last_message_id:
                is_open, _ = st.compute_desired_state()
                if not is_open:
                    txt = guild.get_channel(st.get_text_channel_id()) if st.get_text_channel_id() else None
                    if txt:
                        try:
                            msg = await txt.fetch_message(st.last_message_id)
                            await msg.delete()
                            self.log.info(f"[{st.cfg.name}] 🧹 Orphan embed removed")
                        except Exception:
                            pass
                        st.last_message_id = None
                        st.save()

    # ── Autocomplete ───────────────────────────────────────────────────────

    async def _autocomplete_instance(
        self,
        interaction: discord.Interaction,
        current: str
    ) -> list[app_commands.Choice[str]]:
        names = self._get_names()
        if not names:
            return [app_commands.Choice(name="⚠️ No instances — use /jano setup first", value="__none__")]
        return [
            app_commands.Choice(name=n, value=n)
            for n in names
            if current.lower() in n.lower()
        ][:25]

    async def _check_instances(self, interaction: discord.Interaction, instance: str = None) -> bool:
        if not self.states or instance == "__none__":
            embed = discord.Embed(
                title="⚠️ No instances configured",
                description="No instances have been set up yet.\nUse **/jano setup** to create your first instance.",
                color=0xE67E22
            )
            asyncio.ensure_future(_reply_ephemeral(interaction, embed=embed))
            return False
        return True

    # ══════════════════════════════════════════════════════════════════════
    # SLASH COMMANDS
    # ══════════════════════════════════════════════════════════════════════

    @jano_group.command(name="status", description="Show current status of an instance")
    @app_commands.describe(instance="Instance to check (optional if only one)")
    async def jano_status(self, interaction: discord.Interaction, instance: str = None):
        if not await self._check_instances(interaction, instance):
            return
        st = self._resolve_instance(instance)
        if not st:
            await interaction.response.send_message(
                "❌ Specify an instance. Options: " + ", ".join(self._get_names()),
                ephemeral=True, delete_after=120
            )
            return
        if not self._is_authorized(interaction, st):
            asyncio.ensure_future(_reply_ephemeral(interaction, embed=_no_permission()))
            return

        await interaction.response.defer(ephemeral=True)

        state_txt = "🟢 Open" if st.current_state else "🔴 Closed"
        mode_txt   = "Manual" if st.manual_override is not None else "Schedule"

        embed = discord.Embed(title=f"📊 Status — {st.cfg.name}", color=0x3498DB)
        embed.add_field(name="__**Status**__",  value=f"*Current open/close state*\n{state_txt}", inline=False)
        embed.add_field(name="__**Mode**__",    value=f"*Schedule = automatic, Manual = forced*\n**{mode_txt}**", inline=False)

        h = st.schedule_readable()
        embed.add_field(name="__**Schedule**__",    value=f"*Configured times*\n**{h['opening']} - {h['closing']}**", inline=False)
        embed.add_field(name="__**Active days**__", value=f"*Days active*\n**{h['days']}**", inline=False)

        ceiling = st.active_ceiling()
        embed.add_field(
            name="__**Max manual duration**__",
            value=f"**{ceiling}h**" if ceiling > 0 else "**No limit**",
            inline=False
        )
        si_desc = "*Shows open/closed state on category name with 🟢🔴*"
        si_val  = "**Enabled**" if st.cfg.status_icon else "**Disabled**"
        embed.add_field(name="__**Status Icon**__", value=f"{si_desc}\n{si_val}", inline=False)

        if st.manual_override is not None:
            info = st.manual_mode_info()
            if info:
                if info.get("no_limit"):
                    embed.add_field(name="__**Manual mode**__", value="**No time limit**", inline=False)
                else:
                    embed.add_field(name="__**Manual duration**__",  value=f"**{st.manual_hours_active}h**", inline=False)
                    embed.add_field(name="__**Remaining**__",         value=f"**{_fmt_duration(info['remaining'])}**", inline=False)
                    embed.add_field(name="__**Expires at**__",        value=f"**{info['expires_at'].strftime('%H:%M')}**", inline=False)

        guild = self._get_guild()
        if guild:
            embed.add_field(name="\u200b", value="**── Configured resources ──**", inline=False)
            cat = guild.get_channel(st.get_category_id())
            cat_name   = cat.name if cat else "❌ Not configured"
            embed.add_field(name="📦 __Category__",   value=f"**{cat_name}**" if cat else "❌ Not configured", inline=False)
            txt_channel_id = st.get_text_channel_id()
            embed.add_field(name="💬 __Text channel__",  value=f"<#{txt_channel_id}>" if txt_channel_id and guild.get_channel(txt_channel_id) else "❌ Not configured", inline=False)
            voice_id = st.get_voice_channel_id()
            embed.add_field(name="🔊 __Voice channel__", value=f"<#{voice_id}>" if voice_id and guild.get_channel(voice_id) else "❌ Not configured", inline=False)

            role_id = st.get_role_id()
            if role_id == self._server_id:
                role_txt = "🌐 **@everyone**"
            elif role_id:
                role = guild.get_role(role_id)
                role_txt = f"**{role.name}**" if role else "⚠️ Role not found"
            else:
                role_txt = "❌ Not configured"
            embed.add_field(name="👁️ __Visibility role__", value=role_txt, inline=False)

            mr_id = st.get_mention_role_id()
            mr_txt = f"<@&{mr_id}>" if mr_id else "❌ Not configured"
            embed.add_field(name="📣 __Mention role__", value=mr_txt, inline=False)

            embed.add_field(name="\u200b", value="**── Permissions ──**", inline=False)
            roles_g = [guild.get_role(r) for r in self.command_role_ids_global]
            embed.add_field(
                name="__Command roles (global)__",
                value=", ".join(f"**{r.name}**" for r in roles_g if r) or "❌ Not configured",
                inline=False
            )
            ri = st.cfg.command_role_ids_instance
            if ri is None:
                ri_val = "🔑 **Only Global Role**"
            elif ri == []:
                ri_val = "🌐 @everyone"
            else:
                ri_val = ", ".join(f"**{guild.get_role(r).name}**" for r in ri if guild.get_role(r)) or "❌"
            embed.add_field(name="__Command roles (instance)__", value=ri_val, inline=False)

        await _followup_send(interaction, embed)

    @jano_status.autocomplete("instance")
    async def _ac_status(self, interaction, current):
        return await self._autocomplete_instance(interaction, current)

    # ── /jano comms ───────────────────────────────────────────────────────

    @jano_group.command(name="comms", description="Open, close or resume comms channels")
    @app_commands.describe(
        instance="Instance to act on (auto-selected if only one exists)",
        action="What to do: open channels, close them, or resume automatic schedule"
    )
    async def jano_comms(self, interaction: discord.Interaction, instance: str = None, action: str = None):
        if not await self._check_instances(interaction, instance):
            return
        st = self._resolve_instance(instance)
        if not st:
            await interaction.response.send_message(
                "❌ Specify an instance. Options: " + ", ".join(self._get_names()),
                ephemeral=True, delete_after=120
            )
            return
        if not self._is_authorized(interaction, st):
            asyncio.ensure_future(_reply_ephemeral(interaction, embed=_no_permission()))
            return

        if action not in ("open", "close", "resume"):
            await interaction.response.send_message(
                "❌ Invalid action. Choose: **open**, **close** or **resume**.",
                ephemeral=True, delete_after=60
            )
            return

        if action == "open":
            # Ask for duration via modal
            await interaction.response.send_modal(ModalCommsDuration(st, self))
        elif action == "close":
            await self._comms_close(interaction, st)
        else:
            await self._comms_resume(interaction, st)

    @jano_comms.autocomplete("action")
    async def _ac_comms_action(self, interaction: discord.Interaction, current: str):
        actions = [
            app_commands.Choice(name="open  — manually open channels",   value="open"),
            app_commands.Choice(name="close — manually close channels",  value="close"),
            app_commands.Choice(name="resume — return to schedule",      value="resume"),
        ]
        return [a for a in actions if current.lower() in a.name.lower()]

    @jano_comms.autocomplete("instance")
    async def _ac_comms_instance(self, interaction, current):
        return await self._autocomplete_instance(interaction, current)


    async def _comms_open(self, interaction: discord.Interaction, st: InstanceState, duration: str = None):
        duration_val: float | None = None
        if duration is not None:
            try:
                duration_val = float(duration.strip().replace(",", "."))
                if duration_val < 0:
                    raise ValueError
            except ValueError:
                await interaction.response.send_message(
                    "❌ Invalid duration. Enter a positive number in hours (e.g. 2.5) or 0 for no limit.",
                    ephemeral=True, delete_after=120
                )
                return

        if st.current_state and st.manual_override is True:
            if duration_val is None:
                embed = discord.Embed(
                    title=f"🟢 Already open — {st.cfg.name}",
                    description="The channels are already open in manual mode. No changes made.",
                    color=0x95A5A6
                )
                asyncio.ensure_future(_reply_ephemeral(interaction, embed=embed))
                return
            else:
                st._trimmed_duration = None
                st.activate_manual(True, hours=duration_val)
                info    = st.manual_mode_info()
                ceiling = st.active_ceiling()
                embed   = discord.Embed(
                    title=f"⏱️ Duration updated — {st.cfg.name}",
                    description="Channels remain open. Duration updated.",
                    color=0x3498DB
                )
                if st._trimmed_duration:
                    requested, applied = st._trimmed_duration
                    embed.add_field(name="⚠️ Duration adjusted", value=f"Requested **{requested}h**, max is **{applied}h**.", inline=False)
                if info and not info.get("no_limit"):
                    embed.add_field(name="New duration",   value=f"{st.manual_hours_active}h", inline=False)
                    embed.add_field(name="Remaining",      value=_fmt_duration(info["remaining"]), inline=False)
                    embed.add_field(name="Expires at",     value=info["expires_at"].strftime('%H:%M'), inline=False)
                else:
                    embed.add_field(name="Duration", value=f"Indefinite · max {ceiling}h" if ceiling > 0 else "No limit", inline=False)
                st.save()
                asyncio.ensure_future(_reply_ephemeral(interaction, embed=embed))
                return

        st._trimmed_duration = None
        st.activate_manual(True, hours=duration_val)
        info    = st.manual_mode_info()
        ceiling = st.active_ceiling()

        embed = discord.Embed(
            title=f"🟡 Opening access... — {st.cfg.name}",
            description="⏳ Applying changes. Category status may take a moment to update.",
            color=0xF1C40F
        )
        if st._trimmed_duration:
            requested, applied = st._trimmed_duration
            embed.add_field(name="⚠️ Duration adjusted", value=f"Requested **{requested}h**, max is **{applied}h**.", inline=False)
        if info and not info.get("no_limit"):
            embed.add_field(name="Active duration", value=f"{st.manual_hours_active}h",         inline=False)
            embed.add_field(name="Remaining",       value=_fmt_duration(info["remaining"]),         inline=False)
            embed.add_field(name="Expires at",      value=info["expires_at"].strftime('%H:%M'),  inline=False)
        else:
            embed.add_field(name="Duration", value=f"Indefinite · max {ceiling}h" if ceiling > 0 else "No limit", inline=False)

        await interaction.response.send_message(embed=embed, ephemeral=True)

        async def apply_and_confirm():
            await self._evaluate_instance(st)
            embed_ok = discord.Embed(
                title=f"🟢 Access opened — {st.cfg.name}",
                description="✅ Changes applied successfully.",
                color=0x2ECC71
            )
            if st._trimmed_duration:
                requested, applied = st._trimmed_duration
                embed_ok.add_field(name="⚠️ Duration adjusted", value=f"Requested **{requested}h**, max is **{applied}h**.", inline=False)
            if info and not info.get("no_limit"):
                embed_ok.add_field(name="Active duration", value=f"{st.manual_hours_active}h",        inline=False)
                embed_ok.add_field(name="Remaining",       value=_fmt_duration(info["remaining"]),        inline=False)
                embed_ok.add_field(name="Expires at",      value=info["expires_at"].strftime('%H:%M'), inline=False)
            else:
                embed_ok.add_field(name="Duration", value=f"Indefinite · max {ceiling}h" if ceiling > 0 else "No limit", inline=False)
            try:
                await interaction.edit_original_response(embed=embed_ok)
                msg = await interaction.original_response()
                asyncio.ensure_future(_delete_after(msg))
            except Exception:
                pass

        asyncio.create_task(apply_and_confirm())

    async def _comms_close(self, interaction: discord.Interaction, st: InstanceState):
        await interaction.response.defer(ephemeral=True)

        if not st.current_state and st.manual_override is None:
            embed = discord.Embed(
                title=f"🔴 Already closed — {st.cfg.name}",
                description="Channels already closed and running on schedule. No changes made.",
                color=0x95A5A6
            )
            await _followup_send(interaction, embed)
            return

        st.activate_manual(False)
        await self._evaluate_instance(st)

        info  = st.manual_mode_info()
        embed = discord.Embed(
            title=f"🔴 Access closed — {st.cfg.name}",
            description="✅ Access closed successfully.",
            color=0xE74C3C
        )
        if info and not info.get("no_limit"):
            embed.add_field(
                name="⏳ Remaining manual time",
                value=f"**{_fmt_duration(info['remaining'])}** (expires at {info['expires_at'].strftime('%H:%M')})",
                inline=False
            )
        embed.add_field(name="What next?", value="Keep manual mode or resume automatic schedule.", inline=False)
        vista = ViewCloseConfirm(st, info or {"no_limit": True}, self)
        msg = await interaction.followup.send(embed=embed, view=vista, ephemeral=True, wait=True)
        vista.message = msg

    async def _comms_resume(self, interaction: discord.Interaction, st: InstanceState):
        await interaction.response.defer(ephemeral=True)
        embed = await self._execute_resume(st, "♻️ Schedule resumed")
        await _followup_send(interaction, embed)

    async def _execute_resume(self, st: InstanceState, title: str) -> discord.Embed:
        st.deactivate_manual()
        h = st.schedule_readable()
        config_warning = False
        if h["days"] == "No schedule (manual)":
            st.schedule_override = None
            st.save()
            h = st.schedule_readable()
            config_warning = True
        await self._evaluate_instance(st)
        is_open, _ = st.compute_desired_state()
        state_txt = "🟢 Open" if is_open else "🔴 Closed"
        embed = discord.Embed(
            title=f"{title} — {st.cfg.name}",
            description="Control returns to the configured automatic schedule.",
            color=0x3498DB
        )
        if config_warning:
            embed.add_field(name="⚠️ No schedule defined", value="Default config values restored.", inline=False)
        embed.add_field(name="Active schedule", value=f"{h['opening']} - {h['closing']}", inline=False)
        embed.add_field(name="Active days",     value=h["days"],                          inline=False)
        embed.add_field(name="Current status",  value=state_txt,                         inline=False)
        return embed

    # ── /jano_setup ───────────────────────────────────────────────────────

    @jano_group.command(name="setup", description="Configure bot resources (channels and roles)")
    @app_commands.describe(instance="Instance to configure (optional if only one)")
    async def jano_setup(self, interaction: discord.Interaction, instance: str = None):
        if not self._tiene_rol_global(interaction):
            asyncio.ensure_future(_reply_ephemeral(interaction, embed=_no_permission()))
            return

        guild     = self._get_guild()
        is_global = self._tiene_rol_global(interaction)

        if not self.states:
            vista = ViewSetupEmpty(guild, self)
            embed = discord.Embed(
                title="⚙️ Setup — First time configuration",
                description="No instances configured yet.\n\nAn **instance** is a set of channels the bot will manage — opening and closing on a schedule or manually.\n\nPress **➕ New instance** to create your first one.",
                color=0xE67E22
            )
            await interaction.response.send_message(embed=embed, view=vista, ephemeral=True)
            vista.message = await interaction.original_response()
            return

        if not await self._check_instances(interaction, instance):
            return
        st = self._resolve_instance(instance)
        if not st:
            await interaction.response.send_message(
                "❌ Specify an instance. Options: " + ", ".join(self._get_names()),
                ephemeral=True, delete_after=120
            )
            return
        if not self._is_authorized(interaction, st):
            asyncio.ensure_future(_reply_ephemeral(interaction, embed=_no_permission()))
            return

        vista = ViewSetup(st, is_global, guild, self)
        embed = discord.Embed(
            title=f"⚙️ Setup — {st.cfg.name}",
            description="What would you like to configure?",
            color=0x3498DB
        )
        await interaction.response.send_message(embed=embed, view=vista, ephemeral=True)
        vista.message = await interaction.original_response()

    @jano_setup.autocomplete("instance")
    async def _ac_setup(self, interaction, current):
        return await self._autocomplete_instance(interaction, current)


# ══════════════════════════════════════════════════════════════════════════════
# SHARED UTILITIES
# ══════════════════════════════════════════════════════════════════════════════

def _fmt_duration(td: datetime.timedelta) -> str:
    total_min = int(td.total_seconds() // 60)
    return f"{total_min // 60}:{total_min % 60:02d}"

def _no_permission(msg: str = "❌ You do not have permission to use this command.") -> discord.Embed:
    return discord.Embed(description=msg, color=0xE67E22)

async def _delete_after(message: discord.Message, delay: int = 120):
    await asyncio.sleep(delay)
    try:
        await message.delete()
    except Exception:
        pass

async def _followup_send(interaction: discord.Interaction, embed: discord.Embed, delay: int = 120):
    msg = await interaction.followup.send(embed=embed, ephemeral=True)
    await asyncio.sleep(delay)
    try:
        await msg.delete()
    except Exception:
        pass

async def _reply_ephemeral(interaction: discord.Interaction, delay: int = 120, **kwargs):
    await interaction.response.send_message(ephemeral=True, **kwargs)
    try:
        msg = await interaction.original_response()
        await asyncio.sleep(delay)
        await msg.delete()
    except Exception:
        pass

async def _update_category_name(category, is_open: bool, st: InstanceState):
    """Rename category with 🟢🔴 if status_icon is enabled, or clean up emojis if just disabled."""
    if not st.cfg.status_icon:
        # Status icon OFF — ensure no leftover emojis on the category name
        clean_name = re.sub(r"[🟢🔴]\s*", "", category.name).strip()
        if category.name != clean_name:
            try:
                await category.edit(name=clean_name)
                log.info(f"[Jano/{st.cfg.name}] 🧹 Emojis removed from category name")
            except discord.HTTPException as e:
                if e.status != 429:
                    log.error(f"[Jano/{st.cfg.name}] Error cleaning category name: {e}")
        return
    # Status icon ON — rename with 🟢🔴
    clean_name = re.sub(r"[🟢🔴]\s*", "", category.name).strip()
    if clean_name != st.category_name_cache:
        st.category_name_cache = clean_name
        st.save()
    emoji          = "🟢" if is_open else "🔴"
    target_name = f"{emoji} {clean_name} {emoji}"
    if category.name == target_name:
        return
    try:
        await category.edit(name=target_name)
    except discord.HTTPException as e:
        if e.status != 429:
            log.error(f"[Jano/{st.cfg.name}] Error renaming category: {e}")


# ══════════════════════════════════════════════════════════════════════════════
# BASE VIEW
# ══════════════════════════════════════════════════════════════════════════════

class BotView(discord.ui.View):
    def __init__(self, timeout: int = 120):
        super().__init__(timeout=timeout)
        self.message: discord.Message | None = None

    async def _close_message(self, embed: discord.Embed, delete_after: int = 0):
        if self.message:
            try:
                await self.message.edit(embed=embed, view=None)
                if delete_after > 0:
                    await asyncio.sleep(delete_after)
                    await self.message.delete()
            except Exception:
                pass

    async def on_timeout(self):
        await self._close_message(
            discord.Embed(description="⏱️ Interaction expired — use the command again if needed.", color=0x95A5A6),
            delete_after=30
        )


# ══════════════════════════════════════════════════════════════════════════════
# VIEWS — Close confirmation
# ══════════════════════════════════════════════════════════════════════════════

class ViewCloseConfirm(BotView):
    def __init__(self, st: InstanceState, info_manual, plugin: Jano):
        super().__init__(timeout=120)
        self.st          = st
        self.info_manual = info_manual
        self.plugin      = plugin

    @discord.ui.button(label="Keep manual mode", style=discord.ButtonStyle.secondary, emoji="⏳")
    async def keep_manual(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True)
        await self.plugin._evaluate_instance(self.st)
        if not self.info_manual.get("no_limit"):
            remaining = _fmt_duration(self.info_manual["remaining"])
            expira   = self.info_manual["expires_at"].strftime('%H:%M')
            desc     = f"Channels will return to automatic schedule in **{remaining}** (at **{expira}**)."
        else:
            desc = "Channels will remain closed in manual mode with no time limit."
        embed = discord.Embed(
            title=f"🔴 Access closed — {self.st.cfg.name}",
            description=desc,
            color=0xE74C3C
        )
        self.stop()
        view_resume = ViewResumeAuto(self.st, self.plugin)
        msg = await interaction.followup.send(embed=embed, view=view_resume, ephemeral=True, wait=True)
        view_resume.message = msg

    @discord.ui.button(label="Resume automatic schedule", style=discord.ButtonStyle.primary, emoji="♻️")
    async def resume_schedule(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True)
        self.stop()
        embed = await self.plugin._execute_resume(self.st, "🔴 Access closed · ♻️ Schedule resumed")
        await _followup_send(interaction, embed)


class ViewResumeAuto(BotView):
    def __init__(self, st: InstanceState, plugin: Jano):
        super().__init__(timeout=180)
        self.st     = st
        self.plugin = plugin

    @discord.ui.button(label="Resume automatic schedule", style=discord.ButtonStyle.primary, emoji="♻️")
    async def btn_resume_auto(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True)
        self.stop()
        embed = await self.plugin._execute_resume(self.st, "♻️ Schedule resumed")
        await _followup_send(interaction, embed)


# ══════════════════════════════════════════════════════════════════════════════
# VIEWS — Setup
# ══════════════════════════════════════════════════════════════════════════════

class ViewSetupEmpty(BotView):
    def __init__(self, guild: discord.Guild, plugin: Jano):
        super().__init__(timeout=120)
        self.guild  = guild
        self.plugin = plugin

    @discord.ui.button(label="New instance", style=discord.ButtonStyle.success, emoji="➕")
    async def btn_new(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(WizardStep1Name(self.plugin))


class ViewSetup(BotView):
    def __init__(self, st: InstanceState, is_global: bool, guild: discord.Guild, plugin: Jano):
        super().__init__(timeout=120)
        self.st        = st
        self.is_global = is_global
        self.guild     = guild
        self.plugin    = plugin
        if not is_global:
            self.remove_item(self.configure_access_roles)

    @discord.ui.button(label="Edit instance", style=discord.ButtonStyle.primary, emoji="✏️", row=0)
    async def btn_edit_instance(self, interaction: discord.Interaction, button: discord.ui.Button):
        data  = WizardData.from_state(self.st)
        vista = WizardStep5Summary(data, self.guild, self.plugin, edit_mode=True)
        embed = _wizard_summary_embed(data, self.guild, edit_mode='setup')
        await interaction.response.send_message(embed=embed, view=vista, ephemeral=True)
        vista.message = await interaction.original_response()

    @discord.ui.button(label="Command roles", style=discord.ButtonStyle.primary, emoji="🔑", row=0)
    async def configure_access_roles(self, interaction: discord.Interaction, button: discord.ui.Button):
        view_roles = ViewAccessRoles(self.guild, self.plugin)
        embed = discord.Embed(
            title="🔑 Configure command roles per instance",
            description="Select the roles for each instance.\n\n⚠️ **Your selection replaces current roles entirely.**\n\nIf you skip an instance, it will not be modified.",
            color=0x9B59B6
        )
        for name in self.plugin._get_names():
            st_inst = self.plugin.states[name]
            ri = st_inst.cfg.command_role_ids_instance
            if ri:
                guild_roles = [self.guild.get_role(r) for r in ri]
                val = ", ".join(r.name for r in guild_roles if r) or "❌ Not configured"
            else:
                val = "❌ Not configured"
            embed.add_field(name=f"Current — {name}", value=val, inline=False)
        await interaction.response.send_message(embed=embed, view=view_roles, ephemeral=True)
        view_roles.message = await interaction.original_response()

    @discord.ui.button(label="New instance", style=discord.ButtonStyle.success, emoji="➕", row=1)
    async def btn_new_instance(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not self.plugin._tiene_rol_global(interaction):
            asyncio.ensure_future(_reply_ephemeral(interaction, embed=_no_permission("❌ Only admin roles can create instances.")))
            return
        if len(self.plugin.states) >= 4:
            await interaction.response.send_message(
                "❌ Maximum of 4 instances reached. Delete one first.", ephemeral=True, delete_after=120
            )
            return
        await interaction.response.send_modal(WizardStep1Name(self.plugin))

    @discord.ui.button(label="Delete instance", style=discord.ButtonStyle.danger, emoji="🗑️", row=1)
    async def btn_delete_instance(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not self.plugin._tiene_rol_global(interaction):
            asyncio.ensure_future(_reply_ephemeral(interaction, embed=_no_permission("❌ Only admin roles can delete instances.")))
            return
        vista = ViewSelectDelete(self.guild, self.plugin)
        embed = discord.Embed(
            title="🗑️ Delete instance",
            description="Select the instance to delete.\n\n⚠️ This action is **irreversible**.",
            color=0xE74C3C
        )
        for name, st in self.plugin.states.items():
            cat = self.guild.get_channel(st.get_category_id())
            embed.add_field(name=name, value=f"📦 {cat.name if cat else '❌ Not found'}", inline=True)
        await interaction.response.send_message(embed=embed, view=vista, ephemeral=True)
        vista.message = await interaction.original_response()


# ══════════════════════════════════════════════════════════════════════════════
# VIEWS — Channel & role configuration (setup submenus)
# ══════════════════════════════════════════════════════════════════════════════

class ViewChannels(BotView):
    def __init__(self, st: InstanceState, guild: discord.Guild):
        super().__init__(timeout=120)
        self.st          = st
        self.guild       = guild
        self.selections = {"category": None, "text": None, "voice": None}
        self.type_sel    = None

        select_type = discord.ui.Select(
            placeholder="Category/Channel — Category or direct channel?",
            min_values=0, max_values=1, row=0, custom_id="sel_type",
            options=[
                discord.SelectOption(label="Discord Category",              value="category", emoji="📦"),
                discord.SelectOption(label="Direct channel (text or voice)", value="channel",    emoji="💬"),
            ]
        )
        select_type.callback = self._type_callback
        self.add_item(select_type)

        text_channels = sorted([c for c in guild.channels if isinstance(c, discord.TextChannel)], key=lambda c: c.position)
        if text_channels:
            sel_text = discord.ui.Select(
                placeholder="Text channel for announcements (optional)",
                min_values=0, max_values=1, row=1, custom_id="sel_text",
                options=[discord.SelectOption(label="❌ None", value="__none__", description="Clears the value")] +
                        [discord.SelectOption(label=f"#{c.name}"[:100], value=str(c.id)) for c in text_channels[:24]]
            )
            sel_text.callback = self._make_cb("text")
            self.add_item(sel_text)

        voice_channels = sorted([c for c in guild.channels if isinstance(c, discord.VoiceChannel)], key=lambda c: c.position)
        if voice_channels:
            sel_voice = discord.ui.Select(
                placeholder="Voice channel for announcement embed (optional)",
                min_values=0, max_values=1, row=2, custom_id="sel_voice",
                options=[discord.SelectOption(label="❌ None", value="__none__", description="Clears the value")] +
                        [discord.SelectOption(label=c.name[:100], value=str(c.id)) for c in voice_channels[:24]]
            )
            sel_voice.callback = self._make_cb("voice")
            self.add_item(sel_voice)

        btn = discord.ui.Button(label="Save channels", style=discord.ButtonStyle.success, emoji="💾", row=3)
        btn.callback = self.confirm_callback
        self.add_item(btn)

    def _make_cb(self, key: str):
        async def _cb(interaction: discord.Interaction):
            for item in self.children:
                if isinstance(item, discord.ui.Select) and item.custom_id == f"sel_{key}":
                    val = item.values[0] if item.values else None
                    self.selections[key] = "__none__" if val == "__none__" else (int(val) if val else None)
                    break
            await interaction.response.defer()
        return _cb

    async def _type_callback(self, interaction: discord.Interaction):
        for item in self.children:
            if isinstance(item, discord.ui.Select) and item.custom_id == "sel_type":
                self.type_sel = item.values[0] if item.values else None
                break
        if not self.type_sel:
            return await interaction.response.defer()
        if self.type_sel == "category":
            channels_list = sorted([c for c in self.guild.channels if isinstance(c, discord.CategoryChannel)], key=lambda c: c.position)
        else:
            channels_list = sorted([c for c in self.guild.channels if isinstance(c, (discord.TextChannel, discord.VoiceChannel))], key=lambda c: c.position)
        options = [discord.SelectOption(label=c.name[:100], value=str(c.id)) for c in channels_list[:25]]
        if not options:
            return await interaction.response.defer()
        label = "📦 Select category" if self.type_sel == "category" else "💬 Select channel"
        embed = discord.Embed(title=label, color=0x3498DB)
        picker = ViewCategoryPicker(self, options)
        await interaction.response.send_message(embed=embed, view=picker, ephemeral=True)
        picker.message = await interaction.original_response()

    async def confirm_callback(self, interaction: discord.Interaction):
        changes = []
        warnings  = []
        if self.selections["category"]:
            ch = self.guild.get_channel(self.selections["category"])
            if ch:
                prev_cat_id = self.st.get_category_id()
                self.st.cfg.category_id = ch.id
                self.st.category_name_cache = None
                changes.append(f"Category → {ch.name}")
        if self.selections["text"] == "__none__":
            self.st.cfg.text_channel_id = None
            changes.append("Text channel → ❌ None")
        elif self.selections["text"]:
            ch = self.guild.get_channel(self.selections["text"])
            if ch:
                self.st.cfg.text_channel_id = ch.id
                changes.append(f"Text channel → #{ch.name}")
        if self.selections["voice"] == "__none__":
            self.st.cfg.voice_channel_id = None
            changes.append("Voice channel → ❌ None")
        elif self.selections["voice"]:
            ch = self.guild.get_channel(self.selections["voice"])
            if ch:
                self.st.cfg.voice_channel_id = ch.id
                changes.append(f"Voice channel → #{ch.name}")
        if changes:
            self.st.save()
        self.stop()
        if self.message:
            try:
                await self.message.edit(embed=discord.Embed(
                    title=f"📺 Configure channels — {self.st.cfg.name}",
                    description="✅ Saved — channels updated successfully.", color=0x2ECC71
                ), view=None)
                asyncio.ensure_future(_delete_after(self.message))
            except Exception:
                pass
        embed = discord.Embed(title=f"📺 Channels updated — {self.st.cfg.name}", color=0x2ECC71)
        if changes:
            embed.add_field(name="Changes applied", value="\n".join(changes), inline=False)
        else:
            embed.description = "No changes were made."
        if warnings:
            embed.add_field(name="\u200b", value="\n".join(warnings), inline=False)
        asyncio.ensure_future(_reply_ephemeral(interaction, embed=embed))


class ViewCategoryPicker(BotView):
    def __init__(self, parent: ViewChannels, options: list):
        super().__init__(timeout=120)
        self.parent = parent
        select = discord.ui.Select(placeholder="Select an option", min_values=1, max_values=1, row=0, custom_id="sel_cat_pick", options=options)
        select.callback = self._select_callback
        self.add_item(select)
        btn = discord.ui.Button(label="Apply selection", style=discord.ButtonStyle.primary, emoji="↩️", row=1)
        btn.callback = self._confirmar_callback
        self.add_item(btn)

    async def _select_callback(self, interaction: discord.Interaction):
        for item in self.children:
            if isinstance(item, discord.ui.Select):
                self.parent.selections["category"] = int(item.values[0]) if item.values else None
                break
        await interaction.response.defer()

    async def _confirmar_callback(self, interaction: discord.Interaction):
        if not self.parent.selections["category"]:
            return await interaction.response.defer()
        ch     = self.parent.guild.get_channel(self.parent.selections["category"])
        name = ch.name if ch else "—"
        for item in self.parent.children:
            if isinstance(item, discord.ui.Select) and item.custom_id == "sel_type":
                item.placeholder = f"✅ Category/Channel: {name}"
                break
        self.stop()
        await interaction.response.defer()
        if self.message:
            asyncio.ensure_future(_delete_after(self.message, delay=0))
        embed = discord.Embed(title=f"📺 Configure channels — {self.parent.st.cfg.name}", description="Select the channels.", color=0x3498DB)
        embed.add_field(name="✅ Category / Channel", value=name, inline=False)
        if self.parent.message:
            try:
                await self.parent.message.edit(embed=embed, view=self.parent)
            except Exception:
                pass


class ViewChannelRoles(BotView):
    def __init__(self, st: InstanceState, guild: discord.Guild):
        super().__init__(timeout=120)
        self.st          = st
        self.guild       = guild
        self.selections = {"access": None, "mention": None}

        roles = sorted([r for r in guild.roles if r.name != "@everyone"], key=lambda r: -r.position)
        role_options = [discord.SelectOption(label=r.name[:100], value=str(r.id)) for r in roles[:24]]
        vis_options = [discord.SelectOption(label="🌐 @everyone (all)", value="__everyone__", description="All users")] + role_options[:24]
        men_options = [discord.SelectOption(label="❌ No mention", value="__none__", description="No ping")] + role_options[:24]

        sel_access = discord.ui.Select(placeholder="👁️ Visibility role (empty = no change)", min_values=0, max_values=1, row=0, custom_id="sel_access", options=vis_options)
        sel_access.callback = self._make_cb("access")
        self.add_item(sel_access)

        sel_mention = discord.ui.Select(placeholder="📣 Mention role (empty = no change)", min_values=0, max_values=1, row=1, custom_id="sel_mention", options=men_options)
        sel_mention.callback = self._make_cb("mention")
        self.add_item(sel_mention)

        btn = discord.ui.Button(label="Save roles", style=discord.ButtonStyle.success, emoji="💾", row=2)
        btn.callback = self.confirm_callback
        self.add_item(btn)

    def _make_cb(self, key: str):
        async def _cb(interaction: discord.Interaction):
            for item in self.children:
                if isinstance(item, discord.ui.Select) and item.custom_id == f"sel_{key}":
                    val = item.values[0] if item.values else None
                    self.selections[key] = "__none__" if val == "__none__" else (int(val) if val else None)
                    break
            await interaction.response.defer()
        return _cb

    async def confirm_callback(self, interaction: discord.Interaction):
        changes = []
        if self.selections["access"] == "__everyone__":
            self.st.cfg.role_id = None
            changes.append("Visibility role → 🌐 @everyone")
        elif self.selections["access"] == "__none__":
            self.st.cfg.role_id = None
            changes.append("Visibility role → ❌ None")
        elif self.selections["access"] is not None:
            r = self.guild.get_role(self.selections["access"])
            if r:
                self.st.cfg.role_id = r.id
                changes.append(f"Visibility role → {r.name}")
        if self.selections["mention"] == "__none__":
            self.st.cfg.mention_role_id = None
            changes.append("Mention role → ❌ No mention")
        elif self.selections["mention"] is not None:
            r = self.guild.get_role(self.selections["mention"])
            if r:
                self.st.cfg.mention_role_id = r.id
                changes.append(f"Mention role → {r.name}")
        if changes:
            self.st.save()
        self.stop()
        if self.message:
            try:
                await self.message.edit(embed=discord.Embed(
                    title=f"👥 Configure roles — {self.st.cfg.name}",
                    description="✅ Saved — roles updated successfully.", color=0x2ECC71
                ), view=None)
                asyncio.ensure_future(_delete_after(self.message))
            except Exception:
                pass
        embed = discord.Embed(title=f"👥 Roles updated — {self.st.cfg.name}", color=0x2ECC71)
        if changes:
            embed.add_field(name="Changes applied", value="\n".join(changes), inline=False)
        else:
            embed.description = "No changes were made."
        asyncio.ensure_future(_reply_ephemeral(interaction, embed=embed))


class ModalManualLimit(discord.ui.Modal, title="Configure manual mode limit"):
    _f_hours = discord.ui.TextInput(
        label="Max hours (0 = no limit, empty = keep)",
        placeholder="E.g.: 2.5 · 0 = unlimited · leave empty to keep",
        required=False, max_length=5
    )

    def __init__(self, st: InstanceState):
        super().__init__()
        self.st = st
        ceiling = st.active_ceiling()
        self._f_hours.default = str(ceiling) if ceiling > 0 else "0"

    async def on_submit(self, interaction: discord.Interaction):
        raw = self._f_hours.value.strip().replace(",", ".")
        if not raw:
            ceiling = self.st.active_ceiling()
            embed = discord.Embed(title=f"⏱️ Manual limit — {self.st.cfg.name}", color=0x3498DB)
            embed.description = "No changes were made."
            embed.add_field(name="Current maximum", value=f"**{ceiling}h**" if ceiling > 0 else "**No limit**", inline=False)
            asyncio.ensure_future(_reply_ephemeral(interaction, embed=embed))
            return
        try:
            val = float(raw)
            if val < 0:
                raise ValueError
        except ValueError:
            await interaction.response.send_message("❌ Invalid value. Enter a positive number or 0 for no limit.", ephemeral=True, delete_after=120)
            return
        old_ceiling = self.st.active_ceiling()
        self.st.max_hours_override = val if val > 0 else 0
        self.st.save()
        trim_warning = None
        if self.st.manual_override is not None and self.st.manual_hours_active > 0:
            new_ceiling = self.st.active_ceiling()
            if new_ceiling > 0 and self.st.manual_hours_active > new_ceiling:
                duracion_anterior = self.st.manual_hours_active
                self.st.manual_hours_active = new_ceiling
                self.st.save()
                trim_warning = (duracion_anterior, new_ceiling)
        embed = discord.Embed(title=f"⏱️ Manual limit updated — {self.st.cfg.name}", color=0x2ECC71)
        embed.add_field(name="Previous maximum", value=f"**{old_ceiling}h**" if old_ceiling > 0 else "**No limit**", inline=False)
        embed.add_field(name="New maximum",      value=f"**{val}h**" if val > 0 else "**No limit**", inline=False)
        if trim_warning:
            embed.add_field(name="⚠️ Active duration adjusted", value=f"Was **{trim_warning[0]}h** → trimmed to **{trim_warning[1]}h**.", inline=False)
        asyncio.ensure_future(_reply_ephemeral(interaction, embed=embed))


# ══════════════════════════════════════════════════════════════════════════════
# VIEWS — Delete instance
# ══════════════════════════════════════════════════════════════════════════════

class ViewSelectDelete(BotView):
    def __init__(self, guild: discord.Guild, plugin: Jano):
        super().__init__(timeout=120)
        self.guild       = guild
        self.plugin      = plugin
        self._selected_name = None

        options = [discord.SelectOption(label=n, value=n, description=f"Delete '{n}'") for n in plugin._get_names()]
        select = discord.ui.Select(placeholder="Select instance to delete...", min_values=1, max_values=1, options=options, row=0)
        select.callback = self._select_callback
        self.add_item(select)

        btn = discord.ui.Button(label="Continue →", style=discord.ButtonStyle.danger, emoji="⚠️", row=1)
        btn.callback = self._next_callback
        self.add_item(btn)

    async def _select_callback(self, interaction: discord.Interaction):
        for item in self.children:
            if isinstance(item, discord.ui.Select):
                self._selected_name = item.values[0] if item.values else None
                break
        await interaction.response.defer()

    async def _next_callback(self, interaction: discord.Interaction):
        if not self._selected_name:
            await interaction.response.send_message("❌ Please select an instance first.", ephemeral=True, delete_after=120)
            return
        is_last = len(self.plugin.states) == 1
        await interaction.response.send_modal(ModalConfirmDelete(self._selected_name, is_last, self.plugin))


class ModalConfirmDelete(discord.ui.Modal, title="⚠️ Confirm deletion"):
    confirmacion = discord.ui.TextInput(label='Type DELETE to confirm', placeholder="DELETE", required=True, max_length=10)

    def __init__(self, name: str, is_last: bool, plugin: Jano):
        super().__init__()
        self.name    = name
        self.is_last = is_last
        self.plugin    = plugin
        titulo = f"Delete '{name}' — type DELETE"
        if len(titulo) <= 45:
            self.title = titulo

    async def on_submit(self, interaction: discord.Interaction):
        if self.confirmacion.value.strip() != "DELETE":
            await interaction.response.send_message(
                "❌ Incorrect confirmation. Instance was **not** deleted.", ephemeral=True, delete_after=120
            )
            return
        name = self.name
        if name not in self.plugin.states:
            await interaction.response.send_message(f"❌ Instance '{name}' no longer exists.", ephemeral=True, delete_after=120)
            return
        await self.plugin._delete_instance(name)
        log.info(f"[Jano] 🗑️ Instance '{name}' deleted by {interaction.user}")
        if self.is_last:
            embed = discord.Embed(
                title=f"🗑️ Instance '{name}' deleted",
                description="⚠️ **This was the last instance.**\n\nUse **/jano setup** to create a new one.",
                color=0xE74C3C
            )
        else:
            embed = discord.Embed(
                title=f"✅ Instance '{name}' deleted",
                description=f"Remaining: {', '.join(self.plugin.states.keys())}",
                color=0x2ECC71
            )
        asyncio.ensure_future(_reply_ephemeral(interaction, embed=embed))


# ══════════════════════════════════════════════════════════════════════════════
# WIZARD — New / Edit instance (5 steps)
# ══════════════════════════════════════════════════════════════════════════════

class WizardData:
    def __init__(self):
        self.name:           str   = ""
        self.original_name:  str   = ""
        self.st:               object = None
        self.category_id:      int   = None
        self.text_channel_id:  int   = None
        self.voice_channel_id: int   = None
        self.role_id:          int   = None
        self.mention_role_id:  int   = None
        self.active_days:     list  = []
        self.opening_time:    str   = "19:00"
        self.closing_time:      str   = "22:00"
        self.max_manual_hours: float = 0.0
        self.status_icon:      bool  = True

    @classmethod
    def from_state(cls, st: InstanceState) -> "WizardData":
        d = cls()
        d.st               = st
        d.original_name  = st.cfg.name
        d.name           = st.cfg.name
        d.category_id      = st.cfg.category_id
        d.text_channel_id  = st.cfg.text_channel_id
        d.voice_channel_id = st.cfg.voice_channel_id
        d.role_id          = st.cfg.role_id
        d.mention_role_id  = st.cfg.mention_role_id
        d.active_days     = list(st.cfg.active_days)
        d.opening_time    = st.cfg.opening_time
        d.closing_time      = st.cfg.closing_time
        d.max_manual_hours = st.cfg.max_manual_hours
        d.status_icon      = st.cfg.status_icon
        return d


def _wizard_embed(step: int, name: str, description: str) -> discord.Embed:
    titles = {2: "📺 Channels", 3: "👥 Roles", 6: "📋 Summary"}
    return discord.Embed(title=f"⚙️ '{name}' — {titles.get(step, f'Step {step}')}", description=description, color=0x3498DB)

def _wizard_edit_embed(step: int, name: str, description: str) -> discord.Embed:
    titles = {2: "📺 Channels", 3: "👥 Roles", 4: "📅 Schedule & Limit", 5: "📋 Summary"}
    return discord.Embed(title=f"✏️ '{name}' — {titles.get(step, f'Step {step}')}", description=description, color=0x3498DB)

def _wizard_summary_embed(data: WizardData, guild: discord.Guild, edit_mode=False) -> discord.Embed:
    day_map = {0:"Mon",1:"Tue",2:"Wed",3:"Thu",4:"Fri",5:"Sat",6:"Sun"}
    if edit_mode == 'setup':
        title, description = f"⚙️ Instance settings — '{data.name}'", "Current configuration. Use ✏️ buttons to modify, then **💾 Save changes**."
    elif edit_mode:
        title, description = f"📋 Review changes — '{data.name}'", "Review changes. Press **💾 Save changes** to confirm."
    else:
        title, description = f"📋 Review — '{data.name}'", "Review settings. Press **✅ Create instance** to confirm."
    embed = discord.Embed(title=title, description=description, color=0x9B59B6)
    cat = guild.get_channel(data.category_id)
    embed.add_field(name="📦 Category / Channel", value=f"**{cat.name}**" if cat else "❌ Not set", inline=False)
    txt = guild.get_channel(data.text_channel_id) if data.text_channel_id else None
    embed.add_field(name="💬 Text channel",  value=f"**#{txt.name}**" if txt else "*Not configured*", inline=True)
    voice = guild.get_channel(data.voice_channel_id) if data.voice_channel_id else None
    embed.add_field(name="🔊 Voice channel", value=f"**{voice.name}**" if voice else "*Not configured*", inline=True)
    embed.add_field(name="\u200b", value="\u200b", inline=False)
    if data.role_id:
        role = guild.get_role(data.role_id)
        role_txt = f"**{role.name}**" if role else "⚠️ Not found"
    else:
        role_txt = "**🌐 @everyone**"
    embed.add_field(name="👁️ Visibility role", value=role_txt, inline=True)
    if data.mention_role_id:
        mr = guild.get_role(data.mention_role_id)
        mention_txt = f"**{mr.name}**" if mr else "⚠️ Not found"
    else:
        mention_txt = "*Not configured*"
    embed.add_field(name="📣 Mention role", value=mention_txt, inline=True)
    embed.add_field(name="\u200b", value="\u200b", inline=False)
    if data.active_days:
        days_txt = ", ".join(day_map[d] for d in data.active_days if d in day_map)
        embed.add_field(name="📅 Schedule",    value=f"**{data.opening_time} - {data.closing_time}**", inline=True)
        embed.add_field(name="📆 Active days", value=f"**{days_txt}**", inline=True)
    else:
        embed.add_field(name="📅 Schedule", value="*No schedule — manual mode only*", inline=False)
    embed.add_field(name="\u200b", value="\u200b", inline=False)
    embed.add_field(name="⏱️ Manual limit", value=f"**{data.max_manual_hours}h**" if data.max_manual_hours > 0 else "**No limit**", inline=False)
    si_val = "**Enabled** — open/closed state shown on category name with 🟢🔴" if data.status_icon else "**Disabled** — category name is never modified"
    embed.add_field(name="Status Icon", value=si_val, inline=False)
    return embed


# ── Step 1: Name ──────────────────────────────────────────────────────────────

def _parse_status_icon(value: str, default: bool = False) -> bool:
    """Parse a yes/no text input into a boolean. Tolerant of common variants."""
    v = value.strip().lower()
    if v in ("yes", "y", "on", "1", "true"):
        return True
    if v in ("no", "n", "off", "0", "false"):
        return False
    return default


class WizardStep1Name(discord.ui.Modal, title="Instance name & Status Icon"):

    def __init__(self, plugin: Jano):
        super().__init__()
        self.plugin = plugin
        self._f_nombre = discord.ui.TextInput(
            label="Instance name (required)",
            placeholder="E.g.: Missions, Training, Events...",
            required=True, max_length=32
        )
        self._f_status = discord.ui.TextInput(
            label="Status Icon — show 🟢🔴 on category? (yes/no)",
            placeholder="yes = show 🟢🔴 on category  |  no = keep original name  |  default: no",
            required=False, max_length=5
        )
        self.add_item(self._f_nombre)
        self.add_item(self._f_status)

    async def on_submit(self, interaction: discord.Interaction):
        name = self._f_nombre.value.strip()
        if name in self.plugin.states:
            await interaction.response.send_message(f"❌ An instance named **{name}** already exists.", ephemeral=True, delete_after=120)
            return
        data  = WizardData()
        data.name      = name
        data.status_icon = _parse_status_icon(self._f_status.value, default=False)
        guild = self.plugin._get_guild()
        vista = WizardStep2Channels(data, guild, self.plugin)
        embed = _wizard_embed(2, name, "Select the channels for this instance.\n\n**Category/Channel** is required.\nText and voice channels are optional.")
        embed.add_field(name="📦 Category / Channel", value="*Required — what the bot will open and close*", inline=False)
        embed.add_field(name="💬 Text channel",       value="*Channel for opening announcements (optional)*", inline=False)
        embed.add_field(name="🔊 Voice channel",      value="*Voice channel shown in the announcement (optional)*", inline=False)
        await interaction.response.send_message(embed=embed, view=vista, ephemeral=True)
        vista.message = await interaction.original_response()


# ── Step 2: Channels ──────────────────────────────────────────────────────────

class WizardStep2Channels(BotView):
    def __init__(self, data: WizardData, guild: discord.Guild, plugin: Jano, summary=None, edit_mode: bool = False):
        super().__init__(timeout=120)
        self.data         = data
        self.guild        = guild
        self.plugin       = plugin
        self.type_sel     = None
        self.summary      = summary
        self.edit_mode = edit_mode

        ph_cat = "📦 Change Category/Channel (skip to keep)" if edit_mode else "📦 Category / Channel — select type first"
        sel_type = discord.ui.Select(placeholder=ph_cat, min_values=0, max_values=1, row=0, custom_id="w_sel_type",
            options=[
                discord.SelectOption(label="Discord Category",              value="category", emoji="📦"),
                discord.SelectOption(label="Direct channel (text or voice)", value="channel",    emoji="💬"),
            ])
        sel_type.callback = self._type_callback
        self.add_item(sel_type)

        text_channels = sorted([c for c in guild.channels if isinstance(c, discord.TextChannel)], key=lambda c: c.position)
        if text_channels:
            lbl_none = "❌ Remove" if edit_mode else "❌ None"
            sel_text = discord.ui.Select(placeholder="💬 Text channel for announcements (optional)", min_values=0, max_values=1, row=1, custom_id="w_sel_text",
                options=[discord.SelectOption(label=lbl_none, value="__none__")] +
                        [discord.SelectOption(label=f"#{c.name}"[:100], value=str(c.id)) for c in text_channels[:24]])
            sel_text.callback = self._make_cb("text")
            self.add_item(sel_text)

        voice_channels = sorted([c for c in guild.channels if isinstance(c, discord.VoiceChannel)], key=lambda c: c.position)
        if voice_channels:
            lbl_none = "❌ Remove" if edit_mode else "❌ None"
            sel_voice = discord.ui.Select(placeholder="🔊 Voice channel for announcement (optional)", min_values=0, max_values=1, row=2, custom_id="w_sel_voice",
                options=[discord.SelectOption(label=lbl_none, value="__none__")] +
                        [discord.SelectOption(label=c.name[:100], value=str(c.id)) for c in voice_channels[:24]])
            sel_voice.callback = self._make_cb("voice")
            self.add_item(sel_voice)

        btn = discord.ui.Button(label="Apply", style=discord.ButtonStyle.primary, emoji="💾", row=3)
        btn.callback = self._next_callback
        self.add_item(btn)

    def _make_cb(self, key: str):
        async def _cb(interaction: discord.Interaction):
            for item in self.children:
                if isinstance(item, discord.ui.Select) and item.custom_id == f"w_sel_{key}":
                    val = item.values[0] if item.values else None
                    if key == "text":
                        self.data.text_channel_id = None if (not val or val == "__none__") else int(val)
                    else:
                        self.data.voice_channel_id = None if (not val or val == "__none__") else int(val)
                    break
            await interaction.response.defer()
        return _cb

    async def _type_callback(self, interaction: discord.Interaction):
        for item in self.children:
            if isinstance(item, discord.ui.Select) and item.custom_id == "w_sel_type":
                self.type_sel = item.values[0] if item.values else None
                break
        if not self.type_sel:
            return await interaction.response.defer()
        if self.type_sel == "category":
            channels_list = sorted([c for c in self.guild.channels if isinstance(c, discord.CategoryChannel)], key=lambda c: c.position)
        else:
            channels_list = sorted([c for c in self.guild.channels if isinstance(c, (discord.TextChannel, discord.VoiceChannel))], key=lambda c: c.position)
        options = [discord.SelectOption(label=c.name[:100], value=str(c.id)) for c in channels_list[:25]]
        if not options:
            return await interaction.response.defer()
        label  = "📦 Select category" if self.type_sel == "category" else "💬 Select channel"
        picker = WizardCategoryPicker(self, options)
        await interaction.response.send_message(embed=discord.Embed(title=label, color=0x3498DB), view=picker, ephemeral=True)
        picker.message = await interaction.original_response()

    async def _next_callback(self, interaction: discord.Interaction):
        if not self.data.category_id:
            await interaction.response.send_message("❌ You must select a Category/Channel before continuing.", ephemeral=True, delete_after=120)
            return
        self.stop()
        if self.summary:
            embed_summary = _wizard_summary_embed(self.data, self.guild)
            try:
                await self.summary.message.edit(embed=embed_summary, view=self.summary)
            except Exception:
                pass
            embed_closed = discord.Embed(title="✅ Channels updated", description="Changes saved above.\n\nPress **💾 Save changes** to apply.", color=0x2ECC71)
            await interaction.response.edit_message(embed=embed_closed, view=None)
            msg = await interaction.original_response()
            asyncio.ensure_future(_delete_after(msg, delay=10))
            return
        vista = WizardStep3Roles(self.data, self.guild, self.plugin, edit_mode=self.edit_mode)
        if self.edit_mode:
            embed = _wizard_edit_embed(3, self.data.name, "Update the roles. Leave selects untouched to keep current values.")
            r = self.guild.get_role(self.data.role_id) if self.data.role_id else None
            embed.add_field(name="👁️ Current Visibility role", value=f"**{r.name}**" if r else "**🌐 @everyone**", inline=True)
            mr = self.guild.get_role(self.data.mention_role_id) if self.data.mention_role_id else None
            embed.add_field(name="📣 Current Mention role", value=f"**{mr.name}**" if mr else "*Not configured*", inline=True)
        else:
            embed = _wizard_embed(3, self.data.name, "Select the roles.\n\nBoth optional. Empty Visibility role = @everyone.")
            embed.add_field(name="👁️ Visibility role", value="*Role that gains/loses access*\n*(empty = @everyone)*", inline=False)
            embed.add_field(name="📣 Mention role",    value="*Role pinged when channels open on schedule (optional)*", inline=False)
        embed_closed = discord.Embed(description="✅ Channels saved — continuing to roles...", color=0x2ECC71)
        await interaction.response.edit_message(embed=embed_closed, view=None)
        msg_cerrado = await interaction.original_response()
        asyncio.ensure_future(_delete_after(msg_cerrado, delay=3))
        msg = await interaction.followup.send(embed=embed, view=vista, ephemeral=True, wait=True)
        vista.message = msg


class WizardCategoryPicker(BotView):
    def __init__(self, parent: WizardStep2Channels, options: list):
        super().__init__(timeout=120)
        self.parent = parent
        select = discord.ui.Select(placeholder="Select an option", min_values=1, max_values=1, row=0, custom_id="w_cat_pick", options=options)
        select.callback = self._select_callback
        self.add_item(select)
        btn = discord.ui.Button(label="Apply selection", style=discord.ButtonStyle.primary, emoji="↩️", row=1)
        btn.callback = self._confirmar_callback
        self.add_item(btn)

    async def _select_callback(self, interaction: discord.Interaction):
        for item in self.children:
            if isinstance(item, discord.ui.Select):
                self.parent.data.category_id = int(item.values[0]) if item.values else None
                break
        await interaction.response.defer()

    async def _confirmar_callback(self, interaction: discord.Interaction):
        if not self.parent.data.category_id:
            return await interaction.response.defer()
        ch     = self.parent.guild.get_channel(self.parent.data.category_id)
        name = ch.name if ch else "—"
        for item in self.parent.children:
            if isinstance(item, discord.ui.Select) and item.custom_id == "w_sel_type":
                item.placeholder = f"✅ Selected: {name}"
                break
        self.stop()
        await interaction.response.defer()
        if self.message:
            asyncio.ensure_future(_delete_after(self.message, delay=0))
        embed_parent = _wizard_embed(2, self.parent.data.name, "Select the channels for this instance.")
        embed_parent.add_field(name="✅ Category / Channel", value=f"**{name}**", inline=False)
        if self.parent.message:
            try:
                await self.parent.message.edit(embed=embed_parent, view=self.parent)
            except Exception:
                pass


# ── Step 3: Roles ─────────────────────────────────────────────────────────────

class WizardStep3Roles(BotView):
    def __init__(self, data: WizardData, guild: discord.Guild, plugin: Jano, summary=None, edit_mode: bool = False):
        super().__init__(timeout=120)
        self.data            = data
        self.guild           = guild
        self.plugin          = plugin
        self.summary         = summary
        self.edit_mode    = edit_mode
        self._sel_visibility = None
        self._sel_mention    = None

        roles = sorted([r for r in guild.roles if r.name != "@everyone"], key=lambda r: -r.position)
        role_options = [discord.SelectOption(label=r.name[:100], value=str(r.id)) for r in roles[:25]]
        vis_options = [discord.SelectOption(label="🌐 @everyone (all)", value="__everyone__", description="Apply to all")] + role_options[:24]
        men_options = [discord.SelectOption(label="❌ No mention",      value="__none__", description="No ping")]       + role_options[:24]

        sel_vis = discord.ui.Select(placeholder="👁️ Visibility role (optional — empty = @everyone)", min_values=0, max_values=1, row=0, custom_id="w_sel_vis", options=vis_options)
        sel_vis.callback = self._make_cb("vis")
        self.add_item(sel_vis)

        sel_mention = discord.ui.Select(placeholder="📣 Mention role (optional — empty = no ping)", min_values=0, max_values=1, row=1, custom_id="w_sel_men", options=men_options)
        sel_mention.callback = self._make_cb("men")
        self.add_item(sel_mention)

        btn = discord.ui.Button(label="Apply", style=discord.ButtonStyle.primary, emoji="💾", row=2)
        btn.callback = self._next_callback
        self.add_item(btn)

    def _make_cb(self, key: str):
        async def _cb(interaction: discord.Interaction):
            for item in self.children:
                if isinstance(item, discord.ui.Select) and item.custom_id == f"w_sel_{key}":
                    val = item.values[0] if item.values else None
                    if key == "vis":
                        self._sel_visibility = val
                    else:
                        self._sel_mention = val
                    break
            await interaction.response.defer()
        return _cb

    async def _next_callback(self, interaction: discord.Interaction):
        if self._sel_visibility is not None:
            self.data.role_id = None if self._sel_visibility == "__everyone__" else int(self._sel_visibility)
        if self._sel_mention is not None:
            self.data.mention_role_id = None if self._sel_mention == "__none__" else int(self._sel_mention)
        self.stop()
        if self.summary:
            embed_summary = _wizard_summary_embed(self.data, self.guild)
            try:
                await self.summary.message.edit(embed=embed_summary, view=self.summary)
            except Exception:
                pass
            embed_closed = discord.Embed(title="✅ Roles updated", description="Changes saved above.\n\nPress **💾 Save changes** to apply.", color=0x2ECC71)
            await interaction.response.edit_message(embed=embed_closed, view=None)
            msg = await interaction.original_response()
            asyncio.ensure_future(_delete_after(msg, delay=10))
            return
        if self.message:
            try:
                await self.message.edit(embed=discord.Embed(description="✅ Roles updated.", color=0x2ECC71), view=None)
                asyncio.ensure_future(_delete_after(self.message))
            except Exception:
                pass
        modal = WizardStep4Schedule(self.data, self.plugin) if not self.edit_mode else WizardEditStep4Schedule(self.data, self.plugin)
        await interaction.response.send_modal(modal)


# ── Step 4: Schedule ─────────────────────────────────────────────────────────

class ViewRetry(BotView):
    """Shows an error message with a button to reopen the schedule modal."""
    def __init__(self, modal_class, modal_kwargs: dict, error: str):
        super().__init__(timeout=120)
        self.modal_class  = modal_class
        self.modal_kwargs = modal_kwargs
        self.error        = error
        btn = discord.ui.Button(label="✏️ Fix and try again", style=discord.ButtonStyle.primary)
        btn.callback = self._reopen
        self.add_item(btn)

    async def _reopen(self, interaction: discord.Interaction):
        self.stop()
        if self.message:
            try:
                await self.message.delete()
            except Exception:
                pass
        await interaction.response.send_modal(self.modal_class(**self.modal_kwargs))


class WizardStep4Schedule(discord.ui.Modal):

    def __init__(self, data: WizardData, plugin: Jano, summary=None, error: str = None):
        super().__init__(title="Schedule & Limit")
        self.data    = data
        self.plugin  = plugin
        self.summary = summary
        self._f_dias = discord.ui.TextInput(
            label="Active days (empty = manual mode only)",
            placeholder="E.g.: 0,1,2,3,4  (0=Mon, 6=Sun)",
            required=False, max_length=20,
            default=",".join(str(d) for d in data.active_days) if data.active_days else ""
        )
        self._f_apertura = discord.ui.TextInput(
            label="Opening time (HH:MM)",
            placeholder="E.g.: 18:15",
            required=False, max_length=5,
            default=data.opening_time if data.active_days else ""
        )
        self._f_cierre = discord.ui.TextInput(
            label="Closing time (HH:MM)",
            placeholder="E.g.: 21:30",
            required=False, max_length=5,
            default=data.closing_time if data.active_days else ""
        )
        self._f_horas = discord.ui.TextInput(
            label="Max manual hours (0 or empty = no limit)",
            placeholder="E.g.: 2.5",
            required=False, max_length=5,
            default=str(data.max_manual_hours) if data.max_manual_hours > 0 else ""
        )
        for f in [self._f_dias, self._f_apertura, self._f_cierre, self._f_horas]:
            self.add_item(f)

    async def _send_error(self, interaction, error: str):
        """Send ephemeral error message with button to reopen modal with pre-filled data."""
        vista = ViewRetry(
            modal_class=WizardStep4Schedule,
            modal_kwargs={"data": self.data, "plugin": self.plugin, "summary": self.summary},
            error=error
        )
        embed = discord.Embed(
            title="⚠️ Invalid input — Schedule & Limit",
            description=f"**{error}**\n\nPress the button below to go back and correct it.",
            color=0xE74C3C
        )
        await interaction.response.send_message(embed=embed, view=vista, ephemeral=True)
        try:
            vista.message = await interaction.original_response()
        except Exception:
            pass


    async def on_submit(self, interaction: discord.Interaction):
        data         = self.data
        days_raw     = self._f_dias.value.strip()
        opening_raw = self._f_apertura.value.strip()
        closing_raw   = self._f_cierre.value.strip()
        hours_raw    = self._f_horas.value.strip().replace(",", ".")
        pattern = re.compile(r"^\d{1,2}:\d{2}$")
        if days_raw:
            if not opening_raw or not closing_raw:
                await self._send_error(interaction, "Opening and closing times are required when days are specified")
                return
            if not pattern.match(opening_raw) or not pattern.match(closing_raw):
                await self._send_error(interaction, "Invalid time format — use HH:MM (e.g. 18:15)")
                return
            try:
                open_h, open_m = map(int, opening_raw.split(":"))
                close_h, close_m = map(int, closing_raw.split(":"))
                assert 0 <= open_h <= 23 and 0 <= open_m <= 59
                assert 0 <= close_h <= 23 and 0 <= close_m <= 59
                # Allow overnight: only reject if opening == closing
                assert open_h * 60 + open_m != close_h * 60 + close_m
            except (ValueError, AssertionError):
                await self._send_error(interaction, "Opening and closing times cannot be the same")
                return
            try:
                new_days = [int(d.strip()) for d in days_raw.split(",") if d.strip().isdigit()]
                assert all(0 <= d <= 6 for d in new_days) and len(new_days) > 0
            except (ValueError, AssertionError):
                await self._send_error(interaction, "Invalid days — use numbers 0 (Mon) to 6 (Sun) separated by commas")
                return
            data.active_days, data.opening_time, data.closing_time = new_days, opening_raw, closing_raw
        else:
            data.active_days  = []
            data.opening_time = opening_raw or "19:00"
            data.closing_time   = closing_raw   or "22:00"
        if hours_raw:
            try:
                val = float(hours_raw)
                if val < 0:
                    raise ValueError
            except ValueError:
                await self._send_error(interaction, "Invalid duration — enter a positive number (e.g. 2.5) or 0 for no limit")
                return
        else:
            val = 0.0
        data.max_manual_hours = val
        if self.summary:
            self.summary.data = data
            guild = self.plugin._get_guild()
            embed = _wizard_summary_embed(data, guild)
            try:
                await self.summary.message.edit(embed=embed, view=self.summary)
            except Exception:
                pass
            await interaction.response.send_message(
                embed=discord.Embed(
                    title="✅ Schedule updated",
                    description="Review the summary and press the **green button** to confirm.",
                    color=0x2ECC71
                ), ephemeral=True, delete_after=10)
            return
        guild = self.plugin._get_guild()
        vista = WizardStep5Summary(data, guild, self.plugin)
        embed = _wizard_summary_embed(data, guild)
        await interaction.response.send_message(embed=embed, view=vista, ephemeral=True)
        vista.message = await interaction.original_response()


class WizardEditStep4Schedule(discord.ui.Modal):

    def __init__(self, data: WizardData, plugin: Jano, error: str = None):
        super().__init__(title="📅 Schedule & Limit")
        self.data   = data
        self.plugin = plugin
        self._f_dias = discord.ui.TextInput(
            label="Active days (empty = keep current)",
            placeholder="E.g.: 0,1,2,3,4  (0=Mon, 6=Sun)",
            required=False, max_length=20,
            default=",".join(str(d) for d in data.active_days) if data.active_days else ""
        )
        self._f_apertura = discord.ui.TextInput(
            label="Opening time HH:MM (empty = keep current)",
            placeholder="E.g.: 18:15",
            required=False, max_length=5,
            default=data.opening_time if data.active_days else ""
        )
        self._f_cierre = discord.ui.TextInput(
            label="Closing time HH:MM (empty = keep current)",
            placeholder="E.g.: 21:30",
            required=False, max_length=5,
            default=data.closing_time if data.active_days else ""
        )
        self._f_horas = discord.ui.TextInput(
            label="Max manual hours (empty = keep current)",
            placeholder="E.g.: 2.5  |  0 = no limit",
            required=False, max_length=5,
            default=str(data.max_manual_hours) if data.max_manual_hours > 0 else "0"
        )
        for f in [self._f_dias, self._f_apertura, self._f_cierre, self._f_horas]:
            self.add_item(f)

    async def _send_error(self, interaction, error: str):
        vista = ViewRetry(
            modal_class=WizardEditStep4Schedule,
            modal_kwargs={"data": self.data, "plugin": self.plugin},
            error=error
        )
        embed = discord.Embed(
            title="⚠️ Invalid input — Schedule & Limit",
            description=f"**{error}**\n\nPress the button below to go back and correct it.",
            color=0xE74C3C
        )
        await interaction.response.send_message(embed=embed, view=vista, ephemeral=True)
        try:
            vista.message = await interaction.original_response()
        except Exception:
            pass


    async def on_submit(self, interaction: discord.Interaction):
        data         = self.data
        days_raw     = self._f_dias.value.strip()
        opening_raw = self._f_apertura.value.strip()
        closing_raw   = self._f_cierre.value.strip()
        hours_raw    = self._f_horas.value.strip().replace(",", ".")
        pattern = re.compile(r"^\d{1,2}:\d{2}$")
        if days_raw:
            open_t = opening_raw or data.opening_time
            close_t = closing_raw   or data.closing_time
            if not pattern.match(open_t) or not pattern.match(close_t):
                await self._send_error(interaction, "Invalid time format — use HH:MM (e.g. 18:15)")
                return
            try:
                open_h, open_m = map(int, open_t.split(":"))
                close_h, close_m = map(int, close_t.split(":"))
                assert 0 <= open_h <= 23 and 0 <= open_m <= 59 and 0 <= close_h <= 23 and 0 <= close_m <= 59
                # Allow overnight: only reject if opening == closing
                assert open_h * 60 + open_m != close_h * 60 + close_m
                new_days = [int(d.strip()) for d in days_raw.split(",") if d.strip().isdigit()]
                assert all(0 <= d <= 6 for d in new_days) and len(new_days) > 0
            except (ValueError, AssertionError):
                await self._send_error(interaction, "Invalid times or days — check format and try again")
                return
            data.active_days, data.opening_time, data.closing_time = new_days, open_t, close_t
        elif opening_raw or closing_raw:
            data.opening_time = opening_raw or data.opening_time
            data.closing_time   = closing_raw   or data.closing_time
        if hours_raw:
            try:
                val = float(hours_raw)
                if val < 0:
                    raise ValueError
                data.max_manual_hours = val
            except ValueError:
                await self._send_error(interaction, "Invalid duration — enter a positive number (e.g. 2.5) or 0 for no limit")
                return
        guild = self.plugin._get_guild()
        vista = WizardEditStep5Summary(data, guild, self.plugin)
        embed = _wizard_summary_embed(data, guild, edit_mode=True)
        await interaction.response.send_message(embed=embed, view=vista, ephemeral=True)
        vista.message = await interaction.original_response()


class WizardStep5Summary(BotView):
    def __init__(self, data: WizardData, guild: discord.Guild, plugin: Jano, edit_mode=False):
        super().__init__(timeout=120)
        self.data         = data
        self.guild        = guild
        self.plugin       = plugin
        self.edit_mode = edit_mode

        btn_name = discord.ui.Button(label="✏️ Name / Icon", style=discord.ButtonStyle.secondary, row=0)
        btn_ch   = discord.ui.Button(label="✏️ Channels",            style=discord.ButtonStyle.secondary, row=0)
        btn_rol  = discord.ui.Button(label="✏️ Roles",               style=discord.ButtonStyle.secondary, row=0)
        btn_sch  = discord.ui.Button(label="✏️ Schedule & Limit",    style=discord.ButtonStyle.secondary, row=0)
        btn_name.callback = self._edit_name
        btn_ch.callback   = self._edit_channels
        btn_rol.callback  = self._edit_roles
        btn_sch.callback  = self._edit_schedule
        for b in [btn_name, btn_ch, btn_rol, btn_sch]:
            self.add_item(b)

        lbl = "💾 Save changes" if edit_mode else "✅ Create instance"
        btn_ok = discord.ui.Button(label=lbl, style=discord.ButtonStyle.success, row=1)
        btn_ok.callback = self._confirmar
        self.add_item(btn_ok)

        btn_cancel = discord.ui.Button(label="Cancel", style=discord.ButtonStyle.secondary, emoji="❌", row=1)
        btn_cancel.callback = self._cancelar
        self.add_item(btn_cancel)

    async def _update_summary(self, interaction: discord.Interaction):
        if self.edit_mode == 'setup':
            self.edit_mode = True
        embed = _wizard_summary_embed(self.data, self.guild, edit_mode=self.edit_mode)
        await interaction.response.edit_message(embed=embed, view=self)

    async def _edit_name(self, interaction: discord.Interaction):
        await interaction.response.send_modal(WizardEditName(self))

    async def _edit_channels(self, interaction: discord.Interaction):
        vista = WizardStep2Channels(self.data, self.guild, self.plugin, summary=self)
        embed = _wizard_embed(2, self.data.name, "Update the channels.")
        cat = self.guild.get_channel(self.data.category_id)
        if cat:
            embed.add_field(name="✅ Current Category / Channel", value=f"**{cat.name}**", inline=False)
        await interaction.response.send_message(embed=embed, view=vista, ephemeral=True)
        vista.message = await interaction.original_response()

    async def _edit_roles(self, interaction: discord.Interaction):
        vista = WizardStep3Roles(self.data, self.guild, self.plugin, summary=self)
        embed = _wizard_embed(3, self.data.name, "Update the roles.")
        await interaction.response.send_message(embed=embed, view=vista, ephemeral=True)
        vista.message = await interaction.original_response()

    async def _edit_schedule(self, interaction: discord.Interaction):
        await interaction.response.send_modal(WizardStep4Schedule(self.data, self.plugin, summary=self))

    async def _confirmar(self, interaction: discord.Interaction):
        self.stop()
        d = self.data
        if self.edit_mode:
            st  = d.st
            cfg = st.cfg
            old_name = cfg.name
            cfg.name, cfg.category_id, cfg.role_id  = d.name, d.category_id, d.role_id
            cfg.text_channel_id, cfg.voice_channel_id  = d.text_channel_id, d.voice_channel_id
            cfg.mention_role_id, cfg.active_days      = d.mention_role_id, d.active_days
            cfg.opening_time, cfg.closing_time         = d.opening_time, d.closing_time
            cfg.max_manual_hours                       = d.max_manual_hours
            cfg.status_icon                            = d.status_icon
            if d.name != old_name and old_name in self.plugin.states:
                self.plugin.states[d.name] = self.plugin.states.pop(old_name)
                # Rename in DB
                asyncio.ensure_future(self._rename_in_db(old_name, d.name))
            st.save()
            # Apply category rename immediately so the change is visible at once
            asyncio.ensure_future(self.plugin._evaluate_instance(st))
            if self.message:
                try:
                    await self.message.edit(embed=discord.Embed(title=f"✅ Instance '{d.name}' updated!", color=0x2ECC71), view=None)
                    asyncio.ensure_future(_delete_after(self.message))
                except Exception:
                    pass
            await interaction.response.send_message(embed=discord.Embed(
                title=f"✅ Changes saved — {d.name}", description="All changes applied immediately.", color=0x2ECC71
            ), ephemeral=True, delete_after=120)
            log.info(f"[Jano] Instance '{d.name}' edited by {interaction.user}")
        else:
            primera = len(self.plugin.states) == 0
            cfg = InstanceConfig(
                name=d.name, server_id=self.plugin._server_id,
                category_id=d.category_id, role_id=d.role_id,
                text_channel_id=d.text_channel_id, voice_channel_id=d.voice_channel_id,
                mention_role_id=d.mention_role_id, active_days=d.active_days,
                opening_time=d.opening_time, closing_time=d.closing_time,
                max_manual_hours=d.max_manual_hours,
                command_role_ids_global=self.plugin.command_role_ids_global,
                command_role_ids_instance=None,
                status_icon=d.status_icon,
            )
            await self.plugin._create_instance(cfg)
            if self.message:
                try:
                    await self.message.edit(embed=discord.Embed(
                        title=f"✅ Instance '{d.name}' created!",
                        description="You can now use all bot commands with this instance.", color=0x2ECC71
                    ), view=None)
                    asyncio.ensure_future(_delete_after(self.message))
                except Exception:
                    pass
            await interaction.response.send_message(embed=discord.Embed(
                title=f"✅ Instance created — {d.name}",
                description=f"The instance **{d.name}** is ready.\n\nUse `/jano setup` to modify its configuration.", color=0x2ECC71
            ), ephemeral=True, delete_after=120)
            log.info(f"[Jano] Instance '{d.name}' created by {interaction.user}")

    async def _rename_in_db(self, old: str, new: str):
        try:
            async with self.plugin.apool.connection() as conn:
                await conn.execute("UPDATE jano_instances SET name = %s WHERE name = %s", (new, old))
        except Exception as e:
            log.error(f"[Jano] Error renaming instance in DB: {e}")

    async def _cancelar(self, interaction: discord.Interaction):
        self.stop()
        if self.message:
            try:
                await self.message.edit(embed=discord.Embed(description="❌ Cancelled. No changes were made.", color=0x95A5A6), view=None)
                asyncio.ensure_future(_delete_after(self.message, delay=10))
            except Exception:
                pass
        await interaction.response.defer()


class WizardEditName(discord.ui.Modal, title="Name & Status Icon"):

    def __init__(self, summary: WizardStep5Summary):
        super().__init__()
        self.summary = summary
        # Store field references as instance attributes for access in on_submit
        self._f_nombre = discord.ui.TextInput(
            label="Instance name (empty = keep current)",
            placeholder="Leave empty to keep current name",
            required=False,
            max_length=32,
            default=summary.data.name,
        )
        self._f_status = discord.ui.TextInput(
            label="Status Icon — show 🟢🔴 on category? (yes/no)",
            placeholder="yes = show 🟢🔴  |  no = keep original name  |  empty = keep current",
            required=False,
            max_length=5,
            default="yes" if summary.data.status_icon else "no",
        )
        self.add_item(self._f_nombre)
        self.add_item(self._f_status)

    async def on_submit(self, interaction: discord.Interaction):
        new_name = self._f_nombre.value.strip()
        # Empty name = keep current
        if not new_name:
            new_name = self.summary.data.name
        if new_name != self.summary.data.name and new_name in self.summary.plugin.states:
            await interaction.response.send_message(f"❌ An instance named **{new_name}** already exists.", ephemeral=True, delete_after=120)
            return
        self.summary.data.name = new_name
        # Empty status_icon = keep current value
        raw_si = self._f_status.value.strip()
        if raw_si:
            self.summary.data.status_icon = _parse_status_icon(raw_si, default=self.summary.data.status_icon)
        await self.summary._update_summary(interaction)
        # Remind user to save — send a followup that auto-deletes
        try:
            msg = await interaction.followup.send(
                embed=discord.Embed(
                    description="✏️ Name / Icon updated in the summary above.\n\n⚠️ Review the summary and press the **green button** to confirm.",
                    color=0x3498DB
                ),
                ephemeral=True, wait=True
            )
            asyncio.ensure_future(_delete_after(msg, delay=10))
        except Exception:
            pass


class WizardEditStep5Summary(BotView):
    def __init__(self, data: WizardData, guild: discord.Guild, plugin: Jano):
        super().__init__(timeout=120)
        self.data   = data
        self.guild  = guild
        self.plugin = plugin

        btn_ok = discord.ui.Button(label="Save changes", style=discord.ButtonStyle.success, emoji="💾", row=0)
        btn_ok.callback = self._confirmar
        self.add_item(btn_ok)

        btn_cancel = discord.ui.Button(label="Cancel", style=discord.ButtonStyle.secondary, emoji="❌", row=0)
        btn_cancel.callback = self._cancelar
        self.add_item(btn_cancel)

    async def _confirmar(self, interaction: discord.Interaction):
        self.stop()
        d, st, cfg = self.data, self.data.st, self.data.st.cfg
        old_name = cfg.name
        cfg.name, cfg.category_id, cfg.role_id  = d.name, d.category_id, d.role_id
        cfg.text_channel_id, cfg.voice_channel_id  = d.text_channel_id, d.voice_channel_id
        cfg.mention_role_id, cfg.active_days      = d.mention_role_id, d.active_days
        cfg.opening_time, cfg.closing_time         = d.opening_time, d.closing_time
        cfg.max_manual_hours                       = d.max_manual_hours
        if d.name != old_name and old_name in self.plugin.states:
            self.plugin.states[d.name] = self.plugin.states.pop(old_name)
            asyncio.ensure_future(self._rename_in_db(old_name, d.name))
        st.save()
        if self.message:
            try:
                await self.message.edit(embed=discord.Embed(title=f"✅ Instance '{d.name}' updated!", color=0x2ECC71), view=None)
            except Exception:
                pass
        await interaction.response.send_message(embed=discord.Embed(
            title=f"✅ Changes saved — {d.name}", description="All changes applied. The bot uses the new config immediately.", color=0x2ECC71
        ), ephemeral=True, delete_after=120)
        log.info(f"[Jano] ✏️ Instance '{d.name}' edited by {interaction.user}")

    async def _rename_in_db(self, old: str, new: str):
        try:
            async with self.plugin.apool.connection() as conn:
                await conn.execute("UPDATE jano_instances SET name = %s WHERE name = %s", (new, old))
        except Exception as e:
            log.error(f"[Jano] Error renaming in DB: {e}")

    async def _cancelar(self, interaction: discord.Interaction):
        self.stop()
        if self.message:
            await self._close_message(discord.Embed(description="❌ Edit cancelled. No changes made.", color=0x95A5A6))
        asyncio.ensure_future(_reply_ephemeral(interaction, content="Edit cancelled."))


# ══════════════════════════════════════════════════════════════════════════════
# VIEWS — Command roles per instance
# ══════════════════════════════════════════════════════════════════════════════

class ViewAccessRoles(BotView):
    def __init__(self, guild: discord.Guild, plugin: Jano):
        super().__init__(timeout=120)
        self.guild       = guild
        self.plugin      = plugin
        self.selections = {n: None for n in plugin._get_names()}

        everyone = discord.SelectOption(label="🌐 @everyone (all)", value="__everyone__", description="All users")
        none_val  = discord.SelectOption(label="❌ None",             value="__none__", description="Clears roles")
        roles = sorted([r for r in guild.roles if r.name != "@everyone"], key=lambda r: -r.position)
        options = [everyone, none_val] + [discord.SelectOption(label=r.name[:100], value=str(r.id)) for r in roles[:23]]

        for idx, name in enumerate(plugin._get_names()):
            select = discord.ui.Select(
                placeholder=f"Roles for: {name}",
                min_values=0, max_values=min(10, len(options)),
                row=idx, custom_id=f"role_select_{name}", options=options
            )
            select.callback = self._make_callback(name)
            self.add_item(select)

        btn = discord.ui.Button(label="Apply selection", style=discord.ButtonStyle.primary, emoji="↩️",
                                row=len(plugin._get_names()), custom_id="apply_roles")
        btn.callback = self._apply_callback
        self.add_item(btn)

    def _make_callback(self, name: str):
        async def _callback(interaction: discord.Interaction):
            for item in self.children:
                if isinstance(item, discord.ui.Select) and item.custom_id == f"role_select_{name}":
                    vals = item.values
                    if not vals:
                        self.selections[name] = None
                    elif "__everyone__" in vals:
                        self.selections[name] = ["__everyone__"]
                    elif "__none__" in vals:
                        self.selections[name] = []
                    else:
                        self.selections[name] = [int(v) for v in vals]
                    break
            await interaction.response.defer()
        return _callback

    async def _apply_callback(self, interaction: discord.Interaction):
        lines = []
        for name, selection in self.selections.items():
            st_inst = self.plugin.states[name]
            if selection is None:
                ri = st_inst.cfg.command_role_ids_instance or []
                if ri:
                    names = ", ".join(self.guild.get_role(r).name for r in ri if self.guild.get_role(r))
                else:
                    names = "❌ None / 🌐 @everyone"
                lines.append(f"**{name}** → *(no change)* {names}")
            elif selection == ["__everyone__"]:
                lines.append(f"**{name}** → 🌐 @everyone")
            elif selection == []:
                lines.append(f"**{name}** → ❌ None")
            else:
                names = ", ".join(self.guild.get_role(r).name for r in selection if self.guild.get_role(r))
                lines.append(f"**{name}** → {names}")

        embed = discord.Embed(
            title="🔑 Review — Instance command roles",
            description="Review your selection and press **Save instance roles** to apply.",
            color=0x9B59B6
        )
        embed.add_field(name="Pending changes", value="\n".join(lines) or "No changes selected.", inline=False)
        view_save = ViewSaveAccessRoles(self)
        if self.message:
            try:
                await self.message.edit(embed=discord.Embed(
                    title="🔑 Configure command roles per instance",
                    description="✅ Selection applied — review below and confirm.", color=0x9B59B6
                ), view=None)
            except Exception:
                pass
        await interaction.response.send_message(embed=embed, view=view_save, ephemeral=True)
        view_save.message = await interaction.original_response()

    async def confirm_callback(self, interaction: discord.Interaction):
        changes = []
        for name, selection in self.selections.items():
            if selection is None:
                continue
            st_inst = self.plugin.states[name]
            if selection == ["__everyone__"]:
                st_inst.cfg.command_role_ids_instance = []
                st_inst.save()
                changes.append(f"**{name}** → 🌐 @everyone")
            elif selection == []:
                st_inst.cfg.command_role_ids_instance = None
                st_inst.save()
                changes.append(f"**{name}** → ❌ None")
            else:
                valid_roles = [r for r in selection if self.guild.get_role(r)]
                if valid_roles:
                    st_inst.cfg.command_role_ids_instance = valid_roles
                    st_inst.save()
                    role_names = ", ".join(self.guild.get_role(r).name for r in valid_roles if self.guild.get_role(r))
                    changes.append(f"**{name}** → {role_names}")
        self.stop()
        embed = discord.Embed(title="🔑 Command roles updated", color=0x2ECC71)
        if changes:
            embed.add_field(name="Changes applied", value="\n".join(changes), inline=False)
        else:
            embed.description = "No changes were made."
        try:
            await _followup_send(interaction, embed)
        except Exception:
            asyncio.ensure_future(_reply_ephemeral(interaction, embed=embed))


class ViewSaveAccessRoles(BotView):
    def __init__(self, parent: ViewAccessRoles):
        super().__init__(timeout=120)
        self.parent = parent
        btn = discord.ui.Button(label="Save instance roles", style=discord.ButtonStyle.success, emoji="💾", row=0, custom_id="save_roles")
        btn.callback = self._save_callback
        self.add_item(btn)

    async def _save_callback(self, interaction: discord.Interaction):
        self.stop()
        try:
            await interaction.response.edit_message(embed=discord.Embed(
                title="🔑 Configure command roles per instance",
                description="✅ Saved — command roles updated successfully.", color=0x2ECC71
            ), view=None)
        except Exception:
            pass
        await self.parent.confirm_callback(interaction)


# ══════════════════════════════════════════════════════════════════════════════
# Required by DCSServerBot plugin system
# ══════════════════════════════════════════════════════════════════════════════

async def setup(bot: DCSServerBot):
    await bot.add_cog(Jano(bot))
