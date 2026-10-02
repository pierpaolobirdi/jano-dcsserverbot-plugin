"""
Jano Plugin for DCSServerBot
Manages Discord channel visibility on a configurable schedule or manually.
"""

from __future__ import annotations

import asyncio
import datetime
import logging
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Type

import discord
from zoneinfo import ZoneInfo
from discord import app_commands
from discord.ext import tasks

import psycopg
import psycopg.rows
from core import Group, Plugin, TEventListener
from services.bot import DCSServerBot

log = logging.getLogger(__name__)

# Default timezone — overridden per-plugin-instance via jano.yaml (timezone: "Europe/Madrid").
# Never read TZ directly; always use plugin.tz or pass it explicitly so the value
# stays scoped to the plugin instance and does not leak across hot-reloads.
_DEFAULT_TZ = "Europe/Madrid"

# Internal version of this file only — updated manually in commands.py, independent of version.py.
COMMANDS_VERSION = "4.0.1"

_MAX_INSTANCES = 4

_DAY_NAMES    = {0: "Mon", 1: "Tue", 2: "Wed", 3: "Thu", 4: "Fri", 5: "Sat", 6: "Sun"}
_TIME_PATTERN = re.compile(r"^\d{1,2}:\d{2}$")
_STATUS_ICONS = re.compile(r"[🟢🔴]\s*")

_FOOTER_SEPARATOR = "▬" * 36
# Leading zero-width space keeps an empty line above the separator (Discord trims plain leading newlines).
EMBED_FOOTER = f"\u200b\n{_FOOTER_SEPARATOR}\nJano v.{COMMANDS_VERSION}"


class JanoEmbed(discord.Embed):
    """discord.Embed that always carries the Jano footer (separator + version)."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.set_footer(text=EMBED_FOOTER)


# ══════════════════════════════════════════════════════════════════════════════
# DATA MODEL — InstanceConfig + InstanceState (persisted in PostgreSQL)
# ══════════════════════════════════════════════════════════════════════════════

@dataclass
class InstanceConfig:
    """Configuration for one Jano instance (category group)."""

    name:                      str
    server_id:                 int
    category_id:               int
    role_id:                   int | None            # None → @everyone
    text_channel_id:           int | None
    voice_channel_id:          int | None
    mention_role_id:           int | None
    active_days:               list
    opening_time:              str
    closing_time:              str
    max_manual_hours:          float
    command_role_ids_instance: list | None
    status_icon:               bool = True           # True = rename category with 🟢🔴
    tz:                        ZoneInfo = field(default_factory=lambda: ZoneInfo(_DEFAULT_TZ))

    def effective_role_id(self):
        """Return the role ID to use for permission overwrites.

        In Discord, every guild has a built-in @everyone role whose ID is
        identical to the guild (server) ID.  When role_id is None the user
        chose @everyone, so we return server_id — the correct ID for that role.
        """
        return self.role_id if self.role_id else self.server_id


class InstanceState:
    """Mutable runtime state for one Jano instance. Persisted in PostgreSQL."""

    def __init__(self, cfg: InstanceConfig, plugin: "Jano"):
        self.cfg                 = cfg
        self.plugin              = plugin        # back-reference for DB access
        self.current_state       = None
        self.category_name_cache = None
        self.last_message_id     = None
        self.manual_override     = None
        self.override_ts         = None
        self.manual_hours_active = cfg.max_manual_hours
        self._trimmed_duration   = None
        self._evaluating         = False

    def active_ceiling(self):
        return self.cfg.max_manual_hours

    def schedule_readable(self):
        cfg = self.cfg
        if not cfg.active_days:
            return {"days": "No schedule (manual)", "opening": "—", "closing": "—"}
        return {
            "days":    ", ".join(_DAY_NAMES[d] for d in cfg.active_days if d in _DAY_NAMES),
            "opening": cfg.opening_time,
            "closing": cfg.closing_time,
        }

    # ── Manual mode ────────────────────────────────────────────────────────

    def manual_expired(self):
        if self.override_ts and self.manual_hours_active > 0:
            elapsed_hours = (datetime.datetime.now(self.cfg.tz) - self.override_ts).total_seconds() / 3600
            return elapsed_hours >= self.manual_hours_active
        return False

    def activate_manual(self, is_open: bool, hours: float = None):
        self._trimmed_duration = None
        if self.manual_override is None:
            self.override_ts = datetime.datetime.now(self.cfg.tz)
        self.manual_override = is_open
        if is_open:
            ceiling = self.active_ceiling()
            if hours is None or hours == 0:
                self.manual_hours_active = ceiling
            else:
                if ceiling > 0 and hours > ceiling:
                    self.manual_hours_active = ceiling
                    self._trimmed_duration   = (hours, ceiling)
                else:
                    self.manual_hours_active = hours

    def deactivate_manual(self):
        self.manual_override = None
        self.override_ts     = None

    def manual_mode_info(self):
        if self.manual_override is None or not self.override_ts:
            return None
        if self.manual_hours_active <= 0:
            return {"no_limit": True}
        now       = datetime.datetime.now(self.cfg.tz)
        total     = datetime.timedelta(hours=self.manual_hours_active)
        elapsed   = now - self.override_ts
        remaining = total - elapsed
        if remaining.total_seconds() < 0:
            remaining = datetime.timedelta(seconds=0)
        return {"no_limit": False, "remaining": remaining, "expires_at": now + remaining}

    # ── Desired state ──────────────────────────────────────────────────────

    def compute_desired_state(self):
        if self.manual_override is not None and self.manual_expired():
            return None, "EXPIRED"
        if self.manual_override is not None:
            return self.manual_override, "MANUAL"
        cfg = self.cfg
        days, open_t, close_t = cfg.active_days, cfg.opening_time, cfg.closing_time
        if not days:
            return False, "NO_SCHEDULE"
        now          = datetime.datetime.now(self.cfg.tz)
        current_time = now.hour * 60 + now.minute
        open_h, open_m       = map(int, open_t.split(":"))
        close_h, close_m  = map(int, close_t.split(":"))
        opening_min = open_h * 60 + open_m
        closing_min = close_h * 60 + close_m
        today       = now.weekday()
        yesterday   = (today - 1) % 7
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

    async def save(self):
        """Persist instance config + state (two upserts in one transaction).

        Always awaited so callers get explicit confirmation (or an exception)
        instead of silently losing writes on pool errors or shutdown races.
        """
        await self._save(include_config=True)

    async def save_state(self):
        """Persist only the runtime state — what the scheduler changes on its own.

        Skipped when the instance has been deleted meanwhile, so a late write cannot
        fail on the foreign key or bring a deleted instance back.
        """
        if self.plugin.states.get(self.cfg.name) is not self:
            return
        await self._save(include_config=False)

    async def _save(self, include_config: bool):
        cfg = self.cfg
        try:
            async with self.plugin.apool.connection() as conn:
                async with conn.transaction():
                    if include_config:
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
                                active_days               = EXCLUDED.active_days,
                                opening_time              = EXCLUDED.opening_time,
                                closing_time              = EXCLUDED.closing_time,
                                max_manual_hours          = EXCLUDED.max_manual_hours,
                                command_role_ids_instance = EXCLUDED.command_role_ids_instance,
                                status_icon               = EXCLUDED.status_icon
                        """, (
                            cfg.name, cfg.category_id, cfg.role_id, cfg.text_channel_id,
                            cfg.voice_channel_id, cfg.mention_role_id, cfg.active_days or [],
                            cfg.opening_time, cfg.closing_time, cfg.max_manual_hours,
                            cfg.command_role_ids_instance, cfg.status_icon,
                        ))
                    await conn.execute("""
                        INSERT INTO jano_state
                            (name, current_state, category_name_cache, last_message_id,
                             manual_override, override_ts, manual_hours_active)
                        VALUES (%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT (name) DO UPDATE SET
                            current_state       = EXCLUDED.current_state,
                            category_name_cache = EXCLUDED.category_name_cache,
                            last_message_id     = EXCLUDED.last_message_id,
                            manual_override     = EXCLUDED.manual_override,
                            override_ts         = EXCLUDED.override_ts,
                            manual_hours_active = EXCLUDED.manual_hours_active
                    """, (
                        cfg.name, self.current_state, self.category_name_cache, self.last_message_id,
                        self.manual_override,
                        self.override_ts.isoformat() if self.override_ts else None,
                        self.manual_hours_active,
                    ))
        except Exception as e:
            log.error(f"[Jano/{cfg.name}] Error saving state: {e}")
            raise

    @classmethod
    def restore(cls, cfg: "InstanceConfig", state_row, plugin: "Jano") -> "InstanceState":
        """Reconstruct an InstanceState from a DB state row after startup.

        Using a classmethod keeps the constructor simple (all fields at their
        defaults) and makes it clear that this is an alternative construction
        path, not a mutation of an already-initialised object.

        Returns a fully initialised InstanceState.  If state_row is None the
        instance is returned in its default (all-None) state.
        """
        st = cls(cfg, plugin)
        if state_row is None:
            return st

        st.current_state       = state_row["current_state"]
        st.category_name_cache = state_row["category_name_cache"]
        st.last_message_id     = state_row["last_message_id"]
        hours_active           = state_row["manual_hours_active"]
        st.manual_hours_active = hours_active if hours_active is not None else cfg.max_manual_hours
        # max_hours_override / schedule_override are no longer used; warn if old data is being ignored.
        if state_row["max_hours_override"] is not None or state_row["schedule_override"]:
            log.warning(f"[Jano/{cfg.name}] Ignoring obsolete max_hours_override / schedule_override stored in DB")

        manual_override = state_row["manual_override"]
        override_ts     = state_row["override_ts"]
        if manual_override is not None and override_ts:
            if isinstance(override_ts, str):
                ts = datetime.datetime.fromisoformat(override_ts)
            else:
                ts = override_ts
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=cfg.tz)
            st.override_ts     = ts
            st.manual_override = manual_override
            if st.manual_expired():
                log.info(f"[Jano/{cfg.name}] ⏳ Manual mode expired offline → resetting")
                st.manual_override = None
                st.override_ts     = None
            else:
                log.info(f"[Jano/{cfg.name}] 🔄 Manual mode restored ({'OPEN' if manual_override else 'CLOSED'})")
        else:
            st.manual_override = None
            st.override_ts     = None

        return st


# ══════════════════════════════════════════════════════════════════════════════
# PLUGIN — modal used by /jano comms open, then the Jano cog
# ══════════════════════════════════════════════════════════════════════════════

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
            self._f_duration.label       = "Duration in hours (0 or empty = no limit)"
            self._f_duration.placeholder = "e.g. 2  or  2.5  ·  0 or empty = no limit"
        # Note: _f_duration is a class-level TextInput, discord.py adds it automatically
        # Do NOT call self.add_item() — that would duplicate it

    async def on_submit(self, interaction: discord.Interaction):
        raw                    = self._f_duration.value.strip().replace(",", ".")
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
        await self.plugin._comms_open(interaction, self.st, duration or None)



class Jano(Plugin):
    """DCSServerBot plugin — manages Discord comms channels by schedule or manually."""

    # ── Slash command groups ───────────────────────────────────────────────
    jano_group = Group(
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
        # Timezone for schedule calculations — set in cog_load from jano.yaml.
        # Stored here (not as a module global) so hot-reloads and multiple
        # plugin instances stay isolated.
        self.tz: ZoneInfo = ZoneInfo(_DEFAULT_TZ)
        # Open-channel announcement template — set in cog_load from jano.yaml
        self._open_message_template: dict = {}

    # ── Plugin lifecycle ───────────────────────────────────────────────────

    async def cog_load(self) -> None:
        await super().cog_load()
        self.log.debug("  => Plugin loading...")
        cfg = self.get_config() or {}

        # Timezone — stored on self, never on a module global
        tz_name = cfg.get("timezone", _DEFAULT_TZ)
        try:
            self.tz = ZoneInfo(tz_name)
        except Exception:
            self.log.warning(f"Invalid timezone '{tz_name}', using default '{_DEFAULT_TZ}'")
            self.tz = ZoneInfo(_DEFAULT_TZ)

        # Open-channel announcement template (configurable in jano.yaml)
        self._open_message_template = cfg.get("open_message", {}) or {}

        # Ensure tables exist before anything else
        await self._ensure_tables()
        # Store raw values from YAML (can be role names or numeric IDs).
        # Resolution to IDs happens in on_ready() once the guild is available.
        self._command_role_ids_raw = cfg.get("command_role_ids", []) or []

    async def cog_unload(self) -> None:
        if self.scheduler.is_running():
            self.scheduler.cancel()
        await super().cog_unload()

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
        self.log.debug(f"Ready - {len(self.states)} instance(s) loaded.")

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
        user_roles     = [r.id for r in interaction.user.roles] if hasattr(interaction.user, "roles") else []
        global_roles   = self.command_role_ids_global
        instance_roles = (st.cfg.command_role_ids_instance if st else None) or []
        if not global_roles and not instance_roles:
            return True
        if global_roles and any(r in user_roles for r in global_roles):
            return True
        if instance_roles and any(r in user_roles for r in instance_roles):
            return True
        return False

    async def _ensure_tables(self):
        """Create Jano tables if they do not exist yet (runs db/tables.sql, the single schema source).
        Called early in cog_load so tables are ready before on_ready.
        Safe to call multiple times — the script uses IF NOT EXISTS / ON CONFLICT DO NOTHING.
        """
        try:
            schema = (Path(__file__).parent / "db" / "tables.sql").read_text(encoding="utf-8")
            async with self.apool.connection() as conn:
                await conn.execute(schema)
        except Exception as e:
            self.log.error(f"Error creating tables: {e}")

    async def _migrate_db(self):
        """Apply any missing DB schema changes automatically on startup.
        Add new ALTER TABLE statements to `migrations` for every future schema change —
        IF NOT EXISTS ensures they are safe to run repeatedly."""
        migrations = [
            # v1.0 → v1.1: status_icon per instance
            "ALTER TABLE jano_instances ADD COLUMN IF NOT EXISTS status_icon BOOLEAN NOT NULL DEFAULT true",
            # ── Add future migrations below this line ──────────────────────────
        ]
        try:
            async with self.apool.connection() as conn:
                for sql in migrations:
                    await conn.execute(sql)
                await self._migrate_legacy_columns(conn)
        except Exception as e:
            self.log.error(f"Error applying DB migrations: {e}")

    # TODO (pending): remove _LEGACY_COLUMN_RENAMES and _migrate_legacy_columns() once every
    # installation has been migrated to the English column names (v1.2+). After that, nothing
    # else in the plugin depends on the old Spanish names.
    _LEGACY_COLUMN_RENAMES = (
        # (table, old column, new column)
        ("jano_instances", "dias_activos",               "active_days"),
        ("jano_instances", "hora_apertura",             "opening_time"),
        ("jano_instances", "hora_cierre",               "closing_time"),
        ("jano_instances", "max_horas_manual",          "max_manual_hours"),
        ("jano_instances", "command_role_ids_instancia", "command_role_ids_instance"),
        ("jano_state",     "ultimo_mensaje_id",         "last_message_id"),
        ("jano_state",     "nombre_categoria_cache",    "category_name_cache"),
        ("jano_state",     "override_manual",           "manual_override"),
        ("jano_state",     "override_timestamp",        "override_ts"),
        ("jano_state",     "horas_manual_activo",       "manual_hours_active"),
        ("jano_state",     "max_horas_override",        "max_hours_override"),
        ("jano_state",     "horario_override",          "schedule_override"),
        ("jano_state",     "estado_actual",             "current_state"),
    )

    async def _migrate_legacy_columns(self, conn):
        """Rename old Spanish columns to their English names (v1.1 → v1.2).

        One catalog query finds which legacy columns still exist; only those are renamed,
        so on an already-migrated database this costs a single SELECT and no ALTERs.
        """
        tables = sorted({t for t, _, _ in self._LEGACY_COLUMN_RENAMES})
        cur    = await conn.execute(
            "SELECT table_name, column_name FROM information_schema.columns "
            "WHERE table_schema = current_schema() AND table_name = ANY(%s)",
            (tables,),
        )
        existing = {(t, c) for t, c in await cur.fetchall()}
        for table, old, new in self._LEGACY_COLUMN_RENAMES:
            if (table, old) in existing and (table, new) not in existing:
                await conn.execute(f"ALTER TABLE {table} RENAME COLUMN {old} TO {new}")
                self.log.info(f"DB migration: {table}.{old} → {new}")

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

                    # Instances + their runtime state in a single query
                    await cur.execute("""
                        SELECT i.*,
                               s.current_state, s.category_name_cache, s.last_message_id,
                               s.manual_override, s.override_ts, s.manual_hours_active,
                               s.max_hours_override, s.schedule_override
                        FROM jano_instances i
                        LEFT JOIN jano_state s ON s.name = i.name
                    """)
                    inst_rows = await cur.fetchall()

            for r in inst_rows:
                name = r["name"]
                if name in self.states:
                    continue
                cfg = InstanceConfig(
                    name                      = name,
                    server_id                 = self._server_id,
                    category_id               = r["category_id"],
                    role_id                   = r["role_id"],
                    text_channel_id           = r["text_channel_id"],
                    voice_channel_id          = r["voice_channel_id"],
                    mention_role_id           = r["mention_role_id"],
                    active_days               = list(r["active_days"] or []),
                    opening_time              = r["opening_time"],
                    closing_time              = r["closing_time"],
                    max_manual_hours          = r["max_manual_hours"],
                    command_role_ids_instance = list(r["command_role_ids_instance"] or []) or None,
                    status_icon               = r["status_icon"],
                    tz                        = self.tz,
                )
                st                = InstanceState.restore(cfg, r, self)
                self.states[name] = st
                self.log.debug(f"Instance '{name}' restored from DB")

            if not self.states:
                self.log.debug("No instances configured. Use /jano setup to create one.")

        except Exception as e:
            self.log.error(f"Error loading state from DB: {e}")

    async def _create_instance(self, cfg: InstanceConfig) -> InstanceState:
        """Add a new instance to memory and persist config to DB."""
        st                    = InstanceState(cfg, self)
        self.states[cfg.name] = st
        await st.save()
        self.log.info(f"Instance '{cfg.name}' created.")
        return st

    async def _delete_instance(self, name: str):
        """Remove instance from memory and DB.

        Deletes the open-channel announcement embed from Discord (if any) before
        removing the instance from the database.  Without this step the message
        would be orphaned in the text channel with no way to clean it up later.
        """
        st = self.states.get(name)
        if st and st.last_message_id:
            guild = self._get_guild()
            if guild:
                txt = _channel(guild, st.cfg.text_channel_id)
                if txt:
                    try:
                        msg = await txt.fetch_message(st.last_message_id)
                        await msg.delete()
                        self.log.info(f"[{name}] 🧹 Embed deleted on instance removal")
                    except Exception:
                        pass
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
            try:
                await self._evaluate_instance(st)
            except Exception:
                # One failing instance must not stop the scheduler loop (tasks.loop dies on exceptions).
                self.log.exception(f"[{st.cfg.name}] Error while evaluating instance")

    async def _evaluate_instance(self, st: InstanceState):
        if st._evaluating:
            return
        st._evaluating = True
        try:
            is_open, source = st.compute_desired_state()
            if source == "EXPIRED":
                self.log.info(f"[{st.cfg.name}] ⏳ Manual mode expired → returning to schedule")
                st.deactivate_manual()
                await st.save_state()
                is_open, source = st.compute_desired_state()

            guild = self._get_guild()
            if not guild:
                return

            role     = guild.get_role(st.cfg.effective_role_id())
            category = guild.get_channel(st.cfg.category_id)
            text_ch  = _channel(guild, st.cfg.text_channel_id)

            if not role or not category:
                self.log.warning(f"[{st.cfg.name}] Role or category not found, skipping.")
                return

            await _update_category_name(category, is_open, st)

            current_perm = category.permissions_for(role).view_channel
            needs_change = current_perm != is_open

            if not needs_change:
                if st.current_state != is_open:
                    st.current_state = is_open
                    await st.save_state()
                return

            self.log.info(f"[{st.cfg.name}] 🔄 Applying ({source}) → {'OPEN' if is_open else 'CLOSED'}")

            for attempt in range(3):
                try:
                    overwrite              = category.overwrites_for(role)
                    overwrite.view_channel = is_open
                    await category.set_permissions(role, overwrite=overwrite)
                    break
                except discord.HTTPException as e:
                    if e.status == 429:
                        # Do not mark the state as applied — the next scheduler tick retries.
                        self.log.warning(f"[{st.cfg.name}] Rate limit on permissions, will retry next cycle")
                        return
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
                        voice_id  = st.cfg.voice_channel_id
                        inst_name = st.cfg.name
                        tpl       = self._open_message_template
                        # Build announcement embed from jano.yaml template or defaults.
                        # Supported keys under open_message:
                        #   title: "Custom title — {name}"
                        #   body:  "Custom body — connect to {voice}"
                        # {name} and {voice} are replaced with the instance name and
                        # voice-channel mention respectively.
                        voice_mention = f"<#{voice_id}>" if voice_id else "Voice Channel"
                        def _fmt(text: str) -> str:
                            return text.replace("{name}", inst_name).replace("{voice}", voice_mention)

                        title        = _fmt(tpl.get("title", "🟢   __**{name} ACCESS CHANNELS ARE OPEN**__"))
                        body_default = (
                            f"The **{inst_name}** channels will remain open until the end of the event."
                            f"\n\nPlease connect to the channel:\n\n{voice_mention}\n\n"
                            f"Before the start of **{inst_name}**."
                        )
                        desc       = _fmt(tpl.get("body", body_default))
                        embed      = JanoEmbed(title=title, description=desc, color=0x2ECC71)
                        mention_id = st.cfg.mention_role_id
                        content    = f"<@&{mention_id}>" if mention_id else None
                        msg        = await text_ch.send(
                            content=content,
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
            await st.save_state()
        finally:
            st._evaluating = False

    async def _release_category(self, category_id: int, instance_name: str):
        """Clean up a category Jano no longer manages: remove the 🟢/🔴 markers from its name.

        Permissions are intentionally left untouched.  Skipped if another instance still
        manages the same category.
        """
        if any(other.cfg.category_id == category_id for other in self.states.values()):
            return
        guild = self._get_guild()
        category = guild.get_channel(category_id) if guild else None
        if not category:
            return
        try:
            clean_name = _strip_status_icons(category.name)
            if clean_name != category.name:
                await category.edit(name=clean_name)
            self.log.info(f"[{instance_name}] 🧹 Previous category '{clean_name}' released")
        except discord.HTTPException as e:
            self.log.warning(f"[{instance_name}] Could not clean previous category: {e}")

    async def _clean_orphan_embeds(self):
        guild = self._get_guild()
        if not guild:
            return
        for st in self.states.values():
            if st.last_message_id:
                is_open, _ = st.compute_desired_state()
                if not is_open:
                    txt = _channel(guild, st.cfg.text_channel_id)
                    if txt:
                        try:
                            msg = await txt.fetch_message(st.last_message_id)
                            await msg.delete()
                            self.log.info(f"[{st.cfg.name}] 🧹 Orphan embed removed")
                        except Exception:
                            pass
                        st.last_message_id = None
                        await st.save_state()

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

    async def _prepare(self, interaction: discord.Interaction, instance: str | None) -> InstanceState | None:
        """Common command prelude: resolve the target instance and check permissions.

        Replies to the user and returns None when the command cannot proceed.
        """
        if not self.states or instance == "__none__":
            _ephemeral(interaction, embed=JanoEmbed(
                title="⚠️ No instances configured",
                description="No instances have been set up yet.\nUse **/jano setup** to create your first instance.",
                color=0xE67E22
            ))
            return None
        st = self._resolve_instance(instance)
        if not st:
            await interaction.response.send_message(
                "❌ Specify an instance. Options: " + ", ".join(self._get_names()),
                ephemeral=True, delete_after=120
            )
            return None
        if not self._is_authorized(interaction, st):
            _ephemeral(interaction, embed=_no_permission())
            return None
        return st

    # ══════════════════════════════════════════════════════════════════════
    # SLASH COMMANDS
    # ══════════════════════════════════════════════════════════════════════

    @jano_group.command(name="status", description="Show current status of an instance")
    @app_commands.describe(instance="Instance to check (optional if only one)")
    async def jano_status(self, interaction: discord.Interaction, instance: str = None):
        st = await self._prepare(interaction, instance)
        if not st:
            return

        await interaction.response.defer(ephemeral=True)

        state_txt = "🟢 Open" if st.current_state else "🔴 Closed"
        mode_txt  = "Manual" if st.manual_override is not None else "Schedule"

        embed = JanoEmbed(title=f"📊 Status — {st.cfg.name}", color=0x3498DB)
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
            cat = guild.get_channel(st.cfg.category_id)
            embed.add_field(name="📦 __Category__", value=f"**{cat.name}**" if cat else "❌ Not configured", inline=False)
            for label, channel_id in (("💬 __Text channel__", st.cfg.text_channel_id), ("🔊 __Voice channel__", st.cfg.voice_channel_id)):
                embed.add_field(name=label, value=f"<#{channel_id}>" if _channel(guild, channel_id) else "❌ Not configured", inline=False)

            role_id = st.cfg.effective_role_id()
            if role_id == self._server_id:
                role_txt = "🌐 **@everyone**"
            elif role_id:
                role     = guild.get_role(role_id)
                role_txt = f"**{role.name}**" if role else "⚠️ Role not found"
            else:
                role_txt = "❌ Not configured"
            embed.add_field(name="👁️ __Visibility role__", value=role_txt, inline=False)

            mr_id  = st.cfg.mention_role_id
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

    # ── /jano comms ───────────────────────────────────────────────────────

    @jano_group.command(name="comms", description="Open, close or resume comms channels")
    @app_commands.describe(
        instance="Instance to act on (auto-selected if only one exists)",
        action="What to do: open channels, close them, or resume automatic schedule"
    )
    async def jano_comms(self, interaction: discord.Interaction, instance: str = None, action: str = None):
        st = await self._prepare(interaction, instance)
        if not st:
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

    async def _comms_open(self, interaction: discord.Interaction, st: InstanceState, duration_val: float | None = None):
        if st.current_state and st.manual_override is True:
            if duration_val is None:
                embed = JanoEmbed(
                    title=f"🟢 Already open — {st.cfg.name}",
                    description="The channels are already open in manual mode. No changes made.",
                    color=0x95A5A6
                )
                _ephemeral(interaction, embed=embed)
                return
            else:
                st.activate_manual(True, hours=duration_val)
                await st.save_state()
                embed = JanoEmbed(
                    title=f"⏱️ Duration updated — {st.cfg.name}",
                    description="Channels remain open. Duration updated.",
                    color=0x3498DB
                )
                _add_duration_fields(embed, st, st.manual_mode_info(), label="New duration")
                _ephemeral(interaction, embed=embed)
                return

        st.activate_manual(True, hours=duration_val)
        await st.save_state()
        info = st.manual_mode_info()

        embed = JanoEmbed(
            title=f"🟡 Opening access... — {st.cfg.name}",
            description="⏳ Applying changes. Category status may take a moment to update.",
            color=0xF1C40F
        )
        _add_duration_fields(embed, st, info)

        await interaction.response.send_message(embed=embed, ephemeral=True)

        async def apply_and_confirm():
            await self._evaluate_instance(st)
            embed_ok = JanoEmbed(
                title=f"🟢 Access opened — {st.cfg.name}",
                description="✅ Changes applied successfully.",
                color=0x2ECC71
            )
            _add_duration_fields(embed_ok, st, info)
            try:
                await interaction.edit_original_response(embed=embed_ok)
                msg = await interaction.original_response()
                _spawn(_delete_after(msg))
            except Exception:
                pass

        _spawn(apply_and_confirm())

    async def _comms_close(self, interaction: discord.Interaction, st: InstanceState):
        await interaction.response.defer(ephemeral=True)

        if not st.current_state and st.manual_override is None:
            embed = JanoEmbed(
                title=f"🔴 Already closed — {st.cfg.name}",
                description="Channels already closed and running on schedule. No changes made.",
                color=0x95A5A6
            )
            await _followup_send(interaction, embed)
            return

        st.activate_manual(False)
        await st.save_state()
        await self._evaluate_instance(st)

        info  = st.manual_mode_info()
        embed = JanoEmbed(
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
        view         = ViewCloseConfirm(st, info or {"no_limit": True}, self)
        msg          = await interaction.followup.send(embed=embed, view=view, ephemeral=True, wait=True)
        view.message = msg

    async def _comms_resume(self, interaction: discord.Interaction, st: InstanceState):
        await interaction.response.defer(ephemeral=True)
        embed = await self._execute_resume(st, "♻️ Schedule resumed")
        await _followup_send(interaction, embed)

    async def _execute_resume(self, st: InstanceState, title: str) -> discord.Embed:
        st.deactivate_manual()
        await st.save_state()
        h = st.schedule_readable()
        await self._evaluate_instance(st)
        is_open, _ = st.compute_desired_state()
        state_txt = "🟢 Open" if is_open else "🔴 Closed"
        embed     = JanoEmbed(
            title=f"{title} — {st.cfg.name}",
            description="Control returns to the configured automatic schedule.",
            color=0x3498DB
        )
        if not st.cfg.active_days:
            embed.add_field(name="⚠️ No schedule defined", value="The instance has no active days, so channels stay closed until opened manually.", inline=False)
        embed.add_field(name="Active schedule", value=f"{h['opening']} - {h['closing']}", inline=False)
        embed.add_field(name="Active days",     value=h["days"],                          inline=False)
        embed.add_field(name="Current status",  value=state_txt,                         inline=False)
        return embed

    # ── /jano_setup ───────────────────────────────────────────────────────

    @jano_group.command(name="setup", description="Configure bot resources (channels and roles)")
    @app_commands.describe(instance="Instance to configure (optional if only one)")
    async def jano_setup(self, interaction: discord.Interaction, instance: str = None):
        if not self._is_authorized(interaction):
            _ephemeral(interaction, embed=_no_permission())
            return

        guild = self._get_guild()

        if not self.states:
            view  = ViewSetupEmpty(guild, self)
            embed = JanoEmbed(
                title="⚙️ Setup — First time configuration",
                description="No instances configured yet.\n\nAn **instance** is a set of channels the bot will manage — opening and closing on a schedule or manually.\n\nPress **➕ New instance** to create your first one.",
                color=0xE67E22
            )
            await interaction.response.send_message(embed=embed, view=view, ephemeral=True)
            view.message = await interaction.original_response()
            return

        st = await self._prepare(interaction, instance)
        if not st:
            return

        view  = ViewSetup(st, guild, self)
        embed = JanoEmbed(
            title=f"⚙️ Setup — {st.cfg.name}",
            description="What would you like to configure?",
            color=0x3498DB
        )
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)
        view.message = await interaction.original_response()

    # One shared autocomplete for the "instance" argument of every command.
    jano_status.autocomplete("instance")(_autocomplete_instance)
    jano_comms.autocomplete("instance")(_autocomplete_instance)
    jano_setup.autocomplete("instance")(_autocomplete_instance)


# ══════════════════════════════════════════════════════════════════════════════
# SHARED UTILITIES
# ══════════════════════════════════════════════════════════════════════════════

def _strip_status_icons(name: str) -> str:
    """Category name without the 🟢/🔴 markers Jano adds."""
    return _STATUS_ICONS.sub("", name).strip()

def _channel(guild: discord.Guild, channel_id: int | None):
    """Guild channel for an optional ID (None when unset or missing)."""
    return guild.get_channel(channel_id) if channel_id else None

def _fmt_duration(td: datetime.timedelta) -> str:
    total_min = int(td.total_seconds() // 60)
    return f"{total_min // 60}:{total_min % 60:02d}"

def _add_duration_fields(embed: discord.Embed, st: InstanceState, info: dict | None, label: str = "Active duration"):
    """Append the manual-mode duration fields (trim warning, duration, remaining, expiry) to an embed."""
    if st._trimmed_duration:
        requested, applied = st._trimmed_duration
        embed.add_field(name="⚠️ Duration adjusted", value=f"Requested **{requested}h**, max is **{applied}h**.", inline=False)
    if info and not info.get("no_limit"):
        embed.add_field(name=label,        value=f"{st.manual_hours_active}h", inline=False)
        embed.add_field(name="Remaining",  value=_fmt_duration(info["remaining"]), inline=False)
        embed.add_field(name="Expires at", value=info["expires_at"].strftime('%H:%M'), inline=False)
    else:
        ceiling = st.active_ceiling()
        embed.add_field(name="Duration", value=f"Indefinite · max {ceiling}h" if ceiling > 0 else "No limit", inline=False)

def _no_permission(msg: str = "❌ You do not have permission to use this command.") -> discord.Embed:
    return JanoEmbed(description=msg, color=0xE67E22)

async def _delete_after(message: discord.Message, delay: int = 120):
    await asyncio.sleep(delay)
    try:
        await message.delete()
    except Exception:
        pass

async def _followup_send(interaction: discord.Interaction, embed: discord.Embed, delay: int = 120):
    msg = await interaction.followup.send(embed=embed, ephemeral=True)
    _spawn(_delete_after(msg, delay))

_background_tasks: set = set()

def _spawn(coro) -> asyncio.Task:
    """Run a coroutine in the background, keeping a reference so it is not garbage-collected."""
    task = asyncio.create_task(coro)
    _background_tasks.add(task)
    task.add_done_callback(_background_tasks.discard)
    return task

def _ephemeral(interaction: discord.Interaction, delay: int = 120, **kwargs) -> asyncio.Task:
    """Send an ephemeral reply in the background and delete it after `delay` seconds."""
    async def _run():
        await interaction.response.send_message(ephemeral=True, **kwargs)
        try:
            msg = await interaction.original_response()
        except Exception:
            return
        await _delete_after(msg, delay)
    return _spawn(_run())

def _channels_of(guild: discord.Guild, *types) -> list:
    """Guild channels of the given types, ordered by position."""
    return sorted([c for c in guild.channels if isinstance(c, types)], key=lambda c: c.position)

def _roles_of(guild: discord.Guild) -> list:
    """Guild roles (without @everyone), highest first."""
    return sorted([r for r in guild.roles if r.name != "@everyone"], key=lambda r: -r.position)

def _selected_values(view: discord.ui.View, custom_id: str) -> list:
    """Values currently chosen in the Select with the given custom_id."""
    for item in view.children:
        if isinstance(item, discord.ui.Select) and item.custom_id == custom_id:
            return list(item.values)
    return []

def _selected(view: discord.ui.View, custom_id: str):
    """First value chosen in the Select with the given custom_id, or None."""
    values = _selected_values(view, custom_id)
    return values[0] if values else None


async def _update_category_name(category, is_open: bool, st: InstanceState):
    """Keep the category name in sync: 🟢/🔴 markers when status_icon is on, none when it is off."""
    clean_name = _strip_status_icons(category.name)
    if st.cfg.status_icon:
        if clean_name != st.category_name_cache:
            st.category_name_cache = clean_name
            await st.save_state()
        emoji       = "🟢" if is_open else "🔴"
        target_name = f"{emoji} {clean_name} {emoji}"
    else:
        target_name = clean_name   # leave no leftover emojis on the name
    if category.name == target_name:
        return
    try:
        await category.edit(name=target_name)
        if not st.cfg.status_icon:
            log.info(f"[Jano/{st.cfg.name}] 🧹 Emojis removed from category name")
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

    async def _finish(self, embed: discord.Embed, delay: int = 120):
        """Replace the message with a final embed (no buttons) and delete it after `delay` seconds."""
        if not self.message:
            return
        try:
            await self.message.edit(embed=embed, view=None)
        except Exception:
            return
        _spawn(_delete_after(self.message, delay))

    async def on_timeout(self):
        await self._finish(
            JanoEmbed(description="⏱️ Interaction expired — use the command again if needed.", color=0x95A5A6),
            delay=30
        )


# ══════════════════════════════════════════════════════════════════════════════
# VIEWS — Close confirmation
# ══════════════════════════════════════════════════════════════════════════════

async def _resume_and_reply(view, interaction: discord.Interaction, title: str):
    """Shared handler of the 'Resume automatic schedule' buttons."""
    await interaction.response.defer(ephemeral=True)
    view.stop()
    embed = await view.plugin._execute_resume(view.st, title)
    await _followup_send(interaction, embed)


class ViewCloseConfirm(BotView):
    def __init__(self, st: InstanceState, info_manual, plugin: Jano):
        super().__init__()
        self.st          = st
        self.info_manual = info_manual
        self.plugin      = plugin

    @discord.ui.button(label="Keep manual mode", style=discord.ButtonStyle.secondary, emoji="⏳")
    async def keep_manual(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True)
        await self.plugin._evaluate_instance(self.st)
        if not self.info_manual.get("no_limit"):
            remaining = _fmt_duration(self.info_manual["remaining"])
            expires   = self.info_manual["expires_at"].strftime('%H:%M')
            desc      = f"Channels will return to automatic schedule in **{remaining}** (at **{expires}**)."
        else:
            desc = "Channels will remain closed in manual mode with no time limit."
        embed = JanoEmbed(
            title=f"🔴 Access closed — {self.st.cfg.name}",
            description=desc,
            color=0xE74C3C
        )
        self.stop()
        view_resume         = ViewResumeAuto(self.st, self.plugin)
        msg                 = await interaction.followup.send(embed=embed, view=view_resume, ephemeral=True, wait=True)
        view_resume.message = msg

    @discord.ui.button(label="Resume automatic schedule", style=discord.ButtonStyle.primary, emoji="♻️")
    async def resume_schedule(self, interaction: discord.Interaction, button: discord.ui.Button):
        await _resume_and_reply(self, interaction, "🔴 Access closed · ♻️ Schedule resumed")


class ViewResumeAuto(BotView):
    def __init__(self, st: InstanceState, plugin: Jano):
        super().__init__(timeout=180)
        self.st     = st
        self.plugin = plugin

    @discord.ui.button(label="Resume automatic schedule", style=discord.ButtonStyle.primary, emoji="♻️")
    async def btn_resume_auto(self, interaction: discord.Interaction, button: discord.ui.Button):
        await _resume_and_reply(self, interaction, "♻️ Schedule resumed")


# ══════════════════════════════════════════════════════════════════════════════
# VIEWS — Setup
# ══════════════════════════════════════════════════════════════════════════════

class ViewSetupEmpty(BotView):
    def __init__(self, guild: discord.Guild, plugin: Jano):
        super().__init__()
        self.guild  = guild
        self.plugin = plugin

    @discord.ui.button(label="New instance", style=discord.ButtonStyle.success, emoji="➕")
    async def btn_new(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(WizardStep1Name(self.plugin))


class ViewSetup(BotView):
    def __init__(self, st: InstanceState, guild: discord.Guild, plugin: Jano):
        super().__init__()
        self.st     = st
        self.guild  = guild
        self.plugin = plugin

    @discord.ui.button(label="Edit instance", style=discord.ButtonStyle.primary, emoji="✏️", row=0)
    async def btn_edit_instance(self, interaction: discord.Interaction, button: discord.ui.Button):
        data  = WizardData.from_state(self.st)
        view  = WizardStep5Summary(data, self.guild, self.plugin, edit_mode=True)
        embed = _wizard_summary_embed(data, self.guild, edit_mode='setup')
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)
        view.message = await interaction.original_response()

    @discord.ui.button(label="Command roles", style=discord.ButtonStyle.primary, emoji="🔑", row=0)
    async def configure_access_roles(self, interaction: discord.Interaction, button: discord.ui.Button):
        view_roles = ViewAccessRoles(self.guild, self.plugin)
        embed      = JanoEmbed(
            title="🔑 Configure command roles per instance",
            description="Select the roles for each instance.\n\n⚠️ **Your selection replaces current roles entirely.**\n\nIf you skip an instance, it will not be modified.",
            color=0x9B59B6
        )
        for name in self.plugin._get_names():
            st_inst = self.plugin.states[name]
            ri      = st_inst.cfg.command_role_ids_instance
            if ri:
                guild_roles = [self.guild.get_role(r) for r in ri]
                val         = ", ".join(r.name for r in guild_roles if r) or "❌ Not configured"
            else:
                val = "❌ Not configured"
            embed.add_field(name=f"Current — {name}", value=val, inline=False)
        await interaction.response.send_message(embed=embed, view=view_roles, ephemeral=True)
        view_roles.message = await interaction.original_response()

    @discord.ui.button(label="New instance", style=discord.ButtonStyle.success, emoji="➕", row=1)
    async def btn_new_instance(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not self.plugin._is_authorized(interaction):
            _ephemeral(interaction, embed=_no_permission("❌ Only admin roles can create instances."))
            return
        if len(self.plugin.states) >= _MAX_INSTANCES:
            await interaction.response.send_message(
                f"❌ Maximum of {_MAX_INSTANCES} instances reached. Delete one first.", ephemeral=True, delete_after=120
            )
            return
        await interaction.response.send_modal(WizardStep1Name(self.plugin))

    @discord.ui.button(label="Delete instance", style=discord.ButtonStyle.danger, emoji="🗑️", row=1)
    async def btn_delete_instance(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not self.plugin._is_authorized(interaction):
            _ephemeral(interaction, embed=_no_permission("❌ Only admin roles can delete instances."))
            return
        view  = ViewSelectDelete(self.guild, self.plugin)
        embed = JanoEmbed(
            title="🗑️ Delete instance",
            description="Select the instance to delete.\n\n⚠️ This action is **irreversible**.",
            color=0xE74C3C
        )
        for name, st in self.plugin.states.items():
            cat = self.guild.get_channel(st.cfg.category_id)
            embed.add_field(name=name, value=f"📦 {cat.name if cat else '❌ Not found'}", inline=True)
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)
        view.message = await interaction.original_response()


# ══════════════════════════════════════════════════════════════════════════════
# VIEWS — Delete instance
# ══════════════════════════════════════════════════════════════════════════════

class ViewSelectDelete(BotView):
    def __init__(self, guild: discord.Guild, plugin: Jano):
        super().__init__()
        self.guild          = guild
        self.plugin         = plugin
        self._selected_name = None

        options         = [discord.SelectOption(label=n, value=n, description=f"Delete '{n}'") for n in plugin._get_names()]
        select          = discord.ui.Select(placeholder="Select instance to delete...", min_values=1, max_values=1, options=options, row=0)
        select.callback = self._select_callback
        self.add_item(select)

        btn          = discord.ui.Button(label="Continue →", style=discord.ButtonStyle.danger, emoji="⚠️", row=1)
        btn.callback = self._next_callback
        self.add_item(btn)

    async def _select_callback(self, interaction: discord.Interaction):
        self._selected_name = next((i.values[0] for i in self.children if isinstance(i, discord.ui.Select) and i.values), None)
        await interaction.response.defer()

    async def _next_callback(self, interaction: discord.Interaction):
        if not self._selected_name:
            await interaction.response.send_message("❌ Please select an instance first.", ephemeral=True, delete_after=120)
            return
        is_last = len(self.plugin.states) == 1
        await interaction.response.send_modal(ModalConfirmDelete(self._selected_name, is_last, self.plugin))


class ModalConfirmDelete(discord.ui.Modal, title="⚠️ Confirm deletion"):
    confirmation = discord.ui.TextInput(label='Type DELETE to confirm', placeholder="DELETE", required=True, max_length=10)

    def __init__(self, name: str, is_last: bool, plugin: Jano):
        super().__init__()
        self.name    = name
        self.is_last = is_last
        self.plugin  = plugin
        title_text   = f"Delete '{name}' — type DELETE"
        if len(title_text) <= 45:
            self.title = title_text

    async def on_submit(self, interaction: discord.Interaction):
        if self.confirmation.value.strip() != "DELETE":
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
            embed = JanoEmbed(
                title=f"🗑️ Instance '{name}' deleted",
                description="⚠️ **This was the last instance.**\n\nUse **/jano setup** to create a new one.",
                color=0xE74C3C
            )
        else:
            embed = JanoEmbed(
                title=f"✅ Instance '{name}' deleted",
                description=f"Remaining: {', '.join(self.plugin.states.keys())}",
                color=0x2ECC71
            )
        _ephemeral(interaction, embed=embed)


# ══════════════════════════════════════════════════════════════════════════════
# WIZARD — New / Edit instance (5 steps)
# ══════════════════════════════════════════════════════════════════════════════

@dataclass
class WizardData:
    name:             str          = ""
    st:               object       = None      # InstanceState being edited (None when creating)
    category_id:      int | None   = None
    text_channel_id:  int | None   = None
    voice_channel_id: int | None   = None
    role_id:          int | None   = None
    mention_role_id:  int | None   = None
    active_days:      list         = field(default_factory=list)
    opening_time:     str          = "19:00"
    closing_time:     str          = "22:00"
    max_manual_hours: float        = 0.0
    status_icon:      bool         = True

    @classmethod
    def from_state(cls, st: InstanceState) -> "WizardData":
        cfg = st.cfg
        return cls(
            name=cfg.name, st=st, category_id=cfg.category_id,
            text_channel_id=cfg.text_channel_id, voice_channel_id=cfg.voice_channel_id,
            role_id=cfg.role_id, mention_role_id=cfg.mention_role_id,
            active_days=list(cfg.active_days), opening_time=cfg.opening_time,
            closing_time=cfg.closing_time, max_manual_hours=cfg.max_manual_hours,
            status_icon=cfg.status_icon,
        )


def _wizard_embed(step: int, name: str, description: str) -> discord.Embed:
    titles = {2: "📺 Channels", 3: "👥 Roles"}
    return JanoEmbed(title=f"⚙️ '{name}' — {titles.get(step, f'Step {step}')}", description=description, color=0x3498DB)

def _wizard_summary_embed(data: WizardData, guild: discord.Guild, edit_mode=False) -> discord.Embed:
    if edit_mode == 'setup':
        title, description = f"⚙️ Instance settings — '{data.name}'", "Current configuration. Use ✏️ buttons to modify, then **💾 Save changes**."
    elif edit_mode:
        title, description = f"📋 Review changes — '{data.name}'", "Review changes. Press **💾 Save changes** to confirm."
    else:
        title, description = f"📋 Review — '{data.name}'", "Review settings. Press **✅ Create instance** to confirm."
    embed = JanoEmbed(title=title, description=description, color=0x9B59B6)
    cat   = guild.get_channel(data.category_id)
    embed.add_field(name="📦 Category / Channel", value=f"**{cat.name}**" if cat else "❌ Not set", inline=False)
    txt = guild.get_channel(data.text_channel_id) if data.text_channel_id else None
    embed.add_field(name="💬 Text channel",  value=f"**#{txt.name}**" if txt else "*Not configured*", inline=True)
    voice = guild.get_channel(data.voice_channel_id) if data.voice_channel_id else None
    embed.add_field(name="🔊 Voice channel", value=f"**{voice.name}**" if voice else "*Not configured*", inline=True)
    embed.add_field(name="\u200b", value="\u200b", inline=False)
    if data.role_id:
        role     = guild.get_role(data.role_id)
        role_txt = f"**{role.name}**" if role else "⚠️ Not found"
    else:
        role_txt = "**🌐 @everyone**"
    embed.add_field(name="👁️ Visibility role", value=role_txt, inline=True)
    if data.mention_role_id:
        mr          = guild.get_role(data.mention_role_id)
        mention_txt = f"**{mr.name}**" if mr else "⚠️ Not found"
    else:
        mention_txt = "*Not configured*"
    embed.add_field(name="📣 Mention role", value=mention_txt, inline=True)
    embed.add_field(name="\u200b", value="\u200b", inline=False)
    if data.active_days:
        days_txt = ", ".join(_DAY_NAMES[d] for d in data.active_days if d in _DAY_NAMES)
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

async def _return_to_summary(view, interaction: discord.Interaction, title: str):
    """After editing one wizard step from the summary: refresh the summary and close this step."""
    try:
        await view.summary.message.edit(embed=_wizard_summary_embed(view.data, view.guild), view=view.summary)
    except Exception:
        pass
    embed_closed = JanoEmbed(title=title, description="Changes saved above.\n\nPress **💾 Save changes** to apply.", color=0x2ECC71)
    await interaction.response.edit_message(embed=embed_closed, view=None)
    msg = await interaction.original_response()
    _spawn(_delete_after(msg, delay=10))


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
        self.plugin  = plugin
        self._f_name = discord.ui.TextInput(
            label="Instance name (required)",
            placeholder="E.g.: Missions, Training, Events...",
            required=True, max_length=32
        )
        self._f_status = discord.ui.TextInput(
            label="Status Icon — show 🟢🔴 on category? (yes/no)",
            placeholder="yes = show 🟢🔴 on category  |  no = keep original name  |  default: no",
            required=False, max_length=5
        )
        self.add_item(self._f_name)
        self.add_item(self._f_status)

    async def on_submit(self, interaction: discord.Interaction):
        name = self._f_name.value.strip()
        if len(self.plugin.states) >= _MAX_INSTANCES:
            await interaction.response.send_message(
                f"❌ Maximum of {_MAX_INSTANCES} instances reached. Delete one first.", ephemeral=True, delete_after=120
            )
            return
        if name in self.plugin.states:
            await interaction.response.send_message(f"❌ An instance named **{name}** already exists.", ephemeral=True, delete_after=120)
            return
        data             = WizardData()
        data.name        = name
        data.status_icon = _parse_status_icon(self._f_status.value, default=False)
        guild            = self.plugin._get_guild()
        view             = WizardStep2Channels(data, guild, self.plugin)
        embed            = _wizard_embed(2, name, "Select the channels for this instance.\n\n**Category/Channel** is required.\nText and voice channels are optional.")
        embed.add_field(name="📦 Category / Channel", value="*Required — what the bot will open and close*", inline=False)
        embed.add_field(name="💬 Text channel",       value="*Channel for opening announcements (optional)*", inline=False)
        embed.add_field(name="🔊 Voice channel",      value="*Voice channel shown in the announcement (optional)*", inline=False)
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)
        view.message = await interaction.original_response()


# ── Step 2: Channels ──────────────────────────────────────────────────────────

class WizardStep2Channels(BotView):
    def __init__(self, data: WizardData, guild: discord.Guild, plugin: Jano, summary=None):
        super().__init__()
        self.data     = data
        self.guild    = guild
        self.plugin   = plugin
        self.type_sel = None
        self.summary  = summary

        sel_type = discord.ui.Select(placeholder="📦 Category / Channel — select type first", min_values=0, max_values=1, row=0, custom_id="w_sel_type",
            options=[
                discord.SelectOption(label="Discord Category",              value="category", emoji="📦"),
                discord.SelectOption(label="Direct channel (text or voice)", value="channel",    emoji="💬"),
            ])
        sel_type.callback = self._type_callback
        self.add_item(sel_type)

        text_channels = _channels_of(guild, discord.TextChannel)
        if text_channels:
            sel_text = discord.ui.Select(placeholder="💬 Text channel for announcements (optional)", min_values=0, max_values=1, row=1, custom_id="w_sel_text",
                options=[discord.SelectOption(label="❌ None", value="__none__")] +
                        [discord.SelectOption(label=f"#{c.name}"[:100], value=str(c.id)) for c in text_channels[:24]])
            sel_text.callback = self._make_cb("text")
            self.add_item(sel_text)

        voice_channels = _channels_of(guild, discord.VoiceChannel)
        if voice_channels:
            sel_voice = discord.ui.Select(placeholder="🔊 Voice channel for announcement (optional)", min_values=0, max_values=1, row=2, custom_id="w_sel_voice",
                options=[discord.SelectOption(label="❌ None", value="__none__")] +
                        [discord.SelectOption(label=c.name[:100], value=str(c.id)) for c in voice_channels[:24]])
            sel_voice.callback = self._make_cb("voice")
            self.add_item(sel_voice)

        btn          = discord.ui.Button(label="Apply", style=discord.ButtonStyle.primary, emoji="💾", row=3)
        btn.callback = self._next_callback
        self.add_item(btn)

    def _make_cb(self, key: str):
        async def _cb(interaction: discord.Interaction):
            val = _selected(self, f"w_sel_{key}")
            setattr(self.data, f"{key}_channel_id", None if (not val or val == "__none__") else int(val))
            await interaction.response.defer()
        return _cb

    async def _type_callback(self, interaction: discord.Interaction):
        self.type_sel = _selected(self, "w_sel_type")
        if not self.type_sel:
            return await interaction.response.defer()
        if self.type_sel == "category":
            channels_list = _channels_of(self.guild, discord.CategoryChannel)
        else:
            channels_list = _channels_of(self.guild, discord.TextChannel, discord.VoiceChannel)
        options = [discord.SelectOption(label=c.name[:100], value=str(c.id)) for c in channels_list[:25]]
        if not options:
            return await interaction.response.defer()
        label  = "📦 Select category" if self.type_sel == "category" else "💬 Select channel"
        picker = WizardCategoryPicker(self, options)
        await interaction.response.send_message(embed=JanoEmbed(title=label, color=0x3498DB), view=picker, ephemeral=True)
        picker.message = await interaction.original_response()

    async def _next_callback(self, interaction: discord.Interaction):
        if not self.data.category_id:
            await interaction.response.send_message("❌ You must select a Category/Channel before continuing.", ephemeral=True, delete_after=120)
            return
        self.stop()
        if self.summary:
            await _return_to_summary(self, interaction, "✅ Channels updated")
            return
        view  = WizardStep3Roles(self.data, self.guild, self.plugin)
        embed = _wizard_embed(3, self.data.name, "Select the roles.\n\nBoth optional. Empty Visibility role = @everyone.")
        embed.add_field(name="👁️ Visibility role", value="*Role that gains/loses access*\n*(empty = @everyone)*", inline=False)
        embed.add_field(name="📣 Mention role",    value="*Role pinged when channels open on schedule (optional)*", inline=False)
        embed_closed = JanoEmbed(description="✅ Channels saved — continuing to roles...", color=0x2ECC71)
        await interaction.response.edit_message(embed=embed_closed, view=None)
        msg_closed = await interaction.original_response()
        _spawn(_delete_after(msg_closed, delay=3))
        msg          = await interaction.followup.send(embed=embed, view=view, ephemeral=True, wait=True)
        view.message = msg


class WizardCategoryPicker(BotView):
    def __init__(self, parent: WizardStep2Channels, options: list):
        super().__init__()
        self.parent     = parent
        select          = discord.ui.Select(placeholder="Select an option", min_values=1, max_values=1, row=0, custom_id="w_cat_pick", options=options)
        select.callback = self._select_callback
        self.add_item(select)
        btn          = discord.ui.Button(label="Apply selection", style=discord.ButtonStyle.primary, emoji="↩️", row=1)
        btn.callback = self._confirm_callback
        self.add_item(btn)

    async def _select_callback(self, interaction: discord.Interaction):
        val                          = _selected(self, "w_cat_pick")
        self.parent.data.category_id = int(val) if val else None
        await interaction.response.defer()

    async def _confirm_callback(self, interaction: discord.Interaction):
        if not self.parent.data.category_id:
            return await interaction.response.defer()
        ch   = self.parent.guild.get_channel(self.parent.data.category_id)
        name = ch.name if ch else "—"
        for item in self.parent.children:
            if isinstance(item, discord.ui.Select) and item.custom_id == "w_sel_type":
                item.placeholder = f"✅ Selected: {name}"
                break
        self.stop()
        await interaction.response.defer()
        if self.message:
            _spawn(_delete_after(self.message, delay=0))
        embed_parent = _wizard_embed(2, self.parent.data.name, "Select the channels for this instance.")
        embed_parent.add_field(name="✅ Category / Channel", value=f"**{name}**", inline=False)
        if self.parent.message:
            try:
                await self.parent.message.edit(embed=embed_parent, view=self.parent)
            except Exception:
                pass


# ── Step 3: Roles ─────────────────────────────────────────────────────────────

class WizardStep3Roles(BotView):
    def __init__(self, data: WizardData, guild: discord.Guild, plugin: Jano, summary=None):
        super().__init__()
        self.data            = data
        self.guild           = guild
        self.plugin          = plugin
        self.summary         = summary
        self._sel_visibility = None
        self._sel_mention    = None

        role_options = [discord.SelectOption(label=r.name[:100], value=str(r.id)) for r in _roles_of(guild)[:25]]
        vis_options  = [discord.SelectOption(label="🌐 @everyone (all)", value="__everyone__", description="Apply to all")] + role_options[:24]
        men_options  = [discord.SelectOption(label="❌ No mention",      value="__none__", description="No ping")]       + role_options[:24]

        sel_vis          = discord.ui.Select(placeholder="👁️ Visibility role (optional — empty = @everyone)", min_values=0, max_values=1, row=0, custom_id="w_sel_vis", options=vis_options)
        sel_vis.callback = self._make_cb("vis")
        self.add_item(sel_vis)

        sel_mention          = discord.ui.Select(placeholder="📣 Mention role (optional — empty = no ping)", min_values=0, max_values=1, row=1, custom_id="w_sel_men", options=men_options)
        sel_mention.callback = self._make_cb("men")
        self.add_item(sel_mention)

        btn          = discord.ui.Button(label="Apply", style=discord.ButtonStyle.primary, emoji="💾", row=2)
        btn.callback = self._next_callback
        self.add_item(btn)

    def _make_cb(self, key: str):
        async def _cb(interaction: discord.Interaction):
            val = _selected(self, f"w_sel_{key}")
            if key == "vis":
                self._sel_visibility = val
            else:
                self._sel_mention = val
            await interaction.response.defer()
        return _cb

    async def _next_callback(self, interaction: discord.Interaction):
        if self._sel_visibility is not None:
            self.data.role_id = None if self._sel_visibility == "__everyone__" else int(self._sel_visibility)
        if self._sel_mention is not None:
            self.data.mention_role_id = None if self._sel_mention == "__none__" else int(self._sel_mention)
        self.stop()
        if self.summary:
            await _return_to_summary(self, interaction, "✅ Roles updated")
            return
        await self._finish(JanoEmbed(description="✅ Roles updated.", color=0x2ECC71))
        await interaction.response.send_modal(WizardStep4Schedule(self.data, self.plugin))


# ── Step 4: Schedule ─────────────────────────────────────────────────────────

class ViewRetry(BotView):
    """Shows an error message with a button to reopen the schedule modal."""
    def __init__(self, modal_class, modal_kwargs: dict):
        super().__init__()
        self.modal_class  = modal_class
        self.modal_kwargs = modal_kwargs
        btn               = discord.ui.Button(label="✏️ Fix and try again", style=discord.ButtonStyle.primary)
        btn.callback      = self._reopen
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

    def __init__(self, data: WizardData, plugin: Jano, summary=None):
        super().__init__(title="Schedule & Limit")
        self.data    = data
        self.plugin  = plugin
        self.summary = summary
        self._f_days = discord.ui.TextInput(
            label="Active days (empty = manual mode only)",
            placeholder="E.g.: 0,1,2,3,4  (0=Mon, 6=Sun)",
            required=False, max_length=20,
            default=",".join(str(d) for d in data.active_days) if data.active_days else ""
        )
        self._f_opening = discord.ui.TextInput(
            label="Opening time (HH:MM)",
            placeholder="E.g.: 18:15",
            required=False, max_length=5,
            default=data.opening_time if data.active_days else ""
        )
        self._f_closing = discord.ui.TextInput(
            label="Closing time (HH:MM)",
            placeholder="E.g.: 21:30",
            required=False, max_length=5,
            default=data.closing_time if data.active_days else ""
        )
        self._f_hours = discord.ui.TextInput(
            label="Max manual hours (0 or empty = no limit)",
            placeholder="E.g.: 2.5",
            required=False, max_length=5,
            default=str(data.max_manual_hours) if data.max_manual_hours > 0 else ""
        )
        for f in [self._f_days, self._f_opening, self._f_closing, self._f_hours]:
            self.add_item(f)

    async def _send_error(self, interaction, error: str):
        """Send ephemeral error message with button to reopen modal with pre-filled data."""
        view = ViewRetry(
            modal_class=WizardStep4Schedule,
            modal_kwargs={"data": self.data, "plugin": self.plugin, "summary": self.summary}
        )
        embed = JanoEmbed(
            title="⚠️ Invalid input — Schedule & Limit",
            description=f"**{error}**\n\nPress the button below to go back and correct it.",
            color=0xE74C3C
        )
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)
        try:
            view.message = await interaction.original_response()
        except Exception:
            pass


    async def on_submit(self, interaction: discord.Interaction):
        data        = self.data
        days_raw    = self._f_days.value.strip()
        opening_raw = self._f_opening.value.strip()
        closing_raw = self._f_closing.value.strip()
        hours_raw   = self._f_hours.value.strip().replace(",", ".")
        if days_raw:
            if not opening_raw or not closing_raw:
                await self._send_error(interaction, "Opening and closing times are required when days are specified")
                return
            if not _TIME_PATTERN.match(opening_raw) or not _TIME_PATTERN.match(closing_raw):
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
            data.closing_time = closing_raw   or "22:00"
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
            guild             = self.plugin._get_guild()
            embed             = _wizard_summary_embed(data, guild)
            try:
                await self.summary.message.edit(embed=embed, view=self.summary)
            except Exception:
                pass
            await interaction.response.send_message(
                embed=JanoEmbed(
                    title="✅ Schedule updated",
                    description="Review the summary and press the **green button** to confirm.",
                    color=0x2ECC71
                ), ephemeral=True, delete_after=10)
            return
        guild = self.plugin._get_guild()
        view  = WizardStep5Summary(data, guild, self.plugin)
        embed = _wizard_summary_embed(data, guild)
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)
        view.message = await interaction.original_response()


class WizardStep5Summary(BotView):
    def __init__(self, data: WizardData, guild: discord.Guild, plugin: Jano, edit_mode=False):
        super().__init__()
        self.data      = data
        self.guild     = guild
        self.plugin    = plugin
        self.edit_mode = edit_mode

        btn_name          = discord.ui.Button(label="✏️ Name / Icon", style=discord.ButtonStyle.secondary, row=0)
        btn_ch            = discord.ui.Button(label="✏️ Channels",            style=discord.ButtonStyle.secondary, row=0)
        btn_rol           = discord.ui.Button(label="✏️ Roles",               style=discord.ButtonStyle.secondary, row=0)
        btn_sch           = discord.ui.Button(label="✏️ Schedule & Limit",    style=discord.ButtonStyle.secondary, row=0)
        btn_name.callback = self._edit_name
        btn_ch.callback   = self._edit_channels
        btn_rol.callback  = self._edit_roles
        btn_sch.callback  = self._edit_schedule
        for b in [btn_name, btn_ch, btn_rol, btn_sch]:
            self.add_item(b)

        lbl             = "💾 Save changes" if edit_mode else "✅ Create instance"
        btn_ok          = discord.ui.Button(label=lbl, style=discord.ButtonStyle.success, row=1)
        btn_ok.callback = self._confirm
        self.add_item(btn_ok)

        btn_cancel          = discord.ui.Button(label="Cancel", style=discord.ButtonStyle.secondary, emoji="❌", row=1)
        btn_cancel.callback = self._cancel
        self.add_item(btn_cancel)

    async def _update_summary(self, interaction: discord.Interaction):
        if self.edit_mode == 'setup':
            self.edit_mode = True
        embed = _wizard_summary_embed(self.data, self.guild, edit_mode=self.edit_mode)
        await interaction.response.edit_message(embed=embed, view=self)

    async def _edit_name(self, interaction: discord.Interaction):
        await interaction.response.send_modal(WizardEditName(self))

    async def _edit_channels(self, interaction: discord.Interaction):
        view  = WizardStep2Channels(self.data, self.guild, self.plugin, summary=self)
        embed = _wizard_embed(2, self.data.name, "Update the channels.")
        cat   = self.guild.get_channel(self.data.category_id)
        if cat:
            embed.add_field(name="✅ Current Category / Channel", value=f"**{cat.name}**", inline=False)
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)
        view.message = await interaction.original_response()

    async def _edit_roles(self, interaction: discord.Interaction):
        view  = WizardStep3Roles(self.data, self.guild, self.plugin, summary=self)
        embed = _wizard_embed(3, self.data.name, "Update the roles.")
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)
        view.message = await interaction.original_response()

    async def _edit_schedule(self, interaction: discord.Interaction):
        await interaction.response.send_modal(WizardStep4Schedule(self.data, self.plugin, summary=self))

    async def _confirm(self, interaction: discord.Interaction):
        self.stop()
        d = self.data
        if self.edit_mode:
            st       = d.st
            cfg      = st.cfg
            old_name = cfg.name
            # Rename in the DB first (and wait for it): saving under the new name before the
            # rename would insert a duplicate row instead of updating the existing one.
            if d.name != old_name and old_name in self.plugin.states:
                if not await self._rename_in_db(old_name, d.name):
                    await interaction.response.send_message(
                        "❌ Could not rename the instance in the database. No changes were made.",
                        ephemeral=True, delete_after=120
                    )
                    return
                self.plugin.states[d.name] = self.plugin.states.pop(old_name)
            old_category_id = cfg.category_id
            if d.category_id != old_category_id:
                st.category_name_cache = None
            cfg.name, cfg.category_id, cfg.role_id = d.name, d.category_id, d.role_id
            cfg.text_channel_id, cfg.voice_channel_id = d.text_channel_id, d.voice_channel_id
            cfg.mention_role_id, cfg.active_days = d.mention_role_id, d.active_days
            cfg.opening_time, cfg.closing_time = d.opening_time, d.closing_time
            cfg.max_manual_hours = d.max_manual_hours
            cfg.status_icon = d.status_icon
            await st.save()
            if d.category_id != old_category_id:
                await self.plugin._release_category(old_category_id, d.name)
            # Apply category rename immediately so the change is visible at once
            _spawn(self.plugin._evaluate_instance(st))
            await self._finish(JanoEmbed(title=f"✅ Instance '{d.name}' updated!", color=0x2ECC71))
            await interaction.response.send_message(embed=JanoEmbed(
                title=f"✅ Changes saved — {d.name}", description="All changes applied immediately.", color=0x2ECC71
            ), ephemeral=True, delete_after=120)
            log.info(f"[Jano] Instance '{d.name}' edited by {interaction.user}")
        else:
            if len(self.plugin.states) >= _MAX_INSTANCES or d.name in self.plugin.states:
                await interaction.response.send_message(
                    f"❌ Cannot create '{d.name}': the instance limit ({_MAX_INSTANCES}) was reached or the name is already taken.",
                    ephemeral=True, delete_after=120
                )
                return
            cfg = InstanceConfig(
                name=d.name, server_id=self.plugin._server_id,
                category_id=d.category_id, role_id=d.role_id,
                text_channel_id=d.text_channel_id, voice_channel_id=d.voice_channel_id,
                mention_role_id=d.mention_role_id, active_days=d.active_days,
                opening_time=d.opening_time, closing_time=d.closing_time,
                max_manual_hours=d.max_manual_hours,
                command_role_ids_instance=None,
                status_icon=d.status_icon,
                tz=self.plugin.tz,
            )
            await self.plugin._create_instance(cfg)
            await self._finish(JanoEmbed(
                title=f"✅ Instance '{d.name}' created!",
                description="You can now use all bot commands with this instance.", color=0x2ECC71
            ))
            await interaction.response.send_message(embed=JanoEmbed(
                title=f"✅ Instance created — {d.name}",
                description=f"The instance **{d.name}** is ready.\n\nUse `/jano setup` to modify its configuration.", color=0x2ECC71
            ), ephemeral=True, delete_after=120)
            log.info(f"[Jano] Instance '{d.name}' created by {interaction.user}")

    async def _rename_in_db(self, old: str, new: str) -> bool:
        try:
            async with self.plugin.apool.connection() as conn:
                await conn.execute("UPDATE jano_instances SET name = %s WHERE name = %s", (new, old))
            return True
        except Exception as e:
            log.error(f"[Jano] Error renaming instance in DB: {e}")
            return False

    async def _cancel(self, interaction: discord.Interaction):
        self.stop()
        await self._finish(JanoEmbed(description="❌ Cancelled. No changes were made.", color=0x95A5A6), delay=10)
        await interaction.response.defer()


class WizardEditName(discord.ui.Modal, title="Name & Status Icon"):

    def __init__(self, summary: WizardStep5Summary):
        super().__init__()
        self.summary = summary
        # Store field references as instance attributes for access in on_submit
        self._f_name = discord.ui.TextInput(
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
        self.add_item(self._f_name)
        self.add_item(self._f_status)

    async def on_submit(self, interaction: discord.Interaction):
        new_name = self._f_name.value.strip()
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
                embed=JanoEmbed(
                    description="✏️ Name / Icon updated in the summary above.\n\n⚠️ Review the summary and press the **green button** to confirm.",
                    color=0x3498DB
                ),
                ephemeral=True, wait=True
            )
            _spawn(_delete_after(msg, delay=10))
        except Exception:
            pass


# ══════════════════════════════════════════════════════════════════════════════
# VIEWS — Command roles per instance
# ══════════════════════════════════════════════════════════════════════════════

class ViewAccessRoles(BotView):
    def __init__(self, guild: discord.Guild, plugin: Jano):
        super().__init__()
        self.guild      = guild
        self.plugin     = plugin
        self.selections = {n: None for n in plugin._get_names()}

        everyone = discord.SelectOption(label="🌐 @everyone (all)", value="__everyone__", description="All users")
        none_val = discord.SelectOption(label="❌ None",             value="__none__", description="Clears roles")
        options  = [everyone, none_val] + [discord.SelectOption(label=r.name[:100], value=str(r.id)) for r in _roles_of(guild)[:23]]

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
            vals = _selected_values(self, f"role_select_{name}")
            if not vals:
                self.selections[name] = None
            elif "__everyone__" in vals:
                self.selections[name] = ["__everyone__"]
            elif "__none__" in vals:
                self.selections[name] = []
            else:
                self.selections[name] = [int(v) for v in vals]
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

        embed = JanoEmbed(
            title="🔑 Review — Instance command roles",
            description="Review your selection and press **Save instance roles** to apply.",
            color=0x9B59B6
        )
        embed.add_field(name="Pending changes", value="\n".join(lines) or "No changes selected.", inline=False)
        view_save = ViewSaveAccessRoles(self)
        if self.message:
            try:
                await self.message.edit(embed=JanoEmbed(
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
                await st_inst.save()
                changes.append(f"**{name}** → 🌐 @everyone")
            elif selection == []:
                st_inst.cfg.command_role_ids_instance = None
                await st_inst.save()
                changes.append(f"**{name}** → ❌ None")
            else:
                valid_roles = [r for r in selection if self.guild.get_role(r)]
                if valid_roles:
                    st_inst.cfg.command_role_ids_instance = valid_roles
                    await st_inst.save()
                    role_names = ", ".join(self.guild.get_role(r).name for r in valid_roles if self.guild.get_role(r))
                    changes.append(f"**{name}** → {role_names}")
        self.stop()
        embed = JanoEmbed(title="🔑 Command roles updated", color=0x2ECC71)
        if changes:
            embed.add_field(name="Changes applied", value="\n".join(changes), inline=False)
        else:
            embed.description = "No changes were made."
        try:
            await _followup_send(interaction, embed)
        except Exception:
            _ephemeral(interaction, embed=embed)


class ViewSaveAccessRoles(BotView):
    def __init__(self, parent: ViewAccessRoles):
        super().__init__()
        self.parent  = parent
        btn          = discord.ui.Button(label="Save instance roles", style=discord.ButtonStyle.success, emoji="💾", row=0, custom_id="save_roles")
        btn.callback = self._save_callback
        self.add_item(btn)

    async def _save_callback(self, interaction: discord.Interaction):
        self.stop()
        try:
            await interaction.response.edit_message(embed=JanoEmbed(
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
