"""
Jano Plugin for DCSServerBot
Manages Discord channel visibility on a configurable schedule or manually.
"""

from __future__ import annotations

import asyncio
import datetime
import io
import json
import logging
import math
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
import zipfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Type

import discord
from zoneinfo import ZoneInfo
from discord import app_commands
from discord.ext import tasks

import psycopg.rows
from core import Group, Plugin, TEventListener, utils
from services.bot import DCSServerBot

log = logging.getLogger(__name__)

# Default timezone — overridden per-plugin-instance via jano.yaml (timezone: "Europe/Madrid").
# Never read TZ directly; always use plugin.tz or pass it explicitly so the value
# stays scoped to the plugin instance and does not leak across hot-reloads.
_DEFAULT_TZ = "Europe/Madrid"

# Internal version of this file only — updated manually in commands.py, independent of version.py.
# install.cmd reads this line to show the installed/new version: keep the format COMMANDS_VERSION = "x.y.z"
COMMANDS_VERSION = "5.0.7"

_MAX_INSTANCES = 4

_DAY_NAMES    = {0: "Mon", 1: "Tue", 2: "Wed", 3: "Thu", 4: "Fri", 5: "Sat", 6: "Sun"}
_TIME_PATTERN = re.compile(r"(\d{1,2}):(\d{2})")
_STATUS_ICONS = re.compile(r"[🟢🔴]\s*")

_FOOTER_SEPARATOR = "▬" * 36
# Leading zero-width space keeps an empty line above the separator (Discord trims plain leading newlines).
EMBED_FOOTER = f"\u200b\n{_FOOTER_SEPARATOR}\nJano v.{COMMANDS_VERSION}"


class JanoEmbed(discord.Embed):
    """discord.Embed that always carries the Jano footer (separator + version)."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.set_footer(text=EMBED_FOOTER)


def _add_field(modal: discord.ui.Modal, label: str, **kwargs) -> discord.ui.TextInput:
    """Add a text field to a modal and return it.

    The visible label lives in a discord.ui.Label wrapper: TextInput(label=…) is deprecated since
    discord.py 2.6. Labels are limited to 45 characters.
    """
    field = discord.ui.TextInput(**kwargs)
    modal.add_item(discord.ui.Label(text=label, component=field))
    return field

def _parse_hhmm(text: str) -> int | None:
    """Minutes since midnight for 'H:MM' / 'HH:MM', or None when the text is not a valid time."""
    m = _TIME_PATTERN.fullmatch(text.strip())
    if not m:
        return None
    hours, minutes = int(m[1]), int(m[2])
    return hours * 60 + minutes if hours <= 23 and minutes <= 59 else None

def _parse_hours(text: str) -> float | None:
    """Hours typed by a user ('2', '2,5'), or None when it is not a finite number >= 0."""
    try:
        value = float(text.strip().replace(",", "."))
    except ValueError:
        return None
    return value if math.isfinite(value) and value >= 0 else None

def _parse_days(text: str) -> list[int] | None:
    """Weekday numbers (0=Mon … 6=Sun) from a comma-separated text, or None when invalid/empty."""
    days = [int(d) for d in (t.strip() for t in text.split(",")) if d.isdecimal()]
    return days if days and all(d <= 6 for d in days) else None


@dataclass
class ManualInfo:
    """Snapshot of an active manual mode: unlimited, or time remaining until it expires."""
    no_limit:   bool
    remaining:  datetime.timedelta | None = None
    expires_at: datetime.datetime | None  = None


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

    def manual_mode_info(self) -> ManualInfo | None:
        if self.manual_override is None or not self.override_ts:
            return None
        if self.manual_hours_active <= 0:
            return ManualInfo(no_limit=True)
        now       = datetime.datetime.now(self.cfg.tz)
        total     = datetime.timedelta(hours=self.manual_hours_active)
        elapsed   = now - self.override_ts
        remaining = total - elapsed
        if remaining.total_seconds() < 0:
            remaining = datetime.timedelta(seconds=0)
        return ManualInfo(no_limit=False, remaining=remaining, expires_at=now + remaining)

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
        opening_min, closing_min = _parse_hhmm(open_t), _parse_hhmm(close_t)
        if opening_min is None or closing_min is None:
            return False, "NO_SCHEDULE"   # corrupted times stored in the DB: stay closed
        now          = datetime.datetime.now(self.cfg.tz)
        current_time = now.hour * 60 + now.minute
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

    def __init__(self, st: "InstanceState", plugin: "Jano"):
        super().__init__()
        self.st     = st
        self.plugin = plugin
        ceiling     = st.active_ceiling()
        if ceiling > 0:
            label       = f"Duration in hours — max is {ceiling}h"
            placeholder = f"1 to {ceiling}  ·  0 = apply max ({ceiling}h)  ·  empty = apply max"
        else:
            label       = "Duration in hours (0 or empty = no limit)"
            placeholder = "e.g. 2  or  2.5  ·  0 or empty = no limit"
        self._f_duration = _add_field(self, label, placeholder=placeholder, required=False, max_length=10)

    async def on_submit(self, interaction: discord.Interaction):
        raw      = self._f_duration.value.strip()
        duration = _parse_hours(raw) if raw else None
        if raw and duration is None:
            await _error(interaction, "❌ Invalid duration. Enter a positive number in hours (e.g. 2.5) or 0 for no limit.")
            return
        # 0 / empty = no explicit duration. A value above the ceiling is trimmed by activate_manual,
        # which records it in _trimmed_duration so the reply shows the adjustment.
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
        # Global command role IDs, resolved from jano.yaml in on_ready
        self.command_role_ids_global: list[int] = []
        # Raw command_role_ids from jano.yaml (role names or numeric IDs), resolved in on_ready
        self._command_role_ids_raw: list = []
        # Server ID (guild ID) — read from bot's guild
        self._server_id: int = 0
        # Timezone for schedule calculations — set in cog_load from jano.yaml.
        # Stored here (not as a module global) so hot-reloads and multiple
        # plugin instances stay isolated.
        self.tz: ZoneInfo = ZoneInfo(_DEFAULT_TZ)
        # Open-channel announcement template — set in cog_load from jano.yaml
        self._open_message_template: dict = {}
        # True while /jano upgrade is downloading/installing (only one at a time)
        self._upgrading: bool = False

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
        self._resolve_yaml_roles(guild)
        await self._migrate_db()
        await self._load_state()
        await self._clean_orphan_embeds()
        await self._evaluate_all()
        if not self.scheduler.is_running():
            self.scheduler.start()
        self.log.debug(f"Ready - {len(self.states)} instance(s) loaded.")
        await self._finish_restart_notice()

    def _resolve_yaml_roles(self, guild: discord.Guild):
        """Resolve role names or numeric IDs from jano.yaml into the global command role IDs.
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
        self.command_role_ids_global = resolved

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
        """Who may use the commands. Instance roles: None = only the global roles, [] = @everyone, ids = global + those roles."""
        instance_roles = st.cfg.command_role_ids_instance if st else None
        if instance_roles == []:
            return True   # the instance was set to @everyone
        allowed = set(self.command_role_ids_global) | set(instance_roles or [])
        if not allowed:
            return True   # no command roles configured anywhere: everyone may use the commands
        return any(r.id in allowed for r in getattr(interaction.user, "roles", []))

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
                async with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
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
                    command_role_ids_instance = None if r["command_role_ids_instance"] is None else list(r["command_role_ids_instance"]),
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
            txt = _channel(guild, st.cfg.text_channel_id) if guild else None
            if txt and await _delete_message(txt, st.last_message_id):
                self.log.info(f"[{name}] 🧹 Embed deleted on instance removal")
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
                if is_open and not st.last_message_id and source == "SCHEDULE":
                    mention_id = st.cfg.mention_role_id
                    msg = await text_ch.send(
                        content=f"<@&{mention_id}>" if mention_id else None,
                        embed=self._announcement_embed(st),
                        allowed_mentions=discord.AllowedMentions(roles=True)
                    )
                    st.last_message_id = msg.id
                elif not is_open and st.last_message_id:
                    await _delete_message(text_ch, st.last_message_id)
                    st.last_message_id = None

            st.current_state = is_open
            await st.save_state()
        finally:
            st._evaluating = False

    def _announcement_embed(self, st: InstanceState) -> discord.Embed:
        """Embed posted when the channels open on schedule, built from the jano.yaml template or defaults.

        Supported keys under open_message:
          title: "Custom title — {name}"
          body:  "Custom body — connect to {voice}"
        {name} and {voice} are replaced with the instance name and the voice-channel mention.
        """
        voice_id      = st.cfg.voice_channel_id
        voice_mention = f"<#{voice_id}>" if voice_id else "Voice Channel"
        tpl           = self._open_message_template

        def fmt(text: str) -> str:
            return text.replace("{name}", st.cfg.name).replace("{voice}", voice_mention)

        body_default = (
            f"The **{st.cfg.name}** channels will remain open until the end of the event."
            f"\n\nPlease connect to the channel:\n\n{voice_mention}\n\n"
            f"Before the start of **{st.cfg.name}**."
        )
        return JanoEmbed(
            title=fmt(tpl.get("title", "🟢   __**{name} ACCESS CHANNELS ARE OPEN**__")),
            description=fmt(tpl.get("body", body_default)),
            color=0x2ECC71
        )

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
                        if await _delete_message(txt, st.last_message_id):
                            self.log.info(f"[{st.cfg.name}] 🧹 Orphan embed removed")
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
            await _error(interaction, "❌ Specify an instance. Options: " + ", ".join(self._get_names()))
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
                if info.no_limit:
                    embed.add_field(name="__**Manual mode**__", value="**No time limit**", inline=False)
                else:
                    embed.add_field(name="__**Manual duration**__",  value=f"**{st.manual_hours_active}h**", inline=False)
                    embed.add_field(name="__**Remaining**__",         value=f"**{_fmt_duration(info.remaining)}**", inline=False)
                    embed.add_field(name="__**Expires at**__",        value=f"**{info.expires_at.strftime('%H:%M')}**", inline=False)

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
            embed.add_field(
                name="__Command roles (global)__",
                value=", ".join(f"**{n}**" for n in _role_names(guild, self.command_role_ids_global)) or "❌ Not configured",
                inline=False
            )
            ri = st.cfg.command_role_ids_instance
            if ri is None:
                ri_val = "🔑 **Only Global Role**"
            elif ri == []:
                ri_val = "🌐 @everyone"
            else:
                ri_val = ", ".join(f"**{n}**" for n in _role_names(guild, ri)) or "❌"
            embed.add_field(name="__Command roles (instance)__", value=ri_val, inline=False)

        await _followup_send(interaction, embed)

    # ── /jano comms ───────────────────────────────────────────────────────

    @jano_group.command(name="comms", description="Open, close or resume comms channels")
    @app_commands.describe(
        instance="Instance to act on (auto-selected if only one exists)",
        action="What to do: open channels, close them, or resume automatic schedule"
    )
    @app_commands.choices(action=[
        app_commands.Choice(name="open  — manually open channels",  value="open"),
        app_commands.Choice(name="close — manually close channels", value="close"),
        app_commands.Choice(name="resume — return to schedule",     value="resume"),
    ])
    async def jano_comms(self, interaction: discord.Interaction, instance: str = None, action: str = None):
        st = await self._prepare(interaction, instance)
        if not st:
            return

        if not action:
            await _error(interaction, "❌ Choose an action: **open**, **close** or **resume**.", 60)
            return

        if action == "open":
            # Ask for duration via modal
            await interaction.response.send_modal(ModalCommsDuration(st, self))
        elif action == "close":
            await self._comms_close(interaction, st)
        else:
            await self._comms_resume(interaction, st)

    async def _comms_open(self, interaction: discord.Interaction, st: InstanceState, duration_val: float | None = None):
        if st.current_state and st.manual_override is True:
            if duration_val is None:
                embed = JanoEmbed(
                    title=f"🟢 Already open — {st.cfg.name}",
                    description="The channels are already open in manual mode. No changes made.",
                    color=0x95A5A6
                )
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
        if info and not info.no_limit:
            embed.add_field(
                name="⏳ Remaining manual time",
                value=f"**{_fmt_duration(info.remaining)}** (expires at {info.expires_at.strftime('%H:%M')})",
                inline=False
            )
        embed.add_field(name="What next?", value="Keep manual mode or resume automatic schedule.", inline=False)
        view         = ViewCloseConfirm(st, self)
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

        if not self.states:
            view  = ViewSetupEmpty(self)
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

        view  = ViewSetup(st, self._get_guild(), self)
        embed = JanoEmbed(
            title=f"⚙️ Setup — {st.cfg.name}",
            description="What would you like to configure?",
            color=0x3498DB
        )
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)
        view.message = await interaction.original_response()

    # ── /jano upgrade ─────────────────────────────────────────────────────

    @jano_group.command(name="upgrade", description="Admin only: update Jano from GitHub (release or development branch)")
    @app_commands.check(utils.restricted_check)
    @utils.app_has_role("Admin")
    async def jano_upgrade(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        release, dev, notes = None, None, []
        try:
            release = await self._upgrade_lookup()
        except Exception as e:
            self.log.error(f"Jano: release check failed: {e}", exc_info=not isinstance(e, UpgradeError))
            notes.append(f"Could not check the releases: {e}")
        try:
            dev = await self._upgrade_lookup_dev()
        except Exception as e:
            self.log.error(f"Jano: development-branch check failed: {e}", exc_info=not isinstance(e, UpgradeError))
            notes.append(f"Could not check the development branch: {e}")
        if release and dev and dev["version"] <= release["version"]:
            dev = None   # the release is as new (or newer) and more stable: no point in offering dev
        if release is None and dev is None:
            text = (f"❌ {' '.join(notes)}" if notes else
                    f"✅ You already have the newest version (Ver. {COMMANDS_VERSION}).")
            await _followup_send(interaction, JanoEmbed(description=text, color=0xE74C3C if notes else 0x2ECC71),
                                 delay=30 if notes else 10)
            return
        view = _UpgradeView(self, interaction.user.id, release, dev)
        view.message = await interaction.followup.send(embed=_upgrade_embed(COMMANDS_VERSION, release, dev, notes),
                                                       view=view, ephemeral=True, wait=True)

    async def _http_get(self, url: str, as_json: bool = False):
        """GET `url` (HTTPS only, at most UPGRADE_MAX_BYTES). JSON or bytes."""
        import aiohttp
        if not url.startswith("https://"):
            raise UpgradeError("Refusing a non-HTTPS download address.")
        timeout = aiohttp.ClientTimeout(total=60)
        async with aiohttp.ClientSession(timeout=timeout, headers={"User-Agent": "Jano-upgrade",
                                                                 "Accept": "application/vnd.github+json"}) as session:
            async with session.get(url) as resp:
                if resp.status != 200:
                    raise UpgradeError(f"GitHub answered {resp.status}.")
                if resp.url.scheme != "https":
                    raise UpgradeError("Refusing a download that left HTTPS.")
                chunks, size = [], 0
                async for chunk in resp.content.iter_chunked(65536):   # the whole body, not just what has arrived
                    size += len(chunk)
                    if size > UPGRADE_MAX_BYTES:
                        raise UpgradeError("The download is larger than expected.")
                    chunks.append(chunk)
        data = b"".join(chunks)
        return json.loads(data) if as_json else data

    async def _upgrade_lookup(self) -> dict | None:
        """The release to offer, or None when the installed one is the newest."""
        releases = await self._http_get(f"https://api.github.com/repos/{UPGRADE_REPO}/releases?per_page=15", as_json=True)
        return _pick_release(releases, _release_version(COMMANDS_VERSION) or (0, 0, 0))

    async def _upgrade_lookup_dev(self) -> dict | None:
        """The development branch, if its version is above the installed one (None if not). The zip is
        downloaded and checked now, so what is offered is exactly what would be installed."""
        data = await self._http_get(f"https://api.github.com/repos/{UPGRADE_REPO}/zipball/dev")
        text = _zip_version(_read_release_zip(data))
        version = _release_version(text)
        if not version or version <= (_release_version(COMMANDS_VERSION) or (0, 0, 0)):
            return None
        return {"channel": "dev", "version": version, "text": text, "tag": "dev", "prerelease": True,
                "notes": "", "data": data}

    @staticmethod
    def _plugin_dir() -> str:
        return os.path.dirname(os.path.abspath(__file__))

    def _run_migration(self, migrate_source: bytes) -> str:
        """Run the release's migrate_config.py on this installation's jano.yaml. Returns "" when fine,
        else a short note (the file is then left as it was)."""
        yaml_path = os.path.join(self.node.config_dir, "plugins", "jano.yaml")
        if not os.path.exists(yaml_path):
            return ""
        with tempfile.TemporaryDirectory() as tmp:
            script = os.path.join(tmp, "migrate_config.py")
            with open(script, "wb") as f:
                f.write(migrate_source)
            try:
                res = subprocess.run([sys.executable, "-I", script, yaml_path], capture_output=True, text=True,
                                     timeout=120)
            except Exception as e:
                return f"configuration migration could not run ({e})"
        if res.returncode != 0:
            self.log.warning(f"Jano: configuration migration failed: {res.stdout.strip()} {res.stderr.strip()}")
            return "configuration migration reported an error (your file is unchanged)"
        return ""

    async def _upgrade_run(self, interaction: discord.Interaction, release: dict) -> None:
        """Download (or take the checked dev zip), install, telling the admin each step, then ask whether
        to restart. Any failure leaves the previous files in place."""
        if self._upgrading:
            await interaction.followup.send("An update is already running.", ephemeral=True)
            return
        self._upgrading = True

        async def say(text: str, view=None, color: int = 0x3498DB) -> None:
            await interaction.edit_original_response(embed=JanoEmbed(description=text, color=color), view=view)
        try:
            if release.get("data") is None:
                await say("⏳ Downloading the release...")
                data = await self._http_get(release["zip_url"])
            else:
                data = release["data"]
            await say("🔎 Checking the files...")
            files = _read_release_zip(data, release["text"])
            await say("📦 Installing...")
            changed = await asyncio.to_thread(_install_release_files, self._plugin_dir(), files)
            note = await asyncio.to_thread(self._run_migration, files["migrate_config.py"])
        except UpgradeError as e:
            await say(f"❌ Update stopped: {e}", color=0xE74C3C)
            _spawn(_later(30, interaction.delete_original_response))
            return
        except Exception as e:
            self.log.error(f"Jano: update failed: {e}", exc_info=True)
            await say(f"❌ Update failed, the previous version is still in place: {e}", color=0xE74C3C)
            _spawn(_later(30, interaction.delete_original_response))
            return
        finally:
            self._upgrading = False
        self.log.info(f"Jano: updated {COMMANDS_VERSION} -> {release['text']} "
                      f"({len(changed)} file(s) replaced, restart pending).")
        view = _RestartView(self, interaction.user.id, release["text"])
        view.message = await interaction.original_response()
        await say(f"✅ Updated to Ver. {release['text']} ({len(changed)} file(s) replaced; the old ones are kept in "
                  f"`plugins/jano/.backup`)." + (f" ⚠️ {note}." if note else "")
                  + "\nDo you want to restart DCSServerBot now so the update takes effect?",
                  view=view, color=0x2ECC71)

    # The notice that closes a restart: the new process finishes what the old one started.

    def _restart_notice_file(self) -> str:
        return os.path.join(self._plugin_dir(), ".restart_notice.json")

    def _save_restart_notice(self, interaction: discord.Interaction, expected: str) -> None:
        try:
            with open(self._restart_notice_file(), "w", encoding="utf-8") as f:
                json.dump({"app_id": interaction.application_id, "token": interaction.token,
                           "message_id": interaction.message.id, "expected": expected, "created": time.time()}, f)
            self.log.info("Jano: restart notice saved for the next start.")
        except Exception as e:
            self.log.warning(f"Jano: could not save the restart notice: {e!r}")

    def _notice_webhook(self, app_id, token):
        return discord.Webhook.partial(app_id, token, client=self.bot)

    async def _finish_restart_notice(self) -> None:
        """After a restart asked from /jano upgrade: say whether the update was applied and which version
        runs, then remove the message. The file is always deleted: used, stale or unreadable."""
        path = self._restart_notice_file()
        if not os.path.exists(path):
            return
        try:
            with open(path, encoding="utf-8") as f:
                data = json.load(f)
        except Exception:
            data = None
        try:
            os.remove(path)
        except OSError:
            pass
        try:
            if not isinstance(data, dict) or time.time() - float(data["created"]) > 14 * 60:
                return   # too old: Discord no longer accepts its token
            expected = str(data["expected"])
            if expected == COMMANDS_VERSION:
                text = f"✅ DCSServerBot has been updated successfully.\nJano Ver. {COMMANDS_VERSION} is now running."
                color = 0x2ECC71
            else:
                text = (f"⚠️ DCSServerBot restarted, but the update was not applied.\n"
                        f"Jano Ver. {COMMANDS_VERSION} is running (Ver. {expected} was expected).")
                color = 0xE67E22
            hook = self._notice_webhook(data["app_id"], data["token"])
            last_error = None
            # The message the "Restart now" button sat on: "@original" for that button's token; its
            # numeric id as a second way if Discord refuses the first.
            for target in ("@original", int(data["message_id"])):
                try:
                    await hook.edit_message(target, embeds=[JanoEmbed(description=text, color=color)], view=None)
                except Exception as e:
                    last_error = e
                    continue
                self.log.info(f"Jano: restart notice delivered ({text[:2]}).")
                _spawn(_later(20, lambda target=target: hook.delete_message(target)))
                return
            self.log.warning(f"Jano: could not update the restart message: {last_error!r}")
        except Exception as e:
            self.log.warning(f"Jano: could not finish the restart notice: {e!r}")

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

def _add_duration_fields(embed: discord.Embed, st: InstanceState, info: ManualInfo | None, label: str = "Active duration"):
    """Append the manual-mode duration fields (trim warning, duration, remaining, expiry) to an embed."""
    if st._trimmed_duration:
        requested, applied = st._trimmed_duration
        embed.add_field(name="⚠️ Duration adjusted", value=f"Requested **{requested}h**, max is **{applied}h**.", inline=False)
    if info and not info.no_limit:
        embed.add_field(name=label,        value=f"{st.manual_hours_active}h", inline=False)
        embed.add_field(name="Remaining",  value=_fmt_duration(info.remaining), inline=False)
        embed.add_field(name="Expires at", value=info.expires_at.strftime('%H:%M'), inline=False)
    else:
        ceiling = st.active_ceiling()
        embed.add_field(name="Duration", value=f"Indefinite · max {ceiling}h" if ceiling > 0 else "No limit", inline=False)

async def _error(interaction: discord.Interaction, text: str, delay: int = 120):
    """Reply with an ephemeral error text that disappears after `delay` seconds."""
    await interaction.response.send_message(text, ephemeral=True, delete_after=delay)

def _role_names(guild: discord.Guild, role_ids) -> list[str]:
    """Names of the roles that still exist in the guild."""
    return [r.name for r in (guild.get_role(rid) for rid in role_ids) if r]

def _command_roles_label(guild: discord.Guild, role_ids: list | None, none_text: str) -> str:
    """Readable form of an instance's command roles (None = only global roles, [] = @everyone)."""
    if role_ids is None:
        return none_text
    if not role_ids:
        return "🌐 @everyone"
    return ", ".join(_role_names(guild, role_ids)) or none_text

async def _delete_message(channel, message_id: int) -> bool:
    """Delete a channel message by ID; False when it no longer exists or cannot be deleted."""
    try:
        msg = await channel.fetch_message(message_id)
        await msg.delete()
        return True
    except Exception:
        return False

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
    def __init__(self, st: InstanceState, plugin: Jano):
        super().__init__()
        self.st     = st
        self.plugin = plugin

    @discord.ui.button(label="Keep manual mode", style=discord.ButtonStyle.secondary, emoji="⏳")
    async def keep_manual(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True)
        await self.plugin._evaluate_instance(self.st)
        info = self.st.manual_mode_info()   # computed now, not when the close message was sent
        if info is None:
            desc = "Manual mode is no longer active — channels follow the automatic schedule."
        elif info.no_limit:
            desc = "Channels will remain closed in manual mode with no time limit."
        else:
            desc = f"Channels will return to automatic schedule in **{_fmt_duration(info.remaining)}** (at **{info.expires_at.strftime('%H:%M')}**)."
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
# UPGRADE FROM GITHUB — /jano upgrade
# ══════════════════════════════════════════════════════════════════════════════

UPGRADE_REPO = "pierpaolobirdi/jano-dcsserverbot-plugin"
UPGRADE_PREFIX = "plugins/jano/"
UPGRADE_FILES = ("plugins/jano/__init__.py", "plugins/jano/commands.py", "plugins/jano/listener.py",
                 "plugins/jano/version.py", "plugins/jano/db/tables.sql", "migrate_config.py")
UPGRADE_MAX_BYTES = 25 * 1024 * 1024


class UpgradeError(Exception):
    """A refusal the admin should read as is (nothing was changed)."""


async def _later(delay: float, action) -> None:
    """Run the async `action` once, `delay` seconds from now (errors ignored: the message may already
    be gone or its interaction token expired)."""
    await asyncio.sleep(delay)
    try:
        await action()
    except Exception as e:
        log.debug(f"Jano: clean-up of a message failed: {e!r}")


def _release_version(text) -> tuple[int, int, int] | None:
    """(5, 0, 2) from a tag like 'v.5.0.2' or '5.0.2'; None if it has no x.y.z."""
    m = re.search(r"(\d+)\.(\d+)\.(\d+)", str(text or ""))
    return tuple(int(x) for x in m.groups()) if m else None


def _pick_release(releases: list, current: tuple[int, int, int]) -> dict | None:
    """The newest published release above `current`; its code is GitHub's own zip of the tag.
    Drafts are ignored; a pre-release is returned with prerelease=True."""
    best = None
    for rel in releases or []:
        if rel.get("draft"):
            continue
        version = _release_version(rel.get("tag_name"))
        if not version or version <= current or not rel.get("zipball_url"):
            continue
        if best is None or version > best["version"]:
            best = {"version": version, "text": ".".join(map(str, version)), "tag": rel.get("tag_name", ""),
                    "prerelease": bool(rel.get("prerelease")), "notes": rel.get("body") or "",
                    "published": rel.get("published_at") or "", "zip_url": rel["zipball_url"]}
    return best


def _zip_version(files: dict[str, bytes]) -> str | None:
    m = re.search(rb'^COMMANDS_VERSION\s*=\s*"([^"]+)"', files[UPGRADE_PREFIX + "commands.py"], re.MULTILINE)
    return m.group(1).decode() if m else None


def _read_release_zip(data: bytes, version_text: str | None = None) -> dict[str, bytes]:
    """The plugin's files out of GitHub's zip of a tag or branch (everything sits under one folder named
    after the repository and commit). Only the expected files are taken, the rest of the repository is
    ignored; every .py must compile, tables.sql must be the Jano schema and commands.py must announce the
    tag's version."""
    try:
        zf = zipfile.ZipFile(io.BytesIO(data))
        entries = [i.filename for i in zf.infolist() if not i.is_dir()]
    except zipfile.BadZipFile:
        raise UpgradeError("The downloaded file is not a valid zip.")
    tops = {n.split("/", 1)[0] for n in entries if "/" in n}
    if len(tops) != 1:
        raise UpgradeError("The downloaded zip does not have the expected layout.")
    top = tops.pop() + "/"
    files = {}
    for name in UPGRADE_FILES:
        if top + name not in entries:
            raise UpgradeError(f"The release does not contain {name}.")
        files[name] = zf.read(top + name)
    for name, content in files.items():
        if name.endswith(".py"):
            try:
                compile(content, name, "exec")
            except SyntaxError as e:
                raise UpgradeError(f"{name} does not compile ({e.msg}, line {e.lineno}).")
    if b"jano_instances" not in files[UPGRADE_PREFIX + "db/tables.sql"]:
        raise UpgradeError("db/tables.sql is not the Jano schema.")
    found = _zip_version(files)
    if not found or (version_text is not None and found != version_text):
        raise UpgradeError("The version inside the release does not match its tag.")
    return files


def _install_release_files(plugin_dir: str, files: dict[str, bytes]) -> list[str]:
    """Replace the plugin's files with the release's, keeping a copy of the old ones in
    <plugin_dir>/.backup (the previous backup is replaced). Files that are already identical are left
    alone; other files in the folder are never touched. Any failure puts the old files back and raises.
    Returns the names of the files that changed."""
    def path_of(rel: str) -> str:
        return os.path.join(plugin_dir, *rel.split("/"))

    old, changed = {}, {}
    for name, content in files.items():
        if not name.startswith(UPGRADE_PREFIX):
            continue                                 # migrate_config.py is run from a temporary folder, not installed
        rel = name[len(UPGRADE_PREFIX):]
        current = None
        if os.path.exists(path_of(rel)):
            with open(path_of(rel), "rb") as f:
                current = f.read()
        if current != content:
            changed[rel], old[rel] = content, current
    if not changed:
        return []
    backup = os.path.join(plugin_dir, ".backup")
    shutil.rmtree(backup, ignore_errors=True)
    for rel, content in old.items():
        if content is not None:
            dest = os.path.join(backup, *rel.split("/"))
            os.makedirs(os.path.dirname(dest), exist_ok=True)
            with open(dest, "wb") as f:
                f.write(content)
    done = []
    try:
        for rel, content in changed.items():
            os.makedirs(os.path.dirname(path_of(rel)), exist_ok=True)
            with open(path_of(rel) + ".new", "wb") as f:
                f.write(content)
            os.replace(path_of(rel) + ".new", path_of(rel))
            done.append(rel)
    except Exception:
        for rel in done:
            if old[rel] is None:
                os.remove(path_of(rel))
            else:
                with open(path_of(rel), "wb") as f:
                    f.write(old[rel])
        for rel in changed:
            try:
                os.remove(path_of(rel) + ".new")
            except OSError:
                pass
        raise
    return list(changed)


def _upgrade_embed(current_text: str, release: dict | None, dev: dict | None, notes: list[str]) -> discord.Embed:
    embed = JanoEmbed(title="⬆️ Jano — update", description=((release or {}).get("notes", "").strip()[:900] or None),
                      color=0xF39C12 if (release or {}).get("prerelease") else 0x2ECC71)
    embed.add_field(name="Installed", value=f"Ver. {current_text}", inline=True)
    if release:
        embed.add_field(name="Release", value=f"Ver. {release['text']}" + (" ⚠️ pre-release" if release["prerelease"] else ""),
                        inline=True)
    if dev:
        embed.add_field(name="Development branch", value=f"Ver. {dev['text']} ⚠️", inline=True)
    if release and release["prerelease"]:
        embed.add_field(name="⚠️ PRE-RELEASE",
                        value="This release is a pre-release: it may be unstable. Update only if you want to test it.",
                        inline=False)
    if dev:
        embed.add_field(name="⚠️ DEVELOPMENT BRANCH",
                        value="Work in progress: it may contain errors. Updating to it asks you to accept that risk first.",
                        inline=False)
    for note in notes:
        embed.add_field(name="ℹ️", value=note[:500], inline=False)
    embed.add_field(name="What happens",
                    value="The files are replaced (a copy of the old ones is kept). Afterwards you choose whether "
                          "to restart DCSServerBot; the new version only runs after a restart.", inline=False)
    return embed


def _dev_warning_embed(dev: dict) -> discord.Embed:
    embed = JanoEmbed(title="⚠️ Development branch", color=0xE74C3C,
                      description=f"You are about to install **Ver. {dev['text']}** from the development branch.")
    embed.add_field(name="The risk",
                    value="This is code that is still being developed. It can contain errors, change how Jano looks "
                          "or behaves, or stop working. It has not gone through a release.", inline=False)
    embed.add_field(name="If something goes wrong",
                    value="A copy of the current files is kept in `plugins/jano/.backup`, and a release can always "
                          "be installed again with `install.cmd`.", inline=False)
    embed.add_field(name="To continue", value="Press **Accept the risk and update** to confirm you understand this.",
                    inline=False)
    return embed


class _AdminOnlyView(BotView):
    """Only the admin who ran /jano upgrade can press its buttons."""

    def __init__(self, plugin, user_id: int, timeout: int = 120):
        super().__init__(timeout=timeout)
        self.plugin, self.user_id = plugin, user_id

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        return getattr(interaction.user, "id", None) == self.user_id

    async def _cancelled(self, interaction: discord.Interaction) -> None:
        self.stop()
        await interaction.response.edit_message(embed=JanoEmbed(description="Update cancelled.", color=0x95A5A6), view=None)
        _spawn(_later(5, interaction.delete_original_response))


class _UpgradeView(_AdminOnlyView):
    """Update to release / Update to development / Cancel."""

    def __init__(self, plugin, user_id: int, release: dict | None, dev: dict | None):
        super().__init__(plugin, user_id)
        self.release, self.dev = release, dev
        self.update_release.disabled = release is None
        self.update_dev.disabled = dev is None

    @discord.ui.button(label="Update to release", style=discord.ButtonStyle.success)
    async def update_release(self, interaction: discord.Interaction, button):
        self.stop()
        await interaction.response.edit_message(view=None)
        await self.plugin._upgrade_run(interaction, self.release)

    @discord.ui.button(label="Update to development branch", style=discord.ButtonStyle.danger)
    async def update_dev(self, interaction: discord.Interaction, button):
        self.stop()
        view = _DevWarningView(self.plugin, self.user_id, self.dev)
        await interaction.response.edit_message(embed=_dev_warning_embed(self.dev), view=view)
        view.message = await interaction.original_response()

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
    async def cancel(self, interaction: discord.Interaction, button):
        await self._cancelled(interaction)


class _DevWarningView(_AdminOnlyView):
    """The step before a development-branch update: accept the risk, or cancel."""

    def __init__(self, plugin, user_id: int, dev: dict):
        super().__init__(plugin, user_id)
        self.dev = dev

    @discord.ui.button(label="Accept the risk and update", style=discord.ButtonStyle.danger)
    async def accept(self, interaction: discord.Interaction, button):
        self.stop()
        await interaction.response.edit_message(view=None)
        await self.plugin._upgrade_run(interaction, self.dev)

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
    async def cancel(self, interaction: discord.Interaction, button):
        await self._cancelled(interaction)


class _RestartView(_AdminOnlyView):
    """After a successful update: restart DCSServerBot now, or later."""

    def __init__(self, plugin, user_id: int, text: str):
        super().__init__(plugin, user_id, timeout=600)
        self.text = text

    def _later_embed(self) -> discord.Embed:
        return JanoEmbed(description=f"✅ Ver. {self.text} is installed. It takes effect the next time DCSServerBot restarts.",
                         color=0x2ECC71)

    @discord.ui.button(label="Restart now", style=discord.ButtonStyle.danger)
    async def restart_now(self, interaction: discord.Interaction, button):
        self.stop()
        await interaction.response.edit_message(
            embed=JanoEmbed(description="🔄 Restarting DCSServerBot now... this message updates when it is back.",
                            color=0xF1C40F), view=None)
        self.plugin._save_restart_notice(interaction, self.text)
        await asyncio.sleep(1)
        await self.plugin.bot.node.restart()

    @discord.ui.button(label="Later", style=discord.ButtonStyle.secondary)
    async def later(self, interaction: discord.Interaction, button):
        self.stop()
        await interaction.response.edit_message(embed=self._later_embed(), view=None)
        _spawn(_later(30, interaction.delete_original_response))

    async def on_timeout(self) -> None:
        await self._finish(self._later_embed(), delay=30)


# ══════════════════════════════════════════════════════════════════════════════
# VIEWS — Setup
# ══════════════════════════════════════════════════════════════════════════════

class ViewSetupEmpty(BotView):
    def __init__(self, plugin: Jano):
        super().__init__()
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
        view  = WizardStep5Summary(data, self.guild, self.plugin, mode="settings")
        await interaction.response.send_message(embed=view.current_embed(), view=view, ephemeral=True)
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
            val     = _command_roles_label(self.guild, ri, "❌ Not configured")
            embed.add_field(name=f"Current — {name}", value=val, inline=False)
        await interaction.response.send_message(embed=embed, view=view_roles, ephemeral=True)
        view_roles.message = await interaction.original_response()

    @discord.ui.button(label="New instance", style=discord.ButtonStyle.success, emoji="➕", row=1)
    async def btn_new_instance(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not self.plugin._is_authorized(interaction):
            _ephemeral(interaction, embed=_no_permission("❌ Only admin roles can create instances."))
            return
        if len(self.plugin.states) >= _MAX_INSTANCES:
            await _error(interaction, f"❌ Maximum of {_MAX_INSTANCES} instances reached. Delete one first.")
            return
        await interaction.response.send_modal(WizardStep1Name(self.plugin))

    @discord.ui.button(label="Delete instance", style=discord.ButtonStyle.danger, emoji="🗑️", row=1)
    async def btn_delete_instance(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not self.plugin._is_authorized(interaction):
            _ephemeral(interaction, embed=_no_permission("❌ Only admin roles can delete instances."))
            return
        view  = ViewSelectDelete(self.plugin)
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
    def __init__(self, plugin: Jano):
        super().__init__()
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
            await _error(interaction, "❌ Please select an instance first.")
            return
        is_last = len(self.plugin.states) == 1
        await interaction.response.send_modal(ModalConfirmDelete(self._selected_name, is_last, self.plugin))


class ModalConfirmDelete(discord.ui.Modal, title="⚠️ Confirm deletion"):
    def __init__(self, name: str, is_last: bool, plugin: Jano):
        super().__init__()
        self.name         = name
        self.is_last      = is_last
        self.plugin       = plugin
        self.confirmation = _add_field(self, "Type DELETE to confirm", placeholder="DELETE", required=True, max_length=10)
        title_text   = f"Delete '{name}' — type DELETE"
        if len(title_text) <= 45:
            self.title = title_text

    async def on_submit(self, interaction: discord.Interaction):
        if self.confirmation.value.strip() != "DELETE":
            await _error(interaction, "❌ Incorrect confirmation. Instance was **not** deleted.")
            return
        name = self.name
        if name not in self.plugin.states:
            await _error(interaction, f"❌ Instance '{name}' no longer exists.")
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

_SUMMARY_TEXTS = {
    # mode: (title, description)
    "create":   ("📋 Review — '{name}'",          "Review settings. Press **✅ Create instance** to confirm."),
    "settings": ("⚙️ Instance settings — '{name}'", "Current configuration. Use ✏️ buttons to modify, then **💾 Save changes**."),
    "review":   ("📋 Review changes — '{name}'",   "Review changes. Press **💾 Save changes** to confirm."),
}

def _wizard_summary_embed(data: WizardData, guild: discord.Guild, mode: str) -> discord.Embed:
    """Summary of the wizard data. mode: 'create' (new instance), 'settings' (editing, nothing changed yet) or 'review'."""
    title, description = (text.format(name=data.name) for text in _SUMMARY_TEXTS[mode])
    embed = JanoEmbed(title=title, description=description, color=0x9B59B6)
    cat   = guild.get_channel(data.category_id)
    embed.add_field(name="📦 Category / Channel", value=f"**{cat.name}**" if cat else "❌ Not set", inline=False)
    txt = _channel(guild, data.text_channel_id)
    embed.add_field(name="💬 Text channel",  value=f"**#{txt.name}**" if txt else "*Not configured*", inline=True)
    voice = _channel(guild, data.voice_channel_id)
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

async def _refresh_summary(summary: "WizardStep5Summary"):
    """Redraw the summary message after one of its values changed."""
    summary.mark_changed()
    try:
        await summary.message.edit(embed=summary.current_embed(), view=summary)
    except Exception:
        pass


async def _return_to_summary(view, interaction: discord.Interaction, title: str):
    """After editing one wizard step from the summary: refresh the summary and close this step."""
    await _refresh_summary(view.summary)
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
        self._f_name = _add_field(
            self, "Instance name (required)",
            placeholder="E.g.: Missions, Training, Events...",
            required=True, max_length=32
        )
        self._f_status = _add_field(
            self, "Status Icon — show 🟢🔴 on category? (yes/no)",
            placeholder="yes = show 🟢🔴 on category  |  no = keep original name  |  default: no",
            required=False, max_length=5
        )

    async def on_submit(self, interaction: discord.Interaction):
        name = self._f_name.value.strip()
        if len(self.plugin.states) >= _MAX_INSTANCES:
            await _error(interaction, f"❌ Maximum of {_MAX_INSTANCES} instances reached. Delete one first.")
            return
        if name in self.plugin.states:
            await _error(interaction, f"❌ An instance named **{name}** already exists.")
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
        self.data    = data
        self.guild   = guild
        self.plugin  = plugin
        self.summary = summary

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
        type_sel = _selected(self, "w_sel_type")
        if not type_sel:
            return await interaction.response.defer()
        if type_sel == "category":
            channels_list = _channels_of(self.guild, discord.CategoryChannel)
        else:
            channels_list = _channels_of(self.guild, discord.TextChannel, discord.VoiceChannel)
        options = [discord.SelectOption(label=c.name[:100], value=str(c.id)) for c in channels_list[:25]]
        if not options:
            return await interaction.response.defer()
        label  = "📦 Select category" if type_sel == "category" else "💬 Select channel"
        picker = WizardCategoryPicker(self, options)
        await interaction.response.send_message(embed=JanoEmbed(title=label, color=0x3498DB), view=picker, ephemeral=True)
        picker.message = await interaction.original_response()

    async def _next_callback(self, interaction: discord.Interaction):
        if not self.data.category_id:
            await _error(interaction, "❌ You must select a Category/Channel before continuing.")
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
        self._f_days = _add_field(
            self, "Active days (empty = manual mode only)",
            placeholder="E.g.: 0,1,2,3,4  (0=Mon, 6=Sun)",
            required=False, max_length=20,
            default=",".join(str(d) for d in data.active_days) if data.active_days else ""
        )
        self._f_opening = _add_field(
            self, "Opening time (HH:MM)",
            placeholder="E.g.: 18:15",
            required=False, max_length=5,
            default=data.opening_time if data.active_days else ""
        )
        self._f_closing = _add_field(
            self, "Closing time (HH:MM)",
            placeholder="E.g.: 21:30",
            required=False, max_length=5,
            default=data.closing_time if data.active_days else ""
        )
        self._f_hours = _add_field(
            self, "Max manual hours (0 or empty = no limit)",
            placeholder="E.g.: 2.5",
            required=False, max_length=5,
            default=str(data.max_manual_hours) if data.max_manual_hours > 0 else ""
        )

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
        hours_raw   = self._f_hours.value.strip()
        if days_raw:
            if not opening_raw or not closing_raw:
                await self._send_error(interaction, "Opening and closing times are required when days are specified")
                return
            opening_min, closing_min = _parse_hhmm(opening_raw), _parse_hhmm(closing_raw)
            if opening_min is None or closing_min is None:
                await self._send_error(interaction, "Invalid time format — use HH:MM (e.g. 18:15)")
                return
            if opening_min == closing_min:   # overnight schedules (opening > closing) are allowed
                await self._send_error(interaction, "Opening and closing times cannot be the same")
                return
            new_days = _parse_days(days_raw)
            if new_days is None:
                await self._send_error(interaction, "Invalid days — use numbers 0 (Mon) to 6 (Sun) separated by commas")
                return
            data.active_days, data.opening_time, data.closing_time = new_days, opening_raw, closing_raw
        else:
            data.active_days  = []
            data.opening_time = opening_raw or "19:00"
            data.closing_time = closing_raw or "22:00"
        hours = _parse_hours(hours_raw) if hours_raw else 0.0
        if hours is None:
            await self._send_error(interaction, "Invalid duration — enter a positive number (e.g. 2.5) or 0 for no limit")
            return
        data.max_manual_hours = hours
        if self.summary:
            await _refresh_summary(self.summary)
            await interaction.response.send_message(
                embed=JanoEmbed(
                    title="✅ Schedule updated",
                    description="Review the summary and press the **green button** to confirm.",
                    color=0x2ECC71
                ), ephemeral=True, delete_after=10)
            return
        guild = self.plugin._get_guild()
        view  = WizardStep5Summary(data, guild, self.plugin)
        await interaction.response.send_message(embed=view.current_embed(), view=view, ephemeral=True)
        view.message = await interaction.original_response()


class WizardStep5Summary(BotView):
    def __init__(self, data: WizardData, guild: discord.Guild, plugin: Jano, mode: str = "create"):
        super().__init__()
        self.data   = data
        self.guild  = guild
        self.plugin = plugin
        self.mode   = mode   # 'create' | 'settings' | 'review' (see _wizard_summary_embed)

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

        lbl             = "✅ Create instance" if mode == "create" else "💾 Save changes"
        btn_ok          = discord.ui.Button(label=lbl, style=discord.ButtonStyle.success, row=1)
        btn_ok.callback = self._confirm
        self.add_item(btn_ok)

        btn_cancel          = discord.ui.Button(label="Cancel", style=discord.ButtonStyle.secondary, emoji="❌", row=1)
        btn_cancel.callback = self._cancel
        self.add_item(btn_cancel)

    def mark_changed(self):
        """The first change turns the 'settings' view of an existing instance into a 'review'."""
        if self.mode == "settings":
            self.mode = "review"

    def current_embed(self) -> discord.Embed:
        return _wizard_summary_embed(self.data, self.guild, self.mode)

    async def _update_summary(self, interaction: discord.Interaction):
        self.mark_changed()
        await interaction.response.edit_message(embed=self.current_embed(), view=self)

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
        if self.mode != "create":
            st       = d.st
            cfg      = st.cfg
            old_name = cfg.name
            # Rename in the DB first (and wait for it): saving under the new name before the
            # rename would insert a duplicate row instead of updating the existing one.
            if d.name != old_name and old_name in self.plugin.states:
                if not await self._rename_in_db(old_name, d.name):
                    await _error(interaction, "❌ Could not rename the instance in the database. No changes were made.")
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
                await _error(interaction, f"❌ Cannot create '{d.name}': the instance limit ({_MAX_INSTANCES}) was reached or the name is already taken.")
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
        self._f_name = _add_field(
            self, "Instance name (empty = keep current)",
            placeholder="Leave empty to keep current name",
            required=False,
            max_length=32,
            default=summary.data.name,
        )
        self._f_status = _add_field(
            self, "Status Icon — show 🟢🔴 on category? (yes/no)",
            placeholder="yes = show 🟢🔴  |  no = keep original name  |  empty = keep current",
            required=False,
            max_length=5,
            default="yes" if summary.data.status_icon else "no",
        )

    async def on_submit(self, interaction: discord.Interaction):
        new_name = self._f_name.value.strip()
        # Empty name = keep current
        if not new_name:
            new_name = self.summary.data.name
        if new_name != self.summary.data.name and new_name in self.summary.plugin.states:
            await _error(interaction, f"❌ An instance named **{new_name}** already exists.")
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

    def _selection_label(self, selection: list) -> str:
        """Readable form of a non-None selection."""
        if selection == ["__everyone__"]:
            return "🌐 @everyone"
        if not selection:
            return "❌ None"
        return ", ".join(_role_names(self.guild, selection))

    async def _apply_callback(self, interaction: discord.Interaction):
        lines = []
        for name, selection in self.selections.items():
            if selection is None:
                current = self.plugin.states[name].cfg.command_role_ids_instance
                lines.append(f"**{name}** → *(no change)* {_command_roles_label(self.guild, current, '❌ None')}")
            else:
                lines.append(f"**{name}** → {self._selection_label(selection)}")

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
            if selection == ["__everyone__"]:
                stored = []        # [] = @everyone may use the commands
            elif not selection:
                stored = None      # None = only the global roles
            else:
                stored    = [r for r in selection if self.guild.get_role(r)]
                selection = stored
                if not stored:
                    continue
            st_inst = self.plugin.states[name]
            st_inst.cfg.command_role_ids_instance = stored
            await st_inst.save()
            changes.append(f"**{name}** → {self._selection_label(selection)}")
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
