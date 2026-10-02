# Jano — DCSServerBot Plugin

A [DCSServerBot](https://github.com/Special-K-s-Flightsim-Bots/DCSServerBot) plugin that manages Discord channel visibility by schedule or manually. Designed for DCS World communities that want to control access to mission/comms channels automatically based on server activity hours.

---

## What it does

Jano manages one or more **instances**, each controlling a Discord category (or channel) that can be:

- **Opened** automatically on a schedule (e.g. Mon–Fri 18:15–22:00)
- **Closed** automatically outside those hours
- **Opened/closed manually** with an optional duration limit; manual mode expires on its own and returns to the schedule
- **Notified** via a text channel announcement (with an optional role mention) when channels open on schedule
- **Renamed** with 🟢/🔴 status icons on the category name (optional)

Overnight schedules are supported (e.g. 23:00–01:00). Up to **4 instances** can be configured. Everything is stored in PostgreSQL and survives restarts, including an active manual mode.

---

## Commands

All commands use the `/jano` prefix:

| Command | Description |
|---|---|
| `/jano setup` | Create, edit and delete instances; set command roles per instance |
| `/jano status` | Show current status, schedule and config of an instance |
| `/jano comms` | Open, close or resume the automatic schedule |

All commands take an optional `instance` argument (auto-selected when only one exists).

### `/jano comms`
- `action` is chosen from a dropdown: `open`, `close` or `resume`
- **open** — a modal asks for the duration in hours. Leave it empty or enter `0` to use the instance's maximum manual duration (or no limit if none is set). A value above the maximum is trimmed and you are told about it. Opening an instance that is already open in manual mode only updates the duration.
- **close** — closes the channels in manual mode. Buttons let you keep manual mode or resume the schedule right away.
- **resume** — leaves manual mode and goes back to the configured schedule.

### `/jano status`
Shows whether the channels are open, the mode (schedule or manual), the schedule and active days, the maximum manual duration, the remaining manual time, the configured category/channels/roles and who can use the commands.

### `/jano setup`
Opens the setup menu of an instance:

| Button | Action |
|---|---|
| ✏️ Edit instance | Review and change name/icon, channels, roles, schedule and limit, then **Save changes** |
| 🔑 Command roles | Choose which roles can use the commands of each instance |
| ➕ New instance | Start the creation wizard (maximum 4 instances) |
| 🗑️ Delete instance | Delete an instance after typing `DELETE`; its opening announcement is removed too |

With no instances yet, `/jano setup` offers a single **New instance** button. Only users with a global command role can use `/jano setup`.

---

## Requirements

- [DCSServerBot](https://github.com/Special-K-s-Flightsim-Bots/DCSServerBot) v4.x
- Python 3.11+
- PostgreSQL 14+
- discord.py 2.6+ (included with DCSServerBot) — modals use `discord.ui.Label`
- `tzdata` — Windows timezone data (installed automatically by the installer)

---

## Installation

### Option 1 — Automatic installer (recommended)

1. Download the latest release zip from the [Releases](../../releases) page
2. Extract it anywhere on your PC
3. Double-click **`install.cmd`**
4. The installer will:
   - Detect your DCSServerBot installation automatically
   - Install `tzdata` (Windows timezone data) into the DCSServerBot Python environment
   - Add `tzdata` to `requirements.local` so it is reinstalled automatically on every DCSServerBot update
   - Copy all plugin files to the correct locations
   - Preserve your existing `jano.yaml` if it already exists
   - Warn you if `jano` is missing from `main.yaml`

### Option 2 — Manual installation

#### 1. Install tzdata

Jano uses Python's built-in `zoneinfo` for timezone handling. On Windows, timezone data must be installed separately:

```cmd
%USERPROFILE%\.dcssb\Scripts\pip install tzdata
```

To ensure `tzdata` is reinstalled automatically on every DCSServerBot update, add it to `requirements.local` in the root of your DCSServerBot installation:

```
tzdata
```

#### 2. Copy plugin files

Copy the `plugins/jano/` folder to your DCSServerBot plugins directory:

```
DCSServerBot/
└── plugins/
    └── jano/
        ├── __init__.py
        ├── commands.py
        ├── listener.py
        ├── version.py
        └── db/
            └── tables.sql
```

#### 3. Copy configuration file

Copy `config/plugins/jano.yaml` to your DCSServerBot config directory:

```
DCSServerBot/
└── config/
    └── plugins/
        └── jano.yaml
```

#### 4. Enable the plugin

Add `jano` to `opt_plugins` in your `config/main.yaml`:

```yaml
opt_plugins:
  - jano
```

#### 5. Restart DCSServerBot

On first startup, Jano will automatically create the required database tables and register the `/jano` slash commands with Discord.

---

## Configuration

Edit `config/plugins/jano.yaml`:

```yaml
DEFAULT:
  command_role_ids:
    - Admin           # Role name (as defined in DCSServerBot roles)
    - 123456789012    # Or numeric Discord role ID
  timezone: "Europe/Madrid"   # IANA timezone for schedule calculations
```

### Options

| Option | Description | Default |
|---|---|---|
| `command_role_ids` | Roles allowed to use Jano commands on every instance (and the only ones allowed to use `/jano setup`) | `[]` (all users) |
| `timezone` | IANA timezone name for schedule calculations | `Europe/Madrid` |
| `open_message.title` | Title of the schedule-open announcement embed. `{name}` and `{voice}` are replaced at runtime. | Built-in English text |
| `open_message.body` | Body of the schedule-open announcement embed. Same placeholders available. | Built-in English text |

Full list of timezone names: https://en.wikipedia.org/wiki/List_of_tz_database_time_zones

---

## Who can use the commands

- **Global roles** — `command_role_ids` in `jano.yaml`. These roles can use every command on every instance, including `/jano setup`.
- **Instance roles** — set per instance in `/jano setup` → **Command roles**. Each instance can be set to:
  - **None** — only the global roles can use its commands (default)
  - **Specific roles** — the global roles plus those roles
  - **@everyone** — anyone can use `/jano status` and `/jano comms` for that instance
- If no global role is configured and an instance has no roles of its own, everyone can use the commands. This is not recommended.

`/jano setup` always requires a global role, whatever the instance roles are.

---

## Setting up an instance

Run `/jano setup` and follow the wizard:

1. **Name & Status Icon** — give the instance a name and choose whether to show 🟢/🔴 on the category name
2. **Channels** — select the Discord category to open/close, and optionally a text/voice channel for announcements
3. **Roles** — set the visibility role (who gains/loses access) and the mention role (who gets pinged on open)
4. **Schedule & Limit** — configure active days (0=Mon to 6=Sun), opening/closing times (HH:MM), and max manual duration
5. **Review** — confirm and create the instance

When an instance opens **on schedule**, Jano posts an announcement in the text channel (pinging the mention role, if any) and deletes it when the instance closes. Announcements are not posted for manual openings.

If you change the category of an existing instance, Jano removes its 🟢/🔴 markers from the name of the previous category. Permissions of the previous category are left untouched.

---

## Database

Jano uses two PostgreSQL tables, both created automatically on first startup:

- `jano_instances` — instance configuration (channels, roles, schedule)
- `jano_state` — runtime state (open/closed, manual overrides, message IDs)

The schema lives in `plugins/jano/db/tables.sql` and is applied on startup. Migrations run automatically on each startup and are safe to repeat; installations from v1.x have their old Spanish column names renamed to the current English ones.

The global command roles come only from `command_role_ids` in `jano.yaml`. Installations created with an older version may still have an unused `jano_global` table; it can be dropped with `DROP TABLE jano_global;`.

---

## Upgrading to 5.0.0

Copy the new `plugins/jano/` files over the old ones and restart DCSServerBot. `jano.yaml` and the database need no manual changes. Things to know:

- **discord.py 2.6+ is required** (modals use `discord.ui.Label`). Recent DCSServerBot versions already include it.
- **`@everyone` as instance command role now really lets everyone use the commands.** Before, it was shown but ignored whenever global roles were configured, and it was lost after a restart. Check `/jano status` for instances that show "🌐 @everyone".
- **Global command roles are read from `jano.yaml` only.** The `jano_global` table is no longer used.
- Old schedule/max-hours overrides stored in the database are ignored (a warning is logged if any is found).
- Embeds now carry a footer with the plugin version.

---

## License

MIT

---