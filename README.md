# Jano — DCSServerBot Plugin

A [DCSServerBot](https://github.com/Special-K-s-Flightsim-Bots/DCSServerBot) plugin that manages Discord channel visibility by schedule or manually. Designed for DCS World communities that want to control access to mission/comms channels automatically based on server activity hours.

---

## What it does

Jano manages one or more **instances**, each controlling a Discord category (or channel) that can be:

- **Opened** automatically on a schedule (e.g. Mon–Fri 18:15–22:00)
- **Closed** automatically outside those hours
- **Opened/closed manually** with an optional duration limit
- **Notified** via a text channel announcement when channels open
- **Renamed** with 🟢/🔴 status icons on the category name (optional)

Overnight schedules are supported (e.g. 23:00–01:00).

---

## Commands

All commands use the `/jano` prefix:

| Command | Description |
|---|---|
| `/jano setup` | Create or edit instances (wizard-based) |
| `/jano status` | Show current status, schedule and config of an instance |
| `/jano comms` | Open, close or resume automatic schedule |

### `/jano comms` parameters
- `instance` — which instance to act on (auto-selected if only one exists)
- `action` — `open`, `close`, or `resume` (returns to automatic schedule)
- When `open` is selected, a modal appears asking for duration in hours

---

## Requirements

- [DCSServerBot](https://github.com/Special-K-s-Flightsim-Bots/DCSServerBot) v3.x
- Python 3.11+
- PostgreSQL 14+
- discord.py 2.x (included with DCSServerBot)
- **pytz** (must be installed separately — see below)

---

## Installation

### 1. Install pytz

Jano requires the `pytz` library for timezone handling. Install it into the DCSServerBot Python environment:

```cmd
%USERPROFILE%\.dcssb\Scripts\pip install pytz
```

### 2. Copy plugin files

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

### 3. Copy configuration file

Copy `config/plugins/jano.yaml` to your DCSServerBot config directory:

```
DCSServerBot/
└── config/
    └── plugins/
        └── jano.yaml
```

### 4. Enable the plugin

Add `jano` to `opt_plugins` in your `config/main.yaml`:

```yaml
opt_plugins:
  - jano
```

### 5. Start the bot

On first startup, Jano will automatically:
- Create the required PostgreSQL tables (`jano_instances`, `jano_state`, `jano_global`)
- Apply any pending database migrations
- Register the `/jano` slash commands with Discord

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
| `command_role_ids` | Roles allowed to use Jano commands globally | `[]` (all users) |
| `timezone` | IANA timezone name for schedule calculations | `Europe/Madrid` |

Full list of timezone names: https://en.wikipedia.org/wiki/List_of_tz_database_time_zones

---

## Setting up an instance

Run `/jano setup` and follow the wizard:

1. **Name & Status Icon** — give the instance a name and choose whether to show 🟢/🔴 on the category name
2. **Channels** — select the Discord category to open/close, and optionally a text/voice channel for announcements
3. **Roles** — set the visibility role (who gains/loses access) and the mention role (who gets pinged on open)
4. **Schedule & Limit** — configure active days (0=Mon to 6=Sun), opening/closing times (HH:MM), and max manual duration
5. **Review** — confirm and create the instance

---

## Database

Jano uses three PostgreSQL tables, all created automatically on first startup:

- `jano_instances` — instance configuration (channels, roles, schedule)
- `jano_state` — runtime state (open/closed, manual overrides, message IDs)
- `jano_global` — global command role IDs

Database migrations are applied automatically on each startup — safe to run repeatedly.

---

## License

MIT
