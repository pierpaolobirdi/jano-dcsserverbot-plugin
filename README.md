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
| `/jano upgrade` | Admins only: update Jano from GitHub, from Discord |

`/jano setup`, `/jano status` and `/jano comms` take an optional `instance` argument (auto-selected when only one exists).

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

### `/jano upgrade`
Checks GitHub for a newer version of Jano and offers to install it. It needs the `Admin` role of DCSServerBot, replies privately, and only the admin who ran it can press the buttons. It is not an automatic updater: nothing happens unless an admin runs the command and confirms.

1. The card's colour tells how much care the choice needs: **green** when only stable versions are offered, **orange** for a pre-release or when one of the sources could not be checked (the reason is written on the card), **red** whenever the development branch can be chosen. The reply shows the installed version and what is newer, with the release notes. There are three sources: the latest **release**, the **main branch** and the **development branch**. A source is offered only when its version is higher than the installed one and higher than the more stable sources above it (release, then main, then development), so you never see a branch that offers nothing beyond the release. A **pre-release** is marked with a warning. Running the command and pressing **Cancel** is the way to just check.
2. Buttons: **Update to release**, **Update to main branch**, **Update to development branch**, **Cancel** (or let it expire after two minutes). A button is disabled when there is nothing newer on that side.
3. The development branch is work in progress and may contain errors, so choosing it first shows a **warning of the risk**, and nothing is installed until you press **Accept the risk and update**. Release and main branch are installed directly after pressing their button.
4. When the update finishes you are told it worked and asked whether to **restart DCSServerBot now** or **later**. If you restart, the message updates by itself once DCSServerBot is back, confirming which version is running, and then disappears. The new version only takes effect after a restart: choosing *Later* leaves it installed and active at the next restart.

The offer and the development-branch warning are cards; everything after that (progress, result, errors, the restart question) is plain text. Messages remove themselves so the channel stays clean: 5 s after *Cancel*, 10 s after an offer expires or when you are already up to date, 20 s after the post-restart confirmation, after *Later* or when the restart question expires, and 30 s after an error or when GitHub could not be reached. The *Restarting...* message stays until the bot is back.

Safety:
- It downloads only from this plugin's GitHub repository, over HTTPS (a release's zip, or the zip of the main or development branch).
- Only the plugin's own files are taken from that zip (`__init__.py`, `commands.py`, `listener.py`, `version.py`, `db/tables.sql` and the web console schema `schemas/jano_schema.yaml`). Each one is checked before anything is replaced: it must compile, the schema must be Jano's, and the version inside (`PLUGIN_RELEASE` in `version.py`) must be the one announced.
- The previous files are kept in `plugins/jano/.backup` (with a `RESTORE.txt` that says how to go back), and if anything fails halfway they are put back and nothing restarts.
- If the new version installs but does not work (the bot or the plugin fails to start), stop DCSServerBot, copy the files from `plugins/jano/.backup` over the ones in `plugins/jano` (all except `RESTORE.txt`, keeping the `db` and `schemas` subfolders) and start it again. `.backup` holds only the plugin files, not `install.cmd`; to use that, download a release from GitHub.
- Your `jano.yaml` and your database are not touched by the update itself. A changed database schema is applied by Jano when it starts, as always.
- If DCSServerBot is configured with `restrict_commands: true`, `/jano upgrade` is blocked like the rest of its administration commands.
- The restart is the same one as DCSServerBot's own `/node restart` on the master node. It relies on how you start the bot: launched with `run.cmd` (or `run.sh`, or a service that starts it again) it comes back by itself; launched any other way it would only stop.
- The bot needs outbound HTTPS access to `api.github.com`.

`install.cmd` stays the way to do the first installation.

---

## Requirements

- [DCSServerBot](https://github.com/Special-K-s-Flightsim-Bots/DCSServerBot) v3.x
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
   - Show the version being installed and the one already installed (if any)
   - Install `tzdata` (Windows timezone data) into the DCSServerBot Python environment
   - Add `tzdata` to `requirements.local` so it is reinstalled automatically on every DCSServerBot update
   - Copy all plugin files to the correct locations
   - Preserve your existing `jano.yaml` if it already exists, adding only the variables the new version needs (your values and comments stay; the previous file is kept as `jano.yaml.bak` when something changes). This step needs Python, the one of DCSServerBot or the system's
   - Warn you if `jano` is missing from `main.yaml`
   - Tell you when DCSServerBot has to be restarted to load the new version

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
        ├── version.py          (PLUGIN_RELEASE = the release; __version__ = DB schema, stays 1.0)
        ├── db/
        │   └── tables.sql
        └── schemas/
            └── jano_schema.yaml
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

### Settings in the DCSServerBot web console

The plugin ships a schema (`plugins/jano/schemas/jano_schema.yaml`), so the web console's **Settings** page builds a form for `jano.yaml` (the command roles, the timezone and the opening announcement). DCSServerBot also uses the same schema to check the file when it loads. Notes:

- Saves apply after a restart of the bot, like any other plugin setting.
- A new variable is added to the schema together with the plugin. `/jano upgrade` and `install.cmd` install the schema file with the rest, and `migrate_config.py` (which both run) puts it in place too, so it also arrives when the update is made by an older version of the command.
- If you copy the plugin files by hand, copy the `schemas` folder as well, or run `migrate_config.py` on your `jano.yaml`: without the schema the settings form is shown read-only.

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

## Updating

Run `/jano upgrade` from Discord (see above), or run `install.cmd` from the new release: it shows the installed and the new version, replaces the plugin files, keeps your `jano.yaml` and tells you when a restart is needed. Either way, restart DCSServerBot afterwards.

Both also run `migrate_config.py` on your `jano.yaml`: it adds any variable the new version needs (with its default value), renames or removes variables that changed, and leaves everything else exactly as you wrote it. If it changes the file, the previous one is kept as `jano.yaml.bak`. You can run it by hand with `python migrate_config.py path\to\jano.yaml`.

### Changes in 5.0.0

Things to know when coming from an older version (`jano.yaml` and the database need no manual changes):

- **discord.py 2.6+ is required** (modals use `discord.ui.Label`). Recent DCSServerBot versions already include it.
- **`@everyone` as instance command role now really lets everyone use the commands.** Before, it was shown but ignored whenever global roles were configured, and it was lost after a restart. Check `/jano status` for instances that show "🌐 @everyone".
- **Global command roles are read from `jano.yaml` only.** The `jano_global` table is no longer used.
- Old schedule/max-hours overrides stored in the database are ignored (a warning is logged if any is found).
- Embeds now carry a footer with the plugin version.

---

## Versioning (developers)

The release number is `PLUGIN_RELEASE` in `plugins/jano/version.py`, and nowhere else: the embed footer, `/jano upgrade`, `install.cmd` and external tools read it from there. Bump it with every release, keeping the line exactly as it is (`PLUGIN_RELEASE = "x.y.z"`).

`__version__`, in the same file, is **not** the release. DCSServerBot's core reads it as the plugin's database-schema version (it expects `major.minor`, runs `db/update_v<x>.sql` for each step and logs `migrated to version ...` whenever it changes). Jano's own migrations run from `commands.py` at startup, so it stays at `1.0`.

`commands.py` still carries one `COMMANDS_VERSION = "x.y.z"` line. It is a bridge for installations older than 5.1.1, whose `/jano upgrade` accepts a downloaded release only if it finds that line. The code does not use it and a test keeps it equal to `PLUGIN_RELEASE` (so bump both). **Keep it in every release**: without it an installation older than 5.1.1 refuses to jump straight to the new release. For the same reason a file that did not exist in 5.1.1 is not copied by those older upgrades: every file under `plugins/jano/` must be in the list of that version (a test checks it) or be put in place by `migrate_config.py`, which the older upgrade does run from the new release.

The tests need `pip install pytest discord.py "psycopg[binary]" aiohttp pyyaml` and run with `python -m pytest tests`.

---

## License

MIT

---