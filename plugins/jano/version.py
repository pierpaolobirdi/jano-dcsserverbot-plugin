# __version__ is read by DCSServerBot's core as the plugin's DATABASE SCHEMA version
# (core/plugin.py: it expects "major.minor", runs db/update_v<x>.sql for each step and logs
# "migrated to version ..." when it changes). Jano's own migrations run from commands.py at
# startup, so leave it at "1.0"; change it only on purpose, together with a db/update_v*.sql.
__version__ = "1.0"

# The plugin's own release, x.y.z. This is the single source of the version: the footer,
# /jano upgrade, install.cmd and external tools (Nova_Updates) all read it from here.
# DCSServerBot does not look at it. Keep the line in this exact shape: install.cmd and the
# upgrade read it with a pattern.
PLUGIN_RELEASE = "5.1.3"
