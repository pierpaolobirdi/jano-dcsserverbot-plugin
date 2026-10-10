"""Minimal stand-in for DCSServerBot's core package (the real discord.py is used)."""
import logging

from discord import app_commands
from discord.ext import commands


class Plugin(commands.Cog):
    def __init__(self, bot, eventlistener=None):
        self.bot, self.locals, self.log = bot, {}, logging.getLogger("jano.tests")


class TEventListener:
    pass


class Group(app_commands.Group):
    """DCSServerBot's Group only adds behaviour to command(); a plain Group is enough here."""


class utils:
    @staticmethod
    def app_has_role(role):
        return lambda f: f

    @staticmethod
    def restricted_check(interaction):
        return True
