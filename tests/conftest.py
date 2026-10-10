"""Test setup: Jano runs against small stubs of DCSServerBot (tests/stubs) and the real discord.py,
so no bot, Discord or database is needed.

    pip install pytest discord.py psycopg[binary] aiohttp
    python -m pytest tests
"""
import asyncio
import logging
import os
import sys
import types

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path[:0] = [os.path.join(ROOT, "tests", "stubs"), os.path.join(ROOT, "plugins")]

from jano import commands  # noqa: E402


def make_plugin(plugin_dir=None, **attrs):
    """A Jano plugin without running its constructor, with a fake bot that records restarts."""
    p = commands.Jano.__new__(commands.Jano)
    p.log = logging.getLogger("jano.tests")
    p.states, p.command_role_ids_global, p._upgrading = {}, [], False
    p.restarts = []

    async def restart():
        p.restarts.append(True)
    p.bot = types.SimpleNamespace(node=types.SimpleNamespace(restart=restart))
    for k, v in attrs.items():
        setattr(p, k, v)
    if plugin_dir is not None:
        p._plugin_dir = lambda: str(plugin_dir)
    return p


class Sleeps:
    """Records the delays asked for clean-ups instead of waiting for them."""

    def __init__(self):
        self.later, self.deleted, self.tasks = [], [], []

    async def drain(self):
        """Let the background tasks started by the code under test finish."""
        await asyncio.gather(*self.tasks)


@pytest.fixture
def sleeps(monkeypatch):
    rec = Sleeps()
    real_spawn = commands._spawn

    async def later(delay, action):
        rec.later.append(delay)

    async def delete_after(message, delay=120):
        rec.deleted.append(delay)

    async def no_sleep(_delay):
        return None

    def spawn(coro):
        task = real_spawn(coro)
        rec.tasks.append(task)
        return task
    monkeypatch.setattr(commands, "_later", later)
    monkeypatch.setattr(commands, "_delete_after", delete_after)
    monkeypatch.setattr(commands, "_spawn", spawn)
    monkeypatch.setattr(commands.asyncio, "sleep", no_sleep)
    return rec
