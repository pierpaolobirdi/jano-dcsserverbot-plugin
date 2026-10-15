"""The announcement posted when an instance opens on schedule."""
import types
from zoneinfo import ZoneInfo

import pytest

from conftest import commands, make_plugin


def _st(days, opening="19:15", closing="22:00", name="Misiones", voice=123):
    cfg = types.SimpleNamespace(name=name, voice_channel_id=voice, active_days=days, opening_time=opening,
                                closing_time=closing, tz=ZoneInfo("Europe/Madrid"))
    return types.SimpleNamespace(cfg=cfg)


@pytest.mark.parametrize("days,label", [
    ([0, 1, 2, 3, 4], "Mon–Fri"), ([4, 5], "Fri, Sat"), ([5], "Sat"), ([0, 2, 5], "Mon, Wed, Sat"),
    ([0, 1, 2, 5, 6], "Mon–Wed, Sat, Sun"), ([6, 0, 1], "Mon, Tue, Sun"), ([], ""), (None, "")])
def test_days_label(days, label):
    assert commands._days_label(days) == label


def test_default_announcement_shows_the_event_schedule():
    p = make_plugin()
    p._open_message_template = {}
    embed = p._announcement_embed(_st([0, 1, 2, 3, 4]))
    assert "🕒 **Event schedule:** Mon–Fri · 19:15 – 22:00 (Europe/Madrid)" in embed.description
    assert embed.description.index("Event schedule") < embed.description.index("Please connect")


def test_no_schedule_line_when_the_instance_has_no_schedule():
    p = make_plugin()
    p._open_message_template = {}
    assert "Event schedule" not in p._announcement_embed(_st([])).description


def test_custom_templates_can_use_the_schedule_placeholders():
    p = make_plugin()
    p._open_message_template = {"title": "{name}", "body": "{days} {opening}-{closing} | {schedule} | {voice}"}
    embed = p._announcement_embed(_st([0, 1]))
    assert embed.description == "Mon, Tue 19:15-22:00 | Mon, Tue · 19:15 – 22:00 (Europe/Madrid) | <#123>"
    p._open_message_template = {"body": "[{schedule}][{opening}]"}
    assert p._announcement_embed(_st([])).description == "[][]"
