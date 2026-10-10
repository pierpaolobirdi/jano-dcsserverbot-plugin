"""/jano upgrade: release choice, zip checks, install with rollback, the flow and the restart notice."""
import asyncio
import io
import json
import os
import sys
import time
import types
import zipfile

import pytest

from conftest import commands, make_plugin

TOP = "pierpaolobirdi-jano-dcsserverbot-plugin-abc1234/"      # GitHub's zip puts everything in one folder
SCHEMA = "CREATE TABLE IF NOT EXISTS jano_instances (name TEXT);\n"


def _zip(version="9.9.9", drop=(), extra=None, commands_src=None, tables=SCHEMA, top=TOP):
    files = {n: "x = 1\n" for n in commands.UPGRADE_FILES}
    files["plugins/jano/commands.py"] = commands_src or f'COMMANDS_VERSION = "{version}"\n'
    files["plugins/jano/db/tables.sql"] = tables
    files["README.md"] = "the rest of the repository\n"
    files["tests/test_x.py"] = "def test(): pass\n"
    files.update(extra or {})
    for n in drop:
        files.pop(n)
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        for n, c in files.items():
            z.writestr(top + n, c)
    return buf.getvalue()


def _rel(tag, pre=False, draft=False, zipball=True):
    return {"tag_name": tag, "prerelease": pre, "draft": draft, "body": f"notes {tag}",
            "published_at": "2026-10-10T00:00:00Z",
            "zipball_url": f"https://api.github.com/repos/x/zipball/{tag}" if zipball else None}


# ── choosing the release ─────────────────────────────────────────────────────────

def test_version_is_read_from_the_tag():
    assert commands._release_version("v.5.0.2") == (5, 0, 2) and commands._release_version("14.10.0") == (14, 10, 0)
    assert commands._release_version("latest") is None


def test_the_newest_release_above_the_installed_one_is_chosen():
    cur = (5, 0, 3)
    rels = [_rel("v.5.0.3"), _rel("v.5.1.0"), _rel("v.5.10.0"), _rel("v.5.6.0")]
    assert commands._pick_release(rels, cur)["text"] == "5.10.0"             # 5.10 is above 5.6 (not a string compare)
    assert commands._pick_release([_rel("v.5.0.3"), _rel("v.5.0.1")], cur) is None   # equal or older: nothing


def test_drafts_and_releases_without_a_zip_are_skipped():
    cur = (5, 0, 3)
    assert commands._pick_release([_rel("v.9.0.0", draft=True)], cur) is None
    assert commands._pick_release([_rel("v.9.0.0", zipball=False)], cur) is None
    assert commands._pick_release([_rel("v.9.0.0")], cur)["zip_url"].endswith("/zipball/v.9.0.0")


def test_a_prerelease_is_offered_and_flagged():
    got = commands._pick_release([_rel("v.9.0.0", pre=True), _rel("v.8.0.0")], (5, 0, 3))
    assert got["text"] == "9.0.0" and got["prerelease"] is True


# ── the zip ───────────────────────────────────────────────────────────────────────

def test_only_the_expected_files_are_taken_from_the_whole_repository_zip():
    files = commands._read_release_zip(_zip("9.9.9"), "9.9.9")
    assert set(files) == set(commands.UPGRADE_FILES)
    assert commands._zip_version(files) == "9.9.9"


@pytest.mark.parametrize("data, why", [
    (b"not a zip", "valid zip"),
    (_zip(drop=["plugins/jano/listener.py"]), "does not contain plugins/jano/listener.py"),
    (_zip(drop=["plugins/jano/db/tables.sql"]), "does not contain plugins/jano/db/tables.sql"),
    (_zip(extra={"plugins/jano/listener.py": "def broken(:\n"}), "does not compile"),
    (_zip(tables="SELECT 1;\n"), "not the Jano schema"),
    (_zip(commands_src="x = 1\n"), "does not match its tag"),
    (_zip("1.2.3"), "does not match its tag"),                                 # the zip announces 1.2.3, tag says 9.9.9
])
def test_a_zip_that_is_wrong_is_refused(data, why):
    with pytest.raises(commands.UpgradeError, match=why):
        commands._read_release_zip(data, "9.9.9")


def test_two_top_folders_are_refused():
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("a/x", "1")
        z.writestr("b/y", "2")
    with pytest.raises(commands.UpgradeError, match="expected layout"):
        commands._read_release_zip(buf.getvalue())


# ── installing ─────────────────────────────────────────────────────────────────────

def _release_files(version="9.9.9", **kw):
    return commands._read_release_zip(_zip(version, **kw))


def test_install_replaces_the_files_keeps_a_backup_and_leaves_other_files(tmp_path):
    (tmp_path / "commands.py").write_text("old commands")
    (tmp_path / "listener.py").write_text("x = 1\n")           # identical to the release's: left alone
    (tmp_path / "mine.py").write_text("my own file")
    before = os.path.getmtime(tmp_path / "listener.py")
    changed = commands._install_release_files(str(tmp_path), _release_files())
    assert "commands.py" in changed and "listener.py" not in changed
    assert "db/tables.sql" in changed and (tmp_path / "db" / "tables.sql").read_text() == SCHEMA   # new sub-folder
    assert (tmp_path / "commands.py").read_text().startswith('COMMANDS_VERSION = "9.9.9"')
    assert (tmp_path / ".backup" / "commands.py").read_text() == "old commands"
    assert (tmp_path / "mine.py").read_text() == "my own file"
    assert os.path.getmtime(tmp_path / "listener.py") == before
    assert not list(tmp_path.rglob("*.new"))


def test_a_second_install_replaces_the_backup_with_the_state_before_it(tmp_path):
    commands._install_release_files(str(tmp_path), _release_files("9.9.9"))
    commands._install_release_files(str(tmp_path), _release_files("9.9.10"))
    assert (tmp_path / ".backup" / "commands.py").read_text() == 'COMMANDS_VERSION = "9.9.9"\n'


def test_installing_the_same_files_changes_nothing(tmp_path):
    commands._install_release_files(str(tmp_path), _release_files())
    assert commands._install_release_files(str(tmp_path), _release_files()) == []


def test_a_failure_halfway_puts_the_old_files_back(tmp_path, monkeypatch):
    (tmp_path / "__init__.py").write_text("old init")
    (tmp_path / "commands.py").write_text("old commands")
    real, calls = os.replace, []

    def failing(src, dst):
        calls.append(dst)
        if len(calls) == 3:
            raise OSError("disk full")
        return real(src, dst)
    monkeypatch.setattr(commands.os, "replace", failing)
    with pytest.raises(OSError):
        commands._install_release_files(str(tmp_path), _release_files())
    assert (tmp_path / "__init__.py").read_text() == "old init"
    assert (tmp_path / "commands.py").read_text() == "old commands"
    assert not list(tmp_path.rglob("*.new"))
    assert not (tmp_path / "version.py").exists()               # files that did not exist before are removed again


# ── downloading ────────────────────────────────────────────────────────────────────

class _Resp:
    def __init__(self, status=200, chunks=(b"",), scheme="https"):
        self.status, self._chunks, self.url = status, chunks, types.SimpleNamespace(scheme=scheme)
        self.content = self

    async def iter_chunked(self, _n):
        for c in self._chunks:
            yield c

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False


class _Session:
    resp = None

    def __init__(self, **kw):
        pass

    def get(self, url):
        return _Session.resp

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False


@pytest.fixture
def fake_http(monkeypatch):
    monkeypatch.setitem(sys.modules, "aiohttp", types.SimpleNamespace(ClientTimeout=lambda total: None, ClientSession=_Session))

    def answer(resp):
        _Session.resp = resp
    return answer


def test_a_body_that_arrives_in_pieces_is_read_whole(fake_http):
    fake_http(_Resp(chunks=(b'{"a":', b' 1}')))
    assert asyncio.run(make_plugin()._http_get("https://api.github.com/x", as_json=True)) == {"a": 1}


def test_the_download_is_refused_when_too_big_not_https_or_not_200(fake_http, monkeypatch):
    p = make_plugin()
    fake_http(_Resp(status=404))
    with pytest.raises(commands.UpgradeError, match="404"):
        asyncio.run(p._http_get("https://api.github.com/x"))
    fake_http(_Resp(scheme="http"))
    with pytest.raises(commands.UpgradeError, match="left HTTPS"):
        asyncio.run(p._http_get("https://api.github.com/x"))
    with pytest.raises(commands.UpgradeError, match="non-HTTPS"):
        asyncio.run(p._http_get("http://api.github.com/x"))
    monkeypatch.setattr(commands, "UPGRADE_MAX_BYTES", 10)
    fake_http(_Resp(chunks=(b"123456", b"123456")))
    with pytest.raises(commands.UpgradeError, match="larger than expected"):
        asyncio.run(p._http_get("https://api.github.com/x"))


# ── the flow, with a fake Discord interaction ───────────────────────────────────────

class _Sent:
    id = 777

    async def edit(self, **kw):
        pass

    async def delete(self):
        pass


class _Response:
    def __init__(self):
        self.deferred, self.edits = False, []

    async def defer(self, ephemeral=False):
        self.deferred = True

    async def edit_message(self, **kw):
        self.edits.append(kw)


class _Followup:
    def __init__(self):
        self.sent = []

    async def send(self, content=None, *, embed=None, view=None, ephemeral=False, wait=False):
        self.sent.append({"content": content, "embed": embed, "view": view})
        return _Sent()


class _Interaction:
    def __init__(self, user_id=1):
        self.user = types.SimpleNamespace(id=user_id)
        self.response, self.followup = _Response(), _Followup()
        self.message, self.application_id, self.token = types.SimpleNamespace(id=555), 42, "tok"
        self.original, self.deleted = [], 0

    async def edit_original_response(self, **kw):
        self.original.append(kw)

    async def original_response(self):
        return _Sent()

    async def delete_original_response(self):
        self.deleted += 1


def _plugin(tmp_path, monkeypatch, *, release=None, main=None, dev=None, downloads=None):
    """A plugin whose GitHub lookups and downloads are answered from memory."""
    p = make_plugin(tmp_path)
    p.downloads = []
    answers = {"release": release, "main": main, "dev": dev}

    def answer(source):
        if isinstance(answers[source], Exception):
            raise answers[source]
        return answers[source]

    async def lookup():
        return answer("release")

    async def lookup_branch(branch):
        return answer(branch)

    async def http_get(url, as_json=False):
        p.downloads.append(url)
        return (downloads or {})[url]
    p._upgrade_lookup, p._upgrade_lookup_branch, p._http_get = lookup, lookup_branch, http_get
    return p


def _offer(version="9.9.9", pre=False):
    return {"version": commands._release_version(version), "text": version, "tag": f"v.{version}", "prerelease": pre,
            "notes": "the notes", "published": "", "zip_url": f"https://api.github.com/zip/{version}"}


def _dev_offer(version="9.9.9", data=None):
    return {"channel": "dev", "version": commands._release_version(version), "text": version, "tag": "dev",
            "prerelease": True, "notes": "", "data": data if data is not None else _zip(version)}


def test_up_to_date_says_so(tmp_path, monkeypatch, sleeps):
    p, it = _plugin(tmp_path, monkeypatch), _Interaction()

    async def go():
        await commands.Jano.jano_upgrade.callback(p, it)
        await sleeps.drain()
    asyncio.run(go())
    assert it.response.deferred and len(it.followup.sent) == 1
    assert "newest version" in it.followup.sent[0]["embed"].description and it.followup.sent[0]["view"] is None
    assert sleeps.deleted == [10]


def test_a_newer_release_and_a_newer_dev_give_two_buttons_for_the_admin_only(tmp_path, monkeypatch, sleeps):
    p = _plugin(tmp_path, monkeypatch, release=_offer("9.9.9"), dev=_dev_offer("9.9.10"))
    it = _Interaction(user_id=1)

    async def go():
        await commands.Jano.jano_upgrade.callback(p, it)
        view = it.followup.sent[0]["view"]
        assert not view.update_release.disabled and not view.update_dev.disabled
        assert await view.interaction_check(_Interaction(user_id=1))
        assert not await view.interaction_check(_Interaction(user_id=2))
    asyncio.run(go())
    fields = {f.name: f.value for f in it.followup.sent[0]["embed"].fields}
    assert fields["Release"] == "Ver. 9.9.9" and fields["Development branch"].startswith("Ver. 9.9.10")


def test_only_what_is_newer_gets_an_enabled_button(tmp_path, monkeypatch, sleeps):
    async def buttons(release, dev):
        p, it = _plugin(tmp_path, monkeypatch, release=release, dev=dev), _Interaction()
        await commands.Jano.jano_upgrade.callback(p, it)
        view = it.followup.sent[0]["view"]
        return view.update_release.disabled, view.update_dev.disabled
    assert asyncio.run(buttons(_offer(), None)) == (False, True)
    assert asyncio.run(buttons(None, _dev_offer())) == (True, False)


def test_dev_is_offered_only_when_it_is_above_the_release_too(tmp_path, monkeypatch, sleeps):
    p, it = _plugin(tmp_path, monkeypatch, release=_offer("9.9.9"), dev=_dev_offer("9.9.9")), _Interaction()
    asyncio.run(commands.Jano.jano_upgrade.callback(p, it))
    assert it.followup.sent[0]["view"].update_dev.disabled


def test_a_broken_dev_branch_is_reported_but_the_release_stays_available(tmp_path, monkeypatch, sleeps):
    p = _plugin(tmp_path, monkeypatch, release=_offer(), dev=commands.UpgradeError("db/tables.sql is not the Jano schema."))
    it = _Interaction()
    asyncio.run(commands.Jano.jano_upgrade.callback(p, it))
    sent = it.followup.sent[0]
    assert not sent["view"].update_release.disabled and sent["view"].update_dev.disabled
    assert any("Could not check the development branch" in f.value for f in sent["embed"].fields)


def test_nothing_reachable_is_an_error_message_that_goes_after_30_seconds(tmp_path, monkeypatch, sleeps):
    err = commands.UpgradeError("GitHub answered 403.")
    p, it = _plugin(tmp_path, monkeypatch, release=err, dev=err), _Interaction()

    async def go():
        await commands.Jano.jano_upgrade.callback(p, it)
        await sleeps.drain()
    asyncio.run(go())
    assert "403" in it.followup.sent[0]["embed"].description and sleeps.deleted == [30]


def test_a_release_update_installs_then_asks_about_the_restart(tmp_path, monkeypatch, sleeps):
    p = _plugin(tmp_path, monkeypatch, release=_offer("9.9.9"), downloads={"https://api.github.com/zip/9.9.9": _zip("9.9.9")})
    it = _Interaction()

    async def go():
        await commands.Jano.jano_upgrade.callback(p, it)
        view = it.followup.sent[0]["view"]
        await view.update_release.callback(it)
    asyncio.run(go())
    assert (tmp_path / "commands.py").read_text().startswith('COMMANDS_VERSION = "9.9.9"')
    steps = [kw["embed"].description for kw in it.original if kw.get("embed")]
    assert steps[0].startswith("⏳") and steps[1].startswith("🔎") and steps[2].startswith("📦") and steps[-1].startswith("✅")
    assert it.original[-1]["view"].children[0].label == "Restart now"
    assert p._upgrading is False


def test_restart_now_restarts_and_leaves_a_notice_later_does_not(tmp_path, monkeypatch, sleeps):
    p = _plugin(tmp_path, monkeypatch)

    async def go(label):
        it = _Interaction()
        view = commands._RestartView(p, it.user.id, "9.9.9")
        button = next(b for b in view.children if b.label == label)
        await button.callback(it)
        await sleeps.drain()
        return it
    it = asyncio.run(go("Restart now"))
    notice = json.loads((tmp_path / ".restart_notice.json").read_text())
    assert p.restarts == [True]
    assert (notice["app_id"], notice["token"], notice["message_id"], notice["expected"]) == (42, "tok", 555, "9.9.9")
    os.remove(tmp_path / ".restart_notice.json")
    p.restarts.clear()
    it = asyncio.run(go("Later"))
    assert p.restarts == [] and not (tmp_path / ".restart_notice.json").exists()
    assert "takes effect the next time" in it.response.edits[0]["embed"].description and sleeps.later[-1] == 30


def test_the_dev_button_shows_a_risk_warning_and_installs_only_after_accepting(tmp_path, monkeypatch, sleeps):
    dev = _dev_offer("9.9.9")
    p = _plugin(tmp_path, monkeypatch, dev=dev)
    it = _Interaction()

    async def go():
        await commands.Jano.jano_upgrade.callback(p, it)
        view = it.followup.sent[0]["view"]
        await view.update_dev.callback(it)
        assert not (tmp_path / "commands.py").exists()                      # nothing installed yet
        warning = it.response.edits[-1]
        assert warning["embed"].fields[0].name == "The risk"
        await next(b for b in warning["view"].children if b.label.startswith("Accept")).callback(it)
    asyncio.run(go())
    assert (tmp_path / "commands.py").read_text().startswith('COMMANDS_VERSION = "9.9.9"')
    assert p.downloads == []                                                # the zip that was offered is the one installed


def test_cancelling_the_risk_warning_installs_nothing(tmp_path, monkeypatch, sleeps):
    p = _plugin(tmp_path, monkeypatch, dev=_dev_offer())
    it = _Interaction()

    async def go():
        await commands.Jano.jano_upgrade.callback(p, it)
        await it.followup.sent[0]["view"].update_dev.callback(it)
        await next(b for b in it.response.edits[-1]["view"].children if b.label == "Cancel").callback(it)
        await sleeps.drain()
    asyncio.run(go())
    assert not list(tmp_path.iterdir()) and sleeps.later == [5]
    assert it.response.edits[-1]["embed"].description == "Update cancelled."


def test_a_broken_release_installs_nothing_and_says_so(tmp_path, monkeypatch, sleeps):
    p = _plugin(tmp_path, monkeypatch, release=_offer("9.9.9"),
                downloads={"https://api.github.com/zip/9.9.9": _zip("1.2.3")})     # the zip announces another version
    it = _Interaction()

    async def go():
        await commands.Jano.jano_upgrade.callback(p, it)
        await it.followup.sent[0]["view"].update_release.callback(it)
        await sleeps.drain()
    asyncio.run(go())
    assert not list(tmp_path.iterdir())
    assert "Update stopped" in it.original[-1]["embed"].description and sleeps.later == [30] and p._upgrading is False


def test_a_second_update_while_one_runs_is_refused(tmp_path, monkeypatch, sleeps):
    p = _plugin(tmp_path, monkeypatch)
    p._upgrading = True
    it = _Interaction()
    asyncio.run(p._upgrade_run(it, _offer()))
    assert "already running" in it.followup.sent[0]["content"] and not it.original


# ── the notice after a restart ──────────────────────────────────────────────────────

class _Hook:
    def __init__(self, fail_first=False, fail_all=False):
        self.edits, self.fail_first, self.fail_all = [], fail_first, fail_all

    async def edit_message(self, target, **kw):
        if self.fail_all or (self.fail_first and not self.edits):
            self.edits.append(("failed", target))
            raise RuntimeError("Discord refused")
        self.edits.append((target, kw))


def _notice(tmp_path, expected="9.9.9", age=0):
    (tmp_path / ".restart_notice.json").write_text(json.dumps(
        {"app_id": 42, "token": "tok", "message_id": 555, "expected": expected, "created": time.time() - age}))


def test_the_restart_notice_reports_ok_or_a_mismatch(tmp_path, monkeypatch, sleeps):
    for expected, ok in ((commands.COMMANDS_VERSION, True), ("9.9.9", False)):
        sleeps.tasks.clear()                       # each asyncio.run is its own event loop
        p, hook = make_plugin(tmp_path), _Hook()
        p._notice_webhook = lambda app_id, token, hook=hook: hook
        _notice(tmp_path, expected)

        async def go():
            await p._finish_restart_notice()
            await sleeps.drain()
        asyncio.run(go())
        target, kw = hook.edits[0]
        text = kw["embeds"][0].description
        assert target == "@original" and (text.startswith("✅") if ok else text.startswith("⚠️"))
        assert not (tmp_path / ".restart_notice.json").exists()


def test_an_old_or_unreadable_notice_is_deleted_without_touching_discord(tmp_path, monkeypatch, sleeps):
    p, hook = make_plugin(tmp_path), _Hook()
    p._notice_webhook = lambda app_id, token: hook
    _notice(tmp_path, age=15 * 60)
    asyncio.run(p._finish_restart_notice())
    (tmp_path / ".restart_notice.json").write_text("not json")
    asyncio.run(p._finish_restart_notice())
    assert hook.edits == [] and not (tmp_path / ".restart_notice.json").exists()


def test_no_notice_file_means_nothing_happens(tmp_path, sleeps):
    p, hook = make_plugin(tmp_path), _Hook()
    p._notice_webhook = lambda app_id, token: hook
    asyncio.run(p._finish_restart_notice())
    assert hook.edits == []


def test_if_discord_refuses_the_original_the_message_id_is_tried(tmp_path, sleeps):
    p, hook = make_plugin(tmp_path), _Hook(fail_first=True)
    p._notice_webhook = lambda app_id, token: hook
    _notice(tmp_path)
    asyncio.run(p._finish_restart_notice())
    assert hook.edits[0] == ("failed", "@original") and hook.edits[1][0] == 555      # then the message id worked


def test_if_both_ways_fail_it_is_logged_as_a_warning(tmp_path, sleeps, caplog):
    p, hook = make_plugin(tmp_path), _Hook(fail_all=True)
    p._notice_webhook = lambda app_id, token: hook
    _notice(tmp_path)
    with caplog.at_level("WARNING"):
        asyncio.run(p._finish_restart_notice())
    assert "could not update the restart message" in caplog.text


def test_the_upgrade_command_is_registered_in_the_jano_group():
    assert {c.name for c in commands.Jano.jano_group.commands} >= {"status", "comms", "setup", "upgrade"}


# ── the configuration migration that goes with an update ────────────────────────────

REAL_MIGRATOR = open(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "migrate_config.py"),
                     encoding="utf-8").read()


def _with_config(tmp_path, monkeypatch, yaml_text, migrator=REAL_MIGRATOR):
    """A plugin set up for a release update whose zip carries `migrator`, with jano.yaml holding `yaml_text`."""
    cfg_dir = tmp_path / "bot_config"
    (cfg_dir / "plugins").mkdir(parents=True)
    (cfg_dir / "plugins" / "jano.yaml").write_text(yaml_text)
    plugin_dir = tmp_path / "plugin"
    plugin_dir.mkdir()
    p = _plugin(plugin_dir, monkeypatch, release=_offer("9.9.9"),
                downloads={"https://api.github.com/zip/9.9.9": _zip("9.9.9", extra={"migrate_config.py": migrator})})
    p.node = types.SimpleNamespace(config_dir=str(cfg_dir))
    return p, cfg_dir / "plugins" / "jano.yaml", plugin_dir


def _update(p, sleeps):
    it = _Interaction()

    async def go():
        await commands.Jano.jano_upgrade.callback(p, it)
        await it.followup.sent[0]["view"].update_release.callback(it)
        await sleeps.drain()
    asyncio.run(go())
    return it


def test_the_update_migrates_jano_yaml_and_does_not_install_the_migrator(tmp_path, monkeypatch, sleeps):
    p, yaml_path, plugin_dir = _with_config(tmp_path, monkeypatch, "DEFAULT:\n  command_role_ids: []\n")
    it = _update(p, sleeps)
    assert "timezone" in yaml_path.read_text() and (yaml_path.parent / "jano.yaml.bak").exists()
    assert not (plugin_dir / "migrate_config.py").exists()
    assert (plugin_dir / "commands.py").exists() and "⚠️" not in it.original[-1]["embed"].description


def test_a_failing_migration_is_reported_but_the_update_still_counts(tmp_path, monkeypatch, sleeps):
    p, yaml_path, plugin_dir = _with_config(tmp_path, monkeypatch, "DEFAULT:\n  command_role_ids: []\n",
                                            migrator="import sys\nsys.exit(1)\n")
    it = _update(p, sleeps)
    assert yaml_path.read_text() == "DEFAULT:\n  command_role_ids: []\n"
    assert (plugin_dir / "commands.py").exists()
    assert "migration reported an error" in it.original[-1]["embed"].description
    assert it.original[-1]["view"].children[0].label == "Restart now"


def test_no_jano_yaml_means_no_migration(tmp_path, monkeypatch, sleeps):
    p = make_plugin(tmp_path)
    p.node = types.SimpleNamespace(config_dir=str(tmp_path / "nothing_here"))
    assert p._run_migration(REAL_MIGRATOR.encode()) == ""


# ── the main branch as a third source ───────────────────────────────────────────────

def _main_offer(version="9.9.9", data=None):
    return {"channel": "main", "version": commands._release_version(version), "text": version, "tag": "main",
            "prerelease": False, "notes": "", "data": data if data is not None else _zip(version)}


def test_the_most_stable_source_wins_when_versions_tie():
    rel, main, dev = _offer("9.9.9"), _main_offer("9.9.9"), _dev_offer("9.9.9")
    assert commands._pick_offers(rel, main, dev) == (rel, None, None)                 # same everywhere: only the release
    assert commands._pick_offers(None, main, dev) == (None, main, None)               # no release: main beats dev


def test_each_source_is_offered_only_above_the_more_stable_ones():
    rel, main, dev = _offer("9.9.9"), _main_offer("9.9.10"), _dev_offer("9.9.11")
    assert [o and o["text"] for o in commands._pick_offers(rel, main, dev)] == ["9.9.9", "9.9.10", "9.9.11"]
    assert [o and o["text"] for o in commands._pick_offers(rel, _main_offer("9.9.9"), dev)] == ["9.9.9", None, "9.9.11"]
    assert [o and o["text"] for o in commands._pick_offers(rel, main, _dev_offer("9.9.10"))] == ["9.9.9", "9.9.10", None]
    assert [o and o["text"] for o in commands._pick_offers(None, main, _dev_offer("9.9.10"))] == [None, "9.9.10", None]
    assert [o and o["text"] for o in commands._pick_offers(None, None, dev)] == [None, None, "9.9.11"]
    assert commands._pick_offers(None, None, None) == (None, None, None)


def test_three_sources_give_three_enabled_buttons(tmp_path, monkeypatch, sleeps):
    p = _plugin(tmp_path, monkeypatch, release=_offer("9.9.9"), main=_main_offer("9.9.10"), dev=_dev_offer("9.9.11"))
    it = _Interaction()
    asyncio.run(commands.Jano.jano_upgrade.callback(p, it))
    view = it.followup.sent[0]["view"]
    assert [b.label for b in view.children] == ["Update to release", "Update to main branch",
                                                "Update to development branch", "Cancel"]
    assert not view.update_release.disabled and not view.update_main.disabled and not view.update_dev.disabled
    fields = {f.name: f.value for f in it.followup.sent[0]["embed"].fields}
    assert fields["Release"] == "Ver. 9.9.9" and fields["Main branch"] == "Ver. 9.9.10"
    assert fields["Development branch"].startswith("Ver. 9.9.11")


def test_only_main_newer_gives_only_the_main_button(tmp_path, monkeypatch, sleeps):
    p = _plugin(tmp_path, monkeypatch, main=_main_offer("9.9.10"))
    it = _Interaction()
    asyncio.run(commands.Jano.jano_upgrade.callback(p, it))
    view = it.followup.sent[0]["view"]
    assert (view.update_release.disabled, view.update_main.disabled, view.update_dev.disabled) == (True, False, True)


def test_the_main_branch_installs_what_was_offered_without_a_risk_warning(tmp_path, monkeypatch, sleeps):
    p = _plugin(tmp_path, monkeypatch, main=_main_offer("9.9.10"))
    it = _Interaction()

    async def go():
        await commands.Jano.jano_upgrade.callback(p, it)
        await it.followup.sent[0]["view"].update_main.callback(it)
    asyncio.run(go())
    assert (tmp_path / "commands.py").read_text().startswith('COMMANDS_VERSION = "9.9.10"')
    assert p.downloads == [] and it.original[-1]["view"].children[0].label == "Restart now"


def test_a_failing_source_does_not_hide_the_others(tmp_path, monkeypatch, sleeps):
    err = commands.UpgradeError("GitHub answered 404.")
    p = _plugin(tmp_path, monkeypatch, release=err, main=_main_offer("9.9.10"), dev=err)
    it = _Interaction()
    asyncio.run(commands.Jano.jano_upgrade.callback(p, it))
    sent = it.followup.sent[0]
    assert not sent["view"].update_main.disabled and sent["view"].update_release.disabled
    notes = " ".join(f.value for f in sent["embed"].fields if f.name == "ℹ️")
    assert "releases" in notes and "development branch" in notes and "main branch" not in notes


def test_a_branch_lookup_asks_for_that_branchs_zip_and_compares_versions(tmp_path, monkeypatch):
    p = make_plugin(tmp_path)
    asked = []

    async def http_get(url, as_json=False):
        asked.append(url)
        return _zip("9.9.9" if url.endswith("/main") else "1.0.0")
    p._http_get = http_get
    main = asyncio.run(p._upgrade_lookup_branch("main"))
    assert asked[0].endswith("/repos/pierpaolobirdi/jano-dcsserverbot-plugin/zipball/main")
    assert main["channel"] == "main" and main["text"] == "9.9.9" and main["prerelease"] is False and main["data"]
    assert asyncio.run(p._upgrade_lookup_branch("dev")) is None                       # 1.0.0 is below the installed version
    assert asked[1].endswith("/zipball/dev")


def test_zip_without_migrator_is_accepted():
    """Versions older than 5.0.7 (e.g. an old main branch) have no migrate_config.py."""
    files = commands._read_release_zip(_zip(drop=["migrate_config.py"]), "9.9.9")
    assert "migrate_config.py" not in files
    assert "plugins/jano/commands.py" in files
