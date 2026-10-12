"""The schema DCSServerBot's web console builds the settings form from (plugins/jano/schemas)."""
import os
import subprocess
import sys

import pytest
import yaml

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
import migrate_config as mc  # noqa: E402

SCHEMA = os.path.join(ROOT, "plugins", "jano", "schemas", "jano_schema.yaml")


def _default_keys() -> set:
    with open(SCHEMA, encoding="utf-8") as f:
        doc = yaml.safe_load(f)
    return set(doc["mapping"]["DEFAULT"]["mapping"])


def test_every_known_variable_is_in_the_schema_and_nothing_else():
    assert _default_keys() == set(mc.KNOWN_KEYS), sorted(_default_keys() ^ set(mc.KNOWN_KEYS))


DCSSB = os.environ.get("DCSSB_DIR", "/home/user/special-k-s-flightsim-bots/dcsserverbot")
CHECK = r'''
import sys, os
tmp, dcssb, schema, *files = sys.argv[1:]
os.makedirs(tmp + "/core", exist_ok=True); os.makedirs(tmp + "/conf", exist_ok=True)
open(tmp + "/core/__init__.py", "w").close()
open(tmp + "/core/const.py", "w").write("DEFAULT_TAG = 'DEFAULT'")
open(tmp + "/core/commandline.py", "w").write("import types\nCOMMAND_LINE_ARGS = types.SimpleNamespace(config=%r, node='Master')" % (tmp + "/conf"))
open(tmp + "/conf/nodes.yaml", "w").write("Master:\n  instances:\n    DCS_Server: {}\n")
open(tmp + "/conf/servers.yaml", "w").write("Srv1: {}\n")
sys.path.insert(0, tmp); os.chdir(dcssb)
from pykwalify.core import Core
from pykwalify.errors import PyKwalifyException
for f in files:
    c = Core(source_file=f, schema_files=[schema, dcssb + "/schemas/commands_schema.yaml"], file_encoding="utf-8",
             extensions=[dcssb + "/core/utils/validators.py"])
    try:
        c.validate(raise_exception=True); print(os.path.basename(f), "OK")
    except PyKwalifyException:
        print(os.path.basename(f), "INVALID")
'''


def _validate(tmp_path, **docs) -> dict:
    files = []
    for name, text in docs.items():
        p = tmp_path / f"{name}.yaml"
        p.write_text(text, encoding="utf-8")
        files.append(str(p))
    out = subprocess.run([sys.executable, "-c", CHECK, str(tmp_path / "h"), DCSSB, SCHEMA, *files],
                         capture_output=True, text=True, timeout=120)
    return dict(line.split()[:2] for line in out.stdout.splitlines() if line.endswith(("OK", "INVALID")))


@pytest.mark.skipif(not os.path.isdir(DCSSB) or subprocess.run([sys.executable, "-c", "import pykwalify, ruamel.yaml"],
                                                                capture_output=True).returncode != 0,
                    reason="needs pykwalify and a DCSServerBot checkout (DCSSB_DIR)")
def test_the_schema_accepts_our_config_forms_and_refuses_mistakes(tmp_path):
    example = open(os.path.join(ROOT, "config", "plugins", "jano.yaml"), encoding="utf-8").read()
    full = ("DEFAULT:\n  command_role_ids:\n    - Admin\n    - 123456789012345678\n  timezone: Europe/London\n"
            "  open_message:\n    title: \"🟢 {name}\"\n    body: Connect to {voice}\n")
    got = _validate(tmp_path, example=example, full=full,
                    bad_roles="DEFAULT:\n  command_role_ids: Admin\n",
                    bad_message="DEFAULT:\n  open_message: hello\n",
                    unknown="DEFAULT:\n  bogus: 1\n")
    assert got == {"example.yaml": "OK", "full.yaml": "OK", "bad_roles.yaml": "INVALID",
                   "bad_message.yaml": "INVALID", "unknown.yaml": "INVALID"}


def test_the_schema_text_inside_migrate_config_is_the_schema_file():
    with open(SCHEMA, encoding="utf-8") as f:
        assert mc.SCHEMA_TEXT == f.read()


def test_migrate_puts_the_schema_in_place_from_the_working_directory(tmp_path, monkeypatch):
    root = tmp_path / "root"
    (root / "plugins" / "jano").mkdir(parents=True)
    (root / "config" / "plugins").mkdir(parents=True)
    monkeypatch.chdir(root)
    mc.install_schema(str(root / "config" / "plugins" / "jano.yaml"))
    target = root / "plugins" / "jano" / "schemas" / "jano_schema.yaml"
    assert target.read_text(encoding="utf-8") == mc.SCHEMA_TEXT
    target.write_text("old")
    mc.install_schema(str(root / "config" / "plugins" / "jano.yaml"))           # a different one is replaced
    assert target.read_text(encoding="utf-8") == mc.SCHEMA_TEXT


def test_migrate_finds_the_plugin_folder_from_the_config_path_and_skips_quietly_without_one(tmp_path, monkeypatch, capsys):
    root = tmp_path / "root"
    (root / "plugins" / "jano").mkdir(parents=True)
    (root / "config" / "plugins").mkdir(parents=True)
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    monkeypatch.chdir(elsewhere)
    mc.install_schema(str(root / "config" / "plugins" / "jano.yaml"))
    assert (root / "plugins" / "jano" / "schemas" / "jano_schema.yaml").exists()
    other = tmp_path / "nowhere" / "config" / "plugins"
    other.mkdir(parents=True)
    mc.install_schema(str(other / "jano.yaml"))                                  # no plugin folder: no error
    assert "not found" in capsys.readouterr().out
