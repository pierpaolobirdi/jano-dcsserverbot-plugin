"""migrate_config.py: adds the missing variables, keeps everything else, never breaks the file."""
import os

import pytest

import migrate_config as mc

FULL = '''# jano.yaml — comments stay
DEFAULT:
  command_role_ids:
    - Admin
    - 123456789012
  timezone: "Europe/London"   # my zone
  open_message:
    title: "Hi {name}"
    body: "Go {voice}"
'''


def test_a_complete_file_is_left_alone():
    new, changes, notes = mc.migrate(FULL)
    assert changes == [] and notes == []


def test_missing_variables_are_added_with_the_blocks_indentation_and_nothing_else_changes():
    src = FULL.replace('  timezone: "Europe/London"   # my zone\n', "").replace(
        '  open_message:\n    title: "Hi {name}"\n    body: "Go {voice}"\n', "")
    new, changes, _ = mc.migrate(src)
    assert changes == ['added timezone: "Europe/Madrid"', "added open_message: {}"]
    assert new.startswith(src.rstrip("\n"))                      # everything that was there is still there, in place
    assert '\n  timezone: "Europe/Madrid"  # IANA timezone' in new and "\n  open_message: {}  #" in new


def test_variables_are_added_after_the_last_line_of_a_multiline_value():
    src = "DEFAULT:\n  open_message:\n    title: x\n\n# end of file\n"
    new, _, _ = mc.migrate(src)
    assert new.index("    title: x") < new.index("  command_role_ids: []") < new.index("# end of file")


def test_a_commented_variable_counts_as_missing():
    new, changes, _ = mc.migrate('DEFAULT:\n  # timezone: "Europe/Paris"\n  command_role_ids: []\n  open_message: {}\n')
    assert changes == ['added timezone: "Europe/Madrid"'] and '# timezone: "Europe/Paris"' in new


def test_windows_line_endings_are_kept():
    new, changes, _ = mc.migrate("DEFAULT:\r\n  command_role_ids: []\r\n")
    assert changes and "\n" not in new.replace("\r\n", "")


def test_a_file_without_a_final_newline_gets_clean_lines():
    new, _, _ = mc.migrate("DEFAULT:\n  command_role_ids: []")
    assert new.splitlines()[1] == "  command_role_ids: []" and all(line for line in new.splitlines())


def test_a_missing_default_block_is_written():
    new, changes, _ = mc.migrate("# only a comment\n")
    assert new.count("DEFAULT:") == 1 and len(changes) == 3
    assert mc.migrate(new)[1] == []                              # and a second run has nothing left to do


def test_an_empty_default_block_is_filled():
    new, changes, _ = mc.migrate("DEFAULT:\n")
    assert len(changes) == 3 and new.splitlines()[0] == "DEFAULT:" and new.splitlines()[1].startswith("  command_role_ids")


def test_an_inline_default_is_not_touched_but_reported():
    src = "DEFAULT: {}\n"
    new, changes, notes = mc.migrate(src)
    assert new == src and changes == [] and "not written as a block" in notes[0]


def test_renames_and_removals(monkeypatch):
    monkeypatch.setattr(mc, "RENAMED_KEYS", {"zone": "timezone"})
    monkeypatch.setattr(mc, "RETIRED_KEYS", ("old_option",))
    src = ("DEFAULT:\n  command_role_ids: []\n  zone: \"Europe/Rome\"  # kept\n  open_message: {}\n"
           "  old_option:\n    child: 1\n    other: 2\n")
    new, changes, _ = mc.migrate(src)
    assert changes == ["renamed zone -> timezone", "removed old_option"]
    assert 'timezone: "Europe/Rome"  # kept' in new and "old_option" not in new and "child" not in new
    assert mc.migrate(new)[1] == []


def test_a_rename_never_overwrites_an_existing_new_name(monkeypatch):
    monkeypatch.setattr(mc, "RENAMED_KEYS", {"zone": "timezone"})
    src = 'DEFAULT:\n  command_role_ids: []\n  open_message: {}\n  zone: a\n  timezone: b\n'
    new, changes, notes = mc.migrate(src)
    assert new == src and changes == [] and "both zone and timezone" in notes[0]


# ── the command line ──────────────────────────────────────────────────────────────

def test_main_writes_a_backup_only_when_something_changes(tmp_path, capsys):
    path = tmp_path / "jano.yaml"
    path.write_text("DEFAULT:\n  command_role_ids: []\n")
    assert mc.main(["x", str(path)]) == 0
    assert (tmp_path / "jano.yaml.bak").read_text() == "DEFAULT:\n  command_role_ids: []\n"
    assert "timezone" in path.read_text() and "Migrated jano.yaml" in capsys.readouterr().out
    os.remove(tmp_path / "jano.yaml.bak")
    assert mc.main(["x", str(path)]) == 0                         # second run: nothing to do, no backup
    assert not (tmp_path / "jano.yaml.bak").exists() and "up to date" in capsys.readouterr().out
    assert not list(tmp_path.glob("*.new"))


def test_main_reports_errors_with_exit_code_1(tmp_path, capsys):
    assert mc.main(["x"]) == 1
    assert mc.main(["x", str(tmp_path / "missing.yaml")]) == 1
    assert "File not found" in capsys.readouterr().out
    bad = tmp_path / "jano.yaml"
    bad.write_bytes(b"\xff\xfe not utf-8")
    assert mc.main(["x", str(bad)]) == 1


def test_a_file_that_is_not_valid_yaml_is_left_unchanged(tmp_path, capsys):
    pytest.importorskip("yaml")
    path = tmp_path / "jano.yaml"
    path.write_text("DEFAULT:\n  command_role_ids: [unclosed\n")
    assert mc.main(["x", str(path)]) == 1
    assert path.read_text() == "DEFAULT:\n  command_role_ids: [unclosed\n" and not (tmp_path / "jano.yaml.bak").exists()
    assert "not valid YAML" in capsys.readouterr().out


def test_the_shipped_config_needs_no_migration():
    shipped = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "config", "plugins", "jano.yaml")
    with open(shipped, encoding="utf-8") as f:
        assert mc.migrate(f.read())[1] == []


def test_the_result_is_valid_yaml_with_the_original_values():
    yaml = pytest.importorskip("yaml")
    src = FULL.replace('  timezone: "Europe/London"   # my zone\n', "")
    new, _, _ = mc.migrate(src)
    data = yaml.safe_load(new)["DEFAULT"]
    assert data["command_role_ids"] == ["Admin", 123456789012] and data["timezone"] == "Europe/Madrid"
    assert data["open_message"] == {"title": "Hi {name}", "body": "Go {voice}"}
