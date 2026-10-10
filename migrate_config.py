"""
Jano config migration.

Run by install.cmd when jano.yaml already exists, and by /jano upgrade after an update:

    python migrate_config.py <path to jano.yaml>

It edits the file as text, so your values and comments stay exactly as they are. It only
  - adds the variables the file is missing (with their default value),
  - renames variables whose name changed,
  - removes variables that no longer exist.
It prints what it did. When nothing needs to change the file is not touched. When something does,
the previous file is kept next to it as jano.yaml.bak. Exit code: 0 fine, 1 nothing done (error).
"""
import os
import re
import sys

# Variables of the DEFAULT block: name -> (default value, comment written next to it when added)
KNOWN_KEYS = {
    "command_role_ids": ("[]", "roles allowed to use Jano commands (names or IDs); empty = everyone"),
    "timezone": ('"Europe/Madrid"', "IANA timezone used for the schedules"),
    "open_message": ("{}", "optional title/body of the opening announcement"),
}
RENAMED_KEYS = {}      # old name -> new name (value and comment are kept)
RETIRED_KEYS = ()      # names to remove, together with the lines indented below them

_DEFAULT_LINE = re.compile(r"^DEFAULT[ \t]*:(.*)$")
_KEY_LINE = re.compile(r"^([ \t]+)([A-Za-z_][\w-]*)[ \t]*:")


def _is_blank_or_comment(line: str) -> bool:
    stripped = line.strip()
    return not stripped or stripped.startswith("#")


def _find_default_block(lines: list[str]):
    """(index of the DEFAULT line, index after its last line) or None. The block ends at the next line
    that is not indented (comments and blank lines belong to whatever is around them)."""
    for i, line in enumerate(lines):
        m = _DEFAULT_LINE.match(line.rstrip("\r\n"))
        if not m:
            continue
        rest = m.group(1).strip()
        if rest and not rest.startswith("#"):
            return i, None                       # DEFAULT: {} or similar: not a block, cannot be edited safely
        end = i + 1
        while end < len(lines) and (_is_blank_or_comment(lines[end]) or lines[end][:1] in " \t"):
            end += 1
        while end > i + 1 and _is_blank_or_comment(lines[end - 1]):
            end -= 1                             # trailing blank/comment lines are not part of the block
        return i, end
    return None


def _keys_in(lines: list[str], start: int, end: int) -> dict[str, tuple[int, int]]:
    """name -> (line index, indent width) of the variables written in the block (commented ones do not count)."""
    found, indent = {}, None
    for i in range(start, end):
        m = _KEY_LINE.match(lines[i])
        if not m or lines[i].lstrip().startswith("#"):
            continue
        width = len(m.group(1).expandtabs())
        if indent is None:
            indent = width
        if width == indent:
            found[m.group(2)] = (i, width)
    return found


def migrate(content: str) -> tuple[str, list[str], list[str]]:
    """(new content, what changed, notes). Pure: no files involved."""
    eol = "\r\n" if "\r\n" in content else "\n"
    lines = content.splitlines(keepends=True)
    changes, notes = [], []
    if lines and not lines[-1].endswith(("\n", "\r")):
        lines[-1] += eol                           # so appended lines start on their own line

    block = _find_default_block(lines)
    if block is None:
        add = [f"DEFAULT:{eol}"]
        for key, (value, comment) in KNOWN_KEYS.items():
            add.append(f"  {key}: {value}  # {comment} (added by the update){eol}")
            changes.append(f"added {key}: {value}")
        if lines and lines[-1].strip():
            lines.append(eol)
        return "".join(lines + add), changes, notes
    start, end = block
    if end is None:
        notes.append("DEFAULT is not written as a block (for example 'DEFAULT: {}'): left as it is")
        return content, changes, notes

    # Renames and removals first, so that the check for missing variables sees the final names.
    keys = _keys_in(lines, start + 1, end)
    for old, new in RENAMED_KEYS.items():
        if old in keys and new in keys:
            notes.append(f"both {old} and {new} are present: {old} left as it is, remove it by hand")
        elif old in keys:
            i, _ = keys[old]
            lines[i] = re.sub(rf"^([ \t]+){re.escape(old)}(\s*:)", rf"\g<1>{new}\g<2>", lines[i], count=1)
            changes.append(f"renamed {old} -> {new}")
    for gone in RETIRED_KEYS:
        keys = _keys_in(lines, start + 1, end)
        if gone not in keys:
            continue
        i, width = keys[gone]
        j = i + 1
        while j < end and (lines[j].strip() == "" or len(lines[j]) - len(lines[j].lstrip()) > width):
            j += 1
        del lines[i:j]
        end -= j - i
        changes.append(f"removed {gone}")

    keys = _keys_in(lines, start + 1, end)
    width = next(iter(keys.values()))[1] if keys else 2
    insert = []
    for key, (value, comment) in KNOWN_KEYS.items():
        if key not in keys:
            insert.append(f"{' ' * width}{key}: {value}  # {comment} (added by the update){eol}")
            changes.append(f"added {key}: {value}")
    lines[end:end] = insert
    return "".join(lines), changes, notes


def _loads(text: str):
    """Parsed YAML when a YAML library is available (None when there is none)."""
    try:
        import yaml
        return yaml.safe_load(text)
    except ImportError:
        pass
    try:
        from ruamel.yaml import YAML
        return YAML(typ="safe").load(text)
    except ImportError:
        return None


def main(argv: list[str]) -> int:
    if len(argv) < 2:
        print("Usage: migrate_config.py <path to jano.yaml>")
        return 1
    path = argv[1]
    if not os.path.exists(path):
        print(f"File not found: {path}")
        return 1
    try:
        with open(path, "r", encoding="utf-8", newline="") as f:
            content = f.read()
    except (OSError, UnicodeDecodeError) as e:
        print(f"Could not read {path}: {e}")
        return 1

    new, changes, notes = migrate(content)
    for note in notes:
        print(f"  Note: {note}")
    if not changes:
        print("  Nothing to migrate: the configuration is up to date.")
        return 0
    try:                                           # never replace a good file with one that does not parse
        _loads(content)
    except Exception:
        print("  The current file is not valid YAML: left unchanged. Fix it by hand and run the update again.")
        return 1
    try:
        parsed = _loads(new)
    except Exception as e:
        print(f"  The migrated file would not be valid YAML ({e}): left unchanged.")
        return 1
    if parsed is not None and not (isinstance(parsed, dict) and isinstance(parsed.get("DEFAULT"), dict)):
        print("  The migrated DEFAULT block would not be a mapping: left unchanged.")
        return 1
    try:
        with open(path + ".bak", "w", encoding="utf-8", newline="") as f:
            f.write(content)
        with open(path + ".new", "w", encoding="utf-8", newline="") as f:
            f.write(new)
        os.replace(path + ".new", path)
    except OSError as e:
        print(f"  Could not write the migrated file: {e}")
        return 1
    print(f"  Migrated {os.path.basename(path)} (the previous file is kept as {os.path.basename(path)}.bak):")
    for change in changes:
        print(f"    {change}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
