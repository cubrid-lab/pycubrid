#!/usr/bin/env python3
"""Check the agent guidance: root ``AGENTS.md`` plus the guides in ``docs/agents/`` (#926).

Deterministic checks only, no network:

* every relative Markdown link resolves to a file, and every ``#anchor`` to a
  heading of its target (GitHub heading slugs);
* every guide under ``docs/agents/`` is linked from the root ``AGENTS.md``;
* every ``make <target>`` names a Makefile target and every ``scripts/<name>``
  path exists;
* a file that names a Python or CUBRID version links the support matrix or the
  CI policy, which own version lists, instead of carrying its own.

``--retained-from REF`` also checks that every non-blank line of ``AGENTS.md``
at git ``REF`` still appears in the agent guidance, with relative link targets
compared after resolving them to repository paths. Use it when moving rules
between these files: a moved rule must keep its text.

Usage: ``python scripts/check_agents_docs.py [--root DIR] [--retained-from REF]``
"""

from __future__ import annotations

import argparse
import posixpath
import re
import subprocess
import sys
from pathlib import Path

ROOT_DOC = "AGENTS.md"
GUIDE_DIR = "docs/agents"
VERSION_OWNERS = ("docs/SUPPORT_MATRIX.md", "docs/CI_POLICY.md")

# A Markdown link or image target, with an optional "title", 'title' or (title).
_LINK = re.compile(r"\[[^\]]*\]\(\s*<?([^)\s>]+)>?(?:\s+(?:\"[^\"]*\"|'[^']*'|\([^)]*\)))?\s*\)")
_HEADING = re.compile(r"^(#{1,6})\s+(.*?)\s*#*\s*$")
_MAKE = re.compile(r"\bmake\s+([a-z][a-z0-9_-]*)")
_SCRIPT = re.compile(r"\bscripts/[A-Za-z0-9_./-]+\.(?:py|cjs|sh)\b")
_VERSION = re.compile(r"\b(?:Python\s+3\.\d+|CUBRID\s+1\d\.\d)")
_MAKE_TARGET = re.compile(r"^([a-zA-Z0-9_-]+)\s*:(?!=)", re.M)


def agent_files(root: Path) -> list[Path]:
    """Return the root entry point and the guides, in a stable order."""
    files = [root / ROOT_DOC]
    files += sorted((root / GUIDE_DIR).glob("*.md"))
    return [path for path in files if path.is_file()]


def _prose_and_code(text: str) -> tuple[list[tuple[int, str]], list[tuple[int, str]]]:
    """Split numbered lines into prose and fenced-code lines."""
    prose: list[tuple[int, str]] = []
    code: list[tuple[int, str]] = []
    fence = False
    for number, line in enumerate(text.splitlines(), 1):
        if line.lstrip().startswith("```"):
            fence = not fence
            continue
        (code if fence else prose).append((number, line))
    return prose, code


def navigable_links(text: str) -> list[tuple[int, str]]:
    """Return ``(line, target)`` of the links a reader can follow.

    Fenced code and inline code spans are examples, not links.
    """
    prose, _ = _prose_and_code(text)
    return [
        (number, match.group(1))
        for number, line in prose
        for match in _LINK.finditer(re.sub(r"`[^`]*`", "", line))
    ]


def slugify(heading: str) -> str:
    """Return GitHub's anchor for a Markdown heading text."""
    text = re.sub(r"`([^`]*)`", r"\1", heading)
    text = re.sub(r"\[([^\]]*)\]\([^)]*\)", r"\1", text).strip().lower()
    text = re.sub(r"[^\w\- ]", "", text)
    return text.replace(" ", "-")


def anchors(path: Path) -> set[str]:
    """Return the heading anchors of a Markdown file, with GitHub's -1, -2 suffixes."""
    seen: dict[str, int] = {}
    result: set[str] = set()
    prose, _ = _prose_and_code(path.read_text(encoding="utf-8"))
    for _, line in prose:
        match = _HEADING.match(line)
        if not match:
            continue
        slug = slugify(match.group(2))
        count = seen.get(slug, 0)
        seen[slug] = count + 1
        result.add(slug if count == 0 else f"{slug}-{count}")
    return result


def _is_external(target: str) -> bool:
    return bool(re.match(r"[a-zA-Z][a-zA-Z0-9+.-]*:", target))


def check_links(root: Path, path: Path) -> list[str]:
    """Report relative links whose file or anchor does not exist."""
    problems = []
    rel = path.relative_to(root).as_posix()
    for number, target in navigable_links(path.read_text(encoding="utf-8")):
        if _is_external(target):
            continue
        file_part, _, anchor = target.partition("#")
        dest = path if not file_part else (path.parent / file_part).resolve()
        if not dest.exists():
            problems.append(f"{rel}:{number}: link target {target!r} does not exist")
            continue
        if anchor and dest.suffix == ".md" and anchor not in anchors(dest):
            problems.append(
                f"{rel}:{number}: anchor #{anchor} is not a heading of "
                f"{dest.relative_to(root).as_posix()}"
            )
    return problems


def check_reachable(root: Path) -> list[str]:
    """Report guides that the root entry point does not link."""
    entry = root / ROOT_DOC
    if not entry.is_file():
        return [f"{ROOT_DOC}: missing"]
    linked = set()
    for _, target in navigable_links(entry.read_text(encoding="utf-8")):
        file_part = target.partition("#")[0]
        if file_part and not _is_external(file_part):
            linked.add((root / file_part).resolve())
    return [
        f"{guide.relative_to(root).as_posix()}: not linked from {ROOT_DOC}"
        for guide in sorted((root / GUIDE_DIR).glob("*.md"))
        if guide.resolve() not in linked
    ]


def check_commands(root: Path, path: Path) -> list[str]:
    """Report ``make`` targets and ``scripts/`` paths that do not exist."""
    makefile = root / "Makefile"
    targets = (
        set(_MAKE_TARGET.findall(makefile.read_text(encoding="utf-8")))
        if makefile.exists()
        else set()
    )
    problems = []
    rel = path.relative_to(root).as_posix()
    for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        for target in _MAKE.findall(line):
            if target not in targets:
                problems.append(f"{rel}:{number}: `make {target}` is not a Makefile target")
        for script in _SCRIPT.findall(line):
            if not (root / script).is_file():
                problems.append(f"{rel}:{number}: {script} does not exist")
    return problems


def check_versions(root: Path, path: Path) -> list[str]:
    """Require a file that names versions to link the documents that own them."""
    text = path.read_text(encoding="utf-8")
    found = _VERSION.search(text)
    if not found:
        return []
    owners = {(root / owner).resolve() for owner in VERSION_OWNERS}
    for _, target in navigable_links(text):
        file_part = target.partition("#")[0]
        if file_part and not _is_external(file_part):
            if (path.parent / file_part).resolve() in owners:
                return []
    number = text[: found.start()].count("\n") + 1
    return [
        f"{path.relative_to(root).as_posix()}:{number}: names {found.group(0)!r} but links "
        f"neither {' nor '.join(VERSION_OWNERS)}, which own version lists"
    ]


def _normalized(root: Path, path: Path, line: str) -> str:
    """Return a line with collapsed spaces and repository-relative link targets."""

    def resolve(match: re.Match[str]) -> str:
        target = match.group(1)
        if _is_external(target) or target.startswith("#"):
            return match.group(0)
        file_part, sep, anchor = target.partition("#")
        base = path.parent.relative_to(root).as_posix()
        joined = posixpath.normpath(posixpath.join(base, file_part)) if file_part else ""
        return match.group(0).replace(target, joined + sep + anchor)

    return " ".join(_LINK.sub(resolve, line).split())


def check_retained(root: Path, ref: str) -> list[str]:
    """Report lines of ``AGENTS.md`` at ``ref`` missing from the current guidance."""
    old = subprocess.run(
        ["git", "-C", str(root), "show", f"{ref}:{ROOT_DOC}"],
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    current = set()
    for path in agent_files(root):
        for line in path.read_text(encoding="utf-8").splitlines():
            current.add(_normalized(root, path, line))
    missing = []
    for number, line in enumerate(old.splitlines(), 1):
        normalized = _normalized(root, root / ROOT_DOC, line)
        if normalized and normalized not in current:
            missing.append(f"{ref}:{ROOT_DOC}:{number}: not retained: {line.strip()[:100]}")
    return missing


def run(root: Path, retained_from: str | None = None) -> list[str]:
    """Return every problem found under ``root``."""
    problems = check_reachable(root)
    for path in agent_files(root):
        problems += check_links(root, path)
        problems += check_commands(root, path)
        problems += check_versions(root, path)
    if retained_from:
        problems += check_retained(root, retained_from)
    return problems


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--retained-from", metavar="REF")
    args = parser.parse_args(argv)
    root = args.root.resolve()
    problems = run(root, args.retained_from)
    if problems:
        print("Agent guidance problems:")
        for problem in problems:
            print(f"  {problem}")
        return 1
    files = agent_files(root)
    print(f"OK: {len(files)} agent guidance files; links, guides and commands resolve")
    return 0


if __name__ == "__main__":
    sys.exit(main())
