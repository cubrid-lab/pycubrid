"""Shared CommonMark helpers for the repository documentation checks (#927).

``markdown-it-py`` parses each file, so fenced code (backtick or tilde, any
length), inline code spans of any backtick length and reference-style links
follow CommonMark instead of hand-written regular expressions. Only links a
reader can follow are returned: a reference definition without a use renders
nothing, and links inside code are examples.
"""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from pathlib import Path

from markdown_it import MarkdownIt
from markdown_it.token import Token

_PARSER = MarkdownIt("commonmark").enable("table")
_ATTR_ID = re.compile(r"\s*\{[^}]*#([\w-]+)[^}]*\}\s*$")
_HTML_ANCHOR = re.compile(r"<a\s[^>]*\b(?:id|name)\s*=\s*[\"']([^\"']+)[\"']", re.I)
_FENCE_OPEN = re.compile(r"^ {0,3}(`{3,}|~{3,})")
_MAKE_GOAL = re.compile(r"[a-z][a-z0-9_-]*\Z")


@dataclass(frozen=True)
class Link:
    """A followable link: 1-based source line and its raw target."""

    line: int
    target: str


@dataclass(frozen=True)
class Parsed:
    """The parts of a Markdown file the checks need."""

    links: list[Link]
    definitions: list[Link]
    undefined_references: list[Link]
    anchors: set[str]
    code: list[tuple[int, str]]
    unclosed_fences: list[int]


def github_slug(text: str) -> str:
    """Return GitHub's base anchor for a heading's rendered text.

    Lowercase; keep letters, marks, numbers, ``_``, ``-`` and spaces; turn each
    space into ``-`` (github-slugger, which mkdocs' pymdownx slugify matches here).
    """
    kept = []
    for char in text.lower():
        category = unicodedata.category(char)
        if char in " -_" or category[0] in "LMN" or category == "Pc":
            kept.append(char)
    return "".join(kept).replace(" ", "-")


def _site_slug(text: str) -> str:
    """Return the MkDocs site's base anchor: GitHub's, without combining marks.

    ``pymdownx.slugs.slugify`` (mkdocs.yml) drops marks such as Devanagari
    vowel signs that GitHub keeps.
    """
    return "".join(c for c in github_slug(text) if unicodedata.category(c)[0] != "M")


def _allocate(base: str, used: set[str], separator: str) -> str:
    """Return ``base`` or its first free ``<separator>N`` duplicate suffix."""
    slug, count = base, 0
    while slug in used:
        count += 1
        slug = f"{base}{separator}{count}"
    used.add(slug)
    return slug


def _inline_text(token: Token) -> str:
    return "".join(
        child.content for child in token.children or [] if child.type in ("text", "code_inline")
    )


def _walk_inline(token: Token, line: int, links: list[Link]) -> None:
    for child in token.children or []:
        if child.type == "link_open":
            href = str(child.attrGet("href") or "")
            links.append(Link(line, href))
        elif child.type == "image":
            links.append(Link(line, str(child.attrGet("src") or "")))


def _unclosed_fences(lines: list[str], tokens: list[Token]) -> list[int]:
    """Return the opening lines of fences that run to the end of the file."""
    unclosed = []
    for token in tokens:
        if token.type != "fence" or token.map is None:
            continue
        start, end = token.map
        if end < len(lines):
            continue
        opening = _FENCE_OPEN.match(lines[start])
        if opening is None:
            continue
        run = opening.group(1)
        last = lines[end - 1].strip() if end - 1 > start else ""
        closes = last.startswith(run[0] * len(run)) and set(last) <= {run[0]}
        if not closes:
            unclosed.append(start + 1)
    return unclosed


def _reference_uses(lines: list[str], tokens: list[Token]) -> list[Link]:
    """Return ``[text][label]`` uses whose label has no definition.

    markdown-it renders an undefined full reference as plain text, so look for
    the ``][`` pattern in text that is not code.
    """
    undefined = []
    for token in tokens:
        if token.type != "inline" or token.map is None:
            continue
        for child in token.children or []:
            if child.type != "text":
                continue
            for match in re.finditer(r"\]\[([^\]]+)\]", child.content):
                undefined.append(Link(token.map[0] + 1, f"[{match.group(1).lower()}]"))
    return undefined


def parse(path: Path) -> Parsed:
    """Parse one Markdown file."""
    text = path.read_text(encoding="utf-8")
    lines = text.splitlines()
    env: dict[str, object] = {}
    tokens = _PARSER.parse(text, env)
    links: list[Link] = []
    code: list[tuple[int, str]] = []
    # GitHub suffixes duplicate headings with -1, -2 ...; the MkDocs site (Python
    # Markdown's toc) with _1, _2 ... Docs are read in both, so accept both.
    github_used: set[str] = set()
    site_used: set[str] = set()
    anchors: set[str] = set()
    for match in _HTML_ANCHOR.finditer(text):
        anchors.add(match.group(1))
    for index, token in enumerate(tokens):
        if token.map is None:
            continue
        line = token.map[0] + 1
        if token.type == "fence":
            code += [(line + 1 + n, body) for n, body in enumerate(token.content.splitlines())]
        elif token.type == "inline":
            _walk_inline(token, line, links)
            for child in token.children or []:
                if child.type == "code_inline":
                    code.append((line, child.content))
            if index and tokens[index - 1].type == "heading_open":
                heading = _inline_text(token)
                explicit = _ATTR_ID.search(heading)
                if explicit:
                    anchors.add(explicit.group(1))
                    heading = heading[: explicit.start()]
                anchors.add(_allocate(github_slug(heading.strip()), github_used, "-"))
                anchors.add(_allocate(_site_slug(heading.strip()), site_used, "_"))
    references = env.get("references", {})
    definitions = []
    if isinstance(references, dict):
        for label, ref in references.items():
            href = ref.get("href", "") if isinstance(ref, dict) else ""
            map_ = ref.get("map") if isinstance(ref, dict) else None
            number = map_[0] + 1 if map_ else _definition_line(lines, label)
            definitions.append(Link(number, str(href)))
    return Parsed(
        links=links,
        definitions=definitions,
        undefined_references=_reference_uses(lines, tokens),
        anchors=anchors,
        code=sorted(code),
        unclosed_fences=_unclosed_fences(lines, tokens),
    )


def _definition_line(lines: list[str], label: str) -> int:
    pattern = re.compile(rf"^ {{0,3}}\[{re.escape(label)}\]:", re.I)
    for number, line in enumerate(lines, 1):
        if pattern.match(line):
            return number
    return 1


def make_goals(command: str) -> list[str]:
    """Return the explicit goals of every ``make`` invocation in a command line.

    Options (``-j2``, ``-C dir``) and ``VAR=value`` assignments are skipped;
    every remaining operand up to a shell separator is a goal.
    """
    goals = []
    words = re.split(r"\s+", command.strip())
    index = 0
    while index < len(words):
        if words[index] != "make":
            index += 1
            continue
        index += 1
        while index < len(words) and words[index] not in ("&&", "||", ";", "|", "#"):
            word = words[index]
            index += 1
            if word in ("-C", "-f", "-I", "-o", "-W"):
                index += 1
            elif word.startswith("-") or "=" in word:
                continue
            elif _MAKE_GOAL.match(word):
                goals.append(word)
            else:
                break
    return goals


def is_external(target: str) -> bool:
    """Return whether a link target has a URI scheme (``https:``, ``mailto:`` ...)."""
    return bool(re.match(r"[a-zA-Z][a-zA-Z0-9+.-]*:", target))
