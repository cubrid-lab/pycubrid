"""Shared CommonMark helpers for the repository documentation checks (#927).

``markdown-it-py`` parses each file, so fenced code (backtick or tilde, any
length, inside lists and block quotes), inline code spans of any backtick
length and reference-style links follow CommonMark instead of hand-written
regular expressions. Only links a reader can follow are returned: a reference
definition without a use renders nothing, and links inside code are examples.
Every link, code span and undefined reference keeps its own source line, so a
diagnostic and its suppression comment refer to the same line.
"""

from __future__ import annotations

import re
import shlex
import unicodedata
from dataclasses import dataclass
from pathlib import Path

from markdown_it import MarkdownIt
from markdown_it.token import Token

_PARSER = MarkdownIt("commonmark").enable("table")
_ATTR_ID = re.compile(r"\s*\{[^}]*#([\w-]+)[^}]*\}\s*$")
_HTML_ANCHOR = re.compile(r"<a\s[^>]*?\b(?:id|name)\s*=\s*[\"']([^\"']+)[\"']", re.I)
_HTML_COMMENT = re.compile(r"<!--.*?-->", re.S)
_CONTAINER_PREFIX = re.compile(r"^(?:\s*>)*\s*")
_BACKTICKS = re.compile(r"`+")
# A full or collapsed reference link whose brackets are not escaped; an opening
# bracket after a word character (``rows[0][1]``) is an index, not a link.
_REFERENCE = re.compile(r"(?<![\\\]\w])\[((?:[^\[\]\\]|\\.)+)\]\[((?:[^\[\]\\]|\\.)*)\](?!\s*[(:])")
_MAKE_GOAL = re.compile(r"[a-z][a-z0-9_.-]*\Z")
# make options that take a separate argument (``-C dir``) or an optional one (``-j 4``).
_MAKE_ARG = {
    "-C",
    "-f",
    "-I",
    "-o",
    "-W",
    "--directory",
    "--file",
    "--makefile",
    "--include-dir",
    "--old-file",
    "--assume-old",
    "--new-file",
    "--assume-new",
    "--what-if",
}
_MAKE_OPTIONAL_NUMBER = {"-j", "-l", "--jobs", "--load-average", "--max-load"}
_SHELL_SEPARATOR = re.compile(r"&&|\|\||[;|&()\n]")


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
    space into ``-`` (github-slugger).
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


def _mask_code_spans(raw: str) -> str:
    """Replace inline code spans (any backtick run length) by spaces, keeping newlines."""
    out = list(raw)
    position = 0
    while True:
        opening = _BACKTICKS.search(raw, position)
        if opening is None:
            return "".join(out)
        run = opening.group(0)
        closing = re.compile(rf"(?<!`){run}(?!`)").search(raw, opening.end())
        if closing is None:
            position = opening.end()
            continue
        for index in range(opening.start(), closing.end()):
            if out[index] != "\n":
                out[index] = " "
        position = closing.end()


def _inline_items(token: Token) -> list[tuple[int, Token]]:
    """Return ``(line offset, child)`` for the children of an inline token.

    Soft and hard breaks start a new line; a code span that spans lines moves
    the following children down by its newlines, found in the raw source.
    """
    raw = token.content
    cursor, offset = 0, 0
    items = []
    for child in token.children or []:
        items.append((offset, child))
        if child.type in ("softbreak", "hardbreak"):
            newline = raw.find("\n", cursor)
            if newline >= 0:
                cursor = newline + 1
            offset += 1
        elif child.type == "code_inline" and child.markup:
            start = raw.find(child.markup, cursor)
            end = raw.find(child.markup, start + len(child.markup)) if start >= 0 else -1
            if start >= 0 and end >= 0:
                offset += raw.count("\n", start, end)
                cursor = end + len(child.markup)
    return items


def _closes(line: str, markup: str) -> bool:
    """Return whether ``line`` (with block-quote markers removed) closes the fence."""
    body = _CONTAINER_PREFIX.sub("", line).rstrip()
    return len(body) >= len(markup) and set(body) == {markup[0]}


def _unclosed_fences(lines: list[str], tokens: list[Token]) -> list[int]:
    """Return the opening lines of fences that run to the end of their container."""
    unclosed = []
    for token in tokens:
        if token.type != "fence" or token.map is None or not token.markup:
            continue
        start, end = token.map
        if end - 1 <= start or not _closes(lines[end - 1], token.markup):
            unclosed.append(start + 1)
    return unclosed


def _undefined_references(token: Token, defined: set[str]) -> list[Link]:
    """Return unescaped ``[text][label]`` uses, outside code, whose label is undefined."""
    if token.map is None:
        return []
    raw = _mask_code_spans(token.content)
    found = []
    for match in _REFERENCE.finditer(raw):
        label = " ".join((match.group(2) or match.group(1)).split()).casefold()
        if label not in defined:
            line = token.map[0] + 1 + raw.count("\n", 0, match.start())
            found.append(Link(line, f"[{label}]"))
    return found


def _html_anchors(tokens: list[Token]) -> set[str]:
    """Return ``<a id>``/``<a name>`` anchors of HTML blocks and inline HTML, not comments."""
    found = set()
    for token in tokens:
        sources = [token.content] if token.type == "html_block" else []
        if token.type == "inline":
            sources += [c.content for c in token.children or [] if c.type == "html_inline"]
        for source in sources:
            found |= set(_HTML_ANCHOR.findall(_HTML_COMMENT.sub("", source)))
    return found


def parse(path: Path) -> Parsed:
    """Parse one Markdown file."""
    text = path.read_text(encoding="utf-8")
    lines = text.splitlines()
    env: dict[str, object] = {}
    tokens = _PARSER.parse(text, env)
    references = env.get("references", {})
    references = references if isinstance(references, dict) else {}
    defined = {" ".join(str(label).split()).casefold() for label in references}
    links: list[Link] = []
    code: list[tuple[int, str]] = []
    undefined: list[Link] = []
    # GitHub keeps a literal ``{#id}`` in its slug and suffixes duplicates with
    # -1, -2 ...; the MkDocs site (attr_list, toc) uses the id and _1, _2 ...
    github_used: set[str] = set()
    site_used: set[str] = set()
    anchors = _html_anchors(tokens)
    for index, token in enumerate(tokens):
        if token.map is None:
            continue
        line = token.map[0] + 1
        if token.type == "fence":
            code += [(line + 1 + n, body) for n, body in enumerate(token.content.splitlines())]
            continue
        if token.type != "inline":
            continue
        for offset, child in _inline_items(token):
            if child.type == "link_open":
                links.append(Link(line + offset, str(child.attrGet("href") or "")))
            elif child.type == "image":
                links.append(Link(line + offset, str(child.attrGet("src") or "")))
            elif child.type == "code_inline":
                code.append((line + offset, child.content))
        undefined += _undefined_references(token, defined)
        if index and tokens[index - 1].type == "heading_open":
            heading = _inline_text(token).strip()
            anchors.add(_allocate(github_slug(heading), github_used, "-"))
            explicit = _ATTR_ID.search(heading)
            if explicit:
                anchors.add(explicit.group(1))
                site_used.add(explicit.group(1))
            else:
                anchors.add(_allocate(_site_slug(heading), site_used, "_"))
    definitions = []
    for label, ref in references.items():
        if isinstance(ref, dict):
            map_ = ref.get("map")
            number = map_[0] + 1 if map_ else _definition_line(lines, str(label))
            definitions.append(Link(number, str(ref.get("href", ""))))
    return Parsed(
        links=links,
        definitions=definitions,
        undefined_references=undefined,
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


def _words(segment: str) -> list[str]:
    try:
        return shlex.split(segment, comments=True)
    except ValueError:  # an unbalanced quote: fall back to whitespace words
        return segment.split("#", 1)[0].split()


def make_goals(command: str) -> list[str]:
    """Return the explicit goals of every ``make`` invocation in a command line.

    Shell separators (``;``, ``&&``, ``||``, ``|``, ``&``, parentheses,
    newlines) split invocations, with or without spaces. Options are skipped
    with their arguments (``-C dir``, ``--directory=dir``, ``-j 4``), and
    ``VAR=value`` assignments are skipped; every other operand is a goal.
    """
    goals = []
    for segment in _SHELL_SEPARATOR.split(command):
        words = _words(segment)
        if not words or words[0] != "make":
            continue
        index = 1
        while index < len(words):
            word = words[index]
            index += 1
            if word in _MAKE_ARG:
                index += 1
            elif word in _MAKE_OPTIONAL_NUMBER:
                if index < len(words) and words[index].isdigit():
                    index += 1
            elif word.startswith("-") or "=" in word:
                continue
            elif _MAKE_GOAL.match(word):
                goals.append(word)
    return goals


def is_external(target: str) -> bool:
    """Return whether a link target has a URI scheme (``https:``, ``mailto:`` ...)."""
    return bool(re.match(r"[a-zA-Z][a-zA-Z0-9+.-]*:", target))
