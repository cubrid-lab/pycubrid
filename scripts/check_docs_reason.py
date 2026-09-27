r"""Recognize a real standalone docs exception, outside Markdown examples.

>>> has_docs_not_needed_reason(None)
False
>>> has_docs_not_needed_reason('Docs: not needed - tests only')
True
>>> has_docs_not_needed_reason('Docs: not needed - <reason>')
False
>>> has_docs_not_needed_reason('> Docs: not needed - tests only')
False
>>> has_docs_not_needed_reason('````text\n```\nDocs: not needed - tests only\n````')
False
>>> has_docs_not_needed_reason('<!--\nDocs: not needed - tests only\n-->')
False
"""

from __future__ import annotations

import re
from html import unescape
from html.parser import HTMLParser

_PREFIX = "Docs: not needed -"


class _HTMLContext(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=False)
        self.blocked: list[str] = []
        self.lines: dict[int, str] = {}
        self.marker_lines: set[int] = set()

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag in {"blockquote", "pre", "code"}:
            self.blocked.append(tag)

    def handle_startendtag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        # These non-void containers still need an explicit closing tag in HTML.
        self.handle_starttag(tag, attrs)

    def handle_endtag(self, tag: str) -> None:
        if self.blocked and self.blocked[-1] == tag:
            self.blocked.pop()

    def handle_data(self, data: str) -> None:
        if not self.blocked:
            line, column = self.getpos()
            for offset, text in enumerate(data.split("\n")):
                number = line + offset
                self.lines[number] = self.lines.get(number, "") + text
                indentation = len(text) - len(text.lstrip(" "))
                origin = column if offset == 0 else 0
                if origin + indentation <= 3 and text.lstrip(" ").startswith(_PREFIX):
                    self.marker_lines.add(number)

    def handle_entityref(self, name: str) -> None:
        self.handle_data(unescape(f"&{name};").replace("\r", " ").replace("\n", " "))

    def handle_charref(self, name: str) -> None:
        self.handle_data(unescape(f"&#{name};").replace("\r", " ").replace("\n", " "))

    def outside_prefix(self, number: int, prefix: str) -> bool:
        self.feed(prefix)
        return not self.blocked and self.lines.get(number) == prefix


def has_docs_not_needed_reason(body: str | None) -> bool:
    prefix = _PREFIX
    html = _HTMLContext()
    candidates = []
    fence = None
    quoted = False
    for number, raw in enumerate((body or "").splitlines(), 1):
        line = raw.rstrip()
        stripped = line.lstrip()
        marker = re.match(r" {0,3}(`{3,}|~{3,})", line)
        if fence:
            if marker and marker[1][0] == fence[0] and len(marker[1]) >= fence[1]:
                if not line[marker.end() :].strip():
                    fence = None
            html.feed("\n")
            continue
        if not stripped:
            quoted = False
        if quoted:
            html.feed("\n")
            continue
        indentation = re.match(r"(?: {4,}| {0,3}\t)", line)
        quote = re.match(r" {0,3}>", line)
        if indentation:
            if html.outside_prefix(number, line[: indentation.end()]):
                html.feed("\n")
                continue
            html.feed(line[indentation.end() :] + "\n")
        elif quote:
            if html.outside_prefix(number, line[: quote.end()]):
                quoted = True
                html.feed("\n")
                continue
            html.feed(line[quote.end() :] + "\n")
        elif marker and (marker[1][0] == "~" or "`" not in line[marker.end() :]):
            if html.outside_prefix(number, line[: marker.end()]):
                fence = (marker[1][0], len(marker[1]))
                html.feed("\n")
                continue
            html.feed(line[marker.end() :] + "\n")
        else:
            html.feed(line + "\n")
        if re.match(r" {0,3}" + re.escape(prefix), line):
            candidates.append(number)
    for number in candidates:
        line = html.lines.get(number, "").lstrip(" ")
        if number in html.marker_lines and line.startswith(prefix):
            reason = line[len(prefix) :].strip()
            if reason and "<reason>" not in reason:
                return True
    return False
