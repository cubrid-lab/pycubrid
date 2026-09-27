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


def has_docs_not_needed_reason(body: str | None) -> bool:
    prefix = "Docs: not needed -"
    fence = None
    comment = quoted = False
    for raw in (body or "").splitlines():
        line = raw.rstrip()
        stripped = line.lstrip()
        marker = re.match(r" {0,3}(`{3,}|~{3,})", line)
        if fence:
            if marker and marker[1][0] == fence[0] and len(marker[1]) >= fence[1]:
                if not line[marker.end() :].strip():
                    fence = None
            continue
        if comment:
            if "-->" in line:
                comment = line.rfind("<!--") > line.rfind("-->")
            continue
        if marker:
            fence = (marker[1][0], len(marker[1]))
            continue
        if not stripped:
            quoted = False
            continue
        if stripped.startswith(">"):
            quoted = True
            continue
        if "<!--" in line:
            comment = line.rfind("<!--") > line.rfind("-->")
        if not quoted and line.startswith(prefix):
            reason = re.sub(r"<!--.*?(?:-->|$)", "", line[len(prefix) :]).strip()
            if reason and "<reason>" not in reason:
                return True
    return False
