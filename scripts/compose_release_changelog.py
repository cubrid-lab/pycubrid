#!/usr/bin/env python3
"""Compose canonical release notes from main's curated notes and release-please.

Read main's CHANGELOG at the generator base SHA, never a previously composed
candidate. Generated headers belong in RELEASE_CHANGELOG.md; publication keeps
its strict bracket/date contract. Curated and generated entries are merged under
the standard ``###`` headings in the standard order, so the candidate passes
scripts/lint_changelog.py. Generated issue references are rendered as "refs",
not "closes" (see GENERATED_REFERENCES). Call only after the freeze-label check.

With ``--pr-body``, read a GitHub pull request JSON instead and apply the same
reference rewrite to the release PR body, so GitHub does not treat the
generated references as closing keywords.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import re
import sys
from pathlib import Path

VERSION = re.compile(r"^[0-9]+\.[0-9]+\.[0-9]+$")
SECTION = re.compile(r"^## \[([^\]]+)\][^\n]*$", re.MULTILINE)
GENERATED = re.compile(
    r"^##? \[(?P<version>[0-9]+\.[0-9]+\.[0-9]+)\]"
    r"(?:\([^\n]+\))? \((?P<date>[0-9]{4}-[0-9]{2}-[0-9]{2})\)$",
    re.MULTILINE,
)
SUBSECTION = re.compile(r"^### (.+)$", re.MULTILINE)
HEADING_1_2 = re.compile(r"^#{1,2} ", re.MULTILINE)
# Standard ``###`` headings, in order (AGENTS.md "GitHub Release Policy"; kept in
# sync with scripts/lint_changelog.py). release-please renders breaking changes
# under its own heading; they are upgrade notes.
ALLOWED_SECTIONS = (
    "Upgrade notes",
    "Added",
    "Changed",
    "Deprecated",
    "Removed",
    "Fixed",
    "Security",
    "Performance",
    "Documentation",
    "CI",
    "Tests",
)
GENERATED_ALIASES = {"⚠ BREAKING CHANGES": "Upgrade notes"}
# release-please 17.6.0 turns an issue trailer in a commit's final footer
# block ("Refs #1" as well as "Closes #1") into a reference, and its
# changelog template renders all references as ", closes [#1](...)". The
# keyword is lost, so "closes" can be false; "refs" is true for every trailer.
# The match is tied to the commit-hash link that precedes the reference list.
GENERATED_REFERENCES = re.compile(r"(\(\[[0-9a-f]{7,40}\]\([^)\s]+\)\)), closes (?=\[)")


def _find(pattern: re.Pattern[str], text: str, origin: str) -> list[re.Match[str]]:
    """Line-anchored matches outside fenced code blocks; an unclosed fence fails closed.

    Lines inside a fenced code block are content, never headings (the same rule as
    scripts/lint_changelog.py). Fence lines themselves stay part of the surrounding text.
    A release header (``## [``) inside an open fence is an error, not content.
    """
    matches: list[re.Match[str]] = []
    in_fence = False
    pos = 0
    for line in text.splitlines(keepends=True):
        if in_fence and re.match(r"#{1,2} \[", line):
            raise ValueError(f"release header inside an open code fence in {origin}")
        if not in_fence and (match := pattern.match(text, pos)):
            matches.append(match)
        if line.startswith("```"):
            in_fence = not in_fence
        pos += len(line)
    if in_fence:
        raise ValueError(f"unclosed code fence in {origin}")
    return matches


# A GitHub Markdown code fence: three or more backticks or tildes, indented by at
# most three spaces. It closes with the same character, at least as long, alone.
FENCE = re.compile(r" {0,3}(`{3,}|~{3,})")


def _rewrite_references(notes: str) -> str:
    """Rewrite release-please's reference suffix on generated ``* `` entry lines only.

    Lines inside fenced code blocks (backtick or tilde fences) are content and stay
    unchanged.
    """
    lines = []
    fence: str | None = None
    for line in notes.splitlines(keepends=True):
        if fence is None and line.startswith("* "):
            line = GENERATED_REFERENCES.sub(r"\1, refs ", line, count=1)
        match = FENCE.match(line)
        if match:
            marker = match.group(1)
            if fence is None:
                fence = marker
            elif (
                marker[0] == fence[0]
                and len(marker) >= len(fence)
                and not line[match.end() :].strip()
            ):
                fence = None
        lines.append(line)
    return "".join(lines)


def _split(text: str, origin: str, aliases: dict[str, str]) -> tuple[str, dict[str, list[str]]]:
    """Split notes into the text before the first ``###`` and per-heading bodies."""
    heads = _find(SUBSECTION, text, origin)
    preamble = text[: heads[0].start() if heads else len(text)].strip("\n")
    bodies: dict[str, list[str]] = {}
    for i, head in enumerate(heads):
        title = head.group(1).strip()
        title = aliases.get(title, title)
        if title not in ALLOWED_SECTIONS:
            raise ValueError(f"unsupported {origin} section '### {head.group(1).strip()}'")
        stop = heads[i + 1].start() if i + 1 < len(heads) else len(text)
        body = text[head.end() : stop].strip("\n")
        if body.strip():
            bodies.setdefault(title, []).append(body)
    return preamble, bodies


# Only same-repository, explicit PR/issue/commit links can identify a duplicate.
# A generated primary PR link is adjacent to its commit link. A bare "refs"
# issue link is weaker and needs independent provenance and wording evidence.
_ENTRY = re.compile(r"^[-*] ", re.MULTILINE)
_OWN_PR = re.compile(r"https://github\.com/cubrid-lab/pycubrid/pull/([0-9]+)")
_OWN_ISSUE = re.compile(r"https://github\.com/cubrid-lab/pycubrid/issues/([0-9]+)")
_GENERATED_PR = re.compile(
    r"\(\[#(?P<number>[0-9]+)\]"
    r"\(https://github\.com/cubrid-lab/pycubrid/(?:issues|pull)/(?P=number)\)\)"
    r"\s+\(\[[0-9a-f]{7,40}\]"
    r"\(https://github\.com/cubrid-lab/pycubrid/commit/[0-9a-f]{7,40}\)\)"
)
_PRIMARY_CHANGE = re.compile(
    r"\(\[#[0-9]+\]\(https://github\.com/[^)\s]+/(?:issues|pull)/[0-9]+\)\)"
)
_OWN_COMMIT = re.compile(
    r"\(\[(?P<short>[0-9a-f]{7,40})\]"
    r"\(https://github\.com/cubrid-lab/pycubrid/commit/(?P<full>[0-9a-f]{7,40})\)\)"
)
_WORD = re.compile(r"[a-z][a-z0-9_]{2,}", re.IGNORECASE)
_COMMON = {
    "the",
    "and",
    "for",
    "with",
    "from",
    "that",
    "this",
    "are",
    "was",
    "now",
    "not",
    "how",
    "can",
    "via",
    "its",
    "into",
    "using",
    "when",
    "also",
    "make",
    "show",
    "docs",
    "documentation",
    "release",
    "refs",
    "github",
    "cubrid",
    "pycubrid",
    "issues",
    "pull",
    "commit",
}


def _unfenced_prose(notes: str) -> str:
    """Extract visible prose for matching, never links inside Markdown examples."""
    output: list[str] = []
    fence: str | None = None
    for line in notes.splitlines(keepends=True):
        found = FENCE.match(line)
        if found:
            marker = found.group(1)
            if fence is None:
                fence = marker
            elif (
                marker[0] == fence[0]
                and len(marker) >= len(fence)
                and not line[found.end() :].strip()
            ):
                fence = None
            continue
        if fence is None:
            output.append(line)
    return "".join(output)


def _entries(notes: str, origin: str) -> list[str]:
    """Find top-level bullets outside either Markdown fence style."""
    starts: list[int] = []
    fence: str | None = None
    pos = 0
    for line in notes.splitlines(keepends=True):
        marker_match = FENCE.match(line)
        if marker_match:
            marker = marker_match.group(1)
            if fence is None:
                fence = marker
            elif (
                marker[0] == fence[0]
                and len(marker) >= len(fence)
                and not line[marker_match.end() :].strip()
            ):
                fence = None
        elif fence is None and _ENTRY.match(line):
            starts.append(pos)
        pos += len(line)
    if fence is not None:
        raise ValueError(f"unclosed code fence in {origin}")
    return [
        notes[start : starts[i + 1] if i + 1 < len(starts) else len(notes)]
        for i, start in enumerate(starts)
    ]


def _words(notes: str) -> set[str]:
    """Compare wording only after fenced examples and URLs have been excluded."""
    prose = re.sub(r"https?://[^\s)]+", " ", notes)
    return {word for word in _WORD.findall(prose.lower()) if word not in _COMMON}


def _dedupe_generated(
    curated: dict[str, list[str]], generated: dict[str, list[str]]
) -> dict[str, list[str]]:
    """Conservatively omit generated copies of clearly matching curated changes.

    Use unique same-repository PR identities plus shared behavior wording, or
    unique issue-only references with local commit provenance and a stronger
    wording match. Also drop a bare generated bullet whose single commit is
    already listed, anywhere in the release, by exactly one bullet with a PR
    link. Any ambiguity leaves the original generated entry intact.
    """
    from collections import Counter

    curated_entries = [
        entry
        for blocks in curated.values()
        for block in blocks
        for entry in _entries(block, "curated entries")
    ]
    generated_entries = [
        entry
        for blocks in generated.values()
        for block in blocks
        for entry in _entries(block, "generated entries")
    ]
    curated_prose = [_unfenced_prose(entry) for entry in curated_entries]
    generated_prose = [_unfenced_prose(entry) for entry in generated_entries]
    curated_pr_count = Counter(
        number for prose in curated_prose for number in set(_OWN_PR.findall(prose))
    )
    generated_pr_count = Counter(
        number
        for prose in generated_prose
        for number in {match.group("number") for match in _GENERATED_PR.finditer(prose)}
    )
    curated_issue_count = Counter(
        number for prose in curated_prose for number in set(_OWN_ISSUE.findall(prose))
    )
    # Count every reference, including those attached to a separate PR bullet.
    # Otherwise a shared issue falsely appears to have one generated use.
    generated_issue_count = Counter(
        number for prose in generated_prose for number in set(_OWN_ISSUE.findall(prose))
    )

    def commit_shas(prose: str) -> set[str]:
        return {m.group("full") for m in _OWN_COMMIT.finditer(prose)}

    # Count *all* bullets naming a commit before deciding that a bare line is
    # redundant. One PR-linked bullet plus two distinct bare descriptions is
    # ambiguous; dropping either one could lose a separately documented change.
    generated_commit_count = Counter(sha for prose in generated_prose for sha in commit_shas(prose))

    def repeats_commit(entry: str) -> bool:
        # release-please can list one squash commit twice when its body carries a
        # second conventional-commit line: once with the PR reference, once bare.
        # Drop only a truly single-line bare entry, and only when exactly one
        # other generated entry in this release names the commit with a PR link.
        # Examine the original entry, including fenced examples/continuations.
        prose = _unfenced_prose(entry)
        shas = commit_shas(prose)
        if (
            len(shas) != 1
            or len(entry.strip().splitlines()) != 1
            or generated_commit_count[next(iter(shas))] != 2
            or _OWN_ISSUE.search(prose)
            or _OWN_PR.search(prose)
            or _PRIMARY_CHANGE.search(prose)
        ):
            return False
        sha = next(iter(shas))
        if not any(sha.startswith(m.group("short")) for m in _OWN_COMMIT.finditer(prose)):
            return False
        keepers = [
            other
            for other in generated_prose
            if other != prose and commit_shas(other) == shas and _GENERATED_PR.search(other)
        ]
        return len(keepers) == 1

    def is_duplicate(entry: str) -> bool:
        prose = _unfenced_prose(entry)
        if repeats_commit(entry):
            return True
        pr_ids = {match.group("number") for match in _GENERATED_PR.finditer(prose)}
        if len(pr_ids) == 1:
            number = next(iter(pr_ids))
            if curated_pr_count[number] != 1 or generated_pr_count[number] != 1:
                return False
            matches = [part for part in curated_prose if number in _OWN_PR.findall(part)]
            return len(matches) == 1 and len(_words(prose) & _words(matches[0])) >= 2
        if pr_ids or _PRIMARY_CHANGE.search(prose):
            return False

        issue_ids = set(_OWN_ISSUE.findall(prose))
        if len(issue_ids) != 1:
            return False
        issue = next(iter(issue_ids))
        if curated_issue_count[issue] != 1 or generated_issue_count[issue] != 1:
            return False
        if not re.search(
            rf"\brefs\s+\[#{issue}\]\(https://github\.com/cubrid-lab/pycubrid/issues/{issue}\)",
            prose,
            flags=re.IGNORECASE,
        ):
            return False
        # An issue reference alone does not prove a generated change. The link
        # must include an actual local commit SHA/link pair, not a foreign PR.
        if not any(
            match.group("full").startswith(match.group("short"))
            for match in _OWN_COMMIT.finditer(prose)
        ):
            return False
        matches = [part for part in curated_prose if issue in _OWN_ISSUE.findall(part)]
        return len(matches) == 1 and len(_words(prose) & _words(matches[0])) >= 3

    result: dict[str, list[str]] = {}
    for section, blocks in generated.items():
        retained: list[str] = []
        for block in blocks:
            heads = _find(_ENTRY, block, "generated entries")
            if not heads or block[: heads[0].start()].strip():
                # Preserve an independent section introduction; never silently
                # drop its text as a side effect of removing a nearby bullet.
                retained.append(block)
                continue
            entries = _entries(block, "generated entries")
            duplicates = {i for i, entry in enumerate(entries) if is_duplicate(entry)}
            if not duplicates:
                retained.append(block)
                continue
            # Each entry keeps its own trailing separator, so joining the kept
            # entries leaves a tight list tight and a loose list loose.
            if remaining := "".join(
                entry for i, entry in enumerate(entries) if i not in duplicates
            ).rstrip("\n"):
                retained.append(remaining)
        if retained:
            result[section] = retained
    return result


def compose(base: str, generated: str, version: str) -> str:
    """Pure, fail-closed and deterministic at a given generator base/input."""
    if not VERSION.fullmatch(version):
        raise ValueError("candidate version must be MAJOR.MINOR.PATCH")
    sections = _find(SECTION, base, "main CHANGELOG")
    if not sections or sections[0].group(1) != "Unreleased":
        raise ValueError("main CHANGELOG must start with [Unreleased]")
    if sum(s.group(1) == "Unreleased" for s in sections) != 1:
        raise ValueError("main CHANGELOG must have exactly one [Unreleased]")
    if any(s.group(1) == version for s in sections):
        raise ValueError("candidate already exists in main CHANGELOG")
    if len(sections) > 1:
        latest = sections[1].group(1)

        def key(value: str) -> tuple[int, ...]:
            return tuple(int(n) for n in value.split("."))

        if not VERSION.fullmatch(latest) or key(version) <= key(latest):
            raise ValueError("candidate must be newer than main's latest section")
    headers = _find(GENERATED, generated, "generated notes")
    if not headers or headers[0].group("version") != version:
        raise ValueError("generated top section must match the candidate")
    if sum(h.group("version") == version for h in headers) != 1:
        raise ValueError("duplicate generated candidate section")
    date = headers[0].group("date")
    dt.date.fromisoformat(date)
    stop = headers[1].start() if len(headers) > 1 else len(generated)
    notes = generated[headers[0].end() : stop].strip()
    if not notes or _find(HEADING_1_2, notes, "generated notes"):
        raise ValueError("empty or unsupported generated section")
    notes = _rewrite_references(notes)
    generated_preamble, generated_bodies = _split(notes, "generated", GENERATED_ALIASES)
    if generated_preamble.strip() or not generated_bodies:
        raise ValueError("generated notes must sit under ### headings")
    head = sections[0]
    history_start = sections[1].start() if len(sections) > 1 else len(base)
    curated_preamble, curated_bodies = _split(
        base[head.end() : history_start], "curated [Unreleased]", {}
    )
    generated_bodies = _dedupe_generated(curated_bodies, generated_bodies)
    # Merge curated and generated entries under one standard heading each, in the
    # standard order; curated text comes first and is copied unchanged.
    blocks = [curated_preamble] if curated_preamble.strip() else []
    for title in ALLOWED_SECTIONS:
        parts = curated_bodies.get(title, []) + generated_bodies.get(title, [])
        if parts:
            blocks.append(f"### {title}\n\n" + "\n\n".join(p.strip("\n") for p in parts))
    body = "\n\n".join(blocks)
    return (
        base[: head.end()]
        + f"\n\n## [{version}] - {date}\n\n"
        + body
        + ("\n\n" if history_start < len(base) else "\n")
        + base[history_start:]
    )


def rewrite_pr_body(pull_request: dict[str, object]) -> str | None:
    """Return the release PR body with rewritten references, or None if unchanged."""
    head = pull_request.get("head")
    ref = head.get("ref") if isinstance(head, dict) else None
    if not isinstance(ref, str) or not ref.startswith("release-please--branches--main"):
        raise ValueError("pull request is not a release-please release PR")
    body = pull_request.get("body") or ""
    if not isinstance(body, str):
        raise ValueError("pull request body must be text")
    rewritten = _rewrite_references(body)
    return None if rewritten == body else rewritten


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--base-changelog")
    parser.add_argument("--generated-changelog")
    parser.add_argument("--version")
    parser.add_argument("--output")
    parser.add_argument(
        "--pr-body",
        help="GitHub pull request JSON; write a {'body': ...} PATCH payload to --output "
        "only when the body changes",
    )
    args = parser.parse_args(argv)
    if args.pr_body is not None:
        others = (args.base_changelog, args.generated_changelog, args.version)
        if not args.pr_body or any(o is not None for o in others) or not args.output:
            parser.error("--pr-body takes a pull request JSON path and only --output")
        try:
            pull_request = json.loads(Path(args.pr_body).read_text(encoding="utf-8"))
            if not isinstance(pull_request, dict):
                raise ValueError("pull request JSON must be an object")
            body = rewrite_pr_body(pull_request)
        except (ValueError, OSError) as exc:
            print(f"::error::{exc}", file=sys.stderr)
            return 1
        output = Path(args.output)
        if body is None:
            output.unlink(missing_ok=True)
            print("release PR body unchanged")
        else:
            output.write_text(json.dumps({"body": body}), encoding="utf-8")
            print("release PR body references rewritten")
        return 0
    if not args.base_changelog or not args.version:
        parser.error("--base-changelog and --version are required")
    try:
        result = compose(
            Path(args.base_changelog).read_text(encoding="utf-8"),
            Path(args.generated_changelog or "RELEASE_CHANGELOG.md").read_text(encoding="utf-8"),
            args.version,
        )
    except (ValueError, OSError) as exc:
        print(f"::error::{exc}", file=sys.stderr)
        return 1
    Path(args.output or "CHANGELOG.md").write_text(result, encoding="utf-8")
    return 0


if __name__ == "__main__":
    sys.exit(main())
