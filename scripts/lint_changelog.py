#!/usr/bin/env python3
"""Validate CHANGELOG.md structure.
Checks:
    1. First section is [Unreleased]
    2. Exactly one [Unreleased] section
    3. No duplicate version sections
    4. Released versions in descending semver order
    5. No duplicate subsections within a version section
Usage:
    python scripts/lint_changelog.py

Checks:
    1. First section is [Unreleased]
    2. No duplicate version sections
    3. Versions in descending semver order
    4. No duplicate subsections within a version section

Exit codes:
    0 — changelog is valid
    1 — structural error found
"""

from __future__ import annotations

import re
import sys
from pathlib import Path


def main() -> int:
    changelog = Path(__file__).resolve().parent.parent / "CHANGELOG.md"
    if not changelog.exists():
        print(f"ERROR: {changelog} not found", file=sys.stderr)
        return 1

    content = changelog.read_text(encoding="utf-8")
    headers = re.findall(r"^## \[(\S+)\]", content, re.MULTILINE)

    if not headers:
        print("ERROR: No version sections found (expected '## [X.Y.Z]')", file=sys.stderr)
        return 1

    # Rule 1: First header must be [Unreleased]
    if headers[0] != "Unreleased":
        print(
            f"ERROR: First section must be [Unreleased], got [{headers[0]}]",
            file=sys.stderr,
        )
        return 1

    # Rule 2: Exactly one [Unreleased] section
    unreleased_count = headers.count("Unreleased")
    if unreleased_count > 1:
        print(
            f"ERROR: Found {unreleased_count} [Unreleased] sections, expected exactly 1",
            file=sys.stderr,
        )
        return 1

    versions = [h for h in headers if h != "Unreleased"]

    if not versions:
        print("WARNING: No released versions in CHANGELOG (only [Unreleased])")
        return 0

    # Rule 3: No duplicate version sections
    seen: set[str] = set()
    for v in versions:
        if v in seen:
            print(f"ERROR: Duplicate version section [{v}]", file=sys.stderr)
            return 1
        seen.add(v)

    # Rule 4: Released versions in descending semver order
    # Validate per-version so one bad entry doesn't disable all checking.
    try:
        from packaging.version import InvalidVersion, Version

        prev_version: Version | None = None
        prev_name: str | None = None
        for v in versions:
            try:
                current = Version(v)
            except InvalidVersion:
                print(
                    f"WARNING: [{v}] is not valid PEP 440, skipping ordering check for this entry",
                    file=sys.stderr,
                )
                continue

            if prev_version is not None and current > prev_version:
                print(
                    f"ERROR: [{v}] should come before [{prev_name}] "
                    f"(versions must be in descending order)",
                    file=sys.stderr,
                )
                return 1
            prev_version = current
            prev_name = v
    except ImportError:
        print("NOTE: 'packaging' not installed, skipping semver ordering check")

    # Rule 5: No duplicate subsections within a version section
    current_version: str | None = None
    subsections: set[str] = set()
    for line in content.splitlines():
        v_match = re.match(r"^## \[(\S+)\]", line)
        if v_match:
            current_version = v_match.group(1)
            subsections.clear()
            continue

        if current_version is not None:
            sub_match = re.match(r"^###\s+(.+)$", line)
            if sub_match:
                sub_title = sub_match.group(1).strip()
                if sub_title in subsections:
                    print(
                        f"ERROR: Duplicate subsection '### {sub_title}' in [{current_version}]",
                        file=sys.stderr,
                    )
                    return 1
                subsections.add(sub_title)

    print(f"OK: {len(versions)} version(s), order valid")
    return 0


if __name__ == "__main__":
    sys.exit(main())