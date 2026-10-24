"""Acceptance check for the lab-volume-964 agent task (#925, from #964).

Usage: python acceptance.py TREE

Exits 0 when the tutorial in TREE removes the Docker lab together with its
anonymous volume, 1 when it does not, and 2 when TREE is not a directory.
It reads only the four tutorial pages and checks text, deterministically:

- every ``docker rm`` of ``pycubrid-quickstart`` forces the removal of the
  running lab and removes its volume (``-f``/``--force`` and ``-v``/``--volumes``,
  also bundled as ``-fv`` or ``-vf``);
- each page still shows at least one such removal command;
- each Finish Safely page explains ``-v`` together with the volume.

The explanation check is a textual proxy. Whether the wording is correct and
the English and Korean pages say the same thing stays a human review action.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

PAGES = (
    "docs/quickstart.md",
    "docs/ko/quickstart.md",
    "docs/finish-safely.md",
    "docs/ko/finish-safely.md",
)
VOLUME_WORD = {"docs/finish-safely.md": "volume", "docs/ko/finish-safely.md": "볼륨"}
REMOVAL = re.compile(r"docker\s+rm((?:\s+-[-\w]+)*)\s+pycubrid-quickstart\b")


def _options(flags: str) -> set[str]:
    """The long names of ``docker rm`` options, with short bundles such as ``-vf`` split."""
    names = {"f": "force", "v": "volumes", "l": "link"}
    options = set()
    for flag in flags.split():
        if flag.startswith("--"):
            options.add(flag[2:])
        else:
            options.update(names.get(letter, letter) for letter in flag[1:])
    return options


def problems(tree: Path) -> list[str]:
    found = []
    for page in PAGES:
        path = tree / page
        if not path.is_file():
            found.append(f"{page}: missing")
            continue
        text = path.read_text(encoding="utf-8")
        removals = REMOVAL.findall(text)
        if not removals:
            found.append(f"{page}: no docker rm of pycubrid-quickstart")
        for flags in removals:
            options = _options(flags)
            if "force" not in options:
                found.append(
                    f"{page}: docker rm{flags} pycubrid-quickstart cannot remove the running lab"
                )
            if "volumes" not in options:
                found.append(f"{page}: docker rm{flags} pycubrid-quickstart keeps the volume")
        word = VOLUME_WORD.get(page)
        if word and not any(
            "`-v`" in paragraph and word in paragraph.lower() for paragraph in text.split("\n\n")
        ):
            found.append(f"{page}: no paragraph explains that -v removes the volume")
    return found


def main(argv: list[str]) -> int:
    if len(argv) != 1 or not Path(argv[0]).is_dir():
        print("usage: python acceptance.py TREE", file=sys.stderr)
        return 2
    found = problems(Path(argv[0]))
    for problem in found:
        print(problem)
    return 1 if found else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
