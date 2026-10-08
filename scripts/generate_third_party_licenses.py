"""Print the third-party license inventory of the current Python environment.

Run it with the interpreter of an isolated environment that has exactly the
dependency set to inventory installed (#735)::

    uv venv -p 3.12 /tmp/tpl-dev
    uv pip install -p /tmp/tpl-dev/bin/python -e ".[dev]"
    /tmp/tpl-dev/bin/python scripts/generate_third_party_licenses.py --exclude pycubrid

Only the standard library is used, so the generator itself never appears in the
inventory. A license is read from the PEP 639 ``License-Expression`` field, then
from ``License ::`` classifiers, then from a short ``License`` field; anything
else is reported as ``UNKNOWN`` rather than guessed. Any GPL-family mention, or
an unrecognised license, is categorised ``Needs review`` and must be resolved in
THIRD_PARTY_LICENSES.md from the package's own license files.
"""

from __future__ import annotations

import argparse
import importlib.metadata as metadata
import re
import sys

PERMISSIVE = re.compile(
    r"\b(MIT|BSD|Apache|ISC|PSF|Python Software Foundation|Unlicense|0BSD)\b", re.IGNORECASE
)
MPL = re.compile(r"\b(MPL|Mozilla Public License)\b", re.IGNORECASE)
# Any GPL-family mention needs a human reading of the package's license files:
# multiple classifiers do not say whether they combine as OR or AND.
GPL_FAMILY = re.compile(r"\b(A?GPL|LGPL|General Public License)\b", re.IGNORECASE)


def field(dist: metadata.Distribution, key: str) -> str:
    values = dist.metadata.get_all(key) or []
    return str(values[0]).strip() if values else ""


def canonical(name: str) -> str:
    return re.sub(r"[-_.]+", "-", name).lower()


def license_of(dist: metadata.Distribution) -> str:
    expression = field(dist, "License-Expression")
    if expression:
        return expression
    classifiers = [
        c.split("::")[-1].strip()
        for c in dist.metadata.get_all("Classifier") or []
        if c.startswith("License ::") and c.split("::")[-1].strip() != "OSI Approved"
    ]
    if classifiers:
        return " / ".join(sorted(set(classifiers)))
    text = field(dist, "License")
    if text and "\n" not in text and len(text) <= 60:
        return text
    return "UNKNOWN"


def category(license_text: str) -> str:
    if GPL_FAMILY.search(license_text):
        return "Needs review"
    if MPL.search(license_text):
        return "Weak copyleft (MPL-2.0)"
    if PERMISSIVE.search(license_text):
        return "Permissive"
    return "Needs review"


def url_of(dist: metadata.Distribution) -> str:
    for entry in dist.metadata.get_all("Project-URL") or []:
        label, _, url = str(entry).partition(",")
        if label.strip().lower() in {"homepage", "home", "source", "repository", "source code"}:
            return url.strip()
    return field(dist, "Home-page") or "-"


def rows(exclude: set[str]) -> list[tuple[str, str, str, str, str]]:
    seen: dict[str, tuple[str, str, str, str, str]] = {}
    for dist in metadata.distributions():
        name = field(dist, "Name")
        if not name or canonical(name) in exclude:
            continue
        lic = license_of(dist)
        seen[canonical(name)] = (name, dist.version, lic, category(lic), url_of(dist))
    order = {"Permissive": 0, "Weak copyleft (MPL-2.0)": 1, "Needs review": 2}
    return sorted(seen.values(), key=lambda r: (order[r[3]], canonical(r[0])))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("--exclude", action="append", default=[], help="distribution to skip")
    args = parser.parse_args(argv)
    exclude = {canonical(e) for e in args.exclude}
    print("| Name | Version | License | Category | URL |")
    print("|---|---|---|---|---|")
    for name, version, lic, cat, url in rows(exclude):
        print(f"| {name} | {version} | {lic} | {cat} | {url} |")
    return 0


if __name__ == "__main__":
    sys.exit(main())
