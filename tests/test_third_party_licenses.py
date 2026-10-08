"""THIRD_PARTY_LICENSES.md stays consistent with pyproject.toml (#735).

The inventory is a generated snapshot; this check makes drift visible instead of
silent: every declared dev/mutation dependency must appear, exact pins must match,
and every MPL or "Needs review" row must be explained in the prose.
"""

from __future__ import annotations

import re
import tomllib
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
DOC = (ROOT / "THIRD_PARTY_LICENSES.md").read_text(encoding="utf-8")
PROJECT = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))["project"]
pytestmark = pytest.mark.repo_tooling

SECTIONS = {
    "dev": "## Development / test dependencies: `.[dev]`",
    "mutation": "## Mutation-testing dependencies: `.[mutation]`",
}


def canonical(name: str) -> str:
    return re.sub(r"[-_.]+", "-", name).lower()


def table(extra: str) -> dict[str, dict[str, str]]:
    start = DOC.index(SECTIONS[extra])
    end = DOC.find("\n## ", start + 1)
    end = len(DOC) if end == -1 else end
    rows = {}
    for line in DOC[start:end].splitlines():
        cells = [c.strip() for c in line.strip().strip("|").split("|")]
        if len(cells) != 5 or cells[0] in {"Name", "---"} or set(cells[0]) <= {"-"}:
            continue
        rows[canonical(cells[0])] = {"version": cells[1], "license": cells[2], "category": cells[3]}
    return rows


def requirements(extra: str) -> list[tuple[str, str | None]]:
    parsed = []
    for req in PROJECT["optional-dependencies"][extra]:
        match = re.match(r"\s*([A-Za-z0-9_.-]+)(\[[^\]]*\])?\s*(==\s*([^\s,;]+))?", req)
        assert match, req
        parsed.append((canonical(match.group(1)), match.group(4)))
    return parsed


@pytest.mark.parametrize("extra", sorted(SECTIONS))
def test_every_declared_dependency_is_inventoried_with_its_exact_pin(extra: str) -> None:
    rows = table(extra)
    assert len(rows) >= len(requirements(extra))
    for name, pin in requirements(extra):
        assert name in rows, f"{name} from .[{extra}] is missing from THIRD_PARTY_LICENSES.md"
        if pin is not None:
            assert rows[name]["version"] == pin, f"{name} is pinned to {pin} in pyproject.toml"


def test_every_review_and_mpl_row_is_explained() -> None:
    categories = DOC[DOC.index("## License categories") : DOC.index("## Reference test suite")]
    reviewed = categories[categories.index("### Reviewed entries") :]
    for extra in SECTIONS:
        for name, row in table(extra).items():
            if row["category"] == "Needs review":
                assert re.search(rf"\*\*{re.escape(name)}\*\*", reviewed, re.I), name
            elif row["category"].startswith("Weak copyleft"):
                assert f"`{name}`" in categories, f"MPL package {name} not named in the prose"
            else:
                assert row["category"] == "Permissive", (name, row)


def test_no_blanket_permissive_claim_and_runtime_dependency_is_explicit() -> None:
    for stale in ("No dependency is copyleft", "All listed dependencies are distributed under"):
        assert stale not in DOC
    assert "tzdata; sys_platform == 'win32'" in DOC
    assert PROJECT["dependencies"] == ["tzdata; sys_platform == 'win32'"]


def test_generation_inputs_are_recorded() -> None:
    record = DOC[DOC.index("## How the inventories were generated") :]
    assert re.search(r"commit `[0-9a-f]{40}`", record)
    assert re.search(r"CPython 3\.\d+\.\d+ on Linux", record)
    assert "scripts/generate_third_party_licenses.py --exclude pycubrid" in record
    assert '-e ".[dev]"' in record and '-e ".[mutation]"' in record
