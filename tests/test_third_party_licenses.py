"""THIRD_PARTY_LICENSES.md stays consistent with pyproject.toml (#735).

The inventory is a generated snapshot; this check makes drift visible instead of
silent: every declared dev/mutation dependency must appear at a version its declared
range allows, every row's category must be what the generator assigns to its
license, and every MPL or "Needs review" row must be explained in the prose.
"""

from __future__ import annotations

import importlib.util
import re
import tomllib
from pathlib import Path

import pytest
from packaging.requirements import Requirement
from packaging.utils import canonicalize_name

ROOT = Path(__file__).resolve().parents[1]
DOC = (ROOT / "THIRD_PARTY_LICENSES.md").read_text(encoding="utf-8")
PROJECT = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))["project"]
pytestmark = pytest.mark.repo_tooling

_spec = importlib.util.spec_from_file_location(
    "generate_third_party_licenses", ROOT / "scripts" / "generate_third_party_licenses.py"
)
assert _spec and _spec.loader
generator = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(generator)

SECTIONS = {
    "dev": "## Development / test dependencies: `.[dev]`",
    "mutation": "## Mutation-testing dependencies: `.[mutation]`",
}


def table(extra: str) -> dict[str, dict[str, str]]:
    start = DOC.index(SECTIONS[extra])
    end = DOC.find("\n## ", start + 1)
    end = len(DOC) if end == -1 else end
    rows = {}
    for line in DOC[start:end].splitlines():
        cells = [c.strip() for c in line.strip().strip("|").split("|")]
        if len(cells) != 5 or cells[0] in {"Name", "---"} or set(cells[0]) <= {"-"}:
            continue
        rows[canonicalize_name(cells[0])] = {
            "version": cells[1],
            "license": cells[2],
            "category": cells[3],
        }
    return rows


def requirements(extra: str) -> list[Requirement]:
    return [Requirement(req) for req in PROJECT["optional-dependencies"][extra]]


@pytest.mark.parametrize("extra", sorted(SECTIONS))
def test_every_declared_dependency_is_inventoried_within_its_range(extra: str) -> None:
    rows = table(extra)
    assert len(rows) >= len(requirements(extra))
    for req in requirements(extra):
        name = canonicalize_name(req.name)
        assert name in rows, f"{name} from .[{extra}] is missing from THIRD_PARTY_LICENSES.md"
        version = rows[name]["version"]
        assert req.specifier.contains(version, prereleases=True), (
            f"{name} {version} is outside {req.specifier} declared in pyproject.toml"
        )


@pytest.mark.parametrize("extra", sorted(SECTIONS))
def test_every_category_matches_the_generator(extra: str) -> None:
    for name, row in table(extra).items():
        assert generator.category(row["license"]) == row["category"], (name, row)


@pytest.mark.parametrize(
    ("license_text", "expected"),
    [
        ("MIT", "Permissive"),
        ("Apache-2.0 OR BSD-2-Clause", "Permissive"),
        ("Mozilla Public License 2.0 (MPL 2.0)", "Weak copyleft (MPL)"),
        ("MIT AND MPL2", "Weak copyleft (MPL)"),
        ("MIT or GPLv3", "Needs review"),
        ("BSD / LGPLv2+", "Needs review"),
        ("MIT AND CC-BY-SA-4.0", "Needs review"),
        ("Apache-2.0 AND SSPL-1.0", "Needs review"),
        ("UNKNOWN", "Needs review"),
    ],
)
def test_generator_never_calls_a_partly_unknown_license_permissive(
    license_text: str, expected: str
) -> None:
    assert generator.category(license_text) == expected


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
