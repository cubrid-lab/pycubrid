"""THIRD_PARTY_LICENSES.md stays consistent with pyproject.toml (#735).

The inventory is a generated snapshot; this check makes drift visible instead of
silent: every dependency of the dev/mutation dependency groups must appear (within its declared
range, unless it is an exact ``==`` pin, which pyproject.toml already records),
every row's category must be what the generator assigns to its
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
PYPROJECT = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
PROJECT = PYPROJECT["project"]
GROUPS = PYPROJECT["dependency-groups"]
pytestmark = pytest.mark.repo_tooling

_spec = importlib.util.spec_from_file_location(
    "generate_third_party_licenses", ROOT / "scripts" / "generate_third_party_licenses.py"
)
assert _spec and _spec.loader
generator = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(generator)

SECTIONS = {
    "dev": "## Development / test dependencies: `dev` dependency group",
    "mutation": "## Mutation-testing dependencies: `mutation` dependency group",
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


def requirements(group: str) -> list[Requirement]:
    # PEP 735: a group entry is a requirement string or an {include-group = ...} table.
    reqs = []
    for entry in GROUPS[group]:
        if isinstance(entry, dict):
            reqs += requirements(entry["include-group"])
        else:
            reqs.append(Requirement(entry))
    return reqs


@pytest.mark.parametrize("extra", sorted(SECTIONS))
def test_every_declared_dependency_is_inventoried_within_its_range(extra: str) -> None:
    rows = table(extra)
    assert len(rows) >= len(requirements(extra))
    for req in requirements(extra):
        name = canonicalize_name(req.name)
        assert name in rows, (
            f"{name} from the {extra} group is missing from THIRD_PARTY_LICENSES.md"
        )
        if any(s.operator in {"==", "==="} and "*" not in s.version for s in req.specifier):
            continue  # Exact pins are authoritative in pyproject.toml; bumps need no regen.
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


def test_reviewed_rows_keep_their_review() -> None:
    # Transitive rows are not declared in pyproject.toml; pin the reviewed one.
    assert table("dev")["docutils"]["category"] == "Needs review"


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
    assert "-e . --group dev" in record and "-e . --group mutation" in record


def _dist(**headers: object):
    """A fake distribution whose metadata is an email message, as field() expects."""
    import email.message
    from types import SimpleNamespace

    message = email.message.Message()
    for key, value in headers.items():
        for item in value if isinstance(value, list) else [value]:
            message[key] = str(item)
    return SimpleNamespace(metadata=message)


def test_license_expression_wins_over_classifiers_and_license_field() -> None:
    dist = _dist(
        **{
            "License-Expression": "MIT",
            "Classifier": "License :: OSI Approved :: BSD License",
            "License": "GPL",
        }
    )

    assert generator.license_of(dist) == "MIT"


def test_classifiers_drop_the_bare_osi_entry_and_are_sorted_and_deduplicated() -> None:
    dist = _dist(
        **{
            "Classifier": [
                "License :: OSI Approved :: MIT License",
                "License :: OSI Approved",
                "License :: OSI Approved :: MIT License",
                "License :: OSI Approved :: Apache Software License",
            ]
        }
    )

    assert generator.license_of(dist) == "Apache Software License / MIT License"


def test_a_short_single_line_license_field_is_used() -> None:
    assert generator.license_of(_dist(**{"License": "BSD-3-Clause"})) == "BSD-3-Clause"


@pytest.mark.parametrize(
    "text",
    ["MIT\nfull text", "x" * 61],
    ids=["multi-line", "longer-than-sixty"],
)
def test_unrecognised_license_text_is_never_guessed(text: str) -> None:
    assert generator.license_of(_dist(**{"License": text})) == "UNKNOWN"


def test_sixty_character_license_field_is_still_used() -> None:
    text = "x" * 60

    assert generator.license_of(_dist(**{"License": text})) == text


def test_no_license_metadata_is_unknown() -> None:
    assert generator.license_of(_dist()) == "UNKNOWN"


def test_project_url_prefers_a_known_label() -> None:
    dist = _dist(
        **{
            "Project-URL": [
                "Documentation, https://docs.invalid",
                "Source, https://source.invalid",
            ]
        }
    )

    assert generator.url_of(dist) == "https://source.invalid"


def test_project_url_label_is_case_insensitive_and_stripped() -> None:
    dist = _dist(**{"Project-URL": "  REPOSITORY , https://repo.invalid "})

    assert generator.url_of(dist) == "https://repo.invalid"


def test_home_page_is_the_fallback_before_the_dash() -> None:
    dist = _dist(
        **{
            "Project-URL": "Docs, https://docs.invalid",
            "Home-page": "https://home.invalid",
        }
    )

    assert generator.url_of(dist) == "https://home.invalid"


def test_no_url_metadata_reports_a_dash() -> None:
    assert generator.url_of(_dist()) == "-"
