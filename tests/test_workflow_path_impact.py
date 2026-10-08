"""Changed-path selection follows a per-workflow impact table (#761).

Only ``ci.yml`` itself selects the live PR lanes: it defines and runs them. Every
other workflow selects the repository-tooling lane (policy and workflow tests),
because the live lanes of ``ci.yml`` do not execute that workflow's jobs.
``integration-full.yml`` changes are validated by a manual dispatch on the PR head.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
pytestmark = pytest.mark.repo_tooling

LIVE = {"risk", "tls", "charset", "official"}


def _filters() -> dict[str, list[str]]:
    steps = yaml.safe_load((ROOT / ".github/workflows/ci.yml").read_text())["jobs"][
        "detect-changes"
    ]["steps"]
    return yaml.safe_load(steps[-1]["with"]["filters"])


def _glob_re(pattern: str) -> re.Pattern[str]:
    """Translate a paths-filter (picomatch) glob into a regex for these tests."""
    out, i = "", 0
    while i < len(pattern):
        if pattern.startswith("**/", i):
            out, i = out + "(?:.*/)?", i + 3
        elif pattern.startswith("**", i):
            out, i = out + ".*", i + 2
        elif pattern[i] == "*":
            out, i = out + "[^/]*", i + 1
        elif pattern[i] == "?":
            out, i = out + "[^/]", i + 1
        elif pattern[i] == "{":
            end = pattern.index("}", i)
            alts = [_glob_re(alt).pattern for alt in pattern[i + 1 : end].split(",")]
            out, i = out + "(?:" + "|".join(alts) + ")", end + 1
        else:
            out, i = out + re.escape(pattern[i]), i + 1
    return re.compile(out)


def _selected(path: str) -> set[str]:
    groups = set()
    for group, patterns in _filters().items():
        for pattern in patterns:
            if pattern.startswith("!"):
                hit = not _glob_re(pattern[1:]).fullmatch(path)
            else:
                hit = bool(_glob_re(pattern).fullmatch(path))
            if hit:
                groups.add(group)
                break
    return groups


WORKFLOWS = sorted(p.name for p in (ROOT / ".github/workflows").glob("*.y*ml"))


@pytest.mark.parametrize("name", WORKFLOWS)
def test_workflow_impact_table(name: str) -> None:
    groups = _selected(f".github/workflows/{name}")
    # Every workflow is validated by the tooling lane (policy/workflow tests).
    assert "tooling" in groups, name
    if name == "ci.yml":
        assert LIVE <= groups, "ci.yml defines and runs the live lanes"
    else:
        assert not groups & LIVE, f"{name} does not run in ci.yml's live lanes"


@pytest.mark.parametrize(
    ("path", "expected"),
    [
        ("pycubrid/aio/connection.py", {"code", "risk", "tls", "charset"}),
        ("pycubrid/compat/__init__.py", {"code", "risk", "official"}),
        ("pycubrid/protocol.py", {"code", "risk", "tls", "charset", "official"}),
        ("tests/test_tls_matrix_offline.py", {"code", "risk", "tls"}),
        ("scripts/wait_for_cubrid.py", {"code", "risk", "tooling", "official"}),
        ("pyproject.toml", {"code", "risk", "tooling"}),
        ("docs/CI_POLICY.md", {"docs"}),
        ("README.md", {"docs"}),
    ],
)
def test_representative_paths_select_the_expected_tiers(path: str, expected: set[str]) -> None:
    assert _selected(path) == expected, path


def _marks_repo_tooling(decorators: list[ast.expr]) -> bool:
    return any("repo_tooling" in ast.unparse(d) for d in decorators)


def test_tests_that_read_workflows_run_in_the_tooling_lane() -> None:
    # Non-ci.yml workflow changes select only the tooling lane on PRs, so any test
    # that reads a workflow file must be marked repo_tooling or it runs nowhere.
    offenders = []
    for path in sorted((ROOT / "tests").glob("test_*.py")):
        source = path.read_text()
        if ".github/workflows" not in source:
            continue
        tree = ast.parse(source)
        module_marked = any(
            isinstance(node, ast.Assign)
            and any(getattr(t, "id", "") == "pytestmark" for t in node.targets)
            and "repo_tooling" in ast.unparse(node.value)
            for node in tree.body
        )
        if module_marked:
            continue
        for node in tree.body:
            if not isinstance(node, ast.FunctionDef):
                continue
            if ".github/workflows" not in (ast.get_source_segment(source, node) or ""):
                continue
            if not node.name.startswith("test_"):
                offenders.append(f"{path.name}:{node.name} (helper; mark the module)")
            elif not _marks_repo_tooling(node.decorator_list):
                offenders.append(f"{path.name}:{node.name}")
    assert not offenders, offenders
