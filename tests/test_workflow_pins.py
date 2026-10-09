"""Docs tools and bandit are pinned; scan workflows cancel only superseded PR runs (#782, #783)."""

from __future__ import annotations

import re
import tomllib
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
WORKFLOWS = ROOT / ".github/workflows"
pytestmark = pytest.mark.repo_tooling

REQUIREMENTS = "docs/requirements.txt"
PINNED_LINE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*==\d+(\.\d+)*$")
EXPECTED_GROUP = "${{ github.workflow }}-${{ github.event_name == 'pull_request' && github.ref || github.run_id }}"
EXPECTED_CANCEL = "${{ github.event_name == 'pull_request' }}"


def _runs(workflow: str) -> list[str]:
    jobs = yaml.safe_load((WORKFLOWS / workflow).read_text())["jobs"]
    return [s["run"] for job in jobs.values() for s in job.get("steps", []) if "run" in s]


def _pip_lines(workflow: str) -> list[str]:
    lines = [ln.strip() for run in _runs(workflow) for ln in run.splitlines()]
    return [ln for ln in lines if re.search(r"\bpip3?\s+install\b", ln)]


def test_docs_requirements_are_exact_pins() -> None:
    lines = [
        ln.strip()
        for ln in (ROOT / REQUIREMENTS).read_text().splitlines()
        if ln.strip() and not ln.strip().startswith("#")
    ]
    names = {ln.split("==")[0] for ln in lines}
    assert {"mkdocs", "mkdocs-material", "pymdown-extensions"} <= names
    for ln in lines:
        assert PINNED_LINE.match(ln), f"unpinned docs requirement: {ln}"


def test_docs_workflow_installs_only_from_pinned_file() -> None:
    installs = _pip_lines("docs.yml")
    assert installs, "docs.yml must install the docs tools"
    for ln in installs:
        assert ln == f"pip install -r {REQUIREMENTS}", ln


def test_no_workflow_builds_site_with_unpinned_tools() -> None:
    for wf in WORKFLOWS.glob("*.yml"):
        for ln in (ln for run in _runs(wf.name) for ln in run.splitlines()):
            if re.search(r"\bpip3?\s+install\b", ln) and "mkdocs" in ln:
                pytest.fail(f"{wf.name} installs mkdocs tooling outside the pinned file: {ln}")


def test_dependabot_covers_docs_requirements() -> None:
    cfg = yaml.safe_load((ROOT / ".github/dependabot.yml").read_text())
    dirs = {u["directory"] for u in cfg["updates"] if u["package-ecosystem"] == "pip"}
    assert "/docs" in dirs


@pytest.mark.parametrize("workflow", ["codeql.yml", "security.yml"])
def test_scan_concurrency_cancels_only_pull_requests(workflow: str) -> None:
    cfg = yaml.safe_load((WORKFLOWS / workflow).read_text())
    assert cfg["concurrency"] == {
        "group": EXPECTED_GROUP,
        "cancel-in-progress": EXPECTED_CANCEL,
    }


@pytest.mark.parametrize("workflow", ["security.yml", "maintenance.yml"])
def test_workflows_pin_bandit_to_pyproject(workflow: str) -> None:
    dev = tomllib.loads((ROOT / "pyproject.toml").read_text())["project"]["optional-dependencies"][
        "dev"
    ]
    pinned = next(d for d in dev if d.startswith("bandit"))
    assert pinned == "bandit[toml]==1.9.4" or re.fullmatch(r"bandit\[toml\]==[\d.]+", pinned)
    installs = [ln for ln in _pip_lines(workflow) if "bandit" in ln]
    assert installs, f"{workflow} must install bandit"
    for ln in installs:
        assert ln == f'pip install "{pinned}"', ln
