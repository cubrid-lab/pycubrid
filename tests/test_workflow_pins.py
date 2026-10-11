"""Docs tools and bandit are pinned; scan workflows cancel only superseded PR runs (#782, #783).

Since #784 every contributor tool is declared once, in a pyproject.toml PEP 735
dependency group, and workflows install the group instead of repeating a pin.
"""

from __future__ import annotations

import re
import tomllib
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
WORKFLOWS = ROOT / ".github/workflows"
pytestmark = pytest.mark.repo_tooling

PYPROJECT = tomllib.loads((ROOT / "pyproject.toml").read_text())
GROUPS = PYPROJECT["dependency-groups"]
PINNED = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*(\[[a-z0-9,]+\])?==\d+(\.\d+)*$")
EXPECTED_GROUP = "${{ github.workflow }}-${{ github.event_name == 'pull_request' && github.ref || github.run_id }}"
EXPECTED_CANCEL = "${{ github.event_name == 'pull_request' }}"


def _runs(workflow: str) -> list[str]:
    jobs = yaml.safe_load((WORKFLOWS / workflow).read_text())["jobs"]
    return [s["run"] for job in jobs.values() for s in job.get("steps", []) if "run" in s]


def _pip_lines(workflow: str) -> list[str]:
    lines = [ln.strip() for run in _runs(workflow) for ln in run.splitlines()]
    return [ln for ln in lines if re.search(r"\bpip3?\s+install\b", ln)]


def test_published_metadata_has_no_contributor_extras() -> None:
    # #784: test, lint, docs and release tools stay out of the wheel metadata.
    assert "optional-dependencies" not in PYPROJECT["project"]
    assert {"test", "lint", "security", "release", "docs", "mutation", "dev"} <= set(GROUPS)


def test_docs_group_is_exact_pins() -> None:
    names = {req.split("==")[0] for req in GROUPS["docs"]}
    assert {"mkdocs", "mkdocs-material", "pymdown-extensions"} <= names
    for req in GROUPS["docs"]:
        assert PINNED.match(req), f"unpinned docs requirement: {req}"


def test_docs_workflow_installs_only_the_docs_group() -> None:
    installs = _pip_lines("docs.yml")
    assert installs, "docs.yml must install the docs tools"
    for ln in installs:
        assert ln in {"python -m pip install --upgrade pip", "pip install --group docs"}, ln
    assert "pip install --group docs" in installs


def test_no_workflow_builds_site_with_unpinned_tools() -> None:
    for wf in WORKFLOWS.glob("*.yml"):
        for ln in (ln for run in _runs(wf.name) for ln in run.splitlines()):
            if re.search(r"\bpip3?\s+install\b", ln) and "mkdocs" in ln:
                pytest.fail(f"{wf.name} installs mkdocs tooling outside the docs group: {ln}")


def test_dependabot_root_pip_entry_covers_the_dependency_groups() -> None:
    cfg = yaml.safe_load((ROOT / ".github/dependabot.yml").read_text())
    dirs = {u["directory"] for u in cfg["updates"] if u["package-ecosystem"] == "pip"}
    assert dirs == {"/"}
    assert not (ROOT / "docs/requirements.txt").exists(), "docs pins live in pyproject.toml"


@pytest.mark.parametrize("workflow", ["codeql.yml", "security.yml"])
def test_scan_concurrency_cancels_only_pull_requests(workflow: str) -> None:
    cfg = yaml.safe_load((WORKFLOWS / workflow).read_text())
    assert cfg["concurrency"] == {
        "group": EXPECTED_GROUP,
        "cancel-in-progress": EXPECTED_CANCEL,
    }


def test_bandit_is_pinned_once_and_workflows_install_its_group() -> None:
    (pinned,) = GROUPS["security"]
    assert re.fullmatch(r"bandit\[toml\]==[\d.]+", pinned)
    assert {"include-group": "security"} in GROUPS["dev"]
    security = [ln for ln in _pip_lines("security.yml") if "group" in ln]
    assert security == ["pip install --group security"]
    assert _pip_lines("maintenance.yml")[-1] == "pip install -e . --group dev"
    for workflow in ("security.yml", "maintenance.yml"):
        assert not [ln for ln in _pip_lines(workflow) if "bandit" in ln], workflow


def test_dependabot_groups_only_minor_and_patch_version_updates() -> None:
    # #750: one grouped PR per ecosystem/directory; majors, security updates and
    # the runtime dependency tzdata keep their own PRs.
    cfg = yaml.safe_load((ROOT / ".github/dependabot.yml").read_text())
    groups = {(u["package-ecosystem"], u["directory"]): u.get("groups") for u in cfg["updates"]}
    assert set(groups) == {("pip", "/"), ("github-actions", "/")}
    for key, group in groups.items():
        assert group is not None and len(group) == 1, key
        (spec,) = group.values()
        assert spec["applies-to"] == "version-updates", key
        assert spec["patterns"] == ["*"], key
        assert spec["update-types"] == ["minor", "patch"], key
    assert groups[("pip", "/")]["dev-tools"]["exclude-patterns"] == ["tzdata"]


def test_every_artifact_upload_sets_a_retention_period() -> None:
    # Without retention-days an artifact is kept for the repository default
    # (90 days); every upload states its own period instead (#750).
    missing = []
    for wf in sorted(WORKFLOWS.glob("*.yml")):
        jobs = yaml.safe_load(wf.read_text()).get("jobs", {})
        for job_id, job in jobs.items():
            for step in job.get("steps", []):
                uses = step.get("uses", "")
                if uses.startswith("actions/upload-artifact@"):
                    if "retention-days" not in step.get("with", {}):
                        missing.append(f"{wf.name}:{job_id}:{step.get('name', uses)}")
                if uses.startswith("anchore/sbom-action@"):
                    if step.get("with", {}).get("upload-artifact") is not False:
                        missing.append(f"{wf.name}:{job_id}: sbom-action uploads its own artifact")
    assert missing == []
