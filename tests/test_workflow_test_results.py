"""CI publishes and uploads ``test-results/`` for the offline and integration lanes (#1024).

The lanes that #1028 defines (``test``, ``integration``, ``integration-tls``) write their
JUnit XML under ``test-results/<lane>/`` in CI, append the one-line summary to the job
summary and upload ``test-results/`` on every run, pass or fail, without adding
permissions. Test selection, coverage files and the lane checks are pinned elsewhere.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import pytest
import yaml

pytestmark = pytest.mark.repo_tooling

ROOT = Path(__file__).resolve().parents[1]
WORKFLOWS = ROOT / ".github/workflows"
UPLOAD = "actions/upload-artifact@cf430e030ddbb5b0abf93d22962f4752f3646cd9"  # v7
SHA_PIN = re.compile(r"^[\w.-]+/[\w./-]+@[0-9a-f]{40}$")

# (workflow, job, lane, matrix keys the artifact name must carry)
JOBS = [
    ("ci.yml", "offline-tests", "test", ["python-version"]),
    ("ci.yml", "integration-tests", "integration", ["python-version", "cubrid-version"]),
    ("ci.yml", "integration-tls", "integration-tls", ["python-version", "cubrid-version"]),
    ("integration-full.yml", "offline-endpoints", "test", ["python-version"]),
    ("integration-full.yml", "integration-full", "integration",
     ["python-version", "cubrid-version"]),
    ("integration-full.yml", "integration-tls", "integration-tls",
     ["python-version", "cubrid-version"]),
]  # fmt: skip
PARAMS = [pytest.param(*j, id=f"{j[0]}:{j[1]}") for j in JOBS]


def _steps(wf: str, job: str) -> list[dict[str, Any]]:
    data = yaml.safe_load((WORKFLOWS / wf).read_text())
    return list(data["jobs"][job]["steps"])


def _index(steps: list[dict[str, Any]], needle: str) -> int:
    return next(
        i for i, s in enumerate(steps) if needle in str(s.get("run", "")) + str(s.get("uses", ""))
    )


@pytest.mark.parametrize(("wf", "job", "lane", "keys"), PARAMS)
def test_pytest_writes_its_junit_under_the_lane_directory(
    wf: str, job: str, lane: str, keys: list[str]
) -> None:
    pytest_steps = [s for s in _steps(wf, job) if "python -m pytest" in str(s.get("run", ""))]
    assert pytest_steps
    for step in pytest_steps:
        assert f"--junitxml=test-results/{lane}/junit.xml" in step["run"], step["name"]


@pytest.mark.parametrize(("wf", "job", "lane", "keys"), PARAMS)
def test_summary_step_follows_the_tests_and_always_runs(
    wf: str, job: str, lane: str, keys: list[str]
) -> None:
    steps = _steps(wf, job)
    last_pytest = max(i for i, s in enumerate(steps) if "python -m pytest" in str(s.get("run", "")))
    at = _index(steps, "report_results.py step-summary")
    summary = steps[at]
    assert at > last_pytest
    assert summary["if"] == "always()"
    run = summary["run"]
    assert f"report_results.py summarize --lane {lane} --results-dir test-results/{lane}" in run
    assert f"step-summary --results-dir test-results/{lane}" in run
    # Numeric 0 is falsy in Actions expressions, so `success && 0 || 1` is always 1.
    assert "job.status != 'success' && 1 || 0" in run  # the lane keeps pytest's own status
    assert "&& 0 ||" not in run
    assert run.rstrip().endswith('>> "$GITHUB_STEP_SUMMARY"')


@pytest.mark.parametrize(("wf", "job", "lane", "keys"), PARAMS)
def test_upload_step_is_pinned_always_and_uniquely_named(
    wf: str, job: str, lane: str, keys: list[str]
) -> None:
    steps = _steps(wf, job)
    uploads = [s for s in steps if "test-results/" == str(s.get("with", {}).get("path"))]
    assert len(uploads) == 1
    upload = uploads[0]
    assert upload["uses"] == UPLOAD and SHA_PIN.match(upload["uses"])
    assert upload["if"] == "always()"
    assert _index(steps, "report_results.py step-summary") < steps.index(upload)
    with_ = upload["with"]
    assert with_["if-no-files-found"] == "warn"
    assert 1 <= with_["retention-days"] <= 14
    for key in keys:  # one artifact per matrix entry, or entries would collide
        assert f"matrix.{key}" in with_["name"]
    assert with_["name"].startswith(f"test-results-{lane}")


def test_artifact_names_are_unique_within_each_workflow() -> None:
    for wf in ("ci.yml", "integration-full.yml"):
        names = [
            # The name templates themselves: two jobs with one template would collide.
            with_["name"]
            for w, job, *_ in JOBS
            if w == wf
            for with_ in [
                next(
                    s["with"]
                    for s in _steps(w, job)
                    if str(s.get("with", {}).get("path")) == "test-results/"
                )
            ]
        ]
        assert len(names) == len(set(names))


def test_coverage_report_and_codecov_upload_are_kept() -> None:
    steps = {s.get("name"): s for s in _steps("ci.yml", "offline-tests")}
    run = steps["Run offline tests with coverage"]["run"]
    assert "--cov-report=xml:coverage-py${{ matrix.python-version }}.xml" in run
    assert "--cov-fail-under=95" in run
    assert steps["Upload coverage to Codecov"]["with"]["files"] == (
        "./coverage-py${{ matrix.python-version }}.xml"
    )
    assert (
        "--coverage coverage-py${{ matrix.python-version }}.xml"
        in (steps["Publish test results summary"]["run"])
    )


# The job-level permissions that existed before #1024; no job gained or changed one.
JOB_PERMISSIONS = {
    "ci.yml": {"detect-changes"},
    "integration-full.yml": set(),
}


@pytest.mark.parametrize("wf", ["ci.yml", "integration-full.yml"])
def test_no_permissions_were_added(wf: str) -> None:
    data = yaml.safe_load((WORKFLOWS / wf).read_text())
    with_permissions = {n for n, job in data["jobs"].items() if "permissions" in job}
    assert with_permissions <= JOB_PERMISSIONS[wf]
    for name in with_permissions:
        assert "write" not in data["jobs"][name]["permissions"].values(), name
    assert "write" not in data.get("permissions", {}).values()
