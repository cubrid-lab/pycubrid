"""Every executing workflow job carries an explicit, bounded timeout (#758).

GitHub's default job limit is 360 minutes, so a hung container, socket or
install would hold a runner for six hours. Jobs that call a reusable workflow
cannot set ``timeout-minutes``; repo-local callees are checked through their
own executing jobs, and externally owned callees are an explicit allowlist.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
WORKFLOWS = ROOT / ".github/workflows"
pytestmark = pytest.mark.repo_tooling

MAX_TIMEOUT_MINUTES = 180
GATE_MAX_TIMEOUT_MINUTES = 10

# Reusable workflows owned outside this repository. Their timeouts are set (or
# tracked) in the owning repository, not here. Keep entries exact.
EXTERNAL_REUSABLE_CALLERS = {
    ("ci.yml", "doc-lint"): "cubrid-lab/.github/.github/workflows/doc-lint.yml",
    ("codeql.yml", "analyze"): "cubrid-lab/.github/.github/workflows/codeql.yml",
    ("publish-pypi.yml", "verify-cookbook"): (
        "cubrid-lab/cubrid-cookbook-python/.github/workflows/smoke-test.yml"
    ),
}

GATES = {("ci.yml", "ci-gate"), ("integration-full.yml", "full-matrix-result")}


def _jobs() -> list[tuple[str, str, dict]]:
    found = []
    for path in sorted(WORKFLOWS.glob("*.yml")):
        for name, job in yaml.safe_load(path.read_text())["jobs"].items():
            found.append((path.name, name, job))
    return found


EXECUTING = [(wf, name, job) for wf, name, job in _jobs() if "uses" not in job]
CALLERS = [(wf, name, job) for wf, name, job in _jobs() if "uses" in job]


def _params(jobs: list[tuple[str, str, dict]]) -> list:
    return [pytest.param(wf, name, job, id=f"{wf}:{name}") for wf, name, job in jobs]


def test_every_job_is_either_executing_or_a_reusable_caller() -> None:
    for wf, name, job in EXECUTING:
        assert "runs-on" in job, f"{wf}:{name} has neither runs-on nor uses"


@pytest.mark.parametrize(("wf", "name", "job"), _params(EXECUTING))
def test_executing_job_has_bounded_integer_timeout(wf: str, name: str, job: dict) -> None:
    timeout = job.get("timeout-minutes")
    assert isinstance(timeout, int) and not isinstance(timeout, bool), (
        f"{wf}:{name} must set an integer timeout-minutes (default would be 360)"
    )
    assert 1 <= timeout <= MAX_TIMEOUT_MINUTES, f"{wf}:{name} timeout {timeout} out of range"


@pytest.mark.parametrize(("wf", "name", "job"), _params(CALLERS))
def test_reusable_caller_is_local_or_allowlisted(wf: str, name: str, job: dict) -> None:
    target = job["uses"]
    if target.startswith("./.github/workflows/"):
        callee = ROOT / target.removeprefix("./")
        assert callee.is_file(), f"{wf}:{name} calls missing {target}"
        for callee_name, callee_job in yaml.safe_load(callee.read_text())["jobs"].items():
            if "uses" not in callee_job:
                assert isinstance(callee_job.get("timeout-minutes"), int), (
                    f"{callee.name}:{callee_name} (called from {wf}:{name}) needs a timeout"
                )
        return
    expected = EXTERNAL_REUSABLE_CALLERS.get((wf, name))
    assert expected is not None, f"{wf}:{name} calls external {target}; add it to the allowlist"
    assert target.split("@", 1)[0] == expected


def test_allowlist_has_no_stale_entries() -> None:
    callers = {(wf, name) for wf, name, _ in CALLERS}
    assert set(EXTERNAL_REUSABLE_CALLERS) <= callers


@pytest.mark.parametrize(("wf", "name"), sorted(GATES))
def test_aggregate_gates_have_short_timeouts(wf: str, name: str) -> None:
    job = yaml.safe_load((WORKFLOWS / wf).read_text())["jobs"][name]
    assert job.get("timeout-minutes", 0) <= GATE_MAX_TIMEOUT_MINUTES
    # A timed-out dependency reports `cancelled`; the gate must still run and fail.
    assert "always()" in str(job.get("if", ""))
