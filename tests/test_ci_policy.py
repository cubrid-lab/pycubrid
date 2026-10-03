"""Exercise aggregate gates for selected, skipped and cancelled validation."""

from __future__ import annotations

import os
import json
import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
pytestmark = pytest.mark.repo_tooling


def workflow(name: str) -> dict:
    return yaml.safe_load((ROOT / ".github/workflows" / name).read_text())


def gate() -> dict:
    return workflow("ci.yml")["jobs"]["ci-gate"]


def run_gate(selected: set[str], results: dict[str, str]) -> subprocess.CompletedProcess:
    if shutil.which("bash") is None:
        pytest.skip("GitHub workflow shell requires bash")
    step = gate()["steps"][0]
    env = dict(os.environ)
    for job in gate()["needs"]:
        key = job.upper().replace("-", "_")
        env[f"R_{key}"] = results.get(job, "success" if job in selected else "skipped")
        env[f"E_{key}"] = "true" if job in selected else "false"
    return subprocess.run(
        ["bash", "-eo", "pipefail", "-c", step["run"]],
        env=env,
        text=True,
        capture_output=True,
        check=False,
        timeout=5,
    )


@pytest.mark.parametrize("result", ["failure", "cancelled", "skipped"])
@pytest.mark.parametrize("job", gate()["needs"])
def test_every_selected_job_must_succeed(job: str, result: str) -> None:
    completed = run_gate(set(gate()["needs"]), {job: result})
    assert completed.returncode != 0
    assert result in completed.stdout


def test_docs_only_intentional_skips_pass() -> None:
    completed = run_gate({"validate-target", "detect-changes", "lint", "doc-lint"}, {})
    assert completed.returncode == 0, completed.stdout + completed.stderr


@pytest.mark.parametrize("result", ["failure", "cancelled"])
def test_even_unselected_job_failure_is_not_hidden(result: str) -> None:
    completed = run_gate(
        {"validate-target", "detect-changes", "lint"}, {"integration-tests": result}
    )
    assert completed.returncode != 0


def test_ordinary_code_smoke_without_live_jobs_passes() -> None:
    selected = {"detect-changes", "lint", "offline-tests", "typecheck"}
    selected.add("compat-check")
    selected.add("validate-target")
    assert run_gate(selected, {}).returncode == 0


def test_full_release_call_is_preserved_without_automatic_schedule() -> None:
    full = workflow("integration-full.yml")
    events = full.get("on", full.get(True))
    assert set(events) == {"workflow_dispatch", "workflow_call"}
    assert events["workflow_call"]["inputs"]["sha"]["required"] is True
    matrix = full["jobs"]["integration-full"]["strategy"]["matrix"]
    assert len(matrix["python-version"]) == 5
    assert len(matrix["cubrid-version"]) == 4


def test_pr_smoke_is_separate_from_main_coverage_and_single_linux_lane() -> None:
    jobs = workflow("ci.yml")["jobs"]
    offline = jobs["offline-tests"]
    matrix = offline["strategy"]["matrix"]
    assert matrix["python-version"] == ["3.12"]
    assert matrix.get("os", [offline["runs-on"]]) == ["ubuntu-latest"]
    steps = offline["steps"]
    smoke = next(s for s in steps if s.get("name") == "Run representative PR smoke tests")
    coverage = next(s for s in steps if s.get("name") == "Run offline tests with coverage")
    assert smoke["if"] == (
        "github.event_name == 'pull_request' && needs.detect-changes.outputs.risk != 'true'"
    )
    regression = next(
        s
        for s in steps
        if s.get("name") == "Run relevant PR offline regressions with conservative fallback"
    )
    assert "outputs.risk == 'true'" in regression["if"]
    assert 'pytest tests/ -m "not integration and not repo_tooling"' in regression["run"]
    assert "--cov" not in regression["run"]
    assert coverage["if"] == "github.event_name != 'pull_request'"
    assert "--cov-fail-under=95" in coverage["run"]
    assert "--cov" not in smoke["run"]
    assert "outputs.live" in jobs["integration-tests"]["if"]
    assert "pull_request" in jobs["integration-tests"]["strategy"]["matrix"]


def test_sensitive_execution_paths_select_live_validation() -> None:
    filters = yaml.safe_load(
        workflow("ci.yml")["jobs"]["detect-changes"]["steps"][-1]["with"]["filters"]
    )
    assert "pycubrid/**" in filters["risk"]
    assert "tests/**" in filters["risk"]
    assert filters["code"][0].startswith("!{")
    assert "tests/helpers/tls_*.py" in filters["tls"]
    assert "tests/fixtures/tls/**" in filters["tls"]


@pytest.mark.parametrize(
    "path",
    [
        "tests/test_official_fixture_setup.py",
        "tests/test_upstream_scenario_ledger.py",
        "tests/fixtures/upstream_scenarios.csv",
    ],
)
def test_changed_tooling_regressions_select_the_tooling_lane(path: str) -> None:
    filters = yaml.safe_load(
        workflow("ci.yml")["jobs"]["detect-changes"]["steps"][-1]["with"]["filters"]
    )
    assert path in filters["tooling"]


def test_pr_smoke_paths_exist() -> None:
    import shlex

    steps = workflow("ci.yml")["jobs"]["offline-tests"]["steps"]
    smoke = next(s for s in steps if s.get("name") == "Run representative PR smoke tests")
    paths = [word for word in shlex.split(smoke["run"]) if word.endswith(".py")]
    assert paths
    assert all((ROOT / path).is_file() for path in paths)


@pytest.mark.parametrize("filename", ["ci.yml", "integration-full.yml"])
@pytest.mark.parametrize("phase", ["validate-target", "final"])
@pytest.mark.parametrize(
    "case,expected",
    [
        ("valid", 0),
        ("no-pr", 0),
        ("release", 0),
        ("release-recovery", 0),
        ("stale", 1),
        ("wrong-sha", 1),
        ("short-sha", 1),
        ("bad-pr", 1),
        ("wrong-repo", 1),
        ("closed", 1),
        ("api-error", 1),
    ],
)
def test_manual_validation_rejects_wrong_or_superseded_evidence(
    filename: str, phase: str, case: str, expected: int
) -> None:
    node = shutil.which("node")
    if node is None:
        pytest.skip("GitHub JavaScript guard requires Node.js")
    jobs = workflow(filename)["jobs"]
    final = "ci-gate" if filename == "ci.yml" else "full-matrix-result"
    step = jobs["validate-target" if phase == "validate-target" else final]["steps"][-1]
    script = step["with"]["script"]
    harness = r"""
const {script, kind} = JSON.parse(require('fs').readFileSync(0, 'utf8'));
const sha = 'a'.repeat(40);
const inputs = {sha, pr_number: '644'};
if (kind === 'no-pr') inputs.pr_number = '';
if (kind === 'release-recovery') {delete inputs.sha; delete inputs.pr_number; inputs.action = 'dry-run'; inputs.version = '1.9.0';}
if (kind === 'wrong-sha') inputs.sha = 'b'.repeat(40);
if (kind === 'short-sha') inputs.sha = 'aaaaaaa';
if (kind === 'bad-pr') inputs.pr_number = '0;unsafe';
const context = {eventName: kind === 'release' ? 'push' : 'workflow_dispatch',
  sha, repo: {owner: 'cubrid-lab', repo: 'pycubrid'}, payload: {inputs}};
const github = {rest: {pulls: {get: async () => {
  if (kind === 'api-error') throw new Error('API unavailable');
  return {data: {state: kind === 'closed' ? 'closed' : 'open',
    base: {repo: {full_name: kind === 'wrong-repo' ? 'wrong/repo' : 'cubrid-lab/pycubrid'}},
    head: {sha: kind === 'stale' ? 'b'.repeat(40) : sha}}};
}}}};
const summary = {addRaw() {return this;}, async write() {}};
const core = {summary};
const AsyncFunction = Object.getPrototypeOf(async function(){}).constructor;
new AsyncFunction('github', 'context', 'core', script)(github, context, core)
  .catch(e => {console.error(e.message); process.exitCode = 1;});
"""
    completed = subprocess.run(
        [node, "-e", harness],
        env={**os.environ, "RELEASE_CALL": "true" if case == "release-recovery" else "false"},
        input=json.dumps({"script": script, "kind": case}),
        capture_output=True,
        text=True,
        check=False,
        timeout=5,
    )
    if case == "release-recovery" and filename == "ci.yml":
        expected = 1
    assert completed.returncode == expected, completed.stderr


@pytest.mark.parametrize("filename", ["ci.yml", "integration-full.yml"])
def test_manual_validation_is_forced_and_preflight_precedes_execution(filename: str) -> None:
    data = workflow(filename)
    events = data.get("on", data.get(True))
    assert events["workflow_dispatch"]["inputs"]["sha"]["required"] is True
    jobs = data["jobs"]
    if filename == "integration-full.yml":
        assert events["workflow_call"]["inputs"]["release_call"]["default"] is True
        assert (
            jobs["validate-target"]["steps"][0]["env"]["RELEASE_CALL"]
            == "${{ inputs.release_call || false }}"
        )
    for name, job in jobs.items():
        if name == "validate-target":
            continue
        needs = job.get("needs", [])
        needs = [needs] if isinstance(needs, str) else needs
        assert "validate-target" in needs or "detect-changes" in needs, name
        for step in job.get("steps", []):
            if step.get("uses", "").startswith("actions/checkout@"):
                assert step["with"]["ref"] == "${{ inputs.sha || github.sha }}"
    if filename == "ci.yml":
        for key in ("code", "tooling", "live", "tls", "charset", "official"):
            assert (
                "github.event_name == 'workflow_dispatch' ||"
                in jobs["detect-changes"]["outputs"][key]
            )
