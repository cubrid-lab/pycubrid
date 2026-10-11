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


# "" is what GitHub reports for a needed job that produced no result.
@pytest.mark.parametrize("result", ["failure", "cancelled", "skipped", ""])
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
    assert matrix["python-version"] == ["3.11", "3.12", "3.13", "3.14"]
    tls = full["jobs"]["integration-tls"]["strategy"]["matrix"]
    assert tls["python-version"] == ["3.11", "3.14"]
    cells = workflow("ci.yml")["jobs"]["integration-tests"]["strategy"]["matrix"]
    assert '{"python-version":"3.11","cubrid-version":"10.2"}' in cells
    assert '"3.10"' not in cells
    assert len(matrix["cubrid-version"]) == 4


def test_pr_smoke_is_separate_from_main_coverage_and_single_linux_lane() -> None:
    jobs = workflow("ci.yml")["jobs"]
    offline = jobs["offline-tests"]
    matrix = offline["strategy"]["matrix"]
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


LIVE_JOBS = ("integration-tests", "integration-charset", "integration-tls", "official-differential")


@pytest.mark.parametrize("name", LIVE_JOBS)
def test_live_lanes_start_without_waiting_for_static_and_offline_jobs(name: str) -> None:
    # #760: live lanes run in parallel with lint/typecheck/offline-tests, but keep
    # validate-target (manual SHA/PR-head verification) and detect-changes.
    needs = workflow("ci.yml")["jobs"][name]["needs"]
    assert needs == ["validate-target", "detect-changes"], name


def test_gate_still_requires_static_and_offline_jobs() -> None:
    needs = set(gate()["needs"])
    assert {"lint", "typecheck", "offline-tests", *LIVE_JOBS} <= needs


# --- #745: offline endpoint versions --------------------------------------

EVENTS = ("pull_request", "push", "schedule", "workflow_dispatch")


def evaluate(expression: str, event: str, risk: str) -> object:
    """Evaluate the small GitHub expression subset used by offline-tests routing.

    GitHub's ``&&``/``||`` return an operand like Python's ``and``/``or``, and
    strings are truthy when non-empty, so translating the operators is exact
    for these expressions. Unknown contexts fail loudly with ``NameError``.
    """
    expr = expression.strip()
    if expr.startswith("${{"):
        expr = expr.removeprefix("${{").removesuffix("}}")
    expr = (
        expr.replace("needs.detect-changes.outputs.risk", "risk")
        .replace("github.event_name", "event")
        .replace("fromJSON", "from_json")
        .replace("&&", " and ")
        .replace("||", " or ")
    )
    names = {"event": event, "risk": risk, "from_json": json.loads}
    return eval(expr, {"__builtins__": {}}, names)  # noqa: S307 - fixed workflow text


def offline_cells(event: str, risk: str) -> dict[str, list[str]]:
    """Return {python-version: [test step names that run]} for one event."""
    job = workflow("ci.yml")["jobs"]["offline-tests"]
    versions = evaluate(job["strategy"]["matrix"]["python-version"], event, risk)
    assert isinstance(versions, list)
    tests = [s for s in job["steps"] if "python -m pytest" in s.get("run", "")]
    return {
        version: [s["name"] for s in tests if evaluate(s["if"], event, risk)]
        for version in versions
    }


FULL = "Run offline tests with coverage"
RISK = "Run relevant PR offline regressions with conservative fallback"
SMOKE = "Run representative PR smoke tests"


@pytest.mark.parametrize(
    "event,risk,expected",
    [
        ("pull_request", "false", {"3.12": [SMOKE]}),
        ("pull_request", "", {"3.12": [SMOKE]}),
        ("pull_request", "true", {"3.11": [RISK], "3.14": [RISK]}),
        ("push", "false", {"3.11": [FULL], "3.14": [FULL]}),
        ("push", "true", {"3.11": [FULL], "3.14": [FULL]}),
        ("schedule", "false", {"3.11": [FULL], "3.14": [FULL]}),
        ("schedule", "true", {"3.11": [FULL], "3.14": [FULL]}),
        ("workflow_dispatch", "false", {"3.11": [FULL], "3.14": [FULL]}),
    ],
)
def test_offline_matrix_routes_events_to_endpoint_versions(
    event: str, risk: str, expected: dict[str, list[str]]
) -> None:
    assert offline_cells(event, risk) == expected


@pytest.mark.parametrize("risk", ["true", "false"])
@pytest.mark.parametrize("event", EVENTS)
def test_every_offline_cell_runs_exactly_one_marker_scoped_suite(event: str, risk: str) -> None:
    job = workflow("ci.yml")["jobs"]["offline-tests"]
    runs = {s["name"]: s["run"] for s in job["steps"] if "python -m pytest" in s.get("run", "")}
    supported = {"3.11", "3.12", "3.13", "3.14"}
    for version, steps in offline_cells(event, risk).items():
        assert version in supported
        assert len(steps) == 1, (event, risk, version, steps)
        assert '-m "not integration and not repo_tooling"' in runs[steps[0]]
        if steps[0] != SMOKE:
            assert "pytest tests/" in runs[steps[0]]


SETUP_ACTIONS = ("actions/setup-python@", "astral-sh/setup-uv@")


def test_offline_job_keeps_timeout_sha_and_pinned_actions() -> None:
    job = workflow("ci.yml")["jobs"]["offline-tests"]
    assert job["timeout-minutes"] == 15
    assert job["strategy"]["fail-fast"] is False
    assert job["if"] == "needs.detect-changes.outputs.code == 'true'"
    interpreters = [s for s in job["steps"] if str(s.get("uses", "")).startswith(SETUP_ACTIONS)]
    assert len(interpreters) == len(SETUP_ACTIONS)
    for step in interpreters:
        # The cell's interpreter is the point of the endpoint matrix (#745).
        assert step["with"]["python-version"] == "${{ matrix.python-version }}", step["uses"]
    for step in job["steps"]:
        uses = step.get("uses")
        if uses:
            assert len(uses.split("@", 1)[1]) == 40, uses
        if uses and uses.startswith("actions/checkout@"):
            assert step["with"]["ref"] == "${{ inputs.sha || github.sha }}"
    assert workflow("ci.yml")["permissions"] == {
        "contents": "read",
        "pull-requests": "read",
    }


def test_offline_coverage_names_are_unique_per_python_version() -> None:
    steps = {s.get("name"): s for s in workflow("ci.yml")["jobs"]["offline-tests"]["steps"]}
    report = "coverage-py${{ matrix.python-version }}.xml"
    assert f"--cov-report=xml:{report}" in steps[FULL]["run"]
    artifact = steps["Upload coverage report artifact"]
    assert artifact["with"]["name"] == "offline-coverage-py${{ matrix.python-version }}"
    assert artifact["with"]["path"] == report
    codecov_step = steps["Upload coverage to Codecov"]
    for upload in (artifact, codecov_step):
        assert "github.event_name != 'pull_request'" in upload["if"], upload["name"]
    codecov = codecov_step["with"]
    assert codecov["files"] == f"./{report}"
    assert codecov["flags"] == "offline-py${{ matrix.python-version }}"
    assert codecov["name"] == "offline-py${{ matrix.python-version }}"


def test_gate_expects_offline_tests_whenever_code_is_selected() -> None:
    env = gate()["steps"][0]["env"]
    assert env["E_OFFLINE_TESTS"] == "${{ needs.detect-changes.outputs.code }}"
    assert env["R_OFFLINE_TESTS"] == "${{ needs.offline-tests.result }}"
    assert gate()["if"] == "always()"


ENDPOINT_RUN = {"validate-target", "detect-changes", "lint", "typecheck", "compat-check"}


@pytest.mark.parametrize("result", ["failure", "cancelled", "skipped"])
def test_failed_cancelled_or_missing_endpoint_cell_fails_the_gate(result: str) -> None:
    # A matrix job reports one aggregate result: any failed or cancelled 3.11/3.14
    # cell makes it non-success, and a job that never ran is "skipped".
    completed = run_gate(ENDPOINT_RUN | {"offline-tests"}, {"offline-tests": result})
    assert completed.returncode != 0
    assert f"offline-tests expected success, got {result}" in completed.stdout


def test_successful_endpoint_cells_pass_the_gate() -> None:
    completed = run_gate(ENDPOINT_RUN | {"offline-tests"}, {})
    assert completed.returncode == 0, completed.stdout + completed.stderr


# --- #750: release offline evidence, weekly reuse and schedule spread ------

FULL_GATE = workflow("integration-full.yml")["jobs"]["full-matrix-result"]


def run_full_gate(results: dict[str, str]) -> subprocess.CompletedProcess:
    if shutil.which("bash") is None:
        pytest.skip("GitHub workflow shell requires bash")
    script = FULL_GATE["steps"][0]["run"]
    for job in FULL_GATE["needs"]:
        script = script.replace("${{ needs." + job + ".result }}", results.get(job, "success"))
    assert "${{" not in script, "full-matrix-result reads a context the harness does not set"
    return subprocess.run(
        ["bash", "-eo", "pipefail", "-c", script],
        text=True,
        capture_output=True,
        check=False,
        timeout=5,
    )


def test_release_gate_passes_when_every_dependency_succeeds() -> None:
    completed = run_full_gate({})
    assert completed.returncode == 0, completed.stdout + completed.stderr


# "" is the result GitHub reports for a dependency that never produced one.
@pytest.mark.parametrize("result", ["failure", "cancelled", "skipped", ""])
@pytest.mark.parametrize("job", FULL_GATE["needs"])
def test_release_gate_rejects_any_non_success_dependency(job: str, result: str) -> None:
    assert run_full_gate({job: result}).returncode != 0


def test_release_runs_the_offline_endpoint_cells_itself() -> None:
    # #750: the release no longer relies on a cancellable main push run for #745.
    full = workflow("integration-full.yml")["jobs"]
    job = full["offline-endpoints"]
    assert "if" not in job
    assert job["needs"] == "validate-target"
    assert job["strategy"]["fail-fast"] is False
    assert job["strategy"]["matrix"] == {"python-version": list(offline_cells("push", "false"))}
    ci_steps = workflow("ci.yml")["jobs"]["offline-tests"]["steps"]

    def run_of(steps: list, name: str) -> str:
        return next(s for s in steps if s.get("name") == name)["run"]

    for name in ("Install dependencies", FULL):
        assert run_of(job["steps"], name) == run_of(ci_steps, name), name
    assert "--cov-fail-under=95" in run_of(job["steps"], FULL)
    assert "offline-endpoints" in FULL_GATE["needs"]
    assert "needs.offline-endpoints.result" in FULL_GATE["steps"][0]["run"]
    assert workflow("publish-pypi.yml")["jobs"]["matrix"]["uses"] == (
        "./.github/workflows/integration-full.yml"
    )


def _normalise_release_copy(steps: list) -> list:
    """Steps with the per-workflow checkout credentials and setup-uv cache mode removed."""
    normalised = []
    for step in steps:
        step = dict(step)
        uses = str(step.get("uses", ""))
        if uses.startswith("actions/checkout@"):
            step["with"] = {k: v for k, v in step["with"].items() if k != "persist-credentials"}
        elif uses.startswith("astral-sh/setup-uv@"):
            step["with"] = {k: v for k, v in step["with"].items() if k != "enable-cache"}
        normalised.append(step)
    return normalised


# #750: release-run copies of ci.yml lanes the release used to take from the
# (cancellable, never-required) main push run of ci.yml.
RELEASE_COPIES = ("lint", "typecheck", "compat-check", "repo-tooling-tests")


@pytest.mark.parametrize("name", RELEASE_COPIES)
def test_release_runs_the_ci_lane_itself_with_the_same_steps(name: str) -> None:
    full = workflow("integration-full.yml")["jobs"]
    ci = workflow("ci.yml")["jobs"][name]
    job = full[name]
    # Only validate-target may gate the copy; a skip there fails the release gate.
    assert job["needs"] == "validate-target"
    assert "if" not in job and "continue-on-error" not in job
    assert not any("if" in s or "continue-on-error" in s for s in job["steps"])
    for key in ("name", "strategy", "runs-on", "timeout-minutes"):
        assert job.get(key) == ci.get(key), key
    checkout = job["steps"][0]
    assert checkout["uses"].startswith("actions/checkout@")
    assert checkout["with"] == {
        "ref": "${{ inputs.sha || github.sha }}",
        "persist-credentials": False,
    }
    assert _normalise_release_copy(job["steps"]) == _normalise_release_copy(ci["steps"])
    assert name in FULL_GATE["needs"]
    assert f'"${{{{ needs.{name}.result }}}}" != "success"' in FULL_GATE["steps"][0]["run"]


def test_release_gate_needs_every_release_lane() -> None:
    assert set(FULL_GATE["needs"]) == {
        "validate-target",
        "integration-full",
        "integration-tls",
        "integration-charset",
        "version-differential",
        "official-differential",
        "offline-endpoints",
        *RELEASE_COPIES,
    }
    jobs = set(workflow("integration-full.yml")["jobs"]) - {"full-matrix-result"}
    assert jobs == set(FULL_GATE["needs"]), "every release job must feed the gate"


DETECT = workflow("ci.yml")["jobs"]["detect-changes"]
CODE_FAMILY = ("code", "live", "extended", "tls", "charset", "official")


def detect_output(key: str, event: str, filt: dict[str, str], reuse: dict[str, str]) -> bool:
    """Evaluate one detect-changes output expression for given step outputs."""
    expr = DETECT["outputs"][key].strip().removeprefix("${{").removesuffix("}}")
    names: dict[str, str] = {"event": event}
    for prefix, values in (("filter", filt), ("reuse", reuse)):
        for name in ("code", "tooling", "risk", "tls", "charset", "official", "docs"):
            names[f"{prefix}_{name}"] = values.get(name, "")
            expr = expr.replace(f"steps.{prefix}.outputs.{name}", f"{prefix}_{name}")
    expr = expr.replace("github.event_name", "event").replace("&&", " and ").replace("||", " or ")
    return bool(eval(expr, {"__builtins__": {}}, names))  # noqa: S307 - fixed workflow text


ALL_CHANGED = {name: "true" for name in ("code", "tooling", "risk", "tls", "charset", "official")}


@pytest.mark.parametrize("key", CODE_FAMILY)
def test_weekly_run_skips_code_lanes_only_when_proven_at_the_same_sha(key: str) -> None:
    assert detect_output(key, "schedule", ALL_CHANGED, {}) is True
    assert detect_output(key, "schedule", ALL_CHANGED, {"code": "false"}) is True
    assert detect_output(key, "schedule", ALL_CHANGED, {"code": "true"}) is False
    # Proven tooling never hides code lanes.
    assert detect_output(key, "schedule", ALL_CHANGED, {"tooling": "true"}) is True


def test_weekly_run_skips_tooling_only_when_proven_at_the_same_sha() -> None:
    assert detect_output("tooling", "schedule", ALL_CHANGED, {"code": "true"}) is True
    assert detect_output("tooling", "schedule", ALL_CHANGED, {"tooling": "true"}) is False


@pytest.mark.parametrize("event", ["pull_request", "push", "workflow_dispatch"])
@pytest.mark.parametrize("key", [*CODE_FAMILY, "tooling"])
def test_reuse_cannot_change_other_events(event: str, key: str) -> None:
    # The reuse step runs only on schedule; elsewhere its outputs are empty.
    reuse_step = next(s for s in DETECT["steps"] if s.get("id") == "reuse")
    assert reuse_step["if"] == "github.event_name == 'schedule'"
    for filt in (ALL_CHANGED, {}):
        assert detect_output(key, event, filt, {}) == detect_output(
            key, event, filt, {"code": "", "tooling": ""}
        )


def test_detect_changes_alone_gains_read_only_actions_access() -> None:
    jobs = workflow("ci.yml")["jobs"]
    assert DETECT["permissions"] == {
        "contents": "read",
        "pull-requests": "read",
        "actions": "read",
    }
    for name, job in jobs.items():
        if name != "detect-changes":
            assert "actions" not in job.get("permissions", {}), name


def run_reuse(runs: object, jobs: object) -> tuple[int, dict[str, str], str]:
    node = shutil.which("node")
    if node is None:
        pytest.skip("GitHub JavaScript guard requires Node.js")
    script = next(s for s in DETECT["steps"] if s.get("id") == "reuse")["with"]["script"]
    harness = r"""
const {script, runs, jobs} = JSON.parse(require('fs').readFileSync(0, 'utf8'));
const sha = 'a'.repeat(40);
const context = {eventName: 'schedule', sha, repo: {owner: 'cubrid-lab', repo: 'pycubrid'}};
const outputs = {}; const warnings = [];
const core = {setOutput: (k, v) => { outputs[k] = v; }, info() {}, warning: m => warnings.push(m)};
const listWorkflowRuns = 'runs'; const listJobsForWorkflowRun = 'jobs';
const github = {rest: {actions: {listWorkflowRuns, listJobsForWorkflowRun}},
  paginate: async (method, params) => {
    if (method === 'runs') {
      if (runs === 'error') throw new Error('API unavailable');
      if (params.head_sha !== sha || params.event !== 'push' || params.branch !== 'main'
          || params.status !== 'success')
        throw new Error('unexpected query ' + JSON.stringify(params));
      return runs;
    }
    if (jobs === 'error') throw new Error('jobs unavailable');
    if (params.filter !== 'latest') throw new Error('unexpected query ' + JSON.stringify(params));
    return jobs[params.run_id] || [];
  }};
const AsyncFunction = Object.getPrototypeOf(async function(){}).constructor;
new AsyncFunction('github', 'context', 'core', script)(github, context, core)
  .then(() => console.log(JSON.stringify({outputs, warnings})))
  .catch(e => {console.error(e.message); process.exitCode = 1;});
"""
    completed = subprocess.run(
        [node, "-e", harness],
        input=json.dumps({"script": script, "runs": runs, "jobs": jobs}),
        capture_output=True,
        text=True,
        check=False,
        timeout=5,
    )
    data = json.loads(completed.stdout or "{}")
    return completed.returncode, data.get("outputs", {}), " ".join(data.get("warnings", []))


SHA = "a" * 40


def push_run(run_id: int, sha: str = SHA, conclusion: str = "success") -> dict:
    return {
        "id": run_id,
        "head_sha": sha,
        "event": "push",
        "conclusion": conclusion,
        "html_url": f"https://example.invalid/{run_id}",
    }


def job(name: str, conclusion: str = "success") -> dict:
    return {"name": name, "conclusion": conclusion}


CODE_RUN_JOBS = [
    job("offline-tests (ubuntu-latest, 3.11)"),
    job("offline-tests (ubuntu-latest, 3.14)"),
]


@pytest.mark.parametrize(
    "runs,jobs,expected",
    [
        ([push_run(1)], {1: CODE_RUN_JOBS}, {"code": "true", "tooling": "false"}),
        (
            [push_run(1)],
            {1: [*CODE_RUN_JOBS, job("repo-tooling-tests (ubuntu-latest)")]},
            {"code": "true", "tooling": "true"},
        ),
        # A docs-only push run of this SHA ran no offline cell: not proven.
        (
            [push_run(1)],
            {1: [job("lint"), job("validate-target")]},
            {"code": "false", "tooling": "false"},
        ),
        ([], {}, {"code": "false", "tooling": "false"}),
        # Defensive: a run of another SHA or a non-success run never counts.
        ([push_run(1, sha="b" * 40)], {1: CODE_RUN_JOBS}, {"code": "false", "tooling": "false"}),
        (
            [push_run(1, conclusion="cancelled")],
            {1: CODE_RUN_JOBS},
            {"code": "false", "tooling": "false"},
        ),
        # One failed or skipped cell means the family is not proven.
        (
            [push_run(1)],
            {
                1: [
                    job("offline-tests (ubuntu-latest, 3.11)"),
                    job("offline-tests (ubuntu-latest, 3.14)", "failure"),
                ]
            },
            {"code": "false", "tooling": "false"},
        ),
        (
            [push_run(1)],
            {1: [job("offline-tests (ubuntu-latest, 3.11)", "skipped")]},
            {"code": "false", "tooling": "false"},
        ),
    ],
)
def test_reuse_proves_a_family_only_from_a_successful_same_sha_push_run(
    runs: list, jobs: dict, expected: dict[str, str]
) -> None:
    code, outputs, warnings = run_reuse(runs, {str(k): v for k, v in jobs.items()})
    assert code == 0
    assert outputs == expected
    assert not warnings


def test_reuse_lookup_failure_selects_the_lanes() -> None:
    code, outputs, warnings = run_reuse("error", {})
    assert code == 0
    assert outputs == {"code": "false", "tooling": "false"}
    assert "API unavailable" in warnings


def test_reuse_jobs_listing_failure_selects_the_lanes() -> None:
    code, outputs, warnings = run_reuse([push_run(1)], "error")
    assert code == 0
    assert outputs == {"code": "false", "tooling": "false"}
    assert "jobs unavailable" in warnings


def cron(name: str) -> str:
    data = workflow(name)
    (entry,) = data.get("on", data.get(True))["schedule"]
    return entry["cron"]


def test_weekly_ci_and_bug_hunt_run_on_different_days() -> None:
    # #750: a later cron start does not mean the earlier run finished.
    ci_day = cron("ci.yml").split()[4]
    bug_hunt_day = cron("bug-hunt.yml").split()[4]
    assert ci_day != bug_hunt_day
    assert cron("bug-hunt.yml") == "0 4 * * 4"


def test_bug_hunt_keeps_its_activity_guard_and_non_pr_triggers() -> None:
    data = workflow("bug-hunt.yml")
    assert set(data.get("on", data.get(True))) == {"schedule", "workflow_dispatch"}
    jobs = data["jobs"]
    for name, body in jobs.items():
        if name != "activity":
            assert body["if"] == "needs.activity.outputs.changed == 'true'", name
    assert "7 days ago" in jobs["activity"]["steps"][-1]["run"]


# --- #786: documentation site build on pull requests ------------------------


def test_docs_build_job_mirrors_docs_workflow_and_stays_build_only() -> None:
    job = workflow("ci.yml")["jobs"]["docs-build"]
    assert job["needs"] == "detect-changes"
    assert job["if"] == "needs.detect-changes.outputs.site == 'true'"
    assert job["timeout-minutes"] == 10
    assert "permissions" not in job, "inherits the read-only workflow permissions"
    assert "environment" not in job
    steps = job["steps"]
    checkout = next(s for s in steps if s["uses"].startswith("actions/checkout@"))
    assert checkout["with"]["persist-credentials"] is False
    assert checkout["with"]["ref"] == "${{ inputs.sha || github.sha }}"
    for step in steps:
        if "uses" in step:
            assert len(step["uses"].split("@", 1)[1].split()[0]) == 40, step["uses"]
            assert "pages" not in step["uses"], "PRs never upload or deploy a Pages artifact"
    runs = [s["run"] for s in steps if "run" in s]
    assert "uv pip install --system -r docs/requirements.txt" in runs[0]
    # Same order and commands as docs.yml's build job.
    docs_runs = [s["run"] for s in workflow("docs.yml")["jobs"]["build"]["steps"] if "run" in s]
    assert (
        runs[1:]
        == docs_runs[1:]
        == [
            "python scripts/generate_llms_full.py",
            "mkdocs build --strict",
        ]
    )
    assert docs_runs[0] == "pip install -r docs/requirements.txt"


def test_docs_site_selection_covers_every_site_input() -> None:
    filters = yaml.safe_load(
        workflow("ci.yml")["jobs"]["detect-changes"]["steps"][-1]["with"]["filters"]
    )
    assert set(filters["site"]) == {
        "docs/**",  # content, nav assets and docs/requirements.txt
        "mkdocs.yml",
        "scripts/generate_llms_full.py",
        ".github/workflows/docs.yml",
        ".github/workflows/ci.yml",
    }


def test_docs_site_output_is_forced_only_for_manual_dispatch() -> None:
    out = DETECT["outputs"]["site"]
    assert "github.event_name == 'workflow_dispatch' ||" in out
    assert "steps.filter.outputs.site == 'true'" in out
    assert "reuse" not in out


def test_gate_expects_docs_build_exactly_when_the_site_is_selected() -> None:
    env = gate()["steps"][0]["env"]
    assert env["E_DOCS_BUILD"] == "${{ needs.detect-changes.outputs.site }}"
    assert env["R_DOCS_BUILD"] == "${{ needs.docs-build.result }}"
    assert "docs-build" in gate()["needs"]
    assert 'check docs-build "$R_DOCS_BUILD" "$E_DOCS_BUILD"' in gate()["steps"][0]["run"]


def test_docs_only_change_that_selects_the_site_passes_only_with_a_green_build() -> None:
    base = {"validate-target", "detect-changes", "lint", "doc-lint", "docs-build"}
    assert run_gate(base, {}).returncode == 0
    for result in ("failure", "cancelled", "skipped"):
        completed = run_gate(base, {"docs-build": result})
        assert completed.returncode != 0
        assert f"docs-build expected success, got {result}" in completed.stdout
    # Not selected: a skipped build passes, a failed one still blocks.
    assert run_gate(base - {"docs-build"}, {}).returncode == 0
    assert run_gate(base - {"docs-build"}, {"docs-build": "failure"}).returncode != 0
