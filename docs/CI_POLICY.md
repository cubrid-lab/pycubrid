# CI execution policy

Routine CI uses representative combinations instead of a Cartesian version/OS matrix.

| Trigger | Runtime validation |
| --- | --- |
| Documentation-only PR | Documentation and policy checks; no runtime suite or CUBRID provisioning |
| Ordinary code PR | One Ubuntu/Python 3.12 offline smoke lane; no full coverage claim |
| High-risk PR | One full offline regression lane without coverage plus Python 3.14/CUBRID 11.4; targeted additional lanes where relevant |
| Code push to main | One Ubuntu/Python 3.12 full offline suite with the existing 95% coverage floor; oldest/newest live endpoints |
| Monday 03:00 UTC | Same representative policy, comparing changes in the previous seven days; unchanged/docs-only history does not select runtime tests |
| Explicit full dispatch or release | Existing full Python 3.11–3.14 × CUBRID 10.2/11.0/11.2/11.4 integration workflow and mandatory release lanes |

The PR smoke suite is deliberately bounded. Contributors must run the regression
checks relevant to their change locally and record commands/results in the PR.
Passing smoke is not evidence that the whole offline suite or coverage floor ran.
`make test` and `make integration` remain available without changing their scope.

Change selection is in `ci.yml`'s `detect-changes` job. Non-documentation paths
are code by default, so new source/configuration files do not silently become docs.
All driver and test paths, including new modules, plus dependency, build, script
and workflow changes conservatively select representative pre-merge integration
and the existing offline regression suite on one Linux/Python lane. Other code
changes retain the bounded smoke suite. Repository tooling tests run in one Linux lane when tooling changes.
Changes to `tests/test_official_fixture_setup.py`, `tests/test_upstream_scenario_ledger.py`
or `tests/fixtures/upstream_scenarios.csv` explicitly select that same tooling lane;
these marker-based checks are excluded from the driver offline lane.
The static lint job continues on all events, including generated documentation checks.

The aggregate required-check name stays stable and includes change detection.
Selected jobs must succeed: skipped, failed or cancelled selected checks fail the
gate. Only intentionally unselected jobs may skip. Keep branch protection on the
aggregate gate; do not require every old matrix cell name after reducing matrices.
Branch protection must be inspected before merge. No protection-setting change
is part of this PR.

Full verification has no automatic nightly schedule. `integration-full.yml`
retains manual dispatch and the release `workflow_call` with the immutable candidate
SHA. Both manual workflows require a full `sha` input matching the dispatched
branch commit. Supply `pr_number` when collecting PR evidence: preflight and the
final gate reject a closed PR, an API failure or a superseded head. Every checkout
uses the immutable requested/run SHA, which the guard reports in the summary.
Routine CI manual dispatch forces all representative runtime/tooling lanes, even
with an empty default-branch diff; the full workflow forces the full matrix. No release publisher/generator is changed.

Concurrency isolates event/ref groups and still cancels superseded PR runs. Main pushes and PR merge refs are
not assumed to have identical SHAs. Weekly change selection uses the previous
seven days, not a persisted last-success cache; a failed weekly run must be
rerun or followed by manual validation rather than treated as successful evidence.

GitHub charges have not been attributed to a workflow or runner SKU. Reduced job
counts establish less repeated work; they are not a measured monetary saving.
Compare subsequent Actions jobs/runner minutes and the actual billing category
before making a cost claim.

The deep bug-hunt workflow runs weekly with one Python 3.12/CUBRID 11.4 cell,
skipping unchanged weeks and retaining its mutation/performance/downstream checks and wide Hypothesis profile.
TLS, EUC-KR and official differential jobs are selected by related paths on PRs;
they remain available in main, changed-weekly and full release validation.

## Job timeouts

Every executing job sets an integer `timeout-minutes` (GitHub's default is 360
minutes), so a hung container, socket or install fails within its budget instead
of holding a runner for six hours (#758). Short jobs get roughly 3–5× their
observed maximum Actions duration with a floor: 5 minutes for gates and small jobs,
10–15 for lint/type/offline/tooling and 20–30 for live integration lanes. The two
long weekly jobs are explicit exceptions with smaller multipliers. Property/fault/soak
gets 120 (about 1.9× its observed 63; its soak step is separately capped at 90).
Mutation testing gets 300, about 2.5× its single successful 120-minute run; this is
the one documented exception to the 180-minute cap and stays below the 360 default.
Bounding the mutation scope (sharding) is tracked in #750.
Aggregate gates (`ci-gate`, `full-matrix-result`) run with `if: always()` and a
short timeout; a timed-out dependency reports a non-success result
(`cancelled`/`failure`), which the gate treats as a failure.

Jobs that call a reusable workflow cannot set `timeout-minutes`. Repo-local
callees (`publish-pypi.yml` → `integration-full.yml`) are covered through their
own jobs. The externally owned callees are an explicit allowlist in
`tests/test_workflow_timeouts.py`: the shared `doc-lint` and `codeql` workflows in
`cubrid-lab/.github`, and the cookbook smoke test. That test parses every
workflow and fails when an executing job lacks a bounded timeout, or when a new
external caller is not allowlisted.

## Dependency installation

`ci.yml` and `integration-full.yml` install dependencies with uv (#759): each
Python job runs `astral-sh/setup-uv` pinned to a commit SHA, with uv itself pinned
(`version: "0.12.17"`) and uv's cache keyed on `pyproject.toml`, then
`uv pip install --system` into the `actions/setup-python` interpreter, and logs the
result with `uv pip freeze --system`. Only the installer changes. Resolution
follows the same `pyproject.toml` constraints: before the switch, `.[dev]` on
Python 3.12 resolved to the same 63 packages and versions with pip and uv (after
PEP 503 name normalization). The packaging smoke test keeps plain `pip` in its
throwaway virtual environments, because it proves the built wheel and sdist install
with the tool end users run. `python-canary.yml` also stays on `pip` for preview
interpreters. `tests/test_workflow_installs.py` enforces the pinned uv setup, the
version log, and that no other `pip install` remains in these workflows.

## Python 3.15 preview preparation

`python-canary.yml` is manual-only: supply the full SHA and dispatch the branch
at that commit. One Ubuntu/standard-GIL lane selects Python 3.15 with prereleases
allowed, prints the actual interpreter/dependency versions, runs full offline
regressions and validates fresh wheel/sdist installs. Failed setup/install/tests
fail the run normally. It is separate from required PR checks and release gates;
there is no new schedule, PR matrix cell or CUBRID provisioning. This lane alone
does not establish official support, live database or free-threaded compatibility.
