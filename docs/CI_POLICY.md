# CI execution policy

Routine CI uses representative combinations instead of a Cartesian version/OS matrix.

| Trigger | Runtime validation |
| --- | --- |
| Documentation-only PR | Documentation and policy checks; no runtime suite or CUBRID provisioning |
| Ordinary code PR | One Ubuntu/Python 3.12 offline smoke lane; no full coverage claim |
| High-risk PR | Full offline regressions without coverage on Python 3.11 and 3.14, plus Python 3.14/CUBRID 11.4; targeted additional lanes where relevant |
| Code push to main | Full offline suite on Ubuntu/Python 3.11 and 3.14, each with the existing 95% coverage floor; oldest/newest live endpoints |
| Monday 03:00 UTC | Same policy as a main push (including the Python 3.11/3.14 offline suite), comparing changes in the previous seven days; unchanged/docs-only history does not select runtime tests |
| Explicit full dispatch or release | Existing full Python 3.11–3.14 × CUBRID 10.2/11.0/11.2/11.4 integration workflow and mandatory release lanes |

The PR smoke suite is deliberately bounded. Contributors must run the regression
checks relevant to their change locally and record commands/results in the PR.
Passing smoke is not evidence that the whole offline suite or coverage floor ran.
`make test` and `make integration` remain available without changing their scope.

Change selection is in `ci.yml`'s `detect-changes` job. Non-documentation paths
are code by default, so new source/configuration files do not silently become docs.
All driver and test paths, including new modules, plus dependency, build and script
changes and `ci.yml` itself conservatively select representative pre-merge
integration and the existing offline regression suite on the oldest and newest
supported Python (see [Offline endpoint versions](#offline-endpoint-versions)).
Other workflow changes select the repository-tooling lane instead (see
[Workflow change impact](#workflow-change-impact)). Other code
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

## Offline endpoint versions

The `offline-tests` job picks its Python matrix from the event (#745), so the
full offline suite runs on the oldest (3.11) and newest (3.14) supported Python
wherever it already ran in full, while ordinary PRs stay on one cheap cell.

| Event | `offline-tests` cells | Suite |
| --- | --- | --- |
| Ordinary code PR (`risk` not selected) | Python 3.12 | Representative smoke tests |
| Risk-selected PR | Python 3.11 and 3.14 | Full offline suite, no coverage |
| Push to main, Monday schedule, manual dispatch | Python 3.11 and 3.14 | Full offline suite with the 95% coverage floor |

Every cell keeps the `not integration and not repo_tooling` selection, the
15-minute timeout and the immutable `inputs.sha || github.sha` checkout. Coverage
reports are written per Python version (`coverage-py<version>.xml`), uploaded as
the `offline-coverage-py<version>` artifact and sent to Codecov with the
`offline-py<version>` flag, so the two cells never overwrite each other.

The weekly schedule is owned by `ci.yml` itself: `integration-full.yml` has no
schedule and runs only live suites, and `bug-hunt.yml` runs targeted property and
fault suites, so no other weekly lane duplicates this evidence (#750). Python 3.11
here is the latest 3.11 patch release from `actions/setup-python`; it does not
exercise 3.11.0–3.11.2, which keep their targeted regression (#744).

`ci-gate` sees one aggregate `offline-tests` result. A failed or cancelled cell
makes that result non-success, and a skipped job is accepted only when no code
changed, so a missing or failed endpoint cell cannot turn the gate green.

## Workflow change impact

Changed-path selection follows a per-workflow impact table (#761). Only `ci.yml`
selects the live PR lanes (`risk`, `tls`, `charset`, `official`), because it defines
and runs them. Every other workflow under `.github/` selects the repository-tooling
lane, whose policy and workflow tests parse and check it; the live lanes of
`ci.yml` do not execute another workflow's jobs, so running them adds no detection.

| Changed workflow | PR validation |
| --- | --- |
| `ci.yml` | All lanes it defines, plus tooling |
| `integration-full.yml` | Tooling, plus a manual `workflow_dispatch` of `integration-full.yml` on the PR head, linked in the PR |
| `publish-pypi.yml`, `release-please.yml` | Tooling (release workflow tests) |
| `bug-hunt.yml`, `python-canary.yml` | Tooling; dispatch when the run itself changes |
| Other workflows | Tooling; `pr-title.yml` and `docs-sync.yml` also run themselves on the PR |

`tests/test_workflow_path_impact.py` evaluates the filters against every workflow
file and against representative source, test and documentation paths.

## Parallel live lanes

`integration-tests`, `integration-charset`, `integration-tls` and
`official-differential` depend only on `validate-target` and `detect-changes`
(#760), so they start alongside lint, type checking and offline tests instead of
after them. `validate-target` stays a dependency because it verifies the manual
dispatch SHA against the PR head. `ci-gate` still needs every job, so a lint, type or
offline failure keeps the gate red even when the live lanes pass. The trade-off is
that a PR that fails lint can still spend live-lane runner time.

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
(`version: "0.12.17"`), then `uv pip install --system` into the
`actions/setup-python` interpreter, and logs the result with
`uv pip freeze --system`. setup-uv receives the same `python-version` spec as
setup-python (for example `"3.12"` or the matrix value), so its cache key carries the
job's Python minor version rather than the patch release `uv python find` would
report, which differs between runner images and would make the key miss. With the
`pyproject.toml` hash and a per-job `cache-suffix`, each job and Python version keeps
its own cache. The same input exports `UV_PYTHON`, and `uv pip install --system`
picks the first matching interpreter on `PATH`, which is setup-python's. Routine CI always caches
(`enable-cache: true`); `integration-full.yml` uses `auto`, which disables caching only
for tag pushes, `release`, `pull_request_target` and `workflow_run` events. The release
path (`publish-pypi.yml` on a `main` push, or its recovery dispatch) therefore still
restores caches. That is safe because a uv cache holds only downloaded and built
wheels: every run re-resolves from the same constraints, so a cache can speed an
install but cannot change the resolved versions. Only the installer changes. Resolution
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
