# CI execution policy

Routine CI uses representative combinations instead of a Cartesian version/OS matrix.

| Trigger | Runtime validation |
| --- | --- |
| Documentation-only PR | Documentation and policy checks; no runtime suite or CUBRID provisioning |
| Ordinary code PR | One Ubuntu/Python 3.12 offline smoke lane; no full coverage claim |
| High-risk PR | Full offline regressions without coverage on Python 3.11 and 3.14, plus Python 3.14/CUBRID 11.4; targeted additional lanes where relevant |
| Code push to main | Full offline suite on Ubuntu/Python 3.11 and 3.14, each with the existing 95% coverage floor; oldest/newest live endpoints |
| Monday 03:00 UTC | Same policy as a main push (including the Python 3.11/3.14 offline suite), comparing changes in the previous seven days; unchanged/docs-only history does not select runtime tests, and lanes a successful push run of the same SHA already ran are not repeated (see [Event tiers and cost evidence](#event-tiers-and-cost-evidence)) |
| Explicit full dispatch or release | Existing full Python 3.11–3.14 × CUBRID 10.2/11.0/11.2/11.4 integration workflow and mandatory release lanes, plus the Python 3.11/3.14 offline suite with the 95% floor |

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
The weekly run then drops a lane family only when a successful push run of the
exact same SHA already ran it (#750).

GitHub charges have not been attributed to a workflow or runner SKU. Reduced job
counts establish less repeated work; they are not a measured monetary saving.
Compare subsequent Actions jobs/runner minutes and the actual billing category
before making a cost claim.

The deep bug-hunt workflow runs weekly (Thursday 04:00 UTC) with one Python 3.12/CUBRID 11.4 cell,
skipping unchanged weeks and retaining its mutation/performance/downstream checks and wide Hypothesis profile.
TLS, EUC-KR and official differential jobs are selected by related paths on PRs;
they remain available in main, changed-weekly and full release validation.

## Event tiers and cost evidence

#750 gives each expensive event one purpose. Pull requests stay fast and
representative. `main` pushes carry the endpoint evidence. The weekly schedules
cover what a push run did not already prove at the same SHA, plus the deep bug
hunt. The release runs the full matrix and fails closed. Changes were made only
where another lane makes the same assertions on the same SHA.

### Measurements

Measured with the GitHub REST API (`actions/workflows/<file>/runs` and
`actions/runs/<id>/jobs?filter=latest`) on 2026-10-09, before this change.

- **Runner minutes**: the sum of `completed_at - started_at` over each executed
  job (latest attempt).
- **Rounded minutes**: each job rounded up to a whole minute, which is how
  GitHub meters. The repository is public on standard runners, so neither is a
  billed charge.
- **Wall minutes**: the first job start to the last job end.
- **Executed job**: conclusion neither `skipped` nor empty. Runs where no job
  executed are left out. These are 120 pull-request runs waiting for approval
  (`action_required`) and 4 that failed at startup.
- **Failed / cancelled**: run conclusions. Cancelled runs are listed apart:
  every one in the sample was superseded by a newer run in the same concurrency
  group.

The window starts at 2026-10-03 06:04 UTC, after `a65360a` introduced the
current representative tiers. The weekly and push samples predate #745's 3.11 and 3.14 offline
cells (landed 2026-10-09 02:06 UTC): the 2026-10-05 weekly run had a single
`offline-tests (3.12)` cell, and only 2 of the 51 code push runs are post-#745.
The release sample uses its current workflow shape. The `ci.yml` runs are classified as follows:

- **Pull requests**: a risk PR executed any of `integration-tests`,
  `integration-tls`, `integration-charset` or `official-differential`, or an
  `offline-tests` cell on Python 3.11. Otherwise an ordinary code PR executed
  `offline-tests`. Any other PR is docs-only.
- **Pushes**: a code push executed `offline-tests`.
- **Releases**: `publish-pypi.yml` push runs in which the `Full compatibility
  matrix` jobs ran.

| Category | Window (runs) | Median jobs | Median runner min | Median rounded min | Median wall min | Failed / cancelled |
| --- | --- | --- | --- | --- | --- | --- |
| Docs-only PR | 2026-10-03..09 (24) | 8 | 1.0 | 8 | 0.7 | 0 / 0 |
| Ordinary code PR | 2026-10-03..08 (10) | 11.5 | 2.3 | 11.5 | 1.0 | 3 (lint) / 1 |
| Risk PR | 2026-10-03..09 (91) | 15 | 7.5 | 18 | 3.6 | 4 (lint, one also typecheck) / 8 |
| Main push, code | 2026-10-03..09 (50) | 17 | 10.7 | 23 | 4.0 | 0 / 5 |
| Main push, no code | 2026-10-03..09 (17) | 8 | 1.0 | 8 | 0.7 | 0 / 0 |
| Weekly `ci.yml` (Monday 03:00) | 2026-10-05 (1) | 18 | 11.4 | 24 | 4.2 | 0 / 0 |
| Weekly `bug-hunt.yml` (Monday 04:00) | 2026-10-05 (1) | 7 | 149.4 | 152 | 83.0 | 1 (mutation: runner shutdown after 82.8 min) / 0 |
| `bug-hunt.yml` dispatch | 2026-10-04 (2) | 7 | 110.1 | 112.5 | 74.6 | 0 / 1 |
| `integration-full.yml` dispatch | 2026-10-04 (3) | 27 | 40.6 | 53 | 8.4 | 0 / 0 |
| Release (`publish-pypi.yml` with `integration-full.yml`) | 2026-10-04..09 (2) | 34 | 41.8 | 60.5 | 10.2 | 1 (cookbook verification after publish) / 0 |

- **No defect findings in the scheduled lanes.** No live lane, official
  differential, TLS or charset job failed on any push, weekly, dispatch or
  release run in the window. Every PR failure was lint, plus one typecheck
  failure.
- **The weekly `bug-hunt.yml` failure was infrastructure.** Its only failure
  was a runner shutdown during mutation testing.
- **Earlier bug-hunt shape.** The nightly 15-cell bug-hunt shape (2026-09-18 to
  10-02, 15 runs) had a median of 739 runner minutes. Every run failed, in
  downstream dogfooding and the mutmut 2.x configuration, both since fixed.
- **Samples are small.** The weekly samples are one run each, because both
  workflows moved to the weekly shape in early October. The release sample has
  two runs.

Median per-job runner minutes on successful code pushes:

- `integration-tests`: 1.9 (3.14/11.4) and 1.5 (3.11/10.2)
- `offline-tests`: 1.7 (3.12, before #745), 1.5 (3.11), 1.4 (3.14)
- `integration-tls`: 1.4
- official differential: 1.2
- `integration-charset`: 0.9
- `repo-tooling-tests`: 0.7
- typecheck, packaging and lint: 0.3 each
- public API check: 0.2

### Coverage ownership

| Coverage | Owner (workflow / job) | Event tier |
| --- | --- | --- |
| Offline suite, 95% floor | `ci.yml` `offline-tests`; `integration-full.yml` `offline-endpoints` | Ordinary PR: 3.12 smoke. Risk PR: 3.11 + 3.14 without coverage. Main push, weekly, dispatch: 3.11 + 3.14. Release and full dispatch: 3.11 + 3.14 |
| Live endpoints | `ci.yml` `integration-tests`; `integration-full.yml` `integration-full` (4 × 4) | Risk PR: 3.14/11.4. Main push, weekly, dispatch: plus 3.11/10.2. Release and full dispatch: all 16 cells |
| TLS | `ci.yml` `integration-tls` (3.14/11.4); `integration-full.yml` `integration-tls` (3.11 + 3.14) | TLS-path PR, non-PR code; release and full dispatch |
| EUC-KR charset | `ci.yml` and `integration-full.yml` `integration-charset` | Charset-path PR, non-PR code; release and full dispatch |
| Official CUBRIDdb differential | `ci.yml` and `integration-full.yml` `official-differential` | Official-path PR, non-PR code; release and full dispatch |
| CUBRID version differential | `integration-full.yml` `version-differential` | Release and full dispatch |
| Property, protocol, state-machine, fault, chaos, soak (wide profile) | `bug-hunt.yml` `property-and-fault` | Thursday 04:00 UTC with changes in the previous 7 days, and dispatch |
| Mutation testing | `bug-hunt.yml` `mutation` | Same as bug hunt |
| Benchmark trend | `bug-hunt.yml` `perf-trend` | Same as bug hunt |
| Downstream corpus (advisory) | `bug-hunt.yml` `downstream-corpus` | Same as bug hunt |
| Type checking | `ci.yml` `typecheck` | Every code event (not repeated by the release) |
| Public API baseline | `ci.yml` `compat-check` | Every code event (not repeated by the release) |
| Packaging | `ci.yml` `packaging-smoke-test`; `publish-pypi.yml` `consistency` and `build` | Risk PR, non-PR code; release |
| Repository tooling | `ci.yml` `repo-tooling-tests` | Tooling-path events |
| Python 3.15 preview (advisory) | `python-canary.yml` | Manual only |

### Consolidation

- **The weekly `ci.yml` run reuses same-SHA push evidence.** The one weekly
  run in the window (2026-10-05, `c2a4f1db`) re-ran, at the same SHA, the 17
  jobs that the push run had passed five hours earlier, plus
  `repo-tooling-tests`.
  - **Why the runs match.** Both are non-PR events, so they select the same
    matrix cells and run the same commands from the same `ci.yml`.
  - **What the weekly run now does.** Its `detect-changes` job (with read-only
    `actions: read`) looks up push runs of the exact SHA. It drops the code
    lanes when one successful push run executed `offline-tests`, and the
    repository-tooling lane when one executed `repo-tooling-tests`, with no
    failed or skipped cell.
  - **When it still runs everything.** A docs-only head commit, a head push
    run that failed or was cancelled, or a failed lookup selects the lanes
    exactly as before.
  - **Why the backstop stays.** Since 2026-09-01, 3 of 16 cancelled code
    push runs were superseded by a run that selected no code lanes, so the
    7-day diff stays the backstop for a cancelled push.
  - **Drift is not this lane's job.** The weekly run never owned dependency or
    runner-image drift, because quiet weeks already select nothing.
- **`bug-hunt.yml` moves from Monday 04:00 UTC to Thursday 04:00 UTC.** It no
  longer stacks on the Monday `ci.yml`, CodeQL, SBOM, security and maintenance
  schedules. A later cron start does not mean that the earlier run finished.
  - **Activity guard.** It is unchanged: a schedule runs only with changes in
    the previous 7 days.
  - **No duplicate to remove.** Its `integration and not slow and not tls`
    session uses Python 3.12 and the wide Hypothesis profile. That makes it a
    different cell and profile from `ci.yml`'s 3.11/3.14 cells, so it stays.
- **The release-merge overlap is kept.**
  - **The overlap.** On a release commit, the `ci.yml` push run's live lanes
    (`integration-tests` 3.14/11.4 and 3.11/10.2, TLS 3.14/11.4, charset,
    official differential) are a subset of `integration-full.yml` on the same
    SHA, with identical commands. That is about 6.9 runner minutes per release
    (two releases in the window).
  - **Why it stays.** Skipping them would make the push run's evidence depend
    on a release detection that can stop before the matrix (`consistency`
    failure). A code commit would then have no live evidence anywhere.
- **Pull requests and main pushes are unchanged.**

Expected effect, computed from the measured runs above:

| Category | Jobs before → after | Runner min before → after | Rounded min before → after |
| --- | --- | --- | --- |
| Ordinary code PR, risk PR, main push | unchanged | unchanged | unchanged |
| Weekly `ci.yml`, head push run green with code lanes | 19 → 9 | about 13 → about 1.8 | about 26 → 9 |
| Weekly `ci.yml`, head push run not green or docs-only | 19 → 19 | about 13 → about 13 | about 26 → about 26 |
| Weekly `bug-hunt.yml` | 7 → 7 (Thursday) | 149.4 → 149.4 | 152 → 152 |
| Release | 34 → 36 | 41.8 → about 44.7 | 60.5 → about 64.5 |

These are expectations. Compare them with the next weeks of Actions data before
claiming a saving.

### Release evidence

`publish-pypi.yml` calls `integration-full.yml` at the release SHA through
`workflow_call`. `build` and `publish` need that `matrix` job, so a failed,
cancelled or skipped matrix stops publication. `full-matrix-result` fails unless
every dependency reports `success`.

The release does not read any `ci.yml` run. Before #750, the Python 3.11/3.14
offline suite with the 95% floor (#745) at the release commit came only from
the `ci.yml` push run.

- **Nothing required that run.** No step in the release path checked it.
- **A later merge could cancel it.** The `ci-push-refs/heads/main` concurrency
  group cancels an older push run when a newer push arrives. That happened to
  16 push runs since 2026-09-01.

`integration-full.yml` now runs `offline-endpoints`. It uses Python 3.11 and
3.14 with the same install and pytest command as `ci.yml` `offline-tests`, at
the exact SHA. `full-matrix-result` requires it. This adds about 2.9 runner
minutes per release.

Type checking, the public API baseline, lint and repository tooling are still
not repeated by the release; the release path relies on the release PR and the
`main` push run for them.

## Offline endpoint versions

The `offline-tests` job picks its Python matrix from the event (#745), so the
full offline suite runs on the oldest (3.11) and newest (3.14) supported Python
wherever it already ran in full, while ordinary PRs stay on one cheap cell.

| Event | `offline-tests` cells | Suite |
| --- | --- | --- |
| Ordinary code PR (`risk` not selected) | Python 3.12 | Representative smoke tests |
| Risk-selected PR | Python 3.11 and 3.14 | Full offline suite, no coverage |
| Push to main, Monday schedule, manual dispatch | Python 3.11 and 3.14 | Full offline suite with the 95% coverage floor |
| Release and full dispatch (`integration-full.yml` `offline-endpoints`) | Python 3.11 and 3.14 | Same install and command, required by `full-matrix-result` |

Every cell keeps the `not integration and not repo_tooling` selection, the
15-minute timeout and the immutable `inputs.sha || github.sha` checkout. In
`ci.yml`, coverage reports are written per Python version
(`coverage-py<version>.xml`), uploaded as the `offline-coverage-py<version>`
artifact and sent to Codecov with the `offline-py<version>` flag, so the two
cells never overwrite each other. The `integration-full.yml` `offline-endpoints`
cells upload nothing.

The weekly schedule is owned by `ci.yml` itself: `integration-full.yml` has no
schedule (it repeats the two cells only for a release or a full dispatch), and
`bug-hunt.yml` runs targeted property and fault suites, so no other weekly lane
duplicates this evidence (#750). Python 3.11
here is the latest 3.11 patch release from `actions/setup-python`; it does not
exercise 3.11.0–3.11.2, which keep their targeted regression (#744).

`ci-gate` sees one aggregate `offline-tests` result. A failed or cancelled cell
makes that result non-success, and a skipped job is accepted only when no code
changed, or, on the weekly schedule, when a successful push run of the same SHA
already ran it, so a missing or failed endpoint cell cannot turn the gate green.

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

## Pinned documentation tools and scan concurrency

`docs.yml` installs the site tools only from `docs/requirements.txt`, which pins
`mkdocs`, `mkdocs-material` and `pymdown-extensions` to exact versions (#782), so an
upstream release cannot break the `mkdocs build --strict` gate without a repository
change. Dependabot (pip, `/docs`) proposes updates, and `mkdocs.yml` excludes the file
from the published site.

`ci.yml` also builds the site on pull requests (#786), so a bump or a docs edit that
breaks `mkdocs build --strict` fails before merge instead of later on `main`. The
`docs-build` job is selected by the `site` path filter: `docs/**` (content and
`docs/requirements.txt`), `mkdocs.yml`, `scripts/generate_llms_full.py`,
`.github/workflows/docs.yml` and `.github/workflows/ci.yml`. Root `*.md` files are not
site inputs (the changelog is linked, not staged), so they do not select it, and
`workflow_dispatch` forces it like the other lanes. The job runs the `docs.yml` build
steps (install from `docs/requirements.txt`, `scripts/generate_llms_full.py`,
`mkdocs build --strict`) with read-only permissions, pinned actions, a 10-minute timeout
and `persist-credentials: false`; it never uploads a Pages artifact or deploys. The
`CI Gate` expects `docs-build` to succeed exactly when `site` is selected and accepts a
skip otherwise, so the required `CI Gate` check blocks a broken docs build.
`tests/test_ci_policy.py` and `tests/test_workflow_path_impact.py` enforce this.

`codeql.yml` and `security.yml` declare caller-level `concurrency` with group
`${{ github.workflow }}-${{ github.event_name == 'pull_request' && github.ref || github.run_id }}` and
`cancel-in-progress: ${{ github.event_name == 'pull_request' }}` (#783): a new push to a
pull request cancels the superseded scan, while every other event gets a unique per-run
group, because within one group GitHub replaces a pending run even when
`cancel-in-progress` is false, so main pushes and scheduled runs are never cancelled or dropped. `security.yml` installs `bandit[toml]==1.9.4`, the version pinned in
`pyproject.toml`. `tests/test_workflow_pins.py` enforces all three.

## Python 3.15 preview preparation

`python-canary.yml` is manual-only: supply the full SHA and dispatch the branch
at that commit. One Ubuntu/standard-GIL lane selects Python 3.15 with prereleases
allowed, prints the actual interpreter/dependency versions, runs full offline
regressions and validates fresh wheel/sdist installs. Failed setup/install/tests
fail the run normally. It is separate from required PR checks and release gates;
there is no new schedule, PR matrix cell or CUBRID provisioning. This lane alone
does not establish official support, live database or free-threaded compatibility.
