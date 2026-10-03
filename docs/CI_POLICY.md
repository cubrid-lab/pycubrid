# CI execution policy

Routine CI uses representative combinations instead of a Cartesian version/OS matrix.

| Trigger | Runtime validation |
| --- | --- |
| Documentation-only PR | Documentation and policy checks; no runtime suite or CUBRID provisioning |
| Ordinary code PR | One Ubuntu/Python 3.12 offline smoke lane; no full coverage claim |
| High-risk PR | One full offline regression lane without coverage plus Python 3.14/CUBRID 11.4; targeted additional lanes where relevant |
| Code push to main | One Ubuntu/Python 3.12 full offline suite with the existing 95% coverage floor; oldest/newest live endpoints |
| Monday 03:00 UTC | Same representative policy, comparing changes in the previous seven days; unchanged/docs-only history does not select runtime tests |
| Explicit full dispatch or release | Existing full Python 3.10–3.14 × CUBRID 10.2/11.0/11.2/11.4 integration workflow and mandatory release lanes |

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
