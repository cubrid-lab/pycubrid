# Testing and CI guide

> Moved from the root [AGENTS.md](../../AGENTS.md) (#926). Owner: pycubrid maintainers.
> Every rule in the root file still applies. Scope: Workflows, CI lane summary and test layout. [docs/CI_POLICY.md](../CI_POLICY.md) owns the exact lanes and versions.

## CI Matrix

### Workflows

| File | Trigger | Purpose |
|---|---|---|
| `.github/workflows/ci.yml` | PR, main, weekly, manual | Minimum PR smoke and representative integration; see docs/CI_POLICY.md |
| `.github/workflows/integration-full.yml` | Manual dispatch, release workflow_call | Full supported compatibility matrix |
| `.github/workflows/release-please.yml` | Push main or manual dispatch | Prepare a PR-only release candidate (version + generated notes + canonical curated CHANGELOG) |
| `.github/workflows/publish-pypi.yml` | Push to main; recovery dispatch (`resume` / `verify-only` / `dry-run`) | Detect a merged release, then matrix, build, tag + GitHub Release + PyPI, cookbook verification, summary |
| `.github/workflows/docs-sync.yml` | PR | Requires matching docs or a `Docs: not needed -` reason (see Documentation definition of done) |
| `.github/workflows/pr-title.yml` | PR | Conventional PR title; rejects closing keywords for other repositories' issues |
| `.github/workflows/docs.yml` | Push to main, manual | Build and deploy the MkDocs site |
| `.github/workflows/python-canary.yml` | Manual dispatch | Advisory fresh-install check on the newest Python |

Other workflows (security, CodeQL, SBOM, maintenance, bug hunt, labels, issue triage,
stale, badge sync, Dependabot auto-merge) are scheduled or housekeeping jobs; read the
workflow file before relying on one.

### CI Matrix

- PR runtime smoke: a single Python cell, selected for code changes.
- High-risk PR offline and integration: full existing regressions on the oldest and
  newest supported Python, plus targeted live lanes.
- main, changed-weekly and dispatch: full offline suite with coverage on the oldest and
  newest supported Python, live Python 3.14/CUBRID 11.4 and 3.11/CUBRID 10.2.
- Full integration: manual and every release; no automatic nightly full matrix.
- Exact Python/CUBRID versions, change classification and gate requirements live in
  the [CI policy](../CI_POLICY.md) and the workflow files; do not copy them here.

## Test Structure

`tests/` is flat: one `test_*.py` module per behavior or contract, not one per source
module, so look for a test by the behavior it pins (`rg -l fetchmany tests/`). Markers are
declared in `pyproject.toml` `[tool.pytest.ini_options]`; the main ones are:

- `integration` — needs a live CUBRID server (`test_integration*.py` and others); skipped
  when no server is configured, excluded from `make test`.
- `repo_tooling` — repository policy and tooling checks (docs, release scripts, CI
  workflow contracts, PR title) with no driver behavior.
- `tls`, `version_matrix`, `official_differential`, `slow`, `benchmark` — dedicated
  CI lanes; see [docs/CI_POLICY.md](../CI_POLICY.md).

Shared fixtures are in `tests/conftest.py`, helpers in `tests/helpers/` and recorded data
(official API inventory, differential claims, golden bytes) in `tests/fixtures/`. Sync and
async parity tests usually parametrize over both cursor or connection classes. Test layout,
the backslash-escape-mode pin and the fast-versus-tooling split are described in
[docs/DEVELOPMENT.md](../DEVELOPMENT.md#running-tests).
