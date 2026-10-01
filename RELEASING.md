# Releasing pycubrid

Merging a reviewed release PR is the only normal way to release `pycubrid`.
Nobody pushes tags or runs a publish workflow by hand, and ordinary PR merges
never deploy. This procedure is kept identical (except for package names and
version files) with the sibling cubrid-lab repositories.

Key invariants:

- The version is single-sourced from `pycubrid/__init__.py` (`__version__`).
- `CHANGELOG.md` is hand-curated and the only source of release notes,
  including the Upgrade notes and the [`RELEASE_POLICY.md`](RELEASE_POLICY.md)
  classification. It is not generated.
- A release is decided from git facts on `main`, never from a PR title.
- The workflows never delete PyPI files, never move a tag and never create a
  version that was not merged through a release PR.

## Normal flow

```text
prepare-release.yml  ->  release PR (review, edit notes)  ->  squash-merge  ->  release.yml
```

### 1. Prepare the release PR

```bash
gh workflow run prepare-release.yml -f version=X.Y.Z
```

The workflow (dispatch it from `main`) validates `X.Y.Z` (greater than the
current `__version__`, no existing tag or `release/vX.Y.Z` branch), then
`scripts/prepare_release.py`:

- moves everything under `## [Unreleased]` into `## [X.Y.Z] - <UTC date>` and
  leaves an empty `## [Unreleased]` above it;
- sets `__version__ = "X.Y.Z"` in `pycubrid/__init__.py`.

It runs `make release-check VERSION=X.Y.Z` on the result and only then pushes
`release/vX.Y.Z` and opens the PR **`chore: release vX.Y.Z`**.

### 2. Review the release PR

- Edit the CHANGELOG section as needed (Upgrade notes, wording, the
  RELEASE_POLICY classification) by pushing to `release/vX.Y.Z`. You can also
  correct the date there; the release reads whatever dated section is merged.
- **Start CI.** The PR is created with `GITHUB_TOKEN`, and GitHub does not start
  workflows for events caused by `GITHUB_TOKEN`, so CI does not run on it by
  itself. Close and reopen the PR, or push any commit (including your edits, or
  `git commit --allow-empty -m "ci: run checks"`) to the branch. No extra
  secret is needed; a maintainer PAT is not required.
- Local re-check if you edit by hand: `make release-check VERSION=X.Y.Z`.

### 3. Squash-merge

Keep the title `chore: release vX.Y.Z`. The merge commit starts `release.yml`.

### 4. Automatic release (`release.yml`)

Every push to `main` runs the cheap **detect** job
(`scripts/release_detect.py`). It is a release only when all of these hold at
the pushed commit:

1. `__version__` differs from the first parent,
2. the version is `MAJOR.MINOR.PATCH` and `CHANGELOG.md` has a dated
   `## [X.Y.Z] - YYYY-MM-DD` section,
3. tag `vX.Y.Z` does not exist, or already points at this commit (resume).

Otherwise the run ends with **"no release"** (an ordinary merge shows
`__version__ unchanged (…) compared with the first parent`). A version change
without a dated section, or with a tag at another commit, also ends as "no
release" and adds a warning annotation.

For a release, the jobs run in one workflow run, pinned to the merge commit SHA
and chained with explicit `needs:` (tags and Releases created with
`GITHUB_TOKEN` start no other workflow):

| Job | What it does |
| --- | --- |
| `consistency` | `make release-check VERSION=X.Y.Z` at the SHA: `__version__`, CHANGELOG lint and dated section, `build` + `twine check`. |
| `matrix` | The full Python × CUBRID matrix, TLS, EUC-KR and version-differential lanes: `integration-full.yml` called through `workflow_call` at the SHA. |
| `build` | Builds the wheel and sdist **once**, `twine check`, wheel/sdist install smoke tests, extracts the release notes, generates the SPDX SBOM and records SHA-256 hashes. Artifacts `release-dist` and `release-meta` are kept for 14 days. |
| `publish` | In the `pypi` environment: re-checks the hashes, creates the annotated tag `vX.Y.Z` at the SHA (or accepts one already there), creates a **draft** GitHub Release with the notes and `sbom.spdx.json`, uploads the same artifact to PyPI through `scripts/pypi_duplicate_guard.py` and Trusted Publishing (OIDC), then publishes the Release. |
| `verify-cookbook` | Calls the cookbook smoke test (`smoke-test.yml` of cubrid-cookbook-python) as a **reusable workflow** with `package=pycubrid`, `version=X.Y.Z` and a `request_id`; its jobs run inside this release run. |
| `require-cookbook` | Fails unless the called workflow succeeded and its outputs report `status == success` with `installed_version == requested_version == X.Y.Z`. |
| `summary` | Always runs; one table with SHA, tag, version, artifact hashes, matrix result, PyPI and Release URLs, cookbook run and the final state. |

Only `publish` has write access (`contents: write` for the tag and Release,
`id-token: write` for PyPI); every other job reads.

#### Cookbook verification

`verify-cookbook` uses the release verification contract of
[cubrid-cookbook-python](https://github.com/cubrid-lab/cubrid-cookbook-python/blob/main/CONTRIBUTING.md)
("Calling the smoke test from a release workflow"):

```yaml
uses: cubrid-lab/cubrid-cookbook-python/.github/workflows/smoke-test.yml@<40-hex cookbook main commit> # main
with:
  package: pycubrid
  version: X.Y.Z
  request_id: pycubrid-vX.Y.Z-<run id>-<run attempt>
```

The cookbook jobs (`Cookbook release verification / Smoke Tests (CUBRID 11.2)`,
`… (CUBRID 11.4)` and `… / Release verification report`) run as jobs of the
release run with its own `GITHUB_TOKEN`: the calling job grants only
`contents: read`, and **no token, secret or polling** is involved. They install
exactly `pycubrid==X.Y.Z` from PyPI (with a bounded retry for publication
delay, never a fallback to the latest release) and upload the report artifact
`release-verification-<request_id>` to the release run. The called workflow's
outputs `status`, `requested_version`, `installed_version` and `artifact` are
checked by `require-cookbook` and shown in the summary. A failed or cancelled
called workflow is a failed verification.

The pin is a full commit SHA of the cookbook's `main` branch. Dependabot
(`github-actions` ecosystem) updates it: the cookbook's version tags do not
contain the pinned commit, so Dependabot proposes the newest `main` commit.
To bump it by hand, replace the SHA with the current
`gh api repos/cubrid-lab/cubrid-cookbook-python/commits/main --jq .sha` and keep
the `# main` comment.

#### Final states in the summary

| Final state | Meaning |
| --- | --- |
| `no release: …` | Ordinary push; nothing ran after `detect`. |
| `failed before publish in <job> …` | `consistency`, `matrix` or `build` failed. No tag, no Release, no upload. |
| `publish failure; … may be partial` | Failed inside `publish`; see recovery below. |
| `published and verified` | Done. |
| `published; post-release verification failed` | On PyPI, but the called cookbook workflow failed or was cancelled, or its outputs do not report `status == success` with `installed_version == X.Y.Z`. |
| `dry run …` / `verification only …` | Recovery dispatch results (below). |

## Failure and recovery

| Situation | What happened | What to do |
| --- | --- | --- |
| `detect` says "no release" on a release merge | Version unchanged, CHANGELOG section not dated, or the tag exists at another commit (see the warning). | Fix through a new PR. If the tag is at another commit, that version is taken: prepare `X.Y.(Z+1)`. |
| `consistency`, `matrix` or `build` failed | Nothing published; no tag, no Release. | Transient (flaky lane, runner error): `gh run rerun <run-id> --failed`. Real defect at that commit: `X.Y.Z` stays unpublished. Fix it in a normal PR (no version change, so no release), then prepare `X.Y.(Z+1)`; in that release PR fold the unpublished `## [X.Y.Z]` entries into the new section. A skipped version number on PyPI is harmless. |
| `publish` failed (tag/Release/PyPI error, partial upload) | The tag and a draft Release may exist; PyPI may hold some files. | `gh run rerun <run-id> --failed` of the **same** run. It reuses the verified artifact, accepts the tag at the same SHA, reuses the draft Release, and the duplicate guard drops files PyPI already serves byte for byte. |
| Same version rebuilt (new run instead of rerun) | The rebuild's bytes differ from files already on PyPI. | The guard fails on the hash mismatch, by design. Use `rerun --failed` within the 14-day artifact retention; otherwise treat it as a broken release. |
| `verify-cookbook` or `require-cookbook` failed | **Published**; the cookbook verification failed, was cancelled or did not start. | Fix the cause. If the cookbook call itself (`verify-cookbook`) failed or was cancelled, `gh run rerun <run-id> --failed` re-runs it with a new `request_id` (a new run attempt). If the call succeeded but `require-cookbook` rejected its outputs, `--failed` only re-runs that gate against the same outputs; in that case, or if the call never started, use the `verify-only` dispatch below, which requests a new cookbook run. Never republish. |
| Broken release on PyPI | Versions are immutable. | Yank it on PyPI (project settings → Releases → Yank) and release `X.Y.(Z+1)` through a new release PR. Never delete a version or move a tag. |

The duplicate guard (`scripts/pypi_duplicate_guard.py`) reads
`https://pypi.org/pypi/pycubrid/X.Y.Z/json` and compares each file in the
verified `dist/` with the file PyPI serves under the same name: not on PyPI
(HTTP 404) → uploaded; same SHA-256 → skipped; different SHA-256, a PyPI file
the build did not produce, or PyPI unreachable → the job fails and nothing is
uploaded. Only an explicit "every file already on PyPI" skips the upload step.
If PyPI's JSON API lags right after an upload and does not list a file yet,
that file goes to the upload step, which is still safe: PyPI answers a
byte-identical re-upload of an existing filename with success and rejects
different bytes with `400 File already exists`. Before treating that rejection
as a broken release, re-query `https://pypi.org/pypi/pycubrid/X.Y.Z/json` and
compare the published SHA-256 with the run's `SHA256SUMS` (artifact
`release-meta`): a match means the file is fine and `gh run rerun --failed`
completes the release; a mismatch is a broken release.

### Recovery dispatch (the only manual entry point)

`release.yml` has one `workflow_dispatch` with an `action` input. It never
creates a new version, never moves a tag and never deletes anything.

| `action` | Allowed when | Runs |
| --- | --- | --- |
| `resume` | Dispatched from `main`; tag `vX.Y.Z` exists; its commit is on `main`; `__version__` and a dated CHANGELOG section at that commit equal `X.Y.Z`. | consistency → matrix → build → publish → verify at the tag commit. For an interrupted release whose run can no longer be rerun (for example the artifacts expired before anything reached PyPI). A rebuilt file that differs from one already on PyPI fails the guard. |
| `verify-only` | Same conditions as `resume`. | Only `verify-cookbook`, `require-cookbook` and `summary` for the already-published version, through the same reusable cookbook workflow. |
| `dry-run` | Any branch; `X.Y.Z` must equal `__version__` at the dispatched commit and have a dated CHANGELOG section. | consistency → matrix → build → verify-cookbook → require-cookbook, **no** tag, Release or upload. The cookbook jobs verify the already-published `X.Y.Z`. |

```bash
gh workflow run release.yml -f action=resume -f version=X.Y.Z
gh workflow run release.yml -f action=verify-only -f version=X.Y.Z
gh workflow run release.yml --ref <branch> -f action=dry-run -f version=X.Y.Z
```

### Dry-run evidence

Before dispatching, audit `gh run list -R cubrid-lab/pycubrid --workflow=release.yml`
for an existing `dry-run`/`resume`/`verify-only` run at the current `release.yml`
revision (including `scripts/release_detect.py`, `scripts/release_summary.py`
and the other scripts the jobs check out from the workflow's own commit): a
run against an older revision of those scripts does not cover code paths
changed since. An ordinary push whose `detect` finds "no release" (`build`,
`matrix`, `publish` and `verify-cookbook` all `skipped`) proves detection
only, not the full dry-run path below.

Last full dry run: [run 36862670587](https://github.com/cubrid-lab/pycubrid/actions/runs/36862670587),
dispatched `-f action=dry-run -f version=1.8.0` from `main` at commit
[`bf53d2c`](https://github.com/cubrid-lab/pycubrid/commit/bf53d2ca45290533ad8d6b47cbb54c4a9ba533c5)
(workflow file and scripts unchanged since
[`45f3374`](https://github.com/cubrid-lab/pycubrid/commit/45f3374b4cfa9d191316c935b3801e827aa94b1b),
#548, which updated `scripts/release_summary.py` after
[`834ae96`](https://github.com/cubrid-lab/pycubrid/commit/834ae96e8612dcaf2b3dd4dde08db4eac1fc440b)/#546
first wired the cookbook call into `release.yml`). No earlier recorded run
had exercised the full path: the only prior
run, [36778024615](https://github.com/cubrid-lab/pycubrid/actions/runs/36778024615),
was an ordinary push on which `detect` found "no release", so `consistency`,
`matrix`, `build`, `publish` and `verify-cookbook` were all skipped.

Job conclusions for 36862670587: `detect` success (mode `dry-run`, tag
`v1.8.0` reported `different` — it already exists at the published commit,
untouched by this dry run), `consistency` success, `matrix` (full
compatibility matrix) success, `build` success, `publish` **skipped** (no
tag, Release or PyPI upload — verified: nothing changed on PyPI or in this
repository's tags/Releases), `verify-cookbook` success, `require-cookbook`
success (installed `1.8.0` == requested `1.8.0`), `summary` success with
final state `dry run passed; nothing published; cookbook verification
success`.

Build artifact SHA-256 (built fresh by this run and uploaded only as the
14-day `release-dist` run artifact, never to PyPI; recorded here as
dry-run evidence):

```
d1c32a3ea22d0be2e52345976c0d30e280347260305d16b726dc342b9cc01ca6  pycubrid-1.8.0-py3-none-any.whl
c13207dae94aaf239bc84325ae0bfe986c5123b3dc00b841e39a630bedcbd04e  pycubrid-1.8.0.tar.gz
```

**Limit:** the `build` job's own smoke tests do install the freshly built
wheel and sdist into clean virtual environments and check their imports and
metadata, so a passing dry run proves the fresh artifacts install cleanly.
What it does not prove is that those fresh artifacts pass the cookbook
suite: `verify-cookbook` on a `dry-run` installs the already-published
`pycubrid==1.8.0` from PyPI (the cookbook's release verification contract
only ever installs from PyPI), never the wheel this dry run just built.
Only `publish` followed by its own `verify-cookbook` verifies the newly
built artifact through the cookbook.

## Repository settings this relies on

- Squash merge only; the PR title becomes the commit title.
- Settings → Actions → General: "Allow GitHub Actions to create and approve
  pull requests" (for `prepare-release.yml`).
- Environment `pypi`: deployment branches limited to `main`; PyPI Trusted
  Publisher for `cubrid-lab/pycubrid`, workflow `release.yml`, environment
  `pypi` (<https://pypi.org/manage/project/pycubrid/settings/publishing/>).
- No secret for the cookbook verification: the smoke test runs as a reusable
  workflow inside the release run.
- No tag protection rule that blocks `github-actions[bot]` from creating
  `v*` tags.
