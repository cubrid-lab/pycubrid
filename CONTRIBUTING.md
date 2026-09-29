# Contributing to pycubrid

Thank you for your interest in contributing to `pycubrid`.

Write GitHub issues, PRs and comments in English; localized documentation remains
welcome, and no specific translation tool is required.

## Development Setup

### Prerequisites

- Python 3.10+
- Git
- Docker (for integration tests)

### Installation

```bash
git clone https://github.com/cubrid-lab/pycubrid.git
cd pycubrid

python3 -m venv venv
source venv/bin/activate

pip install -e ".[dev]"
```

## Running Tests

### Offline tests

```bash
make test
```

### Integration tests

```bash
docker compose up -d
export CUBRID_TEST_URL="cubrid://dba@localhost:33000/testdb"
pytest tests/ -m "integration and not slow and not tls" -v
docker compose down -v
```

### Async TLS integration tests (optional)

The dedicated CI TLS lane selects `integration and tls` against an SSL-enabled
broker. For local broker/certificate setup, follow the existing
[async TLS instructions](docs/DEVELOPMENT.md#async-tls-integration-tests).
Record live checks not run and their reason; maintainers coordinate missing
broker/version coverage for connection or protocol changes.

## Code Style

This project uses Ruff for linting and formatting.

```bash
make lint
```

To auto-fix:

```bash
make format
```

Activate the project development environment (`pip install -e ".[dev]"`) before
running Make or pre-commit. The Ruff and Mypy pre-commit hooks are `repo: local`,
`language: system` hooks that invoke `python3 -m ruff`/`python3 -m mypy` from that
same active environment, so there is a single source of truth for each tool's
version: the exact pin in `pyproject.toml`. Activate that environment (or a venv
where it's installed) whenever a commit should run the hooks; otherwise
Ruff/Mypy are missing or a stale/global version silently runs instead of the
pinned one. `make tooling-check` verifies the installed Ruff/Mypy versions
against those pins and confirms the hooks are wired to run through the active
environment. Make and CI share `LINT_PATHS` (`pycubrid tests scripts demos
examples`) with explicit Python/pyi discovery and hook types, so Markdown is not
reformatted; package-only strict Mypy remains separate, and its pre-commit hook
explicitly checks `pycubrid/`.

When updating either tool, change its dev pin in `pyproject.toml`, reinstall
`.[dev]`, then run `make check-all` and `pre-commit run --all-files`; there is no
separate hook revision to edit. The drift gate rejects missing/ambiguous pins,
an installed version that no longer matches the pin, and narrowed scopes.
Dependabot's `pip` ecosystem can bump the `pyproject.toml` pin on its own and CI
stays green, since the pre-commit hooks always run whatever is installed.

## Pull Request Guidelines

1. Keep changes focused and explain the motivation in the PR description.
2. Add or update tests for behavior changes.
3. Run `make check-all` and `make test`; report commands/results and checks not run.
4. Run integration tests for connection/protocol-related updates.
5. Update `CHANGELOG.md` for user-visible changes.

Outside contributors provide motivation, implementation, tests and affected docs.
Maintainers coordinate internal Oracle/agent reviews, integration coverage and
release classification. These project tools are not an installation prerequisite
for external contributions. Preserve contributor authorship; add tool attribution
only when that tool actually produced a commit.

If docs are unnecessary, put a real reason on a standalone physical source line beginning
`Docs: not needed -`. Empty text, `<reason>`, quotations, comments and fenced examples
do not grant an exemption. The line may be adjacent to ordinary prose; it does not
need its own paragraph. The existing `docs-not-needed` label is a separate
maintainer-controlled exception; neither docs exception bypasses code, security or
release checks.
Up to three leading spaces are allowed; tab/four-space code examples and raw HTML
`blockquote`/`pre`/`code` blocks do not grant an exemption.

For translation help, name the missing language(s) and explain the constraint in
the PR body. That request does not authorize deferral. Maintainers explicitly approve
the existing `translations-deferred` label and own the recorded follow-up. Korean
README synchronization remains required; other translations remain advisory.

When changing docs, regenerate `docs/llms-full.txt` with
`python scripts/generate_llms_full.py` (it also copies the canonical
`docs/llms.txt` index to the root `llms.txt`; edit only `docs/llms.txt`, and CI
fails if either generated file is stale) and run the existing site check
`mkdocs build --strict` after installing its documented tooling
(`mkdocs-material pymdown-extensions`). AI review feedback is separate from commands
actually executed; report both accurately, including gaps and existing warnings.

Maintainers update shared workflow callers through a reviewed upstream commit SHA:
verify the target workflow and its `workflow_call` inputs at that commit, update
callers together and run required checks. The shared doc-lint workflow still fetches
main-based configuration/scanner assets, so caller pinning does not freeze those assets.

## Reporting Issues

Search for an existing issue first, then use the closest issue form. Keep its
prefilled title prefix (`fix:`, `feat:`, `chore:`, or `perf:`); for a custom
issue, use a short type prefix such as `docs:`, `ci:`, or `test:`. An optional
scope goes before the colon, for example `fix(protocol): ...`.

Reporters describe impact and reproduction; they do **not** need permission
to apply GitHub labels. Maintainers assign a type label, one
`priority: <value>` and one `size: <value>` label (plus `area:` when relevant).
Topical labels such as `testing` may also be present.
Human-submitted CLI/API issues with incomplete metadata receive
`status: needs triage`. The maintainer corrects the metadata and removes
that label. Workflows creating issues with `GITHUB_TOKEN` must set the title
and labels themselves: GitHub does not start another workflow from that event.

When filing an issue, include:

- Python version
- CUBRID server version
- Minimal reproduction snippet
- Full traceback or error output

Describe urgency and estimated effort. Maintainers or triagers assign the actual
canonical `priority:`/`size:` GitHub labels; reporter label permissions are not required.

## Bug Discovery → Regression Workflow

pycubrid runs an adversarial "Bug Hunt" test layer (property fuzzing,
protocol fuzzing, state machines, metamorphic parity, fault injection,
differential, mutation, resource/soak). Every defect surfaced by any of
these — or by manual review or downstream dogfooding — MUST follow this
cycle before the fix lands:

```
Bug
 → Minimal reproduction
 → GitHub Issue (label: bug + area:)
 → Failing regression test (committed FIRST, red)
 → Fix
 → Permanent regression contract (the test is now green and kept)
```

Rules:

1. **No bug fix without a regression test** unless it is technically
   impossible to write one; if impossible, say so explicitly in the PR.
2. **`xfail` requires a linked issue.** Use `@pytest.mark.xfail(strict=True,
   reason="issue #NNN: ...")` for a known-but-unfixed defect so the guard
   flips to a hard failure (XPASS) the moment the bug is fixed, forcing the
   `xfail` marker to be removed in the fixing PR.
3. **No blanket `|| true`** or bare `except: pass` to hide failures in test
   or CI code.
4. **An unexpected `XPASS` is a signal**, not noise: the referenced bug is
   fixed — remove the `xfail` and assert the correct behavior in the same PR.
5. **Classify known limitations** as driver / server / upstream so a reader
   can tell whether the divergence is pycubrid's responsibility. Documented
   server-behavior and implementation-difference divergences are pinned in
   the relevant test (e.g. the CUBRIDdb differential suite) rather than left
   as silent skips.

Worked example: issue #362 (`Lob.read` silent truncation) was found by the
LOB adversarial suite, filed with a minimal repro, guarded by a strict
`xfail` regression test, then fixed — the `xfail` became a passing assertion
in the fixing PR.

See [`RELEASE_POLICY.md`](RELEASE_POLICY.md) §7 for where behavior-change
classifications are recorded.
