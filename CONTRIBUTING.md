# Contributing to pycubrid

Thank you for your interest in contributing to `pycubrid`.

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
running Make or pre-commit. `make tooling-check` verifies the installed Ruff/Mypy
versions and hook revisions against the exact pins in `pyproject.toml`. Make and CI
share `LINT_PATHS` (`pycubrid tests scripts demos examples`) with explicit Python/pyi discovery
and hook types, so Markdown is not reformatted; package-only strict Mypy remains
separate, and its pre-commit hook explicitly checks `pycubrid/`.

When updating either tool, change its dev pin and matching hook revision in the
same PR, reinstall `.[dev]`, then run `make check-all` and
`pre-commit run --all-files`. The drift gate rejects missing/ambiguous pins,
version mismatches, and narrowed scopes. Hook updates use this documented process;
there is no additional Dependabot ecosystem configuration.

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

If docs are unnecessary, put a real reason on a standalone paragraph line beginning
`Docs: not needed -`. Empty text, `<reason>`, quotations, comments and fenced examples
do not grant an exemption. Leave a blank line after a quoted block before the real
reason. The existing `docs-not-needed` label is a separate maintainer-controlled
exception; neither docs exception bypasses code, security or release checks.

For translation help, name the missing language(s) and explain the constraint in
the PR body. That request does not authorize deferral. Maintainers explicitly approve
the existing `translations-deferred` label and own the recorded follow-up. Korean
README synchronization remains required; other translations remain advisory.

When changing docs, regenerate `docs/llms-full.txt` with
`python scripts/generate_llms_full.py` and run the existing site check
`mkdocs build --strict` after installing its documented tooling
(`mkdocs-material pymdown-extensions`). AI review feedback is separate from commands
actually executed; report both accurately, including gaps and existing warnings.

Maintainers update shared workflow callers through a reviewed upstream commit SHA:
verify the target workflow and its `workflow_call` inputs at that commit, update
callers together and run required checks. The shared doc-lint workflow still fetches
main-based configuration/scanner assets, so caller pinning does not freeze those assets.

## Reporting Issues

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
