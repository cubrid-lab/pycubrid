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
pip install pytest-cov
```

## Running Tests

### Offline tests

```bash
pytest tests/ -v --ignore=tests/test_integration.py \
  --cov=pycubrid --cov-report=term-missing --cov-fail-under=95
```

### Integration tests

```bash
docker compose up -d
export CUBRID_TEST_URL="cubrid://dba@localhost:33000/testdb"
pytest tests/test_integration.py -v
docker compose down -v
```

### Async TLS integration tests (optional)

`tests/test_aio_ssl_integration.py` exercises the live `loop.start_tls()`
upgrade against a CUBRID broker with `SSL=ON`. CI runs this in the
`integration-tls` job in `.github/workflows/integration-full.yml`; for
local runs:

```bash
# Spin up a broker with SSL=ON (mirrors the CI job)
make integration-tls

# Or manually:
docker compose up -d
docker compose exec <broker-container> bash -c "sed -i 's/^SSL=OFF/SSL=ON/' \
    \$CUBRID/conf/cubrid_broker.conf && cubrid broker restart"
docker compose exec <broker-container> cat \
    \$CUBRID/conf/cas_server_cert.pem > /tmp/cubrid-broker.pem

export CUBRID_TLS_TEST_HOST=localhost
export CUBRID_TLS_TEST_PORT=33000
export CUBRID_TLS_TEST_CA=/tmp/cubrid-broker.pem
export CUBRID_TLS_TEST_DB=testdb
export CUBRID_TLS_TEST_USER=dba
pytest tests/test_aio_ssl_integration.py -v
```

The `test_aio_ssl_handshake_failure` test is auto-skipped on Python 3.10 due
to a known CPython asyncio TLS handshake bug on Python 3.10 — run
the suite on 3.11+ to cover the negative path.

## Code Style

This project uses Ruff for linting and formatting.

```bash
ruff check pycubrid/ tests/
ruff format --check pycubrid/ tests/
```

To auto-fix:

```bash
ruff check --fix pycubrid/ tests/
ruff format pycubrid/ tests/
```

## Pull Request Guidelines

1. Keep changes focused and explain the motivation in the PR description.
2. Add or update tests for behavior changes.
3. Ensure lint and offline tests pass before submitting.
4. Run integration tests for connection/protocol-related updates.
5. Update `CHANGELOG.md` for user-visible changes.

## Reporting Issues

When filing an issue, include:

- Python version
- CUBRID server version
- Minimal reproduction snippet
- Full traceback or error output

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
