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
to CPython [gh-142352](https://github.com/python/cpython/issues/142352) — run
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
