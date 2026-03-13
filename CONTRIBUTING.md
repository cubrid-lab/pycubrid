# Contributing to pycubrid

Thank you for your interest in contributing to `pycubrid`.

## Development Setup

### Prerequisites

- Python 3.10+
- Git
- Docker (for integration tests)

### Installation

```bash
git clone https://github.com/cubrid-labs/pycubrid.git
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
