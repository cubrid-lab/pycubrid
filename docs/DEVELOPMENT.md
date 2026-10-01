# Development Guide

Everything you need to set up, test, and contribute to pycubrid.

---

## Table of Contents

- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Project Structure](#project-structure)
- [Running Tests](#running-tests)
  - [Offline Tests](#offline-tests)
  - [Sync/Async Replay Parity](#syncasync-replay-parity)
  - [Integration Tests](#integration-tests)
  - [Code Coverage](#code-coverage)
- [Docker Setup](#docker-setup)
- [Code Style](#code-style)
- [Makefile Commands](#makefile-commands)
- [CI/CD](#cicd)
- [Architecture Overview](#architecture-overview)
- [Adding a New Packet Type](#adding-a-new-packet-type)
- [Adding a New Data Type](#adding-a-new-data-type)
- [Release Process](#release-process)

---

## Prerequisites

| Requirement   | Version  | Notes |
|---------------|----------|-------|
| Python        | 3.10+    | Uses `X | Y` union syntax, `match` statements |
| Docker        | Latest   | Only for integration tests |
| CUBRID Server | 10.2–11.4 | Via Docker or local install |

---

## Installation

```bash
# Clone the repository
git clone https://github.com/cubrid-lab/pycubrid.git
cd pycubrid

# Install in development mode with dev dependencies
pip install -e ".[dev]"

# Or use the Makefile
make install
```

### Dev Dependencies

| Package     | Purpose |
|-------------|---------|
| `pytest`    | Test framework |
| `pytest-cov`| Coverage reporting |
| `ruff`      | Linter and formatter |

---

## Project Structure

```mermaid
graph TD
    root[pycubrid/]

    pkg["pycubrid/ - Main package (9 modules)"]
    tests[tests/ - Test suite]
    docs[docs/ - Documentation]
    pyproject[pyproject.toml - Package configuration]
    makefile[Makefile - Development commands]
    compose[docker-compose.yml - CUBRID container setup]
    changelog[CHANGELOG.md - Release history]
    contributing[CONTRIBUTING.md - Contribution guidelines]
    license[LICENSE - MIT license]
    readme[README.md - Project overview]

    root --> pkg
    root --> tests
    root --> docs
    root --> pyproject
    root --> makefile
    root --> compose
    root --> changelog
    root --> contributing
    root --> license
    root --> readme

    pkg --> init[__init__.py - Public API, PEP 249 module attributes]
    pkg --> connection["connection.py - Connection class (TCP, CAS handshake)"]
    pkg --> cursor["cursor.py - Cursor class (execute, fetch, iterate)"]
    pkg --> types[types.py - PEP 249 type objects and constructors]
    pkg --> exceptions[exceptions.py - Full PEP 249 exception hierarchy]
    pkg --> constants["constants.py - CAS protocol enums (41 function codes, 27+ types)"]
    pkg --> protocol["protocol.py - 20 packet classes (serialize/deserialize)"]
    pkg --> packet[packet.py - PacketWriter + PacketReader primitives]
    pkg --> lob["lob.py - LOB (BLOB/CLOB) support"]
    pkg --> typed[py.typed - PEP 561 marker]

    tests --> conftest["conftest.py - Shared fixtures (mock connection, mock socket)"]
    tests --> test_connection[test_connection.py - Connection lifecycle tests]
    tests --> test_cursor[test_cursor.py - Cursor operations tests]
    tests --> test_types[test_types.py - Type object tests]
    tests --> test_exceptions[test_exceptions.py - Exception hierarchy tests]
    tests --> test_constants[test_constants.py - Constants enumeration tests]
    tests --> test_protocol[test_protocol.py - Packet serialization/deserialization tests]
    tests --> test_packet[test_packet.py - PacketWriter/PacketReader tests]
    tests --> test_lob[test_lob.py - LOB tests]
    tests --> test_pep249[test_pep249.py - PEP 249 compliance tests]
    tests --> test_integration["test_integration.py - Live DB integration tests (requires Docker)"]
    tests --> test_suite[test_suite.py - Extended test suite]

    docs --> doc_connection[CONNECTION.md - Connection guide]
    docs --> doc_types[TYPES.md - Type system reference]
    docs --> doc_api[API_REFERENCE.md - Complete API documentation]
    docs --> doc_protocol[PROTOCOL.md - CAS protocol reference]
    docs --> doc_dev[DEVELOPMENT.md - This file]
    docs --> doc_examples[EXAMPLES.md - Usage examples]
```

---

## Running Tests

### Offline Tests

Most tests are **offline** — they mock the CUBRID connection and test packet serialization, cursor logic, type mapping, and exception handling without a database.

```bash
# Run all offline tests with coverage
pytest tests/ -v --ignore=tests/test_integration.py \
  --cov=pycubrid --cov-report=term-missing --cov-fail-under=95

# Or use the Makefile
make test
```

### Backslash-escape-mode pin

`tests/conftest.py` autouse-pins `no_backslash_escapes` to its legacy default
for every test, because most tests build a `Connection`/`AsyncConnection`
over a scripted fake socket that cannot answer the live `CHAR_LENGTH` escape
probe. A module that needs the *real* probe (against a live server, or a
scripted fake broker that answers it) opts out with `pytest.mark.no_escape_pin`,
registered alongside the module's other markers, e.g.:

```python
pytestmark = [pytest.mark.integration, pytest.mark.no_escape_pin]
```

Opt-out used to be a hardcoded list of filename substrings, which silently
matched unrelated modules (#524, e.g. `"test_integration"` matched every
`test_integration_*.py` file). Mark the module explicitly instead of adding a
new filename fragment.

### Fast Driver Tests vs. Repository Tooling Checks

Among the offline tests, a `repo_tooling`-marked subset (registered in
`pyproject.toml`) checks repository policy and tooling — the docs-sync script,
the PR-title validator, the release scripts, workflow-YAML contracts, the
shared quality gate, and similar (#558). These carry no pycubrid driver
behavior and are excluded from `pycubrid`'s own coverage, so they run in the
dedicated `repo-tooling-tests` CI job instead of the `offline-tests` matrix,
keeping routine driver feedback fast. Moving the check does not change
whether CI requires it: `repo-tooling-tests` is still a required job in the
CI Gate, just like `offline-tests`.

```bash
# Fast driver lane — mocked driver behavior only (what offline-tests runs)
pytest tests/ -m "not integration and not repo_tooling" -v

# Repository tooling lane — policy/tooling checks (what repo-tooling-tests runs)
pytest tests/ -m "repo_tooling" -v

# Both lanes together, still offline (no live CUBRID server)
pytest tests/ -m "not integration" -v
```

A module opts into the tooling lane with an explicit `pytestmark = pytest.mark.repo_tooling`,
not a file move or a path-based collection rule, so nothing needs reorganizing
on disk and nothing is silently dropped from `pytest tests/` (every marker is
additive to the default collection; only `-m` selects or excludes it at run
time). `docs-sync.yml` runs `test_docs_reason.py` with a bare
`python -m unittest discover` and no dependency install, so that module (and
`test_pr_title.py`, at risk of the same thing) imports `pytest` in a
`try`/`except ModuleNotFoundError` and falls back to an empty `pytestmark`
when it is missing — the marker would be meaningless there anyway. A module
only ever run through pytest does not need this guard.

Pure scalar-formatting cases live in `tests/test_param_security.py`'s shared
golden matrix, which checks both backslash modes without a connection (#563).
`tests/test_aio_cursor_parity.py` keeps small sync/async format/bind adapter
checks, including rejection before escape-mode negotiation. Ordinary recovery
tests prefer observable bound SQL and replay sessions; generation fences,
malformed replies and other unobservable safety invariants remain white-box tests.

### Sync/Async Replay Parity

`tests/test_replay_parity.py` checks, offline and in a few seconds, that the
sync `Connection` and the async `AsyncConnection` behave the same against the
same broker. `tests/helpers/replay_broker.py` is a threaded in-process CAS
broker: it accepts any number of TCP sessions (so reconnects are real), answers
each request through a per-scenario *script* with deterministic default replies
built by `tests/helpers/cas_reply.py`, and records every request it receives.

Each scenario is a list of public operations (`open`, `close`, `connect`,
`execute`, `fetchall`, `commit`, `ping`, ...) plus a script. It is replayed
through both drivers, each against a fresh broker, and four aspects are
compared: step outcomes (return value or exception type), the exact requests
sent (session, CAS function, echoed CAS_INFO, arguments), whether the connection
is still usable afterwards (`ping(reconnect=False)`), and the number of TCP
sessions. A scenario names the aspects in which the drivers are *meant* to
differ, with a reason; every other difference fails. A known divergence that is
not fixed yet is recorded with `unintended=` and runs as a strict `xfail`. Each
scenario also carries a `check` run against both observations, so it cannot
silently stop exercising its path.

```bash
pytest tests/test_replay_parity.py -v
```

Covered: connect/close, reconnect after close, autocommit set/restore (explicit,
constructor, never set), handle invalidation by commit/rollback, the OUT_TRAN
`CHECK_CAS` probe, one `CHECK_CAS` recovery (with autocommit restore and
escape-mode re-probe) and a failed recovery, SQL bound to a replaced session
(generation fence, #471/#485), failed `ping()` with and without recovery,
malformed and truncated replies (#533), `DataError` keeping the session (#512)
and the fetch-page `DataError` contract (#536). Task cancellation exists only
in `pycubrid.aio` and is covered by `tests/test_async_cancellation.py` instead.

**Per-operation round-trip budgets (#557):** `Observation.step_functions(i)`
returns the exact, ordered CAS functions sent while running `steps[i]` alone —
separate from connect/setup (step 0, which includes any constructor
autocommit setter and the backslash-escape-mode probe) and from every other
step. `FIRST_INSERT_BUDGET`, `REUSED_CURSOR_INSERT_BUDGET`,
`SELECT_TO_INSERT_BUDGET`, `MANUAL_INSERT_EXECUTE_BUDGET` /
`MANUAL_INSERT_COMMIT_BUDGET`, `FETCH_PAGINATION_BUDGET`, and the
`ESCAPE_EXPLICIT_*` / `ESCAPE_AUTOMATIC_*` budgets name these exact sequences;
each is asserted with list equality, which catches a dropped safety request
(e.g. a missing `CHECK_CAS` liveness probe) exactly as it catches an added
round trip — neither can pass as an "optimization". Each budget also requires
successful operation outcomes and a reusable session; fetch scenarios check the
returned rows. This prevents a malformed reply or wrong result from passing just
because its request count stayed within the budget. Their scripts
(`_autocommit_insert`, `_manual_insert_last_insert_id`) reply to an INSERT's
`PREPARE_AND_EXECUTE` with an explicit `OUT_TRAN` (autocommitting: the
implicit transaction already committed) or `IN_TRAN` (manual: left open for
`commit()`) status, and give `GET_LAST_INSERT_ID` a well-formed value — the
broker's generic default reply for it is a bare response code, which the
driver (correctly) rejects as malformed. These scenarios are the
reproducibility baseline for future round-trip-reduction work (#419/#488/#525):
production optimization and `CHECK_CAS` removal are out of scope here.

The budgets above run against a broker that reports statement pooling off, where
deferred close (#488) never applies, so they are unchanged by it.
`Scenario.statement_pooling=1` makes the broker report pooling on:
`REUSED_CURSOR_INSERT_POOLED_BUDGET` (the previous INSERT's `CLOSE_REQ_HANDLE`
and the `CHECK_CAS` gating it are gone: 4 requests instead of 6) and
`SELECT_TO_INSERT_POOLED_BUDGET` (`CLOSE_REQ_HANDLE` gone: 3 instead of 4) lock
in what deferred close removes, and their checks assert that the next
`PREPARE_AND_EXECUTE` carries exactly the released handle id.
Additional deferred-close scenarios in `tests/test_deferred_close.py` verify
queue overflow, transaction-boundary draining and reconnect safety.

To add a scenario, append a `Scenario` to `SCENARIOS` with its steps, a script
built from `_on(...)` (for example `_hang_up_after_ok` to recycle the CAS after a
reply) and a `check`.

**Intended differences** (not failures):

| Difference | Reason |
|---|---|
| `AsyncConnection(...)` does not connect; `await pycubrid.aio.connect(...)` does | `__init__` cannot await |
| Async autocommit is set with `await conn.set_autocommit(v)` | a property setter cannot await |
| Every async I/O method is a coroutine | asyncio API |
| Async `create_lob()` raises `NotSupportedError` and sends no `LOB_NEW` | no async LOB support (scenario `create_lob`) |
| Task cancellation | async only; excluded from parity |

**Unintended differences found by the harness** (all fixed in #521):

| Difference | Fix |
|---|---|
| Sync `connect()` after `close()` did not re-send an explicit `autocommit` (#520) | `connect()` restores explicit session state on every replacement session |
| Sync `connect(autocommit=True)` used the reconnecting property setter: an extra `CHECK_CAS` between `SET_DB_PARAMETER` and `COMMIT`, a CAS recycled there split them across sessions, and a failure raised the native error with the socket left open | applied on the opened session only, with `OperationalError` on failure, like async |
| Sync `ping(reconnect=False)` kept a session whose `CHECK_CAS` returned a negative code, and the next request reconnected silently | the broken session is closed, like async |
| Async escape probe on a `CHECK_CAS` replacement session sent `auto_commit=0`; every other escape probe (both drivers) sends the connection's flag | the replacement probe sends the connection's flag |

A shared gap (no parity difference) was fixed at the same time: with automatic
escape detection, a CAS recycled right after the probe's `ROLLBACK` made
`connect()` fail before autocommit was applied; both drivers now verify that
OUT_TRAN session with `CHECK_CAS` first and replace it once.

Another shared gap was fixed in #551 (scenario
`autocommit_setter_survives_recycle_after_set_db_parameter`): the public
autocommit setter sent `SET_DB_PARAMETER` and its `COMMIT` on different CAS
sessions when the CAS was recycled between them. The new value is now recorded
before the `COMMIT`, so that request's single reconnect restores it on the
replacement session first; a failed `COMMIT` closes the connection and keeps the
previous value.

### Mutation Testing

Line coverage proves code *runs*; mutation testing proves the tests *catch wrong
behavior*. The driver core (packet/protocol serialization, cursor/connection
lifecycle, LOB I/O) is configured under `[tool.mutmut]` in `pyproject.toml`, run
against the offline suite:

```bash
pip install -e ".[dev,mutation]"
make mutation          # mutmut run && mutmut results
```

Focus on meaningful surviving mutants (a flipped comparison or dropped cleanup
that no test kills), not the raw score.

### Integration Tests

Integration tests (marker `integration`) require a running CUBRID instance.
The simplest path is Docker through the Makefile:

```bash
make integration                          # broker published on localhost:33000
make integration CUBRID_TEST_PORT=33522   # same, on a port nothing else uses
```

`make integration` starts the compose service, waits with
`scripts/wait_for_cubrid.py` (fails the run if the broker is not ready within
about three minutes instead of sleeping a fixed time), runs
`-m "integration and not tls"` with every endpoint field set explicitly, audits
the JUnit report with `scripts/check_integration_lanes.py --results` (an
all-skipped or unclassified-skip run fails), and always removes the container.
TLS tests need an SSL-enabled broker; see
[Async TLS integration tests](#async-tls-integration-tests).

**Enabling integration vs. choosing the endpoint.** Integration tests are
*enabled* when `CUBRID_TEST_URL` or `CUBRID_TEST_HOST` is set to a non-empty
value. The *endpoint* is then resolved field by field by one shared helper,
`tests/_cubrid_endpoint.py`, used by every integration module, the
`tests/conftest.py` gate and `scripts/wait_for_cubrid.py`:

1. the per-field variables `CUBRID_TEST_HOST`, `CUBRID_TEST_PORT`,
   `CUBRID_TEST_DB`, `CUBRID_TEST_USER`, `CUBRID_TEST_PASSWORD` win;
2. otherwise the matching component of
   `CUBRID_TEST_URL=cubrid://user[:password]@host[:port]/database`;
3. otherwise the defaults `localhost`, `33000`, `testdb`, `dba`, empty password.

An empty per-field variable counts as unset, except `CUBRID_TEST_PASSWORD=""`,
which is an explicit empty password. A scheme-less `CUBRID_TEST_URL` (such as
`1`) only enables integration. A `CUBRID_TEST_URL` with a foreign scheme, no
host, a non-numeric port or a malformed database name makes every integration
test error (offline tests are unaffected). Exporting both the URL and the per-field
variables (as CI does) behaves exactly as before; a URL naming a non-default
host or port is now honored instead of silently testing `localhost:33000`.

To run against a server you already started (no Docker lifecycle), set the
endpoint explicitly, preferably with the per-field variables:

```bash
CUBRID_TEST_HOST=127.0.0.1 CUBRID_TEST_PORT=33522 \
  CUBRID_TEST_DB=testdb CUBRID_TEST_USER=dba CUBRID_TEST_PASSWORD= \
  pytest tests/ -m "integration and not slow and not tls" -v
# equivalent: CUBRID_TEST_URL="cubrid://dba@127.0.0.1:33522/testdb" pytest ...
```

**Skip vs. error.** With no endpoint configured, integration tests skip so a
bare `pytest` stays green. With an endpoint configured, the gate probes it once
per session (`SELECT 1`, five-second timeouts); if it is unreachable, every
plain integration test **errors** with the endpoint (without the password) and
the connection error instead of skipping, and pytest exits non-zero. No test
module probes the server at import time. To skip integration tests, unset both
`CUBRID_TEST_URL` and `CUBRID_TEST_HOST`.

The CAS-recycling regressions in `tests/test_integration_cas_reconnect.py`
(#485) change broker parameters with `broker_changer` and run `cubrid broker
reset` inside the server container. They skip unless
`CUBRID_TEST_DOCKER_CONTAINER` names that container, for example
`export CUBRID_TEST_DOCKER_CONTAINER="$(docker compose ps -q cubrid)"`.
`make integration` and the CI integration lanes set it; every change is
restored before the test returns.

#### Async TLS integration tests

`tests/test_aio_ssl_integration.py` adds async TLS coverage for `pycubrid.aio`,
and `tests/test_tls_matrix_integration.py` runs the TLS negative and lifecycle
matrix (unknown CA, hostname mismatch, plaintext/TLS refusal in both
directions, pinned TLS 1.2/1.3, `ssl=True` versus a caller `SSLContext`, read
timeout and dropped transport followed by a TLS reconnect, file-descriptor
leaks) against both the sync and async drivers. Both carry the `integration` and `tls` markers. The repository's
default `docker-compose.yml` starts a plaintext broker only, so these tests are
skipped unless you point them at a separate TLS-enabled broker.

The broker-independent half of the matrix (expired and self-signed
certificates, interrupted or stalled handshakes, TLS-version floors, downgrade
attempts on reconnect, no plaintext fallback) runs offline in
`tests/test_tls_matrix_offline.py` against an in-process OpenSSL peer
(`tests/helpers/tls_broker.py`), so it is part of `make test`. Its test PKI
lives in `tests/fixtures/tls/`; regenerate it with
`tests/fixtures/tls/generate.sh`. The async connect hang on an interrupted
handshake (#513) has its own offline regression suite,
`tests/test_aio_tls_handshake_hang.py`.

Export the normal integration variables plus these TLS overrides as needed:

```bash
export CUBRID_TLS_TEST_HOST=localhost
export CUBRID_TLS_TEST_PORT=33001
export CUBRID_TLS_TEST_DB=testdb
export CUBRID_TLS_TEST_USER=dba
export CUBRID_TLS_TEST_PASSWORD=

# Optional: private CA bundle for ssl.SSLContext/load_verify_locations().
export CUBRID_TLS_TEST_CA_FILE="$PWD/certs/ca.pem"

# Optional: alternate reachable host/IP for hostname-mismatch coverage.
export CUBRID_TLS_TEST_MISMATCH_HOST=127.0.0.1

# Optional: an SSL=OFF broker port on the same server (the stock query_editor
# broker listens on 30000) for TLS-client-to-plaintext-broker refusal coverage.
export CUBRID_TLS_TEST_PLAIN_PORT=30000

# If the broker uses a private CA, also point the process default trust store
# at it so the ssl=True cases (test_aio_ssl_connect_default_context and the
# ssl=True rows of the matrix) can verify the broker.
export SSL_CERT_FILE="$CUBRID_TLS_TEST_CA_FILE"
```

The optional variables only gate individual cases locally: a case skips only
when its configuration is missing. Once `CUBRID_TLS_TEST_CA_FILE` is set, a
broker that is unreachable or not serving TLS fails the tests instead of
skipping them. In CI every variable is set, and
`scripts/check_integration_lanes.py` fails the TLS lane on any skip in these
modules, so a lane that silently skips is red, not green.

Broker-side TLS must already be enabled (`SSL=ON` in `cubrid_broker.conf`) and
the broker certificate must match `CUBRID_TLS_TEST_HOST`. Then run:

```bash
pytest tests/ -m "integration and tls" -v
```

##### Automated TLS coverage in CI

You do not need to run the steps above locally for routine development —
`.github/workflows/integration-full.yml` includes an `integration-tls` job
(Python {3.10, 3.14} × CUBRID 11.4) that:

1. Starts a CUBRID 11.4 container manually (so the broker config can be
   patched after the container is up).
2. Generates a fresh self-signed certificate (`CN=localhost`, `SAN=DNS:localhost`),
   injects it into the container as `cas_ssl_cert.{crt,key}`, then flips
   `SSL=OFF` → `SSL=ON` for `BROKER1` and restarts the broker so the new
   cert is picked up.
3. Exports the generated CA bundle to the Python test fixture via
   `CUBRID_TLS_TEST_CA_FILE` and `SSL_CERT_FILE`.
4. Probes the broker with a real TLS handshake and fails the job loudly
   if TLS is not actually serving — silent skips are explicitly rejected.
5. Runs every `integration and tls` test (`tests/test_aio_ssl_integration.py`
   and `tests/test_tls_matrix_integration.py`) against the TLS broker with the
   `CUBRID_TLS_TEST_*` env vars wired up automatically, including
   `CUBRID_TLS_TEST_PLAIN_PORT=30000` for the container's `SSL=OFF`
   `query_editor` broker.

> **Python 3.10 note**: The driver uses a certificate-verification preflight to
> handle the known CPython async TLS verification failure in 3.10
> ([#156](https://github.com/cubrid-lab/pycubrid/issues/156)). The TLS lane now requires
> every selected test to run, including hostname-verification failure; provisioning
> skips cannot pass the job. Broker status/restart commands run as the `cubrid` service
> owner so the TLS job operates on the actual broker.

This job runs on the same triggers as the rest of `integration-full`
(nightly, via `workflow_dispatch`, and as the release gate called by `release.yml`). `ci.yml` runs the same
lane per pull request as a single Python 3.14 × CUBRID 11.4 cell, and only when
TLS-relevant paths change (the connection modules, `pycubrid/__init__.py`,
`pycubrid/protocol.py`, `pycubrid/aio/`, the TLS and SSL tests,
`tests/helpers/tls_*.py`, `tests/fixtures/tls/`, the lane audit script or
workflows), so
routine PRs do not pay for it.

### Code Coverage

Current test metrics:

| Metric | Value |
|--------|-------|
| Offline tests | 471 |
| Integration tests | 41 |
| Statement coverage | 99.88% |
| Statements | 1,134 |
| Missed | 1 |
| CI threshold | 95% |

```bash
# Generate HTML coverage report
pytest tests/ --ignore=tests/test_integration.py \
  --cov=pycubrid --cov-report=html

# Open in browser
open htmlcov/index.html
```

---

## Docker Setup

### docker-compose.yml

```yaml
services:
  cubrid:
    image: cubrid/cubrid:11.2
    container_name: cubrid-test
    ports:
      - "33000:33000"
    environment:
      CUBRID_DB: testdb
```

### Commands

```bash
# Start with default CUBRID 11.2
docker compose up -d

# Start with specific version
CUBRID_VERSION=11.4 docker compose up -d

# Check container status
docker compose ps

# View logs
docker compose logs -f cubrid

# Stop and cleanup
docker compose down -v
```

### Connection Details

| Parameter | Value |
|-----------|-------|
| Host | `localhost` |
| Port | `33000` |
| Database | `testdb` |
| User | `dba` |
| Password | (empty) |

---

## Code Style

### Ruff Configuration

```toml
[tool.ruff]
line-length = 100
target-version = "py310"
```

### Conventions

- **Imports**: `from __future__ import annotations` in every module
- **Type hints**: Full typing; PEP 561 compliant (`py.typed`)
- **super()**: Always `super().__init__()`, never `super(ClassName, self)`
- **Line length**: 100 characters
- **Docstrings**: Google-style for all public methods and classes
- **Naming**:
  - Classes: `PascalCase` (e.g., `PacketWriter`, `ColumnMetaData`)
  - Private methods: `_underscore_prefix` (e.g., `_parse_byte`, `_write_int`)
  - Constants: `UPPER_SNAKE_CASE` (e.g., `CAS_INFO`, `DATA_LENGTH`)

### Linting

```bash
# Check for issues
make lint

# Auto-fix
make format

# Check tool pins and the active environment separately
make tooling-check
```

Make and regular/maintenance CI share the `LINT_PATHS` list in `Makefile`:
`pycubrid tests scripts demos examples`. Ruff CLI discovery and hook types are explicitly
Python/pyi-only; Markdown is outside this formatting contract. Hooks use the same maintained-file scope;
Mypy remains strict and package-only. The tools run through the active Python
environment, so install `.[dev]` and activate it before running checks.

`pyproject.toml` owns the exact Ruff/Mypy versions. The Ruff and Mypy pre-commit
hooks are `repo: local` / `language: system` hooks that invoke `python3 -m ruff`
and `python3 -m mypy` against that same active environment, so there is no
separate hook revision to keep in sync: bumping the dev pin (Dependabot's `pip`
ecosystem does exactly this) and reinstalling `.[dev]` is enough. Activate that
environment (or a venv where it's installed) whenever a commit should run the
hooks; otherwise Ruff/Mypy are missing or a stale/global version silently runs
instead of the pinned one. Update the dev pin, reinstall `.[dev]`, and run
`make check-all` plus `pre-commit run --all-files`. `make tooling-check` rejects
pin, installed-version, hook-scope, and CI-scope drift before
lint/format/typecheck.

### Anti-Patterns (Never Do)

- No f-string interpolation in SQL queries (SQL injection risk)
- No `super(ClassName, self)` — use `super()` only
- No Python 2 constructs
- No empty `except` blocks (except in cleanup paths like `close()`)
- No type suppression (`# type: ignore` without explanation)

---

## Makefile Commands

| Command | Description |
|---------|-------------|
| `make install` | Install in dev mode with all dependencies |
| `make test` | Run offline tests with coverage |
| `make lint` | Run ruff check + format check |
| `make format` | Auto-fix lint and formatting issues |
| `make integration` | Docker → readiness wait → integration tests → skip audit → cleanup (`CUBRID_TEST_PORT=<port>` to move the broker) |
| `make integration-local` | Integration tests against an already-running server (`CUBRID_TEST_URL` or `CUBRID_TEST_HOST`/`PORT`) |
| `make clean` | Remove build artifacts |

---

## CI/CD

The regular and full integration workflows run `python scripts/wait_for_cubrid.py`
before the tests. It resolves the endpoint exactly like the test suite (per-field
`CUBRID_TEST_HOST`, `CUBRID_TEST_PORT`, `CUBRID_TEST_DB`, `CUBRID_TEST_USER`,
`CUBRID_TEST_PASSWORD`, then `CUBRID_TEST_URL`, then `localhost:33000/testdb`,
user `dba`, empty password), executes `SELECT 1`, and
fails the job if no probe succeeds within 30 attempts with 5 seconds between
attempts. Infrastructure failure therefore stops the test step.
Each connection and read has a five-second timeout, configurable through
`CUBRID_TEST_CONNECT_TIMEOUT` and `CUBRID_TEST_READ_TIMEOUT`. A connected broker
whose `SELECT 1` fails is not ready. The cursor and connection are closed on both
success and failure, including cursor-cleanup errors.

Integration tests are assigned by pytest markers, with no fixed test-count or
filename-glob inventory:

| Lane | Selection | Executable workflow path |
|---|---|---|
| Normal | `integration and not slow and not tls` | Regular PR/push CI, full compatibility matrix, and nightly bug hunt |
| Slow | `integration and slow and not tls` | Nightly/manual bug hunt: soak, chaos, and concurrency stress |
| TLS | `integration and tls` | Dedicated TLS jobs in regular CI and the full workflow |
| Official differential | `integration and official_differential` | Required `official-differential` job (Python 3.10, CUBRID 10.2 and 11.4) in regular CI and the full workflow |

`python scripts/check_integration_lanes.py` collects the current marker inventory
and checks that each lane has an executable workflow command. The JUnit audit
(`--results FILE`) fails unknown skips or empty/all-skipped runs. Outside its own
lane the official-driver differential skips as `official-lane-only`, and platforms
without `/proc` have an explicit skip category. Missing broker/TLS configuration
is not an accepted CI skip. `--lane official` accepts no skip at all.

The official-driver differential (#446) compares pycubrid with the official
`CUBRIDdb`/`_cubrid` driver built from pinned source. To reproduce it locally
(Linux x86_64, git, CMake 3.21 or newer, a C compiler and Python 3.10 headers),
run:

```bash
python3.10 scripts/build_official_oracle.py --out .official-oracle
PYTHONPATH=.official-oracle PYCUBRID_OFFICIAL_ORACLE_REQUIRED=1 \
  PYCUBRID_OFFICIAL_ORACLE_MANIFEST=.official-oracle/oracle.json \
  PYCUBRID_DIFFERENTIAL_EVIDENCE=official-evidence.jsonl \
  CUBRID_TEST_URL=cubrid://dba@localhost:33000/testdb \
  python3.10 -m pytest tests/ -m "integration and official_differential"
python scripts/check_official_differential.py --evidence official-evidence.jsonl
```

A new or changed claim goes in `tests/fixtures/official_differential_claims.json`.
Its case goes in `CASES` in `tests/test_official_differential.py`. Then run
`python scripts/check_official_differential.py --write-docs`. A divergence is
either fixed, or recorded as a `deviation` with a reason, an issue and both
observed values. Never edit an expected value just to match current output. See
[the compatibility guide](UPSTREAM_COMPATIBILITY.md#official-driver-differential-gate-446).
The nightly bug hunt also retains separate offline protocol, fault-broker, and
placeholder checks under the wider Hypothesis profile.

`tests/test_protocol_fuzz.py` mutates realistic broker replies built by
`tests/helpers/cas_reply.py` (#523): execute and FETCH replies with column
metadata and populated rows for every common type, plus schema, batch and LOB
replies. Each seed records its expected decoded values and the offsets of its
length words, counts and field boundaries, so the unmutated seed is an exact
round-trip check and mutations aim at truncation and length/count mismatches.
To seed a new column mix, add a `ResultSet` to `RESULT_SETS`; the FETCH and
execute targets pick it up. A new reply builder needs its own round-trip test
and fuzz target. Example budgets come from the Hypothesis
profile (`pr`: 50 examples per target, about 2 s for the module; `nightly`: 1000).

### Documentation exceptions and contributor validation

The docs gate accepts a populated `Docs: not needed -` reason on a standalone
physical source line outside quotes, comments and code fences. It may be adjacent
to ordinary prose without its own paragraph. The existing `docs-not-needed` label
remains a separate exception.
Markers may have zero to three leading spaces; indented code and raw HTML quote,
preformatted or code blocks remain examples rather than authorizations.
Run `make docs-reason-check` for helper doctests and real event-JSON workflow cases;
these checks also run in `make check-all` and docs-sync CI.

Contributors report actual commands/results and checks not run with reasons;
optional AI review is recorded separately. Maintainers coordinate internal reviews,
actual issue labels and explicitly approved `translations-deferred` follow-ups.
Translation requests in a PR body do not grant approval. Korean README sync remains
required; other community translations remain advisory.

The shared doc-lint and CodeQL callers use reviewed commit SHAs with verified
`workflow_call` inputs. Existing permissions, advisory rollout and required gates
remain intact. The doc-lint workflow still downloads main-based config/scanner
assets; pinning its caller is not a complete freeze of those assets.

### GitHub Actions Workflows

| Workflow | Trigger | Description |
|----------|---------|-------------|
| `ci.yml` | Push to main, PRs | Lint + offline tests (Python 3.10–3.14) + integration |
| `integration-full.yml` | Nightly, manual dispatch, called by `release.yml` | Full Python × CUBRID compatibility matrix |
| `prepare-release.yml` | Manual dispatch (`-f version=X.Y.Z`) | Open the `chore: release vX.Y.Z` PR (dated CHANGELOG section + version bump) |
| `release.yml` | Push to main, recovery dispatch | Detect a merged release PR, then full matrix, build, tag + GitHub Release + PyPI, cookbook verification |

### CI Matrix

- **Offline**: Python 3.10, 3.11, 3.12, 3.13, 3.14
- **Integration**: two selected cells, Python 3.14 / CUBRID 11.4 and Python 3.10 / CUBRID 10.2 (reduced PR matrix;
  the full 5×4 matrix runs in `integration-full.yml`)

### PR verification cost (#564)

Measured from real `ci.yml` runs (GitHub REST `/actions/runs/{id}/timing`
`run_duration_ms`, and job-step `started_at`/`completed_at`), not estimates.
The ordinary run object omits `run_duration_ms`; the `/timing` endpoint
provides it. Baseline code PR:
[run 36929613502](https://github.com/cubrid-lab/pycubrid/actions/runs/36929613502),
2026-10-01, 306s (5m06s), with all integration paths selected. A separate
docs-only example, [PR #587](https://github.com/cubrid-lab/pycubrid/pull/587)
(`RELEASING.md` only), took 419s in
[run 36865409050](https://github.com/cubrid-lab/pycubrid/actions/runs/36865409050):
all four code/TLS-gated integration jobs skipped and doc-lint passed. Its
`detect-changes` job did not start until about three minutes after the workflow,
so that elapsed time mainly illustrates queue variance, not a cache comparison.

| Job group | Jobs | Wall time (longest job) | Notes |
|---|---|---|---|
| `offline-tests` matrix | 10 (2 OS × 5 Python) | 78s–124s | Editable dev installs took 11–23s; macOS was 9–51% slower than Linux by Python version in this one run, not a stable ratio. |
| `integration-tests` / `integration-charset` / `integration-tls` / `official-differential` | 5 jobs, 6 CUBRID containers | 65s–118s | Service-container initialization took 15–41s where present; TLS starts Docker inside its own step. Editable dev installs took 17–22s. |
| `repo-tooling-tests` matrix | 2 (ubuntu, macos) | 32s–51s | Editable dev installs took 14–15s. |
| `lint` / `typecheck` / `compat-check` / `packaging-smoke-test` | 4 | 9s–24s | Lint/typecheck install dev tools; compat installs the package only (3s), packaging installs `build` (2s). |
| `doc-lint` (reusable) | path-gated on docs changes | 2s–8s per sub-step | Skips entirely when no Markdown/`docs/**` changed. |

The `needs:` graph has parallel roots: `detect-changes`, the offline matrix,
lint, typecheck, repository tooling and compat-check. In the baseline run,
offline jobs started *before* `detect-changes` finished. Packaging waits for
all offline cells; the container-based jobs then wait for packaging, offline,
lint, typecheck and `detect-changes`, and `ci-gate` waits for their results.
Queue time and the slowest prerequisite branch also affect workflow elapsed
time; a simple sum of job durations is not the critical path.

**Fixable setup cost**: the baseline expanded to 21 jobs using
`actions/setup-python`; 19 performed an editable dev install, while compat
installed `-e .` and packaging installed `build`. Each job still needs its
own install. The new `cache: pip` input caches pip's global download cache,
**not** the installed environment. Its key includes OS, Python version and
the dependency-file hash: matching OS/Python jobs can reuse downloads once a
cache is saved, including on later runs, but distinct matrix cells do not
share a single cache. Concurrent first-run jobs can all miss. See the
[setup-python caching guide](https://github.com/actions/setup-python#caching-packages-dependencies).
The first changed-head run had a pip cache miss for Ubuntu/Python 3.10 and
saved the cache afterward; it took 328s versus the 306s baseline. That cold
run does **not** demonstrate an overall speedup. Docker startup is also a
substantial cost and remains unchanged here.

**Path-filter trigger audit**: spot-checked `detect-changes` outputs against
actual job results across recent PR runs.
[PR #597](https://github.com/cubrid-lab/pycubrid/pull/597), which fixes
[issue #595](https://github.com/cubrid-lab/pycubrid/issues/595) without
touching TLS paths, produced a `skipped` `integration-tls` job in
[run 36879861578](https://github.com/cubrid-lab/pycubrid/actions/runs/36879861578),
while both regular integration cells, charset and official differential
passed. Docs-only [PR #587](https://github.com/cubrid-lab/pycubrid/pull/587)
skipped all four code/TLS-gated integration jobs as intended. The unchanged
`ci-gate` accepts `skipped` only for these path-gated jobs, not a failed or
cancelled one.
The audit found one real gap: `scripts/wait_for_cubrid.py` is invoked by
every container-based job (`integration-tests`, `integration-charset`,
`official-differential`) but was missing from the `code:` filter list, so a
PR touching only that script would have skipped all code-gated integration
coverage before merge. Added to `code:` and locked by a repository-tooling
test, so this only *adds* coverage and cannot produce a new skip.

The failure gate also has real evidence: in
[run 36776514307](https://github.com/cubrid-lab/pycubrid/actions/runs/36776514307),
a claimed official behavior comparison failed and the `CI Gate` failed.
A repository-tooling test now executes the unchanged gate shell with synthetic
`failure`/`cancelled` official and integration results; each exits nonzero,
while expected docs-only skips pass. Neither the official comparison nor the
gate was weakened.

**Changes made** (both additive/safe; no job removed, no coverage reduced, no
required check or branch-protection context touched, `ci-gate`'s
pass/fail logic for skipped vs. failed/cancelled required jobs is unchanged):

1. `cache: pip` + `cache-dependency-path: pyproject.toml` added to every
   `actions/setup-python` step in `ci.yml` (10 YAML steps, 21 expanded jobs).
   Jobs with a matching OS/Python/cache key can reuse downloaded wheels once
   an earlier job or run has saved them; the editable install still runs.
2. `scripts/wait_for_cubrid.py` added to the `code:` path filter (closes the
   gap above).

**After**: this PR's first changed-head run,
[36932083505 attempt 1](https://github.com/cubrid-lab/pycubrid/actions/runs/36932083505),
missed the Ubuntu/Python 3.10 pip cache and saved it afterward. Its elapsed
time was about 328s, *longer* than the 306s baseline. One rerun of the
same head (attempt 2) logged a cache hit and successful restore for that
OS/Python key and finished in 295s by `/timing`: 11s (about 3.6%) below
the baseline and about 33s below its cold attempt. Across the same 19
editable-dev install steps, the sum of per-job durations was 321s baseline,
275s cold and 266s warm. Those jobs overlap, so their sum is **not**
wall-clock time saved; individual installs varied (the warm lint install
was slower). The observed result supports a modest, targeted setup gain,
not a guaranteed per-PR speedup or proof that the cache alone caused the
workflow-level difference. Runner queue and Docker startup also varied.

---

## Architecture Overview

```mermaid
graph TD
    user[User Code] --> init[__init__.py - Module API connect/types/exceptions]
    init --> connection[connection.py - TCP socket, CAS handshake, session]
    connection --> cursor[cursor.py - SQL execution, parameter binding, fetch]
    cursor --> protocol[protocol.py - 20 packet classes serialize/deserialize]
    protocol --> packet[packet.py - PacketWriter + PacketReader binary I/O]
    packet --> constants[constants.py - CAS function codes, data types, enums]

    types[types.py - PEP 249 types] --> cursor
    exceptions[exceptions.py - PEP 249 errors] --> connection
    lob[lob.py - LOB objects] --> connection
```

### Data Flow

1. **User** calls `cursor.execute("SELECT ...")`
2. **Cursor** binds parameters, creates `PrepareAndExecutePacket`
3. **Connection** calls `_send_and_receive(packet)`
4. **PacketWriter** serializes the request with protocol header
5. **Socket** sends bytes to CAS server
6. **Socket** receives response bytes
7. **PacketReader** deserializes the response
8. **Packet** parses column metadata and row data
9. **Cursor** stores rows for `fetchone()`/`fetchall()`

---

## Adding a New Packet Type

To add a new CAS function:

1. **Add the function code** to `CASFunctionCode` in `constants.py`:

   ```python
   class CASFunctionCode(IntEnum):
       # ... existing codes ...
       MY_NEW_FUNCTION = 42
   ```

2. **Create the packet class** in `protocol.py`:

   ```python
   class MyNewPacket:
       """Description (FC=42)."""

       def __init__(self, arg1: int) -> None:
           self.arg1 = arg1
           self.result: str = ""

       def write(self, cas_info: bytes) -> bytes:
           writer = PacketWriter()
           writer._write_byte(CASFunctionCode.MY_NEW_FUNCTION)
           writer.add_int(self.arg1)
           payload = writer.to_bytes()
           header = build_protocol_header(len(payload), cas_info)
           return header + payload

       def parse(self, data: bytes) -> None:
           reader = PacketReader(data)
           _ = reader._parse_bytes(DataSize.CAS_INFO)
           response_code = reader._parse_int()
           if response_code < 0:
               remaining = len(data) - 8
               _raise_error(reader, remaining)
           # Parse result-specific data
           self.result = reader._parse_null_terminated_string(response_code)
   ```

3. **Add tests** in `tests/test_protocol.py`:

   ```python
   def test_my_new_packet_write():
       packet = MyNewPacket(arg1=42)
       data = packet.write(b"\x00\x00\x00\x00")
       assert len(data) > 8  # Header + payload
   ```

---

## Adding a New Data Type

To support a new CUBRID data type:

1. **Add the type code** to `CUBRIDDataType` in `constants.py`
2. **Add the reader** in `_read_value()` in `protocol.py`
3. **Add the writer** in `PacketWriter` in `packet.py` (if needed)
4. **Add tests** for both reading and writing

---

## Release Process

Releases are maintainer-only and follow [RELEASING.md](https://github.com/cubrid-lab/pycubrid/blob/main/RELEASING.md):
`prepare-release.yml` opens a release PR (version bump + dated CHANGELOG section, checked
with `make release-check VERSION=X.Y.Z`); after review and squash-merge, `release.yml`
runs the full matrix, builds once, tags, publishes to PyPI and verifies the cookbook
automatically. Nobody pushes tags or publishes by hand.
