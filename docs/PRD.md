# PRD: pycubrid — Pure Python DB-API 2.0 Driver for CUBRID

## 1. Overview

**Project**: pycubrid
**Current Version**: 0.5.0
**Status**: Production-ready
**Repository**: [github.com/cubrid-labs/pycubrid](https://github.com/cubrid-labs/pycubrid)
**License**: MIT

### 1.1 Problem Statement

CUBRID's existing Python driver (`CUBRIDdb`) is a C-extension module with significant
limitations:

- Requires C compiler and CUBRID C libraries to install
- Platform compatibility issues (fails on many Linux distributions, macOS ARM)
- No official wheels — users must compile from source
- Not PEP 249 compliant — missing standard exception hierarchy, type objects
- No type hints, no PEP 561 marker
- Abandoned maintenance — last meaningful update years ago

Modern Python developers expect:

- `pip install` that just works — no C build dependencies
- Full PEP 249 (DB-API 2.0) compliance
- Type annotations for IDE support
- Context managers for resource cleanup
- Comprehensive test coverage

### 1.2 What Was Built

A complete pure Python implementation of the CUBRID CAS protocol:

- **Full PEP 249 (DB-API 2.0) compliance** — standard exception hierarchy, type objects, cursor interface
- **Pure Python** — no C extensions, no compilation, works everywhere Python runs
- **Direct CAS protocol** — speaks CUBRID's binary protocol natively over TCP
- **471 offline tests** with **99.88% code coverage**
- **PEP 561 typed package** — `py.typed` marker for modern IDE and static analysis
- **LOB support** — CLOB and BLOB handling via `create_lob()`
- **Prepared statements** — server-side statement preparation and execution
- **Batch operations** — `executemany()` and `executemany_batch()` for bulk inserts
- **CI/CD** — Python 3.10–3.13 × CUBRID 11.2–11.4 matrix

### 1.3 Success Criteria — Status

| Criterion | Target | Achieved |
|---|---|---|
| Pure Python (no C extensions) | ✅ | ✅ `pip install pycubrid` |
| PEP 249 (DB-API 2.0) compliant | ✅ | ✅ Full API compliance |
| Offline tests (no live DB) | ✅ | ✅ 471 tests, 99.88% coverage |
| LOB (CLOB/BLOB) support | ✅ | ✅ `create_lob()`, read/write |
| Prepared statements | ✅ | ✅ `cursor.prepare()` / `cursor.execute()` |
| CI/CD with version matrix | ✅ | ✅ Py 3.10–3.13 × CUBRID 11.2–11.4 |
| Publishable to PyPI | ✅ | ✅ Release workflow on tag |
| ≥ 95% code coverage | ✅ | ✅ 99.88% (CI-enforced) |
| Comprehensive documentation | ✅ | ✅ 6 guide files + README |
| PEP 561 typed package | ✅ | ✅ `py.typed` marker |

---

## 2. Technical Architecture

### 2.1 Module Structure

```
pycubrid/                    # 10 modules
├── __init__.py              # Public API — connect(), types, exceptions, __version__
├── connection.py            # Connection class — connect, commit, rollback, cursor, LOB
├── cursor.py                # Cursor class — execute, fetch, prepare, callproc, iterator
├── types.py                 # DB-API 2.0 type objects and constructors
├── exceptions.py            # PEP 249 exception hierarchy
├── constants.py             # CAS function codes, data types, protocol constants
├── protocol.py              # CAS wire protocol packet classes (18 packet types)
├── packet.py                # Low-level packet reader/writer
├── lob.py                   # LOB (Large Object) support
└── py.typed                 # PEP 561 marker
```

### 2.2 Dependency Matrix

| Package | Version | Purpose |
|---|---|---|
| Python | ≥ 3.10 | Runtime |
| pytest | ≥ 7.0 | Testing (dev) |
| ruff | ≥ 0.4 | Lint + format (dev) |

**Zero runtime dependencies** — pycubrid uses only the Python standard library.

### 2.3 PEP 249 Compliance

| Attribute | Value |
|---|---|
| `apilevel` | `"2.0"` |
| `threadsafety` | `1` (connections cannot be shared between threads) |
| `paramstyle` | `"qmark"` (positional parameters `?`) |

Full standard exception hierarchy: `Warning`, `Error`, `InterfaceError`, `DatabaseError`,
`OperationalError`, `IntegrityError`, `InternalError`, `ProgrammingError`, `NotSupportedError`

Standard type objects: `STRING`, `BINARY`, `NUMBER`, `DATETIME`, `ROWID`

Standard constructors: `Date()`, `Time()`, `Timestamp()`, `Binary()`,
`DateFromTicks()`, `TimeFromTicks()`, `TimestampFromTicks()`

---

## 3. Implemented Features

### 3.1 Connection Management

- `connect()` factory function with keyword arguments
- Auto-commit control via `connection.autocommit` property
- Context manager support (`with` statement)
- Server version detection via `connection.get_server_version()`
- `connection.close()` for explicit cleanup

### 3.2 Cursor Operations

- `execute(sql, params?)` — execute with optional parameters
- `executemany(sql, seq_of_params)` — batch execute
- `executemany_batch(sql, seq_of_params)` — optimized batch insert
- `fetchone()` / `fetchmany(size)` / `fetchall()` — result retrieval
- `prepare(sql)` — server-side prepared statements
- `callproc(procname, params)` — stored procedure calls
- Iterator protocol — `for row in cursor`
- Context manager — `with conn.cursor() as cur`
- `description` attribute — column metadata after execute

### 3.3 LOB Support

- `connection.create_lob(lob_type)` — create CLOB (type=24) or BLOB (type=23)
- Read/write large text and binary data
- Insert strings/bytes directly into CLOB/BLOB columns

### 3.4 Schema Introspection

- `connection.get_schema_info()` — tables, columns, indexes, constraints

### 3.5 CAS Protocol

Direct implementation of CUBRID's Client Application Server (CAS) binary protocol:

- 18 packet types covering all database operations
- Two-step connection: broker handshake → CAS session
- Big-endian binary codec for all data types
- Server-side cursor with lazy fetch for large result sets

---

## 4. Test Coverage

### 4.1 Test Matrix

| Test File | Tests | Coverage Area |
|---|---|---|
| `test_connection.py` | ~80 | Connection, authentication, auto-commit, context manager |
| `test_cursor.py` | ~100 | Execute, fetch, prepare, callproc, iterator, description |
| `test_types.py` | ~50 | Type objects, constructors, date/time conversion |
| `test_exceptions.py` | ~30 | Exception hierarchy, error codes |
| `test_protocol.py` | ~80 | Packet building, parsing, CAS function codes |
| `test_packet.py` | ~50 | Binary reader/writer, data type encoding |
| `test_lob.py` | ~30 | LOB creation, read, write |
| `test_constants.py` | ~20 | Protocol constants, data type codes |
| `test_integration.py` | 41 | Live DB tests (Docker) |
| **Total** | **471 offline + 41 integration** | **99.88% coverage** |

### 4.2 CI Matrix

| | Python 3.10 | Python 3.11 | Python 3.12 | Python 3.13 |
|---|:---:|:---:|:---:|:---:|
| **Offline Tests** | ✅ | ✅ | ✅ | ✅ |
| **CUBRID 11.4** | ✅ | — | ✅ | — |
| **CUBRID 11.2** | ✅ | — | ✅ | — |

---

## 5. Known Limitations

Limitations imposed by CUBRID or design choices:

| Feature | Status | Reason |
|---|---|---|
| Async support | ❌ | CAS protocol is synchronous; would need asyncio wrapper |
| Connection pooling | ❌ | Not in scope; use SQLAlchemy's pool or external pooler |
| Thread safety level 2+ | ❌ | CUBRID CAS sessions are connection-bound |
| LOB streaming | ⚠️ | LOB data loaded fully into memory |
| Timezone-aware types | ⚠️ | CUBRID stores naive datetimes |
| CUBRID 10.x | ⚠️ | Tested on 11.x; 10.x may work but not in CI |

---

## 6. Documentation

| Document | Content |
|---|---|
| [`README.md`](../README.md) | Landing page with Quick Start |
| [`docs/CONNECTION.md`](CONNECTION.md) | Connection strings, configuration |
| [`docs/TYPES.md`](TYPES.md) | Type mapping, CUBRID-specific types |
| [`docs/API_REFERENCE.md`](API_REFERENCE.md) | Complete API documentation |
| [`docs/PROTOCOL.md`](PROTOCOL.md) | CAS wire protocol reference |
| [`docs/DEVELOPMENT.md`](DEVELOPMENT.md) | Dev setup, testing, Docker, CI/CD |
| [`docs/EXAMPLES.md`](EXAMPLES.md) | Practical usage examples |
| [`CHANGELOG.md`](../CHANGELOG.md) | Release history |
| [`CONTRIBUTING.md`](../CONTRIBUTING.md) | Contribution guidelines |

---

## 7. Roadmap

### v0.6.0 — Performance & Reliability

| Item | Description | Priority |
|---|---|---|
| Connection retry logic | Auto-reconnect on transient network failures | High |
| Batch execute optimization | Reduce round-trips for `executemany()` | Medium |
| LOB streaming | Stream large LOBs without loading fully into memory | Medium |
| Statement cache | Cache prepared statements for repeated queries | Low |

### v1.0.0 — Stable Release

| Item | Description | Priority |
|---|---|---|
| API freeze | Stabilize public API, no breaking changes | High |
| CUBRID 10.x validation | Test and document compatibility with 10.x series | Medium |
| Performance benchmarks | Benchmark vs CUBRIDdb C-extension driver | Low |
| asyncio wrapper | Optional async interface wrapping the sync protocol | Low |

---

## 8. Architecture Decisions

### 8.1 Why Pure Python (no C extensions)

The C-extension driver (`CUBRIDdb`) required users to have CUBRID C libraries installed,
which was a significant barrier — especially on macOS, ARM Linux, and containerized
environments. A pure Python implementation eliminates all build dependencies and
makes `pip install pycubrid` work everywhere.

### 8.2 Why Direct CAS Protocol

Rather than wrapping the C library or using ODBC, pycubrid implements CUBRID's CAS
(Client Application Server) binary protocol directly over TCP. This provides:

- Zero native dependencies
- Full control over connection lifecycle
- Ability to implement features the C driver lacks (e.g., proper LOB handling)

### 8.3 Why PEP 249 Strict Compliance

DB-API 2.0 is the standard Python database interface. Strict compliance means:

- Any code written for `sqlite3`, `psycopg2`, or `mysql-connector` works with minimal
  changes
- SQLAlchemy and other ORMs can use pycubrid as a drop-in driver
- Standard exception handling patterns work out of the box

---

## 9. Ecosystem Integration

pycubrid is the foundational driver in the cubrid-labs Python ecosystem:

```
pycubrid (DB-API 2.0 driver)
    └── sqlalchemy-cubrid (SQLAlchemy 2.0 dialect)
            └── cubrid-cookbook (runnable examples)
                    ├── FastAPI examples
                    ├── Django examples
                    ├── Flask examples
                    ├── Pandas examples
                    ├── Streamlit examples
                    └── Celery examples
```

---

## 10. Example-first Design Philosophy

### Why Example-first

CUBRID's ecosystem is small compared to PostgreSQL or MySQL. For a small-ecosystem
project, the entry barrier must be minimized — users should be able to copy-paste
working code within 30 seconds of reading the documentation.

> Because the ecosystem is still small, the project provides extensive examples
> and cookbook-style documentation to lower the adoption barrier.

### Hello World

```bash
pip install pycubrid
```

```python
import pycubrid

conn = pycubrid.connect(
    host="localhost",
    port=33000,
    database="demodb",
    user="dba",
    password="",
)

cur = conn.cursor()
cur.execute("SELECT 1 + 1")
print(cur.fetchone())  # (2,)

cur.close()
conn.close()
```

This code connects to a CUBRID database, runs a query, and prints the result.
No ORM setup, no configuration files, no boilerplate.

### AI Agent Discoverability

Modern developers increasingly rely on AI coding assistants — Claude Code, OpenCode,
Cursor, GitHub Copilot, Devin, and others. These agents read README files, PRDs,
documentation, and example code to decide which libraries to recommend.

**More examples in documentation = higher probability of AI recommendation.**

By embedding working code directly in the PRD, README, and docs, we ensure that
AI agents can discover, learn, and recommend pycubrid accurately.

### Cookbook Integration

The [cubrid-cookbook](https://github.com/cubrid-labs/cubrid-cookbook) repository provides
production-ready, runnable examples for pycubrid:

| Example | Description |
|---|---|
| `01_connect.py` | Basic connection and query |
| `02_crud.py` | Create, read, update, delete operations |
| `03_transactions.py` | Transaction management with commit/rollback |
| `04_prepared.py` | Prepared statements for repeated queries |
| `05_error_handling.py` | Error handling with PEP 249 exception hierarchy |
| `06_lob.py` | LOB (CLOB/BLOB) operations |

### Inspiration from Successful Projects

Projects that succeeded partly through example-heavy documentation:

| Project | What They Did |
|---|---|
| **FastAPI** | Every endpoint documented with runnable examples; became the fastest-growing Python web framework |
| **LangChain** | Cookbook-first approach drove explosive adoption in the AI space |
| **SQLAlchemy** | Extensive ORM cookbook and tutorial; de facto Python ORM for 15+ years |
| **Pandas** | "10 Minutes to pandas" and cookbook lowered entry barrier for data science |

pycubrid follows the same philosophy: **examples are not supplementary — they are the primary documentation.**

---

*Last updated: March 2026 · pycubrid v0.5.0*
