# AGENTS.md

Project knowledge base for AI coding agents.

## Project Overview

**pycubrid** is a Pure Python DB-API 2.0 (PEP 249) driver for the CUBRID relational database.
It communicates with CUBRID via the CAS wire protocol over TCP/IP, requiring no C extensions
or native CCI library.

- **Language**: Python 3.10+
- **Protocol**: CUBRID CAS binary protocol (version 7, since CUBRID 10.0.0)
- **License**: MIT
- **Version**: 0.1.0

## Architecture

```
pycubrid/                   # Main package
├── __init__.py             # Public API — PEP 249 globals, connect(), exports
├── exceptions.py           # Full PEP 249 exception hierarchy (10 classes)
├── types.py                # PEP 249 type objects + constructors
├── constants.py            # CAS protocol constants (function codes, data types, etc.)
├── packet.py               # PacketReader/PacketWriter — binary serialization (planned)
├── protocol.py             # CAS protocol packets — handshake, query, fetch (planned)
├── connection.py           # PEP 249 Connection class (planned)
├── cursor.py               # PEP 249 Cursor class (planned)
└── py.typed                # PEP 561 marker
```

### Module Responsibilities

| Module | Role |
|---|---|
| `__init__.py` | PEP 249 module globals (`apilevel`, `threadsafety`, `paramstyle`), `connect()`, re-exports |
| `exceptions.py` | `Warning`, `Error`, `InterfaceError`, `DatabaseError` + 6 subclasses |
| `types.py` | `DBAPIType` class, `STRING`/`BINARY`/`NUMBER`/`DATETIME`/`ROWID` type objects, constructors |
| `constants.py` | `CASFunctionCode`, `CUBRIDDataType`, `CUBRIDStatementType`, protocol/data-size constants |
| `packet.py` | Low-level binary read/write with big-endian byte ordering (planned) |
| `protocol.py` | High-level CAS packet classes for each function code (planned) |
| `connection.py` | `Connection` — TCP socket management, transactions, autocommit (planned) |
| `cursor.py` | `Cursor` — execute, fetch, description, iteration (planned) |

## Wire Protocol Summary

### Packet Format

```
[0:4]  DATA_LENGTH  (4 bytes, big-endian int)
[4:8]  CAS_INFO     (4 bytes)
[8:]   PAYLOAD      (variable length)
```

### Handshake Flow

1. **ClientInfoExchange**: Send `"CUBRK"` + client type + version (10 bytes, NO header)
2. **OpenDatabase**: Send db/user/password (628 bytes payload with header)
3. **PrepareAndExecute / Prepare+Execute → Fetch → CloseQuery → EndTran → CloseDatabase**

### Key Constants

- Magic string: `"CUBRK"`
- Client type: `CAS_CLIENT_JDBC = 3`
- Protocol version: `7` (since CUBRID 10.0.0)
- Byte order: Big-endian throughout

## Development

### Setup

```bash
git clone https://github.com/cubrid-labs/pycubrid.git
cd pycubrid
make install          # pip install -e ".[dev]"
```

### Key Commands

```bash
make test             # Offline tests with 95% coverage threshold
make lint             # ruff check + format
make format           # Auto-fix lint/format
make integration      # Docker → integration tests → cleanup
```

### Test Commands (manual)

```bash
# Offline (no DB needed)
pytest tests/ -v --ignore=tests/test_integration.py \
  --cov=pycubrid --cov-report=term-missing --cov-fail-under=95

# Integration (requires Docker)
docker compose up -d
export CUBRID_TEST_URL="cubrid://dba@localhost:33000/testdb"
pytest tests/test_integration.py -v
```

## Code Conventions

### Style

- **Linter/Formatter**: Ruff
- **Line length**: 100 characters
- **Target Python**: 3.10+
- **Imports**: `from __future__ import annotations` in every module
- **Type hints**: Full typing; PEP 561 compliant (`py.typed`)
- **super()**: Always `super().__init__()`, never `super(ClassName, self)`

### Anti-Patterns (Never Do)

- No type suppression (`as any`, `@ts-ignore`, etc.)
- No f-string interpolation in SQL queries
- No `super(ClassName, self)` — use `super()` only
- No Python 2 constructs
- No empty `except` blocks

## Test Structure

```
tests/
├── conftest.py              # Shared fixtures
├── test_exceptions.py       # PEP 249 exception hierarchy
├── test_types.py            # Type objects and constructors
├── test_constants.py        # Protocol constants
├── test_packet.py           # PacketReader/PacketWriter (planned)
├── test_protocol.py         # CAS protocol packets (planned)
├── test_connection.py       # Connection class (planned)
├── test_cursor.py           # Cursor class (planned)
├── test_integration.py      # Live DB tests (planned)
└── test_pep249.py           # Full PEP 249 compliance (planned)
```

## Commit Convention

```
<type>: <description>

<body>

Ultraworked with [Sisyphus](https://github.com/code-yeongyu/oh-my-opencode)
Co-authored-by: Sisyphus <clio-agent@sisyphuslabs.ai>
```

Types: `feat`, `fix`, `docs`, `chore`, `ci`, `style`, `test`, `refactor`
