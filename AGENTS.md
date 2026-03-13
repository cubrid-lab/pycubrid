# AGENTS.md

Project knowledge base for AI coding agents.

## Project Overview

**pycubrid** is a Pure Python DB-API 2.0 (PEP 249) driver for the CUBRID relational database.
It communicates with CUBRID via the CAS wire protocol over TCP/IP, requiring no C extensions
or native CCI library.

- **Language**: Python 3.10+
- **Protocol**: CUBRID CAS binary protocol (version 7, since CUBRID 10.0.0)
- **License**: MIT
- **Version**: 0.5.0

## Architecture

```
pycubrid/                   # Main package (9 modules)
├── __init__.py             # Public API — PEP 249 globals, connect(), exports
├── exceptions.py           # Full PEP 249 exception hierarchy (10 classes)
├── types.py                # PEP 249 type objects + constructors
├── constants.py            # CAS protocol constants (function codes, data types, etc.)
├── packet.py               # PacketReader/PacketWriter — binary serialization
├── protocol.py             # CAS protocol packets — 18 packet classes
├── connection.py           # PEP 249 Connection class
├── cursor.py               # PEP 249 Cursor class
├── lob.py                  # LOB (Large Object) support
└── py.typed                # PEP 561 marker
```

### Module Responsibilities

| Module | Role |
|---|---|
| `__init__.py` | PEP 249 module globals (`apilevel`, `threadsafety`, `paramstyle`), `connect()`, re-exports |
| `exceptions.py` | `Warning`, `Error`, `InterfaceError`, `DatabaseError` + 6 subclasses |
| `types.py` | `DBAPIType` class, `STRING`/`BINARY`/`NUMBER`/`DATETIME`/`ROWID` type objects, constructors |
| `constants.py` | `CASFunctionCode` (41 funcs), `CUBRIDDataType` (27+ types), `CUBRIDStatementType`, protocol/data-size constants |
| `packet.py` | Low-level binary read/write with big-endian byte ordering |
| `protocol.py` | High-level CAS packet classes for each function code (18 packet types) |
| `connection.py` | `Connection` — TCP socket management, transactions, autocommit, LOB creation, schema info |
| `cursor.py` | `Cursor` — execute, executemany, fetch, prepare, callproc, description, iteration |
| `lob.py` | `Lob` class — LOB type, length, file locator, packed handle |

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

### Test Stats

- **471 offline tests + 41 integration tests**, **99.88% coverage** (1654 statements, 2 missed)
- Coverage threshold: 95% (CI-enforced)

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
├── test_packet.py           # PacketReader/PacketWriter
├── test_protocol.py         # CAS protocol packets
├── test_connection.py       # Connection class
├── test_cursor.py           # Cursor class
├── test_lob.py              # LOB support
├── test_init.py             # Module-level API tests
├── test_integration.py      # Live DB tests (requires Docker)
└── test_pep249.py           # Full PEP 249 compliance
```

## Documentation

```
docs/
├── CONNECTION.md       # Connection strings, URL format, configuration
├── TYPES.md            # Full type mapping, CUBRID-specific types
├── API_REFERENCE.md    # Complete API documentation
├── PROTOCOL.md         # CAS wire protocol reference
├── DEVELOPMENT.md      # Dev setup, testing, Docker, coverage, CI/CD
├── EXAMPLES.md         # Practical usage examples with code
├── README.ko.md        # Korean translation
├── README.zh.md        # Chinese translation
├── README.hi.md        # Hindi translation
├── README.de.md        # German translation
└── README.ru.md        # Russian translation
```

## Commit Convention

```
<type>: <description>

<body>

Ultraworked with [Sisyphus](https://github.com/code-yeongyu/oh-my-opencode)
Co-authored-by: Sisyphus <clio-agent@sisyphuslabs.ai>
```

Types: `feat`, `fix`, `docs`, `chore`, `ci`, `style`, `test`, `refactor`
