# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/).

## [Unreleased]

## [1.3.1] - 2026-04-21

### Documentation
- **Oracle audit fixes completed** — documentation gaps from the Oracle review were closed across the main guides, with no runtime or public API changes in `pycubrid/`.
- **Driver-level timing hooks documented** — `enable_timing=True` keyword and `PYCUBRID_ENABLE_TIMING` environment variable, `Connection.timing_stats` property, and the `TimingStats` accumulator are now covered in `docs/API_REFERENCE.md` and `docs/PERFORMANCE.md` (closes #16). The implementation has shipped since 1.0.0; this completes the "API documented" acceptance criterion.
- **Async parity wording clarified** — sync vs. async capability differences are now described consistently, including async-specific wording cleanups in the Korean docs.
- **`executemany()` guidance expanded** — bulk operation documentation now explains `executemany()` behavior and usage more clearly.
- **README translations synchronized** — Korean, German, Russian, Chinese, and Hindi READMEs were refreshed to match the current English documentation.

## [1.3.0] - 2026-04-20

### Added
- **SSL/TLS support for sync connections** — `ssl=True` (verified context), `ssl=False`/`None` (disabled), or `ssl=ssl.SSLContext(...)` for custom config on `pycubrid.connect()` (#85)
- **Reconnect / network edge case test suite** — 17 tests covering connection reset, timeout, broken pipe, partial reads, reconnect-after-failure (#87)
- **Concurrency stress tests** — threaded (16 workers × 25 inserts, 32 readers) and asyncio.gather (16 workers, 32 readers) with own-Connection isolation
- **Standalone version check script** — `scripts/check_version.py` AST-based pyproject/`__init__.py` consistency check, replaces fragile inline grep in CI (#88)
- **PyPI classifiers** — `Operating System :: OS Independent`, `Typing :: Typed`, `Programming Language :: Python :: 3 :: Only` (#89)
- **Character encoding documentation** — UTF-8-only contract documented in `docs/CONNECTION.md` (#86)

### Fixed
- **PEP 639 license conflict** — removed redundant `License ::` classifier; SPDX `license = "MIT"` is the single source of truth (follow-up #89)
- **`test_ping_reconnect_also_fails` dual-stack fragility** — patches `socket.create_connection` instead of `socket.socket`

### Deferred
- **#90 Sync/async deduplication** — refactor deferred per Oracle review (high regression risk vs. maintainability gain)

### Async SSL
SSL/TLS for async connections raises `NotSupportedError` — `asyncio.loop.sock_*` APIs reject `SSLSocket`. Use the sync interface for TLS, or async without encryption. Tracked for future asyncio integration.

## [1.2.0] - 2026-04-19

### Added
- **Native `Connection.ping()`** using CHECK_CAS (FC=32) — lightweight CAS-level health check without SQL execution (#70)
- **`errno`/`sqlstate` on `DatabaseError`** — all protocol errors now populate structured error metadata with standard SQLSTATE codes (#71)
- **JSON type decoding** — opt-in `json_deserializer` parameter on `connect()`, CAS protocol bumped to v8, `CUBRIDDataType.JSON = 34` (#72)
- **Collection type decoding** — opt-in `decode_collections` parameter on `connect()`, SET → frozenset, MULTISET → list, SEQUENCE → list (#73)
- **SQLSTATE mapping table** (`error_codes.CAS_ERROR_TO_SQLSTATE`) for 19 common CUBRID error codes
- **Async cursor parity** — sync and async cursors now share identical `_escape_string` and parameter binding logic (#76, #77)
- **Timezone datetime parsing** — `DATETIMETZ`/`TIMESTAMPTZ` wire format decoding with IANA timezone keys (#78)
- **`cursor.nextset()`** for PEP 249 completeness (#79)
- **Configurable `fetch_size`** — pass `fetch_size=N` to `connect()` instead of hardcoded 100 (#81)
- **Async `read_timeout`** — `asyncio.wait_for` wrapping in `_send_and_receive` (#82)
- **Async dual-stack address fallback** — `getaddrinfo` iteration for IPv4/IPv6 in `_create_socket_nonblocking` (#83)
- **`_format_parameter()` hardening** — reject `float('nan')`/`float('inf')` with `ProgrammingError`, `DATETIMETZ` literals for tz-aware datetime (IANA key preferred, UTC offset fallback), `bytearray` support alongside `bytes` (#74)

### Security
- **Hardened parameter binding** — escape backslashes, reject null bytes, escape control characters (\r, \n, \x1a) in client-side SQL interpolation (#74)

### Fixed
- **Cursor registration dedup** — cursors no longer self-register in `__init__`; only `Connection.cursor()` registers (#76)
- **`Cursor.close()` best-effort** — narrowed exception handling to `InterfaceError`/`OperationalError`/`OSError` only (#80)
- **Sync `read_timeout`** — uses `socket.create_connection` for proper timeout enforcement
- **Sync IPv6 dual-stack** — `create_connection` handles address fallback automatically
- **Unreachable return removed** — dead `DATETIMETZ` return path in `_format_parameter()` cleaned up
- **Test isolation** — `_CursorClass` global cache no longer leaks between unit/integration tests
- **Benchmark `demodb` default** — changed to `testdb` matching Docker fixture

### Changed
- CAS protocol version bumped from 7 to 8 (enables native JSON type recognition)
- **BREAKING**: `_bind_parameters()` now only accepts `Sequence` (tuple/list) — `Mapping` (dict) parameter style removed. Use positional `?` parameters only.

## [1.1.0] - 2026-04-18

### Added
- **Native asyncio support** via `pycubrid.aio` module
  - `pycubrid.aio.connect()` — async connection factory
  - `AsyncConnection` — async context manager, commit, rollback, cursor creation
  - `AsyncCursor` — async execute, fetch (one/many/all), iterate, executemany
  - Uses `loop.sock_*` non-blocking socket I/O — reuses existing protocol/packet layers
- 30 new async offline tests (`tests/test_async.py`)

## [1.0.0] - 2026-04-11

### Compatibility Policy

This release establishes the 1.x compatibility contract: the public API follows semantic versioning,
and breaking changes will only occur in major version bumps (2.0+).

### Supported Environments

- **Python**: 3.10, 3.11, 3.12, 3.13
- **CUBRID**: 11.2, 11.4
- **Protocol**: CAS wire protocol version 8 (since CUBRID 10.2+)

### Fixed
- Resolve all mypy errors: explicit `str` return types in `get_server_version`
  and `get_last_insert_id` (`connection.py`)
- Resolve all pyright errors: initialize `response_code` in `PrepareAndExecutePacket`
  and `PreparePacket.__init__` (`protocol.py`); guard `_CursorClass` optional call (`connection.py`)

### Changed
- Development Status classifier updated from "Beta" to "Production/Stable"
- Version bumped to 1.0.0

## [0.7.0] - 2026-04-04

### Added
- `docs/SUPPORT_MATRIX.md`: Comprehensive support matrix documenting Python versions,
  CUBRID versions, PEP 249 compliance, data type mappings, driver features, and known
  limitations — defines the 1.0 support boundary
- Connection pooling section in `docs/CONNECTION.md` clarifying that pycubrid has no
  built-in pool and recommending SQLAlchemy or external pooling

### Fixed
- README documentation table: Removed incorrect "connection pool" reference from
  Connection guide description — pycubrid has no driver-level connection pool

### Changed
- Version bumped to 0.7.0 (stabilization release on path to 1.0)

## [0.6.0] - 2026-03-28

### Added
- Transparent CAS reconnection when broker signals `CAS_INFO_STATUS=INACTIVE`,
  matching the official CUBRID JDBC driver's `UClientSideConnection.checkReconnect()` behaviour
- `_check_reconnect()` method inspects `CAS_INFO[0]` before every request and
  reconnects automatically when the CAS process has been released (`KEEP_CONNECTION=AUTO`)
- `_invalidate_query_handles()` clears stale cursor query handles after
  commit/rollback to prevent `CloseQueryPacket` on dead sockets
- `CAS_INFO` is now updated from every server response so the status byte is always current

### Changed
- `_send_and_receive()` now calls `_check_reconnect()` instead of `_ensure_connected()`
  for automatic reconnection support

### Performance
- Pre-compiled `struct` objects in `packet.py` — eliminates repeated `struct.Struct()`
  instantiation on every read/write call
- Dict-based type dispatch table `_TYPE_READERS` in `protocol.py` — replaces
  long if/elif chain in `_read_value()` for O(1) type dispatch
- Slice-based `fetchall()`/`fetchmany()` in `cursor.py` — replaces per-row
  `fetchone()` loop with direct list slicing
- `executemany()` DML batch path — pre-renders all parameter sets into SQL
  strings and sends a single `BatchExecutePacket` instead of N round-trips
- `recv_into()` in `_recv_exact()` — writes directly into a pre-allocated
  buffer via `memoryview`, avoiding temporary `bytes` allocations
- `TCP_NODELAY` and `SO_KEEPALIVE` socket options on connection creation
- Module-level `_CursorClass` cache — eliminates `importlib.import_module()`
  + `getattr()` on every `Connection.cursor()` call
- SELECT 10K rows fetch: 96ms → 78ms (−19%)
- Connection establishment: 2.24ms → 1.66ms (−26%)
- INSERT execute: 7.81ms → 7.10ms (−9%)

### Fixed
- DDL statements (CREATE TABLE, ALTER TABLE) followed by DML on the same
  connection no longer fail with "connection lost during receive" (closes #23)

## [0.5.0] - 2026-03-12

### Added
- SQLAlchemy integration via `sqlalchemy-cubrid` v2.1.0 (`cubrid+pycubrid://` URL scheme)
- Updated README with SQLAlchemy usage examples

### Changed
- Version bumped to 0.5.0

## [0.4.0] - 2026-03-12

### Added
- `Lob` class for BLOB/CLOB Large Object support (create, write, read)
- `Connection.create_lob()` helper for server-side LOB creation
- `Connection.get_schema_info()` for schema introspection via CAS protocol
- `Cursor.executemany_batch()` for batch execution of multiple SQL statements
- Exported `Lob` from package `__init__.py`

## [0.3.0] - 2026-03-12

### Added
- PEP 249 `Connection` class with full CAS handshake lifecycle
  (`ClientInfoExchange` → `OpenDatabase` → `CloseDatabase`)
- TCP socket management with partial-read handling
- `commit()`, `rollback()`, `close()`, `cursor()` methods
- `autocommit` property for transaction control
- `get_server_version()` and `get_last_insert_id()` helper methods
- Context manager protocol (`with conn:` auto-close)
- PEP 249 `Cursor` class with full query execution
  (`execute`, `executemany`, `fetchone`, `fetchmany`, `fetchall`)
- Client-side parameter binding (str, int, float, None, bool, bytes,
  date, time, datetime, Decimal)
- `description` and `rowcount` attributes per PEP 249 spec
- Iterator protocol and context manager for Cursor
- `callproc()`, `setinputsizes()`, `setoutputsize()` stubs

### Fixed
- Double-parse bug in `_send_and_receive()` — now correctly passes
  response body (without data_length prefix) to packet.parse()

## [0.2.0] - 2026-03-12

### Added
- Wire protocol `PacketWriter` and `PacketReader` for CAS binary frame
  serialization/deserialization (big-endian, length-prefixed fields)
- 18 CAS protocol packet classes (`ClientInfoExchangePacket`, `OpenDatabasePacket`,
  `PreparePacket`, `ExecutePacket`, `PrepareAndExecutePacket`, `FetchPacket`,
  `CloseQueryPacket`, `CommitPacket`, `RollbackPacket`, `CloseDatabasePacket`,
  `GetEngineVersionPacket`, `BatchExecutePacket`, `GetSchemaPacket`,
  `SetDbParameterPacket`, `GetDbParameterPacket`, `GetLastInsertIdPacket`,
  `LOBNewPacket`, `LOBWritePacket`, `LOBReadPacket`)
- Response parsing helpers: `_raise_error`, `_parse_column_metadata`,
  `_parse_result_infos`, `_parse_row_data`, `_read_value`
- `ColumnMetaData` and `ResultInfo` dataclasses for structured query metadata
- Full wire-level value deserialization for all 27+ CUBRID data types

## [0.1.0] - 2026-03-12

### Added
- Initial project scaffolding
- PEP 249 exception hierarchy (Warning, Error, InterfaceError, DatabaseError, DataError,
  OperationalError, IntegrityError, InternalError, ProgrammingError, NotSupportedError)
- PEP 249 type objects (STRING, BINARY, NUMBER, DATETIME, ROWID) and constructors (Date, Time,
  Timestamp, DateFromTicks, TimeFromTicks, TimestampFromTicks, Binary)
- CAS protocol constants (41 function codes, 27+ data types, isolation levels)
