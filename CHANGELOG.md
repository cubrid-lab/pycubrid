# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/).

## [1.0.0] - 2026-04-11

### Stability Guarantee

This release marks the first stable version of pycubrid. The public API is frozen:
breaking changes will only occur in major version bumps (2.0+).

### Supported Environments

- **Python**: 3.10, 3.11, 3.12, 3.13
- **CUBRID**: 11.2, 11.4
- **Protocol**: CAS wire protocol version 7 (since CUBRID 10.0.0)

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
