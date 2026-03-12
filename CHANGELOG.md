# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/).

## [0.1.0] - 2026-03-12

### Added
- Initial project scaffolding
- PEP 249 exception hierarchy (Warning, Error, InterfaceError, DatabaseError, DataError,
  OperationalError, IntegrityError, InternalError, ProgrammingError, NotSupportedError)
- PEP 249 type objects (STRING, BINARY, NUMBER, DATETIME, ROWID) and constructors (Date, Time,
  Timestamp, DateFromTicks, TimeFromTicks, TimestampFromTicks, Binary)
- CAS protocol constants (41 function codes, 27+ data types, isolation levels)

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
