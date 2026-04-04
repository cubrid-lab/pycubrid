# Support Matrix

Compatibility and feature support for pycubrid releases.

---

## Version Compatibility

### Python

| Python Version | Status |
|---|---|
| 3.10 | ✅ Supported |
| 3.11 | ✅ Supported |
| 3.12 | ✅ Supported |
| 3.13 | ✅ Supported |
| < 3.10 | ❌ Not supported |

### CUBRID Server

| CUBRID Version | Status | Notes |
|---|---|---|
| 11.4 | ✅ Supported | Latest stable |
| 11.2 | ✅ Supported | |
| 11.0 | ✅ Supported | |
| 10.2 | ✅ Supported | Minimum tested version |
| < 10.2 | ❌ Not supported | CAS protocol v7 required (since 10.0) |

---

## Feature Support

### PEP 249 (DB-API 2.0)

| Feature | Status | Notes |
|---|---|---|
| `apilevel` | ✅ `"2.0"` | |
| `threadsafety` | ✅ `1` | Threads may share the module, not connections |
| `paramstyle` | ✅ `"qmark"` | `?` parameter markers |
| `connect()` | ✅ | Module-level constructor |
| `Connection` | ✅ | Full lifecycle: commit, rollback, close, autocommit |
| `Cursor` | ✅ | execute, executemany, fetch*, callproc, description, rowcount |
| Exception hierarchy | ✅ | All 10 PEP 249 exception classes |
| Type objects | ✅ | STRING, BINARY, NUMBER, DATETIME, ROWID |
| Type constructors | ✅ | Date, Time, Timestamp, *FromTicks, Binary |
| Context managers | ✅ | Both Connection and Cursor |

### Data Types

| CUBRID Type | Python Type | Status |
|---|---|---|
| INTEGER, BIGINT, SMALLINT, SHORT | `int` | ✅ |
| FLOAT, DOUBLE, MONETARY | `float` | ✅ |
| NUMERIC, DECIMAL | `decimal.Decimal` | ✅ |
| CHAR, VARCHAR, NCHAR, NVARCHAR, STRING | `str` | ✅ |
| DATE | `datetime.date` | ✅ |
| TIME | `datetime.time` | ✅ |
| DATETIME, TIMESTAMP | `datetime.datetime` | ✅ |
| BIT, VARBIT | `bytes` | ✅ |
| BLOB | `bytes` | ✅ |
| CLOB | `str` | ✅ |
| SET, MULTISET, SEQUENCE | `bytes` | ⚠️ Raw wire format (see below) |
| NULL | `None` | ✅ |

### Driver Features

| Feature | Status | Notes |
|---|---|---|
| Pure Python | ✅ | No C extensions or native libraries required |
| CAS protocol v7 | ✅ | Binary protocol over TCP/IP |
| Autocommit control | ✅ | `Connection.autocommit` property |
| LOB support | ✅ | BLOB and CLOB via `Connection.create_lob()` |
| Batch execution | ✅ | `Cursor.executemany()` with `BatchExecutePacket` |
| CAS reconnection | ✅ | Automatic reconnect on broker `INACTIVE` status |
| Schema introspection | ✅ | `Connection.get_schema_info()` |
| Server version | ✅ | `Connection.get_server_version()` |
| Connection timeout | ✅ | `connect_timeout` parameter |
| Connection pooling | ❌ | Use SQLAlchemy or external pool (see [Connection Guide](CONNECTION.md#connection-pooling)) |
| Async/await | ❌ | Not supported — CUBRID has no async wire protocol |
| JSON type | ❌ | CUBRID supports JSON since 10.2, but driver returns raw bytes |

---

## Collection Types (1.0 Contract)

CUBRID collection types (`SET`, `MULTISET`, `SEQUENCE`) are returned as **raw bytes** in their CAS wire format. The driver does not decode collection contents.

This is a deliberate design decision for the 1.0 release:
- The CAS wire format for collection payloads is not publicly documented
- Decoding would require reverse-engineering the binary format
- Applications needing structured collection data should use normalized tables or JSON-encoded strings

See [Type System → Collection Types](TYPES.md#collection-types) for details.

---

## Test Coverage

| Metric | Value |
|---|---|
| Offline tests | 471 |
| Integration tests | 41 |
| Line coverage | 99.88% |
| Coverage threshold | 95% (CI-enforced) |

---

*See also: [Connection Guide](CONNECTION.md) · [Type System](TYPES.md) · [API Reference](API_REFERENCE.md)*
