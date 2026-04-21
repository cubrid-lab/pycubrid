# Support Matrix

Compatibility and feature support for pycubrid releases.

> **Reference:** Current version `1.3.0`. For per-release detail see [`CHANGELOG.md`](../CHANGELOG.md).

---

## Version Compatibility

### Python

| Python Version | Status |
|---|---|
| 3.10 | ✅ Supported |
| 3.11 | ✅ Supported |
| 3.12 | ✅ Supported |
| 3.13 | ✅ Supported |
| 3.14 | ✅ Supported |
| < 3.10 | ❌ Not supported |

### CUBRID Server

| CUBRID Version | Status | Notes |
|---|---|---|
| 11.4 | ✅ Supported | Latest stable |
| 11.2 | ✅ Supported | |
| 11.0 | ✅ Supported | |
| 10.2 | ✅ Supported | Minimum tested version |
| < 10.2 | ❌ Not supported | Current driver targets CAS protocol v8 |

### CI Matrix

| Dimension | PR / push | Nightly + tag + dispatch |
|---|---|---|
| Offline tests | Python 3.10, 3.11, 3.12, 3.13, 3.14 | Same |
| Integration tests | Python {3.10, 3.14} × CUBRID {10.2, 11.0, 11.2, 11.4} = 8 jobs | Python {3.10, 3.11, 3.12, 3.13, 3.14} × CUBRID {10.2, 11.0, 11.2, 11.4} = 20 jobs |

The 5 × 4 full integration matrix is run by `.github/workflows/integration-full.yml` on a nightly schedule, on tagged releases, and on demand via `workflow_dispatch`.

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
| `Cursor.nextset()` | ✅ | Since 1.2.0 (#79) |
| Exception hierarchy | ✅ | All 10 PEP 249 exception classes |
| `errno` / `sqlstate` on `DatabaseError` | ✅ | Since 1.2.0 (#71) — 19 SQLSTATE mappings |
| Type objects | ✅ | STRING, BINARY, NUMBER, DATETIME, ROWID |
| Type constructors | ✅ | Date, Time, Timestamp, *FromTicks, Binary |
| Context managers | ✅ | Both Connection and Cursor |

### Connection Features

| Feature | Status | Since | Notes |
|---|---|---|---|
| `connect_timeout` | ✅ | 1.0.0 | Connect-phase timeout (seconds) |
| `read_timeout` (sync) | ✅ | 1.2.0 (#81) | Per-recv socket timeout |
| `read_timeout` (async) | ✅ | 1.2.0 (#82) | `asyncio.wait_for` wrapping |
| `fetch_size` | ✅ | 1.2.0 (#81) | Configurable result batch size (default 100) |
| `autocommit` property | ✅ | 1.0.0 | Get/set on `Connection` |
| `Connection.ping()` | ✅ | 1.2.0 (#70) | Native CHECK_CAS health check, no SQL needed |
| `get_server_version()` | ✅ | 1.0.0 | Returns version string (e.g. `"11.2.0.0378"`) |
| `get_last_insert_id()` | ✅ | 1.0.0 | After AUTO_INCREMENT INSERT |
| Schema introspection | ✅ | 1.0.0 | `Connection.get_schema_info()` |
| Dual-stack address fallback (sync) | ✅ | 1.0.0 | `getaddrinfo` IPv4/IPv6 iteration |
| Dual-stack address fallback (async) | ✅ | 1.2.0 (#83) | Async equivalent |
| CAS reconnection | ✅ | 1.0.0 | Auto-reconnect on broker `INACTIVE` status |

### TLS / SSL

| Feature | Status | Since | Notes |
|---|---|---|---|
| Sync TLS — `ssl=True` (verified context) | ✅ | 1.3.0 (#85) | Default secure context |
| Sync TLS — `ssl=ssl.SSLContext(...)` | ✅ | 1.3.0 (#85) | Custom context |
| Sync TLS — `ssl=False` / `None` | ✅ | 1.3.0 | Plaintext (default) |
| Async TLS | ❌ | — | `asyncio.loop.sock_*` rejects `SSLSocket`; raises `NotSupportedError`. Use sync interface for TLS or async without encryption. |

### Async (`pycubrid.aio`)

| Feature | Status | Since | Notes |
|---|---|---|---|
| `pycubrid.aio.connect()` | ✅ | 1.1.0 | Similar async surface; `AsyncConnection` does not expose sync-only `ping()` or `create_lob()` |
| `AsyncCursor` execute / fetch / executemany / callproc | ✅ | 1.1.0 | Sync-like cursor APIs with `await`; connection autocommit changes use `set_autocommit()` rather than a property setter |
| `AsyncConnection.commit()` / `rollback()` / `close()` | ✅ | 1.1.0 | |
| Async context managers | ✅ | 1.1.0 | `async with` for both connection and cursor |
| Async `read_timeout` | ✅ | 1.2.0 (#82) | |
| Async dual-stack fallback | ✅ | 1.2.0 (#83) | |
| Async parameter binding parity | ✅ | 1.2.0 (#76, #77) | Shares `_escape_string` with sync |
| Async TLS | ❌ | — | See TLS section above |

### Driver-Level Diagnostics

| Feature | Status | Since | Notes |
|---|---|---|---|
| Optional timing hooks (`enable_timing=True`) | ✅ | 1.0.0 (#54) | Off by default; zero overhead when disabled — see [PERFORMANCE.md](PERFORMANCE.md#timing--profiling-hooks) |
| `PYCUBRID_ENABLE_TIMING` env var | ✅ | 1.0.0 | Truthy: `1`, `true`, `yes` (case-insensitive) |
| `Connection.timing_stats` | ✅ | 1.0.0 | Returns `TimingStats` or `None` |
| `TimingStats` (connect / execute / fetch / close) | ✅ | 1.0.0 | Nanosecond precision, thread-safe |
| DEBUG logging (`pycubrid.connection`, `pycubrid.cursor`, `pycubrid.lob`, `pycubrid.aio.*`) | ✅ | 1.x | Driver emits opt-in debug logs for connection, cursor, LOB, and async operations |

### Data Types

| CUBRID Type | Python Type | Status | Notes |
|---|---|---|---|
| INTEGER, BIGINT, SMALLINT, SHORT | `int` | ✅ | |
| FLOAT, DOUBLE, MONETARY | `float` | ✅ | |
| NUMERIC, DECIMAL | `decimal.Decimal` | ✅ | |
| CHAR, VARCHAR, NCHAR, NVARCHAR, STRING | `str` | ✅ | |
| DATE | `datetime.date` | ✅ | |
| TIME | `datetime.time` | ✅ | |
| DATETIME, TIMESTAMP | `datetime.datetime` | ✅ | Naive (no tzinfo) |
| DATETIMETZ, TIMESTAMPTZ | `datetime.datetime` (tz-aware) | ✅ | Since 1.2.0 (#78) — IANA timezone keys |
| BIT, VARBIT | `bytes` | ✅ | |
| BLOB | `dict` | ✅ | LOB handle dict with `lob_type`, `lob_length`, `file_locator`, `packed_lob_handle`[^lob] |
| CLOB | `dict` | ✅ | LOB handle dict with `lob_type`, `lob_length`, `file_locator`, `packed_lob_handle`[^lob] |
| JSON | `Any` (via deserializer) | ✅ | Since 1.2.0 (#72) — opt-in `json_deserializer=` on `connect()`; CAS protocol v8 |
| SET | `frozenset` | ✅ | Since 1.2.0 (#73) — opt-in `decode_collections=True` on `connect()` |
| MULTISET | `list` | ✅ | Since 1.2.0 (#73) — opt-in `decode_collections=True` |
| SEQUENCE | `list` | ✅ | Since 1.2.0 (#73) — opt-in `decode_collections=True` |
| Collections (default, `decode_collections=False`) | `bytes` | ⚠️ | Raw CAS wire format for backward compatibility |
| OBJECT (OID) | `str` | ⚠️ | Decoded as `OID:@page|slot|volume`; no high-level OID API |
| NULL | `None` | ✅ | |

### Statement / Cursor

| Feature | Status | Since | Notes |
|---|---|---|---|
| `cursor.execute(sql, params)` | ✅ | 1.0.0 | Server-side `PREPARE_AND_EXECUTE` |
| `cursor.executemany(sql, seq)` | ✅ | 1.0.0 | Batches non-SELECT DML via `BatchExecutePacket`; only SELECT falls back to the per-row loop |
| `cursor.executemany_batch(sql_list, auto_commit=None)` | ✅ | 1.0.0 | Single round-trip `BatchExecutePacket` |
| `cursor.callproc(name, params)` | ✅ | 1.0.0 | Stored procedure invocation |
| `cursor.fetchone() / fetchmany() / fetchall()` | ✅ | 1.0.0 | |
| Iterator protocol (`for row in cursor`) | ✅ | 1.0.0 | |
| `cursor.description` | ✅ | 1.0.0 | PEP 249 7-tuple |
| `cursor.rowcount` | ✅ | 1.0.0 | |
| `cursor.lastrowid` | ✅ | 1.0.0 | |

### LOB

| Feature | Status | Since | Notes |
|---|---|---|---|
| `Connection.create_lob(BLOB)` | ✅ | 1.0.0 | |
| `Connection.create_lob(CLOB)` | ✅ | 1.0.0 | |
| LOB read/write | ✅ | 1.0.0 | |
| Insert `bytes`/`str` directly into BLOB/CLOB columns | ✅ | 1.0.0 | Recommended over `Lob` parameter binding (which is not supported) |

### Performance Optimisations

| Feature | Status | Since | Notes |
|---|---|---|---|
| `socket.recv_into` zero-copy receives | ✅ | 1.0.0 | |
| `TCP_NODELAY` enabled | ✅ | 1.0.0 | |
| Cursor class cache | ✅ | 1.0.0 | |
| Pre-compiled `struct` packers | ✅ | 1.0.0 | |
| Type-dispatch table for fetch | ✅ | 1.0.0 | |
| Slice-based fetch | ✅ | 1.0.0 | |
| Batch executemany (`BatchExecutePacket`) | ✅ | 1.0.0 | |

---

## Operational

| Concern | Status | Notes |
|---|---|---|
| Pure Python (no C extensions) | ✅ | `pip install pycubrid` only |
| PEP 561 typed package | ✅ | `py.typed` shipped |
| Connection pooling | ❌ | Not built-in. Use SQLAlchemy `QueuePool` or external pool — see [Connection Guide](CONNECTION.md#connection-pooling) |
| External profiling library dependency | ❌ | Only `time.perf_counter_ns` from stdlib |

---

## Test Coverage

| Metric | Value |
|---|---|
| Offline tests | 770 |
| Total tests | 811 |
| Integration jobs (PR / push) | 8 (Python {3.10, 3.14} × CUBRID 4 versions) |
| Integration jobs (nightly + tag + dispatch) | 20 (Python 5 versions × CUBRID 4 versions) |
| Stress tests | Threaded (16 workers × 25 inserts, 32 readers) and `asyncio.gather` (16 workers, 32 readers) |
| Reconnect / network edge cases | 17 tests covering reset, timeout, broken pipe, partial reads |
| Coverage threshold | 95% (CI-enforced) |

---

[^lob]: Fetching a LOB column returns a handle dictionary, not the content itself. Use
`pycubrid.lob.Lob` with `packed_lob_handle` to read bytes, or insert `str`/`bytes` directly
when writing CLOB/BLOB values.

*See also: [Connection Guide](CONNECTION.md) · [Type System](TYPES.md) · [API Reference](API_REFERENCE.md) · [Performance Guide](PERFORMANCE.md) · [Changelog](../CHANGELOG.md)*
