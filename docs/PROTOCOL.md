# CAS Protocol Reference

Technical documentation for the CUBRID CAS (Common Application Server) wire protocol as implemented by pycubrid.

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Packet Framing](#packet-framing)
- [Connection Handshake](#connection-handshake)
- [Request/Response Lifecycle](#requestresponse-lifecycle)
- [Function Codes](#function-codes)
- [Packet Classes](#packet-classes)
  - [ClientInfoExchangePacket](#clientinfoexchangepacket)
  - [OpenDatabasePacket](#opendatabasepacket)
  - [PrepareAndExecutePacket](#prepareandexecutepacket)
  - [PreparePacket](#preparepacket)
  - [ExecutePacket](#executepacket)
  - [FetchPacket](#fetchpacket)
  - [CommitPacket](#commitpacket)
  - [RollbackPacket](#rollbackpacket)
  - [CloseDatabasePacket](#closedatabasepacket)
  - [CloseQueryPacket](#closequerypacket)
  - [GetEngineVersionPacket](#getengineversionpacket)
  - [GetSchemaPacket](#getschemapacket)
  - [BatchExecutePacket](#batchexecutepacket)
  - [LOBNewPacket](#lobnewpacket)
  - [LOBWritePacket](#lobwritepacket)
  - [LOBReadPacket](#lobreadpacket)
  - [GetLastInsertIdPacket](#getlastinsertidpacket)
  - [GetDbParameterPacket / SetDbParameterPacket](#getdbparameterpacket--setdbparameterpacket)
- [PacketWriter](#packetwriter)
- [PacketReader](#packetreader)
- [Data Types on the Wire](#data-types-on-the-wire)
- [Error Handling](#error-handling)
- [Constants Reference](#constants-reference)

---

## Overview

CUBRID uses a proprietary binary protocol called **CAS** (Common Application Server) for client–server communication. The protocol operates over TCP sockets with big-endian byte ordering.

Key characteristics:

- **Transport**: TCP (default port 33000 for the broker)
- **Byte order**: Big-endian (network byte order)
- **Framing**: 4-byte data length + 4-byte CAS info + payload
- **Handshake**: Magic string `CUBRK` + client type + protocol version
- **Session**: Stateful — session ID maintained after `OpenDatabase`

---

## Architecture

```
┌──────────┐     TCP      ┌──────────┐     ┌──────────┐
│  Client  │ ──────────── │  Broker  │ ──> │  CAS     │
│ (pycubrid)│  port 33000 │          │     │  Process  │
└──────────┘              └──────────┘     └──────────┘
                                                │
                                           ┌────┴────┐
                                           │ CUBRID  │
                                           │ Database│
                                           └─────────┘
```

1. **Client** connects to the **Broker** on port 33000
2. **Broker** performs handshake and may redirect to a CAS process on a different port
3. **Client** opens a database session on the CAS process
4. All subsequent requests go through the CAS session

---

## Packet Framing

All packets after the initial handshake use the following frame format:

```
┌────────────────────┬────────────────────┬──────────────────────┐
│  Data Length (4B)   │  CAS Info (4B)     │  Payload (variable)  │
│  big-endian int32   │  session state     │  function-specific   │
└────────────────────┴────────────────────┴──────────────────────┘
```

| Field       | Size    | Description |
|-------------|---------|-------------|
| Data Length  | 4 bytes | Length of CAS Info + Payload (signed int32, big-endian) |
| CAS Info     | 4 bytes | Session state bytes maintained by the server |
| Payload      | variable | Function code + arguments (requests) or response data |

**Building a header:**

```python
from pycubrid.packet import build_protocol_header

header = build_protocol_header(data_length=42, cas_info=b"\x00\x00\x00\x00")
# Returns 8 bytes: 4-byte length + 4-byte cas_info
```

### Exceptions

The **handshake packet** (`ClientInfoExchangePacket`) does NOT use this framing — it sends raw bytes without a length/cas_info header.

The **open database packet** (`OpenDatabasePacket`) sends raw bytes (628 bytes) without a header, but its *response* uses standard framing.

---

## Connection Handshake

The connection flow has three phases:

### Phase 1: Client Info Exchange

```
Client → Broker (10 bytes, raw):
┌──────────┬──────────┬──────────┬──────────┐
│ "CUBRK"  │ CLIENT   │ CAS_VER  │ Padding  │
│ (5 bytes)│ _JDBC(3) │ (0x47)   │ (3 bytes)│
└──────────┴──────────┴──────────┴──────────┘

Broker → Client (4 bytes):
┌──────────────────────────┐
│ New Connection Port      │
│ (int32, big-endian)      │
└──────────────────────────┘
```

- **Magic string**: `"CUBRK"` (5 ASCII bytes)
- **Client type**: `CLIENT_JDBC = 3` (pycubrid identifies as a JDBC-compatible client)
- **CAS version**: `PROTO_INDICATOR(0x40) | VERSION(7) = 0x47`
- **New port**: If > 0, disconnect from broker and reconnect to this port. If 0, reuse current socket.

### Phase 2: Open Database

```
Client → CAS (628 bytes, raw):
┌──────────────┬──────────────┬──────────────┬──────────────┬──────────────┐
│ Database     │ User         │ Password     │ Extended     │ Reserved     │
│ (32 bytes)   │ (32 bytes)   │ (32 bytes)   │ (512 bytes)  │ (20 bytes)   │
└──────────────┴──────────────┴──────────────┴──────────────┴──────────────┘

CAS → Client (framed response):
┌──────────┬──────────┬──────────┬──────────┬──────────┐
│ CAS Info │ Response │ Broker   │ Session  │          │
│ (4B)     │ Code(4B) │ Info(8B) │ ID (4B)  │          │
└──────────┴──────────┴──────────┴──────────┴──────────┘
```

- Database, user, and password are fixed-length null-padded strings
- **Broker Info** (8 bytes) contains:
  - Byte 0: `db_type`
  - Byte 2: `statement_pooling`
  - Byte 4: `protocol_version` (lower 6 bits)
- **Session ID**: Used for subsequent CAS info tracking

### Phase 3: Ready

After successful `OpenDatabase`, the connection is ready for SQL operations.

---

## Request/Response Lifecycle

Every operation after connection follows this pattern:

```
1. Client builds payload:
   [function_code (1 byte)] [arguments...]

2. Client frames and sends:
   [data_length (4B)] [cas_info (4B)] [payload]

3. Server processes and responds:
   [data_length (4B)] [cas_info (4B)] [response_code (4B)] [result_data...]

4. Client checks response_code:
   >= 0 → Success (code may carry additional info)
   <  0 → Error (followed by error_code + error_message)
```

---

## Function Codes

All 41 CAS function codes, defined in `CASFunctionCode`:

| Code | Name                  | Description |
|------|-----------------------|-------------|
| 1    | `END_TRAN`            | Commit or rollback a transaction |
| 2    | `PREPARE`             | Prepare a SQL statement |
| 3    | `EXECUTE`             | Execute a prepared statement |
| 4    | `GET_DB_PARAMETER`    | Get a database parameter value |
| 5    | `SET_DB_PARAMETER`    | Set a database parameter value |
| 6    | `CLOSE_REQ_HANDLE`    | Close a query/request handle |
| 7    | `CURSOR`              | Position the cursor |
| 8    | `FETCH`               | Fetch result rows |
| 9    | `SCHEMA_INFO`         | Get schema information |
| 10   | `OID_GET`             | Get object by OID |
| 11   | `OID_PUT`             | Update object by OID |
| 15   | `GET_DB_VERSION`      | Get the database engine version |
| 16   | `GET_CLASS_NUM_OBJS`  | Get class object count |
| 17   | `OID_CMD`             | OID operations (drop, lock, etc.) |
| 18   | `COLLECTION`          | Collection operations |
| 19   | `NEXT_RESULT`         | Get next result set |
| 20   | `EXECUTE_BATCH`       | Batch execute multiple SQL statements |
| 21   | `EXECUTE_ARRAY`       | Execute with array binding |
| 22   | `CURSOR_UPDATE`       | Update via cursor |
| 23   | `GET_ATTR_TYPE_STR`   | Get attribute type string |
| 24   | `GET_QUERY_INFO`      | Get query plan information |
| 26   | `SAVEPOINT`           | Set or rollback to savepoint |
| 27   | `PARAMETER_INFO`      | Get parameter metadata |
| 28–30 | `XA_*`               | XA distributed transaction operations |
| 31   | `CON_CLOSE`           | Close the CAS connection |
| 32   | `CHECK_CAS`           | Ping the CAS process |
| 33   | `MAKE_OUT_RS`         | Create output result set |
| 34   | `GET_GENERATED_KEYS`  | Get auto-generated keys |
| 35   | `LOB_NEW`             | Create a new LOB handle |
| 36   | `LOB_WRITE`           | Write data to a LOB |
| 37   | `LOB_READ`            | Read data from a LOB |
| 38   | `END_SESSION`         | End the CAS session |
| 39   | `GET_ROW_COUNT`       | Get affected row count |
| 40   | `GET_LAST_INSERT_ID`  | Get last auto-increment ID |
| 41   | `PREPARE_AND_EXECUTE` | Combined prepare + execute |

---

## Packet Classes

pycubrid implements 18 packet classes in `pycubrid.protocol`. Each class provides:

- `write()` — Serialize the request (some take `cas_info` parameter)
- `parse(data)` — Deserialize the response

### ClientInfoExchangePacket

**Handshake packet** — no standard framing.

```python
packet = ClientInfoExchangePacket()
raw = packet.write()          # 10 bytes, no cas_info needed
packet.parse(response_4bytes) # Parses new_connection_port
```

| Attribute              | Type  | Description |
|------------------------|-------|-------------|
| `new_connection_port`  | `int` | Port for the CAS process (0 = reuse current) |

---

### OpenDatabasePacket

**Open a database session** — sends 628 raw bytes, receives framed response.

```python
packet = OpenDatabasePacket(database="testdb", user="dba", password="")
raw = packet.write()        # 628 bytes, no cas_info
packet.parse(response_data) # Framed response with CAS info prefix
```

| Attribute        | Type   | Description |
|------------------|--------|-------------|
| `cas_info`       | `bytes` | CAS session info (4 bytes) |
| `response_code`  | `int`   | 0 on success, negative on error |
| `broker_info`    | `dict`  | `{db_type, protocol_version, statement_pooling}` |
| `session_id`     | `int`   | Server session identifier |

---

### PrepareAndExecutePacket

**Combined prepare + execute** (FC=41) — the primary packet for SQL operations.

| Attribute            | Type   | Description |
|----------------------|--------|-------------|
| `query_handle`       | `int`  | Server-assigned query handle |
| `statement_type`     | `int`  | `CUBRIDStatementType` code |
| `bind_count`         | `int`  | Number of bind parameters |
| `column_count`       | `int`  | Number of result columns |
| `columns`            | `list[ColumnMetaData]` | Column metadata |
| `total_tuple_count`  | `int`  | Total rows in result set |
| `result_count`       | `int`  | Number of result info entries |
| `result_infos`       | `list[ResultInfo]` | Per-statement result info |
| `tuple_count`        | `int`  | Rows in the initial fetch |
| `rows`               | `list[list[Any]]` | Fetched row data |

---

### PreparePacket

**Prepare a statement** (FC=2) — separate prepare step.

| Attribute        | Type   | Description |
|------------------|--------|-------------|
| `query_handle`   | `int`  | Server-assigned query handle |
| `statement_type` | `int`  | Statement type code |
| `bind_count`     | `int`  | Number of bind parameters |
| `column_count`   | `int`  | Number of result columns |
| `columns`        | `list[ColumnMetaData]` | Column metadata |

---

### ExecutePacket

**Execute a prepared statement** (FC=3).

| Attribute            | Type   | Description |
|----------------------|--------|-------------|
| `total_tuple_count`  | `int`  | Total result rows |
| `result_count`       | `int`  | Number of result infos |
| `result_infos`       | `list[ResultInfo]` | Per-statement results |
| `tuple_count`        | `int`  | Inline fetch row count |
| `rows`               | `list[list[Any]]` | Inline fetched rows |

---

### FetchPacket

**Fetch result rows** (FC=8) — used for paginated row retrieval.

| Attribute      | Type   | Description |
|----------------|--------|-------------|
| `tuple_count`  | `int`  | Number of fetched rows |
| `rows`         | `list[list[Any]]` | Row data |

---

### CommitPacket

**Commit transaction** (FC=1 with `CCITransactionType.COMMIT`).

No result attributes — raises an exception on error.

---

### RollbackPacket

**Rollback transaction** (FC=1 with `CCITransactionType.ROLLBACK`).

No result attributes — raises an exception on error.

---

### CloseDatabasePacket

**Close the CAS session** (FC=31).

---

### CloseQueryPacket

**Release a query handle** (FC=6).

---

### GetEngineVersionPacket

**Get server version** (FC=15).

| Attribute        | Type  | Description |
|------------------|-------|-------------|
| `engine_version` | `str` | Version string (e.g., `"11.2.0.0378"`) |

---

### GetSchemaPacket

**Schema introspection** (FC=9).

| Attribute      | Type  | Description |
|----------------|-------|-------------|
| `query_handle` | `int` | Handle for fetching schema rows |
| `tuple_count`  | `int` | Number of schema entries |

---

### BatchExecutePacket

**Batch execute** (FC=20) — multiple SQL statements in one request.

| Attribute | Type  | Description |
|-----------|-------|-------------|
| `results` | `list[tuple[int, int]]` | `(stmt_type, result_count)` per statement |
| `errors`  | `list[dict]` | `{code, message}` for failed statements |

---

### LOBNewPacket

**Create LOB handle** (FC=35).

| Attribute    | Type    | Description |
|--------------|---------|-------------|
| `lob_handle` | `bytes` | Server-generated LOB handle |

---

### LOBWritePacket

**Write LOB data** (FC=36).

---

### LOBReadPacket

**Read LOB data** (FC=37).

| Attribute    | Type    | Description |
|--------------|---------|-------------|
| `bytes_read` | `int`   | Actual bytes read |
| `lob_data`   | `bytes` | The read data |

---

### GetLastInsertIdPacket

**Get last insert ID** (FC=40).

| Attribute        | Type  | Description |
|------------------|-------|-------------|
| `last_insert_id` | `str` | Last auto-increment value as string |

---

### GetDbParameterPacket / SetDbParameterPacket

**Get/Set database parameters** (FC=4 / FC=5).

| Attribute | Type  | Description |
|-----------|-------|-------------|
| `value`   | `int` | Parameter value (get result / set input) |

Available parameters (`CCIDbParam`):

| Code | Name               | Description |
|------|--------------------|-------------|
| 1    | `ISOLATION_LEVEL`  | Transaction isolation level |
| 2    | `LOCK_TIMEOUT`     | Lock wait timeout |
| 3    | `MAX_STRING_LENGTH`| Maximum string length |
| 4    | `AUTO_COMMIT`      | Auto-commit mode (0/1) |

---

## PacketWriter

`pycubrid.packet.PacketWriter` — serializes data in CAS wire format.

### Public Methods

| Method | Description |
|--------|-------------|
| `add_byte(value)` | Write length-prefixed byte (1B length + 1B value) |
| `add_short(value)` | Write length-prefixed short (4B length + 2B value) |
| `add_int(value)` | Write length-prefixed int (4B length + 4B value) |
| `add_long(value)` | Write length-prefixed long (4B length + 8B value) |
| `add_float(value)` | Write length-prefixed float (4B length + 4B value) |
| `add_double(value)` | Write length-prefixed double (4B length + 8B value) |
| `add_bytes(value)` | Write length-prefixed raw bytes |
| `add_null()` | Write null marker (zero-length) |
| `add_date(y, m, d)` | Write length-prefixed date |
| `add_time(h, m, s)` | Write length-prefixed time |
| `add_timestamp(y,m,d,h,m,s)` | Write length-prefixed timestamp |
| `add_datetime(y,m,d,h,m,s,ms)` | Write length-prefixed datetime |
| `add_cache_time()` | Write cache time (two zero ints) |
| `to_bytes()` | Return all written bytes |

### Internal Methods

| Method | Description |
|--------|-------------|
| `_write_byte(value)` | Raw byte, no length prefix |
| `_write_short(value)` | Raw short (2B) |
| `_write_int(value)` | Raw int (4B) |
| `_write_long(value)` | Raw long (8B) |
| `_write_float(value)` | Raw float (4B) |
| `_write_double(value)` | Raw double (8B) |
| `_write_bytes(value)` | Raw bytes, no prefix |
| `_write_filler(count, value)` | Fill N bytes with value |
| `_write_null_terminated_string(value)` | Length-prefixed UTF-8 string + null terminator |
| `_write_fixed_length_string(value, length)` | Fixed-width null-padded string |

---

## PacketReader

`pycubrid.packet.PacketReader` — deserializes CAS wire data.

### Primitive Parsers

| Method | Returns | Bytes Consumed |
|--------|---------|----------------|
| `_parse_byte()` | `int` | 1 |
| `_parse_short()` | `int` | 2 |
| `_parse_int()` | `int` | 4 |
| `_parse_long()` | `int` | 8 |
| `_parse_float()` | `float` | 4 |
| `_parse_double()` | `float` | 8 |
| `_parse_bytes(count)` | `bytes` | `count` |
| `_parse_null_terminated_string(length)` | `str` | `length` |

### Composite Parsers

| Method | Returns | Description |
|--------|---------|-------------|
| `_parse_date()` | `datetime.date` | 3 shorts (year, month, day) |
| `_parse_time()` | `datetime.time` | 3 shorts (hour, minute, second) |
| `_parse_datetime()` | `datetime.datetime` | 7 shorts (y,m,d,h,m,s,ms) |
| `_parse_timestamp()` | `datetime.datetime` | 6 shorts (y,m,d,h,m,s) |
| `_parse_numeric(size)` | `Decimal` | Null-terminated string → Decimal |
| `_parse_object()` | `str` | OID string (`"OID:@page|slot|volume"`) |
| `read_blob(size)` | `dict` | BLOB handle info |
| `read_clob(size)` | `dict` | CLOB handle info |
| `read_error(length)` | `(int, str)` | Error code and message |
| `bytes_remaining()` | `int` | Unread bytes in buffer |

---

## Data Types on the Wire

Column data is transmitted as a 4-byte size prefix followed by the raw data. The type determines how the data bytes are interpreted:

| Type Code | Name       | Wire Format |
|-----------|------------|-------------|
| 0         | `NULL`     | size ≤ 0 → `None` |
| 1         | `CHAR`     | Null-terminated UTF-8 string |
| 2         | `STRING`   | Null-terminated UTF-8 string |
| 3         | `NCHAR`    | Null-terminated UTF-8 string |
| 4         | `VARNCHAR` | Null-terminated UTF-8 string |
| 5         | `BIT`      | Raw bytes |
| 6         | `VARBIT`   | Raw bytes |
| 7         | `NUMERIC`  | Null-terminated string → `Decimal` |
| 8         | `INT`      | 4-byte big-endian signed int |
| 9         | `SHORT`    | 2-byte big-endian signed short |
| 10        | `MONETARY` | 8-byte big-endian double |
| 11        | `FLOAT`    | 4-byte big-endian float |
| 12        | `DOUBLE`   | 8-byte big-endian double |
| 13        | `DATE`     | 3 shorts: year, month, day |
| 14        | `TIME`     | 3 shorts: hour, minute, second |
| 15        | `TIMESTAMP`| 6 shorts: y, m, d, h, m, s |
| 16–18     | `SET/MULTISET/SEQUENCE` | Raw bytes |
| 19        | `OBJECT`   | 4B page + 2B slot + 2B volume |
| 21        | `BIGINT`   | 8-byte big-endian signed long |
| 22        | `DATETIME` | 7 shorts: y, m, d, h, m, s, ms |
| 23        | `BLOB`     | Packed LOB handle → `dict` |
| 24        | `CLOB`     | Packed LOB handle → `dict` |
| 25        | `ENUM`     | Null-terminated UTF-8 string |

### Column Metadata

Each column in a result set carries metadata parsed from the wire:

```python
@dataclass
class ColumnMetaData:
    column_type: int         # CUBRIDDataType code
    scale: int               # Decimal scale (-1 if not applicable)
    precision: int           # Precision (-1 if not applicable)
    name: str                # Column alias
    real_name: str           # Actual column name
    table_name: str          # Source table name
    is_nullable: bool        # Allows NULL
    default_value: str       # Default value expression
    is_auto_increment: bool  # AUTO_INCREMENT column
    is_unique_key: bool      # Part of unique index
    is_primary_key: bool     # Part of primary key
    is_reverse_index: bool   # Has reverse index
    is_reverse_unique: bool  # Has reverse unique index
    is_foreign_key: bool     # Part of foreign key
    is_shared: bool          # Shared attribute
```

---

## Error Handling

When `response_code < 0`, the response contains an error:

```
┌──────────────┬──────────────────────────────────┐
│ Error Code   │ Error Message                    │
│ (4B int)     │ (null-terminated string)          │
└──────────────┴──────────────────────────────────┘
```

pycubrid classifies errors automatically:

| Error pattern | Exception |
|---------------|-----------|
| `unique`, `duplicate`, `foreign key`, `constraint violation` | `IntegrityError` |
| `syntax`, `unknown class`, `does not exist`, `not found` | `ProgrammingError` |
| All other errors | `DatabaseError` |

---

## Constants Reference

### CAS Protocol Constants

```python
class CASProtocol:
    MAGIC_STRING = "CUBRK"    # Handshake magic
    CLIENT_JDBC = 3           # Client type identifier
    PROTO_INDICATOR = 0x40    # Protocol indicator bit
    VERSION = 7               # Protocol version
    CAS_VERSION = 0x47        # Combined version byte
```

### Wire Data Sizes

```python
class DataSize:
    BYTE = 1          BOOL = 1
    SHORT = 2         INT = 4
    FLOAT = 4         LONG = 8
    DOUBLE = 8        OBJECT = 8
    OID = 8           BROKER_INFO = 8
    DATE = 14         TIME = 14
    DATETIME = 14     TIMESTAMP = 14
    RESULTSET = 4     DATA_LENGTH = 4
    CAS_INFO = 4
```

### Statement Types

| Code | Name | Code | Name |
|------|------|------|------|
| 0 | `ALTER_CLASS` | 20 | `INSERT` |
| 4 | `CREATE_CLASS` | 21 | `SELECT` |
| 5 | `CREATE_INDEX` | 22 | `UPDATE` |
| 9 | `DROP_CLASS` | 23 | `DELETE` |
| 10 | `DROP_INDEX` | 24 | `CALL` |
| 16 | `ROLLBACK_WORK` | 126 | `CALL_SP` |
| 17 | `GRANT` | 127 | `UNKNOWN` |

### Transaction Types

| Code | Name |
|------|------|
| 1 | `COMMIT` |
| 2 | `ROLLBACK` |

### Isolation Levels

| Code | Name |
|------|------|
| 0x01 | `COMMIT_CLASS_UNCOMMIT_INSTANCE` (default) |
| 0x02 | `COMMIT_CLASS_COMMIT_INSTANCE` |
| 0x03 | `REP_CLASS_UNCOMMIT_INSTANCE` |
| 0x04 | `REP_CLASS_COMMIT_INSTANCE` |
| 0x05 | `REP_CLASS_REP_INSTANCE` |
| 0x06 | `SERIALIZABLE` |
