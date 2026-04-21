# Architecture

## Design Objectives

- **Pure Python**: Zero system dependencies, no C extensions or CCI library required, runs anywhere Python runs.
- **Full PEP 249 Compliance**: Implements the Python Database API Specification v2.0 for maximum compatibility.
- **CAS Binary Protocol v8**: Targets the current CUBRID CAS protocol version used by pycubrid.
- **Single-connection Synchronous Model**: Reliable, blocking I/O model suitable for standard application integration.
- **PEP 561 Typed**: Fully type-hinted for modern IDE support and static analysis.

## High-Level Flow

### Phase 1: Connection Handshake

```mermaid
sequenceDiagram
    participant App
    participant pycubrid as pycubrid.connect()
    participant Broker as Broker (port 33000)
    participant CAS as CAS Process
    participant DB as CUBRID Database
    
    App->>pycubrid: connect(host, port, database, user, password)
    rect rgb(230, 245, 255)
      note over pycubrid, CAS: Phase 1 — Broker Handshake
      pycubrid->>Broker: TCP connect to port 33000
      pycubrid->>Broker: ClientInfoExchange ("CUBRK" + CLIENT_JDBC=3 + v8)
      Broker-->>pycubrid: New CAS port (4B int32)
      pycubrid->>CAS: TCP reconnect to CAS port
    end
    rect rgb(230, 255, 230)
      note over pycubrid, DB: Phase 2 — Database Session
      pycubrid->>CAS: OpenDatabase (db, user, password — 628B raw)
      CAS->>DB: Authenticate + open session
      DB-->>CAS: Session established
      CAS-->>pycubrid: CAS Info (4B) + Session ID (4B) + Broker Info (8B)
    end
    pycubrid-->>App: Connection object
```

### Phase 2: Query Lifecycle

```mermaid
sequenceDiagram
    participant App
    participant Cursor
    participant Connection
    participant CAS
    
    App->>Cursor: execute("SELECT ...", params)
    rect rgb(255, 245, 230)
      note over Cursor, CAS: SQL Execution
      Cursor->>Connection: _send_and_receive(PrepareAndExecutePacket)
      Connection->>CAS: [4B length][4B cas_info][FC=41 + SQL + params]
      CAS-->>Connection: [4B length][4B cas_info][result metadata + inline rows]
      Connection-->>Cursor: Parsed response (columns, rows)
    end
    Cursor-->>App: None (results buffered)
    
    App->>Cursor: fetchall()
    alt All rows in initial fetch
      Cursor-->>App: Buffered rows
    else More rows on server
      Cursor->>Connection: _send_and_receive(FetchPacket)
      Connection->>CAS: [4B length][4B cas_info][FC=8 + handle + offset]
      CAS-->>Connection: [4B length][4B cas_info][row data]
      Connection-->>Cursor: Additional rows
      Cursor-->>App: All rows
    end
    
    App->>Cursor: close()
    Cursor->>Connection: _send_and_receive(CloseQueryPacket)
    Connection->>CAS: [FC=6 + query_handle]
```

## CAS Reconnection

```mermaid
sequenceDiagram
    participant Connection
    participant CAS
    
    Connection->>Connection: _check_reconnect() inspects CAS_INFO[0]
    alt CAS status == INACTIVE
      Connection->>CAS: ClientInfoExchange
      CAS-->>Connection: New port
      Connection->>CAS: OpenDatabase
      CAS-->>Connection: New session
      note over Connection: Session restored transparently
    else CAS status == ACTIVE
      note over Connection: No action needed
    end
```

## Module Boundaries

```mermaid
flowchart TD
    init["__init__.py<br/>Public API & PEP 249 globals"]
    conn["connection.py<br/>TCP socket, transactions, LOB"]
    cursor["cursor.py<br/>execute, fetch, callproc"]
    protocol["protocol.py<br/>18 CAS packet classes"]
    packet["packet.py<br/>PacketReader / PacketWriter"]
    constants["constants.py<br/>CASFunctionCode, CUBRIDDataType"]
    types["types.py<br/>DBAPIType, STRING, NUMBER, ..."]
    exceptions["exceptions.py<br/>PEP 249 exception hierarchy"]
    lob["lob.py<br/>Lob class (BLOB/CLOB)"]
    
    init --> conn
    init --> types
    init --> exceptions
    init --> lob
    conn --> protocol
    conn --> packet
    conn --> cursor
    conn --> exceptions
    cursor --> protocol
    cursor --> packet
    cursor --> exceptions
    protocol --> packet
    protocol --> constants
    protocol --> exceptions
    lob --> conn
```

- **`__init__.py` — Public API & PEP 249 Globals**: The entry point of the package. It exposes the `connect` function, exception hierarchy, and DB-API type objects.
- **`connection.py` — TCP Socket & Transaction Management**: Manages the physical TCP connection to the CAS, handles transactions (commit/rollback), and acts as the owner for LOB operations.
- **`cursor.py` — SQL Execution & Result Fetching**: Implements the `Cursor` object, handling SQL preparation, execution, and the various fetch operations while maintaining state of results.
- **`protocol.py` — CAS Packet Classes**: Defines 18 specialized packet classes that map to CUBRID CAS function codes, handling the serialization and deserialization of specific requests and responses.
- **`packet.py` — PacketReader / PacketWriter**: Provides low-level utilities for reading from and writing to the wire format, handling byte order and primitive type serialization.
- **`constants.py` — CAS Constants**: Contains enumeration for CAS function codes, CUBRID data types, and other protocol-level constants.
- **`types.py` — DB-API Types**: Defines the type objects required by PEP 249 and manages the mapping between CUBRID types and Python types.
- **`exceptions.py` — PEP 249 Exceptions**: Implements the standard hierarchy of exceptions required by the DB-API 2.0 specification.
- **`lob.py` — LOB Management**: Implements the `Lob` class for handling Large Object data (BLOB/CLOB), providing an interface for reading and writing data in chunks.

## Packet Format

```text
┌─────────────────┬──────────────┬─────────────────────────┐
│  Data Length     │  CAS Info    │  Payload                │
│  (4 bytes)       │  (4 bytes)   │  (variable length)      │
│  big-endian int  │  session     │  [FC byte][arguments…]  │
└─────────────────┴──────────────┴─────────────────────────┘
```

The handshake packet (`ClientInfoExchange`) does NOT use this framing; it uses a specialized 10-byte fixed header for the initial broker negotiation.

## Type Dispatch

```mermaid
flowchart TD
    wire["Wire Data<br/>[4B size][raw bytes]"]
    dispatch{"_TYPE_READERS<br/>dict dispatch<br/>O(1) lookup"}
    
    wire --> dispatch
    
    dispatch -->|"1-4, 25"| str["str<br/>(UTF-8 decoded)"]
    dispatch -->|"5, 6"| bytes["bytes<br/>(raw binary)"]
    dispatch -->|"7"| decimal["Decimal<br/>(string-parsed)"]
    dispatch -->|"8"| int32["int<br/>(4B signed)"]
    dispatch -->|"9"| int16["int<br/>(2B signed)"]
    dispatch -->|"21"| int64["int<br/>(8B signed)"]
    dispatch -->|"11"| float32["float<br/>(IEEE 754 single)"]
    dispatch -->|"12, 10"| float64["float<br/>(IEEE 754 double)"]
    dispatch -->|"13"| date["datetime.date"]
    dispatch -->|"14"| time["datetime.time"]
    dispatch -->|"15, 22, 29-32"| datetime["datetime.datetime"]
    dispatch -->|"19"| oid["str (OID)"]
    dispatch -->|"23, 24"| lob["dict (LOB handle)"]
    dispatch -->|"16-18"| collection["bytes (opaque)"]
```

## Key Design Decisions

- **Pure Python over C extension**: Zero system dependencies ensure the driver runs anywhere Python is available, simplifying deployment and avoiding cross-compilation issues.
- **CAS protocol v8**: The driver targets the current broker protocol, including JSON-aware parsing paths and modern feature support, without carrying compatibility code for legacy protocol revisions.
- **`qmark` paramstyle**: Using question mark placeholders (`?`) directly matches the expectations of the CAS wire protocol, minimizing the need for complex client-side parameter parsing.
- **Dict-based type dispatch**: Utilizing the `_TYPE_READERS` dictionary provides O(1) lookup performance, ensuring high-speed result parsing compared to iterative conditional checks.
- **Opaque collection types**: Returning SET, MULTISET, and SEQUENCE types as raw `bytes` avoids the performance overhead and complexity of recursive parsing for features that are rarely used in standard applications.
- **Identify as JDBC client**: Sending `CLIENT_JDBC=3` during handshake ensures the CAS treats pycubrid with the same stability and feature set as the official JDBC driver.

## Public API Boundary

```python
# Module-level attributes (PEP 249)
apilevel = "2.0"
threadsafety = 1
paramstyle = "qmark"

# Constructor
connect(host, port, database, user, password, **kwargs) -> Connection

# Exceptions
Warning, Error, InterfaceError, DatabaseError, DataError,
OperationalError, IntegrityError, InternalError,
ProgrammingError, NotSupportedError

# Type Objects
STRING, BINARY, NUMBER, DATETIME, ROWID

# Constructors
Date, Time, Timestamp, DateFromTicks, TimeFromTicks, TimestampFromTicks, Binary

# Extensions
Lob, get_error_description
```

## What This Package Owns / Does Not Own

- **Owns**: CAS wire protocol implementation, PEP 249 interface, type conversion, connection lifecycle, LOB support.
- **Does not own**: Connection pooling (use SQLAlchemy), ORM (use sqlalchemy-cubrid), schema migration (use Alembic), query building (use SQLAlchemy Core).

## Related Documents

- [Protocol Reference](PROTOCOL.md)
- [Connection Guide](CONNECTION.md)
- [Type System](TYPES.md)
- [API Reference](API_REFERENCE.md)
- [Support Matrix](SUPPORT_MATRIX.md)
