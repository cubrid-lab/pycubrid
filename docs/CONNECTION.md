# Connection Guide

This guide covers how to install pycubrid, connect to a CUBRID database, and understand the connection lifecycle.

---

## Table of Contents

- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Connection Function](#connection-function)
- [Connection Examples](#connection-examples)
- [Context Manager Protocol](#context-manager-protocol)
- [Autocommit Mode](#autocommit-mode)
- [Connection Methods](#connection-methods)
- [Broker Handshake](#broker-handshake)
- [Server Version Detection](#server-version-detection)
- [Troubleshooting](#troubleshooting)
- [Docker Quick Start](#docker-quick-start)
- [SQLAlchemy Integration](#sqlalchemy-integration)

---

## Prerequisites

| Requirement   | Version    |
|---------------|------------|
| Python        | 3.10+      |
| CUBRID Server | 10.2–11.4  |

No C compiler or native libraries required — pycubrid is pure Python.

---

## Installation

### From PyPI

```bash
pip install pycubrid
```

### From Source

```bash
git clone https://github.com/cubrid-labs/pycubrid.git
cd pycubrid
pip install -e ".[dev]"
```

---

## Connection Function

```python
pycubrid.connect(
    host="localhost",
    port=33000,
    database="",
    user="dba",
    password="",
    **kwargs,
) -> Connection
```

### Parameters

| Parameter   | Type  | Default       | Description                              |
|-------------|-------|---------------|------------------------------------------|
| `host`      | `str` | `"localhost"` | CUBRID server hostname or IP address     |
| `port`      | `int` | `33000`       | CUBRID broker port                       |
| `database`  | `str` | `""`          | Database name *(required)*               |
| `user`      | `str` | `"dba"`       | Database username                        |
| `password`  | `str` | `""`          | Database password                        |

### Keyword Arguments

| Kwarg             | Type    | Default | Description                           |
|-------------------|---------|---------|---------------------------------------|
| `connect_timeout` | `float` | `None`  | Socket connection timeout in seconds  |

### Return Value

Returns a `Connection` object implementing PEP 249.

---

## Connection Examples

### Basic Connection

```python
import pycubrid

conn = pycubrid.connect(
    host="localhost",
    port=33000,
    database="testdb",
    user="dba",
)

cur = conn.cursor()
cur.execute("SELECT 1 + 1")
print(cur.fetchone())  # (2,)

cur.close()
conn.close()
```

### With Password

```python
conn = pycubrid.connect(
    host="localhost",
    port=33000,
    database="demodb",
    user="dba",
    password="mypassword",
)
```

### Custom Port and Timeout

```python
conn = pycubrid.connect(
    host="db-server.internal",
    port=33100,
    database="production",
    user="app_user",
    password="secret",
    connect_timeout=10.0,  # 10-second timeout
)
```

---

## Context Manager Protocol

pycubrid connections support the `with` statement for automatic resource management:

```python
import pycubrid

with pycubrid.connect(
    host="localhost",
    port=33000,
    database="testdb",
    user="dba",
) as conn:
    cur = conn.cursor()
    cur.execute("INSERT INTO cookbook_users (name) VALUES (?)", ("Alice",))
    # Connection commits automatically on success
# Connection is closed automatically after exiting the block
```

### Behavior

| Scenario          | Action                                  |
|-------------------|-----------------------------------------|
| No exception      | `conn.commit()` then `conn.close()`     |
| Exception raised  | `conn.rollback()` then `conn.close()`   |

The `__enter__` method returns the connection itself. The `__exit__` method:

1. Commits the transaction if no exception occurred
2. Rolls back the transaction if an exception was raised
3. Always closes the connection

### Manual Transaction Control

If you need explicit control, manage transactions directly:

```python
conn = pycubrid.connect(host="localhost", port=33000, database="testdb", user="dba")
try:
    cur = conn.cursor()
    cur.execute("INSERT INTO cookbook_logs (msg) VALUES (?)", ("event",))
    conn.commit()
except Exception:
    conn.rollback()
    raise
finally:
    conn.close()
```

---

## Autocommit Mode

The `autocommit` property controls whether each statement is committed automatically.

```python
# Check current mode
print(conn.autocommit)  # True (server default)

# Disable autocommit for transaction grouping
conn.autocommit = False

# Re-enable autocommit
conn.autocommit = True
```

### Details

| Property           | Description                                              |
|--------------------|----------------------------------------------------------|
| Default value      | `True` (CUBRID server default)                           |
| Getter             | Returns current autocommit state                         |
| Setter (`= True`)  | Sends `SetDbParameterPacket` + `CommitPacket` to server  |
| Setter (`= False`) | Sends `SetDbParameterPacket` to server                   |

> **Note**: When using pycubrid with SQLAlchemy (`cubrid+pycubrid://`), the dialect sets
> `autocommit = False` on each new connection so SQLAlchemy can manage transactions properly.

---

## Connection Methods

| Method                              | Return Type   | Description                                     |
|-------------------------------------|---------------|-------------------------------------------------|
| `cursor()`                          | `Cursor`      | Create a new cursor for executing SQL            |
| `commit()`                          | `None`        | Commit the current transaction                   |
| `rollback()`                        | `None`        | Roll back the current transaction                |
| `close()`                           | `None`        | Close the connection and free resources           |
| `get_server_version()`              | `str`         | Return the CUBRID server version string          |
| `get_last_insert_id()`              | `str`         | Return the last auto-increment ID                |
| `create_lob(lob_type)`              | `Lob`         | Create a new LOB object (CLOB=24, BLOB=23)       |
| `get_schema_info(schema_type, ...)` | `list`        | Query schema metadata from the server            |

### LOB Creation

```python
# Create a CLOB (Character Large Object)
clob = conn.create_lob(24)  # 24 = CLOB
clob.write("Large text content...")

# Create a BLOB (Binary Large Object)
blob = conn.create_lob(23)  # 23 = BLOB
blob.write(b"\x89PNG\r\n...")
```

> **Tip**: For simple inserts, pass strings or bytes directly as query parameters instead of
> creating LOB objects explicitly. See [Examples](EXAMPLES.md) for details.

### Schema Information

```python
# Get schema information (schema_type constants from CUBRID docs)
tables = conn.get_schema_info(schema_type=1)  # Tables
```

---

## Broker Handshake

When `pycubrid.connect()` is called, the following protocol handshake occurs:

```
Client                              Broker (port 33000)
  │                                       │
  ├─── TCP connect ──────────────────────►│
  │                                       │
  ├─── ClientInfoExchangePacket ─────────►│
  │    (magic: "CUBRK", client_type=3)    │
  │                                       │
  │◄── New CAS port (4 bytes) ───────────┤
  │                                       │
  ├─── Reconnect to CAS port ───────────►│  (if port > 0)
  │    (or reuse connection if port = 0)  │
  │                                       │
  ├─── OpenDatabasePacket ───────────────►│
  │    (database, user, password)         │
  │                                       │
  │◄── Session ID ───────────────────────┤
  │                                       │
  │    Connection established ✓           │
```

### Step-by-Step

1. **TCP Connect** — Open a socket to the broker (default port 33000)
2. **Client Info Exchange** — Send the magic string `b"CUBRK"` with client type `CLIENT_JDBC=3` and protocol version bytes
3. **Port Redirect** — The broker responds with a 4-byte big-endian integer:
   - If `port > 0`: Disconnect from broker, reconnect to the new CAS port on the same host
   - If `port == 0`: Reuse the existing connection (direct CAS mode)
4. **Open Database** — Send database name, username, and password via `OpenDatabasePacket`
5. **Session Established** — Server returns a session ID; the connection is ready

---

## Server Version Detection

```python
conn = pycubrid.connect(host="localhost", port=33000, database="testdb", user="dba")
version = conn.get_server_version()
print(version)  # e.g., "11.2.0.0374"
conn.close()
```

The `get_server_version()` method sends a `GetDbVersionPacket` to the server and returns the version as a string.

---

## Troubleshooting

### Common Connection Errors

#### `ConnectionRefusedError` on port 33000

The CUBRID broker is not running or not listening on the expected port.

1. Verify the broker is running:
   ```bash
   cubrid broker status
   ```
2. Check the broker port in `cubrid_broker.conf` (default: 33000)
3. If using Docker:
   ```bash
   docker compose up -d
   docker compose logs cubrid
   ```

#### `Authentication failed`

CUBRID's default `dba` user has no password. If you set one, ensure it matches:

```python
# If dba has no password
conn = pycubrid.connect(host="localhost", port=33000, database="testdb", user="dba")

# If dba has a password
conn = pycubrid.connect(host="localhost", port=33000, database="testdb", user="dba", password="mypassword")
```

#### `TimeoutError` or `socket.timeout`

The server did not respond within the timeout period:

```python
# Increase timeout
conn = pycubrid.connect(
    host="slow-server.example.com",
    port=33000,
    database="testdb",
    user="dba",
    connect_timeout=30.0,
)
```

#### `OperationalError: Connection is closed`

The connection was closed by the server (session timeout, network interruption, or broker restart). Create a new connection:

```python
conn = pycubrid.connect(host="localhost", port=33000, database="testdb", user="dba")
```

---

## Docker Quick Start

For local development, use the provided `docker-compose.yml`:

```bash
# Start CUBRID 11.2 (default)
docker compose up -d

# Start a specific version
CUBRID_VERSION=11.4 docker compose up -d

# Verify it's running
docker compose ps

# Connect with pycubrid
python3 -c "
import pycubrid
with pycubrid.connect(host='localhost', port=33000, database='testdb', user='dba') as conn:
    cur = conn.cursor()
    cur.execute('SELECT 1 + 1')
    print(cur.fetchone())
"

# Stop and clean up
docker compose down -v
```

---

## SQLAlchemy Integration

pycubrid works as a driver for [sqlalchemy-cubrid](https://github.com/cubrid-labs/sqlalchemy-cubrid):

```bash
pip install "sqlalchemy-cubrid[pycubrid]"
```

```python
from sqlalchemy import create_engine, text

engine = create_engine("cubrid+pycubrid://dba@localhost:33000/testdb")

with engine.connect() as conn:
    result = conn.execute(text("SELECT 1"))
    print(result.scalar())
```

All SQLAlchemy features — ORM, Core, Alembic migrations, schema reflection — work transparently with the pycubrid driver.

---

*See also: [Type System](TYPES.md) · [API Reference](API_REFERENCE.md) · [Examples](EXAMPLES.md)*
