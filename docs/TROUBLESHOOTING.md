# Troubleshooting Guide

Comprehensive solutions for common pycubrid issues — connection errors, query problems, type mismatches, LOB handling, performance tuning, and Docker setup.

---

## Table of Contents

- [Connection Issues](#connection-issues)
  - [ConnectionRefusedError on Port 33000](#connectionrefusederror-on-port-33000)
  - [Authentication Failed](#authentication-failed)
  - [TimeoutError or socket.timeout](#timeouterror-or-sockettimeout)
  - [Connection Closed Unexpectedly](#connection-closed-unexpectedly)
  - [Broker Port Redirect Failure](#broker-port-redirect-failure)
- [Query Issues](#query-issues)
  - [ProgrammingError: SQL Syntax](#programmingerror-sql-syntax)
  - [Parameter Binding Errors](#parameter-binding-errors)
  - [Wrong Number of Parameters](#wrong-number-of-parameters)
  - [Reserved Word Conflicts](#reserved-word-conflicts)
  - [Empty Result Set](#empty-result-set)
- [Transaction Issues](#transaction-issues)
  - [Data Not Persisted After Insert](#data-not-persisted-after-insert)
  - [Autocommit Behavior](#autocommit-behavior)
  - [Deadlocks](#deadlocks)
- [Type Mapping Issues](#type-mapping-issues)
  - [Date/Time Handling](#datetime-handling)
  - [Decimal Precision Loss](#decimal-precision-loss)
  - [NULL Handling](#null-handling)
  - [Boolean Values](#boolean-values)
  - [Unicode / NCHAR Encoding](#unicode--nchar-encoding)
- [LOB (CLOB/BLOB) Issues](#lob-clobblob-issues)
  - [LOB Columns Return a Dict, Not Data](#lob-columns-return-a-dict-not-data)
  - [Cannot Pass Lob Object as Parameter](#cannot-pass-lob-object-as-parameter)
  - [LOB Size Limits](#lob-size-limits)
- [Cursor Issues](#cursor-issues)
  - [InterfaceError: Cursor is Closed](#interfaceerror-cursor-is-closed)
  - [fetchone() Returns None Unexpectedly](#fetchone-returns-none-unexpectedly)
  - [rowcount Is -1 After SELECT](#rowcount-is--1-after-select)
  - [executemany() Performance](#executemany-performance)
- [Prepared Statement Issues](#prepared-statement-issues)
  - [prepare() Then execute() Pattern](#prepare-then-execute-pattern)
  - [Mixing Prepared and Direct Execution](#mixing-prepared-and-direct-execution)
- [Docker Issues](#docker-issues)
  - [Container Starts but Cannot Connect](#container-starts-but-cannot-connect)
  - [Database Not Found](#database-not-found)
  - [Container Health Check](#container-health-check)
- [SQLAlchemy Integration Issues](#sqlalchemy-integration-issues)
  - [Wrong Connection URL Format](#wrong-connection-url-format)
  - [Autocommit Conflicts](#autocommit-conflicts)
  - [Connection Pool Exhaustion](#connection-pool-exhaustion)
- [Performance Issues](#performance-issues)
  - [Slow Queries](#slow-queries)
  - [High Memory Usage](#high-memory-usage)
  - [Connection Overhead](#connection-overhead)
- [Debugging Techniques](#debugging-techniques)

---

## Connection Issues

### ConnectionRefusedError on Port 33000

**Symptom:**

```
ConnectionRefusedError: [Errno 111] Connection refused
```

**Causes and fixes:**

1. **CUBRID broker is not running**

   ```bash
   # Check if broker is running
   cubrid broker status

   # Start the broker
   cubrid broker start
   ```

2. **Wrong port** — The broker may be configured on a different port.

   ```bash
   # Check broker port in configuration
   cat $CUBRID/conf/cubrid_broker.conf | grep BROKER_PORT
   ```

3. **Docker container not ready** — The CUBRID container takes a few seconds to initialize.

   ```bash
   # Check container status
   docker compose ps

   # Wait for health check
   docker compose up -d
   sleep 5  # Wait for broker initialization

   # Verify with logs
   docker compose logs cubrid | tail -20
   ```

4. **Firewall or network** — Port 33000 may be blocked.

   ```bash
   # Test port connectivity
   nc -zv localhost 33000

   # On macOS/Linux
   telnet localhost 33000
   ```

---

### Authentication Failed

**Symptom:**

```
OperationalError: Authentication failed
```

**Fixes:**

CUBRID's default `dba` user has **no password**. If you set a password, make sure it matches:

```python
# Default — no password
conn = pycubrid.connect(
    host="localhost",
    port=33000,
    database="testdb",
    user="dba",
)

# With password
conn = pycubrid.connect(
    host="localhost",
    port=33000,
    database="testdb",
    user="dba",
    password="your_password",
)
```

**Common mistakes:**
- Passing `password=""` when the user has a password set
- Passing a password when the user has no password (some CUBRID versions reject this)
- Wrong username — CUBRID usernames are case-insensitive but must exist

---

### TimeoutError or socket.timeout

**Symptom:**

```
TimeoutError: [Errno 110] Connection timed out
socket.timeout: timed out
```

**Fixes:**

1. **Increase timeout** for slow networks:

   ```python
   conn = pycubrid.connect(
       host="remote-server.example.com",
       port=33000,
       database="testdb",
       user="dba",
       connect_timeout=30.0,  # 30-second timeout
   )
   ```

2. **Verify server is reachable:**

   ```bash
   ping remote-server.example.com
   nc -zv remote-server.example.com 33000
   ```

3. **Check for network firewalls** between client and server.

---

### Connection Closed Unexpectedly

**Symptom:**

```
OperationalError: Connection is closed
InterfaceError: Connection is closed
```

**Causes:**

- **Server-side session timeout** — CUBRID broker has a `SESSION_TIMEOUT` setting. Default is 300 seconds (5 minutes) of inactivity.
- **Broker restart** — If the broker restarts, all existing connections are terminated.
- **Network interruption** — Temporary network failure drops the TCP connection.
- **Idle connection cleanup** — The broker may close idle connections to free resources.

**Fix:** Create a new connection when this error occurs:

```python
import pycubrid

def get_connection():
    return pycubrid.connect(
        host="localhost",
        port=33000,
        database="testdb",
        user="dba",
    )

conn = get_connection()
try:
    cur = conn.cursor()
    cur.execute("SELECT 1")
except pycubrid.OperationalError:
    # Reconnect on connection loss
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT 1")
```

**For long-running applications**, use SQLAlchemy with connection pooling — it handles reconnection automatically:

```python
from sqlalchemy import create_engine

engine = create_engine(
    "cubrid+pycubrid://dba@localhost:33000/testdb",
    pool_pre_ping=True,  # Test connection before use
    pool_recycle=1800,    # Recycle connections every 30 minutes
)
```

---

### Broker Port Redirect Failure

**Symptom:**

```
OperationalError: ... (during connection handshake)
```

**Background:** When pycubrid connects to port 33000, the CUBRID broker may redirect the connection to a different CAS (CUBRID Application Server) port. If the redirect port is unreachable, the connection fails.

**Fix:**

1. **Check CAS processes are running:**

   ```bash
   cubrid broker status -b  # Shows broker and CAS process details
   ```

2. **Ensure all CAS ports are reachable** — If using Docker with port forwarding, only port 33000 may be exposed. When the broker redirects to a different port, the connection fails if that port is not forwarded.

   **Docker fix** — Expose a range of ports, or configure the broker to reuse the connection (port 0 mode):

   ```yaml
   # docker-compose.yml
   services:
     cubrid:
       image: cubrid/cubrid:11.2
       ports:
         - "33000:33000"
       environment:
         CUBRID_DB: testdb
   ```

   The default Docker image is configured correctly for single-port access. If you see this error with Docker, check that you're not overriding broker configuration.

---

## Query Issues

### ProgrammingError: SQL Syntax

**Symptom:**

```
ProgrammingError: Syntax error ...
```

**Common causes:**

1. **Using MySQL/PostgreSQL-specific syntax** — CUBRID has its own SQL dialect:

   ```python
   # WRONG — CUBRID doesn't support LIMIT with comma syntax
   cur.execute("SELECT * FROM users LIMIT 0, 10")

   # CORRECT — use LIMIT with OFFSET
   cur.execute("SELECT * FROM users LIMIT 10 OFFSET 0")
   ```

2. **Using reserved words as identifiers** — Quote them with double quotes:

   ```python
   # WRONG — 'value' is a reserved word
   cur.execute("SELECT value FROM config")

   # CORRECT — quote the identifier
   cur.execute('SELECT "value" FROM config')

   # BETTER — avoid reserved words
   cur.execute("SELECT val FROM config")
   ```

3. **Missing semicolons are fine** — pycubrid does not require trailing semicolons (and they may cause errors in some contexts).

---

### Parameter Binding Errors

**Symptom:**

```
ProgrammingError: Cannot convert parameter ...
```

**pycubrid uses `qmark` paramstyle** (question marks). Do not use named parameters or format strings:

```python
# CORRECT — qmark style
cur.execute("SELECT * FROM users WHERE name = ? AND age > ?", ("Alice", 25))

# WRONG — named parameters (not supported)
cur.execute("SELECT * FROM users WHERE name = :name", {"name": "Alice"})

# WRONG — format string (SQL injection risk!)
cur.execute(f"SELECT * FROM users WHERE name = '{name}'")

# WRONG — %s style (not supported)
cur.execute("SELECT * FROM users WHERE name = %s", ("Alice",))
```

**Supported Python types for parameters:**

| Python Type | SQL Result |
|---|---|
| `None` | `NULL` |
| `bool` | `1` or `0` |
| `int`, `float` | Numeric literal |
| `Decimal` | Numeric literal |
| `str` | `'escaped_string'` |
| `bytes` | `X'hex_string'` |
| `datetime.date` | `DATE'YYYY-MM-DD'` |
| `datetime.time` | `TIME'HH:MM:SS'` |
| `datetime.datetime` | `DATETIME'YYYY-MM-DD HH:MM:SS.mmm'` |

---

### Wrong Number of Parameters

**Symptom:**

```
ProgrammingError: Incorrect number of bindings supplied
```

**Fix:** Ensure the number of `?` placeholders matches the number of parameters:

```python
# WRONG — 2 placeholders, 1 parameter
cur.execute("INSERT INTO users (name, age) VALUES (?, ?)", ("Alice",))

# CORRECT — 2 placeholders, 2 parameters
cur.execute("INSERT INTO users (name, age) VALUES (?, ?)", ("Alice", 30))
```

**For single parameters**, pass a tuple (not a bare value):

```python
# WRONG — string is iterable, each character becomes a parameter
cur.execute("SELECT * FROM users WHERE name = ?", "Alice")

# CORRECT — wrap in a tuple
cur.execute("SELECT * FROM users WHERE name = ?", ("Alice",))
```

---

### Reserved Word Conflicts

**Common CUBRID reserved words** that often clash with column/table names:

| Reserved Word | Safe Alternative |
|---|---|
| `value` | `val`, `item_value` |
| `count` | `cnt`, `item_count` |
| `data` | `file_data`, `raw_data` |
| `level` | `user_level`, `access_level` |
| `name` | Usually OK, but check if issues occur |
| `status` | `item_status` |
| `type` | `item_type` |
| `action` | `user_action` |

**To use reserved words as identifiers**, quote them with double quotes:

```python
cur.execute('CREATE TABLE "order" (id INT, "value" VARCHAR(100))')
cur.execute('SELECT "value" FROM "order"')
```

---

### Empty Result Set

**Symptom:** `fetchone()` returns `None` or `fetchall()` returns `[]` when you expect data.

**Common causes:**

1. **Uncommitted INSERT** — data was inserted but not committed:

   ```python
   cur.execute("INSERT INTO users (name) VALUES (?)", ("Alice",))
   conn.commit()  # Don't forget this!
   cur.execute("SELECT * FROM users WHERE name = ?", ("Alice",))
   print(cur.fetchall())
   ```

2. **Different connection** — each connection has its own transaction view. Uncommitted data in one connection is not visible in another.

3. **Case sensitivity** — CUBRID string comparison is case-sensitive by default:

   ```python
   # These return different results
   cur.execute("SELECT * FROM users WHERE name = ?", ("alice",))
   cur.execute("SELECT * FROM users WHERE name = ?", ("Alice",))
   ```

---

## Transaction Issues

### Data Not Persisted After Insert

**Symptom:** Data is inserted successfully (no error), but a subsequent query from a different connection or after reconnection shows no data.

**Cause:** `autocommit` is `False` by default in pycubrid when using the constructor directly. You must call `conn.commit()` explicitly.

```python
conn = pycubrid.connect(host="localhost", port=33000, database="testdb", user="dba")

cur = conn.cursor()
cur.execute("INSERT INTO users (name) VALUES (?)", ("Alice",))
conn.commit()  # Required! Without this, data is lost on close
conn.close()
```

**Or use the context manager** which auto-commits on success:

```python
with pycubrid.connect(host="localhost", port=33000, database="testdb", user="dba") as conn:
    cur = conn.cursor()
    cur.execute("INSERT INTO users (name) VALUES (?)", ("Alice",))
    # Auto-commits on successful exit
```

---

### Autocommit Behavior

**Symptom:** Unexpected commit or rollback behavior.

**Key facts:**

| Scenario | autocommit | Behavior |
|---|---|---|
| Default constructor | `True` (server default) | Each statement commits immediately |
| Via SQLAlchemy | `False` (dialect sets it) | SQLAlchemy manages transactions |
| Context manager exit | N/A | Commits on success, rollbacks on exception |

**To switch modes:**

```python
# Check current mode
print(conn.autocommit)  # True

# Disable for manual transaction control
conn.autocommit = False

cur.execute("INSERT INTO users (name) VALUES (?)", ("Alice",))
cur.execute("INSERT INTO users (name) VALUES (?)", ("Bob",))
conn.commit()  # Both inserts committed together
```

---

### Deadlocks

**Symptom:**

```
OperationalError: Deadlock detected ...
```

**CUBRID uses row-level locking.** Deadlocks occur when two connections hold locks that each other needs.

**Prevention:**

1. Keep transactions short
2. Access tables in a consistent order
3. Use `SELECT ... FOR UPDATE` to lock rows upfront
4. Set appropriate isolation levels

```python
# Lock rows before updating to prevent deadlocks
cur.execute("SELECT * FROM accounts WHERE id = ? FOR UPDATE", (1,))
cur.execute("UPDATE accounts SET balance = balance - 100 WHERE id = ?", (1,))
conn.commit()
```

---

## Type Mapping Issues

### Date/Time Handling

**CUBRID type → Python type mapping:**

| CUBRID Type | Python Type | Example |
|---|---|---|
| `DATE` | `datetime.date` | `date(2025, 1, 15)` |
| `TIME` | `datetime.time` | `time(14, 30, 0)` |
| `DATETIME` | `datetime.datetime` | `datetime(2025, 1, 15, 14, 30, 0)` |
| `TIMESTAMP` | `datetime.datetime` | `datetime(2025, 1, 15, 14, 30, 0)` |

**Common issue — inserting date strings:**

```python
# CORRECT — use Python datetime objects
from datetime import date, datetime

cur.execute("INSERT INTO events (event_date) VALUES (?)", (date(2025, 1, 15),))
cur.execute("INSERT INTO events (event_time) VALUES (?)", (datetime(2025, 1, 15, 14, 30, 0),))

# ALSO CORRECT — CUBRID accepts date literal strings in SQL
cur.execute("INSERT INTO events (event_date) VALUES (DATE'2025-01-15')")
```

---

### Decimal Precision Loss

**Symptom:** Decimal values lose precision when inserted or retrieved.

**Fix:** Use `decimal.Decimal` for exact numeric values:

```python
from decimal import Decimal

# CORRECT — preserves precision
cur.execute("INSERT INTO products (price) VALUES (?)", (Decimal("19.99"),))

# RISKY — float has inherent precision issues
cur.execute("INSERT INTO products (price) VALUES (?)", (19.99,))
```

---

### NULL Handling

**Inserting NULL:**

```python
cur.execute("INSERT INTO users (name, email) VALUES (?, ?)", ("Alice", None))
```

**Checking for NULL in results:**

```python
cur.execute("SELECT email FROM users")
row = cur.fetchone()
if row[0] is None:
    print("Email is NULL")
```

---

### Boolean Values

**CUBRID has no native BOOLEAN type.** Use `SMALLINT` (0/1):

```python
# Insert boolean-like values
cur.execute("INSERT INTO settings (is_active) VALUES (?)", (True,))   # Stored as 1
cur.execute("INSERT INTO settings (is_active) VALUES (?)", (False,))  # Stored as 0

# Read boolean-like values
cur.execute("SELECT is_active FROM settings")
row = cur.fetchone()
is_active = bool(row[0])  # Convert SMALLINT back to bool
```

---

### Unicode / NCHAR Encoding

**CUBRID supports Unicode** through `NCHAR` and `NCHAR VARYING` types. pycubrid handles UTF-8 encoding transparently:

```python
# Unicode strings work directly
cur.execute("INSERT INTO users (name) VALUES (?)", ("김영선",))
cur.execute("INSERT INTO users (name) VALUES (?)", ("日本語テスト",))

cur.execute("SELECT name FROM users")
for row in cur:
    print(row[0])  # Prints correctly: 김영선, 日本語テスト
```

---

## LOB (CLOB/BLOB) Issues

### LOB Columns Return a Dict, Not Data

**Symptom:** Fetching a CLOB/BLOB column returns a dictionary instead of the actual data.

```python
cur.execute("SELECT clob_col FROM my_table")
row = cur.fetchone()
print(row[0])
# {'lob_type': 24, 'lob_length': 1234, 'file_locator': '...', 'packed_lob_handle': b'...'}
```

**This is expected behavior.** CUBRID's CAS protocol returns LOB metadata, not the LOB content inline. To read LOB content, you need to use the LOB handle separately.

**Workaround — insert and retrieve as regular strings/bytes:**

```python
# Insert string directly into CLOB column
cur.execute("INSERT INTO docs (content) VALUES (?)", ("Large text content here...",))
conn.commit()

# For small LOBs, the data may be returned inline
# For large LOBs, you get the metadata dict
```

---

### Cannot Pass Lob Object as Parameter

**Symptom:**

```
ProgrammingError: Cannot convert parameter of type Lob
```

**`Lob` objects cannot be used as query parameters.** Insert strings/bytes directly:

```python
# WRONG — Lob objects cannot be passed as parameters
lob = conn.create_lob(24)  # CLOB
lob.write(b"data")
cur.execute("INSERT INTO docs (content) VALUES (?)", (lob,))  # ERROR!

# CORRECT — pass string directly
cur.execute("INSERT INTO docs (content) VALUES (?)", ("Large text content",))

# CORRECT — pass bytes for BLOB
cur.execute("INSERT INTO docs (binary_data) VALUES (?)", (b"\x89PNG\r\n...",))
```

---

### LOB Size Limits

CUBRID LOB size limits depend on the server configuration. The default maximum is typically sufficient for most use cases, but extremely large objects may need server-side configuration adjustments.

For files larger than a few megabytes, consider:

1. Storing file paths in the database instead of file content
2. Breaking large content into chunks
3. Using CUBRID's file storage configuration options

---

## Cursor Issues

### InterfaceError: Cursor is Closed

**Symptom:**

```
InterfaceError: Cursor is closed
```

**Causes:**

1. **Explicitly closed cursor** — you called `cur.close()` then tried to use it again
2. **Connection closed** — closing a connection closes all its cursors
3. **Context manager exited** — `with conn.cursor() as cur:` closes the cursor on exit

**Fix:** Create a new cursor:

```python
cur = conn.cursor()
cur.execute("SELECT 1")
```

---

### fetchone() Returns None Unexpectedly

**Possible causes:**

1. **No rows in result set** — the query returned 0 rows
2. **Already consumed** — previous `fetchone()` or `fetchall()` consumed all rows
3. **Non-SELECT statement** — `INSERT`, `UPDATE`, `DELETE` don't produce rows

```python
cur.execute("SELECT * FROM users")
row1 = cur.fetchone()  # First row or None
row2 = cur.fetchone()  # Second row or None
# ... continues until None (no more rows)
```

**To re-read results**, execute the query again:

```python
cur.execute("SELECT * FROM users")
all_rows = cur.fetchall()  # Get all at once
# cur.fetchone() would now return None — results already consumed
```

---

### rowcount Is -1 After SELECT

**This is correct PEP 249 behavior.** `rowcount` is only meaningful for INSERT, UPDATE, DELETE statements:

```python
cur.execute("SELECT * FROM users")
print(cur.rowcount)  # -1 (undefined for SELECT)

cur.execute("UPDATE users SET name = 'Bob' WHERE id = 1")
print(cur.rowcount)  # 1 (one row affected)

cur.execute("DELETE FROM users WHERE id > 100")
print(cur.rowcount)  # Number of deleted rows
```

---

### executemany() Performance

**For bulk inserts**, `executemany()` executes each parameter set individually. For better performance with many rows, use `executemany_batch()`:

```python
# Standard executemany — one statement per parameter set
data = [("Alice", 30), ("Bob", 25), ("Charlie", 35)]
cur.executemany("INSERT INTO users (name, age) VALUES (?, ?)", data)

# executemany_batch — sends multiple statements in one request
sql_list = [
    "INSERT INTO users (name, age) VALUES ('Alice', 30)",
    "INSERT INTO users (name, age) VALUES ('Bob', 25)",
    "INSERT INTO users (name, age) VALUES ('Charlie', 35)",
]
cur.executemany_batch(sql_list)
```

**Performance comparison:**

| Method | Round Trips | Best For |
|---|---|---|
| `execute()` in loop | N | Few rows |
| `executemany()` | N | Parameterized inserts |
| `executemany_batch()` | 1 | Many distinct SQL statements |

---

## Prepared Statement Issues

### prepare() Then execute() Pattern

**Correct pattern:**

```python
# Prepare once
cur.prepare("SELECT * FROM users WHERE department = ?")

# Execute multiple times with different parameters
cur.execute(None, ("Engineering",))
engineers = cur.fetchall()

cur.execute(None, ("Marketing",))
marketers = cur.fetchall()
```

**Key points:**

- Pass `None` as the first argument to `execute()` when using a prepared statement
- The parameters go in the second argument
- The prepared statement stays active until a new `prepare()` or `execute("SQL", ...)` call

---

### Mixing Prepared and Direct Execution

**Calling `execute()` with a SQL string replaces the prepared statement:**

```python
cur.prepare("SELECT * FROM users WHERE id = ?")
cur.execute(None, (1,))  # Uses prepared statement

cur.execute("SELECT * FROM departments")  # Replaces prepared statement

cur.execute(None, (2,))  # ERROR or unexpected — no prepared statement active
```

**Best practice:** Use separate cursors for prepared statements and ad-hoc queries:

```python
# Cursor for prepared queries
prep_cur = conn.cursor()
prep_cur.prepare("SELECT * FROM users WHERE id = ?")

# Cursor for ad-hoc queries
adhoc_cur = conn.cursor()
adhoc_cur.execute("SELECT COUNT(*) FROM users")
```

---

## Docker Issues

### Container Starts but Cannot Connect

**Check 1: Container is actually running:**

```bash
docker compose ps
# Should show "running" status
```

**Check 2: Wait for initialization** — CUBRID takes a few seconds to start:

```bash
docker compose up -d
sleep 10  # Wait for full initialization

# Test connection
python3 -c "
import pycubrid
conn = pycubrid.connect(host='localhost', port=33000, database='testdb', user='dba')
print('Connected!')
print('Version:', conn.get_server_version())
conn.close()
"
```

**Check 3: Port mapping is correct:**

```bash
docker compose ps
# Verify 33000->33000/tcp is shown
```

---

### Database Not Found

**Symptom:**

```
OperationalError: ... database 'mydb' not found
```

**The Docker image creates only the database specified in `CUBRID_DB`:**

```yaml
# docker-compose.yml
services:
  cubrid:
    image: cubrid/cubrid:11.2
    environment:
      CUBRID_DB: testdb  # Only this database is created
```

**Fix:** Either:
1. Set `CUBRID_DB` to match your connection's database name
2. Create the database manually inside the container:

   ```bash
   docker compose exec cubrid cubrid createdb mydb
   docker compose exec cubrid cubrid server start mydb
   ```

---

### Container Health Check

**Add a health check to your docker-compose.yml:**

```yaml
services:
  cubrid:
    image: cubrid/cubrid:11.2
    container_name: cubrid-test
    ports:
      - "33000:33000"
    environment:
      CUBRID_DB: testdb
    healthcheck:
      test: ["CMD", "cubrid", "broker", "status"]
      interval: 10s
      timeout: 5s
      retries: 5
      start_period: 15s
```

**Wait for health check in tests:**

```bash
docker compose up -d --wait
# Proceeds only after health check passes
```

---

## SQLAlchemy Integration Issues

### Wrong Connection URL Format

**Correct URL format for pycubrid:**

```python
# pycubrid driver
engine = create_engine("cubrid+pycubrid://dba@localhost:33000/testdb")

# With password
engine = create_engine("cubrid+pycubrid://dba:password@localhost:33000/testdb")
```

**Common mistakes:**

```python
# WRONG — missing driver specification (defaults to C-extension driver)
engine = create_engine("cubrid://dba@localhost:33000/testdb")

# WRONG — wrong port format
engine = create_engine("cubrid+pycubrid://dba@localhost/testdb?port=33000")

# WRONG — wrong scheme
engine = create_engine("pycubrid://dba@localhost:33000/testdb")
```

---

### Autocommit Conflicts

**Symptom:** Data is committed even though you haven't called `session.commit()`.

**Cause:** The CUBRID server default is `autocommit=True`. SQLAlchemy's pycubrid dialect sets `autocommit=False` on each new connection, but if the dialect is misconfigured, the server default takes effect.

**Fix:** Ensure you're using `cubrid+pycubrid://` in the connection URL, which loads the correct dialect that manages autocommit properly.

---

### Connection Pool Exhaustion

**Symptom:**

```
TimeoutError: QueuePool limit of size 5 overflow 10 reached
```

**Fix:** Tune the connection pool:

```python
from sqlalchemy import create_engine

engine = create_engine(
    "cubrid+pycubrid://dba@localhost:33000/testdb",
    pool_size=10,        # Maximum persistent connections
    max_overflow=20,     # Additional connections beyond pool_size
    pool_timeout=30,     # Seconds to wait for available connection
    pool_pre_ping=True,  # Test connections before use
    pool_recycle=1800,   # Recycle connections every 30 minutes
)
```

**Ensure connections are returned to the pool:**

```python
# CORRECT — context manager returns connection
with engine.connect() as conn:
    result = conn.execute(text("SELECT 1"))

# WRONG — connection never returned
conn = engine.connect()
result = conn.execute(text("SELECT 1"))
# conn.close() is never called!
```

---

## Performance Issues

### Slow Queries

**Diagnostic steps:**

1. **Check query execution time** in your application code:

   ```python
   import time

   start = time.perf_counter()
   cur.execute("SELECT * FROM large_table WHERE status = ?", ("active",))
   rows = cur.fetchall()
   elapsed = time.perf_counter() - start
   print(f"Query took {elapsed:.3f}s, returned {len(rows)} rows")
   ```

2. **Add indexes** for frequently queried columns:

   ```sql
   CREATE INDEX idx_status ON large_table (status);
   ```

3. **Use `LIMIT`** to restrict result set size:

   ```python
   cur.execute("SELECT * FROM large_table LIMIT 100")
   ```

---

### High Memory Usage

**Symptom:** Python process consumes excessive memory with large result sets.

**Cause:** `fetchall()` loads all rows into memory at once.

**Fix:** Use `fetchone()` or `fetchmany()` for large result sets:

```python
# WRONG — loads all 1 million rows into memory
cur.execute("SELECT * FROM large_table")
rows = cur.fetchall()  # 1M rows in memory!

# CORRECT — process one row at a time
cur.execute("SELECT * FROM large_table")
for row in cur:  # Iterator protocol — fetches in batches
    process(row)

# ALSO CORRECT — fetch in chunks
cur.execute("SELECT * FROM large_table")
while True:
    batch = cur.fetchmany(1000)
    if not batch:
        break
    for row in batch:
        process(row)
```

---

### Connection Overhead

**Symptom:** Opening connections is slow.

**Cause:** Each `pycubrid.connect()` performs a TCP handshake + CAS broker handshake + database open (3+ round trips).

**Fix for applications:** Use SQLAlchemy connection pooling:

```python
from sqlalchemy import create_engine

# Connection pool reuses existing connections
engine = create_engine(
    "cubrid+pycubrid://dba@localhost:33000/testdb",
    pool_size=5,
    pool_pre_ping=True,
)
```

**Fix for scripts:** Reuse a single connection instead of opening/closing repeatedly.

---

## Debugging Techniques

### Enable Verbose Logging

**pycubrid itself does not have built-in logging**, but you can debug at the Python level:

```python
import pycubrid

conn = pycubrid.connect(host="localhost", port=33000, database="testdb", user="dba")

# Check connection state
print(f"Server version: {conn.get_server_version()}")
print(f"Autocommit: {conn.autocommit}")

# Check cursor state after query
cur = conn.cursor()
cur.execute("SELECT * FROM users")
print(f"Description: {cur.description}")
print(f"Row count: {cur.rowcount}")
```

### Inspect Server Version

```python
conn = pycubrid.connect(host="localhost", port=33000, database="testdb", user="dba")
version = conn.get_server_version()
print(f"CUBRID version: {version}")  # e.g., "11.2.0.0378"
conn.close()
```

### Test Connection Script

Save this as `test_connection.py` for quick verification:

```python
#!/usr/bin/env python3
"""Quick pycubrid connection test."""
import sys
import pycubrid

try:
    conn = pycubrid.connect(
        host="localhost",
        port=33000,
        database="testdb",
        user="dba",
    )
    print(f"✅ Connected to CUBRID {conn.get_server_version()}")

    cur = conn.cursor()
    cur.execute("SELECT 1 + 1")
    result = cur.fetchone()
    print(f"✅ Query result: {result[0]}")

    cur.execute("SELECT COUNT(*) FROM db_class")
    count = cur.fetchone()[0]
    print(f"✅ System tables: {count}")

    cur.close()
    conn.close()
    print("✅ All checks passed")

except pycubrid.OperationalError as e:
    print(f"❌ Connection failed: {e}")
    sys.exit(1)
except pycubrid.ProgrammingError as e:
    print(f"❌ Query failed: {e}")
    sys.exit(1)
```

### SQLAlchemy Debug Logging

```python
import logging

logging.basicConfig()
logging.getLogger("sqlalchemy.engine").setLevel(logging.DEBUG)

engine = create_engine("cubrid+pycubrid://dba@localhost:33000/testdb", echo=True)
```

This shows all SQL statements, parameters, and execution times.

---

*See also: [Connection Guide](CONNECTION.md) · [API Reference](API_REFERENCE.md) · [Examples](EXAMPLES.md) · [Development](DEVELOPMENT.md)*
