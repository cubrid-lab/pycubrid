# Usage Examples

Practical examples for using pycubrid — from basic CRUD to advanced features.

---

## Table of Contents

- [Basic Connection](#basic-connection)
- [CRUD Operations](#crud-operations)
  - [Create Table](#create-table)
  - [Insert Rows](#insert-rows)
  - [Select Rows](#select-rows)
  - [Update Rows](#update-rows)
  - [Delete Rows](#delete-rows)
- [Transactions](#transactions)
  - [Manual Commit/Rollback](#manual-commitrollback)
  - [Context Manager](#context-manager)
  - [Autocommit Mode](#autocommit-mode)
- [Parameterized Queries](#parameterized-queries)
- [Batch Operations](#batch-operations)
  - [executemany](#executemany)
  - [executemany_batch](#executemany_batch)
- [Fetching Strategies](#fetching-strategies)
- [Cursor as Iterator](#cursor-as-iterator)
- [Column Metadata](#column-metadata)
- [LOB Handling](#lob-handling)
- [Schema Introspection](#schema-introspection)
- [Stored Procedures](#stored-procedures)
- [Date and Time](#date-and-time)
- [Error Handling](#error-handling)
- [SQLAlchemy Integration](#sqlalchemy-integration)
- [Connection Pooling Pattern](#connection-pooling-pattern)

---

## Basic Connection

```python
import pycubrid

# Connect to CUBRID
conn = pycubrid.connect(
    host="localhost",
    port=33000,
    database="testdb",
    user="dba",
    password="",
)

# Check server version
print(f"Server: {conn.get_server_version()}")

# Do work...
cur = conn.cursor()
cur.execute("SELECT 1 + 1")
print(cur.fetchone())  # (2,)

# Cleanup
cur.close()
conn.close()
```

---

## CRUD Operations

### Create Table

```python
import pycubrid

conn = pycubrid.connect(database="testdb")
cur = conn.cursor()

cur.execute("""
    CREATE TABLE IF NOT EXISTS cookbook_users (
        id INT AUTO_INCREMENT PRIMARY KEY,
        name VARCHAR(100) NOT NULL,
        email VARCHAR(200) UNIQUE,
        age INT DEFAULT 0,
        created_at DATETIME DEFAULT SYS_DATETIME
    )
""")
conn.commit()

cur.close()
conn.close()
```

### Insert Rows

```python
conn = pycubrid.connect(database="testdb")
cur = conn.cursor()

# Single insert
cur.execute(
    "INSERT INTO cookbook_users (name, email, age) VALUES (?, ?, ?)",
    ["Alice", "alice@example.com", 30],
)
print(f"Inserted ID: {cur.lastrowid}")

# Multiple inserts
users = [
    ["Bob", "bob@example.com", 25],
    ["Carol", "carol@example.com", 28],
    ["Dave", "dave@example.com", 35],
]
cur.executemany(
    "INSERT INTO cookbook_users (name, email, age) VALUES (?, ?, ?)",
    users,
)
print(f"Inserted {cur.rowcount} rows")

conn.commit()
cur.close()
conn.close()
```

### Select Rows

```python
conn = pycubrid.connect(database="testdb")
cur = conn.cursor()

# All rows
cur.execute("SELECT id, name, email, age FROM cookbook_users ORDER BY id")
for row in cur.fetchall():
    print(f"  {row[0]}: {row[1]} ({row[2]}) age={row[3]}")

# Filtered query
cur.execute("SELECT name, age FROM cookbook_users WHERE age > ?", [27])
print(f"\nUsers older than 27:")
for name, age in cur:
    print(f"  {name}: {age}")

cur.close()
conn.close()
```

### Update Rows

```python
conn = pycubrid.connect(database="testdb")
cur = conn.cursor()

cur.execute(
    "UPDATE cookbook_users SET age = ? WHERE name = ?",
    [31, "Alice"],
)
print(f"Updated {cur.rowcount} row(s)")

conn.commit()
cur.close()
conn.close()
```

### Delete Rows

```python
conn = pycubrid.connect(database="testdb")
cur = conn.cursor()

cur.execute("DELETE FROM cookbook_users WHERE name = ?", ["Dave"])
print(f"Deleted {cur.rowcount} row(s)")

conn.commit()
cur.close()
conn.close()
```

---

## Transactions

### Manual Commit/Rollback

```python
conn = pycubrid.connect(database="testdb")
cur = conn.cursor()

try:
    cur.execute("INSERT INTO cookbook_users (name, email) VALUES (?, ?)",
                ["Eve", "eve@example.com"])
    cur.execute("INSERT INTO cookbook_users (name, email) VALUES (?, ?)",
                ["Frank", "frank@example.com"])
    conn.commit()
    print("Transaction committed")
except pycubrid.Error as e:
    conn.rollback()
    print(f"Transaction rolled back: {e}")
finally:
    cur.close()
    conn.close()
```

### Context Manager

The connection context manager auto-commits on success and auto-rolls back on exception:

```python
with pycubrid.connect(database="testdb") as conn:
    cur = conn.cursor()
    cur.execute("INSERT INTO cookbook_users (name, email) VALUES (?, ?)",
                ["Grace", "grace@example.com"])
    # Auto-commits when exiting the `with` block without exception
    # Auto-rollbacks if an exception is raised
```

### Autocommit Mode

```python
conn = pycubrid.connect(database="testdb", autocommit=True)
cur = conn.cursor()

# Each statement commits immediately — no explicit commit needed
cur.execute("INSERT INTO cookbook_users (name) VALUES (?)", ["Heidi"])
cur.execute("INSERT INTO cookbook_users (name) VALUES (?)", ["Ivan"])

# Can also toggle dynamically
conn.autocommit = False
cur.execute("INSERT INTO cookbook_users (name) VALUES (?)", ["Judy"])
conn.commit()  # Manual commit required now

cur.close()
conn.close()
```

---

## Parameterized Queries

pycubrid uses `qmark` parameter style — `?` placeholders:

```python
cur = conn.cursor()

# Positional parameters (list or tuple)
cur.execute("SELECT * FROM cookbook_users WHERE name = ? AND age > ?", ["Alice", 25])

# Dict parameters (values used in order)
cur.execute("SELECT * FROM cookbook_users WHERE name = ? AND age > ?",
            {"name": "Alice", "age": 25})

# Supported types
import datetime
from decimal import Decimal

cur.execute("""
    INSERT INTO cookbook_products (name, price, available, launch_date)
    VALUES (?, ?, ?, ?)
""", [
    "Widget",                              # str  → 'Widget'
    Decimal("19.99"),                       # Decimal → 19.99
    True,                                  # bool → 1
    datetime.date(2025, 6, 15),            # date → DATE'2025-06-15'
])

# None maps to NULL
cur.execute("INSERT INTO cookbook_users (name, email) VALUES (?, ?)",
            ["Nobody", None])
```

---

## Batch Operations

### executemany

Execute the same SQL with different parameter sets:

```python
cur = conn.cursor()

users = [
    ("Alice", 30),
    ("Bob", 25),
    ("Carol", 28),
]
cur.executemany(
    "INSERT INTO cookbook_users (name, age) VALUES (?, ?)",
    users,
)
print(f"Inserted {cur.rowcount} rows")  # 3
conn.commit()
```

### executemany_batch

Execute **different** SQL statements in a single server round-trip:

```python
cur = conn.cursor()

results = cur.executemany_batch([
    "INSERT INTO cookbook_users (name, age) VALUES ('Xena', 40)",
    "INSERT INTO cookbook_users (name, age) VALUES ('Yuri', 22)",
    "UPDATE cookbook_users SET age = 26 WHERE name = 'Bob'",
])

for stmt_type, count in results:
    print(f"Statement type {stmt_type}: affected {count} row(s)")

conn.commit()
```

---

## Fetching Strategies

```python
cur = conn.cursor()
cur.execute("SELECT id, name FROM cookbook_users ORDER BY id")

# fetchone — one row at a time
row = cur.fetchone()
print(f"First: {row}")

# fetchmany — batch of N rows
batch = cur.fetchmany(3)
print(f"Next 3: {batch}")

# fetchall — everything remaining
rest = cur.fetchall()
print(f"Remaining: {len(rest)} rows")
```

### Array Size

Control the default batch size for `fetchmany()`:

```python
cur.arraysize = 50
cur.execute("SELECT * FROM cookbook_users")
batch = cur.fetchmany()  # Fetches up to 50 rows
```

---

## Cursor as Iterator

```python
cur = conn.cursor()
cur.execute("SELECT name, age FROM cookbook_users")

for name, age in cur:
    print(f"{name} is {age} years old")
```

---

## Column Metadata

```python
cur = conn.cursor()
cur.execute("SELECT id, name, email, age FROM cookbook_users")

print("Columns:")
for col in cur.description:
    print(f"  {col[0]:15s} type={col[1]:3d}  precision={col[4]}  nullable={col[6]}")

# Output:
#   id              type=  8  precision=10  nullable=False
#   name            type=  2  precision=100  nullable=False
#   email           type=  2  precision=200  nullable=True
#   age             type=  8  precision=10  nullable=True
```

---

## LOB Handling

### Inserting LOB Data

For most use cases, insert strings or bytes directly:

```python
cur = conn.cursor()

# CLOB — insert text directly
cur.execute("""
    CREATE TABLE IF NOT EXISTS cookbook_documents (
        id INT AUTO_INCREMENT PRIMARY KEY,
        title VARCHAR(100),
        content CLOB
    )
""")
conn.commit()

cur.execute(
    "INSERT INTO cookbook_documents (title, content) VALUES (?, ?)",
    ["Report", "This is a large text document..."],
)
conn.commit()
```

### Reading LOB Data

LOB columns return a dict with metadata:

```python
cur.execute("SELECT title, content FROM cookbook_documents WHERE id = 1")
row = cur.fetchone()

title = row[0]       # "Report"
lob_info = row[1]    # dict
print(f"LOB type: {lob_info['lob_type']}")        # 24 (CLOB)
print(f"LOB length: {lob_info['lob_length']}")     # byte length
print(f"Locator: {lob_info['file_locator']}")      # server file path
```

### Using the Lob Class

For fine-grained LOB control:

```python
from pycubrid.constants import CUBRIDDataType

# Create a LOB handle on the server
lob = conn.create_lob(CUBRIDDataType.CLOB)  # 24

# Write data
lob.write(b"Hello, CUBRID LOB!")

# Read data back
data = lob.read(length=1024, offset=0)
print(data)  # b"Hello, CUBRID LOB!"
```

---

## Schema Introspection

```python
from pycubrid.constants import CCISchemaType

# List all tables
packet = conn.get_schema_info(CCISchemaType.CLASS)
print(f"Found {packet.tuple_count} tables")

# List columns of a specific table
packet = conn.get_schema_info(CCISchemaType.ATTRIBUTE, table_name="cookbook_users")
print(f"Table has {packet.tuple_count} columns")

# Get primary key info
packet = conn.get_schema_info(CCISchemaType.PRIMARY_KEY, table_name="cookbook_users")
print(f"Primary key entries: {packet.tuple_count}")
```

---

## Stored Procedures

```python
cur = conn.cursor()

# Create a stored procedure
cur.execute("""
    CREATE OR REPLACE PROCEDURE cookbook_greet(name VARCHAR)
    AS LANGUAGE JAVA
    NAME 'com.example.Greet.greet(java.lang.String)'
""")
conn.commit()

# Call it
cur.callproc("cookbook_greet", ["World"])
```

> **Note:** CUBRID stored procedures are Java-based. Ensure the Java class is registered on the server.

---

## Date and Time

```python
import datetime
import pycubrid

conn = pycubrid.connect(database="testdb")
cur = conn.cursor()

cur.execute("""
    CREATE TABLE IF NOT EXISTS cookbook_events (
        id INT AUTO_INCREMENT PRIMARY KEY,
        event_name VARCHAR(100),
        event_date DATE,
        event_time TIME,
        event_ts DATETIME
    )
""")
conn.commit()

# Insert with Python datetime objects
cur.execute(
    "INSERT INTO cookbook_events (event_name, event_date, event_time, event_ts) VALUES (?, ?, ?, ?)",
    [
        "Launch Party",
        datetime.date(2025, 6, 15),
        datetime.time(14, 30, 0),
        datetime.datetime(2025, 6, 15, 14, 30, 0),
    ],
)
conn.commit()

# Read back — returns Python datetime objects
cur.execute("SELECT event_name, event_date, event_time, event_ts FROM cookbook_events")
row = cur.fetchone()
print(f"Event: {row[0]}")
print(f"Date:  {row[1]}")  # datetime.date(2025, 6, 15)
print(f"Time:  {row[2]}")  # datetime.time(14, 30)
print(f"TS:    {row[3]}")  # datetime.datetime(2025, 6, 15, 14, 30)

# Using PEP 249 constructors
d = pycubrid.Date(2025, 1, 1)
t = pycubrid.Time(12, 0, 0)
ts = pycubrid.Timestamp(2025, 1, 1, 12, 0, 0)

cur.close()
conn.close()
```

---

## Error Handling

### Catching Specific Errors

```python
import pycubrid

conn = pycubrid.connect(database="testdb")
cur = conn.cursor()

try:
    cur.execute("INSERT INTO cookbook_users (email) VALUES (?)", ["duplicate@example.com"])
    cur.execute("INSERT INTO cookbook_users (email) VALUES (?)", ["duplicate@example.com"])
    conn.commit()
except pycubrid.IntegrityError as e:
    print(f"Duplicate key: {e.msg}")
    conn.rollback()
except pycubrid.ProgrammingError as e:
    print(f"SQL error: {e.msg}")
    conn.rollback()
except pycubrid.OperationalError as e:
    print(f"Connection error: {e.msg}")
except pycubrid.Error as e:
    print(f"Database error: {e.msg} (code={e.code})")
    conn.rollback()
finally:
    cur.close()
    conn.close()
```

### Checking Closed State

```python
conn = pycubrid.connect(database="testdb")
conn.close()

try:
    cur = conn.cursor()
except pycubrid.InterfaceError as e:
    print(f"Expected: {e.msg}")  # "connection is closed"
```

---

## SQLAlchemy Integration

pycubrid works as a driver for [sqlalchemy-cubrid](https://github.com/cubrid-labs/sqlalchemy-cubrid):

```python
from sqlalchemy import create_engine, text, Column, Integer, String
from sqlalchemy.orm import DeclarativeBase, Session

# Connect using pycubrid driver
engine = create_engine("cubrid+pycubrid://dba@localhost:33000/testdb")

# Raw SQL
with engine.connect() as conn:
    result = conn.execute(text("SELECT 1 + 1"))
    print(result.scalar())  # 2

# ORM
class Base(DeclarativeBase):
    pass

class User(Base):
    __tablename__ = "cookbook_sa_users"
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(100))

Base.metadata.create_all(engine)

with Session(engine) as session:
    session.add(User(name="Alice"))
    session.commit()

    users = session.query(User).all()
    for u in users:
        print(f"{u.id}: {u.name}")
```

---

## Connection Pooling Pattern

pycubrid itself does not include a connection pool, but you can use a simple pattern or SQLAlchemy's built-in pool:

### Simple Pool with queue

```python
import queue
import pycubrid

class ConnectionPool:
    def __init__(self, size: int = 5, **connect_kwargs):
        self._pool: queue.Queue = queue.Queue(maxsize=size)
        self._connect_kwargs = connect_kwargs
        for _ in range(size):
            self._pool.put(pycubrid.connect(**connect_kwargs))

    def get(self) -> pycubrid.connection.Connection:
        return self._pool.get()

    def put(self, conn) -> None:
        self._pool.put(conn)

    def close_all(self) -> None:
        while not self._pool.empty():
            conn = self._pool.get_nowait()
            conn.close()

# Usage
pool = ConnectionPool(size=3, database="testdb")

conn = pool.get()
try:
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM cookbook_users")
    print(cur.fetchone())
    cur.close()
finally:
    pool.put(conn)

pool.close_all()
```

### SQLAlchemy Pool (Recommended)

```python
from sqlalchemy import create_engine

engine = create_engine(
    "cubrid+pycubrid://dba@localhost:33000/testdb",
    pool_size=5,
    max_overflow=10,
    pool_pre_ping=True,
)
```
