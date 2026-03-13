# API Reference

Complete API documentation for pycubrid — a pure Python DB-API 2.0 driver for CUBRID.

---

## Table of Contents

- [Module-Level Attributes](#module-level-attributes)
- [Module-Level Constructor](#module-level-constructor)
- [Connection Class](#connection-class)
  - [Constructor](#connection-constructor)
  - [Methods](#connection-methods)
  - [Properties](#connection-properties)
  - [Context Manager](#connection-context-manager)
- [Cursor Class](#cursor-class)
  - [Constructor](#cursor-constructor)
  - [Methods](#cursor-methods)
  - [Properties](#cursor-properties)
  - [Iterator Protocol](#iterator-protocol)
  - [Context Manager](#cursor-context-manager)
- [Lob Class](#lob-class)
  - [Factory Method](#lob-factory-method)
  - [Methods](#lob-methods)
  - [Properties](#lob-properties)
- [Exception Hierarchy](#exception-hierarchy)
  - [Warning](#warning)
  - [Error](#error)
  - [InterfaceError](#interfaceerror)
  - [DatabaseError](#databaseerror)
  - [DataError](#dataerror)
  - [OperationalError](#operationalerror)
  - [IntegrityError](#integrityerror)
  - [InternalError](#internalerror)
  - [ProgrammingError](#programmingerror)
  - [NotSupportedError](#notsupportederror)
- [Type Objects](#type-objects)
- [Type Constructors](#type-constructors)

---

## Module-Level Attributes

These attributes are defined at the module level as required by PEP 249.

| Attribute      | Value     | Description |
|----------------|-----------|-------------|
| `apilevel`     | `"2.0"`   | DB-API specification version |
| `threadsafety` | `1`       | Threads may share the module but not connections |
| `paramstyle`   | `"qmark"` | Question mark parameter style: `WHERE name = ?` |
| `__version__`  | `"0.5.0"` | Package version string |

```python
import pycubrid

print(pycubrid.apilevel)      # "2.0"
print(pycubrid.threadsafety)  # 1
print(pycubrid.paramstyle)    # "qmark"
print(pycubrid.__version__)   # "0.5.0"
```

---

## Module-Level Constructor

### `pycubrid.connect()`

```python
def connect(
    host: str = "localhost",
    port: int = 33000,
    database: str = "",
    user: str = "dba",
    password: str = "",
    **kwargs: Any,
) -> Connection
```

Create a new database connection.

**Parameters:**

| Parameter  | Type  | Default       | Description |
|------------|-------|---------------|-------------|
| `host`     | `str` | `"localhost"` | CUBRID server hostname or IP address |
| `port`     | `int` | `33000`       | CUBRID broker port |
| `database` | `str` | `""`          | Database name |
| `user`     | `str` | `"dba"`       | Database user |
| `password` | `str` | `""`          | Database password |
| `**kwargs` | `Any` | —             | Additional parameters (e.g., `connect_timeout`) |

**Returns:** A new `Connection` instance.

**Raises:** `OperationalError` if the connection cannot be established.

```python
import pycubrid

# Minimal connection
conn = pycubrid.connect(database="testdb")

# Full connection with timeout
conn = pycubrid.connect(
    host="192.168.1.100",
    port=33000,
    database="production",
    user="app_user",
    password="secret",
    connect_timeout=5.0,
)
```

---

## Connection Class

`pycubrid.connection.Connection`

Represents a single connection to a CUBRID database via the CAS broker protocol.

### Connection Constructor

```python
class Connection:
    def __init__(
        self,
        host: str,
        port: int,
        database: str,
        user: str,
        password: str,
        autocommit: bool = False,
        **kwargs: Any,
    ) -> None
```

> **Note:** Do not instantiate `Connection` directly. Use `pycubrid.connect()` instead.

### Connection Methods

#### `connect()`

```python
def connect(self) -> None
```

Establish a TCP CAS session with broker handshake and open the database.
Called automatically by the constructor. Calling on an already-connected instance is a no-op.

**Raises:** `OperationalError` on network failure.

---

#### `close()`

```python
def close(self) -> None
```

Close the connection and all tracked cursors. Sends a `CloseDatabasePacket` to the server. Calling on an already-closed connection is a no-op. After `close()`, any further method calls will raise `InterfaceError`.

```python
conn = pycubrid.connect(database="testdb")
# ... work ...
conn.close()  # Connection and all cursors are closed
```

---

#### `commit()`

```python
def commit(self) -> None
```

Commit the current transaction. Sends a `CommitPacket` to the server.

**Raises:** `InterfaceError` if the connection is closed.

---

#### `rollback()`

```python
def rollback(self) -> None
```

Roll back the current transaction. Sends a `RollbackPacket` to the server.

**Raises:** `InterfaceError` if the connection is closed.

---

#### `cursor()`

```python
def cursor(self) -> Cursor
```

Create and return a new `Cursor` bound to this connection. The cursor is tracked by the connection and will be closed when the connection closes.

**Returns:** A new `Cursor` instance.

**Raises:** `InterfaceError` if the connection is closed.

```python
conn = pycubrid.connect(database="testdb")
cur = conn.cursor()
cur.execute("SELECT 1 + 1")
print(cur.fetchone())  # (2,)
cur.close()
```

---

#### `get_server_version()`

```python
def get_server_version(self) -> str
```

Return the server engine version string (e.g., `"11.2.0.0378"`).

```python
conn = pycubrid.connect(database="testdb")
print(conn.get_server_version())  # "11.2.0.0378"
```

---

#### `get_last_insert_id()`

```python
def get_last_insert_id(self) -> str
```

Return the last auto-increment value generated by an INSERT statement, as a string.

```python
cur.execute("INSERT INTO users (name) VALUES ('alice')")
conn.commit()
print(conn.get_last_insert_id())  # "1"
```

---

#### `create_lob(lob_type)`

```python
def create_lob(self, lob_type: int) -> Lob
```

Create a new LOB (Large Object) on the server.

**Parameters:**

| Parameter  | Type  | Description |
|------------|-------|-------------|
| `lob_type` | `int` | LOB type code: `23` for BLOB, `24` for CLOB |

**Returns:** A new `Lob` instance.

```python
from pycubrid.constants import CUBRIDDataType

lob = conn.create_lob(CUBRIDDataType.CLOB)  # 24
lob.write(b"Hello, CUBRID!")
```

> **Important:** `Lob` objects cannot be passed as query parameters. Insert strings/bytes directly into CLOB/BLOB columns instead. See [TYPES.md](TYPES.md) for details.

---

#### `get_schema_info(schema_type, table_name, pattern_match_flag)`

```python
def get_schema_info(
    self,
    schema_type: int,
    table_name: str = "",
    pattern_match_flag: int = 1,
) -> GetSchemaPacket
```

Query schema information from the server.

**Parameters:**

| Parameter            | Type  | Default | Description |
|----------------------|-------|---------|-------------|
| `schema_type`        | `int` | —       | Schema type code (see `CCISchemaType`) |
| `table_name`         | `str` | `""`    | Table name filter |
| `pattern_match_flag` | `int` | `1`     | Pattern match flag |

**Returns:** A `GetSchemaPacket` with `query_handle` and `tuple_count` attributes.

```python
from pycubrid.constants import CCISchemaType

packet = conn.get_schema_info(CCISchemaType.CLASS)
print(f"Found {packet.tuple_count} tables")
```

**Available `CCISchemaType` values:**

| Code | Name              | Description |
|------|-------------------|-------------|
| 1    | `CLASS`           | Tables |
| 2    | `VCLASS`          | Views |
| 4    | `ATTRIBUTE`       | Columns |
| 11   | `CONSTRAINT`      | Constraints |
| 16   | `PRIMARY_KEY`     | Primary keys |
| 17   | `IMPORTED_KEYS`   | Foreign keys (imported) |
| 18   | `EXPORTED_KEYS`   | Foreign keys (exported) |

---

### Connection Properties

#### `autocommit`

```python
@property
def autocommit(self) -> bool

@autocommit.setter
def autocommit(self, value: bool) -> None
```

Get or set the auto-commit mode. When enabled, each statement is committed immediately. Setting this property sends a `SetDbParameterPacket` and `CommitPacket` to flush the transaction state on the server.

```python
conn = pycubrid.connect(database="testdb")
print(conn.autocommit)  # False

conn.autocommit = True
# Statements now auto-commit
```

---

### Connection Context Manager

`Connection` implements the context manager protocol (`__enter__` / `__exit__`).

- On successful exit: calls `commit()` then `close()`.
- On exception: calls `rollback()` then `close()`.

```python
with pycubrid.connect(database="testdb") as conn:
    cur = conn.cursor()
    cur.execute("INSERT INTO users (name) VALUES ('bob')")
    # Auto-commits on exit

# conn is closed here
```

---

## Cursor Class

`pycubrid.cursor.Cursor`

Represents a database cursor for executing SQL statements and fetching results.

### Cursor Constructor

```python
class Cursor:
    def __init__(self, connection: Connection) -> None
```

> **Note:** Do not instantiate `Cursor` directly. Use `connection.cursor()` instead.

### Cursor Methods

#### `execute(operation, parameters)`

```python
def execute(
    self,
    operation: str,
    parameters: Sequence[Any] | Mapping[str, Any] | None = None,
) -> Cursor
```

Prepare and execute a SQL statement.

**Parameters:**

| Parameter    | Type | Description |
|--------------|------|-------------|
| `operation`  | `str` | SQL statement with optional `?` placeholders |
| `parameters` | `Sequence` or `Mapping` or `None` | Parameter values to bind |

**Returns:** The cursor itself (for chaining).

**Raises:**
- `InterfaceError` if the cursor is closed
- `ProgrammingError` on SQL errors or parameter mismatch

```python
# Simple query
cur.execute("SELECT * FROM users")

# Parameterized query (qmark style)
cur.execute("SELECT * FROM users WHERE age > ?", [21])

# INSERT with parameters
cur.execute("INSERT INTO users (name, age) VALUES (?, ?)", ["alice", 30])
```

**Supported parameter types:**

| Python Type          | SQL Literal |
|----------------------|-------------|
| `None`               | `NULL` |
| `bool`               | `1` / `0` |
| `str`                | `'escaped'` |
| `bytes`              | `X'hex'` |
| `int`, `float`       | Numeric literal |
| `Decimal`            | Numeric literal |
| `datetime.date`      | `DATE'YYYY-MM-DD'` |
| `datetime.time`      | `TIME'HH:MM:SS'` |
| `datetime.datetime`  | `DATETIME'YYYY-MM-DD HH:MM:SS.mmm'` |

---

#### `executemany(operation, seq_of_parameters)`

```python
def executemany(
    self,
    operation: str,
    seq_of_parameters: Sequence[Sequence[Any] | Mapping[str, Any]],
) -> Cursor
```

Execute the same SQL statement repeatedly with different parameter sets. For non-SELECT statements, `rowcount` is set to the cumulative total of affected rows.

```python
data = [("alice", 30), ("bob", 25), ("carol", 28)]
cur.executemany("INSERT INTO users (name, age) VALUES (?, ?)", data)
print(cur.rowcount)  # 3
```

---

#### `executemany_batch(sql_list, auto_commit)`

```python
def executemany_batch(
    self,
    sql_list: list[str],
    auto_commit: bool | None = None,
) -> list[tuple[int, int]]
```

Execute multiple **distinct** SQL statements in a single batch request to the server. This is more efficient than calling `execute()` in a loop when the SQL statements differ.

**Parameters:**

| Parameter     | Type | Description |
|---------------|------|-------------|
| `sql_list`    | `list[str]` | List of complete SQL statements |
| `auto_commit` | `bool` or `None` | Override auto-commit for this batch (default: connection setting) |

**Returns:** List of `(statement_type, result_count)` tuples.

```python
results = cur.executemany_batch([
    "CREATE TABLE t1 (id INT AUTO_INCREMENT PRIMARY KEY, val VARCHAR(50))",
    "INSERT INTO t1 (val) VALUES ('hello')",
    "INSERT INTO t1 (val) VALUES ('world')",
])
# results: [(4, 0), (20, 1), (20, 1)]
# statement_type 4 = CREATE_CLASS, 20 = INSERT
```

> **Note:** `executemany_batch` is a pycubrid extension, not part of PEP 249.

---

#### `fetchone()`

```python
def fetchone(self) -> tuple[Any, ...] | None
```

Fetch the next row of a query result set. Returns `None` when no more rows are available. Automatically fetches more rows from the server when the local buffer is exhausted (100 rows per fetch).

```python
cur.execute("SELECT name, age FROM users")
row = cur.fetchone()
if row:
    name, age = row
```

---

#### `fetchmany(size)`

```python
def fetchmany(self, size: int | None = None) -> list[tuple[Any, ...]]
```

Fetch the next `size` rows. Defaults to `cursor.arraysize` if `size` is not specified.

```python
cur.execute("SELECT * FROM users")
batch = cur.fetchmany(10)  # Up to 10 rows
```

---

#### `fetchall()`

```python
def fetchall(self) -> list[tuple[Any, ...]]
```

Fetch all remaining rows of a query result. Returns an empty list if no rows remain.

```python
cur.execute("SELECT * FROM users")
all_rows = cur.fetchall()
for row in all_rows:
    print(row)
```

---

#### `callproc(procname, parameters)`

```python
def callproc(
    self,
    procname: str,
    parameters: Sequence[Any] = (),
) -> Sequence[Any]
```

Call a stored procedure. Constructs and executes a `CALL procname(?, ?, ...)` statement.

**Returns:** The original `parameters` sequence (as per PEP 249).

```python
cur.callproc("my_procedure", [1, "hello"])
```

---

#### `setinputsizes(sizes)`

```python
def setinputsizes(self, sizes: Any) -> None
```

DB-API no-op. Accepted for compatibility.

---

#### `setoutputsize(size, column)`

```python
def setoutputsize(self, size: int, column: int | None = None) -> None
```

DB-API no-op. Accepted for compatibility.

---

#### `close()`

```python
def close(self) -> None
```

Close the cursor and release the active query handle. Calling on an already-closed cursor is a no-op.

---

### Cursor Properties

#### `description`

```python
@property
def description(self) -> tuple[DescriptionItem, ...] | None
```

Return result-set metadata for the last executed statement, or `None` if no query has been executed or the last statement did not return rows.

Each item is a 7-tuple:

```python
(name, type_code, display_size, internal_size, precision, scale, null_ok)
#  str    int        None           None          int       int    bool
```

| Index | Field          | Type   | Description |
|-------|----------------|--------|-------------|
| 0     | `name`         | `str`  | Column name |
| 1     | `type_code`    | `int`  | CUBRID data type code (see `CUBRIDDataType`) |
| 2     | `display_size` | `None` | Not used |
| 3     | `internal_size`| `None` | Not used |
| 4     | `precision`    | `int`  | Column precision |
| 5     | `scale`        | `int`  | Column scale |
| 6     | `null_ok`      | `bool` | Whether the column is nullable |

```python
cur.execute("SELECT name, age FROM users")
for col in cur.description:
    print(f"{col[0]}: type={col[1]}, precision={col[4]}, nullable={col[6]}")
```

---

#### `rowcount`

```python
@property
def rowcount(self) -> int
```

Number of rows affected by the last `execute()` call. Returns `-1` for SELECT statements or if no statement has been executed.

---

#### `lastrowid`

```python
@property
def lastrowid(self) -> int | None
```

Last generated auto-increment identifier for an INSERT statement. `None` if no INSERT has been executed or the table has no auto-increment column.

```python
cur.execute("INSERT INTO users (name) VALUES ('alice')")
print(cur.lastrowid)  # e.g., 1
```

---

#### `arraysize`

```python
@property
def arraysize(self) -> int

@arraysize.setter
def arraysize(self, value: int) -> None
```

Default number of rows for `fetchmany()`. Defaults to `1`.

**Raises:** `ProgrammingError` if set to a value less than 1.

---

### Iterator Protocol

`Cursor` implements the iterator protocol, allowing you to iterate directly over result rows:

```python
cur.execute("SELECT name, age FROM users")
for name, age in cur:
    print(f"{name} is {age} years old")
```

Calls `fetchone()` internally. Raises `StopIteration` when no more rows.

---

### Cursor Context Manager

```python
with conn.cursor() as cur:
    cur.execute("SELECT 1")
    print(cur.fetchone())
# cur is automatically closed
```

---

## Lob Class

`pycubrid.lob.Lob`

Represents a CUBRID Large Object (BLOB or CLOB).

### Lob Factory Method

#### `Lob.create(connection, lob_type)`

```python
@classmethod
def create(cls, connection: Connection, lob_type: int) -> Lob
```

Create a new LOB object on the server. Prefer using `connection.create_lob()` instead.

**Parameters:**

| Parameter    | Type         | Description |
|--------------|--------------|-------------|
| `connection` | `Connection` | Active database connection |
| `lob_type`   | `int`        | `CUBRIDDataType.BLOB` (23) or `CUBRIDDataType.CLOB` (24) |

**Raises:** `ValueError` if `lob_type` is not BLOB or CLOB.

---

### Lob Methods

#### `write(data, offset)`

```python
def write(self, data: bytes, offset: int = 0) -> int
```

Write bytes to the LOB starting from `offset`.

**Returns:** Number of bytes written.

---

#### `read(length, offset)`

```python
def read(self, length: int, offset: int = 0) -> bytes
```

Read up to `length` bytes from the LOB starting from `offset`.

**Returns:** The read bytes.

---

### Lob Properties

#### `lob_handle`

```python
@property
def lob_handle(self) -> bytes
```

The raw LOB handle bytes used for server communication.

---

#### `lob_type`

```python
@property
def lob_type(self) -> int
```

The LOB type code (`23` = BLOB, `24` = CLOB).

---

### LOB Usage Notes

**LOB columns return a dict on fetch**, not a `Lob` object:

```python
cur.execute("SELECT clob_col FROM my_table")
row = cur.fetchone()
lob_info = row[0]
# {'lob_type': 24, 'lob_length': 1234, 'file_locator': '...', 'packed_lob_handle': b'...'}
```

**To insert LOB data**, pass strings/bytes directly:

```python
cur.execute("INSERT INTO my_table (clob_col) VALUES (?)", ["large text content"])
```

---

## Exception Hierarchy

pycubrid implements the full PEP 249 exception hierarchy:

```
Exception
├── Warning
└── Error
    ├── InterfaceError
    └── DatabaseError
        ├── DataError
        ├── OperationalError
        ├── IntegrityError
        ├── InternalError
        ├── ProgrammingError
        └── NotSupportedError
```

All exceptions have `msg` (str) and `code` (int) attributes. `DatabaseError` and its subclasses additionally have `errno` (int | None) and `sqlstate` (str | None).

---

### Warning

```python
class Warning(Exception):
    def __init__(self, msg: str = "", code: int = 0) -> None
```

Raised for important warnings (e.g., data truncation during insertion).

---

### Error

```python
class Error(Exception):
    def __init__(self, msg: str = "", code: int = 0) -> None
```

Base class for all pycubrid errors.

---

### InterfaceError

```python
class InterfaceError(Error)
```

Raised for errors related to the database interface — calling methods on closed connections/cursors, invalid arguments, etc.

---

### DatabaseError

```python
class DatabaseError(Error):
    def __init__(
        self,
        msg: str = "",
        code: int = 0,
        errno: int | None = None,
        sqlstate: str | None = None,
    ) -> None
```

Base class for database-side errors. Includes `errno` and `sqlstate` for server-reported error details.

---

### DataError

```python
class DataError(DatabaseError)
```

Raised for data processing problems (division by zero, numeric overflow, etc.).

---

### OperationalError

```python
class OperationalError(DatabaseError)
```

Raised for database operation errors (unexpected disconnect, memory errors, transaction failures, connection lost).

---

### IntegrityError

```python
class IntegrityError(DatabaseError)
```

Raised when relational integrity is affected (foreign key violation, duplicate key, constraint violation).

---

### InternalError

```python
class InternalError(DatabaseError)
```

Raised for internal database errors (invalid cursor state, out-of-sync transactions).

---

### ProgrammingError

```python
class ProgrammingError(DatabaseError)
```

Raised for programming errors (SQL syntax errors, table not found, wrong number of parameters, unsupported parameter types).

---

### NotSupportedError

```python
class NotSupportedError(DatabaseError)
```

Raised when an unsupported method or API is called.

---

### Error Classification

pycubrid automatically classifies server errors based on the error message:

| Keywords in error message | Exception raised |
|---------------------------|-----------------|
| `unique`, `duplicate`, `foreign key`, `constraint violation` | `IntegrityError` |
| `syntax`, `unknown class`, `does not exist`, `not found` | `ProgrammingError` |
| All others | `DatabaseError` |

---

## Type Objects

PEP 249 type objects for comparing `cursor.description` type codes:

| Type Object | Description | Matching CUBRID types |
|-------------|-------------|----------------------|
| `STRING`    | String types | `CHAR`, `STRING`, `NCHAR`, `VARNCHAR` |
| `BINARY`    | Binary types | `BIT`, `VARBIT`, `BLOB` |
| `NUMBER`    | Numeric types | `SHORT`, `INT`, `BIGINT`, `FLOAT`, `DOUBLE`, `NUMERIC`, `MONETARY` |
| `DATETIME`  | Date/time types | `DATE`, `TIME`, `DATETIME`, `TIMESTAMP` |
| `ROWID`     | Row ID types | `OBJECT` |

```python
import pycubrid

cur.execute("SELECT name FROM users")
type_code = cur.description[0][1]

if type_code == pycubrid.STRING:
    print("It's a string column")
```

---

## Type Constructors

PEP 249 constructor functions:

| Constructor         | Return Type          | Description |
|---------------------|----------------------|-------------|
| `Date(y, m, d)`     | `datetime.date`      | Construct a date |
| `Time(h, m, s)`     | `datetime.time`      | Construct a time |
| `Timestamp(y,m,d,h,m,s)` | `datetime.datetime` | Construct a timestamp |
| `DateFromTicks(t)`  | `datetime.date`      | Date from Unix ticks |
| `TimeFromTicks(t)`  | `datetime.time`      | Time from Unix ticks |
| `TimestampFromTicks(t)` | `datetime.datetime` | Timestamp from Unix ticks |
| `Binary(s)`         | `bytes`              | Binary string from bytes |

```python
import pycubrid

d = pycubrid.Date(2025, 1, 15)
t = pycubrid.Time(14, 30, 0)
ts = pycubrid.Timestamp(2025, 1, 15, 14, 30, 0)
b = pycubrid.Binary(b"\x00\x01\x02")
```
