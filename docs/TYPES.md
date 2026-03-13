# Type System

Complete reference for pycubrid's PEP 249 type objects, constructors, and CUBRID CCI data type codes.

---

## Table of Contents

- [Overview](#overview)
- [DBAPIType Class](#dbapitype-class)
- [PEP 249 Type Objects](#pep-249-type-objects)
  - [STRING](#string)
  - [BINARY](#binary)
  - [NUMBER](#number)
  - [DATETIME](#datetime)
  - [ROWID](#rowid)
- [PEP 249 Constructors](#pep-249-constructors)
- [CUBRID CCI_U_TYPE Codes](#cubrid-cci_u_type-codes)
- [Type Conversion Table](#type-conversion-table)
- [LOB Type Handling](#lob-type-handling)
- [Collection Types](#collection-types)
- [Usage Examples](#usage-examples)

---

## Overview

pycubrid implements the DB-API 2.0 (PEP 249) type system with:

- **5 type objects** — `STRING`, `BINARY`, `NUMBER`, `DATETIME`, `ROWID`
- **7 constructors** — `Date`, `Time`, `Timestamp`, `DateFromTicks`, `TimeFromTicks`, `TimestampFromTicks`, `Binary`
- **27+ CCI data type codes** — mapping CUBRID wire types to Python types

Type objects enable comparison with `cursor.description` type codes:

```python
import pycubrid

conn = pycubrid.connect(host="localhost", port=33000, database="testdb")
cur = conn.cursor()
cur.execute("SELECT name, age, created_at FROM cookbook_users")

for col in cur.description:
    col_name = col[0]
    col_type = col[1]

    if col_type == pycubrid.STRING:
        print(f"{col_name} is a string column")
    elif col_type == pycubrid.NUMBER:
        print(f"{col_name} is a numeric column")
    elif col_type == pycubrid.DATETIME:
        print(f"{col_name} is a date/time column")
```

---

## DBAPIType Class

The `DBAPIType` class implements the PEP 249 type comparison protocol. Each instance wraps a `frozenset` of integer type codes and compares equal to any code in that set.

```python
from pycubrid.types import DBAPIType

# Create a custom type object
MY_TYPE = DBAPIType("MY_TYPE", frozenset({1, 2, 3}))

# Comparison with integer type codes
assert MY_TYPE == 1      # True — 1 is in the set
assert MY_TYPE == 4      # False — 4 is not in the set
assert MY_TYPE != 4      # True

# Comparison with other DBAPIType instances
OTHER = DBAPIType("OTHER", frozenset({1, 2, 3}))
assert MY_TYPE == OTHER  # True — same value sets
```

### Methods

| Method | Description |
|---|---|
| `__eq__(other)` | Returns `True` if `other` (int) is in `values`, or if `other` (DBAPIType) has the same `values` set |
| `__ne__(other)` | Negation of `__eq__` |
| `__hash__()` | Hash based on the frozen set of values |
| `__repr__()` | Returns `DBAPIType('name')` |

---

## PEP 249 Type Objects

### STRING

Describes string-based columns.

```python
from pycubrid import STRING
```

| Member Type | CCI Code | CUBRID SQL Type |
|---|---|---|
| CHAR | 1 | `CHAR(n)` |
| STRING (VARCHAR) | 2 | `VARCHAR(n)` |
| NCHAR | 3 | `NCHAR(n)` |
| VARNCHAR | 4 | `NCHAR VARYING(n)` |
| ENUM | 25 | `ENUM` |
| CLOB | 24 | `CLOB` |

### BINARY

Describes binary data columns.

```python
from pycubrid import BINARY
```

| Member Type | CCI Code | CUBRID SQL Type |
|---|---|---|
| BIT | 5 | `BIT(n)` |
| VARBIT | 6 | `BIT VARYING(n)` |
| BLOB | 23 | `BLOB` |

### NUMBER

Describes numeric columns.

```python
from pycubrid import NUMBER
```

| Member Type | CCI Code | CUBRID SQL Type | Python Type |
|---|---|---|---|
| SHORT | 9 | `SMALLINT` | `int` |
| INT | 8 | `INTEGER` | `int` |
| BIGINT | 21 | `BIGINT` | `int` |
| FLOAT | 11 | `FLOAT` | `float` |
| DOUBLE | 12 | `DOUBLE` | `float` |
| NUMERIC | 7 | `NUMERIC(p, s)` | `Decimal` |
| MONETARY | 10 | `MONETARY` | `float` |

### DATETIME

Describes date and time columns.

```python
from pycubrid import DATETIME
```

| Member Type | CCI Code | CUBRID SQL Type | Python Type |
|---|---|---|---|
| DATE | 13 | `DATE` | `datetime.date` |
| TIME | 14 | `TIME` | `datetime.time` |
| TIMESTAMP | 15 | `TIMESTAMP` | `datetime.datetime` |
| DATETIME | 22 | `DATETIME` | `datetime.datetime` |
| TIMESTAMPTZ | 29 | `TIMESTAMPTZ` | `datetime.datetime` |
| TIMESTAMPLTZ | 30 | `TIMESTAMPLTZ` | `datetime.datetime` |
| DATETIMETZ | 31 | `DATETIMETZ` | `datetime.datetime` |
| DATETIMELTZ | 32 | `DATETIMELTZ` | `datetime.datetime` |

### ROWID

Describes row identifier columns.

```python
from pycubrid import ROWID
```

| Member Type | CCI Code | CUBRID SQL Type | Python Type |
|---|---|---|---|
| OBJECT | 19 | `OBJECT` (OID) | `str` (`"OID:@page|slot|volume"`) |

---

## PEP 249 Constructors

These functions create Python objects suitable for use as query parameters.

| Constructor | Signature | Returns | Description |
|---|---|---|---|
| `Date` | `(year, month, day)` | `datetime.date` | Calendar date |
| `Time` | `(hour, minute, second)` | `datetime.time` | Time of day |
| `Timestamp` | `(year, month, day, hour, minute, second)` | `datetime.datetime` | Date and time |
| `DateFromTicks` | `(ticks)` | `datetime.date` | Date from Unix timestamp |
| `TimeFromTicks` | `(ticks)` | `datetime.time` | Time from Unix timestamp |
| `TimestampFromTicks` | `(ticks)` | `datetime.datetime` | Datetime from Unix timestamp |
| `Binary` | `(value)` | `bytes` | Binary data from `bytes`, `bytearray`, or `str` (UTF-8) |

### Usage

```python
import pycubrid

# Date/Time constructors
d = pycubrid.Date(2025, 3, 15)
t = pycubrid.Time(14, 30, 0)
ts = pycubrid.Timestamp(2025, 3, 15, 14, 30, 0)

# From Unix timestamps
d2 = pycubrid.DateFromTicks(1710500000.0)
t2 = pycubrid.TimeFromTicks(1710500000.0)
ts2 = pycubrid.TimestampFromTicks(1710500000.0)

# Binary constructor
b1 = pycubrid.Binary(b"\x00\x01\x02")           # from bytes
b2 = pycubrid.Binary(bytearray([0, 1, 2]))      # from bytearray
b3 = pycubrid.Binary("hello")                    # from str → UTF-8 encoded bytes
```

---

## CUBRID CCI_U_TYPE Codes

These integer codes are used on the CAS wire protocol and appear in `cursor.description[n][1]`.

| Constant | Code | CUBRID Type | Category |
|---|---|---|---|
| `UNKNOWN` / `NULL` | 0 | NULL | — |
| `CHAR` | 1 | `CHAR(n)` | STRING |
| `STRING` | 2 | `VARCHAR(n)` | STRING |
| `NCHAR` | 3 | `NCHAR(n)` | STRING |
| `VARNCHAR` | 4 | `NCHAR VARYING(n)` | STRING |
| `BIT` | 5 | `BIT(n)` | BINARY |
| `VARBIT` | 6 | `BIT VARYING(n)` | BINARY |
| `NUMERIC` | 7 | `NUMERIC(p, s)` | NUMBER |
| `INT` | 8 | `INTEGER` | NUMBER |
| `SHORT` | 9 | `SMALLINT` | NUMBER |
| `MONETARY` | 10 | `MONETARY` | NUMBER |
| `FLOAT` | 11 | `FLOAT` | NUMBER |
| `DOUBLE` | 12 | `DOUBLE` | NUMBER |
| `DATE` | 13 | `DATE` | DATETIME |
| `TIME` | 14 | `TIME` | DATETIME |
| `TIMESTAMP` | 15 | `TIMESTAMP` | DATETIME |
| `SET` | 16 | `SET` | Collection |
| `MULTISET` | 17 | `MULTISET` | Collection |
| `SEQUENCE` | 18 | `SEQUENCE` / `LIST` | Collection |
| `OBJECT` | 19 | `OBJECT` (OID) | ROWID |
| `RESULTSET` | 20 | Result set | — |
| `BIGINT` | 21 | `BIGINT` | NUMBER |
| `DATETIME` | 22 | `DATETIME` | DATETIME |
| `BLOB` | 23 | `BLOB` | BINARY |
| `CLOB` | 24 | `CLOB` | STRING |
| `ENUM` | 25 | `ENUM` | STRING |
| `TIMESTAMPTZ` | 29 | `TIMESTAMPTZ` | DATETIME |
| `TIMESTAMPLTZ` | 30 | `TIMESTAMPLTZ` | DATETIME |
| `DATETIMETZ` | 31 | `DATETIMETZ` | DATETIME |
| `DATETIMELTZ` | 32 | `DATETIMELTZ` | DATETIME |

These codes are available as the `CUBRIDDataType` enum:

```python
from pycubrid.constants import CUBRIDDataType

print(CUBRIDDataType.INT)       # CUBRIDDataType.INT (8)
print(CUBRIDDataType.VARCHAR)   # AttributeError — use CUBRIDDataType.STRING (2)
```

---

## Type Conversion Table

How pycubrid converts CUBRID wire types to Python objects when fetching results:

| CUBRID Type | CCI Code | Python Type | Notes |
|---|---|---|---|
| `CHAR`, `VARCHAR`, `NCHAR`, `NCHAR VARYING`, `ENUM` | 1–4, 25 | `str` | Null-terminated, UTF-8 decoded |
| `SHORT` (SMALLINT) | 9 | `int` | 16-bit signed |
| `INTEGER` | 8 | `int` | 32-bit signed |
| `BIGINT` | 21 | `int` | 64-bit signed |
| `FLOAT` | 11 | `float` | IEEE 754 single |
| `DOUBLE`, `MONETARY` | 12, 10 | `float` | IEEE 754 double |
| `NUMERIC` / `DECIMAL` | 7 | `Decimal` | Exact numeric (string-parsed) |
| `DATE` | 13 | `datetime.date` | Calendar date |
| `TIME` | 14 | `datetime.time` | Time of day |
| `TIMESTAMP` | 15 | `datetime.datetime` | Date + time (microsecond = 0) |
| `DATETIME` | 22 | `datetime.datetime` | Date + time + millisecond |
| `TIMESTAMPTZ`, `TIMESTAMPLTZ` | 29, 30 | `datetime.datetime` | Timezone-aware timestamps |
| `DATETIMETZ`, `DATETIMELTZ` | 31, 32 | `datetime.datetime` | Timezone-aware datetimes |
| `BIT`, `BIT VARYING` | 5, 6 | `bytes` | Raw binary data |
| `SET`, `MULTISET`, `SEQUENCE` | 16, 17, 18 | `bytes` | Raw bytes (not decoded) |
| `OBJECT` (OID) | 19 | `str` | Format: `"OID:@page\|slot\|volume"` |
| `BLOB` | 23 | `dict` | LOB handle (see below) |
| `CLOB` | 24 | `dict` | LOB handle (see below) |
| `NULL` / `UNKNOWN` | 0 | `None` | — |

---

## LOB Type Handling

LOB columns (BLOB and CLOB) are **not** returned as content. Instead, they return a **handle dictionary** that can be used with the `Lob` class to read the actual data.

### LOB Handle Structure

When you fetch a BLOB or CLOB column, you receive:

```python
{
    "lob_type": 23,                    # CUBRIDDataType.BLOB (23) or CLOB (24)
    "lob_length": 1024,                # Size of the LOB content in bytes
    "file_locator": "file://.../...",   # Server-side file locator
    "packed_lob_handle": b"...",        # Raw handle bytes for Lob.read()
}
```

### Reading LOB Content

```python
from pycubrid.lob import Lob
from pycubrid.constants import CUBRIDDataType

conn = pycubrid.connect(host="localhost", port=33000, database="testdb")
cur = conn.cursor()

# Insert string data directly into CLOB column
cur.execute("INSERT INTO cookbook_docs (content) VALUES ('Hello, CLOB!')")
conn.commit()

# Fetch returns a LOB handle dict, not the content
cur.execute("SELECT content FROM cookbook_docs WHERE id = 1")
row = cur.fetchone()
lob_handle = row[0]  # dict with lob_type, lob_length, etc.

# To read content, use Lob class with the packed handle
lob = Lob(conn, CUBRIDDataType.CLOB, lob_handle["packed_lob_handle"])
content = lob.read(lob_handle["lob_length"])
print(content)  # b'Hello, CLOB!'
```

> **Important**: You cannot pass `Lob` objects as query parameters. Insert strings directly into CLOB columns and bytes directly into BLOB columns.

---

## Collection Types

CUBRID's collection types (`SET`, `MULTISET`, `SEQUENCE`) are returned as raw `bytes` by pycubrid. The driver does not decode collection contents — they are returned in their wire format.

| CUBRID Type | CCI Code | Python Return Type |
|---|---|---|
| `SET` | 16 | `bytes` |
| `MULTISET` | 17 | `bytes` |
| `SEQUENCE` | 18 | `bytes` |

> **Note**: Collection types are a CUBRID-specific feature. If you need structured collection data, consider using normalized tables or JSON-encoded strings.

---

## Usage Examples

### Type Checking with cursor.description

```python
import pycubrid

conn = pycubrid.connect(host="localhost", port=33000, database="testdb")
cur = conn.cursor()
cur.execute("SELECT * FROM cookbook_users LIMIT 1")

for col in cur.description:
    name, type_code, _, _, precision, scale, nullable = col
    if type_code == pycubrid.STRING:
        print(f"  {name}: STRING (precision={precision})")
    elif type_code == pycubrid.NUMBER:
        print(f"  {name}: NUMBER (precision={precision}, scale={scale})")
    elif type_code == pycubrid.DATETIME:
        print(f"  {name}: DATETIME")
    elif type_code == pycubrid.BINARY:
        print(f"  {name}: BINARY")
    elif type_code == pycubrid.ROWID:
        print(f"  {name}: ROWID (OID)")
    else:
        print(f"  {name}: type_code={type_code}")

cur.close()
conn.close()
```

### Parameter Binding with Constructors

```python
import pycubrid

conn = pycubrid.connect(host="localhost", port=33000, database="testdb")
cur = conn.cursor()

# Use PEP 249 constructors for parameter values
cur.execute(
    "INSERT INTO cookbook_events (event_date, event_time, created_at) VALUES (?, ?, ?)",
    [
        pycubrid.Date(2025, 12, 25),
        pycubrid.Time(10, 0, 0),
        pycubrid.Timestamp(2025, 3, 15, 14, 30, 0),
    ],
)
conn.commit()
cur.close()
conn.close()
```

### Using CUBRIDDataType Enum

```python
from pycubrid.constants import CUBRIDDataType

# Check specific type codes
assert CUBRIDDataType.INT == 8
assert CUBRIDDataType.STRING == 2
assert CUBRIDDataType.DATETIME == 22

# Use in type switch logic
def describe_type(code: int) -> str:
    match code:
        case CUBRIDDataType.INT | CUBRIDDataType.BIGINT | CUBRIDDataType.SHORT:
            return "integer"
        case CUBRIDDataType.FLOAT | CUBRIDDataType.DOUBLE:
            return "floating point"
        case CUBRIDDataType.NUMERIC:
            return "exact decimal"
        case CUBRIDDataType.STRING | CUBRIDDataType.CHAR:
            return "text"
        case _:
            return f"other ({code})"
```

---

*See also: [API Reference](API_REFERENCE.md) · [Examples](EXAMPLES.md) · [Connection Guide](CONNECTION.md)*
