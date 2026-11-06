# Migrating off pycubrid.compat

The opt-in `pycubrid.compat` namespaces (`pycubrid.compat.native`,
`pycubrid.compat.cubriddb` and `pycubrid.compat.cursors`) are deprecated. They
imitate the official `CUBRIDdb` and `_cubrid` drivers. This guide shows what to
use instead, and where nothing replaces a Compat feature.

## What is happening

Creating a Compat connection now emits a `FutureWarning` that links to this
page. Compat keeps working. A later 1.x MINOR release will remove it, at least
28 calendar days after the release that first emits the warning reaches PyPI.
No removal version or date is set yet. The rules are in
[`RELEASE_POLICY.md`](https://github.com/cubrid-lab/pycubrid/blob/main/RELEASE_POLICY.md)
(sections 2 to 4).

Why this matters to you:

- Importing `pycubrid` or `pycubrid.aio` never warns. Only Compat users see the
  warning.
- The 1.9.0 Upgrade note promised that the names released in 1.8.0 would stay
  for the rest of 1.x. That promise is withdrawn. `RELEASE_POLICY.md` section 4
  says so openly.
- Code that does not migrate fails with an import error on the removal release.

## Find your uses

The warning text is:

```text
pycubrid.compat is deprecated and will be removed in a later 1.x MINOR release.
Use pycubrid or pycubrid.aio instead; see https://cubrid-lab.github.io/pycubrid/COMPAT_MIGRATION/
```

It is attributed to the line of your code that creates the connection. By
default Python shows it once for each such line. To find every use, turn it into
an error:

```bash
python -W error::FutureWarning your_app.py
pytest -W error::FutureWarning
```

If your own tests already treat warnings as errors, they now fail on Compat
connections. To silence this one warning while you migrate, use a narrow filter
and remove it afterwards:

```python
import warnings

warnings.filterwarnings(
    "ignore",
    message="pycubrid.compat is deprecated",
    category=FutureWarning,
)
```

Do not use a blanket `ignore` for `FutureWarning`.

### Pin the version

A range such as `pycubrid>=1.8.0,<2.0` can install the removal release
automatically, with no change in your code. Once the removal version is
announced in `CHANGELOG.md`, pin below it until you have migrated, for example
`pycubrid>=1.8.0,<X.Y`, where `X.Y` is the announced removal version.

## Renaming alone is wrong

These differences change behavior without raising an error. Check each one in
your code.

| Topic | Compat | `pycubrid.connect()` |
| --- | --- | --- |
| Transactions | Starts in autocommit | Starts in manual commit. Code that never calls `commit()` loses its writes unless you pass `autocommit=True` |
| Default user | `public` | `dba`. Always pass `user` |
| Binding | Server-side typed binds | Client-side literal in the SQL text. See [Parameter Binding](PARAMETER_BINDING.md) |
| Errors | Redacts broker text ("prepared statement failed on server") | Errors and `executemany(..., prepared=True)` row results carry broker text, which can contain bound values or keys. Review your logging |
| Undecodable value in a complete reply | `OperationalError`, session closed | `DataError`, session kept |
| Statement pooling | `native` `prepare()` raises `NotSupportedError` without broker pooling | Not needed |

## Connecting

### Connect

Covers `native.connect`, `native.connection`, and `cubriddb.Connect`,
`cubriddb.connect`, `cubriddb.connection`, `cubriddb.Connection`.

```python
# before
from pycubrid.compat import native, cubriddb
n = native.connect("CUBRID:db.example:33000:testdb:::", "app", "secret")
w = cubriddb.connect("CUBRID:db.example:33000:testdb:::", "app", "secret", charset="euckr")
# after
import pycubrid
conn = pycubrid.connect(host="db.example", port=33000, database="testdb",
                        user="app", password="secret", autocommit=True, charset="euckr")
```

There is no DSN string. `autocommit=True` keeps the Compat default. Drop it for
manual mode. Compat rejects HA and TLS URL options. The ordinary connection
accepts `ssl`, timeouts and `fetch_size` (see [Connection Options](CONNECTION.md)).
The native connection read three server parameters at connect. The ordinary one
does not, so `lock_timeout`, `max_string_len` and `isolation_level` are gone
(see "Isolation level and lock timeout").

### `cubriddb.Connection.connection`

No equivalent and none needed: there is only one connection object.

## Transactions and session settings

### Close, commit, rollback

The names are the same and return `None`. One behavior differs. A Compat result
declared holdable keeps working after `commit()` in manual mode. An ordinary
cursor does not.

```python
# before: manual mode, the result survives commit
n.set_autocommit(False); c = n.cursor(); c.prepare("SELECT id FROM big"); c.execute()
c.fetch_row(); n.commit(); c.fetch_row()      # continues across pages
# after: finish reading first
cur = conn.cursor(); cur.execute("SELECT id FROM big")
rows = cur.fetchall(); conn.commit()
```

Run live with 30,000 rows: the native cursor read all rows after `commit()`.
The ordinary cursor raised `InterfaceError: result set invalidated before all
rows were fetched` once its buffered rows ran out. A short result fits in the
first reply and hides the problem, so a small test can pass. With
`autocommit=True`, ordinary `fetchall()` read all 30,000 rows.

### Autocommit

Covers `set_autocommit`, `get_autocommit` and the `autocommit` attribute.

```python
# before
n.set_autocommit(False); w.autocommit = False; w.get_autocommit()
# after
conn.autocommit = False; conn.autocommit
```

Differences:

- Native assignment only changed a cached value. Ordinary assignment takes effect
  on the server.
- Ordinary assignment applies `bool(value)`. The wrapper raised `ValueError` for
  a non-bool.
- Ordinary assignment always sends SET and COMMIT, so assigning even the same
  value commits open work. Native committed only on an actual mode change.
- Async: `await conn.set_autocommit(False)`.

### Isolation level and lock timeout

Covers `set_isolation_level`, `isolation_level`, `lock_timeout` and
`max_string_len`.

```python
# before
n.set_isolation_level(5); n.isolation_level; n.lock_timeout; n.max_string_len
# after
cur.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
cur.execute("SET TRANSACTION LOCK TIMEOUT 5")
cur.execute("GET TRANSACTION ISOLATION LEVEL TO iso")
cur.execute("SELECT iso")
cur.fetchone()                            # (5,)
cur.execute("GET TRANSACTION LOCK TIMEOUT TO lk")
cur.execute("SELECT lk")
cur.fetchone()                            # (5.0,)
```

Each read takes two statements, and the variable is selected without `@`. Without
`TO`, the `GET TRANSACTION` statement returns no result set. Unlike the native
attributes, these read the current session each time.

The lock timeout comes back in seconds as a `float`, matching `SET TRANSACTION LOCK
TIMEOUT`, and `-1.0` means wait forever. Native `lock_timeout` was in milliseconds.
There is no equivalent for `max_string_len`.

Settings made with SQL are lost when the driver replaces the session. Reapply
them (see [Connection Options](CONNECTION.md)). Native applied its setting
"without committing". `SET TRANSACTION` does not commit either: an open transaction
stays open and can still roll back. After a replacement, the reads above show the
server defaults, which is how you can tell that a setting was lost.

### Versions and ping

Covers `server_version`, `client_version` and `ping`.

```python
# before
n.server_version(); n.client_version(); n.ping()
# after
conn.get_server_version(); pycubrid.__version__; conn.ping(reconnect=False)
```

`ping` returned `1`. The ordinary one returns `True`, so wrap it in `int(...)`
if you compare. It sends CHECK_CAS, not the `select 1+1` query. The default
`reconnect=True` may replace the session, which native never did. For a query
check:

```python
cur.execute("select 1+1 from db_root")
ok = cur.fetchone()[0] == 2
```

## Schema information

### `native.connection.schema_info`

```python
# before
row = n.schema_info(4, "t", "s")                   # first row as a list, or None
# after: schema types 1 CLASS and 2 VCLASS use flag 1; 4 ATTRIBUTE and 5 CLASS_ATTRIBUTE use flag 2
packet = conn.get_schema_info(4, "t", 2, arg2="s")
try:
    rows = conn.fetch_schema_info(packet)
finally:
    conn.close_schema_info(packet)
row = list(rows[0]) if rows else None
if row is not None:
    row[1] = row[1] & 0xFFFF & ~0xE000             # DOMAIN, as native returned it
```

The ordinary call returns all rows as tuples, and `[]` when there are none.
Run live on a `SET(INT)` column: DOMAIN was `8` from native and `-24568` from
ordinary. The mask above gives `8`. After a failed schema request, native kept
the session on a complete server error. Ordinary `get_schema_info` drops it.
Ordinary supports more schema types.

## Cursors and statements

### Creating a cursor

Covers `connection.cursor()`, `Connection.cursor(dictCursor)`, direct
`Cursor(conn)` and `native.cursor(owner)`.

Use `conn.cursor()`. Direct construction has no public equivalent. Use
`with conn.cursor() as cur:` and close cursors explicitly.

### Prepare, bind, execute, fetch

Covers `prepare`, `bind_param`, `execute` and `fetch_row`.

```python
# before
c = n.cursor(); c.prepare("SELECT id, v FROM t WHERE v = ?"); c.bind_param(1, "a")
count = c.execute(); row = c.fetch_row()
# after
cur = conn.cursor()
cur.execute("SELECT id, v FROM t WHERE v = ?", ("a",))   # returns the cursor
rows = cur.fetchall(); count = len(rows); row = rows[0] if rows else None
```

Native `execute()` returned an int count, also for SELECT. Ordinary `execute()`
returns the cursor, and `rowcount` is `-1` for SELECT. There is no reusable
handle: the SQL is rendered again on each call. There is no ordinary server-side
prepared SELECT.

For repeated DML:

```python
# before
c.prepare("INSERT INTO t(id, v) VALUES (?, ?)")
for a, b in rows:
    c.bind_param(1, a); c.bind_param(2, b); c.execute()
# after: server-side typed binds
cur.executemany("INSERT INTO t(id, v) VALUES (?, ?)", rows, prepared=True); conn.commit()
```

`prepared=True` accepts INSERT, UPDATE, DELETE and MERGE with at least one
marker. A single row is allowed. For failure behavior see "Wrapper `executemany`".

### Wrapper `execute(query, args, set_type)`

```python
# before
n_rows = cur.execute("SELECT id FROM t WHERE id = ?", 1)
cur.execute("INSERT INTO t VALUES (?, ?)", (1, [1, 2]))
# after
from pycubrid import Set
cur.execute("SELECT id FROM t WHERE id = ?", (1,))
cur.execute("INSERT INTO t VALUES (?, ?)", (1, Set([1, 2])))
```

Wrap a single value in a tuple. A bare scalar raised
`ProgrammingError: parameters must be a sequence` when run live. A plain `list`,
`tuple` or `set` raises too. Choose `Set`, `Multiset` or `Sequence`. There is no
equivalent for `set_type`: the element types of the rendered literal apply.
Ordinary accepts `Decimal`, `float` and dates, which the wrapper rejected.

### Wrapper `executemany`

```python
# before: returns None, stops at the first failing group
cur.executemany("INSERT INTO t(id, v) VALUES (?, ?)", rows)
# after, closest to stop-at-first-failure
for row in rows:
    cur.execute("INSERT INTO t(id, v) VALUES (?, ?)", row)
# after, batch: every row runs and failures are reported at the end
cur.executemany("INSERT INTO t(id, v) VALUES (?, ?)", rows, prepared=True)
```

Run live with a duplicate in the middle of three rows. The wrapper, in
autocommit, kept the first row, never ran the third and raised `IntegrityError`.
`prepared=True` in manual mode ran rows 1 and 3, undid row 2, raised
`IntegrityError` and left the commit decision to the caller. The ordinary call
returns the cursor. Plain `executemany` sends literal batches. Its failure
behavior was not compared with the wrapper.

## Reading rows

### Fetching and result metadata

Covers `fetchone`, `fetchmany`, `fetchall`, iteration, `next`, `close`,
`description`, `rowcount`, `arraysize` and `con`.

The fetch names are the same. Replace `cur.next()` with `next(cur)`. `cur.con`
is gone, so keep your own reference to the connection. `description` differs.
Run live: `('id', 8, 0, 0, 10, 0, 0)` from the wrapper and
`('id', 8, None, None, 10, 0, False)` from ordinary. The size fields are `None`
and `null_ok` is a `bool`. It is read-only. `fetchmany(0)` warns on the ordinary
cursor and not on the wrapper.

### Dictionary rows

```python
# before
cur = w.cursor(True); cur.execute(sql); cur.fetchone()
# after
cur = conn.cursor(); cur.execute(sql)
names = [d[0] for d in cur.description]
rows = [dict(zip(names, r)) for r in cur.fetchall()]
```

Run live, the result was identical. With duplicate column names the last one
wins in both.

### `set_fetch_value_converter`

There is no equivalent API. Convert at the call site:

```python
rows = [convert(r, cur.description) for r in cur.fetchall()]
```

The second argument has the shape described under "Fetching and result metadata", so a converter that reads
index 2, 3 or 6 of it sees different values.

## Collections

### SET, MULTISET and SEQUENCE

Covers `connection.set`, `native.set`, `set.imports` and `bind_set`.

```python
# before
s = n.set(); s.imports((1, 2, 2), 8)
c.prepare("INSERT INTO t VALUES (?, ?)"); c.bind_param(1, 1); c.bind_set(2, s); c.execute()
# after, literal
from pycubrid import Set, Multiset, Sequence
cur.execute("INSERT INTO t VALUES (?, ?)", (1, Set([1, 2, 2])))
# after, typed
cur.executemany("INSERT INTO t VALUES (?, ?)", [(1, Set([1, 2]))], prepared=True)
```

The `kind=` argument maps to the class you choose. On the typed path a
`Multiset` is sent as SEQUENCE, as native did. The literal path renders
`MULTISET{...}`. Run live, all three stored rows decoded the same way. Both
drivers return raw `bytes` for collection columns by default. Pass
`pycubrid.connect(decode_collections=True)` to get `frozenset` and `list`
values. Native had no such option. A native set that was never imported bound
NULL. Pass `None` instead.

## Large objects (LOBs)

### BLOB and CLOB

Covers `connection.lob`, `native.lob`, `bind_lob`, `fetch_lob`,
`lob.write/read/seek/imports/export/close` and the `SEEK_*` constants.

```python
# before: write
l = n.lob(); l.write(b"data", "B")
c.prepare("INSERT INTO t(id, b) VALUES (?, ?)"); c.bind_param(1, 1); c.bind_lob(2, l); c.execute()
# after: autocommit must be off
lob = conn.create_lob(23)                 # 23 is BLOB, 24 is CLOB
offset = lob.write(b"data")
offset += lob.write(b"more", offset)      # the offset is explicit and append-only
cur.executemany("INSERT INTO t(id, b) VALUES (?, ?)", [(1, lob)], prepared=True)
conn.commit()

# before: read
l = n.lob(); c.prepare("SELECT c FROM t WHERE id = ?"); c.bind_param(1, 2); c.execute()
c.fetch_lob(1, l); text = l.read(); l.seek(0, native.SEEK_SET)
# after
cur.execute("SELECT c FROM t WHERE id = ?", (2,))
cell = cur.fetchone()[0]                  # None for SQL NULL
lob = pycubrid.Lob(conn, cell["lob_type"], cell["packed_lob_handle"])
text = lob.read(cell["lob_length"]).decode("utf-8")

# before: files
l.imports("in.bin", "B"); l.export("out.bin")
# after: not atomic; write to a temporary file and os.replace() for the same guarantee
file_lob = conn.create_lob(23)            # a new LOB: writes only append
with open("in.bin", "rb") as fh:
    offset = 0
    while chunk := fh.read(65536):
        offset += file_lob.write(chunk, offset)
with open("out.bin", "wb") as fh:
    fh.write(file_lob.read(offset))
```

| Topic | Native | Ordinary (run live) |
| --- | --- | --- |
| Single-row bind | `bind_lob` and `execute` | `execute(sql, (lob,))` raises `ProgrammingError`. Use `executemany(..., prepared=True)` |
| Autocommit | Allowed. The first statement consumes the temporary handle | Refused with `NotSupportedError` |
| Read type | `str`, decoded as UTF-8. A BLOB that is not UTF-8 raises `UnicodeDecodeError` | `bytes` |
| Position | Stateful `seek` | You hold the offset. `SEEK_*` has no use. Use the `os.SEEK_*` meanings in your own code |
| Rebind a fetched handle on the same connection | Yes | Yes (a `Lob` built from the cell) |
| Rebind on another connection | Yes, for committed rows | No equivalent: raises `InterfaceError`. Read the bytes and write a new LOB |
| Write at a non-append offset | `NotSupportedError` before any I/O | The server returns error -1016 |
| Async | None | None |

## Features without an ordinary replacement

Compat features that have no ordinary API. Where a workaround exists, it is
listed.

| Compat feature | Ordinary status | What to do |
| --- | --- | --- |
| `cubriddb.Connection.connection` | No equivalent, not needed | Use the one connection object |
| `max_string_len` | No equivalent | None |
| `set_fetch_value_converter` | No equivalent API | Convert rows at the call site |
| `set_type` argument of wrapper `execute` | No equivalent | The literal's element types apply |
| `result_info` | No equivalent API | See below |
| `data_seek`, `row_seek`, `row_tell` | No equivalent API | See below |
| Result that survives `commit()` | Not supported on large results | Finish reading before commit, or use a second connection |
| LOB bind in autocommit | Refused | Use a manual transaction |
| Rebind a fetched LOB on another connection | No equivalent | Read the bytes and write a new LOB |
| Server-side prepared SELECT, reusable statement handle | No equivalent | Use `execute()`, which renders literals |

### `result_info`

There is no equivalent API. `description` gives name, type, precision, scale
and nullability. The real column name, table name, default value,
auto-increment, unique, primary key, foreign key, reverse index and shared
flags are not public on ordinary cursors. For a known table, `get_schema_info` gives part of this. It cannot map an arbitrary query column.

### `data_seek`, `row_seek`, `row_tell`

There is no equivalent API. Materialize the rows, or page in SQL:

```python
rows = cur.fetchall(); row = rows[n - 1]
cur.execute("SELECT id FROM t ORDER BY id LIMIT ?, ?", (offset, size))
```

Materializing costs memory in proportion to the result. The `LIMIT` form was not
run for this guide.

## What was and was not verified

- The "run live" results were observed once on CUBRID 11.4.6 with Python 3.12.
  Versions 10.2, 11.0 and 11.2 were not verified.
- The `SET TRANSACTION` and `GET TRANSACTION` results were checked live on 10.2, 11.0,
  11.2 and 11.4, sync and async.
- Not verified: a way to read `max_string_len`, the failure behavior of
  plain `executemany` compared with the wrapper, and the `LIMIT ?, ?` form.
- Private use of Compat cannot be counted, so the project cannot say that nobody
  depends on it.
