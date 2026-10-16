# Run Your First Query

This lesson connects to CUBRID, sends one SQL statement and reads the one row
it returns. It is the lesson that follows
[Your First CUBRID Connection](quickstart.md), which prepares the database and
Python.

---

## Before you start

You need what the preparation lesson set up:

- a CUBRID database that answers: the Docker lab printed `CUBRID is ready`
  ([Before you start](quickstart.md#before-you-start)), or you have the five
  values for your own database;
- pycubrid installed in a virtual environment
  ([Step 1](quickstart.md#step-1-install-pycubrid-in-a-virtual-environment)).

Work in the same folder as Step 1. In a new terminal, activate the virtual
environment again with `source .venv/bin/activate`. Each step is a file that you
run from the terminal, so nothing carries over from an earlier Python session.

---

## Write the program

Create a file named `first_query.py` in that folder:

```python
import pycubrid

# Connection settings for the disposable local lab. With your own database,
# replace them with the values from your administrator.
conn = pycubrid.connect(
    host="localhost",  # the machine that runs the CUBRID broker
    port=33000,  # the broker port (33000 is the CUBRID default)
    database="testdb",  # the database name
    user="dba",
    password="",
)

cur = conn.cursor()  # a cursor sends SQL and reads results
cur.execute("SELECT 1 + 1")  # execute sends one SQL statement
print(cur.fetchone())  # fetchone reads one result row

cur.close()
conn.close()
```

---

## Run it

```bash
python first_query.py
```

It prints one row with one value:

```text
(2,)
```

If you see an error instead, find its last line in
[If the first connection fails](quickstart.md#if-the-first-connection-fails).

---

## What the program did

- `pycubrid.connect()` opens a **connection**, your session with the database.
  It needs the five values: host, port, database, user and password.
- `conn.cursor()` creates a **cursor**. A cursor works inside a connection: it
  sends SQL statements and reads their results.
- `cur.execute()` sends one SQL statement to the server.
- `cur.fetchone()` reads one row of the result. A row is a tuple, so the single
  value `2` prints as `(2,)`.
- `close()` ends the cursor and then the connection. Close what you opened, so
  the server does not keep an idle session for you.

By default `connect()` sets no time limit of its own on reaching the server. If
a firewall silently drops the connection, the call waits until the operating
system gives up. With a database on another machine, add for example
`connect_timeout=10` to the `connect()` call; the
[Connection Guide](CONNECTION.md#custom-port-and-timeout) describes it and the
other timeouts.

---

## Next step

The first query only read a computed value. Next,
[save a row and read it back](quickstart.md#step-3-save-a-row-and-read-it-back),
which creates a small table, commits one row and rolls back another.
