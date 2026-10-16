# Commit and Roll Back

This lesson adds a second row to the tutorial table and then undoes it with a
rollback, so you can see what a transaction keeps and what it discards. It
follows [Save a Row and Read It Back](save-a-row.md).

---

## Before you start

You need what the earlier lessons set up:

- `first_saved_row.py` from [Save a Row and Read It Back](save-a-row.md)
  printed `Rows seen by a new connection: [(1, 'my first saved row')]`, so the
  table `pycubrid_quickstart_notes` exists with one row;
- the same folder and an active virtual environment (in a new terminal, run
  `source .venv/bin/activate` again).

---

## Write the program

Create `first_rollback.py` in the same folder, with the same `SETTINGS`:

```python
import pycubrid

SETTINGS = {"host": "localhost", "port": 33000, "database": "testdb", "user": "dba", "password": ""}

conn = pycubrid.connect(**SETTINGS)
cur = conn.cursor()

# 1. Insert a second row, but do not commit it.
cur.execute(
    "INSERT INTO pycubrid_quickstart_notes (id, note) VALUES (?, ?)",
    (2, "this row is rolled back"),
)
cur.execute("SELECT id, note FROM pycubrid_quickstart_notes ORDER BY id")
print("Before rollback:", cur.fetchall())

# 2. Undo it.
conn.rollback()  # rollback discards everything since the last commit

cur.execute("SELECT id, note FROM pycubrid_quickstart_notes ORDER BY id")
print("After rollback:", cur.fetchall())
cur.close()
conn.close()
```

---

## Run it

```bash
python first_rollback.py
```

The second row is visible inside the transaction, then gone after the
rollback:

```text
Before rollback: [(1, 'my first saved row'), (2, 'this row is rolled back')]
After rollback: [(1, 'my first saved row')]
```

---

## What the program did

- A **transaction** groups your changes until you end it. pycubrid starts one
  for you: autocommit is off by default, so nothing is permanent until you call
  `commit()`.
- Inside the transaction, your own connection already sees the uncommitted
  row, which is why the first `SELECT` shows two rows.
- `rollback()` ends the transaction and discards every change since the last
  `commit()`. The row saved in the previous lesson stays, because it was
  committed.
- Closing a connection without `commit()` does not save its changes either.
  Commit what you want to keep before you close.

You can run `first_rollback.py` again: each run adds and removes the same
second row, so it prints the same two lines. If the program is interrupted
before the rollback, the server discards the uncommitted row when the
connection ends.

---

## Next step

You have saved and undone changes. Last,
[finish safely](finish-safely.md): remove the tutorial table and, if you used
it, the Docker lab.
