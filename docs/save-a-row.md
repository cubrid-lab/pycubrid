# Save a Row and Read It Back

This lesson creates a small table, saves one row in it and reads that row from
a new connection. It follows [Run Your First Query](first-query.md).

---

## Before you start

You need what the earlier lessons set up:

- `first_query.py` from [Run Your First Query](first-query.md) printed `(2,)`,
  so the five connection values work;
- the same folder and an active virtual environment (in a new terminal, run
  `source .venv/bin/activate` again).

This lesson creates a table named `pycubrid_quickstart_notes` for your user. Use
the practice database or account from
[Before you start](quickstart.md#before-you-start), not a production database.

---

## Write the program

Create `first_saved_row.py` in the same folder. Put your own five values in
`SETTINGS` if you do not use the Docker lab:

```python
import pycubrid

SETTINGS = {"host": "localhost", "port": 33000, "database": "testdb", "user": "dba", "password": ""}

conn = pycubrid.connect(**SETTINGS)
cur = conn.cursor()

# 1. Create a table that belongs to this tutorial. The comment marks it, so the
#    cleanup script only ever drops this tutorial's own table.
cur.execute(
    "CREATE TABLE pycubrid_quickstart_notes (id INT PRIMARY KEY, note VARCHAR(100))"
    " COMMENT = 'created by the pycubrid quickstart'"
)
conn.commit()

# 2. Insert one row. The ? marks are filled with the values that follow, in order.
cur.execute(
    "INSERT INTO pycubrid_quickstart_notes (id, note) VALUES (?, ?)",
    (1, "my first saved row"),
)
conn.commit()  # commit makes the change permanent and visible to others
cur.close()
conn.close()

# 3. Check from a brand-new connection that the committed row was saved.
check = pycubrid.connect(**SETTINGS)
check_cur = check.cursor()
check_cur.execute("SELECT id, note FROM pycubrid_quickstart_notes ORDER BY id")
print("Rows seen by a new connection:", check_cur.fetchall())
check_cur.close()
check.close()
```

---

## Run it

```bash
python first_saved_row.py
```

A second connection sees the saved row:

```text
Rows seen by a new connection: [(1, 'my first saved row')]
```

---

## What the program did

- `SETTINGS` holds the five connection values once, and `connect(**SETTINGS)`
  passes them as keyword arguments, so both connections use the same values.
- `CREATE TABLE` made the tutorial table. Its comment marks it as the
  tutorial's own, which [Finish Safely](finish-safely.md) checks before it
  drops anything.
- The `?` marks are placeholders: pycubrid replaces each one with the matching
  value from the tuple, safely escaped, before it sends the statement. Always
  pass values this way instead of pasting them into the SQL text yourself.
  [Parameter Binding](PARAMETER_BINDING.md) describes how each Python type is
  sent.
- `commit()` makes the changes permanent. Until you commit, other connections
  do not see them. The new connection `check` proves that the row was saved.
- `fetchall()` reads every remaining row of the result as a list of tuples.

---

## If you run it again

Running the file a second time, or again after it was interrupted, stops at the
first statement with `Class dba.pycubrid_quickstart_notes already exists` (the
owner name, here `dba`, is your user). The table from the earlier run is still
there. Do not drop it by hand: run the cleanup in
[Finish Safely](finish-safely.md#clean-up-the-tutorial-table), then run
`first_saved_row.py` again.

---

## Next step

The row is saved. Next, [commit and roll back](commit-and-rollback.md): add a
second row and undo it before it is saved.
