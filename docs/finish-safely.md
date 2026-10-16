# Finish Safely

This lesson removes what the tutorial created: the tutorial table and, if you
used it, the Docker lab. It also tells you how to recover when a lesson was
interrupted or run twice. It follows
[Commit and Roll Back](commit-and-rollback.md).

---

## Before you start

You need the same folder, the same five connection values and an active
virtual environment (in a new terminal, run `source .venv/bin/activate` again).
You can run this lesson at any point after
[Save a Row and Read It Back](save-a-row.md), also when a lesson failed half
way.

---

## Clean up the tutorial table

`cleanup_quickstart.py` removes the tutorial table, and only if the table of
that name owned by your user carries the tutorial's comment. A table of the
same name that you created yourself, or one that belongs to another user, is
left alone. Create it in the same folder, with the same `SETTINGS`:

```python
import pycubrid

SETTINGS = {"host": "localhost", "port": 33000, "database": "testdb", "user": "dba", "password": ""}
MARK = "created by the pycubrid quickstart"

conn = pycubrid.connect(**SETTINGS)
cur = conn.cursor()
cur.execute(
    "SELECT comment FROM db_class WHERE class_name = ? AND owner_name = CURRENT_USER",
    ("pycubrid_quickstart_notes",),
)
row = cur.fetchone()
if row is None:
    print("Nothing to clean up: pycubrid_quickstart_notes does not exist.")
elif row[0] != MARK:
    print("Not dropped: pycubrid_quickstart_notes was not created by this quickstart.")
else:
    cur.execute("DROP TABLE pycubrid_quickstart_notes")
    conn.commit()
    print("Dropped pycubrid_quickstart_notes.")
cur.close()
conn.close()
```

Run it:

```bash
python cleanup_quickstart.py
```

It prints one of three lines:

| Output | What it means |
|---|---|
| `Dropped pycubrid_quickstart_notes.` | The tutorial table was removed. |
| `Nothing to clean up: pycubrid_quickstart_notes does not exist.` | Your user has no table of that name; nothing was changed. |
| `Not dropped: pycubrid_quickstart_notes was not created by this quickstart.` | Your user has a table of that name without the tutorial's comment. It is left alone. |

### How the check works

`db_class` is the CUBRID catalog of tables. The query looks for a table with
the tutorial's name whose owner is `CURRENT_USER`, the user you connected as,
and reads its comment. The script drops the table only when the comment is the
one that [Save a Row and Read It Back](save-a-row.md) wrote. The table name is
passed with a `?` placeholder, like any other value.

If you see `Not dropped`, the table is not the tutorial's: do not drop it to
make the lessons work. Use another practice database or account instead, or
ask whoever created that table.

---

## Remove the Docker lab

If you used the disposable Docker lab from
[Option B](quickstart.md#option-b-i-need-a-disposable-local-database), remove
the whole container, with its database:

```bash
docker rm -f pycubrid-quickstart
```

This removes only the container named `pycubrid-quickstart`. Skip this step
with your own database.

---

## If a lesson was interrupted or run twice

| What you see | What to do |
|---|---|
| `first_saved_row.py` stops with `Class dba.pycubrid_quickstart_notes already exists` (with your user name) | The table from an earlier run is still there. Run `cleanup_quickstart.py`, then run `first_saved_row.py` again. |
| `first_rollback.py` was interrupted before it printed `After rollback` | Nothing to clean up: the uncommitted row is discarded when the connection ends. Run it again. |
| `first_rollback.py` stops with `Unknown class "dba.pycubrid_quickstart_notes"` (with your user name) | Run `first_saved_row.py` from [Save a Row and Read It Back](save-a-row.md) first. |

---

## Next step

You have finished the lessons. From here:

- [Examples](EXAMPLES.md): updates, deletes, `with` blocks, async code and LOBs.
- [Connection Options](CONNECTION.md): every connection option, URLs, timeouts
  and TLS.
- [Troubleshooting](TROUBLESHOOTING.md) and [FAQ](faq.md) when something does
  not work.
