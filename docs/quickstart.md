# Your First CUBRID Connection

pycubrid is a Python **connector**: a library your program uses to talk to a
CUBRID database. The database itself is a separate program, the CUBRID server,
that must already be running somewhere. Installing pycubrid does not install
CUBRID or create a database.

This page is the first of five short lessons. It prepares a database and
Python, starting from an empty folder. The next lessons each add one step:
[run your first query](first-query.md), [save a row](save-a-row.md),
[commit and roll back](commit-and-rollback.md) and
[finish safely](finish-safely.md). You do not need to know DB-API, the CUBRID
wire protocol or this project's CI to follow them; those live in the
[reference pages](#next-steps).

---

## Before you start

You need Python 3.11 or later and a CUBRID database you can connect to. Pick
the option that matches your situation.

### Option A: I already have a CUBRID database

Ask its administrator (or look in your own setup) for these five values:

- **host**: the machine that runs CUBRID, for example `db.example.com` or `localhost`;
- **port**: the CUBRID broker port, the entry point that accepts connections (the default is `33000`);
- **database**: the database name, for example `testdb`;
- **user** and **password**: an account for that database.

The lessons after this page create and later remove one small table. Use a
practice database, or an account that is allowed to create tables there. Do not
use a production database or its `dba` account for this tutorial. Put your
values in place of the lab values in each lesson's program, then continue with
Step 1.

### Option B: I need a disposable local database

This starts a throwaway CUBRID 11.4 server with a database named `testdb` in
Docker. It needs [Docker](https://docs.docker.com/get-started/get-docker/)
and a shell such as bash. The commands were verified on Linux on x86_64
(amd64); the `cubrid/cubrid:11.4` image is published for amd64 only, so other
architectures (for example Apple silicon) and Windows are unverified.

First start the container:

```bash
docker run -d --name pycubrid-quickstart -e CUBRID_DB=testdb -p 127.0.0.1:33000:33000 cubrid/cubrid:11.4
```

`127.0.0.1:` publishes the port on this computer only, so no other machine can
connect to the lab. The database needs a little time to start. Paste this into
the same shell; it asks the database every 5 seconds, for up to 3 minutes,
whether it answers:

```bash
ready=no
for attempt in $(seq 36); do
  if docker exec pycubrid-quickstart csql -u dba testdb -c "SELECT 1" >/dev/null 2>&1; then
    ready=yes
    break
  fi
  sleep 5
done
if [ "$ready" = yes ]; then
  echo "CUBRID is ready"
else
  echo "CUBRID did not answer within 3 minutes; see: docker logs pycubrid-quickstart"
fi
```

It usually prints `CUBRID is ready` well within a minute. If it prints
`CUBRID did not answer within 3 minutes` instead, `docker logs pycubrid-quickstart` shows why; if `docker run` itself
failed, for example because port 33000 is already in use, fix that first,
remove the container with `docker rm -f pycubrid-quickstart` and start again.
The lab uses the `dba` account with an empty password. That is only acceptable
for this local, disposable container: never use an empty password on a server
other people can reach.

When you are done with the whole tutorial,
[Finish Safely](finish-safely.md#remove-the-docker-lab) removes the lab.

---

## Step 1: Install pycubrid in a virtual environment

A virtual environment keeps this project's packages separate from the rest of
your system. First check that `python3 --version` prints 3.11 or later; if it
prints an older version, use a newer interpreter by name (for example
`python3.12`) in the first command. pip installs only the pycubrid releases
that support your Python, so an old interpreter gets an old pycubrid. Then run
these commands in an empty folder:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install pycubrid
python -c "import pycubrid; print(pycubrid.__version__)"
```

The last command prints the installed version, for example `1.13.0`. Run every
`python` command below in this same terminal, with the virtual environment
active. Next, [run your first query](first-query.md).

---

## Step 2: Run your first query

The first query has its own lesson: [Run Your First Query](first-query.md)
creates `first_query.py`, runs it and explains the connection and cursor it
uses.

---

## Step 3: Save a row and read it back

Saving data now has two lessons of its own:
[Save a Row and Read It Back](save-a-row.md) creates the tutorial table and
commits one row, and [Commit and Roll Back](commit-and-rollback.md) adds a
second row and undoes it.

### Clean up

Removing the tutorial table and the Docker lab, and recovering from an
interrupted or repeated run, is in [Finish Safely](finish-safely.md).

---

## If the first connection fails

These are the messages pycubrid 1.13.0 printed for each mistake against the
CUBRID 11.4 lab (the last line of the traceback):

| Message | What it means | What to check |
|---|---|---|
| `ModuleNotFoundError: No module named 'pycubrid'` | This Python is not the one pycubrid was installed into. | Activate the virtual environment from Step 1 (`source .venv/bin/activate`) and run `python`, not a system `python3`. |
| `OperationalError: failed to connect to CUBRID broker` | Nothing answered at that host and port. | The server is running (for the lab: `docker ps` lists `pycubrid-quickstart`), the host and port are right, and no firewall blocks the port. |
| `DatabaseError: Failed to connect to database server, 'nosuchdb', ...` | The broker answered, but there is no running database with that name. | The database name; ask the administrator whether that database is started. |
| `OperationalError: User "nobody" is invalid.` | The user does not exist in that database. | The user name. |
| `OperationalError: Incorrect or missing password.` | The user exists, but the password is wrong. | The password. |

Fix the value instead of working around it: do not switch to a production
`dba` account, and do not turn off TLS certificate checks to make an error go
away. More cases are in [Troubleshooting](TROUBLESHOOTING.md#connection-issues).

---

## Recorded walkthrough

<video controls preload="metadata" playsinline width="100%">
  <!-- docs-check: ignore --> <!-- MkDocs serves this page one level down and does not rewrite raw HTML, so this path is relative to the site URL, not the file. -->
  <source src="../assets/videos/pycubrid-demo.mp4" type="video/mp4">
  Your browser cannot play this video. Use the download link below.
</video>

[Download the sync/async CRUD walkthrough](assets/videos/pycubrid-demo.mp4).
The recording uses a local source-built wheel, not a published PyPI release,
and an isolated CUBRID server. It verifies query values, parameterized CRUD,
context-manager commit observed on another connection, async queries and cleanup.
See the [editable sources and recording provenance](https://github.com/cubrid-lab/pycubrid/tree/main/demos).
This is a recorded example, not a full compatibility or performance certificate.

---

## Next steps

- [Examples](EXAMPLES.md): updates, deletes, `with` blocks, async code and LOBs.
- [Connection Options](CONNECTION.md): every connection option, URLs, timeouts and TLS.
- [Troubleshooting](TROUBLESHOOTING.md) and [FAQ](faq.md): more connection, query and
  transaction problems.
- Reference: [API Reference](API_REFERENCE.md), [Type Mapping](TYPES.md),
  [Passing Values Safely (Parameter Binding)](PARAMETER_BINDING.md) and
  [Support Matrix](SUPPORT_MATRIX.md). For contributors, the wire protocol itself is in
  [Protocol](PROTOCOL.md).
