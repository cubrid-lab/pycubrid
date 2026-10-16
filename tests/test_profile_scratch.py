"""Scratch-table ownership of the execute and fetch profilers (#950).

The profilers must never drop or reuse a table they did not create, and must
release the table, cursor and connection they own on success and on failure.
These offline tests drive the actual ``scripts/profile_execute.py`` and
``scripts/profile_fetch.py`` modules with a recording fake connection.
"""

from __future__ import annotations

import importlib.util
import re
import sys
from collections.abc import Callable
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest

import pycubrid
from pycubrid.exceptions import OperationalError, ProgrammingError

_SCRIPTS = Path(__file__).resolve().parent.parent / "scripts"


def _load(name: str) -> ModuleType:
    spec = importlib.util.spec_from_file_location(f"_pycubrid_{name}", _SCRIPTS / f"{name}.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))
scratch = importlib.import_module("_profile_scratch")
profile_execute = _load("profile_execute")
profile_fetch = _load("profile_fetch")

SENTINEL = "_profile_tmp"
_CREATE = re.compile(r"CREATE TABLE (\w+) ")
_DROP = re.compile(r"DROP TABLE(?: IF EXISTS)? (\w+)$")


class _Database:
    """Tables shared by every fake connection, plus a log of all calls."""

    def __init__(self, tables: set[str] | None = None) -> None:
        self.tables = set(tables or ())
        self.log: list[tuple[str, Any]] = []


class _FakeCursor:
    def __init__(self, conn: _FakeConnection) -> None:
        self.conn = conn
        self.closed = False

    def execute(self, sql: str, params: Any = None) -> None:
        db = self.conn.db
        db.log.append(("execute", (sql, params)))
        self.conn.fail("execute", sql)
        created = _CREATE.match(sql)
        if created:
            if created.group(1) in db.tables:
                raise ProgrammingError(f"table {created.group(1)} already exists", -1)
            db.tables.add(created.group(1))
        dropped = _DROP.match(sql)
        if dropped:
            db.tables.discard(dropped.group(1))

    def executemany(self, sql: str, params: Any) -> None:
        self.conn.db.log.append(("executemany", (sql, list(params))))

    def fetchall(self) -> list[Any]:
        self.conn.fail("fetchall", "")
        return []

    def fetchone(self) -> None:
        return None

    def fetchmany(self, size: int) -> list[Any]:
        return []

    def close(self) -> None:
        self.conn.db.log.append(("cursor.close", None))
        self.conn.fail("cursor.close", "")
        self.closed = True


class _FakeConnection:
    def __init__(self, db: _Database, failures: dict[str, Callable[[str], bool]]) -> None:
        self.db = db
        self.failures = failures
        self.closed = False

    def fail(self, step: str, sql: str) -> None:
        check = self.failures.get(step)
        if check is not None and check(sql):
            raise OperationalError(f"injected {step} failure", -1)

    def cursor(self) -> _FakeCursor:
        self.db.log.append(("cursor", None))
        self.fail("cursor", "")
        return _FakeCursor(self)

    def commit(self) -> None:
        self.db.log.append(("commit", None))
        self.fail("commit", "")

    def rollback(self) -> None:
        self.db.log.append(("rollback", None))
        self.fail("rollback", "")

    def close(self) -> None:
        self.db.log.append(("close", None))
        self.closed = True
        self.fail("close", "")


def _install(
    monkeypatch: pytest.MonkeyPatch,
    db: _Database,
    **failures: Callable[[str], bool],
) -> list[_FakeConnection]:
    conns: list[_FakeConnection] = []

    def connect(**_: Any) -> _FakeConnection:
        conn = _FakeConnection(db, failures)
        conns.append(conn)
        return conn

    monkeypatch.setattr(pycubrid, "connect", connect)
    return conns


_TARGET = {"host": "h", "port": 1, "database": "d", "user": "u", "password": ""}


def _run_execute(operation: str = "select") -> None:
    profile_execute.run_execute_cycle(**_TARGET, iterations=2, operation=operation)


def _run_fetch() -> None:
    profile_fetch.run_fetch_cycle(**_TARGET, row_count=3, iterations=2, fetch_size=2)


RUNS = [
    pytest.param(_run_execute, id="execute-select"),
    pytest.param(lambda: _run_execute("all"), id="execute-all"),
    pytest.param(_run_fetch, id="fetch"),
]


def _sql(db: _Database) -> list[str]:
    return [entry[1][0] for entry in db.log if entry[0] in ("execute", "executemany")]


def _created(db: _Database) -> list[str]:
    return [m.group(1) for sql in _sql(db) if (m := _CREATE.match(sql))]


def _dropped(db: _Database) -> list[str]:
    return [m.group(1) for sql in _sql(db) if (m := _DROP.match(sql))]


# --- ownership -----------------------------------------------------------------


@pytest.mark.parametrize("run", RUNS)
def test_existing_scratch_table_is_never_dropped(
    monkeypatch: pytest.MonkeyPatch, run: Callable[[], None]
) -> None:
    db = _Database({SENTINEL})
    conns = _install(monkeypatch, db)

    run()

    assert db.tables == {SENTINEL}
    assert SENTINEL not in _dropped(db)
    (table,) = _created(db)
    assert table != SENTINEL and table.startswith(scratch.TABLE_PREFIX)
    assert _dropped(db) == [table]
    # Every workload statement targets the run's own table.
    assert all(table in sql for sql in _sql(db))
    assert conns[0].closed


@pytest.mark.parametrize("run", RUNS)
def test_create_collision_issues_no_drop(
    monkeypatch: pytest.MonkeyPatch, run: Callable[[], None]
) -> None:
    other = scratch.TABLE_PREFIX + "0123456789ab"
    db = _Database({other})
    conns = _install(monkeypatch, db)
    monkeypatch.setattr(scratch, "new_table_name", lambda: other)

    with pytest.raises(ProgrammingError, match="already exists"):
        run()

    assert db.tables == {other}
    assert _dropped(db) == []
    assert ("cursor.close", None) in db.log
    assert conns[0].closed


@pytest.mark.parametrize("run", RUNS)
def test_failed_create_issues_no_drop(
    monkeypatch: pytest.MonkeyPatch, run: Callable[[], None]
) -> None:
    db = _Database()
    _install(monkeypatch, db, execute=lambda sql: sql.startswith("CREATE"))

    with pytest.raises(OperationalError, match="injected execute"):
        run()

    assert _dropped(db) == []


def test_interleaved_runs_drop_only_their_own_table(monkeypatch: pytest.MonkeyPatch) -> None:
    db = _Database()
    _install(monkeypatch, db)
    seen: dict[str, Any] = {}
    original = profile_fetch.run_fetchall

    def fetchall_with_a_nested_run(cursor: Any, iterations: int, **kwargs: Any) -> None:
        (outer,) = db.tables
        _run_execute("select")  # a second invocation starts and finishes meanwhile
        seen["outer_survived"] = outer in db.tables
        original(cursor, iterations, **kwargs)

    monkeypatch.setattr(profile_fetch, "run_fetchall", fetchall_with_a_nested_run)

    _run_fetch()

    outer, inner = _created(db)
    assert outer != inner
    assert seen["outer_survived"]
    assert _dropped(db) == [inner, outer]
    assert db.tables == set()


# --- release on success and failure ---------------------------------------------


@pytest.mark.parametrize("run", RUNS)
def test_success_releases_table_cursor_and_connection(
    monkeypatch: pytest.MonkeyPatch, run: Callable[[], None]
) -> None:
    db = _Database()
    _install(monkeypatch, db)

    run()

    assert db.tables == set()
    assert [step for step, _ in db.log[-4:]] == ["execute", "commit", "cursor.close", "close"]
    assert ("rollback", None) not in db.log


def test_workload_failure_still_tears_down(monkeypatch: pytest.MonkeyPatch) -> None:
    db = _Database()
    _install(monkeypatch, db, fetchall=lambda _sql: True)

    with pytest.raises(OperationalError, match="injected fetchall") as info:
        _run_execute("select")

    assert not getattr(info.value, "__notes__", [])
    assert db.tables == set()
    steps = [step for step, _ in db.log]
    assert steps[-5:] == ["rollback", "execute", "commit", "cursor.close", "close"]


def test_cursor_acquisition_failure_closes_connection(monkeypatch: pytest.MonkeyPatch) -> None:
    db = _Database()
    conns = _install(monkeypatch, db, cursor=lambda _sql: True)

    with pytest.raises(OperationalError, match="injected cursor"):
        _run_fetch()

    assert _sql(db) == []
    assert conns[0].closed


def test_teardown_failure_after_workload_failure_keeps_primary(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db = _Database()
    _install(
        monkeypatch,
        db,
        fetchall=lambda _sql: True,
        execute=lambda sql: sql.startswith("DROP"),
        close=lambda _sql: True,
    )

    with pytest.raises(OperationalError, match="injected fetchall") as info:
        _run_execute("select")

    notes = info.value.__notes__
    assert any("drop _profile_tmp_" in note for note in notes)
    assert any("connection close" in note for note in notes)
    assert ("cursor.close", None) in db.log


@pytest.mark.parametrize(
    ("failures", "step"),
    [
        ({"execute": lambda sql: sql.startswith("DROP")}, "drop _profile_tmp_"),
        ({"cursor.close": lambda _sql: True}, "cursor close"),
        ({"close": lambda _sql: True}, "connection close"),
    ],
    ids=["drop", "cursor-close", "connection-close"],
)
def test_cleanup_only_failure_is_not_reported_as_success(
    monkeypatch: pytest.MonkeyPatch, failures: dict[str, Callable[[str], bool]], step: str
) -> None:
    db = _Database()
    conns = _install(monkeypatch, db, **failures)

    with pytest.raises(scratch.ScratchCleanupError, match=step):
        _run_fetch()

    assert conns[0].closed


# --- bounded identifiers and unchanged binding ----------------------------------


def test_table_names_are_validated() -> None:
    name = scratch.new_table_name()
    assert re.fullmatch(r"_profile_tmp_[0-9a-f]{12}", name)
    assert scratch.new_table_name() != name
    for bad in ("_profile_tmp", "_profile_tmp_0123456789aB", "x; DROP TABLE t", "_profile_tmp_1"):
        with pytest.raises(ValueError):
            scratch.table_sql("SELECT * FROM {table}", bad)


def test_workload_values_stay_bound_parameters(monkeypatch: pytest.MonkeyPatch) -> None:
    db = _Database()
    _install(monkeypatch, db)

    _run_execute("insert")

    (table,) = _created(db)
    inserts = [entry[1] for entry in db.log if entry[0] == "execute" and "INSERT" in entry[1][0]]
    assert inserts == [
        (f"INSERT INTO {table} (val) VALUES (?)", ("row-0",)),
        (f"INSERT INTO {table} (val) VALUES (?)", ("row-1",)),
    ]


# --- no reconnect during teardown (real Connection, replay broker) --------------


def test_teardown_on_a_lost_session_reports_failure_without_reconnecting() -> None:
    """The CAS drops the session after the workload's final commit (OUT_TRAN).

    Without the teardown guard, the DROP's CHECK_CAS probe fails and the driver
    opens a replacement session to run it; the cleanup must fail instead.
    """
    from .helpers.replay_broker import OUT_TRAN, Reply, Request, Session, ok_body
    from .helpers.replay_broker import run_replay_broker

    last_sql: list[str] = [""]

    def script(request: Request, session: Session) -> Reply | None:
        if request.sql:
            last_sql[0] = request.sql
        if request.function == "END_TRAN" and last_sql[0].startswith("SELECT id, val"):
            # The workload's final commit succeeds, then the CAS drops the session.
            session.status = OUT_TRAN
            return Reply(ok_body(OUT_TRAN), close=True)
        return None

    with run_replay_broker(script) as broker:
        with pytest.raises(scratch.ScratchCleanupError, match="drop _profile_tmp_"):
            profile_execute.run_execute_cycle(
                host="127.0.0.1",
                port=broker.port,
                database="testdb",
                user="dba",
                password="",
                iterations=1,
                operation="select",
            )
        requests = list(broker.requests)

    # No replacement session, and nothing reached the broker after the session
    # was dropped: no CHECK_CAS probe, DROP, rollback or replayed statement.
    assert {r.session for r in requests} == {0}
    assert requests[-1].function == "END_TRAN"
    assert not any((r.sql or "").startswith("DROP") for r in requests)
