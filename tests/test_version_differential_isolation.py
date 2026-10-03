"""Offline cover for the version-differential session isolation (#614).

``tests/test_version_differential.py`` needs four live CUBRID servers, so its
own lane cannot exercise what happens *after* a statement kills the session.
That gap is why a single fatal statement on 10.2 was reported as sixteen
failures, and why the report named only 10.2 — the shared session died before
the other versions reached the statement.

These tests drive ``Server`` against a scripted connection instead of a broker:
one statement kills the session, and the next workload has to run on a fresh
one. The contract that losing a session fails its own test is asserted too, so
the isolation cannot be mistaken for swallowing the defect.
"""

from __future__ import annotations

from typing import Any

import pytest

import pycubrid
from pycubrid.exceptions import Error as DBAPIError
from pycubrid.exceptions import OperationalError

from .helpers.version_matrix import Endpoint

version_differential = pytest.importorskip("tests.test_version_differential")
Server = version_differential.Server
SessionLost = version_differential.SessionLost
Stmt = version_differential.Stmt
Workload = version_differential.Workload
compare = version_differential.compare
normalize_value = version_differential.normalize_value

#: What ``Server._observe`` records for the scripted ``SELECT 1`` probe. Built
#: from ``normalize_value`` rather than spelled out, so these tests do not pin
#: the differential report's value format.
ALIVE_ROWS = ((normalize_value(1),),)


class _DeadSession(OperationalError):
    """Stands in for pycubrid's "connection lost during receive"."""


class _FakeCursor:
    def __init__(self, conn: _FakeConnection) -> None:
        self._conn = conn
        self._rows: list[tuple[Any, ...]] = []
        self.rowcount = -1
        self.lastrowid = None
        self.description: tuple[tuple[Any, ...], ...] | None = None

    def execute(self, sql: str, params: Any = None) -> None:
        if not self._conn.alive:
            raise _DeadSession("connection is closed")
        self._conn.executed.append(sql)
        if sql in self._conn.kills:
            self._conn.alive = False
            raise _DeadSession("connection lost during receive")
        if sql == "SELECT 1":
            self._rows = [(1,)]
            self.description = (("1", None, None, None, None, None, None),)
            return
        self._rows = []
        self.description = None

    def executemany(self, sql: str, params: Any) -> None:
        self.execute(sql, params)

    def fetchone(self) -> tuple[Any, ...] | None:
        return self._rows[0] if self._rows else None

    def fetchall(self) -> list[tuple[Any, ...]]:
        return list(self._rows)

    def close(self) -> None:
        if not self._conn.alive:
            raise _DeadSession("connection is closed")


class _FakeConnection:
    def __init__(self, serial: int, kills: set[str], version: str) -> None:
        self.serial = serial
        self.kills = kills
        self.version = version
        self.alive = True
        self.autocommit = False
        self.executed: list[str] = []

    def cursor(self) -> _FakeCursor:
        if not self.alive:
            raise _DeadSession("connection is closed")
        return _FakeCursor(self)

    def get_server_version(self) -> str:
        return self.version

    def ping(self, reconnect: bool = True) -> bool:
        if reconnect and not self.alive:
            self.alive = True
        return self.alive

    def close(self) -> None:
        if not self.alive:
            raise _DeadSession("connection is closed")
        self.alive = False


class _Broker:
    """Hands out fake connections and remembers every one it opened."""

    def __init__(self, kills: set[str], version: str = "11.4.6.1963") -> None:
        self.kills = kills
        self.version = version
        self.opened: list[_FakeConnection] = []

    def connect(self, **_kwargs: Any) -> _FakeConnection:
        conn = _FakeConnection(len(self.opened) + 1, self.kills, self.version)
        self.opened.append(conn)
        return conn


FATAL_SQL = "SELECT IF(1=0, SET{1}, 0.000)"


@pytest.fixture
def broker(monkeypatch: pytest.MonkeyPatch) -> _Broker:
    created = _Broker(kills={FATAL_SQL})
    monkeypatch.setattr(pycubrid, "connect", created.connect)
    return created


@pytest.fixture
def server(broker: _Broker) -> Any:
    return Server(Endpoint(version="11.4", host="localhost", port=33000))


def test_a_fatal_statement_fails_its_own_test(server: Any, broker: _Broker) -> None:
    with pytest.raises(AssertionError) as caught:
        server.run(Workload(statements=[Stmt(sql=FATAL_SQL)]))

    message = str(caught.value)
    assert "left the session unusable" in message
    assert FATAL_SQL in message
    assert "Reopening the session also failed" not in message


def test_the_next_workload_runs_on_a_fresh_session(server: Any, broker: _Broker) -> None:
    with pytest.raises(AssertionError):
        server.run(Workload(statements=[Stmt(sql=FATAL_SQL)]))

    # Before #614 this raised InterfaceError("connection is closed") instead,
    # and every later test in the module failed the same way.
    observed = server.run(Workload(statements=[Stmt(sql="SELECT 1")]))

    assert observed == [
        {
            "rowcount": -1,
            "lastrowid": None,
            "description": (("1", None, None, None, None, None, None),),
            "rows": ALIVE_ROWS,
        }
    ]
    assert len(broker.opened) == 2, "the dead session should have been replaced exactly once"
    assert server.open_sessions == 2
    assert server.conn is broker.opened[-1]
    assert broker.opened[0].alive is False


def test_repeated_fatal_statements_each_fail_once(server: Any, broker: _Broker) -> None:
    for _ in range(3):
        with pytest.raises(AssertionError, match="left the session unusable"):
            server.run(Workload(statements=[Stmt(sql=FATAL_SQL)]))
        revived = server.run(Workload(statements=[Stmt(sql="SELECT 1")]))
        assert revived[0]["rows"] == ALIVE_ROWS

    assert len(broker.opened) == 4, "one initial session plus one replacement per kill"


def test_a_surviving_error_does_not_replace_the_session(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    broker = _Broker(kills=set())
    monkeypatch.setattr(pycubrid, "connect", broker.connect)
    server = Server(Endpoint(version="11.4", host="localhost", port=33000))

    raising = "SELECT syntax error"
    original = _FakeCursor.execute

    def execute(self: _FakeCursor, sql: str, params: Any = None) -> None:
        if sql == raising:
            raise DBAPIError("syntax error")
        original(self, sql, params)

    monkeypatch.setattr(_FakeCursor, "execute", execute)

    observed = server.run(Workload(statements=[Stmt(sql=raising)]))

    assert observed[0]["error"][0] == "Error"
    assert len(broker.opened) == 1, "a session that survived must not be reopened"
    assert server.open_sessions == 1


def test_scratch_tables_are_not_recreated_after_a_reopen(server: Any, broker: _Broker) -> None:
    server.ensure_table("t_isolation", "CREATE TABLE %s (a INT)")
    assert server.created == ["t_isolation"]

    with pytest.raises(AssertionError):
        server.run(Workload(statements=[Stmt(sql=FATAL_SQL)]))

    before = list(broker.opened[-1].executed)
    server.ensure_table("t_isolation", "CREATE TABLE %s (a INT)")
    assert broker.opened[-1].executed == before, "the table already exists on the server"


# ---------------------------------------------------------------------------
# Attribution across the matrix (#614)
#
# Session recovery alone does not fix *which* versions a fatal statement is
# reported against: compare() used to build its observations in a dict
# comprehension, so the first endpoint to lose its session aborted the rest and
# the report named only that version.
# ---------------------------------------------------------------------------

MATRIX = ("10.2", "11.0", "11.2", "11.4")


def _matrix(monkeypatch: pytest.MonkeyPatch, kills: set[str]) -> tuple[list[Any], _Broker]:
    broker = _Broker(kills=kills)
    monkeypatch.setattr(pycubrid, "connect", broker.connect)
    servers = []
    for version in MATRIX:
        broker.version = f"{version}.0"
        servers.append(Server(Endpoint(version=version, host="localhost", port=33000)))
    return servers, broker


def test_every_endpoint_runs_when_one_loses_its_session(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    servers, broker = _matrix(monkeypatch, kills={FATAL_SQL})

    with pytest.raises(AssertionError) as caught:
        compare(servers, Workload(statements=[Stmt(sql=FATAL_SQL)]))

    message = str(caught.value)
    for version in MATRIX:
        assert version in message, f"{version} is missing from the report"
    # Every endpoint attempted the statement, not just the first one.
    for server in servers:
        assert any(FATAL_SQL in sql for conn in broker.opened for sql in conn.executed)
        assert server.open_sessions == 2, f"{server.version} should have been reopened once"


def test_only_the_affected_versions_are_named(monkeypatch: pytest.MonkeyPatch) -> None:
    """A statement fatal on one version must not be reported against the others."""
    broker = _Broker(kills=set())
    monkeypatch.setattr(pycubrid, "connect", broker.connect)
    servers = []
    for version in MATRIX:
        broker.version = f"{version}.0"
        servers.append(Server(Endpoint(version=version, host="localhost", port=33000)))
    # Only 11.2's connections treat the statement as fatal.
    for conn in broker.opened:
        if conn.version.startswith("11.2"):
            conn.kills = {FATAL_SQL}

    with pytest.raises(AssertionError) as caught:
        compare(servers, Workload(statements=[Stmt(sql=FATAL_SQL)]))

    message = str(caught.value)
    assert "11.2" in message
    assert "completed: 10.2, 11.0, 11.4" in message


def test_a_matrix_with_no_fatal_statement_reaches_the_comparison(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    servers, broker = _matrix(monkeypatch, kills=set())
    # Identical observations on every endpoint, so classify() finds no divergence.
    compare(servers, Workload(statements=[Stmt(sql="SELECT 1")]))
    assert [s.open_sessions for s in servers] == [1, 1, 1, 1]


def test_an_unreported_dead_session_fails_visibly(server: Any, broker: _Broker) -> None:
    """A session that dies outside a statement must not be healed in silence."""
    broker.opened[-1].alive = False

    with pytest.raises(SessionLost, match="already unusable before this workload ran"):
        server.run(Workload(statements=[Stmt(sql="SELECT 1")]))

    # It is reported *and* repaired, so the next workload still runs.
    assert server.run(Workload(statements=[Stmt(sql="SELECT 1")]))[0]["rows"] == ALIVE_ROWS
    assert server.open_sessions == 2


def test_auto_reconnecting_sql_cannot_hide_a_dead_prior_session(
    server: Any, broker: _Broker, monkeypatch: pytest.MonkeyPatch
) -> None:
    dead = broker.opened[-1]
    dead.alive = False
    original_cursor = _FakeConnection.cursor
    implicit_reconnects: list[int] = []

    def auto_reconnecting_cursor(self: _FakeConnection) -> _FakeCursor:
        if not self.alive:
            implicit_reconnects.append(self.serial)
            self.alive = True
        return original_cursor(self)

    monkeypatch.setattr(_FakeConnection, "cursor", auto_reconnecting_cursor)
    with pytest.raises(SessionLost, match="already unusable before this workload ran"):
        server.run(Workload(statements=[Stmt(sql="SELECT 1")]))

    assert not dead.executed, "the dead session must be observed before SQL can reconnect it"
    assert not implicit_reconnects
    assert server.open_sessions == 2
    assert server.conn is broker.opened[-1] and server.conn is not dead
    assert server.run(Workload(statements=[Stmt(sql="SELECT 1")]))[0]["rows"] == ALIVE_ROWS
