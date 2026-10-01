"""Deferred CLOSE_REQ for released cursor handles in autocommit mode (#488).

In autocommit mode a released handle (``cursor.close()``, re-``execute()``, or a
cursor collected without ``close()``) is not closed with its own CLOSE_REQ: its
id rides on the next ``PREPARE_AND_EXECUTE`` as a prepare argument after the
auto-commit flag, which CAS frees before preparing (``fn_prepare_internal``, the
wire mechanism of JDBC's deferred close; JDBC itself defers only statements
without a result set). That saves the CLOSE_REQ and the CHECK_CAS probe its
OUT_TRAN predecessor would need, and lets collected cursors release their handles
at all; ``commit()``/``rollback()`` close any still queued with CLOSE_REQ.

The replay broker allocates the lowest free handle id, as CAS does, so an id
released too late or on the wrong session would collide with a live result.

Only sessions whose broker reports statement pooling defer: without pooling CAS
frees handles at every commit, so a later id could name a new result. Queued
ids belong to one physical session and are dropped, never sent, when it is
replaced.

Like the #557 round-trip budgets in ``tests/test_replay_parity.py`` (which
cover the pooling-on INSERT budgets), every scenario here asserts its exact
request sequence, not only an upper bound.
"""

from __future__ import annotations

import asyncio
import gc
import sys
from collections.abc import Callable
from typing import Any
from unittest.mock import patch

import pytest

import pycubrid
import pycubrid.aio
import pycubrid._connection_common as _connection_common
from pycubrid.aio.cursor import AsyncCursor
from pycubrid.constants import CUBRIDDataType
from pycubrid.cursor import Cursor

from .helpers.cas_reply import Column, ResultSet, int_
from .helpers.fault_broker import build_open_db_body, framed
from .helpers.replay_broker import (
    OUT_TRAN,
    ReplayBroker,
    Reply,
    Request,
    Session,
    run_replay_broker,
    with_status,
)

_TIMEOUT = 5.0
POOLING_ON = 1


class _Driver:
    """Runs the same blocking-style steps through the sync or async driver."""

    def __init__(
        self,
        asynchronous: bool,
        port: int,
        autocommit: bool,
        extra_options: dict[str, Any] | None = None,
    ) -> None:
        self.asynchronous = asynchronous
        self.loop = asyncio.new_event_loop() if asynchronous else None
        options: dict[str, Any] = {
            "host": "127.0.0.1",
            "port": port,
            "database": "testdb",
            "user": "dba",
            "password": "",
            "connect_timeout": _TIMEOUT,
            "read_timeout": _TIMEOUT,
            "no_backslash_escapes": True,
            "autocommit": autocommit,
        }
        options.update(extra_options or {})
        if asynchronous:
            self.conn = self.run(pycubrid.aio.connect(**options))
        else:
            self.conn = pycubrid.connect(**options)

    def run(self, value: Any) -> Any:
        if self.loop is not None and asyncio.iscoroutine(value):
            return self.loop.run_until_complete(value)
        return value

    def cursor(self) -> Any:
        return self.conn.cursor()

    def execute(self, cursor: Any, sql: str) -> None:
        self.run(cursor.execute(sql))

    def select(self, cursor: Any, sql: str = "SELECT 1") -> Any:
        self.run(cursor.execute(sql))
        return self.run(cursor.fetchall())

    def close(self, cursor: Any) -> None:
        self.run(cursor.close())

    def commit(self) -> None:
        self.run(self.conn.commit())

    def connect(self) -> None:
        self.run(self.conn.connect())

    def teardown(self) -> None:
        try:
            self.run(self.conn.close())
        finally:
            if self.loop is not None:
                self.loop.close()


def _autocommit_out_tran(holder: dict[str, ReplayBroker]) -> Callable[..., Reply | None]:
    """A real CAS ends an auto-committed statement OUT_TRAN."""

    def script(request: Request, session: Session) -> Reply | None:
        if request.function == "PREPARE_AND_EXECUTE" and request.args[3] == b"\x01":
            reply = holder["broker"].default_reply(request, session)
            session.status = OUT_TRAN
            assert reply.body is not None
            return Reply(with_status(reply.body, OUT_TRAN))
        return None

    return script


def _run(
    asynchronous: bool,
    steps: Callable[[_Driver], Any],
    *,
    autocommit: bool = True,
    pooling: int = POOLING_ON,
    extra: Callable[[Request, Session], Reply | None] | None = None,
    options: dict[str, Any] | None = None,
    from_start: bool = False,
    results: dict[str, Any] | None = None,
) -> tuple[list[Request], Any]:
    """Run ``steps`` against a fresh broker; return the requests after setup."""
    holder: dict[str, ReplayBroker] = {}
    base = _autocommit_out_tran(holder)

    def script(request: Request, session: Session) -> Reply | None:
        if extra is not None:
            reply = extra(request, session)
            if reply is not None:
                return reply
        return base(request, session)

    with run_replay_broker(script, results=results, statement_pooling=pooling) as broker:
        holder["broker"] = broker
        driver = _Driver(asynchronous, broker.port, autocommit, options)
        start = 0 if from_start else len(broker.requests)
        try:
            result = steps(driver)
        finally:
            requests = broker.requests[start:]
            driver.teardown()
    return requests, result


def _framed(requests: list[Request]) -> list[tuple[str, tuple[int, ...]]]:
    """(function, deferred close ids) of each request, up to the final close."""
    out = []
    for request in requests:
        if request.function == "CON_CLOSE":
            break
        out.append((request.function, request.deferred_closes))
    return out


ASYNC = pytest.mark.parametrize("asynchronous", [False, True], ids=["sync", "async"])
FC41 = "PREPARE_AND_EXECUTE"


@ASYNC
def test_select_then_close_costs_one_probe_and_no_close_request(asynchronous: bool) -> None:
    def steps(d: _Driver) -> list[int]:
        handles = []
        for _ in range(3):
            cursor = d.cursor()
            assert d.select(cursor) == [(1,)]
            handles.append(cursor._query_handle)
            d.close(cursor)
        return handles

    requests, handles = _run(asynchronous, steps)
    # Each SELECT-then-close: one CHECK_CAS (the reply before it was OUT_TRAN)
    # and the statement, which carries the previous cursor's handle.
    assert _framed(requests) == [
        ("CHECK_CAS", ()),
        (FC41, ()),
        ("CHECK_CAS", ()),
        (FC41, (handles[0],)),
        ("CHECK_CAS", ()),
        (FC41, (handles[1],)),
    ]


@ASYNC
def test_reexecute_releases_previous_handle_on_the_same_request(asynchronous: bool) -> None:
    def steps(d: _Driver) -> int:
        cursor = d.cursor()
        d.select(cursor)
        first = cursor._query_handle
        assert d.select(cursor, "SELECT 2") == [(1,)]
        return int(first)

    requests, first = _run(asynchronous, steps)
    assert _framed(requests) == [
        ("CHECK_CAS", ()),
        (FC41, ()),
        ("CHECK_CAS", ()),
        (FC41, (first,)),
    ]


@ASYNC
def test_unclosed_cursors_release_their_handles(asynchronous: bool) -> None:
    def steps(d: _Driver) -> None:
        for _ in range(50):
            cursor = d.cursor()
            d.select(cursor)
            del cursor
            gc.collect()

    requests, _ = _run(asynchronous, steps)
    framed = _framed(requests)
    assert [function for function, _ in framed] == ["CHECK_CAS", FC41] * 50
    # Each statement releases the previous cursor's handle (freed before the
    # prepare, so the CAS reuses that lowest free id): the table never grows.
    assert [deferred for function, deferred in framed if function == FC41] == [()] + [(1,)] * 49


@ASYNC
def test_queued_handles_are_never_sent_to_a_replacement_session(asynchronous: bool) -> None:
    def hang_up_probe(request: Request, session: Session) -> Reply | None:
        # The CAS recycles session 0 after the first statement.
        if (
            session.number == 0
            and request.function == "CHECK_CAS"
            and session.functions.count(FC41) == 1
        ):
            return Reply(close=True)
        return None

    def steps(d: _Driver) -> None:
        cursor = d.cursor()
        d.select(cursor)
        d.close(cursor)  # queued for session 0
        assert d.select(d.cursor(), "SELECT 2") == [(1,)]

    requests, _ = _run(asynchronous, steps, extra=hang_up_probe)
    sessions = {r.session for r in requests}
    assert sessions == {0, 1}
    replacement = [(r.function, r.deferred_closes) for r in requests if r.session == 1]
    assert all(deferred == () for _, deferred in replacement)
    assert [r.sql for r in requests if r.function == FC41] == ["SELECT 1", "SELECT 2"]
    assert "CLOSE_REQ_HANDLE" not in [r.function for r in requests]


@ASYNC
def test_pooling_off_completed_results_need_no_stale_close(asynchronous: bool) -> None:
    """The autocommitting reply already freed the handle (#584)."""

    def steps(d: _Driver) -> None:
        cursor = d.cursor()
        d.select(cursor)
        assert cursor._query_handle is None
        d.close(cursor)
        dropped = d.cursor()
        d.select(dropped)
        del dropped
        gc.collect()
        d.select(d.cursor(), "SELECT 2")

    requests, _ = _run(asynchronous, steps, pooling=0)
    assert _framed(requests) == [
        ("CHECK_CAS", ()),
        (FC41, ()),
        ("CHECK_CAS", ()),
        (FC41, ()),
        # Each complete result was freed by its own autocommit reply.
        ("CHECK_CAS", ()),
        (FC41, ()),
    ]


@ASYNC
def test_manual_commit_closes_explicitly_and_releases_collected_handles(
    asynchronous: bool,
) -> None:
    def steps(d: _Driver) -> int:
        cursor = d.cursor()
        d.select(cursor)
        d.close(cursor)  # manual-commit close: sent now, as before
        dropped = d.cursor()
        d.select(dropped)
        handle = int(dropped._query_handle)
        del dropped
        gc.collect()
        d.select(d.cursor(), "SELECT 2")
        return handle

    requests, dropped_handle = _run(asynchronous, steps, autocommit=False)
    assert _framed(requests) == [
        (FC41, ()),
        ("CLOSE_REQ_HANDLE", ()),
        (FC41, ()),
        (FC41, (dropped_handle,)),
    ]


@ASYNC
def test_queue_is_bounded(asynchronous: bool, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(_connection_common, "DEFERRED_CLOSE_LIMIT", 2)

    def steps(d: _Driver) -> list[int]:
        cursors = [d.cursor() for _ in range(3)]
        for cursor in cursors:
            d.select(cursor)
        handles = [int(cursor._query_handle) for cursor in cursors]
        for cursor in cursors:
            d.close(cursor)
        assert len(d.conn._deferred_closes) == 2
        d.select(d.cursor(), "SELECT 2")
        return handles

    requests, handles = _run(asynchronous, steps)
    assert _framed(requests) == [
        ("CHECK_CAS", ()),
        (FC41, ()),
        ("CHECK_CAS", ()),
        (FC41, ()),
        ("CHECK_CAS", ()),
        (FC41, ()),
        # Queue full: the third close is sent at once.
        ("CHECK_CAS", ()),
        ("CLOSE_REQ_HANDLE", ()),
        ("CHECK_CAS", ()),
        (FC41, (handles[0], handles[1])),
    ]


@ASYNC
def test_request_failing_before_send_keeps_the_queue(asynchronous: bool) -> None:
    def steps(d: _Driver) -> int:
        cursor = d.cursor()
        d.select(cursor)
        handle = int(cursor._query_handle)
        d.close(cursor)
        with pytest.raises(pycubrid.DataError):
            d.execute(d.cursor(), "SELECT '\udcff'")  # unencodable: nothing sent
        d.select(d.cursor(), "SELECT 2")
        return handle

    requests, handle = _run(asynchronous, steps)
    framed = _framed(requests)
    assert framed[-1] == (FC41, (handle,))
    assert [function for function, _ in framed].count(FC41) == 2


@ASYNC
def test_transport_failure_drops_the_queue(asynchronous: bool) -> None:
    def no_reply(request: Request, session: Session) -> Reply | None:
        if session.number == 0 and request.sql == "SELECT 2":
            return Reply(close=True)
        return None

    def steps(d: _Driver) -> None:
        cursor = d.cursor()
        d.select(cursor)
        d.close(cursor)
        with pytest.raises(pycubrid.OperationalError):
            d.select(d.cursor(), "SELECT 2")
        assert d.conn._deferred_closes == []
        d.connect()
        d.select(d.cursor(), "SELECT 3")

    requests, _ = _run(asynchronous, steps, extra=no_reply)
    statements = [(r.session, r.sql, r.deferred_closes) for r in requests if r.function == FC41]
    assert statements == [
        (0, "SELECT 1", ()),
        (0, "SELECT 2", (1,)),  # uncertain: sent once, never replayed
        (1, "SELECT 3", ()),
    ]


@ASYNC
def test_commit_closes_queued_handles(asynchronous: bool) -> None:
    def steps(d: _Driver) -> int:
        cursor = d.cursor()
        d.select(cursor)
        handle = int(cursor._query_handle)
        d.close(cursor)
        d.commit()
        d.select(d.cursor(), "SELECT 2")
        return handle

    requests, handle = _run(asynchronous, steps)
    assert _framed(requests) == [
        ("CHECK_CAS", ()),
        (FC41, ()),
        ("CHECK_CAS", ()),
        ("CLOSE_REQ_HANDLE", ()),
        ("CHECK_CAS", ()),  # the CLOSE_REQ reply is OUT_TRAN, as before #488
        ("END_TRAN", ()),
        ("CHECK_CAS", ()),
        (FC41, ()),
    ]
    assert [r.int_arg(0) for r in requests if r.function == "CLOSE_REQ_HANDLE"] == [handle]


def _state(monkeypatch: pytest.MonkeyPatch) -> Any:
    monkeypatch.setattr(_connection_common, "DEFERRED_CLOSE_LIMIT", 2)
    state = _connection_common.ConnectionCommonMixin()
    state._init_common_state(host="h", port=1, database="d", user="u", password="")
    state._connected = True
    state._statement_pooling = POOLING_ON
    state._broker_db_type = 1
    state._physical_generation = 2
    return state


class _Dropped:
    def __init__(self, handle: int | None) -> None:
        self._query_handle = handle
        self._handle_generation = 2


def test_collected_cursors_beyond_the_bound_are_queued_and_sent_in_batches(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = _state(monkeypatch)
    for handle in (5, None, 6, 7):
        state._defer_dropped_cursor_close(_Dropped(handle))
    assert state._deferred_closes == [(2, 5), (2, 6), (2, 7)]
    assert state._peek_deferred_closes() == (2, (5, 6))  # at most the limit per request
    state._consume_deferred_closes(2)
    assert state._take_deferred_closes() == (7,)
    assert state._deferred_closes == []
    state._statement_pooling = 0
    state._deferred_closes[:] = [(2, 8)]
    assert state._take_deferred_closes() == ()
    assert state._deferred_closes == []


def test_entries_of_an_earlier_session_are_taken_but_not_sent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = _state(monkeypatch)
    state._deferred_closes[:] = [(1, 3), (2, 4)]
    assert state._peek_deferred_closes() == (2, (4,))
    state._statement_pooling = 0
    assert state._peek_deferred_closes() == (2, ())
    state._consume_deferred_closes(2)
    assert state._deferred_closes == []


@ASYNC
def test_cursor_collected_after_a_reconnect_keeps_its_own_generation(asynchronous: bool) -> None:
    """A cursor collected in a reference cycle has left ``_cursors`` first, so a
    retirement in between does not clear its handle; it must not reach the new
    session."""

    def steps(d: _Driver) -> None:
        cursor = d.cursor()
        d.select(cursor)
        assert cursor._query_handle is not None
        d.conn._cursors.discard(cursor)  # what the cycle collector does first
        d.conn._drop_connection()
        d.connect()  # session 1
        del cursor
        gc.collect()
        d.select(d.cursor(), "SELECT 2")

    requests, _ = _run(asynchronous, steps)
    assert [(r.session, r.sql, r.deferred_closes) for r in requests if r.function == FC41] == [
        (0, "SELECT 1", ()),
        (1, "SELECT 2", ()),
    ]


@ASYNC
def test_shard_proxy_closes_immediately(asynchronous: bool) -> None:
    """A shard proxy (db_type 4) ignores deferred-close prepare arguments."""

    def proxy(request: Request, session: Session) -> Reply | None:
        if request.function == "OPEN_DB":
            body = build_open_db_body(
                cas_info=bytes((OUT_TRAN, 0, 0, 0)), statement_pooling=POOLING_ON, db_type=4
            )
            return Reply(raw=framed(body))
        return None

    def steps(d: _Driver) -> None:
        cursor = d.cursor()
        d.select(cursor)
        d.close(cursor)

    requests, _ = _run(asynchronous, steps, extra=proxy)
    assert _framed(requests) == [
        ("CHECK_CAS", ()),
        (FC41, ()),
        ("CHECK_CAS", ()),
        ("CLOSE_REQ_HANDLE", ()),
    ]


@pytest.mark.no_escape_pin
@ASYNC
def test_escape_probe_of_a_replacement_session_closes_its_handle(asynchronous: bool) -> None:
    """Session setup runs in manual mode on the CAS: its probe is never deferred."""

    def recycle_after_first_statement(request: Request, session: Session) -> Reply | None:
        # Session 0 ran the escape probe and SELECT 1; then the CAS goes away.
        if (
            session.number == 0
            and request.function == "CHECK_CAS"
            and session.functions.count(FC41) == 2
        ):
            return Reply(close=True)
        return None

    def steps(d: _Driver) -> None:
        d.select(d.cursor())
        d.select(d.cursor(), "SELECT 2")

    requests, _ = _run(
        asynchronous,
        steps,
        extra=recycle_after_first_statement,
        options={"no_backslash_escapes": None},
        from_start=True,
    )
    for number in (0, 1):
        session = [r for r in requests if r.session == number]
        probe = [i for i, r in enumerate(session) if (r.sql or "").startswith("SELECT CHAR_")]
        assert len(probe) == 1, [r.function for r in session]
        after = [r.function for r in session[probe[0] + 1 : probe[0] + 3]]
        assert after == ["CLOSE_REQ_HANDLE", "END_TRAN"], [r.function for r in session]
    statements = [
        (r.session, r.sql, r.deferred_closes)
        for r in requests
        if r.function == FC41 and not (r.sql or "").startswith("SELECT CHAR_")
    ]
    assert statements == [(0, "SELECT 1", ()), (1, "SELECT 2", ())]


@ASYNC
@pytest.mark.parametrize("boundary", ["commit", "rollback"])
def test_boundary_closes_every_dropped_cursor_beyond_the_bound(
    asynchronous: bool, boundary: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Manual commit, pooling on: dropping more cursors than the per-request
    bound and ending the transaction leaves no handle allocated (as before
    cursors were tracked weakly, when the boundary closed them all)."""
    monkeypatch.setattr(_connection_common, "DEFERRED_CLOSE_LIMIT", 2)
    holder: dict[str, Any] = {}

    def remember(request: Request, session: Session) -> Reply | None:
        holder["session"] = session
        return None

    def steps(d: _Driver) -> list[int]:
        cursors = [d.cursor() for _ in range(5)]
        for cursor in cursors:
            d.select(cursor)
        handles = [int(cursor._query_handle) for cursor in cursors]
        del cursors, cursor
        gc.collect()
        d.run(getattr(d.conn, boundary)())
        assert holder["session"].results == {}
        return handles

    requests, handles = _run(asynchronous, steps, autocommit=False, extra=remember)
    framed = _framed(requests)
    end = "END_TRAN"
    assert [f for f, _ in framed] == [FC41] * 5 + ["CLOSE_REQ_HANDLE"] * 5 + [end]
    closed = [r.int_arg(0) for r in requests if r.function == "CLOSE_REQ_HANDLE"]
    assert sorted(closed) == sorted(handles)


@ASYNC
def test_dropped_cursors_beyond_the_bound_ride_on_later_statements(
    asynchronous: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(_connection_common, "DEFERRED_CLOSE_LIMIT", 2)

    def steps(d: _Driver) -> None:
        cursors = [d.cursor() for _ in range(5)]
        for cursor in cursors:
            d.select(cursor)
        del cursors, cursor
        gc.collect()
        for sql in ("SELECT 2", "SELECT 3", "SELECT 4"):
            d.select(d.cursor(), sql)

    requests, _ = _run(asynchronous, steps)
    batches = [r.deferred_closes for r in requests if r.function == FC41][5:]
    assert [len(batch) for batch in batches] == [2, 2, 2]  # 5 dropped + 1 kept per statement
    assert "CLOSE_REQ_HANDLE" not in [r.function for r in requests]


@ASYNC
def test_stale_id_never_frees_a_live_handle_of_the_replacement_session(
    asynchronous: bool,
) -> None:
    """The replacement session reuses id 1 for a live paged result; the
    collected cursor's id 1 of the old session must not free it."""
    results = {"SELECT v FROM paged": (_paged(), 1)}

    def steps(d: _Driver) -> list[Any]:
        old = d.cursor()
        d.select(old)
        assert old._query_handle == 1
        d.conn._cursors.discard(old)  # what the cycle collector does first
        d.conn._drop_connection()
        d.connect()
        live = d.cursor()
        d.execute(live, "SELECT v FROM paged")
        assert live._query_handle == 1  # the same id on the new session
        del old
        gc.collect()
        d.select(d.cursor(), "SELECT 2")
        return list(d.run(live.fetchall()))

    requests, rows = _run(asynchronous, steps, results=results)
    assert rows == [(1,), (2,), (3,)]
    assert all(r.deferred_closes == () for r in requests if r.session == 1)


def _paged() -> Any:
    return ResultSet(
        "paged",
        (Column("v", CUBRIDDataType.INT, precision=10),),
        ((int_(1),), (int_(2),), (int_(3),)),
    )


class _Raises:
    def _defer_dropped_cursor_close(self, cursor: Any) -> None:
        raise RuntimeError("connection half torn down")


def _collect(cursor_class: type, connection: Any = None) -> list[Any]:
    """Create a cursor without ``__init__`` and let the collector finalize it;
    return what reached ``sys.unraisablehook`` (an exception escaping
    ``__del__``)."""
    unraisable: list[Any] = []
    with patch.object(sys, "unraisablehook", unraisable.append):
        cursor = cursor_class.__new__(cursor_class)  # __init__ never ran
        if connection is not None:
            cursor._connection = connection
        del cursor
        gc.collect()
    return unraisable


@pytest.mark.parametrize("cursor_class", [Cursor, AsyncCursor])
def test_del_without_connection_is_silent(cursor_class: type) -> None:
    unraisable = _collect(cursor_class)
    assert unraisable == []


@pytest.mark.parametrize("cursor_class", [Cursor, AsyncCursor])
def test_del_never_raises(cursor_class: type, caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level("DEBUG"):
        unraisable = _collect(cursor_class, _Raises())
        assert unraisable == []
    assert "Could not queue a collected cursor's handle" in caplog.text
    module = sys.modules[cursor_class.__module__]
    with patch.object(module, "_LOGGER") as logger:
        logger.debug.side_effect = RuntimeError("logging torn down")
        unraisable = _collect(cursor_class, _Raises())
        assert unraisable == []  # still silent
        logger.debug.assert_called_once()
