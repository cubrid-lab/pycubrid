"""Connection context exit keeps the primary error of a retired session (#951).

Leaving ``with conn:`` / ``async with conn:`` commits a healthy body and rolls
back a failed one, then closes. When the body failure already retired the
physical session (a cancelled async read, a transport failure), there is no
transaction left to roll back: rollback would only raise ``InterfaceError`` and
replace the caller's error. These offline tests drive the real
:class:`pycubrid.Connection` and :class:`pycubrid.aio.AsyncConnection` against
the scripted replay broker and pin:

* a cancellation stays a cancellation and the task finishes cancelled;
* a transport ``OperationalError`` is not replaced by ``InterfaceError``;
* a retired session gets no rollback, reconnect or replayed statement, and the
  socket/streams are closed;
* a secondary rollback failure does not replace the body error;
* controls: commit on success, rollback on a healthy failure, and a commit
  failure on a healthy body still propagate as before.
"""

from __future__ import annotations

import asyncio
import threading
from typing import Any

import pytest

import pycubrid
import pycubrid.aio
from pycubrid.aio.cursor import AsyncCursor
from pycubrid.exceptions import DatabaseError, InterfaceError, OperationalError

from .helpers.replay_broker import (
    IN_TRAN,
    Reply,
    Request,
    Session,
    error_body,
    run_replay_broker,
)

GATED_SQL = "SELECT 'gated'"
DROPPED_SQL = "SELECT 'dropped'"

_CONNECT: dict[str, Any] = {
    "host": "127.0.0.1",
    "database": "testdb",
    "user": "dba",
    "password": "",
    "no_backslash_escapes": True,
    "read_timeout": 10,
}


class _Script:
    """Gates ``GATED_SQL``, drops the session on ``DROPPED_SQL``, can fail END_TRAN."""

    def __init__(self, *, end_tran_error: bool = False) -> None:
        self.received = threading.Event()
        self.gate = threading.Event()
        self.end_tran_error = end_tran_error

    def __call__(self, request: Request, session: Session) -> Reply | None:
        if request.sql == GATED_SQL:
            self.received.set()
            assert self.gate.wait(10), "the gate was never opened"
            return Reply(close=True)
        if request.sql == DROPPED_SQL:
            return Reply(close=True)
        if request.function == "END_TRAN" and self.end_tran_error:
            return Reply(error_body(IN_TRAN, -1, "end tran refused"))
        return None


def _functions(requests: list[Request]) -> list[tuple[int, str]]:
    return [(r.session, r.function) for r in requests if r.index >= 0]


def _statements(requests: list[Request]) -> list[str | None]:
    return [r.sql for r in requests if r.function == "PREPARE_AND_EXECUTE"]


def _fail(message: str) -> None:
    """Raise the application error of a context body."""
    raise RuntimeError(message)


# --- async ---------------------------------------------------------------------


def test_async_cancelled_body_stays_cancelled_after_retirement() -> None:
    script = _Script()
    with run_replay_broker(script) as broker:
        cursors: list[AsyncCursor] = []

        async def main() -> tuple[pycubrid.aio.AsyncConnection, asyncio.Task[None]]:
            conn = await pycubrid.aio.connect(port=broker.port, **_CONNECT)

            async def body() -> None:
                async with conn:
                    cursors.append(conn.cursor())
                    await cursors[0].execute(GATED_SQL)

            task = asyncio.ensure_future(body())
            # Cancel only once the request is on the wire (a real I/O wait).
            assert await asyncio.to_thread(script.received.wait, 10)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            # Cleanup ran inline: no background task outlives the context exit.
            assert asyncio.all_tasks() == {asyncio.current_task()}
            script.gate.set()
            return conn, task

        conn, task = asyncio.run(main())
        requests = list(broker.requests)

    assert task.cancelled()
    assert not conn._connected
    assert conn._writer is None
    assert cursors[0]._query_handle is None
    # No rollback, reconnect probe, CLOSE_DATABASE or replay on the retired session.
    assert _functions(requests) == [(0, "PREPARE_AND_EXECUTE")]
    assert _statements(requests) == [GATED_SQL]


def test_async_transport_error_is_not_replaced_by_interface_error() -> None:
    with run_replay_broker(_Script()) as broker:

        async def main() -> pycubrid.aio.AsyncConnection:
            conn = await pycubrid.aio.connect(port=broker.port, **_CONNECT)
            with pytest.raises(OperationalError) as info:
                async with conn:
                    await conn.cursor().execute(DROPPED_SQL)
            assert not isinstance(info.value, InterfaceError)
            return conn

        conn = asyncio.run(main())
        requests = list(broker.requests)

    assert not conn._connected
    assert conn._writer is None
    assert _functions(requests) == [(0, "PREPARE_AND_EXECUTE")]


def test_async_secondary_rollback_failure_keeps_body_error(
    caplog: pytest.LogCaptureFixture,
) -> None:
    with run_replay_broker(_Script(end_tran_error=True)) as broker:

        async def main() -> pycubrid.aio.AsyncConnection:
            conn = await pycubrid.aio.connect(port=broker.port, **_CONNECT)
            with pytest.raises(RuntimeError, match="body failed") as info:
                async with conn:
                    await conn.cursor().execute("SELECT 1")
                    _fail("body failed")
            assert any("DatabaseError" in note for note in info.value.__notes__)
            return conn

        with caplog.at_level("WARNING", logger="pycubrid"):
            conn = asyncio.run(main())
        requests = list(broker.requests)

    assert not conn._connected
    assert (0, "END_TRAN") in _functions(requests)
    assert (0, "CON_CLOSE") in _functions(requests)
    assert any("rollback" in record.getMessage() for record in caplog.records)


def test_async_controls_commit_rollback_and_commit_failure() -> None:
    with run_replay_broker(_Script()) as broker:

        async def main() -> None:
            conn = await pycubrid.aio.connect(port=broker.port, **_CONNECT)
            async with conn:
                await conn.cursor().execute("SELECT 1")
            assert not conn._connected

            conn = await pycubrid.aio.connect(port=broker.port, **_CONNECT)
            with pytest.raises(RuntimeError, match="app"):
                async with conn:
                    await conn.cursor().execute("SELECT 1")
                    _fail("app")
            assert not conn._connected

        asyncio.run(main())
        requests = list(broker.requests)

    # Both sessions ended their transaction and closed.
    assert _functions(requests).count((0, "END_TRAN")) == 1
    assert _functions(requests).count((1, "END_TRAN")) == 1
    assert (0, "CON_CLOSE") in _functions(requests)
    assert (1, "CON_CLOSE") in _functions(requests)

    with run_replay_broker(_Script(end_tran_error=True)) as broker:

        async def failing_commit() -> pycubrid.aio.AsyncConnection:
            conn = await pycubrid.aio.connect(port=broker.port, **_CONNECT)
            with pytest.raises(DatabaseError, match="end tran refused"):
                async with conn:
                    await conn.cursor().execute("SELECT 1")
            return conn

        conn = asyncio.run(failing_commit())
    assert not conn._connected


def test_async_cancellation_during_rollback_stays_cancelled() -> None:
    """A new cancellation that lands while rollback waits is not converted."""
    script = _Script()

    def end_tran_gate(request: Request, session: Session) -> Reply | None:
        if request.function == "END_TRAN":
            script.received.set()
            assert script.gate.wait(10), "the gate was never opened"
            return Reply(close=True)
        return None

    with run_replay_broker(end_tran_gate) as broker:

        async def main() -> tuple[pycubrid.aio.AsyncConnection, asyncio.Task[None]]:
            conn = await pycubrid.aio.connect(port=broker.port, **_CONNECT)

            async def body() -> None:
                async with conn:
                    await conn.cursor().execute("SELECT 1")
                    _fail("app")

            task = asyncio.ensure_future(body())
            assert await asyncio.to_thread(script.received.wait, 10)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            script.gate.set()
            return conn, task

        conn, task = asyncio.run(main())
        requests = list(broker.requests)

    assert task.cancelled()
    assert not conn._connected
    assert conn._writer is None
    assert [s for s, _ in _functions(requests)] == [0] * len(_functions(requests))


# --- sync ----------------------------------------------------------------------


def test_sync_transport_error_is_not_replaced_by_interface_error() -> None:
    with run_replay_broker(_Script()) as broker:
        conn = pycubrid.connect(port=broker.port, **_CONNECT)
        with pytest.raises(OperationalError) as info:
            with conn:
                conn.cursor().execute(DROPPED_SQL)
        assert not isinstance(info.value, InterfaceError)
        requests = list(broker.requests)

    assert not conn._connected
    assert conn._socket is None
    assert _functions(requests) == [(0, "PREPARE_AND_EXECUTE")]
    assert _statements(requests) == [DROPPED_SQL]


def test_sync_secondary_rollback_failure_keeps_body_error() -> None:
    with run_replay_broker(_Script(end_tran_error=True)) as broker:
        conn = pycubrid.connect(port=broker.port, **_CONNECT)
        with pytest.raises(RuntimeError, match="body failed") as info:
            with conn:
                conn.cursor().execute("SELECT 1")
                _fail("body failed")
        requests = list(broker.requests)

    assert any("DatabaseError" in note for note in info.value.__notes__)
    assert not conn._connected
    assert (0, "END_TRAN") in _functions(requests)
    assert (0, "CON_CLOSE") in _functions(requests)


def test_sync_controls_commit_rollback_and_commit_failure() -> None:
    with run_replay_broker(_Script()) as broker:
        conn = pycubrid.connect(port=broker.port, **_CONNECT)
        with conn:
            conn.cursor().execute("SELECT 1")
        assert not conn._connected

        conn = pycubrid.connect(port=broker.port, **_CONNECT)
        with pytest.raises(RuntimeError, match="app"):
            with conn:
                conn.cursor().execute("SELECT 1")
                _fail("app")
        assert not conn._connected
        requests = list(broker.requests)

    assert _functions(requests).count((0, "END_TRAN")) == 1
    assert _functions(requests).count((1, "END_TRAN")) == 1

    with run_replay_broker(_Script(end_tran_error=True)) as broker:
        conn = pycubrid.connect(port=broker.port, **_CONNECT)
        with pytest.raises(DatabaseError, match="end tran refused"):
            with conn:
                conn.cursor().execute("SELECT 1")
    assert not conn._connected
