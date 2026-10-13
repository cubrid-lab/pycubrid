"""Cancellation boundary of async ``executemany()`` row encoding (#882).

``AsyncCursor.executemany()`` encodes every row synchronously before its first
request (#839): no other task on the loop runs meanwhile, and a cancellation
cannot interrupt it. These offline checks pin where a cancellation lands:

* requested while rows are encoded: raised before any request, with nothing
  sent and the session kept (both batch paths yield once after encoding);
* after ``PREPARE`` was written: the session is retired, ``EXECUTE_ARRAY`` is
  never sent;
* after ``EXECUTE_ARRAY`` was written: the rows may have run; the session is
  retired and the batch is never sent again.

No test depends on timing: cancellations are requested from inside the row
iterator or when a given request is written. The measured event-loop hold is in
``docs/PERFORMANCE.md`` (``scripts/measure_async_prepared_encoding.py``).
"""

from __future__ import annotations

import asyncio
import threading
from collections.abc import Callable, Iterator
from typing import Any

import pytest

import pycubrid.aio
from pycubrid.constants import CASFunctionCode
from pycubrid.exceptions import InterfaceError

from .helpers.replay_broker import Reply, Request, Session, run_replay_broker
from .test_batch_row_errors import _BatchScript
from .test_execute_array import INSERT

# DATA_LENGTH and CAS_INFO precede the function code of every framed request.
HEADER = 8
LITERAL_INSERT = "INSERT INTO t (a) VALUES (?)"
_STATEMENTS = ("PREPARE", "EXECUTE_ARRAY", "PREPARE_AND_EXECUTE", "EXECUTE_BATCH")


def _statements(requests: list[Request]) -> list[str]:
    return [r.function for r in requests if r.function in _STATEMENTS]


class _GatedPrepareScript(_BatchScript):
    """Answers ``PREPARE`` only after the test opens ``gate``."""

    def __init__(self) -> None:
        super().__init__({})
        self.gate = threading.Event()

    def __call__(self, request: Request, session: Session) -> Reply | None:
        if request.function == "PREPARE":
            assert self.gate.wait(10), "the PREPARE gate was never opened"
        return super().__call__(request, session)


def _run(
    scenario: Callable[[pycubrid.aio.AsyncConnection], Any],
    *,
    script: _BatchScript | None = None,
) -> tuple[list[Request], Any]:
    with run_replay_broker(script or _BatchScript({})) as broker:

        async def main() -> Any:
            conn = await pycubrid.aio.connect(
                host="127.0.0.1",
                port=broker.port,
                database="testdb",
                user="dba",
                password="",
                no_backslash_escapes=True,
                read_timeout=10,
            )
            try:
                return await scenario(conn)
            finally:
                await conn.close()

        result = asyncio.run(main())
        return list(broker.requests), result


def _rows_that_cancel(task: asyncio.Task[Any], width: int) -> Iterator[tuple[Any, ...]]:
    """Rows whose iteration requests cancellation of ``task`` halfway through."""
    yield (1, "a")[:width]
    task.cancel()
    yield (2, "b")[:width]


async def _select_one(conn: pycubrid.aio.AsyncConnection) -> Any:
    cursor = conn.cursor()
    await cursor.execute("SELECT 1")
    return await cursor.fetchall()


# --- cancellation requested while rows are encoded -----------------------------


@pytest.mark.parametrize(
    ("sql", "width", "prepared", "keeps_handle", "after"),
    [
        # The prepared batch encodes before it releases the previous result; the
        # literal batch releases it first, then renders the rows (#485).
        (INSERT, 2, True, True, ["PREPARE", "EXECUTE_ARRAY"]),
        (LITERAL_INSERT, 1, False, False, ["EXECUTE_BATCH"]),
    ],
    ids=["prepared", "literal-batch"],
)
def test_cancel_during_encoding_sends_nothing_and_keeps_the_session(
    sql: str, width: int, prepared: bool, keeps_handle: bool, after: list[str]
) -> None:
    async def scenario(conn: pycubrid.aio.AsyncConnection) -> Any:
        cursor = conn.cursor()
        await cursor.execute("SELECT 1")
        handle = cursor._query_handle
        task = asyncio.current_task()
        assert task is not None
        with pytest.raises(asyncio.CancelledError):
            await cursor.executemany(sql, _rows_that_cancel(task, width), prepared=prepared)
        task.uncancel()
        state = (conn._connected, cursor._query_handle == handle)
        # The same cursor and session run the next batch normally.
        await cursor.executemany(sql, [(3, "c")[:width], (4, "d")[:width]], prepared=prepared)
        return state, cursor.rowcount

    requests, ((connected, kept_handle), rowcount) = _run(scenario)
    assert connected is True
    assert kept_handle is keeps_handle
    assert rowcount == 2
    assert _statements(requests) == ["PREPARE_AND_EXECUTE", *after]
    assert {r.session for r in requests} == {0}


def test_encoding_is_one_synchronous_step() -> None:
    """Another task on the loop does not run while the rows are encoded."""

    script = _GatedPrepareScript()

    async def scenario(conn: pycubrid.aio.AsyncConnection) -> Any:
        ticks = 0
        done = asyncio.Event()

        async def ticker() -> None:
            nonlocal ticks
            while not done.is_set():
                ticks += 1
                if CASFunctionCode.PREPARE in at_write:
                    # Only this task, running after the PREPARE write, lets the
                    # broker answer it: the driver must have yielded meanwhile.
                    script.gate.set()
                await asyncio.sleep(0)

        seen: list[int] = []

        def rows() -> Iterator[tuple[int, str]]:
            for i in range(200):
                seen.append(ticks)
                yield (i, "v")

        # Ticks counted when PREPARE and EXECUTE_ARRAY are written.
        at_write: dict[int, int] = {}
        writer = conn._writer
        assert writer is not None
        real_write = writer.write

        def write(data: bytes) -> None:
            at_write.setdefault(data[HEADER], ticks)
            real_write(data)

        setattr(writer, "write", write)
        tick_task = asyncio.create_task(ticker())
        await asyncio.sleep(0)
        cursor = conn.cursor()
        await cursor.executemany(INSERT, rows(), prepared=True)
        done.set()
        await asyncio.wait_for(tick_task, timeout=10)
        return seen, at_write, cursor.rowcount

    _, (seen, at_write, rowcount) = _run(scenario, script=script)
    assert rowcount == 200
    assert len(set(seen)) == 1  # no tick between the first and the last row
    # The ticker ran between the PREPARE and EXECUTE_ARRAY writes.
    assert at_write[CASFunctionCode.EXECUTE_ARRAY] > at_write[CASFunctionCode.PREPARE]


# --- cancellation after a request was written ----------------------------------


def _cancel_when_written(
    conn: pycubrid.aio.AsyncConnection, task: asyncio.Task[Any], function: int
) -> None:
    writer = conn._writer
    assert writer is not None
    real_write = writer.write

    def write(data: bytes) -> None:
        real_write(data)
        if data[HEADER] == function:
            task.cancel()

    setattr(writer, "write", write)


@pytest.mark.parametrize(
    ("function", "received"),
    [
        (CASFunctionCode.PREPARE, ["PREPARE"]),
        (CASFunctionCode.EXECUTE_ARRAY, ["PREPARE", "EXECUTE_ARRAY"]),
    ],
    ids=["after-prepare", "after-execute-array"],
)
def test_cancel_after_a_request_retires_the_session_and_never_replays(
    function: int, received: list[str]
) -> None:
    async def scenario(conn: pycubrid.aio.AsyncConnection) -> Any:
        cursor = conn.cursor()
        task = asyncio.current_task()
        assert task is not None
        _cancel_when_written(conn, task, function)
        with pytest.raises(asyncio.CancelledError):
            await cursor.executemany(INSERT, [(1, "a"), (2, "b")], prepared=True)
        task.uncancel()
        connected = conn._connected
        # Nothing reconnects or resends on its own.
        with pytest.raises(InterfaceError, match="connection is closed"):
            await cursor.executemany(INSERT, [(1, "a")], prepared=True)
        await conn.connect()
        return connected, await _select_one(conn)

    requests, (connected, rows) = _run(scenario)
    assert connected is False
    assert rows == [(1,)]
    session0 = [r.function for r in requests if r.session == 0 and r.function in _STATEMENTS]
    assert session0 == received
    assert _statements(requests).count("EXECUTE_ARRAY") == received.count("EXECUTE_ARRAY")
