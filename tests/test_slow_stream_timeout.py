"""What ``read_timeout`` bounds when a reply arrives slowly (#855).

The two drivers apply ``read_timeout`` to different spans of a request:

* sync sets it as the socket timeout, so it bounds **each** send and receive
  call. A peer that keeps delivering pieces of a reply, each within the
  timeout, is waited for however long the whole reply takes;
* async wraps the **whole** request round trip in ``asyncio.wait_for``, so
  the same slow reply fails once the total exceeds ``read_timeout``.

Neither is a wall-clock deadline for the SQL statement, and neither cancels
the statement on the server. These tests pin that documented difference with
a real TCP session against :mod:`tests.helpers.replay_broker`, which sends the
reply in fragments with a fixed pause between them. The margins are wide so
scheduling jitter cannot change the outcome: ``read_timeout`` is 0.3 s, a
pause is 0.1 s (a third of it) and the paused reply takes 0.9 s (three times
it). A stall case uses one 1.0 s pause after part of the frame. The delayed
*complete* reply, caller cancellation before any reply byte and mocked socket
timeouts are covered in ``tests/test_transport_failure_cleanup.py`` and
``tests/test_network_edge_cases.py``; only the partial-frame cases are here.
"""

from __future__ import annotations

import asyncio
import socket
import time
from contextlib import closing
from typing import Any

import pytest

import pycubrid
import pycubrid.aio
from pycubrid.exceptions import InterfaceError, OperationalError

from .helpers.fault_broker import framed
from .helpers.replay_broker import ReplayBroker, Reply, Request, Session, run_replay_broker
from .test_transport_failure_cleanup import _RESULTS, _PAGED_SQL, _assert_retired, _options

_READ_TIMEOUT = 0.3
_SLOW_SQL = "SELECT 1"
_NEXT_SQL = "SELECT 2"
# Ten pieces, 0.1 s apart: each pause is a third of read_timeout, the whole
# reply takes 0.9 s, three times read_timeout.
_DRIP = {"fragments": 10, "fragment_interval": 0.1}
# Half the frame, then a pause longer than read_timeout before the rest.
_STALL = {"fragments": 2, "fragment_interval": 1.0}


def _paced(pacing: dict[str, Any]) -> tuple[Any, dict[str, Any]]:
    """A script that sends the default ``_SLOW_SQL`` reply in paced fragments.

    The holder takes the broker (``"broker"``) and records the framed reply
    length (``"frame"``).
    """
    holder: dict[str, Any] = {}

    def script(request: Request, session: Session) -> Reply | None:
        if request.sql != _SLOW_SQL:
            return None
        body = holder["broker"].default_reply(request, session).body
        assert body is not None
        holder["frame"] = len(framed(body))
        # Every fragment carries bytes, so every pause happens mid-frame.
        assert holder["frame"] >= pacing["fragments"]
        return Reply(body=body, **pacing)

    return script, holder


class _CountingSocket:
    """Delegates to the real socket and counts the reply bytes received."""

    def __init__(self, real: socket.socket) -> None:
        self._real = real
        self.received = 0

    def recv_into(self, buffer: Any, nbytes: int = 0) -> int:
        count = self._real.recv_into(buffer, nbytes)
        self.received += count
        return count

    def __getattr__(self, name: str) -> Any:
        return getattr(self._real, name)


def _record_reads(conn: Any) -> tuple[list[int], asyncio.Event]:
    """Record each completed ``readexactly()``; the event fires on the first."""
    completed: list[int] = []
    first = asyncio.Event()
    real = conn._reader.readexactly

    async def readexactly(size: int) -> bytes:
        data = await real(size)
        completed.append(len(data))
        first.set()
        return data

    conn._reader.readexactly = readexactly
    return completed, first


def _slow_sql_sent(broker: ReplayBroker) -> int:
    return [r.sql for r in broker.requests].count(_SLOW_SQL)


def _sessions_of(broker: ReplayBroker, sql: str) -> list[int]:
    return [r.session for r in broker.requests if r.sql == sql]


# ---------------------------------------------------------------------------
# Sync: read_timeout bounds each socket operation
# ---------------------------------------------------------------------------


def test_sync_slow_reply_within_each_receive_timeout_completes() -> None:
    script, holder = _paced(_DRIP)
    with run_replay_broker(script, results=_RESULTS) as broker:
        holder["broker"] = broker
        options = _options(broker.port, read_timeout=_READ_TIMEOUT)
        with closing(pycubrid.connect(**options)) as conn:
            paged = conn.cursor()
            paged.execute(_PAGED_SQL)
            handle = paged._query_handle

            cur = conn.cursor()
            started = time.monotonic()
            cur.execute(_SLOW_SQL)
            elapsed = time.monotonic() - started

            # The whole reply took longer than read_timeout, yet no receive did.
            assert elapsed > 2 * _READ_TIMEOUT
            assert cur.fetchall() == [(1,)]
            assert conn._connected is True
            assert paged._query_handle == handle
            # The slow frame was read to its end: the next reply is not misread.
            cur.execute(_NEXT_SQL)
            assert cur.fetchall() == [(1,)]
            assert paged.fetchall() == [(1,), (2,), (3,)]
    assert _slow_sql_sent(broker) == 1
    assert {r.session for r in broker.requests} == {0}


def test_sync_stall_mid_frame_retires_session_without_replay() -> None:
    script, holder = _paced(_STALL)
    with run_replay_broker(script, results=_RESULTS) as broker:
        holder["broker"] = broker
        options = _options(broker.port, read_timeout=_READ_TIMEOUT)
        with closing(pycubrid.connect(**options)) as conn:
            paged = conn.cursor()
            paged.execute(_PAGED_SQL)
            conn.get_schema_info(1, "t")
            counting = _CountingSocket(conn._socket)
            conn._socket = counting

            with pytest.raises(OperationalError, match="socket communication failed") as raised:
                conn.cursor().execute(_SLOW_SQL)

            assert isinstance(raised.value.__cause__, socket.timeout)
            # The timeout hit inside the frame: its first half had been read.
            assert 0 < counting.received < holder["frame"]
            _assert_retired(conn, paged)
            assert conn._socket is None
            # Buffered rows stay readable; the next required FETCH fails closed.
            assert paged.fetchone() == (1,)
            with pytest.raises(InterfaceError, match="result set invalidated"):
                paged.fetchone()
            # Nothing reconnects or resends on its own.
            with pytest.raises(InterfaceError, match="connection is closed"):
                conn.cursor()

            assert conn.ping(reconnect=True) is True
            cur = conn.cursor()
            cur.execute(_NEXT_SQL)
            assert cur.fetchall() == [(1,)]
    assert _slow_sql_sent(broker) == 1
    assert _sessions_of(broker, _NEXT_SQL) == [1]


# ---------------------------------------------------------------------------
# Async: read_timeout bounds the whole request round trip
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_async_slow_reply_exceeding_total_read_timeout_times_out() -> None:
    script, holder = _paced(_DRIP)
    with run_replay_broker(script, results=_RESULTS) as broker:
        holder["broker"] = broker
        conn = await pycubrid.aio.connect(**_options(broker.port, read_timeout=_READ_TIMEOUT))
        try:
            paged = conn.cursor()
            await paged.execute(_PAGED_SQL)
            await conn.get_schema_info(1, "t")
            completed, _ = _record_reads(conn)

            with pytest.raises(OperationalError, match=r"read timeout.*read_timeout=0\.3") as r:
                await conn.cursor().execute(_SLOW_SQL)

            assert isinstance(r.value.__cause__, asyncio.TimeoutError)
            # The deadline hit inside the frame: DATA_LENGTH was read, the body not.
            assert completed == [4]
            _assert_retired(conn, paged)
            # The partly read frame goes with the streams it was read from.
            assert conn._writer is None and conn._reader is None
            assert await paged.fetchone() == (1,)
            with pytest.raises(InterfaceError, match="result set invalidated"):
                await paged.fetchone()
            with pytest.raises(InterfaceError, match="connection is closed"):
                conn.cursor()

            assert await conn.ping(reconnect=True) is True
            cur = conn.cursor()
            await cur.execute(_NEXT_SQL)
            assert await cur.fetchall() == [(1,)]
        finally:
            await conn.close()
    assert _slow_sql_sent(broker) == 1
    assert _sessions_of(broker, _NEXT_SQL) == [1]


@pytest.mark.asyncio
async def test_async_slow_reply_within_total_read_timeout_completes() -> None:
    """The same paced reply succeeds once the whole round trip fits the bound."""
    script, holder = _paced(_DRIP)
    with run_replay_broker(script, results=_RESULTS) as broker:
        holder["broker"] = broker
        conn = await pycubrid.aio.connect(**_options(broker.port, read_timeout=5.0))
        try:
            cur = conn.cursor()
            started = time.monotonic()
            await cur.execute(_SLOW_SQL)
            elapsed = time.monotonic() - started

            # The reply was as slow as the one that fails with read_timeout=0.3.
            assert elapsed > 2 * _READ_TIMEOUT
            assert await cur.fetchall() == [(1,)]
            assert conn._connected is True
            await cur.execute(_NEXT_SQL)
            assert await cur.fetchall() == [(1,)]
        finally:
            await conn.close()
    assert _slow_sql_sent(broker) == 1


@pytest.mark.asyncio
async def test_async_caller_cancellation_mid_frame_propagates_without_replay() -> None:
    script, holder = _paced(_DRIP)
    with run_replay_broker(script, results=_RESULTS) as broker:
        holder["broker"] = broker
        conn = await pycubrid.aio.connect(**_options(broker.port))
        try:
            paged = conn.cursor()
            await paged.execute(_PAGED_SQL)
            await conn.get_schema_info(1, "t")
            completed, first = _record_reads(conn)
            task = asyncio.ensure_future(conn.cursor().execute(_SLOW_SQL))
            # Cancel once DATA_LENGTH was read; the body arrives over 0.9 s.
            await asyncio.wait_for(first.wait(), timeout=5.0)
            assert completed == [4]
            assert not task.done()
            task.cancel()
            await asyncio.wait({task})
            with pytest.raises(asyncio.CancelledError):
                task.result()

            _assert_retired(conn, paged)
            assert conn._writer is None and conn._reader is None
            assert await paged.fetchone() == (1,)
            with pytest.raises(InterfaceError, match="result set invalidated"):
                await paged.fetchone()
            with pytest.raises(InterfaceError, match="connection is closed"):
                conn.cursor()

            assert await conn.ping(reconnect=True) is True
            cur = conn.cursor()
            await cur.execute(_NEXT_SQL)
            assert await cur.fetchall() == [(1,)]
        finally:
            await conn.close()
    assert _slow_sql_sent(broker) == 1
    assert _sessions_of(broker, _NEXT_SQL) == [1]
