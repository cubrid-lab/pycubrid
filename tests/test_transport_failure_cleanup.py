"""Transport failures retire the session and its cursor/schema handles (#556).

An uncertain transport failure (socket error, timeout, malformed reply, an
interrupt while a reply is outstanding) leaves the next response boundary
unknown, so the physical session is retired. Every cursor and schema handle of
that session must be retired with it: a handle id kept after ``_connected``
becomes ``False`` names a result on a dead session and would otherwise be sent
again (CLOSE_REQ/FETCH) or mistaken for a live result. Rows already buffered on a
cursor stay readable; the next required FETCH fails explicitly.

The sessions are real TCP sessions against the scripted
:mod:`tests.helpers.replay_broker`; faults are injected on the client side of the
stream so the exact failing call is deterministic.
"""

from __future__ import annotations

import asyncio
import socket
import time
from contextlib import closing
from typing import Any
from unittest.mock import patch

import pytest

import pycubrid
import pycubrid.aio
from pycubrid.constants import CUBRIDDataType as T
from pycubrid.exceptions import InterfaceError, OperationalError
from pycubrid.protocol import PrepareAndExecutePacket

from .helpers.cas_reply import Column, ResultSet, int_
from .helpers.replay_broker import IN_TRAN, Reply, Request, Session, cas_info, run_replay_broker

_TIMEOUT = 5.0
_PAGED_SQL = "SELECT v FROM t"
# Three rows, only the first inline: the cursor keeps an open server handle.
_PAGED = ResultSet(
    "t",
    (Column("v", T.INT, precision=10),),
    ((int_(1),), (int_(2),), (int_(3),)),
)
_RESULTS = {_PAGED_SQL: (_PAGED, 1)}


def _options(port: int, **extra: Any) -> dict[str, Any]:
    options: dict[str, Any] = {
        "host": "127.0.0.1",
        "port": port,
        "database": "testdb",
        "user": "dba",
        "password": "",
        "connect_timeout": _TIMEOUT,
        "no_backslash_escapes": True,
    }
    options.update(extra)
    return options


class _FailingRecvSocket:
    """Sends through the real socket, then fails the reply read with ``exc``."""

    def __init__(self, real: socket.socket, exc: BaseException) -> None:
        self._real = real
        self._exc = exc

    def sendall(self, data: bytes) -> None:
        self._real.sendall(data)

    def recv_into(self, *_args: Any) -> int:
        raise self._exc

    def close(self) -> None:
        self._real.close()


class _Interrupt(BaseException):
    """Stands in for KeyboardInterrupt, which pytest handles specially."""


def _assert_retired(conn: Any, cursor: Any) -> None:
    assert conn._connected is False
    assert cursor._query_handle is None
    assert conn._schema_results == {}


# ---------------------------------------------------------------------------
# Sync
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("exc", "message"),
    [
        (ConnectionResetError(104, "reset"), "socket communication failed"),
        (socket.timeout("timed out"), "socket communication failed"),
    ],
)
def test_sync_socket_error_retires_cursor_and_schema_handles(
    exc: BaseException, message: str
) -> None:
    with run_replay_broker(results=_RESULTS) as broker:
        with closing(pycubrid.connect(**_options(broker.port, read_timeout=_TIMEOUT))) as conn:
            paged = conn.cursor()
            paged.execute(_PAGED_SQL)
            schema = conn.get_schema_info(1, "t")
            assert paged._query_handle is not None
            assert conn._schema_results

            conn._socket = _FailingRecvSocket(conn._socket, exc)
            with pytest.raises(OperationalError, match=message) as raised:
                conn.cursor().execute("SELECT 1")

            assert raised.value.__cause__ is exc
            _assert_retired(conn, paged)
            # Buffered rows stay readable; the next required FETCH fails closed.
            assert paged.fetchone() == (1,)
            with pytest.raises(InterfaceError, match="result set invalidated"):
                paged.fetchone()
            conn.close_schema_info(schema)  # retired: no request on a dead session
    functions = [r.function for r in broker.requests]
    assert functions.count("CLOSE_REQ_HANDLE") == 0
    assert functions.count("FETCH") == 0


def test_sync_malformed_reply_retires_cursor_handles() -> None:
    def script(request: Request, session: Session) -> Reply | None:
        if request.sql == "SELECT 1":
            return Reply(body=cas_info(IN_TRAN) + b"\x00\x00\x00\x01\x01")
        return None

    with run_replay_broker(script, results=_RESULTS) as broker:
        with closing(pycubrid.connect(**_options(broker.port, read_timeout=_TIMEOUT))) as conn:
            paged = conn.cursor()
            paged.execute(_PAGED_SQL)
            with pytest.raises(OperationalError, match="malformed response from broker"):
                conn.cursor().execute("SELECT 1")
            _assert_retired(conn, paged)


def test_sync_interrupt_while_reply_outstanding_retires_session() -> None:
    with run_replay_broker(results=_RESULTS) as broker:
        with closing(pycubrid.connect(**_options(broker.port, read_timeout=_TIMEOUT))) as conn:
            paged = conn.cursor()
            paged.execute(_PAGED_SQL)
            conn._socket = _FailingRecvSocket(conn._socket, _Interrupt())
            with pytest.raises(_Interrupt):
                conn.cursor().execute("SELECT 1")
            # The reply may still arrive; the session must not be reused.
            _assert_retired(conn, paged)
            assert conn._socket is None


def test_sync_pre_send_local_failure_keeps_session() -> None:
    """Only an *attempted* send is uncertain; an encoding failure sends nothing."""
    with run_replay_broker(results=_RESULTS) as broker:
        with closing(pycubrid.connect(**_options(broker.port, read_timeout=_TIMEOUT))) as conn:
            paged = conn.cursor()
            paged.execute(_PAGED_SQL)
            handle = paged._query_handle
            with pytest.raises(pycubrid.DataError):
                conn.cursor().execute("SELECT '\udcff'")
            assert conn._connected is True
            assert paged._query_handle == handle


# ---------------------------------------------------------------------------
# Async
# ---------------------------------------------------------------------------


async def _open_async(port: int, **extra: Any) -> Any:
    conn = await pycubrid.aio.connect(**_options(port, **extra))
    paged = conn.cursor()
    await paged.execute(_PAGED_SQL)
    await conn.get_schema_info(1, "t")
    assert paged._query_handle is not None
    assert conn._schema_results
    return conn, paged


def _fail_reads(conn: Any, exc: BaseException) -> None:
    async def readexactly(_size: int) -> bytes:
        raise exc

    conn._reader.readexactly = readexactly


@pytest.mark.asyncio
@pytest.mark.parametrize("read_timeout", [None, _TIMEOUT])
@pytest.mark.parametrize(
    "timeout_type", [TimeoutError, asyncio.TimeoutError], ids=["socket", "asyncio"]
)
async def test_async_transport_timeout_is_not_called_a_read_timeout(
    read_timeout: float | None,
    timeout_type: type[Exception],
) -> None:
    """A transport TimeoutError (e.g. ETIMEDOUT) is not the read_timeout deadline."""
    exc = timeout_type(110, "Connection timed out")
    with run_replay_broker(results=_RESULTS) as broker:
        conn, paged = await _open_async(broker.port, read_timeout=read_timeout)
        try:
            _fail_reads(conn, exc)
            with pytest.raises(OperationalError) as raised:
                await conn.cursor().execute("SELECT 1")
            assert "read timeout" not in str(raised.value)
            assert "timed out" in str(raised.value)
            assert raised.value.__cause__ is exc
            _assert_retired(conn, paged)
            assert conn._writer is None and conn._reader is None
        finally:
            await conn.close()


@pytest.mark.asyncio
async def test_async_read_timeout_deadline_names_the_option_and_retires_handles() -> None:
    def script(request: Request, session: Session) -> Reply | None:
        if request.sql == "SELECT 1":
            time.sleep(0.5)  # longer than read_timeout; the reply comes too late
        return None

    with run_replay_broker(script, results=_RESULTS) as broker:
        conn, paged = await _open_async(broker.port, read_timeout=0.1)
        try:
            with pytest.raises(OperationalError, match=r"read timeout.*read_timeout=0\.1") as r:
                await conn.cursor().execute("SELECT 1")
            assert isinstance(r.value.__cause__, asyncio.TimeoutError)
            _assert_retired(conn, paged)
            assert conn._writer is None
            # Buffered rows stay readable; the next required FETCH fails closed.
            assert await paged.fetchone() == (1,)
            with pytest.raises(InterfaceError, match="result set invalidated"):
                await paged.fetchone()
        finally:
            await conn.close()


@pytest.mark.asyncio
async def test_async_socket_error_retires_cursor_and_schema_handles() -> None:
    exc = ConnectionResetError(104, "reset")
    with run_replay_broker(results=_RESULTS) as broker:
        conn, paged = await _open_async(broker.port)
        try:
            _fail_reads(conn, exc)
            with pytest.raises(OperationalError, match="socket communication failed") as raised:
                await conn.cursor().execute("SELECT 1")
            assert raised.value.__cause__ is exc
            _assert_retired(conn, paged)
        finally:
            await conn.close()
    assert [r.function for r in broker.requests].count("CLOSE_REQ_HANDLE") == 0


@pytest.mark.asyncio
async def test_async_malformed_reply_retires_cursor_handles() -> None:
    def script(request: Request, session: Session) -> Reply | None:
        if request.sql == "SELECT 1":
            return Reply(body=cas_info(IN_TRAN) + b"\x00\x00\x00\x01\x01")
        return None

    with run_replay_broker(script, results=_RESULTS) as broker:
        conn, paged = await _open_async(broker.port)
        try:
            with pytest.raises(OperationalError, match="malformed response from broker"):
                await conn.cursor().execute("SELECT 1")
            _assert_retired(conn, paged)
        finally:
            await conn.close()


@pytest.mark.asyncio
async def test_async_cleanup_failure_still_retires_handles() -> None:
    """A failing ``wait_closed()`` must not keep live-looking handles."""
    exc = ConnectionResetError(104, "reset")
    with run_replay_broker(results=_RESULTS) as broker:
        conn, paged = await _open_async(broker.port)
        writer = conn._writer
        try:
            _fail_reads(conn, exc)
            with patch.object(writer, "wait_closed", side_effect=RuntimeError("tls shutdown")):
                with pytest.raises(OperationalError, match="socket communication failed"):
                    await conn.cursor().execute("SELECT 1")
            _assert_retired(conn, paged)
            assert conn._writer is None
        finally:
            writer.close()
            await conn.close()


@pytest.mark.asyncio
async def test_async_cleanup_cancellation_still_retires_handles() -> None:
    """Cancelling the stream shutdown stays a CancelledError, with state retired."""
    exc = ConnectionResetError(104, "reset")
    with run_replay_broker(results=_RESULTS) as broker:
        conn, paged = await _open_async(broker.port)
        writer = conn._writer
        try:
            _fail_reads(conn, exc)
            with patch.object(writer, "wait_closed", side_effect=asyncio.CancelledError()):
                with pytest.raises(asyncio.CancelledError):
                    await conn.cursor().execute("SELECT 1")
            _assert_retired(conn, paged)
            assert conn._writer is None
        finally:
            writer.close()
            await conn.close()


@pytest.mark.asyncio
async def test_async_caller_cancellation_retires_handles_without_replay() -> None:
    def script(request: Request, session: Session) -> Reply | None:
        if request.sql == "SELECT 1":
            time.sleep(0.3)
        return None

    with run_replay_broker(script, results=_RESULTS) as broker:
        conn, paged = await _open_async(broker.port)
        try:
            task = asyncio.ensure_future(conn.cursor().execute("SELECT 1"))
            await asyncio.sleep(0.1)
            task.cancel()
            await asyncio.wait({task})
            with pytest.raises(asyncio.CancelledError):
                task.result()
            _assert_retired(conn, paged)
        finally:
            await conn.close()
    assert [r.sql for r in broker.requests].count("SELECT 1") == 1


# ---------------------------------------------------------------------------
# Composition with the failed-execute reset (#373 / #531)
# ---------------------------------------------------------------------------

# Re-executing a cursor sends CLOSE_REQ for its open handle (the session's first
# CLOSE_REQ) and then the new PREPARE_AND_EXECUTE (the session's second one).
_REEXECUTE_FAULTS = [("CLOSE_REQ_HANDLE", 1), ("PREPARE_AND_EXECUTE", 2)]


def _hang_up_on(function: str, occurrence: int) -> Any:
    def script(request: Request, session: Session) -> Reply | None:
        if request.function == function and session.functions.count(function) == occurrence:
            return Reply(close=True)  # no reply: the connection is lost mid-request
        return None

    return script


def _assert_reexecute_retired(conn: Any, cursor: Any, function: str) -> None:
    assert conn._connected is False
    assert cursor._query_handle is None
    if function == "PREPARE_AND_EXECUTE":
        # #531: the old result was released, so the failed execute has none.
        assert cursor.description is None
        assert cursor._rows == []


@pytest.mark.parametrize(("function", "occurrence"), _REEXECUTE_FAULTS)
def test_sync_reexecute_transport_failure_leaves_no_stale_handle(
    function: str, occurrence: int
) -> None:
    with run_replay_broker(_hang_up_on(function, occurrence), results=_RESULTS) as broker:
        with closing(pycubrid.connect(**_options(broker.port, read_timeout=_TIMEOUT))) as conn:
            cursor = conn.cursor()
            cursor.execute(_PAGED_SQL)
            with pytest.raises(OperationalError):
                cursor.execute("SELECT 1")
            _assert_reexecute_retired(conn, cursor, function)
            cursor.close()  # sends nothing for the retired handle
    assert [r.function for r in broker.requests].count("CLOSE_REQ_HANDLE") == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(("function", "occurrence"), _REEXECUTE_FAULTS)
async def test_async_reexecute_transport_failure_leaves_no_stale_handle(
    function: str, occurrence: int
) -> None:
    with run_replay_broker(_hang_up_on(function, occurrence), results=_RESULTS) as broker:
        conn = await pycubrid.aio.connect(**_options(broker.port, read_timeout=_TIMEOUT))
        try:
            cursor = conn.cursor()
            await cursor.execute(_PAGED_SQL)
            with pytest.raises(OperationalError):
                await cursor.execute("SELECT 1")
            _assert_reexecute_retired(conn, cursor, function)
            await cursor.close()
        finally:
            await conn.close()
    assert [r.function for r in broker.requests].count("CLOSE_REQ_HANDLE") == 1


# ---------------------------------------------------------------------------
# A parse callback's OSError is not a transport failure
# ---------------------------------------------------------------------------


def _parse_then_raise(exc: BaseException) -> Any:
    """Parse the complete reply, then fail as a ``json_deserializer`` callback would."""
    original = PrepareAndExecutePacket.parse

    def parse(packet: PrepareAndExecutePacket, data: Any) -> None:
        original(packet, data)
        if packet.query_handle and getattr(packet, "sql", "") == "SELECT 1":
            raise exc

    return patch.object(PrepareAndExecutePacket, "parse", parse)


@pytest.mark.parametrize("exc", [TimeoutError("callback"), ConnectionError("callback")])
def test_sync_parse_callback_os_error_keeps_session(exc: BaseException) -> None:
    with run_replay_broker(results=_RESULTS) as broker:
        with closing(pycubrid.connect(**_options(broker.port, read_timeout=_TIMEOUT))) as conn:
            paged = conn.cursor()
            paged.execute(_PAGED_SQL)
            with _parse_then_raise(exc), pytest.raises(type(exc)) as raised:
                conn.cursor().execute("SELECT 1")
            assert raised.value is exc  # not wrapped
            assert conn._connected is True
            assert paged._query_handle is not None
            assert paged.fetchall() == [(1,), (2,), (3,)]


@pytest.mark.asyncio
@pytest.mark.parametrize("read_timeout", [None, _TIMEOUT])
@pytest.mark.parametrize(
    "exc",
    [TimeoutError("callback"), asyncio.TimeoutError("callback"), ConnectionError("callback")],
    ids=["socket-timeout", "asyncio-timeout", "connection-error"],
)
async def test_async_parse_callback_os_error_keeps_session(
    exc: BaseException, read_timeout: float | None
) -> None:
    with run_replay_broker(results=_RESULTS) as broker:
        conn, paged = await _open_async(broker.port, read_timeout=read_timeout)
        try:
            with _parse_then_raise(exc), pytest.raises(type(exc)) as raised:
                await conn.cursor().execute("SELECT 1")
            assert raised.value is exc  # not wrapped, not called a timeout
            assert conn._connected is True
            assert paged._query_handle is not None
            assert await paged.fetchall() == [(1,), (2,), (3,)]
        finally:
            await conn.close()


@pytest.mark.asyncio
async def test_async_deadline_during_malformed_reply_cleanup_is_a_read_timeout() -> None:
    """The read_timeout deadline can fire while a malformed reply's session is
    being shut down (a TLS peer that never sends close_notify): that is still a
    pycubrid ``OperationalError``, never a bare ``TimeoutError``."""

    def script(request: Request, session: Session) -> Reply | None:
        if request.sql == "SELECT 1":
            return Reply(body=cas_info(IN_TRAN) + b"\x00\x00\x00\x01\x01")
        return None

    with run_replay_broker(script, results=_RESULTS) as broker:
        conn, paged = await _open_async(broker.port, read_timeout=0.2)
        writer = conn._writer

        async def slow_wait_closed() -> None:
            await asyncio.sleep(1.0)

        try:
            with patch.object(writer, "wait_closed", side_effect=slow_wait_closed):
                with pytest.raises(OperationalError, match="read timeout") as raised:
                    await conn.cursor().execute("SELECT 1")
            assert isinstance(raised.value.__cause__, asyncio.TimeoutError)
            _assert_retired(conn, paged)
        finally:
            writer.close()
            await conn.close()


@pytest.mark.asyncio
async def test_async_value_error_from_a_parse_callback_is_still_malformed() -> None:
    with run_replay_broker(results=_RESULTS) as broker:
        conn, paged = await _open_async(broker.port)
        try:
            with _parse_then_raise(ValueError("orjson decode error")):
                with pytest.raises(OperationalError, match="malformed response"):
                    await conn.cursor().execute("SELECT 1")
            _assert_retired(conn, paged)
        finally:
            await conn.close()
