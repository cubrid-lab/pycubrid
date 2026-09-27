"""Schema capabilities remain on their original connection and CAS session."""

from __future__ import annotations

import asyncio
import inspect
import struct
from unittest.mock import MagicMock, patch

import pytest

from pycubrid.aio.connection import AsyncConnection
from pycubrid.connection import Connection
from pycubrid.constants import CUBRIDDataType
from pycubrid.exceptions import InterfaceError, OperationalError
from pycubrid.protocol import (
    CloseDatabasePacket,
    CloseQueryPacket,
    CommitPacket,
    FetchPacket,
    GetSchemaPacket,
    RollbackPacket,
)


CAS_INFO = b"\x01\x00\x00\x00"


def schema_reply(handle: int = 71, count: int = 3) -> bytes:
    name = b"value\x00"
    return (
        CAS_INFO
        + struct.pack(">iii", handle, count, 1)
        + bytes([CUBRIDDataType.INT])
        + struct.pack(">hii", 0, 10, len(name))
        + name
    )


def fetch_reply(values: list[int]) -> bytes:
    data = CAS_INFO + struct.pack(">ii", 0, len(values))
    for index, value in enumerate(values, 1):
        data += struct.pack(">i", index) + bytes(8) + struct.pack(">ii", 4, value)
    return data


class SchemaPeer:
    """Only the transport is scripted; real schema/FETCH/CLOSE parsers run."""

    def __init__(self, count: int = 3) -> None:
        self.count = count
        self.calls: list[tuple[object, bool]] = []
        self.pages = [[10, 20], [30]]
        self.fetch_error: Exception | None = None
        self.close_error: Exception | None = None

    def send(self, packet: object, *, allow_reconnect: bool = True) -> object:
        self.calls.append((packet, allow_reconnect))
        if isinstance(packet, GetSchemaPacket):
            packet.parse(schema_reply(count=self.count))
        elif isinstance(packet, FetchPacket):
            if self.fetch_error:
                raise self.fetch_error
            packet.parse(fetch_reply(self.pages.pop(0)))
        elif isinstance(packet, CloseQueryPacket):
            if self.close_error:
                raise self.close_error
            packet.parse(CAS_INFO + struct.pack(">i", 0))
        return packet

    async def send_async(self, packet: object, *, allow_reconnect: bool = True) -> object:
        return self.send(packet, allow_reconnect=allow_reconnect)


def connected(asynchronous: bool, peer: SchemaPeer) -> Connection | AsyncConnection:
    if asynchronous:
        conn = AsyncConnection("localhost", 33000, "testdb", "dba", "", fetch_size=2)
        conn._writer = MagicMock()
        conn._reader = MagicMock()
        conn._send_and_receive_locked = peer.send_async
    else:
        with (
            patch.object(Connection, "connect"),
            patch.object(Connection, "_negotiate_backslash_escapes"),
        ):
            conn = Connection("localhost", 33000, "testdb", "dba", "", fetch_size=2)
        conn._socket = MagicMock()
        conn._send_and_receive = peer.send
    conn._connected = True
    conn._cas_info = CAS_INFO
    conn._session_id = 1234
    return conn


async def invoke(conn: Connection | AsyncConnection, name: str, *args: object) -> object:
    result = getattr(conn, name)(*args)
    return await result if inspect.isawaitable(result) else result


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.asyncio
async def test_eager_fetch_uses_immutable_original_metadata(asynchronous: bool) -> None:
    peer = SchemaPeer()
    conn = connected(asynchronous, peer)
    packet = await invoke(conn, "get_schema_info", 1, "table", 0)
    assert isinstance(packet, GetSchemaPacket)
    packet.query_handle = 999
    packet.tuple_count = 1000
    packet.columns.clear()
    assert await invoke(conn, "fetch_schema_info", packet) == [(10,), (20,), (30,)]
    fetches = [p for p, _ in peer.calls if isinstance(p, FetchPacket)]
    assert [(p.query_handle, p.current_tuple_count, p.fetch_size) for p in fetches] == [
        (71, 0, 2), (71, 2, 2)
    ]
    assert isinstance(peer.calls[-1][0], CloseQueryPacket)
    assert peer.calls[-1][0].query_handle == 71
    assert all(not reconnect for p, reconnect in peer.calls[1:])
    await invoke(conn, "close_schema_info", packet)
    assert len(peer.calls) == 4
    with pytest.raises(InterfaceError, match="retired"):
        await invoke(conn, "fetch_schema_info", packet)


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.asyncio
async def test_zero_rows_still_close_and_preserve_historical_fields(asynchronous: bool) -> None:
    peer = SchemaPeer(0)
    conn = connected(asynchronous, peer)
    packet = await invoke(conn, "get_schema_info", 1)
    assert await invoke(conn, "fetch_schema_info", packet) == []
    assert (packet.query_handle, packet.tuple_count) == (71, 0)
    assert [type(p) for p, _ in peer.calls] == [GetSchemaPacket, CloseQueryPacket]


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("operation", ["fetch_schema_info", "close_schema_info"])
@pytest.mark.asyncio
async def test_foreign_or_unowned_packets_fail_before_rpc(asynchronous: bool, operation: str) -> None:
    peer = SchemaPeer()
    conn = connected(asynchronous, peer)
    foreign = connected(asynchronous, SchemaPeer())
    packet = await invoke(foreign, "get_schema_info", 1)
    for invalid in (packet, GetSchemaPacket(1), object()):
        with pytest.raises(InterfaceError, match="owned"):
            await invoke(conn, operation, invalid)
    assert peer.calls == []


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("boundary", ["commit", "rollback", "close", "_drop_connection"])
@pytest.mark.asyncio
async def test_boundary_closes_or_retires_active_result(asynchronous: bool, boundary: str) -> None:
    peer = SchemaPeer()
    conn = connected(asynchronous, peer)
    packet = await invoke(conn, "get_schema_info", 1)
    await invoke(conn, boundary)
    calls = len(peer.calls)
    await invoke(conn, "close_schema_info", packet)
    with pytest.raises(InterfaceError, match="retired"):
        await invoke(conn, "fetch_schema_info", packet)
    assert len(peer.calls) == calls
    if boundary != "_drop_connection":
        assert isinstance(peer.calls[1][0], CloseQueryPacket)
        expected = {"commit": CommitPacket, "rollback": RollbackPacket, "close": CloseDatabasePacket}
        assert isinstance(peer.calls[2][0], expected[boundary])


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("pages", [[[]], [[10, 20, 30, 40]], [[10], []]])
@pytest.mark.asyncio
async def test_invalid_fetch_progress_never_returns_partial_rows(
    asynchronous: bool, pages: list[list[int]]
) -> None:
    peer = SchemaPeer()
    peer.pages = pages
    conn = connected(asynchronous, peer)
    packet = await invoke(conn, "get_schema_info", 1)
    with pytest.raises(OperationalError, match="schema"):
        await invoke(conn, "fetch_schema_info", packet)
    assert isinstance(peer.calls[-1][0], CloseQueryPacket)
    with pytest.raises(InterfaceError, match="retired"):
        await invoke(conn, "fetch_schema_info", packet)


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("fetch_fails", [False, True])
@pytest.mark.asyncio
async def test_cleanup_failure_retires_session_and_preserves_primary_error(
    asynchronous: bool, fetch_fails: bool, caplog: pytest.LogCaptureFixture
) -> None:
    peer = SchemaPeer()
    primary = OperationalError("original FETCH failure")
    cleanup = OperationalError("FC6 cleanup failure")
    peer.close_error = cleanup
    if fetch_fails:
        peer.fetch_error = primary
    conn = connected(asynchronous, peer)
    packet = await invoke(conn, "get_schema_info", 1)
    with pytest.raises(OperationalError) as raised:
        await invoke(conn, "fetch_schema_info", packet)
    assert raised.value is (primary if fetch_fails else cleanup)
    assert not conn._connected
    if fetch_fails:
        assert "schema" in caplog.text.lower()
    await invoke(conn, "close_schema_info", packet)


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["get_schema_info", "fetch_schema_info", "close_schema_info"])
async def test_cancel_during_schema_io_drops_session_without_second_rpc(operation: str) -> None:
    peer = SchemaPeer()
    conn = connected(True, peer)
    assert isinstance(conn, AsyncConnection)
    packet = await conn.get_schema_info(1)
    started = asyncio.Event()

    async def blocked(packet: object, *, allow_reconnect: bool = True) -> object:
        started.set()
        await asyncio.Future()
        return packet

    conn._send_and_receive_locked = blocked
    task = asyncio.create_task(invoke(conn, operation, 1 if operation == "get_schema_info" else packet))
    await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert not conn._connected
    assert conn._writer is None
    await conn.close_schema_info(packet)


@pytest.mark.asyncio
async def test_cancel_waiting_for_lock_does_not_drop_active_session() -> None:
    peer = SchemaPeer()
    conn = connected(True, peer)
    assert isinstance(conn, AsyncConnection)
    packet = await conn.get_schema_info(1)
    async with conn._lock:
        task = asyncio.create_task(conn.fetch_schema_info(packet))
        await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert conn._connected
    assert await conn.fetch_schema_info(packet) == [(10,), (20,), (30,)]


@pytest.mark.asyncio
async def test_transaction_waits_until_fetch_and_close_complete() -> None:
    peer = SchemaPeer()
    conn = connected(True, peer)
    assert isinstance(conn, AsyncConnection)
    packet = await conn.get_schema_info(1)
    started = asyncio.Event()
    resume = asyncio.Event()

    async def gated(packet: object, *, allow_reconnect: bool = True) -> object:
        if isinstance(packet, FetchPacket) and not started.is_set():
            started.set()
            await resume.wait()
        return peer.send(packet, allow_reconnect=allow_reconnect)

    conn._send_and_receive_locked = gated
    fetching = asyncio.create_task(conn.fetch_schema_info(packet))
    await started.wait()
    committing = asyncio.create_task(conn.commit())
    await asyncio.sleep(0)
    assert not committing.done()
    resume.set()
    assert await fetching == [(10,), (20,), (30,)]
    await committing
    assert [type(p) for p, _ in peer.calls][-2:] == [CloseQueryPacket, CommitPacket]
