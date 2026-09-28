"""Schema capabilities remain on their original connection and CAS session."""

from __future__ import annotations

import asyncio
import inspect
import struct
import gc
import weakref
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from pycubrid.aio.connection import AsyncConnection
from pycubrid.connection import Connection
from pycubrid.constants import CASFunctionCode, CUBRIDDataType
from pycubrid.exceptions import InterfaceError, OperationalError
from pycubrid.protocol import (
    BatchExecutePacket,
    CloseDatabasePacket,
    CloseQueryPacket,
    CommitPacket,
    FetchPacket,
    GetEngineVersionPacket,
    GetSchemaPacket,
    PrepareAndExecutePacket,
    RollbackPacket,
    SetDbParameterPacket,
)
from tests.test_network_edge_cases import make_mock_stream_pair, make_socket_from_chunks


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
        conn._writer.wait_closed = AsyncMock()
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
        (71, 0, 2),
        (71, 2, 2),
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
async def test_foreign_or_unowned_packets_fail_before_rpc(
    asynchronous: bool, operation: str
) -> None:
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
        expected = {
            "commit": CommitPacket,
            "rollback": RollbackPacket,
            "close": CloseDatabasePacket,
        }
        assert isinstance(peer.calls[2][0], expected[boundary])


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("pages", [[[]], [[10, 20, 30, 40]], [[10], []]])
@pytest.mark.asyncio
async def test_invalid_fetch_progress_never_returns_partial_rows(
    asynchronous: bool, pages: list[list[int]]
) -> None:
    peer = SchemaPeer()
    peer.pages = [list(page) for page in pages]
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
    primary = OperationalError("original FETCH failure", code=-671, errno=-671)
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
    task = asyncio.create_task(
        invoke(conn, operation, 1 if operation == "get_schema_info" else packet)
    )
    await asyncio.wait_for(started.wait(), timeout=1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        _ = await task
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
            _ = await task
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
    await asyncio.wait_for(started.wait(), timeout=1)
    committing = asyncio.create_task(conn.commit())
    await asyncio.sleep(0)
    assert not committing.done()
    resume.set()
    assert await fetching == [(10,), (20,), (30,)]
    _ = await committing
    assert [type(p) for p, _ in peer.calls][-2:] == [CloseQueryPacket, CommitPacket]


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.asyncio
async def test_dropped_caller_reference_is_kept_until_connection_cleanup(
    asynchronous: bool,
) -> None:
    peer = SchemaPeer()
    conn = connected(asynchronous, peer)
    packet = await invoke(conn, "get_schema_info", 1)
    reference = weakref.ref(packet)
    peer.calls.clear()
    del packet
    gc.collect()
    assert reference() is not None
    await invoke(conn, "rollback")
    assert isinstance(peer.calls[0][0], CloseQueryPacket)
    gc.collect()
    assert reference() is None


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("teardown", ["physical", "reconnect"])
@pytest.mark.asyncio
async def test_transport_retirement_cannot_send_original_handle_in_new_session(
    asynchronous: bool, teardown: str
) -> None:
    peer = SchemaPeer()
    conn = connected(asynchronous, peer)
    packet = await invoke(conn, "get_schema_info", 1)
    if teardown == "physical":
        if isinstance(conn, AsyncConnection):
            await conn._close_streams()
        else:
            conn._safe_close_socket()
    else:
        conn._invalidate_query_handles_for_reconnect()
    conn._connected = True
    conn._session_id = 9999
    calls = len(peer.calls)
    await invoke(conn, "close_schema_info", packet)
    with pytest.raises(InterfaceError, match="retired"):
        await invoke(conn, "fetch_schema_info", packet)
    assert len(peer.calls) == calls


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.asyncio
async def test_failed_schema_creation_discards_uncertain_prior_resources(
    asynchronous: bool,
) -> None:
    peer = SchemaPeer()
    conn = connected(asynchronous, peer)
    owned = await invoke(conn, "get_schema_info", 1)
    error = OperationalError("schema creation failed")

    def fail(packet: object, **kwargs: object) -> object:
        raise error

    if isinstance(conn, AsyncConnection):
        conn._send_and_receive_locked = AsyncMock(side_effect=fail)
    else:
        conn._send_and_receive = fail
    with pytest.raises(OperationalError) as raised:
        await invoke(conn, "get_schema_info", 1)
    assert raised.value is error
    assert not conn._connected
    await invoke(conn, "close_schema_info", owned)


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.asyncio
async def test_malformed_fetch_body_retires_without_attempting_close_rpc(
    asynchronous: bool,
) -> None:
    peer = SchemaPeer()
    conn = connected(asynchronous, peer)
    packet = await invoke(conn, "get_schema_info", 1)
    # Real transport framing succeeds, but the FETCH body is truncated.
    body = CAS_INFO + struct.pack(">i", 0)
    frame = struct.pack(">i", len(body) - 4)
    if isinstance(conn, AsyncConnection):
        del conn._send_and_receive_locked
        conn._reader, conn._writer, _ = make_mock_stream_pair([frame, body])
        transport = conn._writer
    else:
        del conn._send_and_receive
        conn._socket = make_socket_from_chunks([frame, body])
        transport = conn._socket
    with pytest.raises(OperationalError, match="malformed"):
        await invoke(conn, "fetch_schema_info", packet)
    assert not conn._connected
    assert transport.write.call_count == 1 if asynchronous else transport.sendall.call_count == 1
    await invoke(conn, "close_schema_info", packet)


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("failure", ["eof", "negative_length"])
@pytest.mark.parametrize("operation", ["get_schema_info", "fetch_schema_info", "close_schema_info"])
@pytest.mark.asyncio
async def test_transport_frame_failure_never_sends_close_over_unread_reply(
    asynchronous: bool, failure: str, operation: str
) -> None:
    peer = SchemaPeer()
    conn = connected(asynchronous, peer)
    packet = await invoke(conn, "get_schema_info", 1)
    chunks = [] if failure == "eof" else [struct.pack(">i", -1)]
    if isinstance(conn, AsyncConnection):
        del conn._send_and_receive_locked
        conn._reader, conn._writer, _ = make_mock_stream_pair(chunks)
        if failure == "eof":
            conn._reader.readexactly.side_effect = asyncio.IncompleteReadError(b"", 4)
        transport = conn._writer
    else:
        del conn._send_and_receive
        conn._socket = make_socket_from_chunks(chunks)
        transport = conn._socket
    with pytest.raises(OperationalError):
        await invoke(conn, operation, 1 if operation == "get_schema_info" else packet)
    assert not conn._connected
    assert transport.write.call_count == 1 if asynchronous else transport.sendall.call_count == 1
    await invoke(conn, "close_schema_info", packet)


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.asyncio
async def test_decoded_native_fetch_error_can_close_without_discarding_session(
    asynchronous: bool,
) -> None:
    peer = SchemaPeer()
    conn = connected(asynchronous, peer)
    packet = await invoke(conn, "get_schema_info", 1)
    error = CAS_INFO + struct.pack(">ii", -1, -671) + b"untranslated native failure\x00"
    ok = CAS_INFO + struct.pack(">i", 0)
    chunks = [struct.pack(">i", len(body) - 4) for body in (error, ok)]
    responses = [chunks[0], error, chunks[1], ok]
    if isinstance(conn, AsyncConnection):
        del conn._send_and_receive_locked
        conn._reader, conn._writer, _ = make_mock_stream_pair(responses)
        transport = conn._writer
    else:
        del conn._send_and_receive
        conn._socket = make_socket_from_chunks(responses)
        transport = conn._socket
    with pytest.raises(OperationalError) as raised:
        await invoke(conn, "fetch_schema_info", packet)
    assert raised.value.code == raised.value.errno == -671
    assert conn._connected
    assert transport.write.call_count == 2 if asynchronous else transport.sendall.call_count == 2
    await invoke(conn, "close_schema_info", packet)


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.asyncio
async def test_schema_fetch_timeout_retires_without_second_rpc(asynchronous: bool) -> None:
    peer = SchemaPeer()
    conn = connected(asynchronous, peer)
    packet = await invoke(conn, "get_schema_info", 1)
    if isinstance(conn, AsyncConnection):
        del conn._send_and_receive_locked
        conn._reader, conn._writer, _ = make_mock_stream_pair()
        conn._read_timeout = 0.01

        async def never_reply(size: int) -> bytes:
            await asyncio.Future()
            return b""

        conn._reader.readexactly.side_effect = never_reply
        transport = conn._writer
    else:
        del conn._send_and_receive
        conn._socket = make_socket_from_chunks([])
        conn._socket.sendall.side_effect = TimeoutError("socket timed out")
        transport = conn._socket
    with pytest.raises(OperationalError):
        await invoke(conn, "fetch_schema_info", packet)
    assert not conn._connected
    assert transport.write.call_count == 1 if asynchronous else transport.sendall.call_count == 1
    await invoke(conn, "close_schema_info", packet)


@pytest.mark.asyncio
async def test_schema_registration_and_transaction_are_one_atomic_lock_scope() -> None:
    peer = SchemaPeer()
    conn = connected(True, peer)
    assert isinstance(conn, AsyncConnection)
    started = asyncio.Event()
    resume = asyncio.Event()

    async def gated(packet: object, *, allow_reconnect: bool = True) -> object:
        if isinstance(packet, GetSchemaPacket):
            started.set()
            await resume.wait()
        return peer.send(packet, allow_reconnect=allow_reconnect)

    conn._send_and_receive_locked = gated
    creating = asyncio.create_task(conn.get_schema_info(1))
    await asyncio.wait_for(started.wait(), 1)
    committing = asyncio.create_task(conn.commit())
    await asyncio.sleep(0)
    assert not committing.done()
    resume.set()
    packet = await creating
    _ = await committing
    assert [type(p) for p, _ in peer.calls] == [GetSchemaPacket, CloseQueryPacket, CommitPacket]
    with pytest.raises(InterfaceError, match="retired"):
        await conn.fetch_schema_info(packet)


@pytest.mark.asyncio
@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("enabled", [False, True])
async def test_autocommit_boundary_closes_and_retires_schema(
    asynchronous: bool, enabled: bool
) -> None:
    peer = SchemaPeer()
    conn = connected(asynchronous, peer)
    packet = await invoke(conn, "get_schema_info", 1)
    if isinstance(conn, AsyncConnection):
        await conn.set_autocommit(enabled)
    else:
        conn.autocommit = enabled
    assert [type(value) for value, _ in peer.calls] == [
        GetSchemaPacket,
        CloseQueryPacket,
        SetDbParameterPacket,
        CommitPacket,
    ]
    count = len(peer.calls)
    with pytest.raises(InterfaceError, match="retired"):
        await invoke(conn, "fetch_schema_info", packet)
    await invoke(conn, "close_schema_info", packet)
    assert len(peer.calls) == count
    assert conn.autocommit is enabled


@pytest.mark.asyncio
async def test_autocommit_sequence_holds_one_async_lock() -> None:
    peer = SchemaPeer()
    conn = connected(True, peer)
    assert isinstance(conn, AsyncConnection)
    packet = await conn.get_schema_info(1)
    started, resume = asyncio.Event(), asyncio.Event()

    async def gated(value: object, *, allow_reconnect: bool = True) -> object:
        if isinstance(value, SetDbParameterPacket):
            peer.calls.append((value, allow_reconnect))
            started.set()
            await resume.wait()
            return value
        return peer.send(value, allow_reconnect=allow_reconnect)

    conn._send_and_receive_locked = gated
    changing = asyncio.create_task(conn.set_autocommit(True))
    await asyncio.wait_for(started.wait(), 1)
    creating = asyncio.create_task(conn.get_schema_info(1))
    await asyncio.sleep(0)
    assert not creating.done()
    resume.set()
    _ = await changing
    current = await creating
    assert [type(value) for value, _ in peer.calls] == [
        GetSchemaPacket,
        CloseQueryPacket,
        SetDbParameterPacket,
        CommitPacket,
        GetSchemaPacket,
    ]
    with pytest.raises(InterfaceError, match="retired"):
        await conn.fetch_schema_info(packet)
    await conn.close_schema_info(current)


@pytest.mark.parametrize("interruption", [KeyboardInterrupt, SystemExit])
def test_sync_interruption_drops_without_closing_over_pending_fetch(
    interruption: type[BaseException],
) -> None:
    peer = SchemaPeer()
    conn = connected(False, peer)
    assert isinstance(conn, Connection)
    packet = conn.get_schema_info(1)
    del conn._send_and_receive
    pending = fetch_reply([10, 20])
    closed = CAS_INFO + struct.pack(">i", 0)
    transport = make_socket_from_chunks(
        [
            struct.pack(">i", len(pending) - 4),
            pending,
            struct.pack(">i", len(closed) - 4),
            closed,
        ]
    )
    receive = transport.recv_into.side_effect
    interrupted = False

    def interrupt_once(buffer: memoryview, size: int) -> int:
        nonlocal interrupted
        if not interrupted:
            interrupted = True
            raise interruption("interrupted receive")
        return int(receive(buffer, size))

    transport.recv_into.side_effect = interrupt_once
    conn._socket = transport
    with pytest.raises(interruption):
        conn.fetch_schema_info(packet)
    assert not conn._connected
    assert transport.sendall.call_count == 1
    transport.close.assert_called_once()
    conn.close_schema_info(packet)


@pytest.mark.asyncio
@pytest.mark.parametrize("interruption", [KeyboardInterrupt, SystemExit])
async def test_async_interruption_drops_without_closing_over_pending_fetch(
    interruption: type[BaseException],
) -> None:
    conn = connected(True, SchemaPeer())
    assert isinstance(conn, AsyncConnection)
    packet = await conn.get_schema_info(1)
    del conn._send_and_receive_locked
    pending = fetch_reply([10, 20])
    closed = CAS_INFO + struct.pack(">i", 0)
    conn._reader, conn._writer, _ = make_mock_stream_pair(
        [
            struct.pack(">i", len(pending) - 4),
            pending,
            struct.pack(">i", len(closed) - 4),
            closed,
        ]
    )
    reader, writer = conn._reader, conn._writer
    receive = reader.readexactly.side_effect
    interrupted = False
    primary = interruption("interrupted receive")

    async def interrupt_once(size: int) -> bytes:
        nonlocal interrupted
        if not interrupted:
            interrupted = True
            raise primary
        return next(receive)

    reader.readexactly.side_effect = interrupt_once
    with pytest.raises(interruption) as raised:
        await conn.fetch_schema_info(packet)
    assert raised.value is primary
    assert not conn._connected
    assert not conn._schema_results
    assert writer.write.call_count == 1  # FC8 only; never FC6 on an uncertain stream.
    writer.close.assert_called_once()
    assert conn._reader is None and conn._writer is None
    await conn.close_schema_info(packet)


@pytest.mark.asyncio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_schema_request_uses_negotiated_protocol_version(asynchronous: bool) -> None:
    peer = SchemaPeer()
    conn = connected(asynchronous, peer)
    conn._protocol_version = 4
    packet = await invoke(conn, "get_schema_info", 1, "table", 0)
    payload = b"\x09" + struct.pack(">ii", 4, 1) + struct.pack(">i", 6) + b"table\x00"
    payload += struct.pack(">i", 0) + struct.pack(">iB", 1, 0)
    encoded = packet.write(CAS_INFO)
    assert encoded == struct.pack(">i", len(payload)) + CAS_INFO + payload
    await invoke(conn, "close_schema_info", packet)


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("packet_kind", ["execute", "batch", "version"])
@pytest.mark.parametrize("auto_commit", [False, True])
@pytest.mark.asyncio
async def test_implicit_autocommit_packet_closes_owned_schema_before_send(
    asynchronous: bool, packet_kind: str, auto_commit: bool
) -> None:
    peer = SchemaPeer()
    conn = connected(asynchronous, peer)
    owned = await invoke(conn, "get_schema_info", 1)
    assert len(conn._schema_results) == 1
    packet = {
        "execute": PrepareAndExecutePacket("UPDATE owned SET id=id", auto_commit=auto_commit),
        "batch": BatchExecutePacket(["UPDATE owned SET id=id"], auto_commit=auto_commit),
        "version": GetEngineVersionPacket(auto_commit=auto_commit),
    }[packet_kind]
    closing = CAS_INFO + struct.pack(">i", 0)
    chunks = [struct.pack(">i", len(closing) - 4), closing]

    class AutoRequestReached(Exception):
        pass

    should_close = auto_commit
    if isinstance(conn, AsyncConnection):
        del conn._send_and_receive_locked
        conn._reader, conn._writer, _ = make_mock_stream_pair(chunks)
        transport = conn._writer
        transport.write.side_effect = (
            [None, AutoRequestReached()] if should_close else AutoRequestReached()
        )
    else:
        del conn._send_and_receive
        conn._socket = make_socket_from_chunks(chunks)
        transport = conn._socket
        transport.sendall.side_effect = (
            [None, AutoRequestReached()] if should_close else AutoRequestReached()
        )

    with pytest.raises(AutoRequestReached):
        await invoke(conn, "_send_and_receive", packet)
    writes = transport.write.call_args_list if asynchronous else transport.sendall.call_args_list
    expected = ([6] if should_close else []) + [
        {"execute": 41, "batch": 20, "version": 15}[packet_kind]
    ]
    assert [entry.args[0][8] for entry in writes] == expected
    if should_close:
        assert not conn._schema_results
        sent = len(writes)
        with pytest.raises(InterfaceError, match="retired"):
            await invoke(conn, "fetch_schema_info", owned)
        assert (
            transport.write.call_count if asynchronous else transport.sendall.call_count
        ) == sent
    else:
        assert len(conn._schema_results) == 1


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("packet_kind", ["batch", "version"])
@pytest.mark.asyncio
async def test_failed_schema_close_aborts_implicit_autocommit_packet(
    asynchronous: bool, packet_kind: str
) -> None:
    peer = SchemaPeer()
    conn = connected(asynchronous, peer)
    owned = await invoke(conn, "get_schema_info", 1)
    if isinstance(conn, AsyncConnection):
        del conn._send_and_receive_locked
        conn._reader, conn._writer, _ = make_mock_stream_pair()
        transport = conn._writer
        transport.write.side_effect = OSError("FC6 failed")
    else:
        del conn._send_and_receive
        conn._socket = make_socket_from_chunks([])
        transport = conn._socket
        transport.sendall.side_effect = OSError("FC6 failed")
    packet = (
        BatchExecutePacket(["UPDATE owned SET id=id"], auto_commit=True)
        if packet_kind == "batch"
        else GetEngineVersionPacket(auto_commit=True)
    )
    with pytest.raises(OperationalError, match="socket communication failed"):
        await invoke(conn, "_send_and_receive", packet)
    writes = transport.write.call_args_list if asynchronous else transport.sendall.call_args_list
    assert [entry.args[0][8] for entry in writes] == [6]
    assert not conn._connected
    await invoke(conn, "close_schema_info", owned)


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("packet_kind", ["execute", "batch", "version"])
@pytest.mark.asyncio
async def test_implicit_autocommit_without_owned_schema_adds_no_close(
    asynchronous: bool, packet_kind: str
) -> None:
    conn = connected(asynchronous, SchemaPeer())
    packet = {
        "execute": PrepareAndExecutePacket("SELECT 1", auto_commit=True),
        "batch": BatchExecutePacket(["SELECT 1"], auto_commit=True),
        "version": GetEngineVersionPacket(auto_commit=True),
    }[packet_kind]

    class AutoRequestReached(Exception):
        pass

    if isinstance(conn, AsyncConnection):
        del conn._send_and_receive_locked
        conn._reader, conn._writer, _ = make_mock_stream_pair([])
        transport = conn._writer
        transport.write.side_effect = AutoRequestReached()
    else:
        del conn._send_and_receive
        conn._socket = make_socket_from_chunks([])
        transport = conn._socket
        transport.sendall.side_effect = AutoRequestReached()
    with pytest.raises(AutoRequestReached):
        await invoke(conn, "_send_and_receive", packet)
    writes = transport.write.call_args_list if asynchronous else transport.sendall.call_args_list
    assert [entry.args[0][8] for entry in writes] == [
        {"execute": 41, "batch": 20, "version": 15}[packet_kind]
    ]
    assert not conn._schema_results


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("packet_kind", ["batch", "version"])
@pytest.mark.asyncio
async def test_implicit_autocommit_keeps_out_tran_socket_after_schema_close(
    asynchronous: bool,
    packet_kind: str,
) -> None:
    conn = connected(asynchronous, SchemaPeer())
    owned = await invoke(conn, "get_schema_info", 1)
    closing = b"\x00\x00\x00\x00" + struct.pack(">i", 0)
    chunks = [
        struct.pack(">i", len(closing) - 4),
        closing,
        struct.pack(">i", len(closing) - 4),
        closing,
    ]
    observed: list[int] = []

    if isinstance(conn, AsyncConnection):
        del conn._send_and_receive_locked
        conn._reader, conn._writer, _ = make_mock_stream_pair(chunks)
        transport = conn._writer

        async def checked(*, allow_reconnect: bool = True) -> None:
            observed.append(conn._cas_info[0])

        conn._check_reconnect_locked = checked
    else:
        del conn._send_and_receive
        conn._socket = make_socket_from_chunks(chunks)
        transport = conn._socket

        def checked(*, allow_reconnect: bool = True) -> None:
            observed.append(conn._cas_info[0])

        conn._check_reconnect = checked
    packet = (
        BatchExecutePacket(["SELECT 1"], auto_commit=True)
        if packet_kind == "batch"
        else GetEngineVersionPacket(auto_commit=True)
    )
    packet.parse = MagicMock()
    result = await invoke(conn, "_send_and_receive", packet)
    assert result is packet
    writes = transport.write.call_args_list if asynchronous else transport.sendall.call_args_list
    expected_code = (
        CASFunctionCode.EXECUTE_BATCH if packet_kind == "batch" else CASFunctionCode.GET_DB_VERSION
    )
    assert [entry.args[0][8] for entry in writes] == [
        CASFunctionCode.CLOSE_REQ_HANDLE,
        expected_code,
    ]
    assert observed == [1, 1, 0]
    assert not conn._schema_results
    with pytest.raises(InterfaceError, match="retired"):
        await invoke(conn, "fetch_schema_info", owned)


@pytest.mark.asyncio
async def test_cancel_during_implicit_schema_close_never_sends_autocommit_packet() -> None:
    peer = SchemaPeer()
    conn = connected(True, peer)
    assert isinstance(conn, AsyncConnection)
    owned = await conn.get_schema_info(1)
    del conn._send_and_receive_locked
    started = asyncio.Event()

    async def blocked(packet: object) -> object:
        if isinstance(packet, CloseQueryPacket):
            started.set()
            await asyncio.Future()
        return peer.send(packet)

    conn._do_send_and_receive = blocked
    task = asyncio.create_task(
        conn._send_and_receive(BatchExecutePacket(["UPDATE owned SET id=id"], auto_commit=True))
    )
    await asyncio.wait_for(started.wait(), 1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        _ = await task
    assert not conn._connected
    assert conn._writer is None
    assert not conn._schema_results
    assert not any(isinstance(packet, BatchExecutePacket) for packet, _ in peer.calls)
    await conn.close_schema_info(owned)
