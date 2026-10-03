"""CAS_INFO OUT_TRAN must not be confused with a released CAS session (#468)."""

from __future__ import annotations

import asyncio
import struct
import uuid
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from ._parity_helpers import ADAPTERS, ParityAdapter
from pycubrid.exceptions import InterfaceError, OperationalError
from pycubrid.protocol import CommitPacket
from .test_aio_ping import make_async_connection
from .test_network_edge_cases import (
    build_simple_ok_response,
    make_connected_connection,
    make_socket_from_chunks,
)


def test_sync_out_tran_does_not_reconnect() -> None:
    conn, sock = make_connected_connection()
    conn._record_reply_cas_info(b"\x00\x01\x02\x03")
    ok = build_simple_ok_response(b"\x00\x01\x02\x03")
    sock.recv_into.side_effect = make_socket_from_chunks([ok[:4], ok[4:]]).recv_into.side_effect
    conn.connect = MagicMock()
    sends = sock.sendall.call_count

    # A live OUT_TRAN CAS answers the CHECK_CAS probe (#485): same session.
    assert conn._check_reconnect() is False
    assert sock.sendall.call_count == sends + 1  # the probe was really sent

    assert conn._socket is sock
    assert conn._cas_info[0] == 0
    sock.close.assert_not_called()
    conn.connect.assert_not_called()


@pytest.mark.asyncio
async def test_async_out_tran_does_not_reconnect() -> None:
    conn, _, writer = make_async_connection()
    conn._record_reply_cas_info(b"\x00\x01\x02\x03")
    conn.connect = AsyncMock()

    async def live_probe(packet: object) -> object:
        conn._record_reply_cas_info(b"\x00\x01\x02\x03")
        return SimpleNamespace(response_code=0)

    conn._do_send_and_receive = AsyncMock(side_effect=live_probe)

    # A live OUT_TRAN CAS answers the CHECK_CAS probe (#485): same session.
    assert await conn._check_reconnect() is False
    conn._do_send_and_receive.assert_awaited_once()  # the probe was really sent

    assert conn._writer is writer
    assert conn._cas_info[0] == 0
    writer.close.assert_not_called()
    conn.connect.assert_not_awaited()


def test_sync_eof_retires_transport_without_replaying_sql() -> None:
    conn, sock = make_connected_connection()
    sock.recv_into.side_effect = [0]

    with pytest.raises(OperationalError, match="connection lost during receive"):
        conn._send_and_receive(CommitPacket())

    assert conn._connected is False
    assert conn._socket is None
    sends = sock.sendall.call_count
    with pytest.raises(InterfaceError, match="closed"):
        conn._send_and_receive(CommitPacket())
    assert sock.sendall.call_count == sends


@pytest.mark.asyncio
async def test_async_incomplete_reply_retires_transport_without_replay() -> None:
    conn, reader, writer = make_async_connection()
    writer.drain = AsyncMock()
    reader.readexactly = AsyncMock(side_effect=asyncio.IncompleteReadError(partial=b"", expected=4))

    with pytest.raises(OperationalError, match="connection lost during receive"):
        await conn._send_and_receive(CommitPacket())

    assert conn._connected is False
    assert conn._writer is None
    writes = writer.write.call_count
    with pytest.raises(InterfaceError, match="closed"):
        await conn._send_and_receive(CommitPacket())
    assert writer.write.call_count == writes


def test_sync_invalid_frame_length_retires_transport() -> None:
    conn, sock = make_connected_connection()
    frame = struct.pack(">i", -1)
    sock.recv_into.side_effect = make_socket_from_chunks([frame]).recv_into.side_effect

    with pytest.raises(OperationalError, match="invalid DATA_LENGTH"):
        conn._send_and_receive(CommitPacket())

    assert conn._connected is False
    assert conn._socket is None


@pytest.mark.asyncio
async def test_async_invalid_frame_length_retires_transport() -> None:
    conn, reader, writer = make_async_connection()
    writer.drain = AsyncMock()
    reader.readexactly = AsyncMock(return_value=struct.pack(">i", -1))

    with pytest.raises(OperationalError, match="invalid DATA_LENGTH"):
        await conn._send_and_receive(CommitPacket())

    assert conn._connected is False
    assert conn._writer is None


@pytest.mark.asyncio
async def test_async_cancelled_reply_retires_transport() -> None:
    conn, reader, writer = make_async_connection()
    writer.drain = AsyncMock()
    reader.readexactly = AsyncMock(side_effect=asyncio.CancelledError())

    with pytest.raises(asyncio.CancelledError):
        await conn._send_and_receive(CommitPacket())

    assert conn._connected is False
    assert conn._writer is None


@pytest.mark.integration
@pytest.mark.no_escape_pin
@pytest.mark.asyncio
@pytest.mark.parametrize("adapter", ADAPTERS, ids=[adapter.kind for adapter in ADAPTERS])
@pytest.mark.parametrize("boundary", ["commit", "rollback"])
async def test_transaction_boundary_preserves_session_settings(
    adapter: ParityAdapter, boundary: str
) -> None:
    conn = await adapter.connect()
    marker = f"@session_468_{uuid.uuid4().hex[:10]}"

    async def execute(sql: str) -> tuple[object, ...] | None:
        cur = adapter.cursor(conn)
        try:
            await adapter.execute(cur, sql)
            return await adapter.fetchone(cur) if sql.startswith("SELECT ") else None
        finally:
            await adapter.close_cursor(cur)

    async def isolation_level() -> object:
        await execute("GET TRANSACTION ISOLATION LEVEL TO X")
        row = await execute("SELECT X")
        assert row is not None
        return row[0]

    try:
        await execute("SET TRANSACTION ISOLATION LEVEL 6")
        await execute(f"SET {marker} = 42")
        assert str(await isolation_level()) == "6"
        marker_value = await execute(f"SELECT {marker}")
        assert marker_value is not None

        transport = adapter.transport_token(conn)
        await getattr(adapter, boundary)(conn)
        assert conn._cas_info[0] == 0
        assert adapter.transport_token(conn) is transport

        assert str(await isolation_level()) == "6"
        assert await execute(f"SELECT {marker}") == marker_value
        assert adapter.transport_token(conn) is transport
    finally:
        await adapter.close_connection(conn)
