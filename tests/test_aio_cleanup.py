from __future__ import annotations

import asyncio
import struct
from collections.abc import Coroutine
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from pycubrid.aio.connection import AsyncConnection
from pycubrid.exceptions import OperationalError
from pycubrid.protocol import CommitPacket


def build_simple_ok_response(cas_info: bytes | bytearray = b"\x01\x01\x02\x03") -> bytes:
    body = cas_info + struct.pack(">i", 0)
    return struct.pack(">i", len(body) - 4) + body


def make_stream_pair(read_chunks: list[bytes]) -> tuple[MagicMock, MagicMock]:
    reader = MagicMock()
    reader.readexactly = AsyncMock(side_effect=list(read_chunks))
    writer = MagicMock()
    writer.drain = AsyncMock()
    writer.close = MagicMock()
    writer.wait_closed = AsyncMock()
    return reader, writer


async def raise_timeout_and_close_coro(
    coro: Coroutine[object, object, object], timeout: float | None = None
) -> None:
    del timeout
    coro.close()
    raise asyncio.TimeoutError


def make_connected_async_connection(
    read_chunks: list[bytes], read_timeout: float | None = None
) -> tuple[AsyncConnection, MagicMock, MagicMock]:
    conn = AsyncConnection("localhost", 33000, "testdb", "dba", "", read_timeout=read_timeout)
    conn._connected = True
    conn._cas_info = b"\x01\x01\x02\x03"
    reader, writer = make_stream_pair(read_chunks)
    conn._reader = reader
    conn._writer = writer
    return conn, reader, writer


@pytest.mark.asyncio
async def test_truncated_response_disconnects_and_ping_reconnects() -> None:
    frame = build_simple_ok_response()
    conn, _, _ = make_connected_async_connection([frame[:4], frame[4:]])

    with patch("pycubrid.protocol.CommitPacket.parse", side_effect=IndexError("truncated body")):
        with pytest.raises(OperationalError, match="malformed response from broker"):
            await conn._send_and_receive(CommitPacket())

    assert conn._connected is False
    assert conn._writer is None

    reconnect = AsyncMock(
        side_effect=lambda: (
            setattr(conn, "_connected", True),
            setattr(conn, "_reader", MagicMock()),
            setattr(conn, "_writer", MagicMock()),
        )
    )
    conn.connect = reconnect

    assert await conn.ping(reconnect=True) is True
    reconnect.assert_awaited_once()
    assert conn._connected is True


@pytest.mark.asyncio
async def test_send_and_receive_timeout_awaits_stream_shutdown() -> None:
    conn, _, writer = make_connected_async_connection([], read_timeout=0.5)

    with patch(
        "pycubrid.aio.connection.asyncio.wait_for",
        new=AsyncMock(side_effect=raise_timeout_and_close_coro),
    ):
        with pytest.raises(OperationalError, match="read timeout"):
            await conn._send_and_receive(CommitPacket())

    writer.close.assert_called_once_with()
    writer.wait_closed.assert_awaited_once()
    assert conn._connected is False
    assert conn._writer is None


@pytest.mark.asyncio
async def test_malformed_response_raises_operational_error_and_disconnects() -> None:
    frame = build_simple_ok_response()
    conn, _, _ = make_connected_async_connection([frame[:4], frame[4:]])

    with patch("pycubrid.protocol.CommitPacket.parse", side_effect=ValueError("bad frame")):
        with pytest.raises(OperationalError, match="malformed response from broker"):
            await conn._send_and_receive(CommitPacket())

    assert conn._connected is False
    assert conn._writer is None
