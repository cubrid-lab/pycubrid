from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from pycubrid.aio.connection import AsyncConnection
from pycubrid.aio.cursor import AsyncCursor
from pycubrid.cursor import Cursor
from pycubrid.exceptions import InterfaceError
from pycubrid.protocol import FetchPacket


def _make_sync_connection(*, fetch_size: int = 100) -> MagicMock:
    conn = MagicMock()
    conn._fetch_size = fetch_size
    conn._timing = None
    conn._cursors = set()
    conn._connected = True
    conn._decode_collections = False
    conn._json_deserializer = None
    conn._ensure_connected = MagicMock()
    conn._send_and_receive = MagicMock(side_effect=lambda packet: packet)
    return conn


def test_cursor_nextset_returns_none() -> None:
    cursor = Cursor(_make_sync_connection())

    assert cursor.nextset() is None


def test_cursor_nextset_on_closed_cursor_raises() -> None:
    cursor = Cursor(_make_sync_connection())
    cursor.close()

    with pytest.raises(InterfaceError, match="Cursor is closed"):
        cursor.nextset()


def test_cursor_uses_configured_fetch_size() -> None:
    connection = _make_sync_connection(fetch_size=37)
    cursor = Cursor(connection)
    cursor._query_handle = 5
    cursor._total_tuple_count = 1
    cursor._columns = []

    def send(packet: object) -> object:
        assert isinstance(packet, FetchPacket)
        assert packet.fetch_size == 37
        packet.rows = []
        return packet

    connection._send_and_receive = MagicMock(side_effect=send)

    assert cursor._fetch_more_rows() is False


@pytest.mark.asyncio
async def test_async_cursor_nextset_returns_none() -> None:
    connection = AsyncConnection("localhost", 33000, "db", "dba", "", fetch_size=64)
    connection._connected = True
    cursor = AsyncCursor(connection)

    assert await cursor.nextset() is None


@pytest.mark.asyncio
async def test_async_cursor_nextset_on_closed_cursor_raises() -> None:
    connection = AsyncConnection("localhost", 33000, "db", "dba", "")
    connection._connected = True
    cursor = AsyncCursor(connection)
    await cursor.close()

    with pytest.raises(InterfaceError, match="Cursor is closed"):
        await cursor.nextset()


@pytest.mark.asyncio
async def test_async_cursor_uses_configured_fetch_size() -> None:
    connection = AsyncConnection("localhost", 33000, "db", "dba", "", fetch_size=41)
    connection._connected = True
    connection._send_and_receive = AsyncMock(side_effect=lambda packet: packet)
    cursor = AsyncCursor(connection)
    cursor._query_handle = 9
    cursor._total_tuple_count = 1
    cursor._columns = []

    async def send(packet: object) -> object:
        assert isinstance(packet, FetchPacket)
        assert packet.fetch_size == 41
        packet.rows = []
        return packet

    connection._send_and_receive = AsyncMock(side_effect=send)

    assert await cursor._fetch_more_rows() is False
