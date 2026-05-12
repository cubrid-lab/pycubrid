from __future__ import annotations

import asyncio
import socket
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from pycubrid.aio.connection import AsyncConnection
from pycubrid.aio.cursor import AsyncCursor
from pycubrid.connection import Connection
from pycubrid.cursor import Cursor
from pycubrid.exceptions import InterfaceError, OperationalError, ProgrammingError


def make_cursor_connection() -> MagicMock:
    conn = MagicMock()
    conn._fetch_size = 100
    conn._timing = None
    conn._cursors = set()
    conn._no_backslash_escapes = False
    conn._ensure_connected = MagicMock()
    conn._send_and_receive = MagicMock()
    conn.autocommit = False
    conn._protocol_version = 1
    conn._decode_collections = False
    conn._json_deserializer = None
    return conn


def make_async_cursor_connection() -> MagicMock:
    conn = MagicMock()
    conn._fetch_size = 100
    conn._timing = None
    conn._cursors = set()
    conn._no_backslash_escapes = False
    conn._ensure_connected = MagicMock()
    conn._send_and_receive = AsyncMock()
    conn.autocommit = False
    conn._protocol_version = 1
    conn._decode_collections = False
    conn._json_deserializer = None
    return conn


def make_connection_stub() -> Connection:
    conn = Connection.__new__(Connection)
    conn._connected = True
    conn._cursors = set()
    conn._fetch_size = 100
    conn._timing = None
    conn._ensure_connected = MagicMock()
    return conn


def make_async_connection_stub() -> AsyncConnection:
    conn = AsyncConnection.__new__(AsyncConnection)
    conn._connected = True
    conn._cursors = set()
    conn._fetch_size = 100
    conn._timing = None
    conn._ensure_connected = MagicMock()
    return conn


def test_sync_bind_parameters_rejects_mapping() -> None:
    cursor = Cursor(make_cursor_connection())

    with pytest.raises(ProgrammingError, match="parameters must be a sequence"):
        cursor._bind_parameters("SELECT ?", cast(Any, {"a": 1}))


def test_sync_bind_parameters_accepts_sequence() -> None:
    cursor = Cursor(make_cursor_connection())

    assert cursor._bind_parameters("SELECT ?", [42]) == "SELECT 42"


def test_sync_cursor_registration_is_owned_by_connection() -> None:
    conn = make_connection_stub()
    direct_cursor = Cursor(conn)

    with patch("pycubrid.connection._CursorClass", Cursor):
        factory_cursor = conn.cursor()

    assert direct_cursor not in conn._cursors
    assert factory_cursor in conn._cursors


def test_sync_cursor_close_is_best_effort_when_connection_dead() -> None:
    conn = make_cursor_connection()
    cursor = Cursor(conn)
    cursor._query_handle = 7
    conn._cursors.add(cursor)
    conn._ensure_connected.side_effect = InterfaceError("dead")

    cursor.close()

    assert cursor._closed is True
    assert cursor._query_handle is None
    assert cursor not in conn._cursors


def test_sync_connection_stores_read_timeout() -> None:
    conn = Connection.__new__(Connection)
    conn._read_timeout = 5.0

    assert conn._read_timeout == 5.0


def test_sync_create_socket_uses_create_connection() -> None:
    conn = Connection.__new__(Connection)
    conn._connect_timeout = 1.5
    conn._read_timeout = None
    sock = MagicMock(spec=socket.socket)

    with patch("socket.create_connection", return_value=sock) as create_connection:
        result = conn._create_socket("localhost", 33000)

    assert result is sock
    create_connection.assert_called_once_with(("localhost", 33000), timeout=1.5)


@pytest.mark.asyncio
async def test_async_bind_parameters_rejects_mapping() -> None:
    cursor = AsyncCursor(make_async_cursor_connection())

    with pytest.raises(ProgrammingError, match="parameters must be a sequence"):
        cursor._bind_parameters("SELECT ?", cast(Any, {"a": 1}))


def test_async_bind_parameters_accepts_sequence() -> None:
    cursor = AsyncCursor(make_async_cursor_connection())

    assert cursor._bind_parameters("SELECT ?", [42]) == "SELECT 42"


def test_async_cursor_registration_is_owned_by_connection() -> None:
    conn = make_async_connection_stub()
    direct_cursor = AsyncCursor(conn)
    factory_cursor = conn.cursor()

    assert direct_cursor not in conn._cursors
    assert factory_cursor in conn._cursors


@pytest.mark.asyncio
async def test_async_cursor_close_is_best_effort_when_connection_dead() -> None:
    conn = make_async_cursor_connection()
    cursor = AsyncCursor(conn)
    cursor._query_handle = 9
    conn._cursors.add(cursor)
    conn._ensure_connected.side_effect = InterfaceError("dead")

    await cursor.close()

    assert cursor._closed is True
    assert cursor._query_handle is None
    assert cursor not in conn._cursors


def test_async_connection_stores_read_timeout() -> None:
    conn = AsyncConnection("localhost", 33000, "testdb", "dba", "", read_timeout=5.0)

    assert conn._read_timeout == 5.0


@pytest.mark.asyncio
async def test_async_open_connection_uses_asyncio_open_connection() -> None:
    conn = AsyncConnection("localhost", 33000, "testdb", "dba", "")
    reader = AsyncMock(spec=asyncio.StreamReader)
    writer = MagicMock(spec=asyncio.StreamWriter)
    writer.transport = MagicMock()
    sock = MagicMock()
    writer.transport.get_extra_info.return_value = sock

    with (
        patch(
            "pycubrid.aio.connection.asyncio.open_connection",
            new=AsyncMock(return_value=(reader, writer)),
        ) as open_connection,
    ):
        result = await conn._open_connection("localhost", 33000)

    assert result == (reader, writer)
    open_connection.assert_awaited_once_with("localhost", 33000, ssl=None)
    sock.setsockopt.assert_any_call(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    sock.setsockopt.assert_any_call(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)


@pytest.mark.asyncio
async def test_async_open_connection_uses_wait_for_with_timeout() -> None:
    conn = AsyncConnection("localhost", 33000, "testdb", "dba", "")
    conn._connect_timeout = 1.25
    reader = AsyncMock(spec=asyncio.StreamReader)
    writer = MagicMock(spec=asyncio.StreamWriter)
    writer.transport = MagicMock()
    writer.transport.get_extra_info.return_value = None
    open_connection = AsyncMock(return_value=(reader, writer))
    wait_for = AsyncMock(return_value=(reader, writer))

    with (
        patch("pycubrid.aio.connection.asyncio.open_connection", new=open_connection),
        patch("pycubrid.aio.connection.asyncio.wait_for", new=wait_for),
    ):
        result = await conn._open_connection("localhost", 33000)

    assert result == (reader, writer)
    wait_for.assert_awaited_once()


@pytest.mark.asyncio
async def test_async_read_timeout_raises_operational_error() -> None:
    conn = AsyncConnection("localhost", 33000, "testdb", "dba", "", read_timeout=0.001)
    conn._connected = True
    conn._reader = AsyncMock(spec=asyncio.StreamReader)
    conn._writer = MagicMock(spec=asyncio.StreamWriter)
    conn._cas_info = b"\x01\x00\x00\x00"

    async def slow_send_receive(packet: Any) -> Any:
        del packet
        await asyncio.sleep(10)

    with (
        patch.object(conn, "_check_reconnect", new_callable=AsyncMock),
        patch.object(conn, "_do_send_and_receive", side_effect=slow_send_receive),
    ):
        with pytest.raises(OperationalError, match="read timeout"):
            packet = MagicMock()
            await conn._send_and_receive(packet)

    assert conn._connected is False
