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
async def test_async_create_socket_nonblocking_uses_getaddrinfo() -> None:
    conn = AsyncConnection("localhost", 33000, "testdb", "dba", "")
    sock = MagicMock(spec=socket.socket)
    infos = [(socket.AF_INET6, socket.SOCK_STREAM, socket.IPPROTO_TCP, "", ("::1", 33000, 0, 0))]
    fake_loop = MagicMock()
    fake_loop.sock_connect = AsyncMock()

    with (
        patch("socket.getaddrinfo", return_value=infos) as getaddrinfo,
        patch("socket.socket", return_value=sock) as socket_ctor,
        patch("pycubrid.aio.connection.asyncio.get_running_loop", return_value=fake_loop),
    ):
        result = await conn._create_socket_nonblocking("localhost", 33000)

    assert result is sock
    getaddrinfo.assert_called_once_with("localhost", 33000, socket.AF_UNSPEC, socket.SOCK_STREAM)
    socket_ctor.assert_called_once_with(socket.AF_INET6, socket.SOCK_STREAM, socket.IPPROTO_TCP)
    sock.setblocking.assert_called_once_with(False)


@pytest.mark.asyncio
async def test_async_dual_stack_fallback() -> None:
    conn = AsyncConnection("localhost", 33000, "testdb", "dba", "")
    sock_v4 = MagicMock(spec=socket.socket)
    infos = [
        (socket.AF_INET6, socket.SOCK_STREAM, socket.IPPROTO_TCP, "", ("::1", 33000, 0, 0)),
        (socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP, "", ("127.0.0.1", 33000)),
    ]
    sockets = iter([MagicMock(spec=socket.socket), sock_v4])
    fake_loop = MagicMock()
    call_count = 0

    async def fail_first_connect(sock: Any, addr: Any) -> None:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise OSError("ipv6 unreachable")

    fake_loop.sock_connect = fail_first_connect

    with (
        patch("socket.getaddrinfo", return_value=infos),
        patch("socket.socket", side_effect=sockets),
        patch("pycubrid.aio.connection.asyncio.get_running_loop", return_value=fake_loop),
    ):
        result = await conn._create_socket_nonblocking("localhost", 33000)

    assert result is sock_v4


@pytest.mark.asyncio
async def test_async_read_timeout_raises_operational_error() -> None:
    conn = AsyncConnection("localhost", 33000, "testdb", "dba", "", read_timeout=0.001)
    conn._connected = True
    conn._socket = MagicMock(spec=socket.socket)
    conn._cas_info = b"\x01\x00\x00\x00"

    async def slow_send_receive(loop: Any, packet: Any) -> Any:
        await asyncio.sleep(10)

    with (
        patch.object(conn, "_check_reconnect", new_callable=AsyncMock),
        patch.object(conn, "_do_send_and_receive", side_effect=slow_send_receive),
    ):
        with pytest.raises(OperationalError, match="read timeout"):
            packet = MagicMock()
            await conn._send_and_receive(packet)

    assert conn._connected is False
