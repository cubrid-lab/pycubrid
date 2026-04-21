from __future__ import annotations

import struct
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from pycubrid.aio.connection import AsyncConnection
from pycubrid.exceptions import InterfaceError, OperationalError
from pycubrid.protocol import CheckCasPacket


def make_async_connection() -> tuple[AsyncConnection, MagicMock]:
    conn = AsyncConnection("localhost", 33000, "testdb", "dba", "")
    conn._connected = True
    conn._cas_info = b"\x01\x01\x02\x03"
    sock = MagicMock()
    conn._socket = sock
    conn._invalidate_query_handles = MagicMock()
    return conn, sock


class TestAsyncConnectionPing:
    @pytest.mark.asyncio
    async def test_ping_success(self) -> None:
        conn, _ = make_async_connection()
        conn._send_and_receive = AsyncMock(return_value=SimpleNamespace(response_code=0))

        assert await conn.ping() is True

    @pytest.mark.asyncio
    async def test_ping_negative_response(self) -> None:
        conn, _ = make_async_connection()
        conn._send_and_receive = AsyncMock(return_value=SimpleNamespace(response_code=-1))

        assert await conn.ping() is False

    @pytest.mark.asyncio
    async def test_ping_on_closed_connection_no_reconnect(self) -> None:
        conn, _ = make_async_connection()
        conn._connected = False
        conn._socket = None
        conn.connect = AsyncMock()

        assert await conn.ping(reconnect=False) is False

        conn.connect.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_ping_on_closed_connection_reconnects(self) -> None:
        conn, _ = make_async_connection()
        conn._connected = False
        conn._socket = None
        conn.connect = AsyncMock(side_effect=lambda: setattr(conn, "_connected", True))
        invalidate = MagicMock()
        conn._invalidate_query_handles = invalidate

        assert await conn.ping(reconnect=True) is True
        assert conn._connected is True
        assert invalidate.call_count == 1

    @pytest.mark.asyncio
    async def test_ping_inactive_cas_info_no_reconnect(self) -> None:
        conn, _ = make_async_connection()
        conn._cas_info = b"\x00\x01\x02\x03"
        conn._send_and_receive = AsyncMock(return_value=SimpleNamespace(response_code=0))

        assert await conn.ping(reconnect=False) is True

        conn._send_and_receive.assert_awaited_once()
        packet = conn._send_and_receive.call_args.args[0]
        assert isinstance(packet, CheckCasPacket)
        assert conn._send_and_receive.call_args.kwargs == {"allow_reconnect": False}

    @pytest.mark.asyncio
    async def test_ping_inactive_cas_info_with_reconnect(self) -> None:
        conn, sock = make_async_connection()
        conn._cas_info = b"\x00\x01\x02\x03"
        invalidate = MagicMock()
        conn._invalidate_query_handles = invalidate
        conn._do_send_and_receive = AsyncMock(return_value=SimpleNamespace(response_code=0))

        async def fake_connect() -> None:
            assert conn._connected is False
            conn._connected = True
            conn._socket = MagicMock()

        conn.connect = AsyncMock(side_effect=fake_connect)

        assert await conn.ping(reconnect=True) is True
        assert conn._connected is True
        assert invalidate.call_count == 1
        sock.close.assert_called_once_with()

    @pytest.mark.asyncio
    async def test_ping_socket_error_with_reconnect(self) -> None:
        conn, _ = make_async_connection()
        conn._send_and_receive = AsyncMock(side_effect=OperationalError("socket failed"))

        async def fake_connect() -> None:
            assert conn._connected is False
            conn._connected = True

        conn.connect = AsyncMock(side_effect=fake_connect)

        assert await conn.ping(reconnect=True) is True

    @pytest.mark.asyncio
    async def test_ping_socket_error_no_reconnect(self) -> None:
        conn, _ = make_async_connection()
        conn._send_and_receive = AsyncMock(side_effect=OperationalError("socket failed"))

        assert await conn.ping(reconnect=False) is False

    @pytest.mark.asyncio
    async def test_ping_malformed_frame_no_reconnect(self) -> None:
        conn, _ = make_async_connection()
        conn._send_and_receive = AsyncMock(side_effect=struct.error("bad frame"))

        assert await conn.ping(reconnect=False) is False

    @pytest.mark.asyncio
    async def test_ping_interface_error_with_reconnect(self) -> None:
        conn, _ = make_async_connection()
        conn._send_and_receive = AsyncMock(side_effect=InterfaceError("closed"))

        async def fake_connect() -> None:
            assert conn._connected is False
            conn._connected = True

        conn.connect = AsyncMock(side_effect=fake_connect)

        assert await conn.ping(reconnect=True) is True

    @pytest.mark.asyncio
    async def test_send_and_receive_skips_reconnect_when_disallowed(self) -> None:
        conn, sock = make_async_connection()
        conn._cas_info = b"\x00\x01\x02\x03"
        conn.connect = AsyncMock()
        packet = SimpleNamespace(response_code=0)
        conn._do_send_and_receive = AsyncMock(return_value=packet)

        result = await conn._send_and_receive(packet, allow_reconnect=False)

        assert result is packet
        sock.close.assert_not_called()
        conn.connect.assert_not_awaited()
