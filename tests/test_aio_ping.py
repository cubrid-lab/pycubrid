from __future__ import annotations

import asyncio
import struct
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from pycubrid.aio.connection import AsyncConnection
from pycubrid.exceptions import InterfaceError, OperationalError
from pycubrid.protocol import CheckCasPacket


def make_async_connection() -> tuple[AsyncConnection, MagicMock, MagicMock]:
    conn = AsyncConnection("localhost", 33000, "testdb", "dba", "")
    conn._connected = True
    conn._cas_info = b"\x01\x01\x02\x03"
    reader = MagicMock()
    writer = MagicMock()
    writer.close = MagicMock()
    writer.drain = AsyncMock()
    writer.wait_closed = AsyncMock()
    conn._reader = reader
    conn._writer = writer
    conn._invalidate_query_handles_for_reconnect = MagicMock()

    async def negotiate() -> None:
        conn._no_backslash_escapes = True

    conn._negotiate_backslash_escapes = AsyncMock(side_effect=negotiate)
    return conn, reader, writer


class TestAsyncConnectionPing:
    @pytest.mark.asyncio
    async def test_ping_success(self) -> None:
        conn, _, _ = make_async_connection()
        conn._send_and_receive_locked = AsyncMock(return_value=SimpleNamespace(response_code=0))

        assert await conn.ping() is True

    @pytest.mark.asyncio
    async def test_ping_negative_response(self) -> None:
        conn, _, _ = make_async_connection()
        conn._send_and_receive_locked = AsyncMock(return_value=SimpleNamespace(response_code=-1))

        assert await conn.ping(reconnect=False) is False

    @pytest.mark.asyncio
    async def test_ping_negative_response_reconnects_once(self) -> None:
        conn, _, writer = make_async_connection()
        conn._send_and_receive_locked = AsyncMock(return_value=SimpleNamespace(response_code=-1))
        replacement = MagicMock()

        async def reconnect() -> None:
            conn._connected = True
            conn._writer = replacement

        conn._connect_locked = AsyncMock(side_effect=reconnect)

        assert await conn.ping(reconnect=True) is True
        assert conn._writer is replacement
        writer.close.assert_called_once()
        conn._connect_locked.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_concurrent_negative_responses_share_one_recovery(self) -> None:
        conn, _, original_writer = make_async_connection()

        async def probe(*args: object, **kwargs: object) -> SimpleNamespace:
            return SimpleNamespace(response_code=-1 if conn._writer is original_writer else 0)

        conn._send_and_receive_locked = AsyncMock(side_effect=probe)
        replacement = MagicMock()
        replacement.wait_closed = AsyncMock()

        physical_connects = 0

        async def reconnect() -> None:
            nonlocal physical_connects
            if conn._connected:
                return
            physical_connects += 1
            conn._connected = True
            conn._writer = replacement
            conn._reader = MagicMock()

        conn._connect_locked = AsyncMock(side_effect=reconnect)

        assert await asyncio.gather(conn.ping(reconnect=True), conn.ping(reconnect=True)) == [
            True,
            True,
        ]
        assert physical_connects == 1

    @pytest.mark.asyncio
    async def test_negative_probe_fences_concurrent_query_until_recovery(self) -> None:
        conn, _, original_writer = make_async_connection()
        probe_started = asyncio.Event()
        release_probe = asyncio.Event()
        query_used_broken_writer: list[bool] = []
        replacement = MagicMock()

        async def fake_request(packet: object) -> object:
            if isinstance(packet, CheckCasPacket):
                probe_started.set()
                await release_probe.wait()
                packet.response_code = -1
            else:
                query_used_broken_writer.append(conn._writer is original_writer)
            return packet

        async def reconnect() -> None:
            conn._connected = True
            conn._writer = replacement
            conn._reader = MagicMock()

        conn._do_send_and_receive = AsyncMock(side_effect=fake_request)
        conn._connect_locked = AsyncMock(side_effect=reconnect)

        ping_task = asyncio.create_task(conn.ping(reconnect=True))
        await probe_started.wait()
        query_task = asyncio.create_task(conn._send_and_receive("query"))
        await asyncio.sleep(0)  # Queue the query behind the in-flight probe.
        assert not query_task.done()
        assert query_used_broken_writer == []
        release_probe.set()

        assert await ping_task is True
        query_result = (await asyncio.gather(query_task, return_exceptions=True))[0]
        assert query_result == "query" or isinstance(
            query_result, (InterfaceError, OperationalError)
        )
        assert not any(query_used_broken_writer)

    @pytest.mark.asyncio
    async def test_ping_on_closed_connection_no_reconnect(self) -> None:
        conn, _, _ = make_async_connection()
        conn._connected = False
        conn._writer = None
        conn.connect = AsyncMock()

        assert await conn.ping(reconnect=False) is False

        conn.connect.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_ping_on_closed_connection_reconnects(self) -> None:
        conn, _, _ = make_async_connection()
        conn._connected = False
        conn._writer = None
        conn._connect_locked = AsyncMock(side_effect=lambda: setattr(conn, "_connected", True))
        invalidate = MagicMock()
        conn._invalidate_query_handles_for_reconnect = invalidate

        assert await conn.ping(reconnect=True) is True
        assert conn._connected is True
        assert invalidate.call_count == 1

    @pytest.mark.asyncio
    async def test_disconnected_ping_waits_for_initial_setup(self) -> None:
        conn, _, _ = make_async_connection()
        conn._connected = False
        conn._reader = None
        conn._writer = None
        conn._setup_done.clear()

        async def reconnect() -> None:
            conn._connected = True

        conn._connect_locked = AsyncMock(side_effect=reconnect)

        task = asyncio.create_task(conn.ping(reconnect=True))
        await asyncio.sleep(0)
        waited_for_setup = not task.done()
        conn._setup_done.set()

        assert waited_for_setup
        assert await task is True
        conn._connect_locked.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_concurrent_disconnected_pings_restore_once(self) -> None:
        conn, _, _ = make_async_connection()
        conn._connected = False
        conn._reader = None
        conn._writer = None
        reconnect_started = asyncio.Event()
        release_reconnect = asyncio.Event()

        physical_connects = 0

        async def reconnect() -> None:
            nonlocal physical_connects
            reconnect_started.set()
            await release_reconnect.wait()
            if conn._connected:
                return
            physical_connects += 1
            conn._connected = True
            conn._reader = MagicMock()
            conn._writer = MagicMock()

        conn._connect_locked = AsyncMock(side_effect=reconnect)
        conn._send_and_receive_locked = AsyncMock(return_value=SimpleNamespace(response_code=0))

        first = asyncio.create_task(conn.ping(reconnect=True))
        await reconnect_started.wait()
        second = asyncio.create_task(conn.ping(reconnect=True))
        await asyncio.sleep(0)
        release_reconnect.set()

        assert await asyncio.gather(first, second) == [True, True]
        assert physical_connects == 1

    @pytest.mark.asyncio
    async def test_ping_inactive_cas_info_no_reconnect(self) -> None:
        conn, _, _ = make_async_connection()
        conn._cas_info = b"\x00\x01\x02\x03"
        conn._send_and_receive_locked = AsyncMock(return_value=SimpleNamespace(response_code=0))

        assert await conn.ping(reconnect=False) is True

        conn._send_and_receive_locked.assert_awaited_once()
        packet = conn._send_and_receive_locked.call_args.args[0]
        assert isinstance(packet, CheckCasPacket)
        assert conn._send_and_receive_locked.call_args.kwargs == {"allow_reconnect": False}

    @pytest.mark.asyncio
    async def test_ping_out_tran_uses_same_session(self) -> None:
        conn, _, writer = make_async_connection()
        conn._cas_info = b"\x00\x01\x02\x03"
        invalidate = MagicMock()
        conn._invalidate_query_handles_for_reconnect = invalidate
        conn._do_send_and_receive = AsyncMock(return_value=SimpleNamespace(response_code=0))
        conn.connect = AsyncMock()

        assert await conn.ping(reconnect=True) is True
        assert conn._connected is True
        assert conn._writer is writer
        invalidate.assert_not_called()
        writer.close.assert_not_called()
        conn.connect.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_ping_socket_error_with_reconnect(self) -> None:
        conn, _, _ = make_async_connection()
        conn._send_and_receive_locked = AsyncMock(side_effect=OperationalError("socket failed"))

        async def fake_connect() -> None:
            assert conn._connected is False
            conn._connected = True

        conn._connect_locked = AsyncMock(side_effect=fake_connect)

        assert await conn.ping(reconnect=True) is True

    @pytest.mark.asyncio
    async def test_ping_socket_error_no_reconnect(self) -> None:
        conn, _, _ = make_async_connection()
        conn._send_and_receive_locked = AsyncMock(side_effect=OperationalError("socket failed"))

        assert await conn.ping(reconnect=False) is False

    @pytest.mark.asyncio
    async def test_ping_malformed_frame_no_reconnect(self) -> None:
        conn, _, _ = make_async_connection()
        conn._send_and_receive_locked = AsyncMock(side_effect=struct.error("bad frame"))

        assert await conn.ping(reconnect=False) is False

    @pytest.mark.asyncio
    async def test_ping_interface_error_with_reconnect(self) -> None:
        conn, _, _ = make_async_connection()
        conn._send_and_receive_locked = AsyncMock(side_effect=InterfaceError("closed"))

        async def fake_connect() -> None:
            assert conn._connected is False
            conn._connected = True

        conn._connect_locked = AsyncMock(side_effect=fake_connect)

        assert await conn.ping(reconnect=True) is True

    @pytest.mark.asyncio
    async def test_send_and_receive_skips_reconnect_when_disallowed(self) -> None:
        conn, _, writer = make_async_connection()
        conn._cas_info = b"\x00\x01\x02\x03"
        conn.connect = AsyncMock()
        packet = SimpleNamespace(response_code=0)
        conn._do_send_and_receive = AsyncMock(return_value=packet)

        result = await conn._send_and_receive(packet, allow_reconnect=False)

        assert result is packet
        writer.close.assert_not_called()
        conn.connect.assert_not_awaited()
