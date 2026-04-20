from __future__ import annotations

import asyncio
import socket
import struct
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from pycubrid.aio.connection import AsyncConnection
from pycubrid.connection import Connection
from pycubrid.exceptions import OperationalError
from pycubrid.protocol import CommitPacket


def build_handshake_response(port: int = 0) -> bytes:
    return struct.pack(">i", port)


def build_open_db_response(
    cas_info: bytes | bytearray = b"\x01\x01\x02\x03", session_id: int = 1234
) -> bytes:
    body = cas_info + struct.pack(">i", 0)
    body += b"\x00" * 8
    body += struct.pack(">i", session_id)
    return struct.pack(">i", len(body) - 4) + body


def build_simple_ok_response(cas_info: bytes | bytearray = b"\x01\x01\x02\x03") -> bytes:
    body = cas_info + struct.pack(">i", 0)
    return struct.pack(">i", len(body) - 4) + body


def make_socket_from_chunks(chunks: list[bytes]) -> MagicMock:
    sock = MagicMock()
    queue = list(chunks)

    def recv_into(buffer: memoryview | bytearray, _nbytes: int = 0) -> int:
        if not queue:
            return 0
        chunk = queue.pop(0)
        size = min(len(chunk), len(buffer))
        buffer[:size] = chunk[:size]
        if size < len(chunk):
            queue.insert(0, chunk[size:])
        return size

    sock.recv_into.side_effect = recv_into
    return sock


def make_connected_connection() -> tuple[Connection, MagicMock]:
    open_db = build_open_db_response()
    sock = make_socket_from_chunks([build_handshake_response(), open_db[:4], open_db[4:]])
    with patch("socket.create_connection", return_value=sock):
        conn = Connection("localhost", 33000, "testdb", "dba", "")
    return conn, sock


class FakeLoop:
    def __init__(self, recv_chunks: list[bytes] | None = None) -> None:
        self._recv_chunks = list(recv_chunks or [])
        self.sock_connect = AsyncMock()
        self.sock_sendall = AsyncMock()

    async def sock_recv_into(self, _sock: MagicMock, buffer: memoryview) -> int:
        if not self._recv_chunks:
            return 0
        chunk = self._recv_chunks.pop(0)
        size = min(len(chunk), len(buffer))
        buffer[:size] = chunk[:size]
        if size < len(chunk):
            self._recv_chunks.insert(0, chunk[size:])
        return size


async def raise_timeout_and_close_coro(coro: object, timeout: float | None = None) -> None:
    del timeout
    close = getattr(coro, "close", None)
    if callable(close):
        close()
    raise asyncio.TimeoutError


class TestConnectionNetworkEdgeCases:
    def test_connection_reset_error_during_send_raises_operational_error(self) -> None:
        conn, sock = make_connected_connection()
        sock.sendall.side_effect = ConnectionResetError("reset during send")

        with pytest.raises(OperationalError, match="socket communication failed"):
            conn._send_and_receive(CommitPacket())

        assert conn._connected is False
        assert conn._socket is None

    def test_connection_reset_error_during_recv_raises_operational_error(self) -> None:
        conn, sock = make_connected_connection()
        sock.recv_into.side_effect = ConnectionResetError("reset during recv")

        with pytest.raises(OperationalError, match="socket communication failed"):
            conn._send_and_receive(CommitPacket())

        assert conn._connected is False
        assert conn._socket is None

    def test_broken_pipe_error_during_send_raises_operational_error(self) -> None:
        conn, sock = make_connected_connection()
        sock.sendall.side_effect = BrokenPipeError("broken pipe")

        with pytest.raises(OperationalError, match="socket communication failed"):
            conn._send_and_receive(CommitPacket())

        assert conn._connected is False

    def test_socket_timeout_during_connect_raises_operational_error(self) -> None:
        with patch("socket.create_connection", side_effect=socket.timeout("timed out")):
            with pytest.raises(OperationalError, match="failed to connect"):
                Connection("localhost", 33000, "testdb", "dba", "")

    def test_socket_timeout_during_query_read_raises_operational_error(self) -> None:
        conn, sock = make_connected_connection()
        sock.recv_into.side_effect = socket.timeout("timed out")

        with pytest.raises(OperationalError, match="socket communication failed"):
            conn._send_and_receive(CommitPacket())

        assert conn._connected is False
        assert conn._socket is None

    def test_partial_read_zero_bytes_raises_operational_error(self) -> None:
        conn, sock = make_connected_connection()
        sock.recv_into.side_effect = [0]

        with pytest.raises(OperationalError, match="connection lost during receive"):
            conn._send_and_receive(CommitPacket())

    def test_partial_read_fewer_bytes_than_expected_is_retried(self) -> None:
        conn, _ = make_connected_connection()
        frame = build_simple_ok_response(conn._cas_info)
        partial_sock = make_socket_from_chunks([frame[:2], frame[2:4], frame[4:7], frame[7:]])
        conn._socket = partial_sock

        packet = conn._send_and_receive(CommitPacket())

        assert packet is not None
        assert partial_sock.recv_into.call_count == 4

    def test_cas_info_inactive_triggers_reconnect_on_next_request(self) -> None:
        conn, sock = make_connected_connection()
        inactive_frame = build_simple_ok_response(b"\x00\x01\x02\x03")
        sock.recv_into.side_effect = make_socket_from_chunks(
            [inactive_frame[:4], inactive_frame[4:]]
        ).recv_into.side_effect

        conn._send_and_receive(CommitPacket())

        reconnect_sock = make_socket_from_chunks([inactive_frame[:4], inactive_frame[4:]])

        def reconnect() -> None:
            conn._socket = reconnect_sock
            conn._cas_info = b"\x01\x01\x02\x03"
            conn._connected = True

        conn.connect = MagicMock(side_effect=reconnect)
        conn._send_and_receive(CommitPacket())

        conn.connect.assert_called_once()
        assert sock.close.called

    def test_oserror_network_unreachable_during_connect_raises_operational_error(self) -> None:
        with patch("socket.create_connection", side_effect=OSError("Network is unreachable")):
            with pytest.raises(OperationalError, match="failed to connect"):
                Connection("localhost", 33000, "testdb", "dba", "")

    def test_connection_refused_during_connect_raises_operational_error(self) -> None:
        with patch("socket.create_connection", side_effect=ConnectionRefusedError("refused")):
            with pytest.raises(OperationalError, match="failed to connect"):
                Connection("localhost", 33000, "testdb", "dba", "")

    def test_connect_timeout_parameter_is_passed_to_create_connection(self) -> None:
        open_db = build_open_db_response()
        sock = make_socket_from_chunks([build_handshake_response(), open_db[:4], open_db[4:]])
        with patch("socket.create_connection", return_value=sock) as create_connection:
            Connection("localhost", 33000, "testdb", "dba", "", connect_timeout=1.25)

        create_connection.assert_called_once_with(("localhost", 33000), timeout=1.25)

    def test_read_timeout_parameter_is_applied_to_socket(self) -> None:
        open_db = build_open_db_response()
        sock = make_socket_from_chunks([build_handshake_response(), open_db[:4], open_db[4:]])
        with patch("socket.create_connection", return_value=sock):
            Connection("localhost", 33000, "testdb", "dba", "", read_timeout=4.5)

        sock.settimeout.assert_called_once_with(4.5)


class TestAsyncConnectionNetworkEdgeCases:
    @pytest.mark.asyncio
    async def test_asyncio_timeout_error_during_connect_raises_operational_error(self) -> None:
        conn = AsyncConnection("localhost", 33000, "testdb", "dba", "", connect_timeout=0.5)
        sock = MagicMock()
        loop = FakeLoop()

        with (
            patch(
                "pycubrid.aio.connection.socket.getaddrinfo",
                return_value=[(socket.AF_INET, socket.SOCK_STREAM, 0, "", ("localhost", 33000))],
            ),
            patch("pycubrid.aio.connection.socket.socket", return_value=sock),
            patch("pycubrid.aio.connection.asyncio.get_running_loop", return_value=loop),
            patch(
                "pycubrid.aio.connection.asyncio.wait_for",
                new=AsyncMock(side_effect=raise_timeout_and_close_coro),
            ),
        ):
            with pytest.raises(OperationalError, match="could not connect"):
                await conn._create_socket_nonblocking("localhost", 33000)

        sock.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_connection_reset_error_during_async_recv_raises_operational_error(self) -> None:
        conn = AsyncConnection("localhost", 33000, "testdb", "dba", "")
        conn._connected = True
        conn._socket = MagicMock()
        conn._cas_info = b"\x01\x01\x02\x03"
        loop = FakeLoop()
        loop.sock_recv_into = AsyncMock(side_effect=ConnectionResetError("reset during recv"))

        with patch("pycubrid.aio.connection.asyncio.get_running_loop", return_value=loop):
            with pytest.raises(OperationalError, match="socket communication failed"):
                await conn._send_and_receive(CommitPacket())

        assert conn._connected is False
        assert conn._socket is None

    @pytest.mark.asyncio
    async def test_partial_async_read_zero_bytes_raises_operational_error(self) -> None:
        conn = AsyncConnection("localhost", 33000, "testdb", "dba", "")
        conn._connected = True
        conn._socket = MagicMock()
        conn._cas_info = b"\x01\x01\x02\x03"
        loop = FakeLoop()
        loop.sock_recv_into = AsyncMock(return_value=0)

        with patch("pycubrid.aio.connection.asyncio.get_running_loop", return_value=loop):
            with pytest.raises(OperationalError, match="connection lost during receive"):
                await conn._send_and_receive(CommitPacket())

    @pytest.mark.asyncio
    async def test_async_read_timeout_during_query_raises_operational_error(self) -> None:
        conn = AsyncConnection("localhost", 33000, "testdb", "dba", "", read_timeout=0.5)
        conn._connected = True
        conn._socket = MagicMock()
        conn._cas_info = b"\x01\x01\x02\x03"

        with (
            patch("pycubrid.aio.connection.asyncio.get_running_loop", return_value=FakeLoop()),
            patch(
                "pycubrid.aio.connection.asyncio.wait_for",
                new=AsyncMock(side_effect=raise_timeout_and_close_coro),
            ),
        ):
            with pytest.raises(OperationalError, match="read timeout"):
                await conn._send_and_receive(CommitPacket())

        assert conn._connected is False
        assert conn._socket is None

    @pytest.mark.asyncio
    async def test_partial_async_read_fewer_bytes_than_expected_is_retried(self) -> None:
        conn = AsyncConnection("localhost", 33000, "testdb", "dba", "")
        conn._connected = True
        conn._socket = MagicMock()
        conn._cas_info = b"\x01\x01\x02\x03"
        frame = build_simple_ok_response(b"\x01\x01\x02\x03")
        loop = FakeLoop([frame[:2], frame[2:4], frame[4:6], frame[6:]])

        with patch("pycubrid.aio.connection.asyncio.get_running_loop", return_value=loop):
            packet = await conn._send_and_receive(CommitPacket())

        assert packet is not None
        assert loop.sock_sendall.await_count == 1
