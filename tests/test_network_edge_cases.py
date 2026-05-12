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


def make_mock_stream_pair(
    read_chunks: list[bytes] | None = None,
) -> tuple[MagicMock, MagicMock, MagicMock]:
    reader = MagicMock(spec=asyncio.StreamReader)
    reader.readexactly = AsyncMock(side_effect=list(read_chunks or []))
    writer = MagicMock(spec=asyncio.StreamWriter)
    writer.drain = AsyncMock()
    writer.close = MagicMock()
    writer.wait_closed = AsyncMock()
    writer.transport = MagicMock()
    mock_socket = MagicMock()
    writer.transport.get_extra_info.return_value = mock_socket
    return reader, writer, mock_socket


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
        open_connection = AsyncMock(side_effect=raise_timeout_and_close_coro)

        with (
            patch(
                "pycubrid.aio.connection.asyncio.wait_for",
                new=AsyncMock(side_effect=raise_timeout_and_close_coro),
            ),
            patch("pycubrid.aio.connection.asyncio.open_connection", new=open_connection),
        ):
            with pytest.raises(OperationalError, match="could not connect"):
                await conn._open_connection("localhost", 33000)

        open_connection.assert_called_once_with("localhost", 33000, ssl=None)

    @pytest.mark.asyncio
    async def test_connection_reset_error_during_async_recv_raises_operational_error(self) -> None:
        conn = AsyncConnection("localhost", 33000, "testdb", "dba", "")
        conn._connected = True
        conn._cas_info = b"\x01\x01\x02\x03"
        reader, writer, _ = make_mock_stream_pair()
        reader.readexactly = AsyncMock(side_effect=ConnectionResetError("reset during recv"))
        conn._reader = reader
        conn._writer = writer

        with pytest.raises(OperationalError, match="socket communication failed"):
            await conn._send_and_receive(CommitPacket())

        assert conn._connected is False
        assert conn._writer is None

    @pytest.mark.asyncio
    async def test_partial_async_read_zero_bytes_raises_operational_error(self) -> None:
        conn = AsyncConnection("localhost", 33000, "testdb", "dba", "")
        conn._connected = True
        conn._cas_info = b"\x01\x01\x02\x03"
        reader, writer, _ = make_mock_stream_pair()
        reader.readexactly = AsyncMock(
            side_effect=asyncio.IncompleteReadError(partial=b"", expected=4)
        )
        conn._reader = reader
        conn._writer = writer

        with pytest.raises(OperationalError, match="connection lost during receive"):
            await conn._send_and_receive(CommitPacket())

    @pytest.mark.asyncio
    async def test_async_read_timeout_during_query_raises_operational_error(self) -> None:
        conn = AsyncConnection("localhost", 33000, "testdb", "dba", "", read_timeout=0.5)
        conn._connected = True
        conn._cas_info = b"\x01\x01\x02\x03"
        conn._reader, conn._writer, _ = make_mock_stream_pair()

        with patch(
            "pycubrid.aio.connection.asyncio.wait_for",
            new=AsyncMock(side_effect=raise_timeout_and_close_coro),
        ):
            with pytest.raises(OperationalError, match="read timeout"):
                await conn._send_and_receive(CommitPacket())

        assert conn._connected is False
        assert conn._writer is None

    @pytest.mark.asyncio
    async def test_partial_async_read_fewer_bytes_than_expected_is_retried(self) -> None:
        conn = AsyncConnection("localhost", 33000, "testdb", "dba", "")
        conn._connected = True
        conn._cas_info = b"\x01\x01\x02\x03"
        frame = build_simple_ok_response(b"\x01\x01\x02\x03")
        reader, writer, _ = make_mock_stream_pair([frame[:4], frame[4:]])
        conn._reader = reader
        conn._writer = writer

        packet = await conn._send_and_receive(CommitPacket())

        assert packet is not None
        assert writer.write.call_count == 1
