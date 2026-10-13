"""Sync/async parity for failed ping() reconnects and broker redirect ports (#896)."""

from __future__ import annotations

import struct
from unittest.mock import MagicMock, patch

import pytest

from pycubrid.aio.connection import AsyncConnection
from pycubrid.connection import Connection
from pycubrid.constants import DataSize
from pycubrid.exceptions import OperationalError

from .test_async import make_mock_stream_pair
from .test_connection import (
    build_handshake_response,
    build_open_db_response,
    make_connected_connection,
    make_socket,
)

_WRONG_PASSWORD = b"Incorrect or missing password.\x00"


def build_open_db_error(code: int = -171, message: bytes = _WRONG_PASSWORD) -> bytes:
    """OPEN_DATABASE reply rejected by the server (DBMS_ERROR_INDICATOR, code, text)."""
    body = b"\x01\x01\x02\x03" + struct.pack(">ii", -2, code) + message
    return struct.pack(">i", len(body) - DataSize.CAS_INFO) + body


@pytest.fixture
def connected_ports() -> list[int]:
    return []


@pytest.fixture
def socket_queue(monkeypatch: pytest.MonkeyPatch, connected_ports: list[int]) -> list[MagicMock]:
    queue: list[MagicMock] = []

    def fake_create_connection(
        address: tuple[str, int], *args: object, **kwargs: object
    ) -> MagicMock:
        del args, kwargs
        connected_ports.append(address[1])
        if not queue:
            raise AssertionError(f"unexpected connection to port {address[1]}")
        return queue.pop(0)

    monkeypatch.setattr("socket.create_connection", fake_create_connection)
    return queue


def _rejecting_socket() -> MagicMock:
    reply = build_open_db_error()
    return make_socket([build_handshake_response(), reply[:4], reply[4:]])


class TestSyncPingReconnectRejected:
    def test_closed_connection_reconnect_rejected_returns_false(
        self, socket_queue: list[MagicMock]
    ) -> None:
        conn, _ = make_connected_connection(socket_queue)
        conn._connected = False
        conn._socket = None
        socket_queue.append(_rejecting_socket())

        assert conn.ping(reconnect=True) is False
        assert conn._connected is False

    def test_check_cas_failure_reconnect_rejected_returns_false(
        self, socket_queue: list[MagicMock]
    ) -> None:
        conn, sock = make_connected_connection(socket_queue)
        sock.sendall.side_effect = OSError("broken pipe")
        socket_queue.append(_rejecting_socket())

        assert conn.ping(reconnect=True) is False
        assert conn._connected is False


class TestAsyncPingReconnectRejected:
    @pytest.mark.asyncio
    async def test_closed_connection_reconnect_rejected_returns_false(self) -> None:
        conn = AsyncConnection("localhost", 33000, "testdb", "dba", "", no_backslash_escapes=True)
        reply = build_open_db_error()
        streams = make_mock_stream_pair([build_handshake_response(), reply[:4], reply[4:]])

        with patch.object(conn, "_open_connection", return_value=streams[:2]):
            assert await conn.ping(reconnect=True) is False
        assert conn._connected is False


class TestSyncRedirectPortRange:
    @pytest.mark.parametrize("port", [65536, 70000, 2**31 - 1])
    def test_out_of_range_redirect_raises_operational_error(
        self, socket_queue: list[MagicMock], connected_ports: list[int], port: int
    ) -> None:
        handshake_sock = make_socket([build_handshake_response(port)])
        socket_queue.append(handshake_sock)

        with pytest.raises(OperationalError, match=f"invalid port {port}"):
            Connection("localhost", 33000, "testdb", "dba", "")

        assert connected_ports == [33000]
        handshake_sock.close.assert_called()

    def test_highest_valid_redirect_port_is_used(
        self, socket_queue: list[MagicMock], connected_ports: list[int]
    ) -> None:
        open_db = build_open_db_response()
        socket_queue.extend(
            [
                make_socket([build_handshake_response(65535)]),
                make_socket([open_db[:4], open_db[4:]]),
            ]
        )

        conn = Connection("localhost", 33000, "testdb", "dba", "", no_backslash_escapes=True)

        assert conn._connected is True
        assert connected_ports == [33000, 65535]


class TestAsyncRedirectPortRange:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("port", [65536, 70000, 2**31 - 1])
    async def test_out_of_range_redirect_raises_operational_error(self, port: int) -> None:
        conn = AsyncConnection("127.0.0.1", 33000, "testdb", "dba", "", no_backslash_escapes=True)
        reader, writer, _ = make_mock_stream_pair([build_handshake_response(port)])
        real_open = AsyncConnection._open_connection
        calls: list[int] = []

        async def open_connection(host: str, target_port: int) -> object:
            calls.append(target_port)
            if len(calls) == 1:
                return reader, writer
            # The real path: asyncio raises a raw OverflowError for this port.
            return await real_open(conn, host, target_port)

        with patch.object(conn, "_open_connection", side_effect=open_connection):
            with pytest.raises(OperationalError, match=f"invalid port {port}"):
                await conn.connect()

        assert calls == [33000]
        assert conn._connected is False
        writer.close.assert_called()
