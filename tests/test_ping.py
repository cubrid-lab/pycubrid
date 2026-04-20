"""Tests for Connection.ping() using native CHECK_CAS (FC=32)."""

from __future__ import annotations

import struct
from unittest.mock import MagicMock

import pytest

from pycubrid.constants import CASFunctionCode, DataSize
from pycubrid.protocol import CheckCasPacket

from .test_connection import (
    build_handshake_response,
    build_open_db_response,
    build_simple_ok_response,
    make_connected_connection,
    make_socket,
)


@pytest.fixture
def socket_queue(monkeypatch: pytest.MonkeyPatch) -> list[MagicMock]:
    queue: list[MagicMock] = []

    def fake_create_connection(*args: object, **kwargs: object) -> MagicMock:
        del args, kwargs
        if not queue:
            raise AssertionError("socket queue is empty")
        return queue.pop(0)

    monkeypatch.setattr("socket.create_connection", fake_create_connection)
    return queue


class TestCheckCasPacket:
    def test_write_contains_function_code(self) -> None:
        packet = CheckCasPacket()
        cas_info = b"\x01\x02\x03\x04"
        data = packet.write(cas_info)
        assert len(data) > 8
        assert data[8] == CASFunctionCode.CHECK_CAS

    def test_parse_success(self) -> None:
        cas_info = b"\x01\x02\x03\x04"
        body = cas_info + struct.pack(">i", 0)
        packet = CheckCasPacket()
        packet.parse(body)
        assert packet.response_code == 0

    def test_parse_positive_response(self) -> None:
        cas_info = b"\x01\x02\x03\x04"
        body = cas_info + struct.pack(">i", 42)
        packet = CheckCasPacket()
        packet.parse(body)
        assert packet.response_code == 42

    def test_parse_negative_response(self) -> None:
        cas_info = b"\x01\x02\x03\x04"
        body = cas_info + struct.pack(">i", -1)
        packet = CheckCasPacket()
        packet.parse(body)
        assert packet.response_code == -1


class TestConnectionPing:
    def test_ping_success(self, socket_queue: list[MagicMock]) -> None:
        conn, sock = make_connected_connection(socket_queue)
        ok_resp = build_simple_ok_response()
        sock.recv.side_effect = [ok_resp[:4], ok_resp[4:]]
        assert conn.ping() is True

    def test_ping_negative_response(self, socket_queue: list[MagicMock]) -> None:
        conn, sock = make_connected_connection(socket_queue)
        cas_info = b"\x01\x01\x02\x03"
        body = cas_info + struct.pack(">i", -1)
        resp = struct.pack(">i", len(body) - DataSize.CAS_INFO) + body
        sock.recv.side_effect = [resp[:4], resp[4:]]
        assert conn.ping() is False

    def test_ping_on_closed_connection_reconnects(self, socket_queue: list[MagicMock]) -> None:
        conn, _ = make_connected_connection(socket_queue)
        conn._connected = False
        conn._socket = None

        open_db = build_open_db_response()
        reconnect_sock = make_socket(
            [
                build_handshake_response(),
                open_db[:4],
                open_db[4:],
            ]
        )
        socket_queue.append(reconnect_sock)
        assert conn.ping(reconnect=True) is True
        assert conn._connected is True

    def test_ping_on_closed_connection_no_reconnect(self, socket_queue: list[MagicMock]) -> None:
        conn, _ = make_connected_connection(socket_queue)
        conn._connected = False
        assert conn.ping(reconnect=False) is False

    def test_ping_socket_error_with_reconnect(self, socket_queue: list[MagicMock]) -> None:
        conn, sock = make_connected_connection(socket_queue)
        sock.sendall.side_effect = OSError("broken pipe")

        open_db = build_open_db_response()
        reconnect_sock = make_socket(
            [
                build_handshake_response(),
                open_db[:4],
                open_db[4:],
            ]
        )
        socket_queue.append(reconnect_sock)
        assert conn.ping(reconnect=True) is True

    def test_ping_socket_error_no_reconnect(self, socket_queue: list[MagicMock]) -> None:
        conn, sock = make_connected_connection(socket_queue)
        sock.sendall.side_effect = OSError("broken pipe")
        assert conn.ping(reconnect=False) is False

    def test_ping_reconnect_also_fails(
        self,
        socket_queue: list[MagicMock],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        conn, sock = make_connected_connection(socket_queue)
        sock.sendall.side_effect = OSError("broken pipe")

        original = socket_queue.pop

        def fake_create_connection(*args: object, **kwargs: object) -> MagicMock:
            del args, kwargs
            if socket_queue:
                return original(0)
            raise OSError("still broken")

        monkeypatch.setattr("socket.create_connection", fake_create_connection)
        assert conn.ping(reconnect=True) is False
