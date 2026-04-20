"""Tests for logging output in pycubrid modules."""

from __future__ import annotations

import logging
import struct
from unittest.mock import MagicMock, patch

import pytest

from pycubrid.connection import Connection
from pycubrid.cursor import Cursor
from pycubrid.lob import Lob


def build_handshake_response(port: int = 0) -> bytes:
    return struct.pack(">i", port)


def build_open_db_response(cas_info: bytes = b"\x01\x01\x02\x03", session_id: int = 1234) -> bytes:
    body = cas_info + struct.pack(">i", 0)
    body += b"\x00" * 8
    body += struct.pack(">i", session_id)
    data_length = struct.pack(">i", len(body) - 4)
    return data_length + body


def build_simple_ok_response(cas_info: bytes = b"\x01\x01\x02\x03") -> bytes:
    body = cas_info + struct.pack(">i", 0)
    return struct.pack(">i", len(body) - 4) + body


def make_socket(recv_chunks: list[bytes]) -> MagicMock:
    sock = MagicMock()
    sock.recv.side_effect = recv_chunks

    def _recv_into(buffer: memoryview | bytearray, nbytes: int = 0) -> int:
        chunk = sock.recv(nbytes)
        n = len(chunk)
        buffer[:n] = chunk
        return n

    sock.recv_into.side_effect = _recv_into
    return sock


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


def make_connected_connection(socket_queue: list[MagicMock]) -> tuple[Connection, MagicMock]:
    open_db = build_open_db_response()
    sock = make_socket([build_handshake_response(), open_db[:4], open_db[4:]])
    socket_queue.append(sock)
    conn = Connection("localhost", 33000, "testdb", "dba", "")
    return conn, sock


class TestConnectionLogging:
    """Verify that Connection emits expected DEBUG log messages."""

    def test_connect_logs_host_and_db(
        self, caplog: pytest.LogCaptureFixture, socket_queue: list[MagicMock]
    ) -> None:
        """connect() should log host, port, and database."""
        open_db = build_open_db_response()
        sock = make_socket([build_handshake_response(), open_db[:4], open_db[4:]])
        socket_queue.append(sock)

        with caplog.at_level(logging.DEBUG, logger="pycubrid.connection"):
            Connection("localhost", 33000, "testdb", "dba", "")

        assert any("localhost" in m and "33000" in m for m in caplog.messages)

    def test_close_logs(
        self, caplog: pytest.LogCaptureFixture, socket_queue: list[MagicMock]
    ) -> None:
        """close() should log a debug message."""
        conn, sock = make_connected_connection(socket_queue)
        close_resp = build_simple_ok_response()
        sock.recv.side_effect = [close_resp[:4], close_resp[4:]]
        caplog.clear()

        with caplog.at_level(logging.DEBUG, logger="pycubrid.connection"):
            conn.close()

        assert any("Closing" in m or "close" in m.lower() for m in caplog.messages)

    def test_commit_logs(
        self, caplog: pytest.LogCaptureFixture, socket_queue: list[MagicMock]
    ) -> None:
        """commit() should log."""
        conn, sock = make_connected_connection(socket_queue)
        resp = build_simple_ok_response()
        sock.recv.side_effect = [resp[:4], resp[4:]]
        caplog.clear()

        with caplog.at_level(logging.DEBUG, logger="pycubrid.connection"):
            conn.commit()

        assert any("commit" in m for m in caplog.messages)

    def test_rollback_logs(
        self, caplog: pytest.LogCaptureFixture, socket_queue: list[MagicMock]
    ) -> None:
        """rollback() should log."""
        conn, sock = make_connected_connection(socket_queue)
        resp = build_simple_ok_response()
        sock.recv.side_effect = [resp[:4], resp[4:]]
        caplog.clear()

        with caplog.at_level(logging.DEBUG, logger="pycubrid.connection"):
            conn.rollback()

        assert any("rollback" in m for m in caplog.messages)

    def test_autocommit_logs(
        self, caplog: pytest.LogCaptureFixture, socket_queue: list[MagicMock]
    ) -> None:
        """Setting autocommit should log the new value."""
        conn, sock = make_connected_connection(socket_queue)
        resp = build_simple_ok_response()
        sock.recv.side_effect = [resp[:4], resp[4:], resp[:4], resp[4:]]
        caplog.clear()

        with caplog.at_level(logging.DEBUG, logger="pycubrid.connection"):
            conn.autocommit = True

        assert any("autocommit" in m for m in caplog.messages)

    def test_send_recv_logs_byte_count(
        self, caplog: pytest.LogCaptureFixture, socket_queue: list[MagicMock]
    ) -> None:
        """send/recv should log byte counts at DEBUG level."""
        conn, sock = make_connected_connection(socket_queue)
        resp = build_simple_ok_response()
        sock.recv.side_effect = [resp[:4], resp[4:]]
        caplog.clear()

        with caplog.at_level(logging.DEBUG, logger="pycubrid.connection"):
            conn.commit()

        byte_logs = [m for m in caplog.messages if "bytes" in m]
        assert len(byte_logs) >= 1


class TestCursorLogging:
    """Verify that Cursor emits expected DEBUG log messages."""

    def test_close_logs(self, caplog: pytest.LogCaptureFixture) -> None:
        """cursor.close() should log the query handle."""
        conn = MagicMock()
        cursor = Cursor(conn)
        cursor._query_handle = 42

        with caplog.at_level(logging.DEBUG, logger="pycubrid.cursor"):
            cursor.close()

        assert any("close" in m.lower() and "42" in m for m in caplog.messages)

    def test_execute_logs_statement_info(self, caplog: pytest.LogCaptureFixture) -> None:
        """execute() should log statement type, cols, rows after execution."""
        conn = MagicMock()
        cursor = Cursor(conn)

        def fake_send_and_receive(p: object) -> None:
            p.statement_type = 1
            p.column_count = 3
            p.total_tuple_count = 10
            p.query_handle = 99
            p.columns = []
            p.cache_time = 0
            p.updatable = False

        conn._send_and_receive.side_effect = fake_send_and_receive

        with caplog.at_level(logging.DEBUG, logger="pycubrid.cursor"):
            cursor.execute("SELECT 1")

        assert any("execute" in m and "cols=3" in m for m in caplog.messages)

    def test_executemany_logs_batch_size(self, caplog: pytest.LogCaptureFixture) -> None:
        """executemany() should log batch size."""
        conn = MagicMock()
        cursor = Cursor(conn)

        with patch.object(cursor, "execute"):
            with caplog.at_level(logging.DEBUG, logger="pycubrid.cursor"):
                cursor.executemany("INSERT INTO t VALUES (?)", [(1,), (2,), (3,)])

        assert any("executemany" in m and "3" in m for m in caplog.messages)


class TestLobLogging:
    """Verify that Lob.create() emits expected DEBUG log messages."""

    def test_create_logs(self, caplog: pytest.LogCaptureFixture) -> None:
        """Lob.create() should log creation info."""
        from pycubrid.constants import CUBRIDDataType

        conn = MagicMock()

        def fake_send_and_receive(p: object) -> None:
            p.lob_handle = b"\x00" * 16

        conn._send_and_receive.side_effect = fake_send_and_receive
        conn._ensure_connected.return_value = None

        with caplog.at_level(logging.DEBUG, logger="pycubrid.lob"):
            Lob.create(conn, CUBRIDDataType.BLOB)

        assert any("LOB created" in m for m in caplog.messages)


class TestLoggingDoesNotLeakParameters:
    """Ensure that SQL parameters are never logged (security)."""

    def test_execute_does_not_log_param_values(self, caplog: pytest.LogCaptureFixture) -> None:
        """Parameter values must NOT appear in log output."""
        conn = MagicMock()
        cursor = Cursor(conn)

        def fake_send_and_receive(p: object) -> None:
            p.statement_type = 1
            p.column_count = 1
            p.total_tuple_count = 1
            p.query_handle = 1
            p.columns = []
            p.cache_time = 0
            p.updatable = False

        conn._send_and_receive.side_effect = fake_send_and_receive

        secret = "super_secret_password_12345"
        with caplog.at_level(logging.DEBUG, logger="pycubrid.cursor"):
            cursor.execute("SELECT * FROM users WHERE password = ?", (secret,))

        full_log = "\n".join(caplog.messages)
        assert secret not in full_log
