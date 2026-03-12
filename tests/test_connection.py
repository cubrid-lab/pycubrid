from __future__ import annotations

import struct
import sys
import types
from collections.abc import Callable
from unittest.mock import MagicMock, patch

import pytest

from pycubrid.connection import Connection
from pycubrid.constants import CASFunctionCode, CUBRIDDataType
from pycubrid.exceptions import InterfaceError, OperationalError
from pycubrid.protocol import GetSchemaPacket


def build_handshake_response(port: int = 0) -> bytes:
    return struct.pack(">i", port)


def build_open_db_response(cas_info: bytes = b"\x00\x01\x02\x03", session_id: int = 1234) -> bytes:
    body = cas_info + struct.pack(">i", 0)
    body += b"\x00" * 8
    body += struct.pack(">i", session_id)
    data_length = struct.pack(">i", len(body))
    return data_length + body


def build_simple_ok_response(cas_info: bytes = b"\x00\x01\x02\x03") -> bytes:
    body = cas_info + struct.pack(">i", 0)
    return struct.pack(">i", len(body)) + body


def build_server_version_response(version: str, cas_info: bytes = b"\x00\x01\x02\x03") -> bytes:
    payload = version.encode("utf-8") + b"\x00"
    body = cas_info + struct.pack(">i", len(payload)) + payload
    return struct.pack(">i", len(body)) + body


def build_last_insert_id_response(
    last_insert_id: str, cas_info: bytes = b"\x00\x01\x02\x03"
) -> bytes:
    payload = last_insert_id.encode("utf-8") + b"\x00"
    body = cas_info + struct.pack(">i", len(payload)) + payload
    return struct.pack(">i", len(body)) + body


@pytest.fixture
def cursor_module(monkeypatch: pytest.MonkeyPatch) -> type:
    module = types.ModuleType("pycubrid.cursor")

    class DummyCursor:
        def __init__(self, connection: Connection) -> None:
            self.connection = connection
            self.closed = False

        def close(self) -> None:
            self.closed = True

    setattr(module, "Cursor", DummyCursor)
    monkeypatch.setitem(sys.modules, "pycubrid.cursor", module)
    return DummyCursor


@pytest.fixture
def socket_queue(monkeypatch: pytest.MonkeyPatch) -> list[MagicMock]:
    queue: list[MagicMock] = []

    def fake_socket(*args: object, **kwargs: object) -> MagicMock:
        del args, kwargs
        if not queue:
            raise AssertionError("socket queue is empty")
        return queue.pop(0)

    monkeypatch.setattr("socket.socket", fake_socket)
    return queue


def make_socket(recv_chunks: list[bytes]) -> MagicMock:
    sock = MagicMock()
    sock.recv.side_effect = recv_chunks
    return sock


def make_connected_connection(socket_queue: list[MagicMock]) -> tuple[Connection, MagicMock]:
    open_db = build_open_db_response()
    sock = make_socket(
        [
            build_handshake_response(),
            open_db[:4],
            open_db[4:],
        ]
    )
    socket_queue.append(sock)
    conn = Connection("localhost", 33000, "testdb", "dba", "")
    return conn, sock


class TestConnectionEstablishment:
    def test_connect_success(self, socket_queue: list[MagicMock]) -> None:
        open_db = build_open_db_response(session_id=777)
        sock = make_socket([build_handshake_response(), open_db[:4], open_db[4:]])
        socket_queue.append(sock)

        conn = Connection("localhost", 33000, "testdb", "dba", "")

        assert conn._connected is True
        assert conn._session_id == 777
        assert conn._cas_info == b"\x00\x01\x02\x03"
        assert sock.connect.call_args[0][0] == ("localhost", 33000)

    def test_connect_with_port_redirection(self, socket_queue: list[MagicMock]) -> None:
        first_sock = make_socket([build_handshake_response(33100)])
        open_db = build_open_db_response()
        second_sock = make_socket([open_db[:4], open_db[4:]])
        socket_queue.extend([first_sock, second_sock])

        conn = Connection("localhost", 33000, "testdb", "dba", "")

        assert conn._connected is True
        assert first_sock.close.called
        assert second_sock.connect.call_args[0][0] == ("localhost", 33100)

    def test_connect_failure_raises_operational_error(self, socket_queue: list[MagicMock]) -> None:
        sock = MagicMock()
        sock.connect.side_effect = OSError("boom")
        socket_queue.append(sock)

        with pytest.raises(OperationalError, match="failed to connect"):
            Connection("localhost", 33000, "testdb", "dba", "")

    def test_connect_timeout_applied(self, socket_queue: list[MagicMock]) -> None:
        open_db = build_open_db_response()
        sock = make_socket([build_handshake_response(), open_db[:4], open_db[4:]])
        socket_queue.append(sock)

        Connection("localhost", 33000, "testdb", "dba", "", connect_timeout=1.5)

        sock.settimeout.assert_called_once_with(1.5)

    def test_connect_no_op_when_already_connected(self, socket_queue: list[MagicMock]) -> None:
        conn, _ = make_connected_connection(socket_queue)
        conn.connect()
        assert conn._connected is True


class TestConnectionLifecycle:
    def test_close_sends_close_packet_and_closes_socket(
        self,
        socket_queue: list[MagicMock],
        cursor_module: type,
    ) -> None:
        conn, sock = make_connected_connection(socket_queue)
        close_frame = build_simple_ok_response(conn._cas_info)
        sock.recv.side_effect = list(sock.recv.side_effect) + [close_frame[:4], close_frame[4:]]

        cur1 = cursor_module(conn)
        cur2 = cursor_module(conn)
        conn._cursors.update({cur1, cur2})

        conn.close()

        close_packet = sock.sendall.call_args_list[-1].args[0]
        assert close_packet[8] == CASFunctionCode.CON_CLOSE
        assert cur1.closed is True
        assert cur2.closed is True
        assert conn._connected is False
        assert sock.close.called

    def test_close_is_noop_when_already_closed(self, socket_queue: list[MagicMock]) -> None:
        conn, sock = make_connected_connection(socket_queue)
        close_frame = build_simple_ok_response(conn._cas_info)
        sock.recv.side_effect = list(sock.recv.side_effect) + [close_frame[:4], close_frame[4:]]
        conn.close()

        sendall_calls = sock.sendall.call_count
        conn.close()
        assert sock.sendall.call_count == sendall_calls

    def test_close_swallows_close_packet_errors(self, socket_queue: list[MagicMock]) -> None:
        conn, sock = make_connected_connection(socket_queue)
        sock.sendall.side_effect = [None, None, OSError("close failure")]

        conn.close()

        assert conn._connected is False
        assert sock.close.called

    def test_close_swallows_cursor_close_errors(self, socket_queue: list[MagicMock]) -> None:
        conn, sock = make_connected_connection(socket_queue)
        close_frame = build_simple_ok_response(conn._cas_info)
        sock.recv.side_effect = list(sock.recv.side_effect) + [close_frame[:4], close_frame[4:]]

        class BadCursor:
            def close(self) -> None:
                raise RuntimeError("cursor close failed")

        conn._cursors.add(BadCursor())

        conn.close()

        assert conn._connected is False


class TestTransactions:
    def test_commit_sends_commit_packet(self, socket_queue: list[MagicMock]) -> None:
        conn, sock = make_connected_connection(socket_queue)
        frame = build_simple_ok_response(conn._cas_info)
        sock.recv.side_effect = list(sock.recv.side_effect) + [frame[:4], frame[4:]]

        conn.commit()

        packet = sock.sendall.call_args_list[-1].args[0]
        assert packet[8] == CASFunctionCode.END_TRAN

    def test_rollback_sends_rollback_packet(self, socket_queue: list[MagicMock]) -> None:
        conn, sock = make_connected_connection(socket_queue)
        frame = build_simple_ok_response(conn._cas_info)
        sock.recv.side_effect = list(sock.recv.side_effect) + [frame[:4], frame[4:]]

        conn.rollback()

        packet = sock.sendall.call_args_list[-1].args[0]
        assert packet[8] == CASFunctionCode.END_TRAN


class TestCursorAndAutocommit:
    def test_cursor_creates_and_tracks_cursor(
        self,
        socket_queue: list[MagicMock],
        cursor_module: type,
    ) -> None:
        conn, _ = make_connected_connection(socket_queue)
        cur = conn.cursor()

        assert isinstance(cur, cursor_module)
        assert cur in conn._cursors

    def test_autocommit_getter_setter(self, socket_queue: list[MagicMock]) -> None:
        conn, sock = make_connected_connection(socket_queue)
        set_frame = build_simple_ok_response(conn._cas_info)
        commit_frame = build_simple_ok_response(conn._cas_info)
        sock.recv.side_effect = list(sock.recv.side_effect) + [
            set_frame[:4],
            set_frame[4:],
            commit_frame[:4],
            commit_frame[4:],
        ]

        conn.autocommit = True

        sent_packets = [call.args[0] for call in sock.sendall.call_args_list[-2:]]
        assert sent_packets[0][8] == CASFunctionCode.SET_DB_PARAMETER
        assert sent_packets[1][8] == CASFunctionCode.END_TRAN
        assert conn.autocommit is True

    def test_init_with_autocommit_true(self, socket_queue: list[MagicMock]) -> None:
        open_db = build_open_db_response()
        set_frame = build_simple_ok_response()
        commit_frame = build_simple_ok_response()
        sock = make_socket(
            [
                build_handshake_response(),
                open_db[:4],
                open_db[4:],
                set_frame[:4],
                set_frame[4:],
                commit_frame[:4],
                commit_frame[4:],
            ]
        )
        socket_queue.append(sock)

        conn = Connection("localhost", 33000, "testdb", "dba", "", autocommit=True)

        assert conn.autocommit is True


class TestMetadataMethods:
    def test_get_server_version(self, socket_queue: list[MagicMock]) -> None:
        conn, sock = make_connected_connection(socket_queue)
        frame = build_server_version_response("11.2.0.0194", conn._cas_info)
        sock.recv.side_effect = list(sock.recv.side_effect) + [frame[:4], frame[4:]]

        version = conn.get_server_version()

        assert version == "11.2.0.0194"

    def test_get_last_insert_id(self, socket_queue: list[MagicMock]) -> None:
        conn, sock = make_connected_connection(socket_queue)
        frame = build_last_insert_id_response("42", conn._cas_info)
        sock.recv.side_effect = list(sock.recv.side_effect) + [frame[:4], frame[4:]]

        last_id = conn.get_last_insert_id()

        assert last_id == "42"


class TestAdvancedMethods:
    def test_create_lob_delegates_to_lob_create(self, socket_queue: list[MagicMock]) -> None:
        conn, _ = make_connected_connection(socket_queue)
        sentinel_lob = object()

        with patch("pycubrid.lob.Lob.create", return_value=sentinel_lob) as create_mock:
            result = conn.create_lob(CUBRIDDataType.BLOB)

        assert result is sentinel_lob
        create_mock.assert_called_once_with(conn, CUBRIDDataType.BLOB)

    def test_get_schema_info_sends_get_schema_packet(self, socket_queue: list[MagicMock]) -> None:
        conn, _ = make_connected_connection(socket_queue)

        def send_and_receive(packet: object) -> object:
            assert isinstance(packet, GetSchemaPacket)
            packet.query_handle = 77
            packet.tuple_count = 3
            return packet

        conn._send_and_receive = MagicMock(side_effect=send_and_receive)

        packet = conn.get_schema_info(schema_type=1, table_name="users", pattern_match_flag=0)

        assert isinstance(packet, GetSchemaPacket)
        assert packet.schema_type == 1
        assert packet.table_name == "users"
        assert packet.pattern_match_flag == 0
        assert packet.query_handle == 77
        assert packet.tuple_count == 3

    def test_create_lob_raises_interface_error_when_closed(
        self, socket_queue: list[MagicMock]
    ) -> None:
        conn, sock = make_connected_connection(socket_queue)
        close_frame = build_simple_ok_response(conn._cas_info)
        sock.recv.side_effect = list(sock.recv.side_effect) + [close_frame[:4], close_frame[4:]]
        conn.close()

        with pytest.raises(InterfaceError, match="connection is closed"):
            conn.create_lob(CUBRIDDataType.CLOB)

    def test_get_schema_info_raises_interface_error_when_closed(
        self, socket_queue: list[MagicMock]
    ) -> None:
        conn, sock = make_connected_connection(socket_queue)
        close_frame = build_simple_ok_response(conn._cas_info)
        sock.recv.side_effect = list(sock.recv.side_effect) + [close_frame[:4], close_frame[4:]]
        conn.close()

        with pytest.raises(InterfaceError, match="connection is closed"):
            conn.get_schema_info(1)


class TestContextManager:
    def test_context_manager_commits_on_success(self, socket_queue: list[MagicMock]) -> None:
        conn, _ = make_connected_connection(socket_queue)
        conn.commit = MagicMock()
        conn.rollback = MagicMock()
        conn.close = MagicMock()

        with conn as ctx_conn:
            assert ctx_conn is conn

        conn.commit.assert_called_once()
        conn.rollback.assert_not_called()
        conn.close.assert_called_once()

    def test_context_manager_rolls_back_on_exception(self, socket_queue: list[MagicMock]) -> None:
        conn, _ = make_connected_connection(socket_queue)
        conn.commit = MagicMock()
        conn.rollback = MagicMock()
        conn.close = MagicMock()

        with pytest.raises(RuntimeError, match="boom"):
            with conn:
                raise RuntimeError("boom")

        conn.commit.assert_not_called()
        conn.rollback.assert_called_once()
        conn.close.assert_called_once()


class TestErrorHandling:
    _Operation = Callable[[Connection], object]

    @pytest.mark.parametrize(
        "operation",
        [
            lambda c: c.commit(),
            lambda c: c.rollback(),
            lambda c: c.cursor(),
            lambda c: c.get_server_version(),
            lambda c: c.get_last_insert_id(),
            lambda c: c.__enter__(),
            lambda c: c._check_closed(),
            lambda c: c.autocommit,
            lambda c: setattr(c, "autocommit", True),
        ],
    )
    def test_methods_raise_interface_error_after_close(
        self,
        socket_queue: list[MagicMock],
        operation: _Operation,
    ) -> None:
        conn, sock = make_connected_connection(socket_queue)
        close_frame = build_simple_ok_response(conn._cas_info)
        sock.recv.side_effect = list(sock.recv.side_effect) + [close_frame[:4], close_frame[4:]]
        conn.close()

        with pytest.raises(InterfaceError, match="connection is closed"):
            operation(conn)

    def test_send_and_receive_raises_operational_error_on_send_failure(
        self,
        socket_queue: list[MagicMock],
    ) -> None:
        conn, sock = make_connected_connection(socket_queue)
        sock.sendall.side_effect = OSError("write fail")

        with pytest.raises(OperationalError, match="socket communication failed"):
            conn.commit()

    def test_send_and_receive_raises_interface_error_when_socket_none(
        self,
        socket_queue: list[MagicMock],
    ) -> None:
        conn, _ = make_connected_connection(socket_queue)
        conn._socket = None

        with pytest.raises(InterfaceError, match="connection is closed"):
            conn.commit()

    def test_recv_exact_raises_operational_error_on_empty_chunk(
        self,
        socket_queue: list[MagicMock],
    ) -> None:
        conn, _ = make_connected_connection(socket_queue)
        bad_sock = MagicMock()
        bad_sock.recv.side_effect = [b""]

        with pytest.raises(OperationalError, match="connection lost during receive"):
            conn._recv_exact(bad_sock, 1)


class TestSendAndReceiveFraming:
    def test_send_and_receive_handles_partial_reads(self, socket_queue: list[MagicMock]) -> None:
        conn, sock = make_connected_connection(socket_queue)

        class DummyPacket:
            def __init__(self) -> None:
                self.parsed_data: bytes | None = None

            def write(self, cas_info: bytes) -> bytes:
                assert cas_info == conn._cas_info
                return b"REQ"

            def parse(self, data: bytes) -> None:
                self.parsed_data = data

        body = conn._cas_info + struct.pack(">i", 0)
        frame = struct.pack(">i", len(body)) + body
        sock.recv.side_effect = list(sock.recv.side_effect) + [
            frame[:2],
            frame[2:4],
            frame[4:9],
            frame[9:],
        ]

        packet = DummyPacket()
        result = conn._send_and_receive(packet)

        assert result is packet
        assert packet.parsed_data == body
        assert sock.sendall.call_args_list[-1].args[0] == b"REQ"
