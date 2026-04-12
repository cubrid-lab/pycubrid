from __future__ import annotations

import struct
import sys
import threading
import types
from unittest.mock import MagicMock

import pytest

from pycubrid.connection import Connection
from pycubrid.cursor import Cursor
from pycubrid.timing import TimingStats


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


def _make_connected(
    socket_queue: list[MagicMock],
    *,
    enable_timing: bool = False,
) -> tuple[Connection, MagicMock]:
    open_db = build_open_db_response()
    sock = make_socket([build_handshake_response(), open_db[:4], open_db[4:]])
    socket_queue.append(sock)
    conn = Connection("localhost", 33000, "testdb", "dba", "", enable_timing=enable_timing)
    return conn, sock


class TestTimingStats:
    def test_initial_state(self) -> None:
        stats = TimingStats()
        assert stats.connect_total_ns == 0
        assert stats.connect_count == 0
        assert stats.execute_total_ns == 0
        assert stats.execute_count == 0
        assert stats.fetch_total_ns == 0
        assert stats.fetch_count == 0
        assert stats.close_total_ns == 0
        assert stats.close_count == 0

    def test_record_connect(self) -> None:
        stats = TimingStats()
        stats.record_connect(1_000_000)
        stats.record_connect(2_000_000)
        assert stats.connect_count == 2
        assert stats.connect_total_ns == 3_000_000

    def test_record_execute(self) -> None:
        stats = TimingStats()
        stats.record_execute(500_000)
        assert stats.execute_count == 1
        assert stats.execute_total_ns == 500_000

    def test_record_fetch(self) -> None:
        stats = TimingStats()
        stats.record_fetch(750_000)
        stats.record_fetch(250_000)
        stats.record_fetch(1_000_000)
        assert stats.fetch_count == 3
        assert stats.fetch_total_ns == 2_000_000

    def test_record_close(self) -> None:
        stats = TimingStats()
        stats.record_close(100_000)
        assert stats.close_count == 1
        assert stats.close_total_ns == 100_000

    def test_reset(self) -> None:
        stats = TimingStats()
        stats.record_connect(1_000_000)
        stats.record_execute(2_000_000)
        stats.record_fetch(3_000_000)
        stats.record_close(4_000_000)
        stats.reset()
        assert stats.connect_total_ns == 0
        assert stats.connect_count == 0
        assert stats.execute_total_ns == 0
        assert stats.execute_count == 0
        assert stats.fetch_total_ns == 0
        assert stats.fetch_count == 0
        assert stats.close_total_ns == 0
        assert stats.close_count == 0

    def test_repr_zero_calls(self) -> None:
        stats = TimingStats()
        r = repr(stats)
        assert "0 calls" in r
        assert "TimingStats(" in r

    def test_repr_with_data(self) -> None:
        stats = TimingStats()
        stats.record_connect(10_000_000)
        stats.record_execute(5_000_000)
        stats.record_execute(5_000_000)
        r = repr(stats)
        assert "1 calls" in r
        assert "2 calls" in r
        assert "10.000ms total" in r
        assert "5.000ms avg" in r

    def test_fmt_ns_zero_calls(self) -> None:
        assert TimingStats._fmt_ns(0, 0) == "0 calls"

    def test_fmt_ns_with_values(self) -> None:
        result = TimingStats._fmt_ns(10_000_000, 2)
        assert "2 calls" in result
        assert "10.000ms total" in result
        assert "5.000ms avg" in result

    def test_thread_safety(self) -> None:
        stats = TimingStats()
        iterations = 1000
        errors: list[Exception] = []

        def record_many(method_name: str) -> None:
            try:
                method = getattr(stats, method_name)
                for _ in range(iterations):
                    method(1)
            except Exception as exc:
                errors.append(exc)

        threads = [
            threading.Thread(target=record_many, args=("record_connect",)),
            threading.Thread(target=record_many, args=("record_execute",)),
            threading.Thread(target=record_many, args=("record_fetch",)),
            threading.Thread(target=record_many, args=("record_close",)),
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors
        assert stats.connect_count == iterations
        assert stats.execute_count == iterations
        assert stats.fetch_count == iterations
        assert stats.close_count == iterations
        assert stats.connect_total_ns == iterations
        assert stats.execute_total_ns == iterations
        assert stats.fetch_total_ns == iterations
        assert stats.close_total_ns == iterations


class TestConnectionTimingDisabled:
    def test_timing_none_by_default(
        self,
        socket_queue: list[MagicMock],
        cursor_module: type,
    ) -> None:
        conn, _ = _make_connected(socket_queue, enable_timing=False)
        assert conn._timing is None
        assert conn.timing_stats is None

    def test_no_timing_overhead_when_disabled(
        self,
        socket_queue: list[MagicMock],
        cursor_module: type,
    ) -> None:
        conn, sock = _make_connected(socket_queue, enable_timing=False)
        assert conn.timing_stats is None

        ok = build_simple_ok_response()
        sock.recv.side_effect = [ok[:4], ok[4:]]
        conn.close()
        assert conn.timing_stats is None


class TestConnectionTimingEnabled:
    def test_timing_enabled_via_kwarg(
        self,
        socket_queue: list[MagicMock],
        cursor_module: type,
    ) -> None:
        conn, _ = _make_connected(socket_queue, enable_timing=True)
        assert conn._timing is not None
        assert isinstance(conn.timing_stats, TimingStats)

    def test_connect_recorded(
        self,
        socket_queue: list[MagicMock],
        cursor_module: type,
    ) -> None:
        conn, _ = _make_connected(socket_queue, enable_timing=True)
        stats = conn.timing_stats
        assert stats is not None
        assert stats.connect_count == 1
        assert stats.connect_total_ns > 0

    def test_close_recorded(
        self,
        socket_queue: list[MagicMock],
        cursor_module: type,
    ) -> None:
        conn, sock = _make_connected(socket_queue, enable_timing=True)
        ok = build_simple_ok_response()
        sock.recv.side_effect = [ok[:4], ok[4:]]

        stats = conn.timing_stats
        assert stats is not None
        conn.close()
        assert stats.close_count == 1
        assert stats.close_total_ns > 0

    def test_connect_recorded_even_on_failure(
        self,
        socket_queue: list[MagicMock],
        cursor_module: type,
    ) -> None:
        sock = MagicMock()
        sock.connect.side_effect = OSError("boom")
        socket_queue.append(sock)

        with pytest.raises(Exception):
            Connection("localhost", 33000, "testdb", "dba", "", enable_timing=True)


class TestConnectionTimingEnvVar:
    @pytest.mark.parametrize("env_value", ["1", "true", "True", "TRUE", "yes", "YES"])
    def test_explicit_false_overrides_env(
        self,
        env_value: str,
        socket_queue: list[MagicMock],
        cursor_module: type,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("PYCUBRID_ENABLE_TIMING", env_value)
        conn, _ = _make_connected(socket_queue, enable_timing=False)
        assert conn._timing is None

    @pytest.mark.parametrize("env_value", ["1", "true", "True", "TRUE", "yes", "YES"])
    def test_enabled_via_env_no_kwarg(
        self,
        env_value: str,
        socket_queue: list[MagicMock],
        cursor_module: type,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("PYCUBRID_ENABLE_TIMING", env_value)
        open_db = build_open_db_response()
        sock = make_socket([build_handshake_response(), open_db[:4], open_db[4:]])
        socket_queue.append(sock)

        conn = Connection("localhost", 33000, "testdb", "dba", "")
        assert conn._timing is not None
        assert isinstance(conn.timing_stats, TimingStats)

    @pytest.mark.parametrize("env_value", ["0", "false", "no", "", "random"])
    def test_disabled_via_env(
        self,
        env_value: str,
        socket_queue: list[MagicMock],
        cursor_module: type,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("PYCUBRID_ENABLE_TIMING", env_value)
        open_db = build_open_db_response()
        sock = make_socket([build_handshake_response(), open_db[:4], open_db[4:]])
        socket_queue.append(sock)

        conn = Connection("localhost", 33000, "testdb", "dba", "")
        assert conn._timing is None

    def test_env_not_set(
        self,
        socket_queue: list[MagicMock],
        cursor_module: type,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.delenv("PYCUBRID_ENABLE_TIMING", raising=False)
        open_db = build_open_db_response()
        sock = make_socket([build_handshake_response(), open_db[:4], open_db[4:]])
        socket_queue.append(sock)

        conn = Connection("localhost", 33000, "testdb", "dba", "")
        assert conn._timing is None

    def test_kwarg_overrides_env(
        self,
        socket_queue: list[MagicMock],
        cursor_module: type,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("PYCUBRID_ENABLE_TIMING", "1")
        conn, _ = _make_connected(socket_queue, enable_timing=True)
        assert conn._timing is not None


class TestCursorTiming:
    @pytest.fixture
    def mock_connection_with_timing(self) -> MagicMock:
        conn = MagicMock()
        conn.autocommit = False
        conn._connected = True
        conn._cas_info = b"\x01\x01\x02\x03"
        conn._cursors = set()
        conn._ensure_connected = MagicMock()
        conn._timing = TimingStats()

        def send_and_receive(packet: object) -> object:
            return packet

        conn._send_and_receive = MagicMock(side_effect=send_and_receive)
        return conn

    @pytest.fixture
    def mock_connection_no_timing(self) -> MagicMock:
        conn = MagicMock()
        conn.autocommit = False
        conn._connected = True
        conn._cas_info = b"\x01\x01\x02\x03"
        conn._cursors = set()
        conn._ensure_connected = MagicMock()
        conn._timing = None

        def send_and_receive(packet: object) -> object:
            return packet

        conn._send_and_receive = MagicMock(side_effect=send_and_receive)
        return conn

    def test_cursor_caches_timing(self, mock_connection_with_timing: MagicMock) -> None:
        cur = Cursor(mock_connection_with_timing)
        assert cur._timing is mock_connection_with_timing._timing

    def test_cursor_none_timing(self, mock_connection_no_timing: MagicMock) -> None:
        cur = Cursor(mock_connection_no_timing)
        assert cur._timing is None

    def test_execute_records_timing(self, mock_connection_with_timing: MagicMock) -> None:
        from pycubrid.constants import CUBRIDStatementType
        from pycubrid.protocol import ColumnMetaData, PrepareAndExecutePacket

        conn = mock_connection_with_timing

        def fake_send(packet: object) -> object:
            if isinstance(packet, PrepareAndExecutePacket):
                packet.query_handle = 1
                packet.statement_type = CUBRIDStatementType.SELECT
                packet.columns = [
                    ColumnMetaData(
                        name="id", column_type=8, precision=10, scale=0, is_nullable=False
                    )
                ]
                packet.total_tuple_count = 0
                packet.rows = []
                packet.result_infos = []
            return packet

        conn._send_and_receive = MagicMock(side_effect=fake_send)

        cur = Cursor(conn)
        cur.execute("SELECT 1")

        stats = conn._timing
        assert stats.execute_count == 1
        assert stats.execute_total_ns > 0

    def test_execute_no_timing_when_disabled(self, mock_connection_no_timing: MagicMock) -> None:
        from pycubrid.constants import CUBRIDStatementType
        from pycubrid.protocol import ColumnMetaData, PrepareAndExecutePacket

        conn = mock_connection_no_timing

        def fake_send(packet: object) -> object:
            if isinstance(packet, PrepareAndExecutePacket):
                packet.query_handle = 1
                packet.statement_type = CUBRIDStatementType.SELECT
                packet.columns = [
                    ColumnMetaData(
                        name="id", column_type=8, precision=10, scale=0, is_nullable=False
                    )
                ]
                packet.total_tuple_count = 0
                packet.rows = []
                packet.result_infos = []
            return packet

        conn._send_and_receive = MagicMock(side_effect=fake_send)

        cur = Cursor(conn)
        cur.execute("SELECT 1")

    def test_fetch_records_timing(self, mock_connection_with_timing: MagicMock) -> None:
        from pycubrid.protocol import ColumnMetaData, FetchPacket

        conn = mock_connection_with_timing

        def fake_send(packet: object) -> object:
            if isinstance(packet, FetchPacket):
                packet.rows = [(1,), (2,)]
            return packet

        conn._send_and_receive = MagicMock(side_effect=fake_send)

        cur = Cursor(conn)
        cur._query_handle = 1
        cur._row_index = 0
        cur._total_tuple_count = 10
        cur._columns = [
            ColumnMetaData(name="id", column_type=8, precision=10, scale=0, is_nullable=False)
        ]
        cur._statement_type = 1

        cur._fetch_more_rows()

        stats = conn._timing
        assert stats.fetch_count == 1
        assert stats.fetch_total_ns > 0

    def test_fetch_no_timing_when_disabled(self, mock_connection_no_timing: MagicMock) -> None:
        from pycubrid.protocol import ColumnMetaData, FetchPacket

        conn = mock_connection_no_timing

        def fake_send(packet: object) -> object:
            if isinstance(packet, FetchPacket):
                packet.rows = [(1,)]
            return packet

        conn._send_and_receive = MagicMock(side_effect=fake_send)

        cur = Cursor(conn)
        cur._query_handle = 1
        cur._row_index = 0
        cur._total_tuple_count = 10
        cur._columns = [
            ColumnMetaData(name="id", column_type=8, precision=10, scale=0, is_nullable=False)
        ]
        cur._statement_type = 1

        cur._fetch_more_rows()


class TestTimingExport:
    def test_importable_from_package(self) -> None:
        from pycubrid import TimingStats as TS

        assert TS is TimingStats

    def test_in_all(self) -> None:
        import pycubrid

        assert "TimingStats" in pycubrid.__all__
