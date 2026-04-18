from __future__ import annotations

import asyncio
import struct
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from pycubrid.aio.connection import AsyncConnection
from pycubrid.aio.cursor import AsyncCursor
from pycubrid.exceptions import InterfaceError, OperationalError


def build_handshake_response(port: int = 0) -> bytes:
    return struct.pack(">i", port)


def build_open_db_response(
    cas_info: bytes | bytearray = b"\x01\x01\x02\x03", session_id: int = 1234
) -> bytes:
    body = cas_info + struct.pack(">i", 0)
    body += b"\x00" * 8
    body += struct.pack(">i", session_id)
    data_length = struct.pack(">i", len(body) - 4)
    return data_length + body


def build_simple_ok_response(cas_info: bytes | bytearray = b"\x01\x01\x02\x03") -> bytes:
    body = cas_info + struct.pack(">i", 0)
    return struct.pack(">i", len(body) - 4) + body


class FakeLoop:
    """Fake event loop that feeds pre-built byte sequences for socket operations."""

    def __init__(self, recv_chunks: list[bytes]) -> None:
        self._recv_chunks = list(recv_chunks)
        self._recv_index = 0
        self.sent_data: list[bytes] = []
        self.connected_addresses: list[tuple[str, int]] = []

    async def sock_connect(self, sock: MagicMock, address: tuple[str, int]) -> None:
        self.connected_addresses.append(address)

    async def sock_sendall(self, sock: MagicMock, data: bytes) -> None:
        self.sent_data.append(data)

    async def sock_recv_into(self, sock: MagicMock, buffer: memoryview) -> int:
        if self._recv_index >= len(self._recv_chunks):
            return 0
        chunk = self._recv_chunks[self._recv_index]
        self._recv_index += 1
        n = len(chunk)
        buffer[:n] = chunk
        return n


def make_fake_loop_for_connect(session_id: int = 1234) -> FakeLoop:
    """Build a FakeLoop that provides handshake + open_db responses."""
    open_db = build_open_db_response(session_id=session_id)
    return FakeLoop(
        [
            build_handshake_response(),
            open_db[:4],
            open_db[4:],
        ]
    )


@pytest.fixture
def async_conn() -> AsyncConnection:
    return AsyncConnection("localhost", 33000, "testdb", "dba", "")


class TestAsyncConnectionEstablishment:
    @pytest.mark.asyncio
    async def test_connect_success(self, async_conn: AsyncConnection) -> None:
        fake_loop = make_fake_loop_for_connect(session_id=777)

        with (
            patch("pycubrid.aio.connection.asyncio.get_running_loop", return_value=fake_loop),
            patch.object(async_conn, "_create_socket_nonblocking", return_value=MagicMock()),
        ):
            await async_conn.connect()

        assert async_conn._connected is True
        assert async_conn._session_id == 777
        assert async_conn._cas_info == b"\x01\x01\x02\x03"

    @pytest.mark.asyncio
    async def test_connect_with_port_redirection(self, async_conn: AsyncConnection) -> None:
        open_db = build_open_db_response()
        fake_loop = FakeLoop(
            [
                build_handshake_response(33100),
                open_db[:4],
                open_db[4:],
            ]
        )

        sockets_created: list[MagicMock] = []

        def track_socket(host: str, port: int) -> MagicMock:
            s = MagicMock()
            sockets_created.append(s)
            return s

        with (
            patch("pycubrid.aio.connection.asyncio.get_running_loop", return_value=fake_loop),
            patch.object(async_conn, "_create_socket_nonblocking", side_effect=track_socket),
        ):
            await async_conn.connect()

        assert async_conn._connected is True
        assert len(sockets_created) == 2
        assert sockets_created[0].close.called
        assert ("localhost", 33100) in fake_loop.connected_addresses

    @pytest.mark.asyncio
    async def test_connect_failure_raises_operational_error(
        self, async_conn: AsyncConnection
    ) -> None:
        fake_loop = MagicMock()
        fake_loop.sock_connect = AsyncMock(side_effect=OSError("boom"))

        with (
            patch("pycubrid.aio.connection.asyncio.get_running_loop", return_value=fake_loop),
            patch.object(async_conn, "_create_socket_nonblocking", return_value=MagicMock()),
        ):
            with pytest.raises(OperationalError, match="failed to connect"):
                await async_conn.connect()

    @pytest.mark.asyncio
    async def test_connect_noop_when_already_connected(self, async_conn: AsyncConnection) -> None:
        async_conn._connected = True
        await async_conn.connect()
        assert async_conn._connected is True


class TestAsyncConnectionClose:
    @pytest.mark.asyncio
    async def test_close_disconnects(self, async_conn: AsyncConnection) -> None:
        fake_loop = make_fake_loop_for_connect()

        with (
            patch("pycubrid.aio.connection.asyncio.get_running_loop", return_value=fake_loop),
            patch.object(async_conn, "_create_socket_nonblocking", return_value=MagicMock()),
        ):
            await async_conn.connect()

        ok_resp = build_simple_ok_response()
        close_loop = FakeLoop([ok_resp[:4], ok_resp[4:]])
        with patch("pycubrid.aio.connection.asyncio.get_running_loop", return_value=close_loop):
            await async_conn.close()

        assert async_conn._connected is False
        assert async_conn._socket is None

    @pytest.mark.asyncio
    async def test_close_noop_when_not_connected(self, async_conn: AsyncConnection) -> None:
        await async_conn.close()
        assert async_conn._connected is False


class TestAsyncConnectionTransactions:
    @pytest.mark.asyncio
    async def test_commit(self, async_conn: AsyncConnection) -> None:
        fake_loop = make_fake_loop_for_connect()
        with (
            patch("pycubrid.aio.connection.asyncio.get_running_loop", return_value=fake_loop),
            patch.object(async_conn, "_create_socket_nonblocking", return_value=MagicMock()),
        ):
            await async_conn.connect()

        ok_resp = build_simple_ok_response()
        commit_loop = FakeLoop([ok_resp[:4], ok_resp[4:]])
        with patch("pycubrid.aio.connection.asyncio.get_running_loop", return_value=commit_loop):
            await async_conn.commit()

    @pytest.mark.asyncio
    async def test_rollback(self, async_conn: AsyncConnection) -> None:
        fake_loop = make_fake_loop_for_connect()
        with (
            patch("pycubrid.aio.connection.asyncio.get_running_loop", return_value=fake_loop),
            patch.object(async_conn, "_create_socket_nonblocking", return_value=MagicMock()),
        ):
            await async_conn.connect()

        ok_resp = build_simple_ok_response()
        rb_loop = FakeLoop([ok_resp[:4], ok_resp[4:]])
        with patch("pycubrid.aio.connection.asyncio.get_running_loop", return_value=rb_loop):
            await async_conn.rollback()

    @pytest.mark.asyncio
    async def test_commit_on_closed_raises(self, async_conn: AsyncConnection) -> None:
        with pytest.raises(InterfaceError, match="connection is closed"):
            await async_conn.commit()


class TestAsyncConnectionContextManager:
    @pytest.mark.asyncio
    async def test_aenter_returns_self(self, async_conn: AsyncConnection) -> None:
        async_conn._connected = True
        result = await async_conn.__aenter__()
        assert result is async_conn

    @pytest.mark.asyncio
    async def test_aexit_commits_on_success(self, async_conn: AsyncConnection) -> None:
        fake_loop = make_fake_loop_for_connect()
        with (
            patch("pycubrid.aio.connection.asyncio.get_running_loop", return_value=fake_loop),
            patch.object(async_conn, "_create_socket_nonblocking", return_value=MagicMock()),
        ):
            await async_conn.connect()

        ok1 = build_simple_ok_response()
        ok2 = build_simple_ok_response()
        exit_loop = FakeLoop([ok1[:4], ok1[4:], ok2[:4], ok2[4:]])
        with patch("pycubrid.aio.connection.asyncio.get_running_loop", return_value=exit_loop):
            await async_conn.__aexit__(None, None, None)

        assert async_conn._connected is False


class TestAsyncConnectionCursor:
    @pytest.mark.asyncio
    async def test_cursor_returns_async_cursor(self, async_conn: AsyncConnection) -> None:
        async_conn._connected = True
        cur = async_conn.cursor()
        assert isinstance(cur, AsyncCursor)
        assert cur in async_conn._cursors

    @pytest.mark.asyncio
    async def test_cursor_on_closed_raises(self, async_conn: AsyncConnection) -> None:
        with pytest.raises(InterfaceError, match="connection is closed"):
            async_conn.cursor()


class TestAsyncCursorProperties:
    def test_description_default(self) -> None:
        conn = MagicMock()
        conn._timing = None
        conn._cursors = set()
        cur = AsyncCursor(conn)
        assert cur.description is None

    def test_rowcount_default(self) -> None:
        conn = MagicMock()
        conn._timing = None
        conn._cursors = set()
        cur = AsyncCursor(conn)
        assert cur.rowcount == -1

    def test_arraysize_default_and_setter(self) -> None:
        conn = MagicMock()
        conn._timing = None
        conn._cursors = set()
        cur = AsyncCursor(conn)
        assert cur.arraysize == 1
        cur.arraysize = 50
        assert cur.arraysize == 50

    def test_arraysize_rejects_zero(self) -> None:
        conn = MagicMock()
        conn._timing = None
        conn._cursors = set()
        cur = AsyncCursor(conn)
        with pytest.raises(Exception, match="greater than zero"):
            cur.arraysize = 0


class TestAsyncCursorClose:
    @pytest.mark.asyncio
    async def test_close_sets_closed(self) -> None:
        conn = MagicMock()
        conn._timing = None
        conn._cursors = set()
        conn._ensure_connected = MagicMock()
        conn._send_and_receive = AsyncMock()
        cur = AsyncCursor(conn)
        await cur.close()
        assert cur._closed is True

    @pytest.mark.asyncio
    async def test_close_noop_when_already_closed(self) -> None:
        conn = MagicMock()
        conn._timing = None
        conn._cursors = set()
        cur = AsyncCursor(conn)
        cur._closed = True
        await cur.close()


class TestAsyncCursorContextManager:
    @pytest.mark.asyncio
    async def test_aenter_returns_self(self) -> None:
        conn = MagicMock()
        conn._timing = None
        conn._cursors = set()
        cur = AsyncCursor(conn)
        result = await cur.__aenter__()
        assert result is cur

    @pytest.mark.asyncio
    async def test_aexit_closes(self) -> None:
        conn = MagicMock()
        conn._timing = None
        conn._cursors = set()
        conn._ensure_connected = MagicMock()
        conn._send_and_receive = AsyncMock()
        cur = AsyncCursor(conn)
        await cur.__aexit__(None, None, None)
        assert cur._closed is True


class TestAsyncCursorIteration:
    @pytest.mark.asyncio
    async def test_aiter_returns_self(self) -> None:
        conn = MagicMock()
        conn._timing = None
        conn._cursors = set()
        cur = AsyncCursor(conn)
        assert cur.__aiter__() is cur

    @pytest.mark.asyncio
    async def test_anext_raises_stop_when_no_rows(self) -> None:
        conn = MagicMock()
        conn._timing = None
        conn._cursors = set()
        cur = AsyncCursor(conn)
        cur._description = (("id", 1, None, None, 0, 0, False),)
        cur._rows = []
        cur._row_index = 0
        cur._query_handle = None
        with pytest.raises(StopAsyncIteration):
            await cur.__anext__()


class TestAsyncCursorFetch:
    @pytest.mark.asyncio
    async def test_fetchone_returns_row(self) -> None:
        conn = MagicMock()
        conn._timing = None
        conn._cursors = set()
        cur = AsyncCursor(conn)
        cur._description = (("id", 1, None, None, 0, 0, False),)
        cur._rows = [(1,), (2,), (3,)]
        cur._row_index = 0
        cur._query_handle = None
        cur._total_tuple_count = 3

        row = await cur.fetchone()
        assert row == (1,)
        row = await cur.fetchone()
        assert row == (2,)

    @pytest.mark.asyncio
    async def test_fetchall_returns_all_rows(self) -> None:
        conn = MagicMock()
        conn._timing = None
        conn._cursors = set()
        cur = AsyncCursor(conn)
        cur._description = (("id", 1, None, None, 0, 0, False),)
        cur._rows = [(1,), (2,), (3,)]
        cur._row_index = 0
        cur._query_handle = None
        cur._total_tuple_count = 3

        rows = await cur.fetchall()
        assert rows == [(1,), (2,), (3,)]

    @pytest.mark.asyncio
    async def test_fetchmany_returns_requested_count(self) -> None:
        conn = MagicMock()
        conn._timing = None
        conn._cursors = set()
        cur = AsyncCursor(conn)
        cur._description = (("id", 1, None, None, 0, 0, False),)
        cur._rows = [(1,), (2,), (3,), (4,), (5,)]
        cur._row_index = 0
        cur._query_handle = None
        cur._total_tuple_count = 5

        rows = await cur.fetchmany(2)
        assert rows == [(1,), (2,)]
        rows = await cur.fetchmany(2)
        assert rows == [(3,), (4,)]

    @pytest.mark.asyncio
    async def test_fetchone_on_closed_raises(self) -> None:
        conn = MagicMock()
        conn._timing = None
        conn._cursors = set()
        cur = AsyncCursor(conn)
        cur._closed = True
        with pytest.raises(InterfaceError, match="Cursor is closed"):
            await cur.fetchone()

    @pytest.mark.asyncio
    async def test_fetchone_without_result_set_raises(self) -> None:
        conn = MagicMock()
        conn._timing = None
        conn._cursors = set()
        cur = AsyncCursor(conn)
        with pytest.raises(InterfaceError, match="No result set"):
            await cur.fetchone()


class TestAsyncCursorBindParameters:
    def test_bind_parameters(self) -> None:
        conn = MagicMock()
        conn._timing = None
        conn._cursors = set()
        cur = AsyncCursor(conn)
        result = cur._bind_parameters("SELECT * FROM t WHERE id = ?", [42])
        assert result == "SELECT * FROM t WHERE id = 42"

    def test_bind_wrong_count_raises(self) -> None:
        conn = MagicMock()
        conn._timing = None
        conn._cursors = set()
        cur = AsyncCursor(conn)
        with pytest.raises(Exception, match="wrong number"):
            cur._bind_parameters("SELECT ?", [1, 2])
