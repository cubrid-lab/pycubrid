from __future__ import annotations

import asyncio
import datetime
import struct
from decimal import Decimal
from typing import Any, cast
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


def make_mock_stream_pair(
    read_chunks: list[bytes],
) -> tuple[MagicMock, MagicMock, MagicMock]:
    reader = MagicMock(spec=asyncio.StreamReader)
    reader.readexactly = AsyncMock(side_effect=list(read_chunks))
    writer = MagicMock(spec=asyncio.StreamWriter)
    writer.drain = AsyncMock()
    writer.close = MagicMock()
    writer.wait_closed = AsyncMock()
    writer.transport = MagicMock()
    mock_socket = MagicMock()
    writer.transport.get_extra_info.return_value = mock_socket
    return reader, writer, mock_socket


def make_streams_for_connect(session_id: int = 1234) -> tuple[MagicMock, MagicMock, MagicMock]:
    open_db = build_open_db_response(session_id=session_id)
    return make_mock_stream_pair([build_handshake_response(), open_db[:4], open_db[4:]])


@pytest.fixture
def async_conn() -> AsyncConnection:
    return AsyncConnection("localhost", 33000, "testdb", "dba", "")


class TestAsyncConnectionEstablishment:
    def test_fetch_size_is_stored(self) -> None:
        conn = AsyncConnection("localhost", 33000, "testdb", "dba", "", fetch_size=41)

        assert conn._fetch_size == 41

    def test_fetch_size_rejects_invalid(self) -> None:
        with pytest.raises(ValueError, match="fetch_size must be an integer >= 1"):
            AsyncConnection("localhost", 33000, "testdb", "dba", "", fetch_size=0)
        with pytest.raises(ValueError, match="fetch_size must be an integer >= 1"):
            AsyncConnection("localhost", 33000, "testdb", "dba", "", fetch_size=-1)
        with pytest.raises(ValueError, match="fetch_size must be an integer >= 1"):
            AsyncConnection("localhost", 33000, "testdb", "dba", "", fetch_size=True)  # type: ignore[arg-type]

    @pytest.mark.asyncio
    async def test_connect_success(self, async_conn: AsyncConnection) -> None:
        reader, writer, _ = make_streams_for_connect(session_id=777)

        with patch.object(async_conn, "_open_connection", return_value=(reader, writer)):
            await async_conn.connect()

        assert async_conn._connected is True
        assert async_conn._session_id == 777
        assert async_conn._cas_info == b"\x01\x01\x02\x03"

    @pytest.mark.asyncio
    async def test_connect_with_port_redirection(self, async_conn: AsyncConnection) -> None:
        first_reader, first_writer, _ = make_mock_stream_pair([build_handshake_response(33100)])
        open_db = build_open_db_response()
        second_reader, second_writer, _ = make_mock_stream_pair([open_db[:4], open_db[4:]])

        with patch.object(
            async_conn,
            "_open_connection",
            side_effect=[(first_reader, first_writer), (second_reader, second_writer)],
        ) as open_connection:
            await async_conn.connect()

        assert async_conn._connected is True
        assert open_connection.await_count == 2
        first_writer.close.assert_called_once_with()
        first_writer.wait_closed.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_connect_failure_raises_operational_error(
        self, async_conn: AsyncConnection
    ) -> None:
        with patch.object(
            async_conn,
            "_open_connection",
            new_callable=AsyncMock,
            side_effect=OperationalError("could not connect to localhost:33000"),
        ):
            with pytest.raises(OperationalError, match="could not connect"):
                await async_conn.connect()

    @pytest.mark.asyncio
    async def test_connect_noop_when_already_connected(self, async_conn: AsyncConnection) -> None:
        async_conn._connected = True
        await async_conn.connect()
        assert async_conn._connected is True


class TestAsyncConnectionClose:
    @pytest.mark.asyncio
    async def test_close_disconnects(self, async_conn: AsyncConnection) -> None:
        reader, writer, _ = make_streams_for_connect()

        with patch.object(async_conn, "_open_connection", return_value=(reader, writer)):
            await async_conn.connect()

        ok_resp = build_simple_ok_response()
        reader.readexactly = AsyncMock(side_effect=[ok_resp[:4], ok_resp[4:]])
        await async_conn.close()

        assert async_conn._connected is False
        assert async_conn._writer is None

    @pytest.mark.asyncio
    async def test_close_noop_when_not_connected(self, async_conn: AsyncConnection) -> None:
        await async_conn.close()
        assert async_conn._connected is False


class TestAsyncConnectionTransactions:
    @pytest.mark.asyncio
    async def test_commit(self, async_conn: AsyncConnection) -> None:
        reader, writer, _ = make_streams_for_connect()
        with patch.object(async_conn, "_open_connection", return_value=(reader, writer)):
            await async_conn.connect()

        ok_resp = build_simple_ok_response()
        reader.readexactly = AsyncMock(side_effect=[ok_resp[:4], ok_resp[4:]])
        await async_conn.commit()

    @pytest.mark.asyncio
    async def test_rollback(self, async_conn: AsyncConnection) -> None:
        reader, writer, _ = make_streams_for_connect()
        with patch.object(async_conn, "_open_connection", return_value=(reader, writer)):
            await async_conn.connect()

        ok_resp = build_simple_ok_response()
        reader.readexactly = AsyncMock(side_effect=[ok_resp[:4], ok_resp[4:]])
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
        reader, writer, _ = make_streams_for_connect()
        with patch.object(async_conn, "_open_connection", return_value=(reader, writer)):
            await async_conn.connect()

        ok1 = build_simple_ok_response()
        ok2 = build_simple_ok_response()
        reader.readexactly = AsyncMock(side_effect=[ok1[:4], ok1[4:], ok2[:4], ok2[4:]])
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


def _make_mock_conn(autocommit: bool = False) -> MagicMock:
    conn = MagicMock()
    conn._timing = None
    conn._cursors = set()
    conn._ensure_connected = MagicMock()
    conn._send_and_receive = AsyncMock()
    conn._protocol_version = 1
    conn.autocommit = autocommit
    return conn


class TestAsyncCursorLastrowid:
    def test_lastrowid_default_none(self) -> None:
        cur = AsyncCursor(_make_mock_conn())
        assert cur.lastrowid is None


class TestAsyncCursorCloseWithHandle:
    @pytest.mark.asyncio
    async def test_close_with_query_handle_sends_close_packet(self) -> None:
        conn = _make_mock_conn()
        cur = AsyncCursor(conn)
        cur._query_handle = 42
        await cur.close()
        assert cur._closed is True
        assert cur._query_handle is None
        conn._send_and_receive.assert_awaited()


class TestAsyncCursorExecute:
    @pytest.mark.asyncio
    async def test_execute_select_populates_state(self) -> None:
        from pycubrid.constants import CUBRIDStatementType

        conn = _make_mock_conn()
        cur = AsyncCursor(conn)

        async def fake_send(packet):
            packet.query_handle = 7
            packet.statement_type = CUBRIDStatementType.SELECT
            packet.columns = []
            packet.total_tuple_count = 0
            packet.rows = []
            packet.result_infos = []

        conn._send_and_receive = AsyncMock(side_effect=fake_send)
        result = await cur.execute("SELECT 1")
        assert result is cur
        assert cur._query_handle == 7
        assert cur._rowcount == -1

    @pytest.mark.asyncio
    async def test_execute_with_parameters(self) -> None:
        from pycubrid.constants import CUBRIDStatementType

        conn = _make_mock_conn()
        cur = AsyncCursor(conn)

        async def fake_send(packet):
            packet.query_handle = 1
            packet.statement_type = CUBRIDStatementType.SELECT
            packet.columns = []
            packet.total_tuple_count = 0
            packet.rows = []
            packet.result_infos = []

        conn._send_and_receive = AsyncMock(side_effect=fake_send)
        await cur.execute("SELECT * FROM t WHERE id = ?", [42])
        assert cur._query_handle == 1

    @pytest.mark.asyncio
    async def test_execute_dml_uses_result_infos(self) -> None:
        from pycubrid.constants import CUBRIDStatementType

        conn = _make_mock_conn()
        cur = AsyncCursor(conn)

        info = MagicMock()
        info.result_count = 3

        async def fake_send(packet):
            packet.query_handle = 2
            packet.statement_type = CUBRIDStatementType.UPDATE
            packet.columns = []
            packet.total_tuple_count = 0
            packet.rows = []
            packet.result_infos = [info]

        conn._send_and_receive = AsyncMock(side_effect=fake_send)
        await cur.execute("UPDATE t SET x = 1")
        assert cur._rowcount == 3

    @pytest.mark.asyncio
    async def test_execute_dml_no_result_infos(self) -> None:
        from pycubrid.constants import CUBRIDStatementType

        conn = _make_mock_conn()
        cur = AsyncCursor(conn)

        async def fake_send(packet):
            packet.query_handle = 3
            packet.statement_type = CUBRIDStatementType.DELETE
            packet.columns = []
            packet.total_tuple_count = 0
            packet.rows = []
            packet.result_infos = []

        conn._send_and_receive = AsyncMock(side_effect=fake_send)
        await cur.execute("DELETE FROM t")
        assert cur._rowcount == -1

    @pytest.mark.asyncio
    async def test_execute_insert_fetches_lastrowid(self) -> None:
        from pycubrid.constants import CUBRIDStatementType

        conn = _make_mock_conn()
        cur = AsyncCursor(conn)

        call_count = {"n": 0}

        async def fake_send(packet):
            call_count["n"] += 1
            if call_count["n"] == 1:
                packet.query_handle = 4
                packet.statement_type = CUBRIDStatementType.INSERT
                packet.columns = []
                packet.total_tuple_count = 0
                packet.rows = []
                packet.result_infos = []
            else:
                packet.last_insert_id = b"99"

        conn._send_and_receive = AsyncMock(side_effect=fake_send)
        await cur.execute("INSERT INTO t VALUES (1)")
        assert cur._lastrowid == 99

    @pytest.mark.asyncio
    async def test_execute_insert_lastrowid_failure_is_silent(self) -> None:
        from pycubrid.constants import CUBRIDStatementType

        conn = _make_mock_conn()
        cur = AsyncCursor(conn)

        call_count = {"n": 0}

        async def fake_send(packet):
            call_count["n"] += 1
            if call_count["n"] == 1:
                packet.query_handle = 5
                packet.statement_type = CUBRIDStatementType.INSERT
                packet.columns = []
                packet.total_tuple_count = 0
                packet.rows = []
                packet.result_infos = []
            else:
                raise OperationalError("boom")

        conn._send_and_receive = AsyncMock(side_effect=fake_send)
        await cur.execute("INSERT INTO t VALUES (1)")
        assert cur._lastrowid is None

    @pytest.mark.asyncio
    async def test_execute_closes_existing_query_handle(self) -> None:
        from pycubrid.constants import CUBRIDStatementType

        conn = _make_mock_conn()
        cur = AsyncCursor(conn)
        cur._query_handle = 99

        async def fake_send(packet):
            if hasattr(packet, "query_handle") and packet.query_handle == 99:
                return
            packet.query_handle = 1
            packet.statement_type = CUBRIDStatementType.SELECT
            packet.columns = []
            packet.total_tuple_count = 0
            packet.rows = []
            packet.result_infos = []

        conn._send_and_receive = AsyncMock(side_effect=fake_send)
        await cur.execute("SELECT 1")
        assert cur._query_handle == 1


class TestAsyncCursorExecutemany:
    @pytest.mark.asyncio
    async def test_executemany_empty_returns_self(self) -> None:
        conn = _make_mock_conn()
        cur = AsyncCursor(conn)
        result = await cur.executemany("INSERT INTO t VALUES (?)", [])
        assert result is cur

    @pytest.mark.asyncio
    async def test_executemany_select_loops(self) -> None:
        from pycubrid.constants import CUBRIDStatementType

        conn = _make_mock_conn()
        cur = AsyncCursor(conn)

        async def fake_send(packet):
            packet.query_handle = 1
            packet.statement_type = CUBRIDStatementType.SELECT
            packet.columns = []
            packet.total_tuple_count = 0
            packet.rows = []
            packet.result_infos = []

        conn._send_and_receive = AsyncMock(side_effect=fake_send)
        result = await cur.executemany("SELECT ? FROM t", [[1], [2]])
        assert result is cur

    @pytest.mark.asyncio
    async def test_executemany_batch_uses_batch_packet(self) -> None:
        conn = _make_mock_conn()
        cur = AsyncCursor(conn)

        async def fake_send(packet):
            packet.results = [(0, 1), (0, 1)]

        conn._send_and_receive = AsyncMock(side_effect=fake_send)
        result = await cur.executemany("INSERT INTO t VALUES (?)", [[1], [2]])
        assert result is cur
        assert cur._rowcount == 2


class TestAsyncCursorExecutemanyBatch:
    @pytest.mark.asyncio
    async def test_executemany_batch_with_results(self) -> None:
        conn = _make_mock_conn()
        cur = AsyncCursor(conn)

        async def fake_send(packet):
            packet.results = [(0, 5), (0, 3)]

        conn._send_and_receive = AsyncMock(side_effect=fake_send)
        results = await cur.executemany_batch(["INSERT 1", "INSERT 2"])
        assert results == [(0, 5), (0, 3)]
        assert cur._rowcount == 8

    @pytest.mark.asyncio
    async def test_executemany_batch_empty_results(self) -> None:
        conn = _make_mock_conn()
        cur = AsyncCursor(conn)

        async def fake_send(packet):
            packet.results = []

        conn._send_and_receive = AsyncMock(side_effect=fake_send)
        results = await cur.executemany_batch(["NOOP"])
        assert results == []
        assert cur._rowcount == 0

    @pytest.mark.asyncio
    async def test_executemany_batch_uses_explicit_autocommit(self) -> None:
        conn = _make_mock_conn(autocommit=False)
        cur = AsyncCursor(conn)

        captured = {}

        async def fake_send(packet):
            captured["auto_commit"] = packet.auto_commit
            packet.results = []

        conn._send_and_receive = AsyncMock(side_effect=fake_send)
        await cur.executemany_batch(["INSERT 1"], auto_commit=True)
        assert captured["auto_commit"] is True


class TestAsyncCursorFetchMore:
    @pytest.mark.asyncio
    async def test_fetchmany_triggers_fetch_more(self) -> None:
        conn = _make_mock_conn()
        cur = AsyncCursor(conn)
        cur._description = (("id", 1, None, None, 0, 0, False),)
        cur._rows = [(1,)]
        cur._row_index = 0
        cur._query_handle = 1
        cur._total_tuple_count = 3

        async def fake_send(packet):
            packet.rows = [(2,), (3,)]

        conn._send_and_receive = AsyncMock(side_effect=fake_send)
        rows = await cur.fetchmany(3)
        assert rows == [(1,), (2,), (3,)]

    @pytest.mark.asyncio
    async def test_fetch_more_rows_no_handle_returns_false(self) -> None:
        cur = AsyncCursor(_make_mock_conn())
        cur._query_handle = None
        assert await cur._fetch_more_rows() is False

    @pytest.mark.asyncio
    async def test_fetch_more_rows_index_at_end_returns_false(self) -> None:
        cur = AsyncCursor(_make_mock_conn())
        cur._query_handle = 1
        cur._row_index = 5
        cur._total_tuple_count = 5
        assert await cur._fetch_more_rows() is False

    @pytest.mark.asyncio
    async def test_fetch_more_rows_empty_packet_returns_false(self) -> None:
        conn = _make_mock_conn()
        cur = AsyncCursor(conn)
        cur._query_handle = 1
        cur._row_index = 0
        cur._total_tuple_count = 10

        async def fake_send(packet):
            packet.rows = []

        conn._send_and_receive = AsyncMock(side_effect=fake_send)
        assert await cur._fetch_more_rows() is False


class TestAsyncCursorAnextReturnsRow:
    @pytest.mark.asyncio
    async def test_anext_returns_row(self) -> None:
        conn = _make_mock_conn()
        cur = AsyncCursor(conn)
        cur._description = (("id", 1, None, None, 0, 0, False),)
        cur._rows = [(7,)]
        cur._row_index = 0
        cur._query_handle = None
        cur._total_tuple_count = 1
        row = await cur.__anext__()
        assert row == (7,)


class TestAsyncCursorMisc:
    def test_setinputsizes_noop(self) -> None:
        cur = AsyncCursor(_make_mock_conn())
        assert cur.setinputsizes([1, 2, 3]) is None

    def test_setoutputsize_noop(self) -> None:
        cur = AsyncCursor(_make_mock_conn())
        assert cur.setoutputsize(100, 0) is None

    @pytest.mark.asyncio
    async def test_callproc_with_params(self) -> None:
        from pycubrid.constants import CUBRIDStatementType

        conn = _make_mock_conn()
        cur = AsyncCursor(conn)

        captured = {}

        async def fake_send(packet):
            captured["sql"] = getattr(packet, "sql", None)
            packet.query_handle = 1
            packet.statement_type = CUBRIDStatementType.SELECT
            packet.columns = []
            packet.total_tuple_count = 0
            packet.rows = []
            packet.result_infos = []

        conn._send_and_receive = AsyncMock(side_effect=fake_send)
        result = await cur.callproc("myproc", [1, "x"])
        assert result == [1, "x"]
        assert "CALL myproc(" in captured["sql"]

    @pytest.mark.asyncio
    async def test_callproc_no_params(self) -> None:
        from pycubrid.constants import CUBRIDStatementType

        conn = _make_mock_conn()
        cur = AsyncCursor(conn)

        captured = {}

        async def fake_send(packet):
            captured["sql"] = getattr(packet, "sql", None)
            packet.query_handle = 1
            packet.statement_type = CUBRIDStatementType.SELECT
            packet.columns = []
            packet.total_tuple_count = 0
            packet.rows = []
            packet.result_infos = []

        conn._send_and_receive = AsyncMock(side_effect=fake_send)
        await cur.callproc("myproc")
        assert captured["sql"] == "CALL myproc()"


class TestAsyncCursorBindParametersExtra:
    def test_bind_with_mapping_raises(self) -> None:
        cur = AsyncCursor(_make_mock_conn())
        with pytest.raises(Exception, match="parameters must be a sequence"):
            cur._bind_parameters("SELECT ? FROM t WHERE x = ?", cast(Any, {"a": 1, "b": 2}))

    def test_bind_rejects_string(self) -> None:
        cur = AsyncCursor(_make_mock_conn())
        with pytest.raises(Exception, match="parameters must be a sequence"):
            cur._bind_parameters("SELECT ?", "abc")


class TestAsyncCursorFormatParameter:
    def _cur(self) -> AsyncCursor:
        return AsyncCursor(_make_mock_conn())

    def test_none(self) -> None:
        assert self._cur()._format_parameter(None) == "NULL"

    def test_bool_true(self) -> None:
        assert self._cur()._format_parameter(True) == "1"

    def test_bool_false(self) -> None:
        assert self._cur()._format_parameter(False) == "0"

    def test_string_escapes_quote(self) -> None:
        assert self._cur()._format_parameter("a'b") == "'a''b'"

    def test_bytes(self) -> None:
        assert self._cur()._format_parameter(b"\xab\xcd") == "X'abcd'"

    def test_datetime(self) -> None:
        dt = datetime.datetime(2026, 4, 18, 12, 34, 56, 789000)
        assert self._cur()._format_parameter(dt) == "DATETIME'2026-04-18 12:34:56.789'"

    def test_date(self) -> None:
        assert self._cur()._format_parameter(datetime.date(2026, 4, 18)) == "DATE'2026-04-18'"

    def test_time(self) -> None:
        assert self._cur()._format_parameter(datetime.time(12, 34, 56)) == "TIME'12:34:56'"

    def test_decimal(self) -> None:
        assert self._cur()._format_parameter(Decimal("3.14")) == "3.14"

    def test_int(self) -> None:
        assert self._cur()._format_parameter(42) == "42"

    def test_float(self) -> None:
        assert self._cur()._format_parameter(2.5) == "2.5"

    def test_unsupported_raises(self) -> None:
        with pytest.raises(Exception, match="unsupported parameter type"):
            self._cur()._format_parameter(object())

    def test_float_nan_raises(self) -> None:
        with pytest.raises(Exception, match="nan and inf"):
            self._cur()._format_parameter(float("nan"))

    def test_float_inf_raises(self) -> None:
        with pytest.raises(Exception, match="nan and inf"):
            self._cur()._format_parameter(float("inf"))

    def test_bytearray(self) -> None:
        assert self._cur()._format_parameter(bytearray(b"\xca\xfe")) == "X'cafe'"

    def test_datetime_tz_iana(self) -> None:
        from zoneinfo import ZoneInfo

        dt = datetime.datetime(2026, 1, 15, 10, 30, 0, 123000, tzinfo=ZoneInfo("Asia/Seoul"))
        assert self._cur()._format_parameter(dt) == "DATETIMETZ'2026-01-15 10:30:00.123 Asia/Seoul'"

    def test_datetime_tz_utc(self) -> None:
        dt = datetime.datetime(2026, 1, 15, 10, 30, 0, tzinfo=datetime.timezone.utc)
        assert self._cur()._format_parameter(dt) == "DATETIMETZ'2026-01-15 10:30:00.000 +00:00'"


class TestAsyncCursorBuildDescription:
    def test_empty_columns_returns_none(self) -> None:
        cur = AsyncCursor(_make_mock_conn())
        assert cur._build_description([]) is None


class TestAsyncConnectModule:
    @pytest.mark.asyncio
    async def test_connect_module_function(self) -> None:
        import pycubrid.aio as aio_mod

        with patch.object(aio_mod, "AsyncConnection") as mock_cls:
            instance = MagicMock()
            instance.connect = AsyncMock()
            instance.set_autocommit = AsyncMock()
            mock_cls.return_value = instance
            result = await aio_mod.connect(host="h", port=1, database="d", user="u", password="p")
            assert result is instance
            instance.connect.assert_awaited()

    @pytest.mark.asyncio
    async def test_connect_module_function_with_autocommit(self) -> None:
        import pycubrid.aio as aio_mod

        with patch.object(aio_mod, "AsyncConnection") as mock_cls:
            instance = MagicMock()
            instance.connect = AsyncMock()
            instance.set_autocommit = AsyncMock()
            mock_cls.return_value = instance
            await aio_mod.connect(autocommit=True)
            instance.set_autocommit.assert_awaited_with(True)


class TestAsyncConnectSocketLeak:
    """Regression tests for #122: async socket leak on non-OSError during connect."""

    @pytest.mark.asyncio
    async def test_handshake_parse_valueerror_closes_socket(
        self, async_conn: AsyncConnection
    ) -> None:
        reader, writer, _ = make_mock_stream_pair([build_handshake_response()])

        with (
            patch.object(async_conn, "_open_connection", return_value=(reader, writer)),
            patch(
                "pycubrid.protocol.ClientInfoExchangePacket.parse",
                side_effect=ValueError("bad handshake"),
            ),
        ):
            with pytest.raises(OperationalError, match="failed to connect"):
                await async_conn.connect()

        writer.close.assert_called_once_with()
        assert async_conn._connected is False
        assert async_conn._writer is None

    @pytest.mark.asyncio
    async def test_open_db_parse_struct_error_closes_socket(
        self, async_conn: AsyncConnection
    ) -> None:
        reader, writer, _ = make_streams_for_connect()

        with (
            patch.object(async_conn, "_open_connection", return_value=(reader, writer)),
            patch(
                "pycubrid.protocol.OpenDatabasePacket.parse",
                side_effect=struct.error("bad packet"),
            ),
        ):
            with pytest.raises(OperationalError, match="failed to connect"):
                await async_conn.connect()

        writer.close.assert_called_once_with()
        assert async_conn._connected is False
        assert async_conn._writer is None

    @pytest.mark.asyncio
    async def test_open_db_parse_index_error_closes_socket(
        self, async_conn: AsyncConnection
    ) -> None:
        reader, writer, _ = make_streams_for_connect()

        with (
            patch.object(async_conn, "_open_connection", return_value=(reader, writer)),
            patch(
                "pycubrid.protocol.OpenDatabasePacket.parse",
                side_effect=IndexError("broker info truncated"),
            ),
        ):
            with pytest.raises(OperationalError, match="failed to connect"):
                await async_conn.connect()

        writer.close.assert_called_once_with()
        assert async_conn._connected is False
        assert async_conn._writer is None

    @pytest.mark.asyncio
    async def test_handshake_parse_unicode_error_closes_socket(
        self, async_conn: AsyncConnection
    ) -> None:
        reader, writer, _ = make_mock_stream_pair([build_handshake_response()])

        with (
            patch.object(async_conn, "_open_connection", return_value=(reader, writer)),
            patch(
                "pycubrid.protocol.ClientInfoExchangePacket.parse",
                side_effect=UnicodeDecodeError("utf-8", b"\xff", 0, 1, "invalid"),
            ),
        ):
            with pytest.raises(OperationalError, match="failed to connect"):
                await async_conn.connect()

        writer.close.assert_called_once_with()
        assert async_conn._connected is False
        assert async_conn._writer is None
