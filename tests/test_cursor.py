from __future__ import annotations

import datetime
from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from pycubrid.constants import CUBRIDStatementType
from pycubrid.cursor import Cursor
from pycubrid.exceptions import InterfaceError, ProgrammingError
from pycubrid.protocol import (
    BatchExecutePacket,
    CloseQueryPacket,
    ColumnMetaData,
    FetchPacket,
    GetLastInsertIdPacket,
    PrepareAndExecutePacket,
    ResultInfo,
)


@pytest.fixture
def mock_connection() -> MagicMock:
    conn = MagicMock()
    conn.autocommit = False
    conn._connected = True
    conn._cas_info = b"\x00\x01\x02\x03"
    conn._cursors = set()
    conn._ensure_connected = MagicMock()

    def send_and_receive(packet: object) -> object:
        return packet

    conn._send_and_receive = MagicMock(side_effect=send_and_receive)
    return conn


@pytest.fixture
def cursor(mock_connection: MagicMock) -> Cursor:
    return Cursor(mock_connection)


def _set_prepare_packet(
    packet: PrepareAndExecutePacket,
    *,
    stmt_type: int,
    rows: list[list[object]] | None = None,
    total_count: int = 0,
    result_count: int = 0,
    with_columns: bool = True,
) -> None:
    packet.query_handle = 1
    packet.statement_type = stmt_type
    packet.columns = (
        [ColumnMetaData(name="id", column_type=8, precision=10, scale=0, is_nullable=False)]
        if with_columns
        else []
    )
    packet.total_tuple_count = total_count
    packet.rows = rows or []
    packet.result_infos = [ResultInfo(stmt_type=stmt_type, result_count=result_count)]


def test_constructor_initial_state(mock_connection: MagicMock) -> None:
    cur = Cursor(mock_connection)
    assert cur._connection is mock_connection
    assert cur._closed is False
    assert cur.description is None
    assert cur.rowcount == -1
    assert cur.arraysize == 1
    assert cur.lastrowid is None
    assert cur._query_handle is None
    assert cur in mock_connection._cursors


def test_arraysize_setter_and_validation(cursor: Cursor) -> None:
    cursor.arraysize = 3
    assert cursor.arraysize == 3
    with pytest.raises(ProgrammingError, match="arraysize"):
        cursor.arraysize = 0


def test_execute_select_sets_description_and_rowcount(
    cursor: Cursor, mock_connection: MagicMock
) -> None:
    def send(packet: object) -> object:
        if isinstance(packet, PrepareAndExecutePacket):
            _set_prepare_packet(
                packet,
                stmt_type=CUBRIDStatementType.SELECT,
                rows=[[1], [2]],
                total_count=2,
                result_count=2,
            )
        return packet

    mock_connection._send_and_receive.side_effect = send
    result = cursor.execute("SELECT id FROM t")
    assert result is cursor
    assert cursor.rowcount == -1
    assert cursor.description == (("id", 8, None, None, 10, 0, False),)
    assert cursor._query_handle == 1
    assert cursor._rows == [[1], [2]]
    assert cursor._total_tuple_count == 2
    mock_connection._ensure_connected.assert_called()


def test_execute_closes_existing_query_handle(cursor: Cursor, mock_connection: MagicMock) -> None:
    cursor._query_handle = 99

    def send(packet: object) -> object:
        if isinstance(packet, PrepareAndExecutePacket):
            _set_prepare_packet(
                packet, stmt_type=CUBRIDStatementType.SELECT, rows=[[10]], total_count=1
            )
        return packet

    mock_connection._send_and_receive.side_effect = send
    cursor.execute("SELECT 10")
    first_packet = mock_connection._send_and_receive.call_args_list[0].args[0]
    assert isinstance(first_packet, CloseQueryPacket)
    assert first_packet.query_handle == 99


def test_execute_insert_sets_rowcount_and_lastrowid(
    cursor: Cursor, mock_connection: MagicMock
) -> None:
    def send(packet: object) -> object:
        if isinstance(packet, PrepareAndExecutePacket):
            _set_prepare_packet(
                packet,
                stmt_type=CUBRIDStatementType.INSERT,
                result_count=3,
                with_columns=False,
            )
        if isinstance(packet, GetLastInsertIdPacket):
            packet.last_insert_id = "55"
        return packet

    mock_connection._send_and_receive.side_effect = send
    cursor.execute("INSERT INTO t VALUES (1)")
    assert cursor.rowcount == 3
    assert cursor.lastrowid == "55"
    assert cursor.description is None


def test_execute_insert_lastrowid_failure_is_ignored(
    cursor: Cursor, mock_connection: MagicMock
) -> None:
    def send(packet: object) -> object:
        if isinstance(packet, PrepareAndExecutePacket):
            _set_prepare_packet(packet, stmt_type=CUBRIDStatementType.INSERT, result_count=1)
        if isinstance(packet, GetLastInsertIdPacket):
            raise RuntimeError("no id")
        return packet

    mock_connection._send_and_receive.side_effect = send
    cursor.execute("INSERT INTO t VALUES (1)")
    assert cursor.lastrowid is None


def test_execute_non_select_without_result_info_sets_negative_rowcount(
    cursor: Cursor, mock_connection: MagicMock
) -> None:
    def send(packet: object) -> object:
        if isinstance(packet, PrepareAndExecutePacket):
            packet.query_handle = 1
            packet.statement_type = CUBRIDStatementType.UPDATE
            packet.columns = []
            packet.total_tuple_count = 0
            packet.rows = []
            packet.result_infos = []
        return packet

    mock_connection._send_and_receive.side_effect = send
    cursor.execute("UPDATE t SET v = 1")
    assert cursor.rowcount == -1


def test_execute_binds_sequence_parameters_all_supported_types(
    cursor: Cursor, mock_connection: MagicMock
) -> None:
    captured_sql: list[str] = []

    def send(packet: object) -> object:
        if isinstance(packet, PrepareAndExecutePacket):
            captured_sql.append(packet.sql)
            _set_prepare_packet(
                packet, stmt_type=CUBRIDStatementType.SELECT, rows=[[1]], total_count=1
            )
        return packet

    mock_connection._send_and_receive.side_effect = send
    cursor.execute(
        "SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?",
        [
            "O'Hara",
            7,
            2.5,
            None,
            True,
            b"\x0f\xa0",
            datetime.date(2026, 1, 2),
            datetime.time(3, 4, 5, 999000),
            datetime.datetime(2026, 1, 2, 3, 4, 5, 123456),
            Decimal("10.50"),
        ],
    )
    assert captured_sql == [
        "SELECT 'O''Hara', 7, 2.5, NULL, 1, X'0fa0', DATE'2026-01-02', "
        "TIME'03:04:05', DATETIME'2026-01-02 03:04:05.123', 10.50"
    ]


def test_execute_binds_mapping_parameters(cursor: Cursor, mock_connection: MagicMock) -> None:
    captured_sql: list[str] = []

    def send(packet: object) -> object:
        if isinstance(packet, PrepareAndExecutePacket):
            captured_sql.append(packet.sql)
            _set_prepare_packet(
                packet, stmt_type=CUBRIDStatementType.SELECT, rows=[[1]], total_count=1
            )
        return packet

    mock_connection._send_and_receive.side_effect = send
    cursor.execute("SELECT ?, ?", {"a": 1, "b": "x"})
    assert captured_sql == ["SELECT 1, 'x'"]


def test_execute_parameter_count_mismatch_raises(cursor: Cursor) -> None:
    with pytest.raises(ProgrammingError, match="wrong number"):
        cursor.execute("SELECT ?, ?", [1])


def test_execute_unsupported_parameter_container_raises(cursor: Cursor) -> None:
    with pytest.raises(ProgrammingError, match="sequence or mapping"):
        cursor.execute("SELECT ?", "x")


def test_execute_unsupported_parameter_type_raises(cursor: Cursor) -> None:
    with pytest.raises(ProgrammingError, match="unsupported"):
        cursor.execute("SELECT ?", [object()])


def test_fetchone_basic_and_end(cursor: Cursor, mock_connection: MagicMock) -> None:
    def send(packet: object) -> object:
        if isinstance(packet, PrepareAndExecutePacket):
            _set_prepare_packet(
                packet,
                stmt_type=CUBRIDStatementType.SELECT,
                rows=[[1], [2]],
                total_count=2,
                result_count=2,
            )
        return packet

    mock_connection._send_and_receive.side_effect = send
    cursor.execute("SELECT id FROM t")
    assert cursor.fetchone() == (1,)
    assert cursor.fetchone() == (2,)
    assert cursor.fetchone() is None


def test_fetchone_without_result_set_raises(cursor: Cursor) -> None:
    with pytest.raises(InterfaceError, match="No result set"):
        cursor.fetchone()


def test_fetchone_fetches_more_rows(cursor: Cursor, mock_connection: MagicMock) -> None:
    fetch_calls = 0

    def send(packet: object) -> object:
        nonlocal fetch_calls
        if isinstance(packet, PrepareAndExecutePacket):
            _set_prepare_packet(
                packet,
                stmt_type=CUBRIDStatementType.SELECT,
                rows=[[1]],
                total_count=3,
                result_count=3,
            )
        elif isinstance(packet, FetchPacket):
            fetch_calls += 1
            if fetch_calls == 1:
                packet.rows = [[2], [3]]
            else:
                packet.rows = []
        return packet

    mock_connection._send_and_receive.side_effect = send
    cursor.execute("SELECT id FROM t")
    assert cursor.fetchone() == (1,)
    assert cursor.fetchone() == (2,)
    assert cursor.fetchone() == (3,)
    assert cursor.fetchone() is None


def test_fetchone_returns_none_when_query_handle_missing(
    cursor: Cursor, mock_connection: MagicMock
) -> None:
    def send(packet: object) -> object:
        if isinstance(packet, PrepareAndExecutePacket):
            _set_prepare_packet(
                packet, stmt_type=CUBRIDStatementType.SELECT, rows=[], total_count=1
            )
            packet.query_handle = 0
        return packet

    mock_connection._send_and_receive.side_effect = send
    cursor.execute("SELECT id FROM t")
    cursor._query_handle = None
    assert cursor.fetchone() is None


def test_fetchone_returns_none_when_fetch_packet_has_no_rows(
    cursor: Cursor, mock_connection: MagicMock
) -> None:
    def send(packet: object) -> object:
        if isinstance(packet, PrepareAndExecutePacket):
            _set_prepare_packet(
                packet, stmt_type=CUBRIDStatementType.SELECT, rows=[], total_count=5
            )
        elif isinstance(packet, FetchPacket):
            packet.rows = []
        return packet

    mock_connection._send_and_receive.side_effect = send
    cursor.execute("SELECT id FROM t")
    assert cursor.fetchone() is None


def test_fetchmany_with_size_and_default_arraysize(
    cursor: Cursor, mock_connection: MagicMock
) -> None:
    def send(packet: object) -> object:
        if isinstance(packet, PrepareAndExecutePacket):
            _set_prepare_packet(
                packet,
                stmt_type=CUBRIDStatementType.SELECT,
                rows=[[1], [2], [3]],
                total_count=3,
                result_count=3,
            )
        return packet

    mock_connection._send_and_receive.side_effect = send
    cursor.execute("SELECT id FROM t")
    assert cursor.fetchmany(2) == [(1,), (2,)]
    cursor.arraysize = 2
    assert cursor.fetchmany() == [(3,)]


def test_fetchall_returns_remaining_rows(cursor: Cursor, mock_connection: MagicMock) -> None:
    def send(packet: object) -> object:
        if isinstance(packet, PrepareAndExecutePacket):
            _set_prepare_packet(
                packet,
                stmt_type=CUBRIDStatementType.SELECT,
                rows=[[1], [2], [3]],
                total_count=3,
            )
        return packet

    mock_connection._send_and_receive.side_effect = send
    cursor.execute("SELECT id FROM t")
    assert cursor.fetchone() == (1,)
    assert cursor.fetchall() == [(2,), (3,)]


def test_executemany_accumulates_non_select_rowcount(
    cursor: Cursor, mock_connection: MagicMock
) -> None:
    counts = iter([1, 2, 3])

    def send(packet: object) -> object:
        if isinstance(packet, PrepareAndExecutePacket):
            _set_prepare_packet(
                packet,
                stmt_type=CUBRIDStatementType.UPDATE,
                result_count=next(counts),
                with_columns=False,
            )
        return packet

    mock_connection._send_and_receive.side_effect = send
    cursor.executemany("UPDATE t SET v = ?", [[1], [2], [3]])
    assert cursor.rowcount == 6


def test_executemany_select_keeps_rowcount_negative(
    cursor: Cursor, mock_connection: MagicMock
) -> None:
    def send(packet: object) -> object:
        if isinstance(packet, PrepareAndExecutePacket):
            _set_prepare_packet(
                packet, stmt_type=CUBRIDStatementType.SELECT, rows=[[1]], total_count=1
            )
        return packet

    mock_connection._send_and_receive.side_effect = send
    cursor.executemany("SELECT ?", [[1], [2]])
    assert cursor.rowcount == -1


def test_executemany_batch_executes_multiple_sql(
    cursor: Cursor, mock_connection: MagicMock
) -> None:
    expected_results = [(20, 2), (22, 1)]

    def send(packet: object) -> object:
        if isinstance(packet, BatchExecutePacket):
            packet.results = expected_results
        return packet

    mock_connection._send_and_receive.side_effect = send

    result = cursor.executemany_batch(["INSERT INTO t VALUES (1)", "UPDATE t SET v = 2"])

    sent_packet = mock_connection._send_and_receive.call_args.args[0]
    assert isinstance(sent_packet, BatchExecutePacket)
    assert sent_packet.sql_list == ["INSERT INTO t VALUES (1)", "UPDATE t SET v = 2"]
    assert sent_packet.auto_commit is False
    assert result == expected_results
    assert cursor.rowcount == 3


def test_executemany_batch_auto_commit_override(cursor: Cursor, mock_connection: MagicMock) -> None:
    cursor.executemany_batch(["DELETE FROM t"], auto_commit=True)

    sent_packet = mock_connection._send_and_receive.call_args.args[0]
    assert isinstance(sent_packet, BatchExecutePacket)
    assert sent_packet.auto_commit is True


def test_executemany_batch_empty_list_sets_zero_rowcount(
    cursor: Cursor, mock_connection: MagicMock
) -> None:
    def send(packet: object) -> object:
        if isinstance(packet, BatchExecutePacket):
            packet.results = []
        return packet

    mock_connection._send_and_receive.side_effect = send

    result = cursor.executemany_batch([])

    assert result == []
    assert cursor.rowcount == 0


def test_executemany_batch_resets_result_state(cursor: Cursor, mock_connection: MagicMock) -> None:
    cursor._description = (("id", 8, None, None, 10, 0, False),)
    cursor._rows = [[1], [2]]
    cursor._row_index = 1
    cursor._query_handle = 100

    def send(packet: object) -> object:
        if isinstance(packet, BatchExecutePacket):
            packet.results = [(20, 1)]
        return packet

    mock_connection._send_and_receive.side_effect = send

    cursor.executemany_batch(["INSERT INTO t VALUES (1)"])

    assert cursor.description is None
    assert cursor._rows == []
    assert cursor._row_index == 0
    assert cursor._query_handle is None


def test_executemany_batch_rowcount_sums_all_counts(
    cursor: Cursor, mock_connection: MagicMock
) -> None:
    def send(packet: object) -> object:
        if isinstance(packet, BatchExecutePacket):
            packet.results = [(20, 2), (22, 5), (23, 1)]
        return packet

    mock_connection._send_and_receive.side_effect = send

    cursor.executemany_batch(["INSERT INTO t VALUES (1)", "UPDATE t SET v = 3", "DELETE FROM t"])

    assert cursor.rowcount == 8


def test_executemany_batch_closed_cursor_raises(cursor: Cursor) -> None:
    cursor.close()

    with pytest.raises(InterfaceError, match="closed"):
        cursor.executemany_batch(["SELECT 1"])


def test_close_sends_close_query_and_removes_cursor(
    cursor: Cursor, mock_connection: MagicMock
) -> None:
    cursor._query_handle = 44
    cursor.close()
    sent = mock_connection._send_and_receive.call_args.args[0]
    assert isinstance(sent, CloseQueryPacket)
    assert cursor._closed is True
    assert cursor not in mock_connection._cursors


def test_close_is_idempotent(cursor: Cursor, mock_connection: MagicMock) -> None:
    cursor.close()
    first_calls = mock_connection._send_and_receive.call_count
    cursor.close()
    assert mock_connection._send_and_receive.call_count == first_calls


def test_callproc_formats_sql_and_returns_parameters(
    cursor: Cursor, mock_connection: MagicMock
) -> None:
    captured_sql: list[str] = []

    def send(packet: object) -> object:
        if isinstance(packet, PrepareAndExecutePacket):
            captured_sql.append(packet.sql)
            _set_prepare_packet(
                packet, stmt_type=CUBRIDStatementType.SELECT, rows=[[1]], total_count=1
            )
        return packet

    mock_connection._send_and_receive.side_effect = send
    params = (1, "x")
    returned = cursor.callproc("my_proc", params)
    assert returned == params
    assert captured_sql == ["CALL my_proc(1, 'x')"]


def test_callproc_without_parameters(cursor: Cursor, mock_connection: MagicMock) -> None:
    captured_sql: list[str] = []

    def send(packet: object) -> object:
        if isinstance(packet, PrepareAndExecutePacket):
            captured_sql.append(packet.sql)
            _set_prepare_packet(
                packet, stmt_type=CUBRIDStatementType.SELECT, rows=[[1]], total_count=1
            )
        return packet

    mock_connection._send_and_receive.side_effect = send
    returned = cursor.callproc("my_proc")
    assert returned == ()
    assert captured_sql == ["CALL my_proc()"]


def test_iterator_protocol(cursor: Cursor, mock_connection: MagicMock) -> None:
    def send(packet: object) -> object:
        if isinstance(packet, PrepareAndExecutePacket):
            _set_prepare_packet(
                packet,
                stmt_type=CUBRIDStatementType.SELECT,
                rows=[[1], [2]],
                total_count=2,
            )
        return packet

    mock_connection._send_and_receive.side_effect = send
    cursor.execute("SELECT id FROM t")
    assert iter(cursor) is cursor
    assert next(cursor) == (1,)
    assert next(cursor) == (2,)
    with pytest.raises(StopIteration):
        next(cursor)


def test_context_manager_closes_cursor(mock_connection: MagicMock) -> None:
    with Cursor(mock_connection) as cur:
        assert cur._closed is False
    assert cur._closed is True


def test_closed_cursor_raises_interface_error(cursor: Cursor) -> None:
    cursor.close()
    with pytest.raises(InterfaceError, match="closed"):
        cursor.execute("SELECT 1")


def test_setinputsizes_and_setoutputsize_are_noops(cursor: Cursor) -> None:
    cursor.setinputsizes([1, 2, 3])
    cursor.setoutputsize(10, 1)
