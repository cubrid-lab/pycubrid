"""Zero and out-of-range temporal values keep the session usable (#512).

CUBRID accepts zero dates such as ``DATE'0000-00-00'`` and sends them to the
client, but Python ``datetime`` cannot represent year 0. The reply is complete
when the value is decoded, so this is a data problem, not a framing problem:
the driver must raise ``DataError`` and keep the connection, exactly as for
invalid UTF-8 (#492) and unresolvable time zones (#413).
"""

from __future__ import annotations

import struct
from unittest.mock import AsyncMock, MagicMock

import pytest

from pycubrid.aio.connection import AsyncConnection
from pycubrid.aio.cursor import AsyncCursor
from pycubrid.connection import Connection
from pycubrid.constants import CUBRIDDataType, CUBRIDStatementType
from pycubrid.cursor import Cursor
from pycubrid.exceptions import DataError, OperationalError
from pycubrid.packet import PacketReader
from pycubrid.protocol import CloseQueryPacket, FetchPacket, PrepareAndExecutePacket, _read_value
from tests.test_connection import socket_queue  # noqa: F401
from tests.test_invalid_utf8_response import (
    CAS_INFO,
    _async_connection_with_reply,
    _connection_with_reply,
)
from tests.test_json_decode import _build_select_response
from tests.test_network_edge_cases import make_connected_connection as make_edge_connection
from tests.test_prepared_session_fence import _response_socket

_ZERO_TZ = b"+09:00\x00"

# (column type, wire payload of a zero value of that type)
ZERO_VALUES = [
    pytest.param(CUBRIDDataType.DATE, struct.pack(">3h", 0, 0, 0), id="date"),
    pytest.param(CUBRIDDataType.DATETIME, struct.pack(">7h", 0, 0, 0, 0, 0, 0, 0), id="datetime"),
    pytest.param(CUBRIDDataType.TIMESTAMP, struct.pack(">6h", 0, 0, 0, 0, 0, 0), id="timestamp"),
    pytest.param(
        CUBRIDDataType.TIMESTAMPTZ,
        struct.pack(">6h", 0, 0, 0, 0, 0, 0) + _ZERO_TZ,
        id="timestamptz",
    ),
    pytest.param(
        CUBRIDDataType.TIMESTAMPLTZ,
        struct.pack(">6h", 0, 0, 0, 0, 0, 0) + _ZERO_TZ,
        id="timestampltz",
    ),
    pytest.param(
        CUBRIDDataType.DATETIMETZ,
        struct.pack(">7h", 0, 0, 0, 0, 0, 0, 0) + _ZERO_TZ,
        id="datetimetz",
    ),
    pytest.param(
        CUBRIDDataType.DATETIMELTZ,
        struct.pack(">7h", 0, 0, 0, 0, 0, 0, 0) + _ZERO_TZ,
        id="datetimeltz",
    ),
]


@pytest.mark.parametrize(("column_type", "payload"), ZERO_VALUES)
def test_zero_temporal_value_raises_data_error(column_type: int, payload: bytes) -> None:
    with pytest.raises(DataError, match="cannot be represented") as raised:
        _read_value(PacketReader(payload), column_type, len(payload))
    assert isinstance(raised.value.__cause__, ValueError)


def test_out_of_range_time_raises_data_error() -> None:
    payload = struct.pack(">3h", 25, 0, 0)
    with pytest.raises(DataError, match="cannot be represented"):
        _read_value(PacketReader(payload), CUBRIDDataType.TIME, len(payload))


def test_zero_date_collection_element_raises_data_error() -> None:
    element = struct.pack(">3h", 0, 0, 0)
    payload = struct.pack(">Bi", CUBRIDDataType.DATE, 1) + struct.pack(">i", len(element))
    payload += element
    reader = PacketReader(payload, decode_collections=True)
    with pytest.raises(DataError, match="cannot be represented"):
        _read_value(reader, CUBRIDDataType.SET, len(payload))


def test_valid_temporal_value_is_unchanged() -> None:
    payload = struct.pack(">3h", 2024, 1, 15)
    value = _read_value(PacketReader(payload), CUBRIDDataType.DATE, len(payload))
    assert str(value) == "2024-01-15"


def _zero_date_select_body() -> bytes:
    return _build_select_response([(CUBRIDDataType.DATE, "d")], [struct.pack(">3h", 0, 0, 0)])


def test_zero_date_on_fetch_page_raises_data_error() -> None:
    columns = PrepareAndExecutePacket("SELECT d FROM t")
    columns.parse(
        _build_select_response([(CUBRIDDataType.DATE, "d")], [struct.pack(">3h", 2024, 1, 1)])
    )
    row_value = struct.pack(">3h", 0, 0, 0)
    body = CAS_INFO + struct.pack(">ii", 0, 1) + struct.pack(">i", 1) + b"\x00" * 8
    body += struct.pack(">i", len(row_value)) + row_value
    packet = FetchPacket(1, 0, statement_type=CUBRIDStatementType.SELECT)
    with pytest.raises(DataError, match="cannot be represented"):
        packet.parse(body, columns=columns.columns)


def test_sync_zero_date_keeps_connection(
    socket_queue: list,  # noqa: F811
) -> None:
    conn, _ = _connection_with_reply(socket_queue, _zero_date_select_body())
    assert isinstance(conn, Connection)
    with pytest.raises(DataError, match="cannot be represented"):
        conn._send_and_receive(PrepareAndExecutePacket("SELECT d FROM t"))
    assert conn._connected is True
    assert conn._socket is not None


@pytest.mark.asyncio
async def test_async_zero_date_keeps_connection() -> None:
    conn = _async_connection_with_reply(_zero_date_select_body())
    assert isinstance(conn, AsyncConnection)
    with pytest.raises(DataError, match="cannot be represented"):
        await conn._send_and_receive(PrepareAndExecutePacket("SELECT d FROM t"))
    assert conn._connected is True
    assert conn._writer is not None


# --- a zero date must not mask a truncated reply (#383) ----------------------

_ZERO_DATE = struct.pack(">3h", 0, 0, 0)


def _date_columns() -> list:
    columns = PrepareAndExecutePacket("SELECT d FROM t")
    columns.parse(
        _build_select_response([(CUBRIDDataType.DATE, "d")], [struct.pack(">3h", 2024, 1, 1)])
    )
    return columns.columns


def _fetch_body(values: list[bytes], *, cut: int = 0) -> bytes:
    """A FETCH reply with one DATE value per row, minus ``cut`` trailing bytes."""
    body = CAS_INFO + struct.pack(">ii", 0, len(values))
    for index, value in enumerate(values):
        body += struct.pack(">i", index + 1) + b"\x00" * 8
        body += struct.pack(">i", len(value)) + value
    return body[: len(body) - cut] if cut else body


def test_zero_date_before_truncated_row_is_a_framing_error() -> None:
    body = _fetch_body([_ZERO_DATE, struct.pack(">3h", 2024, 1, 2)], cut=3)
    packet = FetchPacket(1, 0, statement_type=CUBRIDStatementType.SELECT)
    with pytest.raises(ValueError, match="past the end") as raised:
        packet.parse(body, columns=_date_columns())
    assert not isinstance(raised.value, DataError)


def test_zero_date_before_overlong_value_is_a_framing_error() -> None:
    # The second value declares more bytes than the reply holds.
    body = _fetch_body([_ZERO_DATE])
    body += struct.pack(">i", 2) + b"\x00" * 8 + struct.pack(">i", 64) + b"\x00" * 6
    body = body[:4] + struct.pack(">ii", 0, 2) + body[12:]
    packet = FetchPacket(1, 0, statement_type=CUBRIDStatementType.SELECT)
    # A DATE cell must be exactly 6 bytes (#523), checked before the overrun.
    with pytest.raises(ValueError, match="cell size 64 does not fit a DATE"):
        packet.parse(body, columns=_date_columns())


# (column type, full-width zero payload, declared size one byte too small)
UNDERSIZED_ZERO_VALUES = [
    pytest.param(CUBRIDDataType.DATE, struct.pack(">3h", 0, 0, 0), 5, id="date"),
    pytest.param(
        CUBRIDDataType.DATETIME, struct.pack(">7h", 0, 0, 0, 0, 0, 0, 0), 13, id="datetime"
    ),
    pytest.param(
        CUBRIDDataType.TIMESTAMP, struct.pack(">6h", 0, 0, 0, 0, 0, 0), 11, id="timestamp"
    ),
    pytest.param(
        CUBRIDDataType.TIMESTAMPTZ, struct.pack(">6h", 0, 0, 0, 0, 0, 0), 11, id="timestamptz"
    ),
    pytest.param(
        CUBRIDDataType.DATETIMETZ, struct.pack(">7h", 0, 0, 0, 0, 0, 0, 0), 13, id="datetimetz"
    ),
]


@pytest.mark.parametrize(("column_type", "payload", "size"), UNDERSIZED_ZERO_VALUES)
def test_undersized_zero_temporal_field_is_a_framing_error(
    column_type: int, payload: bytes, size: int
) -> None:
    # The decoder would read past its field, so this is not a data error.
    with pytest.raises(ValueError, match="wrong field size") as raised:
        _read_value(PacketReader(payload), column_type, size)
    assert not isinstance(raised.value, DataError)


def _date_int_columns() -> list:
    columns = PrepareAndExecutePacket("SELECT d, i FROM t")
    columns.parse(
        _build_select_response(
            [(CUBRIDDataType.DATE, "d"), (CUBRIDDataType.INT, "i")],
            [struct.pack(">3h", 2024, 1, 1), struct.pack(">i", 1)],
        )
    )
    return columns.columns


def _undersized_date_then_int_body() -> bytes:
    # DATE declared as 5 bytes: _parse_date() would take the first byte of the
    # INT length as its sixth byte, and the declared sizes still add up.
    body = CAS_INFO + struct.pack(">ii", 0, 1) + struct.pack(">i", 1) + b"\x00" * 8
    body += struct.pack(">i", 5) + b"\x00" * 5
    body += struct.pack(">i", 4) + struct.pack(">i", 7)
    return body


def test_undersized_zero_date_before_complete_column_is_a_framing_error() -> None:
    packet = FetchPacket(1, 0, statement_type=CUBRIDStatementType.SELECT)
    # The cell size is checked against the DATE width before decoding (#523).
    with pytest.raises(ValueError, match="cell size 5 does not fit a DATE"):
        packet.parse(_undersized_date_then_int_body(), columns=_date_int_columns())


def test_sync_undersized_zero_date_closes_connection(
    socket_queue: list,  # noqa: F811
) -> None:
    conn, _ = _connection_with_reply(socket_queue, _undersized_date_then_int_body())
    packet = FetchPacket(
        1, 0, columns=_date_int_columns(), statement_type=CUBRIDStatementType.SELECT
    )
    with pytest.raises(OperationalError, match="malformed response"):
        conn._send_and_receive(packet)
    assert conn._connected is False


def test_undersized_typed_call_value_is_a_framing_error() -> None:
    # CALL results carry a type byte inside the value; 1 + 5 bytes is short.
    body = CAS_INFO + struct.pack(">ii", 0, 1) + struct.pack(">i", 1) + b"\x00" * 8
    body += struct.pack(">iB", 6, CUBRIDDataType.DATE) + b"\x00" * 5
    body += struct.pack(">i", 4) + struct.pack(">i", 7)
    packet = FetchPacket(1, 0, statement_type=CUBRIDStatementType.CALL)
    with pytest.raises(ValueError, match="cell size 5 does not fit a DATE"):
        packet.parse(body, columns=_date_int_columns())


def test_undersized_collection_element_is_a_framing_error() -> None:
    payload = struct.pack(">Bi", CUBRIDDataType.DATE, 1) + struct.pack(">i", 5) + b"\x00" * 6
    reader = PacketReader(payload, decode_collections=True)
    with pytest.raises(ValueError, match="wrong field size"):
        _read_value(reader, CUBRIDDataType.SET, len(payload))


def test_collection_element_past_collection_size_is_a_framing_error() -> None:
    payload = struct.pack(">Bi", CUBRIDDataType.DATE, 2)
    payload += struct.pack(">i", 6) + _ZERO_DATE + struct.pack(">i", 64) + b"\x00" * 6
    reader = PacketReader(payload, decode_collections=True)
    with pytest.raises(ValueError, match="exceed its size"):
        _read_value(reader, CUBRIDDataType.SEQUENCE, len(payload))


def test_zero_date_in_complete_collection_with_more_elements_raises_data_error() -> None:
    payload = struct.pack(">Bi", CUBRIDDataType.DATE, 3)
    for element in (_ZERO_DATE, struct.pack(">3h", 2024, 1, 2)):
        payload += struct.pack(">i", 6) + element
    payload += struct.pack(">i", -1)
    reader = PacketReader(payload, decode_collections=True)
    with pytest.raises(DataError, match="cannot be represented"):
        _read_value(reader, CUBRIDDataType.SEQUENCE, len(payload))
    assert reader.bytes_remaining() == 0


def test_zero_date_in_complete_multi_row_reply_raises_data_error() -> None:
    body = _fetch_body([struct.pack(">3h", 2024, 1, 2), _ZERO_DATE, struct.pack(">3h", 2024, 1, 3)])
    packet = FetchPacket(1, 0, statement_type=CUBRIDStatementType.SELECT)
    with pytest.raises(DataError, match=r"DATE value \(0, 0, 0\)"):
        packet.parse(body, columns=_date_columns())


def test_sync_truncated_reply_with_zero_date_closes_connection(
    socket_queue: list,  # noqa: F811
) -> None:
    body = _fetch_body([_ZERO_DATE, struct.pack(">3h", 2024, 1, 2)], cut=3)
    conn, _ = _connection_with_reply(socket_queue, body)
    packet = FetchPacket(1, 0, columns=_date_columns(), statement_type=CUBRIDStatementType.SELECT)
    with pytest.raises(OperationalError, match="malformed response"):
        conn._send_and_receive(packet)
    assert conn._connected is False


@pytest.mark.asyncio
async def test_async_truncated_reply_with_zero_date_closes_connection() -> None:
    body = _fetch_body([_ZERO_DATE, struct.pack(">3h", 2024, 1, 2)], cut=3)
    conn = _async_connection_with_reply(body)
    packet = FetchPacket(1, 0, columns=_date_columns(), statement_type=CUBRIDStatementType.SELECT)
    with pytest.raises(OperationalError, match="malformed response"):
        await conn._send_and_receive(packet)
    assert conn._connected is False


# --- cursor state matches #492 ----------------------------------------------


def _reply_with_zero_date(packet: object, **_kwargs: object) -> object:
    if isinstance(packet, PrepareAndExecutePacket):
        packet.parse(_zero_date_select_body())
    return packet


def _mock_connection(asynchronous: bool) -> MagicMock:
    connection = MagicMock()
    connection._timing = None
    connection._cursors = set()
    connection.autocommit = True
    connection._protocol_version = 8
    connection._decode_collections = False
    connection._json_deserializer = None
    if asynchronous:
        connection._send_and_receive = AsyncMock(side_effect=_reply_with_zero_date)
        connection._wait_for_setup_if_needed = AsyncMock()
    else:
        connection._send_and_receive = MagicMock(side_effect=_reply_with_zero_date)
    return connection


def _closed_handles(connection: MagicMock) -> list[int]:
    return [
        c.args[0].query_handle
        for c in connection._send_and_receive.call_args_list
        if isinstance(c.args[0], CloseQueryPacket)
    ]


def test_sync_cursor_owns_handle_after_zero_date() -> None:
    connection = _mock_connection(asynchronous=False)
    cursor = Cursor(connection)
    with pytest.raises(DataError, match="cannot be represented"):
        cursor.execute("SELECT d FROM t")
    assert cursor._query_handle == 1
    assert cursor.description is None
    assert cursor.rowcount == -1
    cursor.close()
    assert _closed_handles(connection) == [1]


@pytest.mark.asyncio
async def test_async_cursor_owns_handle_after_zero_date() -> None:
    connection = _mock_connection(asynchronous=True)
    cursor = AsyncCursor(connection)
    with pytest.raises(DataError, match="cannot be represented"):
        await cursor.execute("SELECT d FROM t")
    assert cursor._query_handle == 1
    assert cursor.description is None
    assert cursor.rowcount == -1
    await cursor.close()
    assert _closed_handles(connection) == [1]


def _reply_with_zero_date_on_second_page(packet: object, **_kwargs: object) -> object:
    if isinstance(packet, PrepareAndExecutePacket):
        packet.parse(
            _build_select_response([(CUBRIDDataType.DATE, "d")], [struct.pack(">3h", 2024, 1, 1)])
        )
        packet.total_tuple_count = 2
    elif isinstance(packet, FetchPacket):
        packet.parse(_fetch_body([_ZERO_DATE]))
    return packet


def test_sync_cursor_keeps_result_set_after_zero_date_on_fetch_page() -> None:
    connection = _mock_connection(asynchronous=False)
    connection._send_and_receive.side_effect = _reply_with_zero_date_on_second_page
    cursor = Cursor(connection)
    cursor.execute("SELECT d FROM t")
    assert cursor.fetchone() is not None
    with pytest.raises(DataError, match="cannot be represented"):
        cursor.fetchone()
    # The server cursor is still open and owned; retrying hits the same row.
    assert cursor._query_handle == 1
    assert cursor.description is not None
    with pytest.raises(DataError, match="cannot be represented"):
        cursor.fetchone()
    cursor.close()
    assert _closed_handles(connection) == [1]


@pytest.mark.asyncio
async def test_async_cursor_keeps_result_set_after_zero_date_on_fetch_page() -> None:
    connection = _mock_connection(asynchronous=True)
    connection._send_and_receive.side_effect = _reply_with_zero_date_on_second_page
    cursor = AsyncCursor(connection)
    await cursor.execute("SELECT d FROM t")
    assert await cursor.fetchone() is not None
    with pytest.raises(DataError, match="cannot be represented"):
        await cursor.fetchone()
    assert cursor._query_handle == 1
    assert cursor.description is not None
    await cursor.close()
    assert _closed_handles(connection) == [1]


# --- pycubrid.compat.native stays fail-closed -------------------------------


def test_native_prepared_zero_date_retires_session() -> None:
    # The explicit prepared API keeps its fail-closed contract, as for #492
    # and #413: any client-side decode failure retires the prepared session.
    conn, sock = make_edge_connection()
    sock.recv_into.side_effect = _response_socket().recv_into.side_effect
    packet = MagicMock()
    packet.write.return_value = b"prepared request"
    packet.parse.side_effect = lambda _body: _read_value(
        PacketReader(_ZERO_DATE), CUBRIDDataType.DATE, len(_ZERO_DATE)
    )
    with pytest.raises(OperationalError, match="malformed response") as raised:
        conn._send_and_receive(packet, expected_generation=conn._physical_generation)
    assert isinstance(raised.value.__cause__, DataError)
    assert conn._connected is False
