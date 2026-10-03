"""Invalid UTF-8 in a fully read reply keeps the session usable (#492)."""

from __future__ import annotations

import struct
from unittest.mock import AsyncMock, MagicMock

import pytest

from pycubrid.aio.connection import AsyncConnection
from pycubrid.aio.cursor import AsyncCursor
from pycubrid.connection import Connection
from pycubrid.constants import CUBRIDDataType, CUBRIDStatementType
from pycubrid.cursor import Cursor
from pycubrid.exceptions import DataError, ProgrammingError
from pycubrid.packet import PacketReader
from pycubrid.protocol import (
    BatchExecutePacket,
    CloseQueryPacket,
    FetchPacket,
    PrepareAndExecutePacket,
    _read_value,
)
from tests.test_connection import make_connected_connection, socket_queue  # noqa: F401
from tests.test_json_decode import _build_select_response

# A 4-byte character cut after two bytes, as CUBRID emits when it truncates
# a message or a VARCHAR value by bytes.
CUT_CHAR = "\U00010000".encode("utf-8")[:2]
CAS_INFO = b"\x00\x01\x02\x03"


def _error_body(code: int, message: bytes) -> bytes:
    return CAS_INFO + struct.pack(">ii", -1, code) + message + b"\x00"


def _select_body(value: bytes) -> bytes:
    return _build_select_response([(CUBRIDDataType.STRING, "v")], [value])


def _frame(body: bytes) -> bytes:
    return struct.pack(">i", len(body) - 4) + body


# --- packet layer ---------------------------------------------------------


def test_error_message_with_cut_character_surfaces_server_error() -> None:
    packet = PrepareAndExecutePacket("INSERT INTO t VALUES (?)")
    with pytest.raises(ProgrammingError) as raised:
        packet.parse(_error_body(-494, b"Cannot coerce 'ab" + CUT_CHAR + b"... to type varchar"))
    assert raised.value.errno == -494
    assert raised.value.sqlstate == "42000"
    assert raised.value.msg == "Cannot coerce 'ab�... to type varchar"


def test_error_message_without_terminator_and_empty_message() -> None:
    reader = PacketReader(b"ab" + CUT_CHAR)
    assert reader._parse_lenient_text(4) == "ab�"
    assert reader._parse_lenient_text(0) == ""


def test_batch_error_message_with_cut_character_is_replaced() -> None:
    text = b"too long " + CUT_CHAR + b"\x00"
    body = struct.pack(">iiBii", 0, 1, CUBRIDStatementType.INSERT, -1, -494)
    body += struct.pack(">i", len(text)) + text + struct.pack(">i", 0)
    packet = BatchExecutePacket(["INSERT INTO t VALUES ('x')"], protocol_version=8)
    packet.parse(CAS_INFO + body)
    assert packet.errors == [{"code": -494, "message": "too long �"}]


@pytest.mark.parametrize(
    "column_type",
    [
        CUBRIDDataType.CHAR,
        CUBRIDDataType.STRING,
        CUBRIDDataType.NCHAR,
        CUBRIDDataType.VARNCHAR,
        CUBRIDDataType.ENUM,
        CUBRIDDataType.JSON,
    ],
)
def test_invalid_text_value_raises_data_error(column_type: int) -> None:
    payload = b"ab" + CUT_CHAR + b"\x00"
    with pytest.raises(DataError, match="not valid UTF-8") as raised:
        _read_value(PacketReader(payload), column_type, len(payload))
    assert isinstance(raised.value.__cause__, UnicodeDecodeError)


def test_invalid_collection_element_raises_data_error() -> None:
    element = b"x" + CUT_CHAR + b"\x00"
    payload = struct.pack(">Bi", CUBRIDDataType.STRING, 1) + struct.pack(">i", len(element))
    payload += element
    reader = PacketReader(payload, decode_collections=True)
    with pytest.raises(DataError, match="not valid UTF-8"):
        _read_value(reader, CUBRIDDataType.SEQUENCE, len(payload))


def test_invalid_fetched_value_raises_data_error() -> None:
    row_value = b"ab" + CUT_CHAR + b"\x00"
    body = CAS_INFO + struct.pack(">ii", 0, 1) + struct.pack(">i", 1) + b"\x00" * 8
    body += struct.pack(">i", len(row_value)) + row_value
    packet = FetchPacket(1, 0, statement_type=CUBRIDStatementType.SELECT)
    columns = PrepareAndExecutePacket("SELECT v FROM t")
    columns.parse(_select_body(b"ok\x00"))
    with pytest.raises(DataError):
        packet.parse(body, columns=columns.columns)


def test_truncated_value_stays_a_framing_error() -> None:
    body = _select_body(b"ab" + CUT_CHAR + b"\x00")
    packet = PrepareAndExecutePacket("SELECT v FROM t")
    with pytest.raises(ValueError, match="past the end") as raised:
        packet.parse(body[:-3])
    assert not isinstance(raised.value, DataError)


def test_invalid_value_before_truncated_row_stays_a_framing_error() -> None:
    # DataError is only for a complete reply: a later short row wins (#512).
    bad = b"ab" + CUT_CHAR + b"\x00"
    body = CAS_INFO + struct.pack(">ii", 0, 2)
    for index, value in enumerate((bad, b"ok\x00")):
        body += struct.pack(">i", index + 1) + b"\x00" * 8
        body += struct.pack(">i", len(value)) + value
    columns = PrepareAndExecutePacket("SELECT v FROM t")
    columns.parse(_select_body(b"ok\x00"))
    packet = FetchPacket(1, 0, statement_type=CUBRIDStatementType.SELECT)
    with pytest.raises(ValueError, match="past the end") as raised:
        packet.parse(body[:-2], columns=columns.columns)
    assert not isinstance(raised.value, DataError)


# --- sync connection and cursor --------------------------------------------


def _connection_with_reply(queue: list[MagicMock], body: bytes) -> tuple[Connection, MagicMock]:
    conn, sock = make_connected_connection(queue)
    frame = _frame(body)
    sock.recv.side_effect = [frame[:4], frame[4:8], frame[8:]]
    return conn, sock


def test_sync_invalid_error_message_keeps_connection(
    socket_queue: list[MagicMock],  # noqa: F811
) -> None:
    conn, _ = _connection_with_reply(socket_queue, _error_body(-494, b"bad " + CUT_CHAR))
    with pytest.raises(ProgrammingError, match="bad �"):
        conn._send_and_receive(PrepareAndExecutePacket("INSERT INTO t VALUES (1)"))
    assert conn._connected is True
    assert conn._socket is not None


def test_sync_invalid_row_value_keeps_connection(
    socket_queue: list[MagicMock],  # noqa: F811
) -> None:
    conn, _ = _connection_with_reply(socket_queue, _select_body(b"ab" + CUT_CHAR + b"\x00"))
    with pytest.raises(DataError):
        conn._send_and_receive(PrepareAndExecutePacket("SELECT v FROM t"))
    assert conn._connected is True
    assert conn._socket is not None


def test_sync_invalid_column_name_raises_data_error(
    socket_queue: list[MagicMock],  # noqa: F811
) -> None:
    # Column names are in the database charset: an undecodable name is a
    # charset mismatch in a fully read reply, not framing damage (#86).
    body = _select_body(b"ok\x00").replace(b"v\x00", b"\xff\x00", 1)
    conn, _ = _connection_with_reply(socket_queue, body)
    with pytest.raises(DataError, match="column metadata is not valid UTF-8"):
        conn._send_and_receive(PrepareAndExecutePacket("SELECT v FROM t"))
    assert conn._connected is True


def _reply_with_bad_row(packet: object) -> object:
    if isinstance(packet, PrepareAndExecutePacket):
        packet.parse(_select_body(b"ab" + CUT_CHAR + b"\x00"))
    return packet


def _mock_connection(asynchronous: bool) -> MagicMock:
    connection = MagicMock()
    connection._timing = None
    connection._cursors = set()
    # A pooling-off broker: CLOSE_REQ is sent, never deferred (#488).
    connection._defer_close = MagicMock(return_value=False)
    connection.autocommit = True
    connection._protocol_version = 8
    connection._decode_collections = False
    connection._json_deserializer = None
    if asynchronous:
        connection._send_and_receive = AsyncMock(side_effect=_reply_with_bad_row)
        connection._wait_for_setup_if_needed = AsyncMock()
    else:
        connection._send_and_receive = MagicMock(side_effect=_reply_with_bad_row)
    return connection


def test_sync_cursor_owns_handle_after_row_decode_failure() -> None:
    connection = _mock_connection(asynchronous=False)
    cursor = Cursor(connection)
    cursor._lastrowid = 7  # left over from an earlier INSERT
    with pytest.raises(DataError):
        cursor.execute("SELECT v FROM t")
    assert cursor._query_handle == 1
    assert cursor.description is None
    assert cursor.rowcount == -1
    assert cursor.lastrowid is None
    cursor.close()
    closes = [
        c.args[0]
        for c in connection._send_and_receive.call_args_list
        if isinstance(c.args[0], CloseQueryPacket)
    ]
    assert [p.query_handle for p in closes] == [1]


# --- async connection and cursor -------------------------------------------


def _async_connection_with_reply(body: bytes) -> AsyncConnection:
    frame = _frame(body)
    conn = AsyncConnection("localhost", 33000, "testdb", "dba", "")
    conn._connected = True
    conn._record_reply_cas_info(b"\x01\x01\x02\x03")  # IN_TRAN: no CHECK_CAS probe first
    reader = MagicMock()
    reader.readexactly = AsyncMock(side_effect=[frame[:4], frame[4:]])
    writer = MagicMock()
    writer.drain = AsyncMock()
    writer.wait_closed = AsyncMock()
    conn._reader = reader
    conn._writer = writer
    return conn


@pytest.mark.asyncio
async def test_async_invalid_error_message_keeps_connection() -> None:
    conn = _async_connection_with_reply(_error_body(-494, b"bad " + CUT_CHAR))
    with pytest.raises(ProgrammingError, match="bad �"):
        await conn._send_and_receive(PrepareAndExecutePacket("INSERT INTO t VALUES (1)"))
    assert conn._connected is True
    assert conn._writer is not None


@pytest.mark.asyncio
async def test_async_invalid_row_value_keeps_connection() -> None:
    conn = _async_connection_with_reply(_select_body(b"ab" + CUT_CHAR + b"\x00"))
    with pytest.raises(DataError):
        await conn._send_and_receive(PrepareAndExecutePacket("SELECT v FROM t"))
    assert conn._connected is True
    assert conn._writer is not None


@pytest.mark.asyncio
async def test_async_cursor_owns_handle_after_row_decode_failure() -> None:
    connection = _mock_connection(asynchronous=True)
    cursor = AsyncCursor(connection)
    cursor._lastrowid = 7  # left over from an earlier INSERT
    with pytest.raises(DataError):
        await cursor.execute("SELECT v FROM t")
    assert cursor._query_handle == 1
    assert cursor.description is None
    assert cursor.rowcount == -1
    assert cursor.lastrowid is None
