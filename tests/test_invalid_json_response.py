"""Invalid JSON text in a fully read reply keeps the session usable (#543).

A ``JSON`` cell whose text is not valid JSON, decoded with the built-in
``json.loads`` deserializer, raised ``json.JSONDecodeError`` -- a ``ValueError``
subclass that the connection layer reported as
``OperationalError('malformed response from broker')`` and closed the
session, although the reply itself was read in full. Under the #492/#512
contract a complete reply holding a value the client cannot represent is a
data problem: ``PacketReader._parse_json`` now raises ``DataError`` (the
``JSONDecodeError`` chained as its cause), and ``_parse_row_data`` applies the
same complete-reply check as for invalid UTF-8 and zero dates before
re-raising it, so an ordinary connection and cursor stay usable. A
caller-supplied ``json_deserializer`` is not wrapped: only the built-in
``json.loads`` path is reclassified. The explicit prepared API
(``pycubrid.compat.native``), which threads the same ``json_deserializer``,
keeps its documented fail-closed contract: it raises ``OperationalError`` and
retires the session, as for invalid UTF-8 and zero dates.

All tests below are offline/synthetic. A live reproduction was attempted on a
dedicated CUBRID 11.4.6 container: CUBRID validates JSON text strictly at
write time (``INSERT``, ``CAST(... AS JSON)``) and rejects every malformed or
JSON5-ish candidate tried (unquoted/single-quoted keys, trailing commas,
``NaN``/``Infinity``, leading-zero/bare-decimal numbers) with its own
``DatabaseError`` (errno -1197) before the value ever reaches the wire, so no
invalid JSON text was reproducible from a genuine server reply; a valid JSON
value still round-trips correctly through the live server with
``json_deserializer=json.loads``. This matches the issue's own reproduction,
which came from the realistic-seed protocol fuzzer (#523), not a live server.
"""

from __future__ import annotations

import json
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

# A JSON cell whose text is not valid JSON at all (unterminated object).
BAD_JSON = b'{"a":'


def _encode_json(value: bytes) -> bytes:
    return value + b"\x00"


def _json_select_body(value: bytes = BAD_JSON) -> bytes:
    return _build_select_response([(CUBRIDDataType.JSON, "payload")], [_encode_json(value)])


# --- packet layer ------------------------------------------------------------


def test_invalid_json_value_raises_data_error() -> None:
    payload = _encode_json(BAD_JSON)
    with pytest.raises(DataError, match="not valid JSON") as raised:
        _read_value(
            PacketReader(payload, json_deserializer=json.loads),
            CUBRIDDataType.JSON,
            len(payload),
        )
    assert isinstance(raised.value.__cause__, json.JSONDecodeError)


def test_valid_json_value_is_unchanged() -> None:
    payload = _encode_json(b'{"a": 1}')
    value = _read_value(
        PacketReader(payload, json_deserializer=json.loads),
        CUBRIDDataType.JSON,
        len(payload),
    )
    assert value == {"a": 1}


def test_custom_json_deserializer_is_not_wrapped() -> None:
    # Only the built-in json.loads path is reclassified; a caller-supplied
    # deserializer's own exceptions are not caught here (#543).
    payload = _encode_json(BAD_JSON)

    def _boom(_value: str) -> None:
        raise RuntimeError("custom deserializer failure")

    with pytest.raises(RuntimeError, match="custom deserializer failure"):
        _read_value(
            PacketReader(payload, json_deserializer=_boom),
            CUBRIDDataType.JSON,
            len(payload),
        )


def test_invalid_json_on_execute_reply_raises_data_error() -> None:
    packet = PrepareAndExecutePacket("SELECT payload FROM t", json_deserializer=json.loads)
    with pytest.raises(DataError, match="not valid JSON"):
        packet.parse(_json_select_body())


def test_invalid_json_on_fetch_page_raises_data_error() -> None:
    columns = PrepareAndExecutePacket("SELECT payload FROM t", json_deserializer=json.loads)
    columns.parse(_json_select_body(b'{"a": 1}'))
    row_value = _encode_json(BAD_JSON)
    body = CAS_INFO + struct.pack(">ii", 0, 1) + struct.pack(">i", 1) + b"\x00" * 8
    body += struct.pack(">i", len(row_value)) + row_value
    packet = FetchPacket(
        1, 0, statement_type=CUBRIDStatementType.SELECT, json_deserializer=json.loads
    )
    with pytest.raises(DataError, match="not valid JSON"):
        packet.parse(body, columns=columns.columns)


def test_truncated_json_value_stays_a_framing_error() -> None:
    body = _json_select_body()
    packet = PrepareAndExecutePacket("SELECT payload FROM t", json_deserializer=json.loads)
    with pytest.raises(ValueError, match="past the end") as raised:
        packet.parse(body[:-3])
    assert not isinstance(raised.value, DataError)


def test_invalid_json_before_truncated_row_stays_a_framing_error() -> None:
    # DataError is only for a complete reply: a later short row wins (#512).
    good = _encode_json(b'{"a": 1}')
    body = CAS_INFO + struct.pack(">ii", 0, 2)
    for index, value in enumerate((_encode_json(BAD_JSON), good)):
        body += struct.pack(">i", index + 1) + b"\x00" * 8
        body += struct.pack(">i", len(value)) + value
    columns = PrepareAndExecutePacket("SELECT payload FROM t", json_deserializer=json.loads)
    columns.parse(_json_select_body(b'{"a": 1}'))
    packet = FetchPacket(
        1, 0, statement_type=CUBRIDStatementType.SELECT, json_deserializer=json.loads
    )
    with pytest.raises(ValueError, match="past the end") as raised:
        packet.parse(body[:-2], columns=columns.columns)
    assert not isinstance(raised.value, DataError)


# --- sync connection and cursor ---------------------------------------------


def test_sync_invalid_json_value_keeps_connection(
    socket_queue: list[MagicMock],  # noqa: F811
) -> None:
    conn, _ = _connection_with_reply(socket_queue, _json_select_body())
    assert isinstance(conn, Connection)
    with pytest.raises(DataError, match="not valid JSON"):
        conn._send_and_receive(
            PrepareAndExecutePacket("SELECT payload FROM t", json_deserializer=json.loads)
        )
    assert conn._connected is True
    assert conn._socket is not None


def _reply_with_bad_json(packet: object, **_kwargs: object) -> object:
    if isinstance(packet, PrepareAndExecutePacket):
        packet.parse(_json_select_body())
    return packet


def _mock_connection(asynchronous: bool) -> MagicMock:
    connection = MagicMock()
    connection._timing = None
    connection._cursors = set()
    connection.autocommit = True
    connection._protocol_version = 8
    connection._decode_collections = False
    connection._json_deserializer = json.loads
    if asynchronous:
        connection._send_and_receive = AsyncMock(side_effect=_reply_with_bad_json)
        connection._wait_for_setup_if_needed = AsyncMock()
    else:
        connection._send_and_receive = MagicMock(side_effect=_reply_with_bad_json)
    return connection


def _closed_handles(connection: MagicMock) -> list[int]:
    return [
        c.args[0].query_handle
        for c in connection._send_and_receive.call_args_list
        if isinstance(c.args[0], CloseQueryPacket)
    ]


def test_sync_cursor_owns_handle_after_json_decode_failure() -> None:
    connection = _mock_connection(asynchronous=False)
    cursor = Cursor(connection)
    cursor._lastrowid = 7  # left over from an earlier INSERT
    with pytest.raises(DataError, match="not valid JSON"):
        cursor.execute("SELECT payload FROM t")
    assert cursor._query_handle == 1
    assert cursor.description is None
    assert cursor.rowcount == -1
    assert cursor.lastrowid is None
    cursor.close()
    assert _closed_handles(connection) == [1]


# --- async connection and cursor --------------------------------------------


@pytest.mark.asyncio
async def test_async_invalid_json_value_keeps_connection() -> None:
    conn = _async_connection_with_reply(_json_select_body())
    assert isinstance(conn, AsyncConnection)
    with pytest.raises(DataError, match="not valid JSON"):
        await conn._send_and_receive(
            PrepareAndExecutePacket("SELECT payload FROM t", json_deserializer=json.loads)
        )
    assert conn._connected is True
    assert conn._writer is not None


@pytest.mark.asyncio
async def test_async_cursor_owns_handle_after_json_decode_failure() -> None:
    connection = _mock_connection(asynchronous=True)
    cursor = AsyncCursor(connection)
    with pytest.raises(DataError, match="not valid JSON"):
        await cursor.execute("SELECT payload FROM t")
    assert cursor._query_handle == 1
    assert cursor.description is None
    assert cursor.rowcount == -1
    await cursor.close()
    assert _closed_handles(connection) == [1]


# --- a later fetch page raising invalid JSON is the same as zero date (#507) -


def _reply_with_bad_json_on_second_page(packet: object, **_kwargs: object) -> object:
    if isinstance(packet, PrepareAndExecutePacket):
        packet.parse(_json_select_body(b'{"a": 1}'))
        packet.total_tuple_count = 2
    elif isinstance(packet, FetchPacket):
        body = CAS_INFO + struct.pack(">ii", 0, 1) + struct.pack(">i", 1) + b"\x00" * 8
        row_value = _encode_json(BAD_JSON)
        body += struct.pack(">i", len(row_value)) + row_value
        packet.parse(body)
    return packet


def test_sync_cursor_keeps_result_set_after_invalid_json_on_fetch_page() -> None:
    connection = _mock_connection(asynchronous=False)
    connection._send_and_receive.side_effect = _reply_with_bad_json_on_second_page
    cursor = Cursor(connection)
    cursor.execute("SELECT payload FROM t")
    assert cursor.fetchone() is not None
    with pytest.raises(DataError, match="not valid JSON"):
        cursor.fetchone()
    # The server cursor is still open and owned; retrying hits the same row.
    assert cursor._query_handle == 1
    assert cursor.description is not None
    with pytest.raises(DataError, match="not valid JSON"):
        cursor.fetchone()
    cursor.close()
    assert _closed_handles(connection) == [1]


@pytest.mark.asyncio
async def test_async_cursor_keeps_result_set_after_invalid_json_on_fetch_page() -> None:
    connection = _mock_connection(asynchronous=True)
    connection._send_and_receive.side_effect = _reply_with_bad_json_on_second_page
    cursor = AsyncCursor(connection)
    await cursor.execute("SELECT payload FROM t")
    assert await cursor.fetchone() is not None
    with pytest.raises(DataError, match="not valid JSON"):
        await cursor.fetchone()
    assert cursor._query_handle == 1
    assert cursor.description is not None
    await cursor.close()
    assert _closed_handles(connection) == [1]


# --- pycubrid.compat.native stays fail-closed -------------------------------


def test_native_prepared_invalid_json_retires_session() -> None:
    # The explicit prepared API keeps its fail-closed contract, as for #492
    # and #512: any client-side decode failure retires the prepared session.
    conn, sock = make_edge_connection()
    sock.recv_into.side_effect = _response_socket().recv_into.side_effect
    payload = _encode_json(BAD_JSON)
    packet = MagicMock()
    packet.write.return_value = b"prepared request"
    packet.parse.side_effect = lambda _body: _read_value(
        PacketReader(payload, json_deserializer=json.loads),
        CUBRIDDataType.JSON,
        len(payload),
    )
    with pytest.raises(OperationalError, match="malformed response") as raised:
        conn._send_and_receive(packet, expected_generation=conn._physical_generation)
    assert isinstance(raised.value.__cause__, DataError)
    assert conn._connected is False
