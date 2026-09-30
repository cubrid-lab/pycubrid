"""Reads past the end of a broker reply are malformed responses (#383).

A length-prefixed field whose declared size does not fit the reply used to be
cut short by a Python slice and returned as if it were complete; a negative
length moved the reader backwards. Every reader primitive now checks
``0 <= length <= remaining`` before it moves, and raises ``ValueError``, which
the sync and async connections report as ``OperationalError("malformed
response from broker")`` and close the connection. A complete reply with a
value Python cannot represent stays ``DataError`` with the session kept
(#492, #512).
"""

from __future__ import annotations

import struct
from collections.abc import Callable
from typing import Any
from unittest.mock import MagicMock

import pytest

from pycubrid.aio.connection import AsyncConnection
from pycubrid.constants import CUBRIDDataType, CUBRIDStatementType
from pycubrid.exceptions import DataError, OperationalError
from pycubrid.packet import PacketReader
from pycubrid.protocol import ColumnMetaData, FetchPacket, LOBReadPacket, _read_value
from tests.test_connection import socket_queue  # noqa: F401
from tests.test_invalid_utf8_response import (
    CAS_INFO,
    _async_connection_with_reply,
    _connection_with_reply,
)

# --- reader primitives -------------------------------------------------------


def _reader_at(data: bytes, offset: int) -> PacketReader:
    reader = PacketReader(data)
    reader._skip_bytes(offset)
    return reader


def test_parse_bytes_past_end_raises_without_moving() -> None:
    reader = PacketReader(b"abc")
    with pytest.raises(ValueError, match="past the end of the broker reply"):
        reader._parse_bytes(10)
    assert reader._offset == 0
    assert reader.bytes_remaining() == 3


def test_parse_bytes_negative_length_raises_without_moving() -> None:
    reader = _reader_at(b"abcdef", 4)
    with pytest.raises(ValueError, match="negative length"):
        reader._parse_bytes(-3)
    assert reader._offset == 4


def test_skip_bytes_negative_raises_without_moving() -> None:
    reader = PacketReader(b"abcdef")
    with pytest.raises(ValueError, match="negative length"):
        reader._skip_bytes(-2)
    assert reader._offset == 0


def test_skip_bytes_past_end_raises_without_moving() -> None:
    reader = _reader_at(b"abcdef", 2)
    with pytest.raises(ValueError, match="past the end of the broker reply"):
        reader._skip_bytes(5)
    assert reader._offset == 2


def test_exact_reads_to_the_end_still_work() -> None:
    reader = PacketReader(b"abcdef")
    assert reader._parse_bytes(0) == b""
    reader._skip_bytes(2)
    assert reader._parse_bytes(4) == b"cdef"
    assert reader.bytes_remaining() == 0


TEXT_READERS = [
    pytest.param(lambda r, n: r._parse_text_value(n), id="text_value"),
    pytest.param(lambda r, n: r._parse_metadata_text(n), id="metadata_text"),
    pytest.param(lambda r, n: r._parse_lenient_text(n), id="lenient_text"),
    pytest.param(lambda r, n: r._parse_null_terminated_string(n), id="null_terminated"),
    pytest.param(lambda r, n: r._parse_numeric(n), id="numeric"),
    pytest.param(lambda r, n: r._parse_json(n), id="json"),
]


@pytest.mark.parametrize("read", TEXT_READERS)
def test_text_read_past_end_raises_without_moving(
    read: Callable[[PacketReader, int], object],
) -> None:
    reader = PacketReader(b"12\x00")
    with pytest.raises(ValueError, match="past the end of the broker reply") as raised:
        read(reader, 8)
    assert not isinstance(raised.value, DataError)
    assert reader._offset == 0


@pytest.mark.parametrize("read", TEXT_READERS[:4])
def test_text_read_of_non_positive_length_is_empty(
    read: Callable[[PacketReader, int], object],
) -> None:
    # Text readers keep treating a non-positive length as empty; they never
    # move the reader for it.
    reader = PacketReader(b"12\x00")
    assert read(reader, 0) == ""
    assert read(reader, -1) == ""
    assert reader._offset == 0


@pytest.mark.parametrize(
    "name", ["_parse_byte", "_parse_short", "_parse_int", "_parse_long", "_parse_double"]
)
def test_fixed_width_read_past_end_raises_without_moving(name: str) -> None:
    reader = _reader_at(b"\x00", 1)
    with pytest.raises((struct.error, IndexError)):
        getattr(reader, name)()
    assert reader._offset == 1


# --- FETCH rows -------------------------------------------------------------


def _fetch_body(rows: list[list[bytes]]) -> bytes:
    """A FETCH reply whose cells are given raw (length word + payload)."""
    body = CAS_INFO + struct.pack(">ii", 0, len(rows))
    for index, cells in enumerate(rows):
        body += struct.pack(">i", index + 1) + b"\x00" * 8 + b"".join(cells)
    return body


def _cell(payload: bytes, declared: int | None = None) -> bytes:
    size = len(payload) if declared is None else declared
    return struct.pack(">i", size) + payload


def _fetch(
    column_types: list[int], body: bytes, *, decode_collections: bool = False
) -> FetchPacket:
    packet = FetchPacket(
        1,
        0,
        columns=[ColumnMetaData(column_type=t) for t in column_types],
        statement_type=CUBRIDStatementType.SELECT,
        decode_collections=decode_collections,
    )
    packet.parse(body)
    return packet


BIT_TYPES = [
    pytest.param(CUBRIDDataType.BIT, id="bit"),
    pytest.param(CUBRIDDataType.VARBIT, id="varbit"),
]


@pytest.mark.parametrize("column_type", BIT_TYPES)
def test_truncated_bit_cell_in_last_row_is_malformed(column_type: int) -> None:
    # The last cell of the last row declares 8 bytes but the reply ends after 2.
    body = _fetch_body([[_cell(b"\x01")], [_cell(b"\xab\xcd", declared=8)]])
    with pytest.raises(ValueError, match="past the end of the broker reply"):
        _fetch([column_type], body)


@pytest.mark.parametrize("column_type", BIT_TYPES)
def test_bit_cell_overrunning_next_cell_is_malformed(column_type: int) -> None:
    # The first cell claims the next cell's bytes and more than the reply holds.
    body = _fetch_body([[_cell(b"\xab\xcd", declared=12), _cell(b"\x01\x02")]])
    with pytest.raises(ValueError, match="past the end of the broker reply"):
        _fetch([column_type, column_type], body)


@pytest.mark.parametrize("column_type", BIT_TYPES)
def test_valid_bit_cells_are_unchanged(column_type: int) -> None:
    body = _fetch_body([[_cell(b"\xab\xcd"), _cell(b"")], [_cell(b"\x01"), _cell(b"\xff" * 3)]])
    packet = _fetch([column_type, column_type], body)
    assert packet.rows == [(b"\xab\xcd", None), (b"\x01", b"\xff\xff\xff")]


@pytest.mark.parametrize(
    ("column_type", "payload"),
    [
        pytest.param(CUBRIDDataType.STRING, b"abc\x00", id="string"),
        pytest.param(CUBRIDDataType.NUMERIC, b"12.5\x00", id="numeric"),
        pytest.param(CUBRIDDataType.JSON, b'{"a": 1}\x00', id="json"),
    ],
)
def test_truncated_text_cell_is_malformed(column_type: int, payload: bytes) -> None:
    body = _fetch_body([[_cell(payload, declared=len(payload) + 16)]])
    with pytest.raises(ValueError, match="past the end of the broker reply"):
        _fetch([column_type], body)


def test_truncated_unknown_type_cell_is_malformed() -> None:
    # A type without a decoder is returned as raw bytes; it is bounded too.
    body = _fetch_body([[_cell(b"\x01\x02", declared=9)]])
    with pytest.raises(ValueError, match="past the end of the broker reply"):
        _fetch([250], body)


def test_trailing_bytes_after_last_row_are_ignored() -> None:
    # Deliberate: a FETCH reply is not required to end at its last row, so
    # bytes after it are not a framing error (only reads past the end are).
    body = _fetch_body([[_cell(struct.pack(">i", 42))]]) + b"JUNKJUNK"
    assert _fetch([CUBRIDDataType.INT], body).rows == [(42,)]


# --- collections ------------------------------------------------------------


def _collection(element_type: int, elements: list[bytes]) -> bytes:
    payload = struct.pack(">Bi", element_type, len(elements))
    for element in elements:
        payload += struct.pack(">i", len(element)) + element
    return payload


def test_collection_element_past_end_of_reply_is_malformed() -> None:
    payload = struct.pack(">Bi", CUBRIDDataType.BIT, 1) + struct.pack(">i", 16) + b"\xab"
    reader = PacketReader(payload, decode_collections=True)
    with pytest.raises(ValueError, match="past the end of the broker reply"):
        _read_value(reader, CUBRIDDataType.SEQUENCE, len(payload))


def test_collection_element_overrunning_into_next_cell_is_malformed() -> None:
    # The elements take more bytes than the collection declares, eating into
    # the next cell, although the reply itself is long enough.
    collection = _collection(CUBRIDDataType.BIT, [b"\x01\x02\x03\x04"])
    body = _fetch_body([[_cell(collection, declared=len(collection) - 2), _cell(b"\x00" * 4)]])
    with pytest.raises(ValueError, match="collection"):
        _fetch([CUBRIDDataType.SEQUENCE, CUBRIDDataType.BIT], body, decode_collections=True)


def test_collection_shorter_than_declared_size_is_malformed() -> None:
    # Elements end before the declared size: the next cell would be misread.
    collection = _collection(CUBRIDDataType.INT, [struct.pack(">i", 1)])
    body = _fetch_body([[_cell(collection + b"\x00\x00", declared=len(collection) + 2)]])
    with pytest.raises(ValueError, match="collection"):
        _fetch([CUBRIDDataType.SEQUENCE], body, decode_collections=True)


def test_raw_collection_past_end_of_reply_is_malformed() -> None:
    collection = _collection(CUBRIDDataType.INT, [struct.pack(">i", 1)])
    body = _fetch_body([[_cell(collection, declared=len(collection) + 4)]])
    with pytest.raises(ValueError, match="past the end of the broker reply"):
        _fetch([CUBRIDDataType.SET], body)


def test_valid_collections_are_unchanged() -> None:
    seq = _collection(CUBRIDDataType.BIT, [b"\x01", b"", b"\x02\x03"])
    ints = _collection(CUBRIDDataType.INT, [struct.pack(">i", 7), struct.pack(">i", 8)])
    body = _fetch_body([[_cell(seq), _cell(ints)]])
    packet = _fetch(
        [CUBRIDDataType.SEQUENCE, CUBRIDDataType.MULTISET], body, decode_collections=True
    )
    assert packet.rows == [([b"\x01", None, b"\x02\x03"], [7, 8])]
    raw = _fetch([CUBRIDDataType.SEQUENCE], _fetch_body([[_cell(seq)]]))
    assert raw.rows == [(seq,)]


# --- LOB --------------------------------------------------------------------


def _lob_read_body(declared: int, payload: bytes) -> bytes:
    return CAS_INFO + struct.pack(">i", declared) + payload


def test_lob_read_declaring_more_than_payload_is_malformed() -> None:
    packet = LOBReadPacket(b"fixture-handle", offset=0, length=10)
    with pytest.raises(ValueError, match="past the end of the broker reply"):
        packet.parse(_lob_read_body(10, b"abc"))
    assert packet.bytes_read == 0
    assert packet.lob_data == b""


def test_lob_short_read_matching_its_payload_is_valid() -> None:
    # The broker may return fewer bytes than requested (#362): declaring 3 and
    # sending 3 is a complete reply.
    packet = LOBReadPacket(b"fixture-handle", offset=0, length=10)
    packet.parse(_lob_read_body(3, b"abc"))
    assert packet.bytes_read == 3
    assert packet.lob_data == b"abc"


def _lob_handle(locator: bytes, locator_size: int | None = None) -> bytes:
    size = len(locator) if locator_size is None else locator_size
    return struct.pack(">iqi", CUBRIDDataType.BLOB, 5, size) + locator


def test_lob_handle_with_locator_past_its_end_is_malformed() -> None:
    handle = _lob_handle(b"file:/x\x00", locator_size=64)
    body = _fetch_body([[_cell(handle)]])
    with pytest.raises(ValueError, match="past the end of the broker reply"):
        _fetch([CUBRIDDataType.BLOB], body)


def test_lob_handle_past_end_of_reply_is_malformed() -> None:
    handle = _lob_handle(b"file:/x\x00")
    body = _fetch_body([[_cell(handle, declared=len(handle) + 8)]])
    with pytest.raises(ValueError, match="past the end of the broker reply"):
        _fetch([CUBRIDDataType.BLOB], body)


def test_valid_lob_handle_is_unchanged() -> None:
    handle = _lob_handle(b"file:/x\x00")
    (row,) = _fetch([CUBRIDDataType.CLOB], _fetch_body([[_cell(handle)]])).rows
    assert row[0]["lob_length"] == 5
    assert row[0]["file_locator"] == "file:/x"
    assert row[0]["packed_lob_handle"] == handle


# --- connection boundary (sync and async) -----------------------------------


def _truncated_bit_fetch() -> tuple[FetchPacket, bytes]:
    packet = FetchPacket(
        1,
        0,
        columns=[ColumnMetaData(column_type=CUBRIDDataType.BIT)],
        statement_type=CUBRIDStatementType.SELECT,
    )
    return packet, _fetch_body([[_cell(b"\xab\xcd", declared=8)]])


def _truncated_lob_read() -> tuple[LOBReadPacket, bytes]:
    return LOBReadPacket(b"fixture-handle", offset=0, length=10), _lob_read_body(10, b"abc")


MALFORMED_REPLIES = [
    pytest.param(_truncated_bit_fetch, id="bit-cell"),
    pytest.param(_truncated_lob_read, id="lob-read"),
]


@pytest.mark.parametrize("make", MALFORMED_REPLIES)
def test_sync_read_past_end_closes_connection(
    make: Callable[[], tuple[Any, bytes]],
    socket_queue: list[MagicMock],  # noqa: F811
) -> None:
    packet, body = make()
    conn, sock = _connection_with_reply(socket_queue, body)
    with pytest.raises(OperationalError, match="malformed response from broker") as raised:
        conn._send_and_receive(packet)
    assert isinstance(raised.value.__cause__, ValueError)
    assert conn._connected is False
    sock.close.assert_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("make", MALFORMED_REPLIES)
async def test_async_read_past_end_closes_connection(make: Callable[[], tuple[Any, bytes]]) -> None:
    packet, body = make()
    conn = _async_connection_with_reply(body)
    assert isinstance(conn, AsyncConnection)
    writer = conn._writer
    assert isinstance(writer, MagicMock)
    with pytest.raises(OperationalError, match="malformed response from broker") as raised:
        await conn._send_and_receive(packet)
    assert isinstance(raised.value.__cause__, ValueError)
    assert conn._connected is False
    writer.close.assert_called()


def test_sync_valid_short_lob_read_keeps_connection(
    socket_queue: list[MagicMock],  # noqa: F811
) -> None:
    conn, _ = _connection_with_reply(socket_queue, _lob_read_body(3, b"abc"))
    packet = LOBReadPacket(b"fixture-handle", offset=0, length=10)
    conn._send_and_receive(packet)
    assert packet.lob_data == b"abc"
    assert conn._connected is True


@pytest.mark.asyncio
async def test_async_valid_short_lob_read_keeps_connection() -> None:
    conn = _async_connection_with_reply(_lob_read_body(3, b"abc"))
    packet = LOBReadPacket(b"fixture-handle", offset=0, length=10)
    await conn._send_and_receive(packet)
    assert packet.lob_data == b"abc"
    assert conn._connected is True


def test_collection_of_unknown_element_type_past_end_of_reply_is_malformed() -> None:
    payload = struct.pack(">Bi", 250, 1) + struct.pack(">i", 1) + b"\x00"
    reader = PacketReader(payload, decode_collections=True)
    with pytest.raises(ValueError, match="past the end of the broker reply"):
        _read_value(reader, CUBRIDDataType.SET, len(payload) + 4)
    assert reader._offset == 0


def test_unrepresentable_element_in_underfilled_collection_is_malformed() -> None:
    # A DataError element (#512) is reported only when the collection is
    # otherwise exact; spare bytes inside its declared size are framing damage.
    zero_date = struct.pack(">3h", 0, 0, 0)
    collection = _collection(CUBRIDDataType.DATE, [zero_date]) + b"\x00\x00"
    reader = PacketReader(collection, decode_collections=True)
    with pytest.raises(ValueError, match="do not match its size") as raised:
        _read_value(reader, CUBRIDDataType.SEQUENCE, len(collection))
    assert not isinstance(raised.value, DataError)
    body = _fetch_body([[_cell(collection)]])
    with pytest.raises(ValueError, match="do not match its size"):
        _fetch([CUBRIDDataType.SEQUENCE], body, decode_collections=True)


def _underfilled_zero_date_collection_fetch() -> tuple[FetchPacket, bytes]:
    collection = _collection(CUBRIDDataType.DATE, [struct.pack(">3h", 0, 0, 0)]) + b"\x00\x00"
    packet = FetchPacket(
        1,
        0,
        columns=[ColumnMetaData(column_type=CUBRIDDataType.SEQUENCE)],
        statement_type=CUBRIDStatementType.SELECT,
        decode_collections=True,
    )
    return packet, _fetch_body([[_cell(collection)]])


def test_sync_unrepresentable_element_in_underfilled_collection_closes_connection(
    socket_queue: list[MagicMock],  # noqa: F811
) -> None:
    packet, body = _underfilled_zero_date_collection_fetch()
    conn, _ = _connection_with_reply(socket_queue, body)
    with pytest.raises(OperationalError, match="malformed response from broker") as raised:
        conn._send_and_receive(packet)
    assert not isinstance(raised.value.__cause__, DataError)
    assert conn._connected is False


@pytest.mark.asyncio
async def test_async_unrepresentable_element_in_underfilled_collection_closes_connection() -> None:
    packet, body = _underfilled_zero_date_collection_fetch()
    conn = _async_connection_with_reply(body)
    with pytest.raises(OperationalError, match="malformed response from broker") as raised:
        await conn._send_and_receive(packet)
    assert not isinstance(raised.value.__cause__, DataError)
    assert conn._connected is False
