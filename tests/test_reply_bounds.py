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

import datetime
import struct
from collections.abc import Callable
from typing import Any
from unittest.mock import MagicMock

import pytest

from pycubrid.aio.connection import AsyncConnection
from pycubrid.constants import CUBRIDDataType, CUBRIDStatementType
from pycubrid.exceptions import DataError, OperationalError
from pycubrid.packet import PacketReader
from pycubrid.protocol import (
    ColumnMetaData,
    ExecutePacket,
    FetchPacket,
    LOBReadPacket,
    PrepareAndExecutePacket,
    PreparePacket,
    _read_value,
)
from tests.helpers import cas_reply
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


_INT_42 = struct.pack(">i", 42)
_DATE = struct.pack(">3h", 2026, 9, 30)
_TSTZ = struct.pack(">6h", 2026, 9, 30, 1, 2, 3) + b"+09:00\x00"
_OID = struct.pack(">ihh", 100, 2, 0)


@pytest.mark.parametrize(
    ("column_type", "payload", "declared"),
    [
        # Found by the FETCH fuzz target (#523): fixed-width readers ignored
        # the cell's size word, so a size past the end of the reply returned
        # the value as if the reply were complete.
        pytest.param(CUBRIDDataType.INT, _INT_42, 1000, id="int-overrunning-reply"),
        pytest.param(CUBRIDDataType.SHORT, b"\x00\x07", 3, id="short-overrunning-reply"),
        pytest.param(CUBRIDDataType.DOUBLE, b"\x00" * 8, 9, id="double-overrunning-reply"),
        pytest.param(CUBRIDDataType.DATE, _DATE, 7, id="date-overrunning-reply"),
        pytest.param(CUBRIDDataType.OBJECT, _OID, 12, id="oid-overrunning-reply"),
        # ...and a size shorter than the type read the following bytes as its own.
        pytest.param(CUBRIDDataType.INT, _INT_42, 2, id="int-shorter-than-value"),
        pytest.param(CUBRIDDataType.DATE, _DATE, 4, id="date-shorter-than-value"),
        pytest.param(CUBRIDDataType.TIMESTAMPTZ, _TSTZ, 5, id="tstz-shorter-than-value"),
    ],
)
def test_fixed_width_cell_whose_size_disagrees_is_malformed(
    column_type: int, payload: bytes, declared: int
) -> None:
    body = _fetch_body([[_cell(payload, declared=declared)]])
    with pytest.raises(ValueError, match="cell size"):
        _fetch([column_type], body)


def test_fixed_width_cell_size_mismatch_in_call_result_is_malformed() -> None:
    # CALL results carry a type byte per cell; the size counts it.
    cell = struct.pack(">iB", 1 + 2, CUBRIDDataType.INT) + _INT_42
    packet = FetchPacket(
        1,
        0,
        columns=[ColumnMetaData(column_type=CUBRIDDataType.NULL)],
        statement_type=CUBRIDStatementType.CALL,
    )
    with pytest.raises(ValueError, match="cell size"):
        packet.parse(_fetch_body([[cell]]))


def _call_fetch(body: bytes, column_count: int = 1) -> FetchPacket:
    packet = FetchPacket(
        1,
        0,
        columns=[ColumnMetaData(column_type=CUBRIDDataType.NULL)] * column_count,
        statement_type=CUBRIDStatementType.CALL,
    )
    packet.parse(body)
    return packet


@pytest.mark.parametrize(
    ("declared", "tail"),
    [
        # The size counts the one header byte it holds; the type byte is outside.
        pytest.param(1, _INT_42, id="size-covers-only-first-header-byte"),
        # The reply ends after the first header byte.
        pytest.param(1, b"", id="reply-ends-inside-header"),
    ],
)
def test_two_byte_cell_header_longer_than_its_cell_is_malformed(declared: int, tail: bytes) -> None:
    # Protocol 8 CALL cells start with 0x80|charset, type (#542). A cell too
    # short for that header must not borrow the next bytes as its type.
    body = _fetch_body([[struct.pack(">iB", declared, 0x83) + tail]])
    with pytest.raises((ValueError, IndexError)):
        _call_fetch(body)


def test_two_byte_cell_header_int_size_mismatch_is_malformed() -> None:
    # Two header bytes + a 3-byte INT: the value width is checked after the header.
    cell = struct.pack(">iBB", 2 + 3, 0x83, CUBRIDDataType.INT) + _INT_42[:3]
    with pytest.raises(ValueError, match="cell size"):
        _call_fetch(_fetch_body([[cell]]))


def test_two_byte_cell_header_size_mismatch_after_unrepresentable_value_is_malformed() -> None:
    # The re-walk before DataError (#512) reads the same two-byte header, so a
    # later CALL cell whose INT does not fill its size is still framing damage.
    zero_date = struct.pack(">BB3h", 0x83, CUBRIDDataType.DATE, 0, 0, 0)
    short_int = struct.pack(">BB", 0x83, CUBRIDDataType.INT) + _INT_42[:3]
    body = _fetch_body([[_cell(zero_date), _cell(short_int)]])
    with pytest.raises(ValueError, match="cell size"):
        _call_fetch(body, column_count=2)


def test_two_byte_cell_header_unrepresentable_value_in_complete_reply_is_data_error() -> None:
    # The re-walk also passes SQL NULL cells and a cell holding only its header.
    zero_date = struct.pack(">BB3h", 0x83, CUBRIDDataType.DATE, 0, 0, 0)
    int_42 = struct.pack(">BB", 0x83, CUBRIDDataType.INT) + _INT_42
    header_only = struct.pack(">BB", 0x83, CUBRIDDataType.INT)
    null = struct.pack(">i", -1)
    body = _fetch_body([[_cell(zero_date), _cell(int_42), _cell(header_only), null]])
    with pytest.raises(DataError):
        _call_fetch(body, column_count=4)


def test_fixed_width_cell_size_mismatch_after_an_unrepresentable_value_is_malformed() -> None:
    # A zero DATE is DataError only for a complete reply (#512). The re-walk
    # that proves completeness must also reject a later INT cell whose size
    # fits the reply but not the value, instead of reporting DataError.
    zero_date = struct.pack(">3h", 0, 0, 0)
    body = _fetch_body([[_cell(zero_date), _cell(_INT_42 + b"\x00", declared=5)]])
    with pytest.raises(ValueError, match="cell size"):
        _fetch([CUBRIDDataType.DATE, CUBRIDDataType.INT], body)


def test_unrepresentable_value_in_a_complete_reply_is_still_data_error() -> None:
    zero_date = struct.pack(">3h", 0, 0, 0)
    body = _fetch_body([[_cell(zero_date), _cell(_INT_42)]])
    with pytest.raises(DataError):
        _fetch([CUBRIDDataType.DATE, CUBRIDDataType.INT], body)


def test_fixed_width_cells_of_their_exact_size_are_unchanged() -> None:
    body = _fetch_body([[_cell(_INT_42), _cell(_DATE), _cell(_TSTZ), _cell(_OID), _cell(b"")]])
    packet = _fetch(
        [
            CUBRIDDataType.INT,
            CUBRIDDataType.DATE,
            CUBRIDDataType.TIMESTAMPTZ,
            CUBRIDDataType.OBJECT,
            CUBRIDDataType.INT,
        ],
        body,
    )
    (row,) = packet.rows
    assert row[0] == 42
    assert row[1] == datetime.date(2026, 9, 30)
    assert row[2] == datetime.datetime(
        2026, 9, 30, 1, 2, 3, tzinfo=datetime.timezone(datetime.timedelta(hours=9))
    )
    assert row[3] == "OID:@100|2|0"
    assert row[4] is None


def test_sync_fixed_width_cell_size_mismatch_closes_connection(
    socket_queue: list[MagicMock],  # noqa: F811
) -> None:
    packet = FetchPacket(1, 0, columns=[ColumnMetaData(column_type=CUBRIDDataType.INT)])
    body = _fetch_body([[_cell(_INT_42, declared=1000)]])
    conn, sock = _connection_with_reply(socket_queue, body)
    with pytest.raises(OperationalError, match="malformed response from broker") as raised:
        conn._send_and_receive(packet)
    assert isinstance(raised.value.__cause__, ValueError)
    assert conn._connected is False
    sock.close.assert_called()


@pytest.mark.parametrize("tuple_count", [-1, -(2**31)])
def test_negative_fetch_tuple_count_is_malformed(tuple_count: int) -> None:
    # A negative count used to parse as an empty page, silently ending the
    # result set early instead of reporting framing damage (#523).
    body = CAS_INFO + struct.pack(">ii", 0, tuple_count)
    with pytest.raises(ValueError, match="negative FETCH tuple count"):
        _fetch([CUBRIDDataType.INT], body)


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


# --- FC41 column metadata (#555) ---------------------------------------------
#
# PREPARE_AND_EXECUTE shares the column-metadata layout with FC2/FC3, but used
# to decode a negative name/real-name/table-name/default length as an empty
# string and a negative column count as "no columns". Both are framing damage.

_FC41_RS = cas_reply.STRINGS
_FC41_FIELDS = ("name", "real_name", "table_name", "default")


def _negative_field(seed: cas_reply.Seed, field: str, value: int) -> bytes:
    """``seed`` with one metadata word set to ``value`` and the reply kept coherent.

    A rewritten text length also drops its payload, so a parser that reads a
    negative length as "empty" stays aligned and decodes the whole reply.
    """
    reply = bytearray(seed.data)
    if field == "column_count":
        (pos,) = seed.column_counts
        struct.pack_into(">i", reply, pos, value)
        return bytes(reply)
    # Second column, so the first column's metadata has already been read.
    pos = seed.metadata_lengths[len(_FC41_FIELDS) + _FC41_FIELDS.index(field)]
    old = struct.unpack_from(">i", reply, pos)[0]
    reply[pos : pos + 4 + old] = struct.pack(">i", value)
    return bytes(reply)


def _fc41_reply(*, field: str | None = None, value: int = -1) -> bytes:
    """A coherent FC41 SELECT reply, optionally with one negative metadata word."""
    seed = cas_reply.prepare_and_execute_reply(_FC41_RS)
    return seed.data if field is None else _negative_field(seed, field, value)


def _fc41_packet() -> PrepareAndExecutePacket:
    return PrepareAndExecutePacket("SELECT * FROM fuzz_t")


FC41_NEGATIVE_FIELDS = [
    pytest.param(field, value, id=f"{field}{value}")
    for field in (*_FC41_FIELDS, "column_count")
    for value in (-1, -(2**31))
]


@pytest.mark.parametrize(("field", "value"), FC41_NEGATIVE_FIELDS)
def test_fc41_negative_metadata_field_is_malformed(field: str, value: int) -> None:
    packet = _fc41_packet()
    with pytest.raises(ValueError, match="negative|invalid prepared column") as raised:
        packet.parse(_fc41_reply(field=field, value=value))
    assert not isinstance(raised.value, DataError)


@pytest.mark.parametrize(("field", "value"), FC41_NEGATIVE_FIELDS)
def test_fc2_negative_metadata_field_is_malformed(field: str, value: int) -> None:
    # The FC2 parity the FC41 path now follows.
    reply = _negative_field(cas_reply.prepare_reply(_FC41_RS), field, value)
    with pytest.raises(ValueError, match="negative|invalid prepared column"):
        PreparePacket("SELECT * FROM fuzz_t").parse(reply)


@pytest.mark.parametrize("field", _FC41_FIELDS)
def test_fc41_zero_length_metadata_is_empty(field: str) -> None:
    # Legal zero-length metadata (no bytes, not even the NUL) stays an empty string.
    packet = _fc41_packet()
    packet.parse(_fc41_reply(field=field, value=0))
    assert getattr(packet.columns[1], field if field != "default" else "default_value") == ""
    assert packet.column_count == len(_FC41_RS.columns)
    assert len(packet.rows) == len(_FC41_RS.rows)


def test_fc41_zero_column_count_without_metadata_is_valid() -> None:
    reply = (
        CAS_INFO
        + struct.pack(">iiBiBi", 7, 0, CUBRIDStatementType.INSERT, 0, 0, 0)
        + struct.pack(">iBi", 1, 0, 1)
        + struct.pack(">Bi", CUBRIDStatementType.INSERT, 1)
        + b"\x00" * 8
        + struct.pack(">iiBi", 0, 0, 0, 0)
    )
    packet = PrepareAndExecutePacket("INSERT INTO t VALUES (1)")
    packet.parse(reply)
    assert packet.column_count == 0
    assert packet.columns == []
    assert packet.result_count == 1


@pytest.mark.parametrize(("field", "value"), FC41_NEGATIVE_FIELDS)
def test_sync_fc41_negative_metadata_field_closes_connection(
    field: str,
    value: int,
    socket_queue: list[MagicMock],  # noqa: F811
) -> None:
    conn, sock = _connection_with_reply(socket_queue, _fc41_reply(field=field, value=value))
    with pytest.raises(OperationalError, match="malformed response from broker") as raised:
        conn._send_and_receive(_fc41_packet())
    assert isinstance(raised.value.__cause__, ValueError)
    assert not isinstance(raised.value.__cause__, DataError)
    assert conn._connected is False
    sock.close.assert_called()


@pytest.mark.asyncio
@pytest.mark.parametrize(("field", "value"), FC41_NEGATIVE_FIELDS)
async def test_async_fc41_negative_metadata_field_closes_connection(field: str, value: int) -> None:
    conn = _async_connection_with_reply(_fc41_reply(field=field, value=value))
    writer = conn._writer
    assert isinstance(writer, MagicMock)
    with pytest.raises(OperationalError, match="malformed response from broker") as raised:
        await conn._send_and_receive(_fc41_packet())
    assert isinstance(raised.value.__cause__, ValueError)
    assert not isinstance(raised.value.__cause__, DataError)
    assert conn._connected is False
    writer.close.assert_called()


def _fc41_invalid_utf8_name() -> bytes:
    """A complete FC41 reply whose second column name is not valid UTF-8."""
    seed = cas_reply.prepare_and_execute_reply(_FC41_RS)
    pos = seed.metadata_lengths[len(_FC41_FIELDS)]
    reply = bytearray(seed.data)
    reply[pos + 4] = 0xFF  # first byte of "c_varchar"
    return bytes(reply)


def test_sync_fc41_valid_and_unrepresentable_metadata_keep_connection(
    socket_queue: list[MagicMock],  # noqa: F811
) -> None:
    conn, _ = _connection_with_reply(socket_queue, _fc41_reply())
    packet = _fc41_packet()
    conn._send_and_receive(packet)
    assert [c.name for c in packet.columns] == [c.name for c in _FC41_RS.columns]
    assert conn._connected is True

    conn, _ = _connection_with_reply(socket_queue, _fc41_invalid_utf8_name())
    with pytest.raises(DataError, match="column metadata is not valid"):
        conn._send_and_receive(_fc41_packet())
    assert conn._connected is True


@pytest.mark.asyncio
async def test_async_fc41_valid_and_unrepresentable_metadata_keep_connection() -> None:
    conn = _async_connection_with_reply(_fc41_reply())
    packet = _fc41_packet()
    await conn._send_and_receive(packet)
    assert [c.name for c in packet.columns] == [c.name for c in _FC41_RS.columns]
    assert conn._connected is True

    conn = _async_connection_with_reply(_fc41_invalid_utf8_name())
    with pytest.raises(DataError, match="column metadata is not valid"):
        await conn._send_and_receive(_fc41_packet())
    assert conn._connected is True


# --- an earlier DataError must not hide later metadata damage (#581) ----------
#
# Column metadata used to raise DataError at the first undecodable name, so the
# remaining metadata was never checked: a reply that was also damaged in a later
# column kept the session as "complete". The metadata is now walked to its end
# by declared lengths before the DataError is re-raised.

METADATA_DAMAGE = ["negative_length", "overrun_length", "truncated"]


def _invalid_first_name_then(seed: cas_reply.Seed, damage: str) -> bytes:
    """Column 0's name is not valid UTF-8 and column 1's metadata is damaged."""
    reply = bytearray(seed.data)
    reply[seed.metadata_lengths[0] + 4] = 0xFF  # first byte of column 0's name
    pos = seed.metadata_lengths[len(_FC41_FIELDS)]  # column 1's name length
    old = struct.unpack_from(">i", reply, pos)[0]
    if damage == "negative_length":
        reply[pos : pos + 4 + old] = struct.pack(">i", -1)
    elif damage == "overrun_length":
        struct.pack_into(">i", reply, pos, len(reply))
    else:
        del reply[pos + 2 :]
    return bytes(reply)


def _fc41_invalid_first_name_then(damage: str) -> bytes:
    return _invalid_first_name_then(cas_reply.prepare_and_execute_reply(_FC41_RS), damage)


def test_fc41_invalid_first_name_alone_is_data_error() -> None:
    seed = cas_reply.prepare_and_execute_reply(_FC41_RS)
    reply = bytearray(seed.data)
    reply[seed.metadata_lengths[0] + 4] = 0xFF
    with pytest.raises(DataError, match="column metadata is not valid"):
        _fc41_packet().parse(bytes(reply))


@pytest.mark.parametrize("damage", METADATA_DAMAGE)
def test_fc41_data_error_does_not_hide_later_metadata_damage(damage: str) -> None:
    with pytest.raises(ValueError) as raised:
        _fc41_packet().parse(_fc41_invalid_first_name_then(damage))
    assert not isinstance(raised.value, DataError)


@pytest.mark.parametrize("damage", METADATA_DAMAGE)
def test_fc2_data_error_does_not_hide_later_metadata_damage(damage: str) -> None:
    reply = _invalid_first_name_then(cas_reply.prepare_reply(_FC41_RS), damage)
    with pytest.raises(ValueError) as raised:
        PreparePacket("SELECT * FROM fuzz_t").parse(reply)
    assert not isinstance(raised.value, DataError)


@pytest.mark.parametrize("damage", METADATA_DAMAGE)
def test_fc3_refreshed_data_error_does_not_hide_later_metadata_damage(damage: str) -> None:
    seed = cas_reply.execute_reply(_FC41_RS, refresh_columns=True)
    packet = ExecutePacket(query_handle=5, statement_type=CUBRIDStatementType.SELECT)
    with pytest.raises(ValueError) as raised:
        packet.parse(_invalid_first_name_then(seed, damage), columns=None)
    assert not isinstance(raised.value, DataError)


@pytest.mark.parametrize("damage", METADATA_DAMAGE)
def test_sync_fc41_data_error_with_later_damage_closes_connection(
    damage: str,
    socket_queue: list[MagicMock],  # noqa: F811
) -> None:
    conn, sock = _connection_with_reply(socket_queue, _fc41_invalid_first_name_then(damage))
    with pytest.raises(OperationalError, match="malformed response from broker") as raised:
        conn._send_and_receive(_fc41_packet())
    assert isinstance(raised.value.__cause__, ValueError)
    assert not isinstance(raised.value.__cause__, DataError)
    assert conn._connected is False
    sock.close.assert_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", METADATA_DAMAGE)
async def test_async_fc41_data_error_with_later_damage_closes_connection(damage: str) -> None:
    conn = _async_connection_with_reply(_fc41_invalid_first_name_then(damage))
    with pytest.raises(OperationalError, match="malformed response from broker") as raised:
        await conn._send_and_receive(_fc41_packet())
    assert isinstance(raised.value.__cause__, ValueError)
    assert not isinstance(raised.value.__cause__, DataError)
    assert conn._connected is False


# --- FC41 counts follow FC2 and FETCH (#581) -----------------------------------
#
# FC41 accepted a negative bind count (FC2 rejects it), read a negative inline
# tuple count as zero rows, passed a negative total_tuple_count through, and had
# no upper bound on the column count. All are framing damage.

_FC41_COUNTS = ("bind_count", "total_tuple_count", "tuple_count")


def _fc41_count_offset(seed: cas_reply.Seed, field: str) -> int:
    if field == "bind_count":
        pos = 13  # CAS_INFO, handle, cache lifetime, statement type
        expected = 0
    elif field == "total_tuple_count":
        last = seed.metadata_lengths[-1]  # the last column's default length
        pos = last + 4 + struct.unpack_from(">i", seed.data, last)[0] + 7  # + 7 flag bytes
        expected = len(_FC41_RS.rows)
    else:
        pos = seed.counts[-1]
        expected = len(_FC41_RS.rows)
    assert struct.unpack_from(">i", seed.data, pos)[0] == expected, field
    return pos


def _fc41_negative_count(field: str, value: int) -> bytes:
    seed = cas_reply.prepare_and_execute_reply(_FC41_RS)
    reply = bytearray(seed.data)
    struct.pack_into(">i", reply, _fc41_count_offset(seed, field), value)
    return bytes(reply)


FC41_NEGATIVE_COUNTS = [
    pytest.param(field, value, id=f"{field}{value}")
    for field in _FC41_COUNTS
    for value in (-1, -(2**31))
]


@pytest.mark.parametrize(("field", "value"), FC41_NEGATIVE_COUNTS)
def test_fc41_negative_count_is_malformed(field: str, value: int) -> None:
    with pytest.raises(ValueError, match="negative") as raised:
        _fc41_packet().parse(_fc41_negative_count(field, value))
    assert not isinstance(raised.value, DataError)


@pytest.mark.parametrize(("field", "value"), FC41_NEGATIVE_COUNTS)
def test_sync_fc41_negative_count_closes_connection(
    field: str,
    value: int,
    socket_queue: list[MagicMock],  # noqa: F811
) -> None:
    conn, sock = _connection_with_reply(socket_queue, _fc41_negative_count(field, value))
    with pytest.raises(OperationalError, match="malformed response from broker") as raised:
        conn._send_and_receive(_fc41_packet())
    assert isinstance(raised.value.__cause__, ValueError)
    assert conn._connected is False
    sock.close.assert_called()


@pytest.mark.asyncio
@pytest.mark.parametrize(("field", "value"), FC41_NEGATIVE_COUNTS)
async def test_async_fc41_negative_count_closes_connection(field: str, value: int) -> None:
    conn = _async_connection_with_reply(_fc41_negative_count(field, value))
    with pytest.raises(OperationalError, match="malformed response from broker") as raised:
        await conn._send_and_receive(_fc41_packet())
    assert isinstance(raised.value.__cause__, ValueError)
    assert conn._connected is False


def test_fc41_zero_counts_are_valid() -> None:
    packet = _fc41_packet()
    packet.parse(_fc41_negative_count("bind_count", 0))
    assert packet.bind_count == 0
    assert len(packet.rows) == len(_FC41_RS.rows)


def test_fc3_negative_inline_tuple_count_is_malformed() -> None:
    seed = cas_reply.execute_reply(_FC41_RS, refresh_columns=True)
    reply = bytearray(seed.data)
    pos = seed.counts[-1]
    assert struct.unpack_from(">i", reply, pos)[0] == len(_FC41_RS.rows)
    struct.pack_into(">i", reply, pos, -1)
    packet = ExecutePacket(query_handle=5, statement_type=CUBRIDStatementType.SELECT)
    with pytest.raises(ValueError, match="negative"):
        packet.parse(bytes(reply), columns=None)


@pytest.mark.parametrize("excess", [1, 2**31 - 1])
def test_fc41_column_count_beyond_the_reply_is_rejected_before_parsing(excess: int) -> None:
    # Every column entry takes at least 31 bytes; FC2 already rejects a count the
    # rest of the reply cannot hold before reading any column.
    seed = cas_reply.prepare_and_execute_reply(_FC41_RS)
    (pos,) = seed.column_counts
    reply = bytearray(seed.data)
    remaining = len(reply) - (pos + 4)
    struct.pack_into(">i", reply, pos, min(remaining // 31 + excess, 2**31 - 1))
    with pytest.raises(ValueError, match="truncated prepared column metadata") as raised:
        _fc41_packet().parse(bytes(reply))
    assert not isinstance(raised.value, DataError)
