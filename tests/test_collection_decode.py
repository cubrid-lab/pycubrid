from __future__ import annotations

import struct
from unittest.mock import MagicMock

import pycubrid
import pytest

from pycubrid.constants import CUBRIDDataType, CUBRIDStatementType, DataSize
from pycubrid.cursor import Cursor
from pycubrid.packet import PacketReader
from pycubrid.protocol import (
    ColumnMetaData,
    FetchPacket,
    PrepareAndExecutePacket,
    _parse_column_metadata,
    _read_value,
)


DEFAULT_CAS_INFO = b"\x00\x01\x02\x03"


def _encode_collection(element_type: int, elements: list[bytes | None]) -> bytes:
    payload = bytearray()
    payload.append(element_type)
    payload.extend(struct.pack(">i", len(elements)))
    for element in elements:
        if element is None:
            payload.extend(struct.pack(">i", 0))
        else:
            payload.extend(struct.pack(">i", len(element)))
            payload.extend(element)
    return bytes(payload)


def _encode_int(value: int) -> bytes:
    return struct.pack(">i", value)


def _encode_string(value: str) -> bytes:
    return value.encode("utf-8") + b"\x00"


def _build_column_metadata(column_type: int, name: str) -> bytes:
    encoded_name = name.encode("utf-8") + b"\x00"
    buf = bytearray()
    if column_type > 0xFF:
        buf.extend((0x80 | (column_type >> 8), column_type & 0xFF))
    else:
        buf.append(column_type)
    buf.extend(struct.pack(">h", 0))
    buf.extend(struct.pack(">i", 0))
    for _ in range(3):
        buf.extend(struct.pack(">i", len(encoded_name)))
        buf.extend(encoded_name)
    buf.append(0)
    buf.extend(struct.pack(">i", 0))
    buf.extend(b"\x00" * 7)
    return bytes(buf)


def _build_row(values: list[bytes | None]) -> bytes:
    row = bytearray()
    row.extend(struct.pack(">i", 0))
    row.extend(b"\x00" * DataSize.OID)
    for value in values:
        if value is None:
            row.extend(struct.pack(">i", 0))
        else:
            row.extend(struct.pack(">i", len(value)))
            row.extend(value)
    return bytes(row)


def _build_result_info(stmt_type: int, result_count: int) -> bytes:
    return (
        bytes([stmt_type])
        + struct.pack(">i", result_count)
        + (b"\x00" * DataSize.OID)
        + struct.pack(">i", 0)
        + struct.pack(">i", 0)
    )


def _build_select_response(columns: list[tuple[int, str]], row_values: list[bytes | None]) -> bytes:
    response = bytearray()
    response.extend(DEFAULT_CAS_INFO)
    response.extend(struct.pack(">i", 1))
    response.extend(struct.pack(">i", 0))
    response.append(CUBRIDStatementType.SELECT)
    response.extend(struct.pack(">i", 0))
    response.append(0)
    response.extend(struct.pack(">i", len(columns)))
    for column_type, name in columns:
        response.extend(_build_column_metadata(column_type, name))
    response.extend(struct.pack(">i", 1))
    response.append(0)
    response.extend(struct.pack(">i", 1))
    response.extend(_build_result_info(CUBRIDStatementType.SELECT, 1))
    response.append(0)
    response.extend(struct.pack(">i", 0))
    response.extend(struct.pack(">i", 0))
    response.extend(struct.pack(">i", 1))
    response.extend(_build_row(row_values))
    return bytes(response)


def test_collection_decoding_opt_out_returns_raw_bytes() -> None:
    payload = _encode_collection(CUBRIDDataType.INT, [_encode_int(1), _encode_int(2)])

    for collection_type in (
        CUBRIDDataType.SET,
        CUBRIDDataType.MULTISET,
        CUBRIDDataType.SEQUENCE,
    ):
        reader = PacketReader(payload)
        assert _read_value(reader, collection_type, len(payload)) == payload


def test_collection_decoding_supports_ints_strings_nulls_and_empty() -> None:
    set_payload = _encode_collection(CUBRIDDataType.INT, [_encode_int(1), _encode_int(2)])
    multiset_payload = _encode_collection(CUBRIDDataType.STRING, [_encode_string("a"), None])
    sequence_payload = _encode_collection(CUBRIDDataType.INT, [])

    assert _read_value(
        PacketReader(set_payload, decode_collections=True),
        CUBRIDDataType.SET,
        len(set_payload),
    ) == frozenset({1, 2})
    assert _read_value(
        PacketReader(multiset_payload, decode_collections=True),
        CUBRIDDataType.MULTISET,
        len(multiset_payload),
    ) == ["a", None]
    assert (
        _read_value(
            PacketReader(sequence_payload, decode_collections=True),
            CUBRIDDataType.SEQUENCE,
            len(sequence_payload),
        )
        == []
    )


def test_nested_collections_remain_raw_bytes() -> None:
    nested_payload = _encode_collection(
        CUBRIDDataType.SET,
        [_encode_collection(CUBRIDDataType.INT, [_encode_int(1)])],
    )

    reader = PacketReader(nested_payload, decode_collections=True)
    assert _read_value(reader, CUBRIDDataType.SEQUENCE, len(nested_payload)) == nested_payload


@pytest.mark.parametrize(
    "column_type,expected",
    [
        (CUBRIDDataType.SET, frozenset()),
        (CUBRIDDataType.MULTISET, []),
        (CUBRIDDataType.SEQUENCE, []),
    ],
)
def test_empty_collection_with_null_element_type(column_type: int, expected: object) -> None:
    # A real broker sends type NULL + count zero for an empty collection.
    payload = _encode_collection(CUBRIDDataType.NULL, [])
    assert _read_value(PacketReader(payload), column_type, len(payload)) == payload
    reader = PacketReader(payload, decode_collections=True)
    assert _read_value(reader, column_type, len(payload)) == expected
    assert reader.bytes_remaining() == 0


def test_prepare_and_execute_packet_decodes_collection_rows_when_enabled() -> None:
    set_payload = _encode_collection(CUBRIDDataType.INT, [_encode_int(1), _encode_int(2)])
    multiset_payload = _encode_collection(CUBRIDDataType.STRING, [_encode_string("x"), None])
    sequence_payload = _encode_collection(CUBRIDDataType.INT, [_encode_int(9), _encode_int(8)])
    response = _build_select_response(
        [
            (CUBRIDDataType.SET, "set_col"),
            (CUBRIDDataType.MULTISET, "multiset_col"),
            (CUBRIDDataType.SEQUENCE, "sequence_col"),
        ],
        [set_payload, multiset_payload, sequence_payload],
    )

    packet = PrepareAndExecutePacket(
        "SELECT collections", protocol_version=7, decode_collections=True
    )
    packet.parse(response)

    assert packet.rows == [(frozenset({1, 2}), ["x", None], [9, 8])]


@pytest.mark.parametrize("extended", [False, True])
@pytest.mark.parametrize("decode", [False, True])
@pytest.mark.parametrize(
    "kind,column_type,values,expected",
    [
        (0x20, CUBRIDDataType.SET, [1, 2, 1], frozenset({1, 2})),
        (0x40, CUBRIDDataType.MULTISET, [1, 1, 2], [1, 1, 2]),
        (0x60, CUBRIDDataType.SEQUENCE, [9, 8, 9], [9, 8, 9]),
    ],
)
def test_collection_kind_in_wire_metadata(
    extended: bool,
    decode: bool,
    kind: int,
    column_type: int,
    values: list[int],
    expected: object,
) -> None:
    # Legacy headers pack the element type into the low five bits. Modern
    # headers carry it in a second byte; both retain the collection-kind bits.
    wire_type = ((kind | 3) << 8 if extended else kind) | CUBRIDDataType.INT
    payload = _encode_collection(CUBRIDDataType.INT, [_encode_int(v) for v in values])
    response = _build_select_response(
        [(wire_type, "items"), (CUBRIDDataType.INT, "scalar")],
        [payload, _encode_int(42)],
    )
    packet = PrepareAndExecutePacket(
        "SELECT items, scalar", protocol_version=7, decode_collections=decode
    )
    packet.parse(response)
    assert packet.columns[0].column_type == column_type
    assert packet.columns[1].column_type == CUBRIDDataType.INT
    assert packet.rows == [(expected if decode else payload, 42)]
    fetch = FetchPacket(packet.query_handle, 1, decode_collections=decode)
    fetch.parse(
        DEFAULT_CAS_INFO + struct.pack(">ii", 0, 1) + _build_row([payload, _encode_int(43)]),
        packet.columns,
        CUBRIDStatementType.SELECT,
    )
    assert fetch.rows == [(expected if decode else payload, 43)]


@pytest.mark.parametrize("column_type", [CUBRIDDataType.DATETIMELTZ, CUBRIDDataType.JSON])
def test_extended_scalar_type_is_not_masked(column_type: int) -> None:
    reader = PacketReader(_build_column_metadata(0x0300 | column_type, "scalar"))
    assert _parse_column_metadata(reader, 1)[0].column_type == column_type
    assert reader.bytes_remaining() == 0


def test_connect_passes_decode_collections_flag(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, bool] = {}

    class DummyConnection:
        def __init__(self, *args: object, **kwargs: object) -> None:
            captured["decode_collections"] = bool(kwargs["decode_collections"])

    monkeypatch.setattr("pycubrid.connection.Connection", DummyConnection)

    _ = pycubrid.connect(database="testdb", decode_collections=True)

    assert captured["decode_collections"] is True


def test_cursor_threads_decode_collections_to_packets() -> None:
    connection = MagicMock()
    connection.autocommit = False
    connection._connected = True
    connection._decode_collections = True
    connection._cursors = set()
    connection._ensure_connected = MagicMock()

    def send_and_receive(packet: object) -> object:
        if isinstance(packet, PrepareAndExecutePacket):
            assert packet.decode_collections is True
            packet.query_handle = 1
            packet.statement_type = CUBRIDStatementType.SELECT
            packet.columns = [ColumnMetaData(name="items", column_type=CUBRIDDataType.SEQUENCE)]
            packet.total_tuple_count = 1
            packet.rows = []
            packet.result_infos = []
        elif isinstance(packet, FetchPacket):
            assert packet.decode_collections is True
            packet.rows = [([1, 2],)]
        return packet

    connection._send_and_receive.side_effect = send_and_receive

    cursor = Cursor(connection)
    _ = cursor.execute("SELECT items FROM t")

    assert cursor.fetchone() == ([1, 2],)


# Exact collection payloads captured from CUBRID 10.2 and 11.4 (#483). A
# collection whose elements are all SQL NULL carries element type NULL (0),
# the element count, and one ``-1`` length word per element with no payload.
_LIVE_NULL_ONE = bytes.fromhex("0000000001ffffffff")
_LIVE_NULL_TWO = bytes.fromhex("0000000002ffffffffffffffff")
_LIVE_EMPTY = bytes.fromhex("0000000000")
_LIVE_INT_MIXED = bytes.fromhex("0800000003ffffffff0000000400000002ffffffff")
_LIVE_STRING_MIXED = bytes.fromhex("0200000002000000026100ffffffff")


@pytest.mark.parametrize(
    "column_type,payload,expected",
    [
        (CUBRIDDataType.SET, _LIVE_NULL_ONE, frozenset({None})),
        (CUBRIDDataType.MULTISET, _LIVE_NULL_ONE, [None]),
        (CUBRIDDataType.SEQUENCE, _LIVE_NULL_ONE, [None]),
        (CUBRIDDataType.SET, _LIVE_NULL_TWO, frozenset({None})),
        (CUBRIDDataType.MULTISET, _LIVE_NULL_TWO, [None, None]),
        (CUBRIDDataType.SEQUENCE, _LIVE_NULL_TWO, [None, None]),
        (CUBRIDDataType.SET, _LIVE_EMPTY, frozenset()),
        (CUBRIDDataType.SEQUENCE, _LIVE_EMPTY, []),
        # Zero-length NULL markers, as older encoders and the helper above emit.
        (CUBRIDDataType.SEQUENCE, _encode_collection(CUBRIDDataType.NULL, [None]), [None]),
        (CUBRIDDataType.SEQUENCE, _LIVE_INT_MIXED, [None, 2, None]),
        (CUBRIDDataType.SEQUENCE, _LIVE_STRING_MIXED, ["a", None]),
    ],
)
def test_live_null_only_collection_payloads(
    column_type: int, payload: bytes, expected: object
) -> None:
    reader = PacketReader(payload, decode_collections=True)
    assert _read_value(reader, column_type, len(payload)) == expected
    assert reader.bytes_remaining() == 0
    raw_reader = PacketReader(payload)
    assert _read_value(raw_reader, column_type, len(payload)) == payload
    assert raw_reader.bytes_remaining() == 0


def test_null_only_collection_row_decodes_through_packets() -> None:
    response = _build_select_response(
        [
            (CUBRIDDataType.SET, "s"),
            (CUBRIDDataType.MULTISET, "m"),
            (CUBRIDDataType.SEQUENCE, "q"),
            (CUBRIDDataType.INT, "n"),
        ],
        [_LIVE_NULL_TWO, _LIVE_NULL_TWO, _LIVE_NULL_ONE, _encode_int(7)],
    )
    packet = PrepareAndExecutePacket("SELECT", protocol_version=7, decode_collections=True)
    packet.parse(response)
    assert packet.rows == [(frozenset({None}), [None, None], [None], 7)]


@pytest.mark.parametrize(
    "payload",
    [
        # Negative element count.
        bytes.fromhex("00ffffffff"),
        # Count larger than the length words present.
        bytes.fromhex("0000000002ffffffff"),
        # Huge count must be rejected before any allocation.
        bytes.fromhex("007fffffffffffffff"),
        # Trailing bytes after the length words.
        bytes.fromhex("0000000001ffffffff00"),
        # A NULL element type cannot carry an element payload or a bogus length.
        bytes.fromhex("000000000100000004"),
        bytes.fromhex("0000000001fffffffe"),
    ],
)
def test_malformed_null_only_collection_is_rejected(payload: bytes) -> None:
    reader = PacketReader(payload, decode_collections=True)
    with pytest.raises(ValueError, match="malformed NULL-only collection"):
        _read_value(reader, CUBRIDDataType.SEQUENCE, len(payload))
