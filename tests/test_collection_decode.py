from __future__ import annotations

import struct
from unittest.mock import MagicMock

import pycubrid
import pytest

from pycubrid.constants import CUBRIDDataType, CUBRIDStatementType, DataSize
from pycubrid.cursor import Cursor
from pycubrid.packet import PacketReader
from pycubrid.protocol import ColumnMetaData, FetchPacket, PrepareAndExecutePacket, _read_value


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
