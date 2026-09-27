"""Offline schema wire groundwork; no live getter or parity certification."""

from __future__ import annotations

import struct
from dataclasses import fields

import pytest

from pycubrid import protocol
from pycubrid.constants import CASFunctionCode, CASProtocol, CUBRIDDataType
from pycubrid.packet import PacketReader


CAS_INFO = b"\x00\x01\x02\x03"


def _string(value: str | None) -> bytes:
    if value is None:
        return struct.pack(">i", 0)
    encoded = value.encode("utf-8") + b"\x00"
    return struct.pack(">i", len(encoded)) + encoded


def _column(wire_type: bytes, name: str = "column") -> bytes:
    return wire_type + struct.pack(">hi", -2, 123) + _string(name)


TYPE_CASES = [
    (b"\x08", CUBRIDDataType.INT),
    (b"\x80\x1d", CUBRIDDataType.TIMESTAMPTZ),
    (b"\x22", CUBRIDDataType.SET),
    (b"\xa0\x02", CUBRIDDataType.SET),
    (b"\x42", CUBRIDDataType.MULTISET),
    (b"\xc0\x02", CUBRIDDataType.MULTISET),
    (b"\x60", CUBRIDDataType.SEQUENCE),
    (b"\xe0\x00", CUBRIDDataType.SEQUENCE),
]


@pytest.mark.parametrize(("wire_type", "column_type"), TYPE_CASES)
@pytest.mark.parametrize("non_null", [0, 1, 7])
def test_select_metadata_preserves_full_tail(
    wire_type: bytes, column_type: int, non_null: int
) -> None:
    """Lock ordinary SELECT decoding before sharing only its type-byte logic."""
    data = (
        _column(wire_type, "alias")
        + _string("attribute")
        + _string("table")
        + bytes([non_null])
        + _string("default")
        + b"\x01\x00\x01\x00\x01\x00\x01"
        + b"tail"
    )
    reader = PacketReader(data)
    column = protocol._parse_column_metadata(reader, 1)[0]
    assert (column.column_type, column.scale, column.precision, column.name) == (
        column_type,
        -2,
        123,
        "alias",
    )
    assert (column.real_name, column.table_name, column.default_value) == (
        "attribute",
        "table",
        "default",
    )
    assert column.is_nullable is (non_null == 0)
    assert (
        column.is_auto_increment,
        column.is_unique_key,
        column.is_primary_key,
        column.is_reverse_index,
        column.is_reverse_unique,
        column.is_foreign_key,
        column.is_shared,
    ) == (True, False, True, False, True, False, True)
    assert reader.bytes_remaining() == 4
    assert reader._parse_bytes(4) == b"tail"


@pytest.mark.parametrize("version", [4, 5, 8])
@pytest.mark.parametrize(
    ("arg1", "arg2"),
    [(None, None), ("", None), (None, ""), ("", ""), ("%", "属性_"), ("left", "right")],
)
def test_schema_request_nullable_strings_and_versions(
    version: int, arg1: str | None, arg2: str | None
) -> None:
    payload = (
        bytes([CASFunctionCode.SCHEMA_INFO])
        + struct.pack(">ii", 4, 19)
        + _string(arg1)
        + _string(arg2)
        + struct.pack(">iB", 1, 3)
    )
    if version >= 5:
        payload += struct.pack(">ii", 4, -1)
    expected = struct.pack(">i", len(payload)) + CAS_INFO + payload
    assert (
        protocol._write_schema_info_request(
            CAS_INFO, 19, arg1, arg2, 3, shard_id=-1, protocol_version=version
        )
        == expected
    )


@pytest.mark.parametrize("flags", [0, 1, 2, 3])
def test_schema_request_default_version_and_shard(flags: int) -> None:
    actual = protocol._write_schema_info_request(CAS_INFO, 4, "table", "attr%", flags)
    explicit = protocol._write_schema_info_request(
        CAS_INFO, 4, "table", "attr%", flags, shard_id=0, protocol_version=CASProtocol.VERSION
    )
    assert actual == explicit
    assert actual[-9:] == struct.pack(">Bii", flags, 4, 0)


@pytest.mark.parametrize(("wire_type", "column_type"), TYPE_CASES)
def test_schema_columns_have_only_condensed_fields(wire_type: bytes, column_type: int) -> None:
    reader = PacketReader(_column(wire_type, "属性") + b"tail")
    column = protocol._parse_schema_column_metadata(reader, 1)[0]
    assert [field.name for field in fields(column)] == ["column_type", "scale", "precision", "name"]
    assert (column.column_type, column.scale, column.precision, column.name) == (
        column_type,
        -2,
        123,
        "属性",
    )
    assert reader.bytes_remaining() == 4
    assert reader._parse_bytes(4) == b"tail"


def test_multiple_schema_columns_stop_before_row_data() -> None:
    reader = PacketReader(_column(b"\x08", "id") + _column(b"\x02", "name") + b"rows")
    columns = protocol._parse_schema_column_metadata(reader, 2)
    assert [column.name for column in columns] == ["id", "name"]
    assert reader._parse_bytes(4) == b"rows"


def test_zero_schema_columns_consume_nothing() -> None:
    reader = PacketReader(b"tail")
    assert protocol._parse_schema_column_metadata(reader, 0) == []
    assert reader.bytes_remaining() == 4


def test_negative_schema_column_count_is_rejected() -> None:
    reader = PacketReader(b"tail")
    with pytest.raises(ValueError, match="column count"):
        protocol._parse_schema_column_metadata(reader, -1)
    assert reader.bytes_remaining() == 4


@pytest.mark.parametrize("length", [-1, 4])
def test_invalid_schema_column_name_length_is_rejected(length: int) -> None:
    reader = PacketReader(b"\x08" + struct.pack(">hii", 0, 10, length) + b"x\x00")
    with pytest.raises(ValueError, match="name length"):
        protocol._parse_schema_column_metadata(reader, 1)


@pytest.mark.parametrize("wire_type", [b"\x08", b"\x80\x1d"])
def test_every_truncated_schema_column_prefix_fails(wire_type: bytes) -> None:
    data = _column(wire_type, "name")
    for length in range(len(data)):
        with pytest.raises((IndexError, struct.error, ValueError)):
            protocol._parse_schema_column_metadata(PacketReader(data[:length]), 1)


def test_schema_column_zero_name_length() -> None:
    reader = PacketReader(b"\x08" + struct.pack(">hii", 0, 10, 0))
    assert protocol._parse_schema_column_metadata(reader, 1)[0].name == ""
    assert reader.bytes_remaining() == 0


def test_schema_packet_activates_corrected_request_and_condensed_columns() -> None:
    """FC9 activation ships in the same slice as owning consumption/cleanup."""
    packet = protocol.GetSchemaPacket(1, "%", 1, arg2="id%")
    assert packet.write(CAS_INFO) == protocol._write_schema_info_request(
        CAS_INFO, 1, "%", "id%", 1
    )
    packet.parse(CAS_INFO + struct.pack(">iii", 17, 3, 1) + _column(b"\x08"))
    assert (packet.query_handle, packet.tuple_count) == (17, 3)
    assert len(packet.columns) == 1
    assert packet.columns[0].name == "column"


@pytest.mark.parametrize("count", [-1, -10])
def test_schema_packet_rejects_negative_row_count(count: int) -> None:
    packet = protocol.GetSchemaPacket(1)
    with pytest.raises(ValueError, match="tuple count"):
        packet.parse(CAS_INFO + struct.pack(">iii", 17, count, 0))
