from __future__ import annotations

import datetime
import struct
from decimal import Decimal
from typing import Callable, cast

import pytest

from pycubrid.constants import CUBRIDDataType, DataSize
from pycubrid.packet import PacketReader, PacketWriter


def _build_lob_handle(db_type: int, lob_length: int, file_locator: str) -> bytes:
    """A packed handle as the CAS sends it; db_type is the DB_TYPE (BLOB 33, CLOB 34)."""
    locator = file_locator.encode("utf-8") + b"\x00"
    return struct.pack(">iqi", db_type, lob_length, len(locator)) + locator


def _call_method(obj: object, method_name: str, *args: object) -> object:
    method = cast(Callable[..., object], getattr(obj, method_name))
    return method(*args)


class TestPacketWriterAddMethods:
    @pytest.mark.parametrize(
        ("method_name", "value", "expected"),
        [
            ("add_byte", 0x7F, struct.pack(">iB", DataSize.BYTE, 0x7F)),
            ("add_int", -2147483648, struct.pack(">ii", DataSize.INT, -2147483648)),
            (
                "add_long",
                9223372036854775807,
                struct.pack(">iq", DataSize.LONG, 9223372036854775807),
            ),
        ],
    )
    def test_add_numeric_methods_exact_bytes(
        self,
        method_name: str,
        value: int | float,
        expected: bytes,
    ) -> None:
        writer = PacketWriter()
        method = cast(Callable[[int | float], None], getattr(writer, method_name))
        method(value)
        assert writer.to_bytes() == expected

    def test_add_bytes_exact_bytes(self) -> None:
        writer = PacketWriter()
        writer.add_bytes(b"ABC")
        assert writer.to_bytes() == struct.pack(">i", 3) + b"ABC"

    def test_add_bytes_zero_length(self) -> None:
        writer = PacketWriter()
        writer.add_bytes(b"")
        assert writer.to_bytes() == struct.pack(">i", 0)

    def test_add_null_exact_bytes(self) -> None:
        writer = PacketWriter()
        writer.add_null()
        assert writer.to_bytes() == b"\x00\x00\x00\x00"

    def test_add_cache_time_exact_bytes(self) -> None:
        writer = PacketWriter()
        writer.add_cache_time()
        expected = struct.pack(">iii", DataSize.LONG, 0, 0)

        assert writer.to_bytes() == expected
        assert len(writer.to_bytes()) == 12

    def test_writer_len_and_to_bytes(self) -> None:
        writer = PacketWriter()
        assert len(writer) == 0

        writer.add_int(2147483647)
        expected = struct.pack(">ii", DataSize.INT, 2147483647)

        assert writer.to_bytes() == expected
        assert len(writer) == len(expected)


class TestPacketWriterRawMethods:
    def test_write_raw_numeric_methods_exact_bytes(self) -> None:
        writer = PacketWriter()
        _ = _call_method(writer, "_write_byte", 255)
        _ = _call_method(writer, "_write_short", -2)
        _ = _call_method(writer, "_write_int", 2147483647)
        _ = _call_method(writer, "_write_long", -9223372036854775808)
        _ = _call_method(writer, "_write_bytes", b"XY")

        expected = (
            struct.pack(">B", 255)
            + struct.pack(">h", -2)
            + struct.pack(">i", 2147483647)
            + struct.pack(">q", -9223372036854775808)
            + b"XY"
        )
        assert writer.to_bytes() == expected

    def test_write_filler_with_positive_and_zero_count(self) -> None:
        writer = PacketWriter()
        _ = _call_method(writer, "_write_filler", 3, 0x41)
        _ = _call_method(writer, "_write_filler", 0, 0x42)
        assert writer.to_bytes() == b"AAA"

    def test_write_null_terminated_string_utf8(self) -> None:
        writer = PacketWriter()
        _ = _call_method(writer, "_write_null_terminated_string", "안녕")

        encoded = "안녕".encode("utf-8")
        expected = struct.pack(">i", len(encoded) + 1) + encoded + b"\x00"
        assert writer.to_bytes() == expected

    def test_write_fixed_length_string_truncate_and_pad(self) -> None:
        writer = PacketWriter()
        _ = _call_method(writer, "_write_fixed_length_string", "abcdef", 4)
        _ = _call_method(writer, "_write_fixed_length_string", "xy", 5, 0x2E)
        _ = _call_method(writer, "_write_fixed_length_string", "ignored", 0, 0x21)

        assert writer.to_bytes() == b"abcdxy..."


class TestPacketReaderParseMethods:
    def test_parse_primitive_methods(self) -> None:
        payload = (
            struct.pack(">B", 0xFE)
            + struct.pack(">h", -321)
            + struct.pack(">i", -2147483648)
            + struct.pack(">q", 9223372036854775807)
            + struct.pack(">f", 1.5)
            + struct.pack(">d", -2.25)
            + b"XYZ"
        )
        reader = PacketReader(payload)

        assert _call_method(reader, "_parse_byte") == 0xFE
        assert _call_method(reader, "_parse_short") == -321
        assert _call_method(reader, "_parse_int") == -2147483648
        assert _call_method(reader, "_parse_long") == 9223372036854775807
        assert _call_method(reader, "_parse_float") == 1.5
        assert _call_method(reader, "_parse_double") == -2.25
        assert _call_method(reader, "_parse_bytes", 3) == b"XYZ"
        assert reader.bytes_remaining() == 0

    def test_parse_null_terminated_string_with_utf8(self) -> None:
        text = "가격"
        encoded = text.encode("utf-8") + b"\x00"
        reader = PacketReader(encoded)

        assert _call_method(reader, "_parse_null_terminated_string", len(encoded)) == text

    def test_parse_null_terminated_string_without_terminator(self) -> None:
        reader = PacketReader(b"abc")
        assert _call_method(reader, "_parse_null_terminated_string", 3) == "abc"

    def test_parse_null_terminated_string_with_non_positive_length(self) -> None:
        reader = PacketReader(b"ignored")
        assert _call_method(reader, "_parse_null_terminated_string", 0) == ""
        assert reader.bytes_remaining() == len(b"ignored")

    def test_parse_date(self) -> None:
        payload = struct.pack(">hhh", 2026, 4, 9)
        reader = PacketReader(payload)
        assert _call_method(reader, "_parse_date") == datetime.date(2026, 4, 9)

    def test_parse_time(self) -> None:
        payload = struct.pack(">hhh", 23, 59, 58)
        reader = PacketReader(payload)
        assert _call_method(reader, "_parse_time") == datetime.time(23, 59, 58)

    def test_parse_datetime(self) -> None:
        payload = struct.pack(">hhhhhhh", 2027, 11, 5, 6, 7, 8, 9)
        reader = PacketReader(payload)
        assert _call_method(reader, "_parse_datetime") == datetime.datetime(
            2027, 11, 5, 6, 7, 8, 9000
        )

    def test_parse_timestamp(self) -> None:
        payload = struct.pack(">hhhhhh", 2028, 6, 1, 2, 3, 4)
        reader = PacketReader(payload)
        assert _call_method(reader, "_parse_timestamp") == datetime.datetime(2028, 6, 1, 2, 3, 4)

    def test_parse_numeric(self) -> None:
        encoded = b"-1234.500\x00"
        reader = PacketReader(encoded)
        assert _call_method(reader, "_parse_numeric", len(encoded)) == Decimal("-1234.500")

    def test_parse_object(self) -> None:
        payload = struct.pack(">ihh", 42, 7, 3)
        reader = PacketReader(payload)
        assert _call_method(reader, "_parse_object") == "OID:@42|7|3"

    def test_bytes_remaining_decreases_while_parsing(self) -> None:
        reader = PacketReader(b"\x01\x02\x03\x04")
        assert reader.bytes_remaining() == 4

        _ = _call_method(reader, "_parse_byte")
        assert reader.bytes_remaining() == 3

        _ = _call_method(reader, "_parse_bytes", 2)
        assert reader.bytes_remaining() == 1


class TestPacketReaderComplexPackets:
    def test_read_blob(self) -> None:
        handle = _build_lob_handle(33, 123456789, "file:/tmp/blob1")
        reader = PacketReader(handle)

        result = reader.read_blob(len(handle))

        assert result["lob_type"] == CUBRIDDataType.BLOB
        assert result["lob_length"] == 123456789
        assert result["file_locator"] == "file:/tmp/blob1"
        assert result["packed_lob_handle"] == handle
        assert reader.bytes_remaining() == 0

    def test_read_clob(self) -> None:
        handle = _build_lob_handle(34, 987654321, "file:/tmp/clob1")
        reader = PacketReader(handle + b"TAIL")

        result = reader.read_clob(len(handle))

        assert result["lob_type"] == CUBRIDDataType.CLOB
        assert result["lob_length"] == 987654321
        assert result["file_locator"] == "file:/tmp/clob1"
        assert result["packed_lob_handle"] == handle
        assert reader.bytes_remaining() == 4

    def test_read_error(self) -> None:
        message = "syntax error"
        encoded = message.encode("utf-8") + b"\x00"
        payload = struct.pack(">i", -1001) + encoded
        reader = PacketReader(payload)

        code, parsed_message = reader.read_error(len(payload))
        assert code == -1001
        assert parsed_message == message
        assert reader.bytes_remaining() == 0

    def test_read_error_with_no_message(self) -> None:
        payload = struct.pack(">i", -77)
        reader = PacketReader(payload)

        code, parsed_message = reader.read_error(len(payload))
        assert code == -77
        assert parsed_message == ""


class TestRoundTrip:
    def test_round_trip_numeric_and_bytes(self) -> None:
        writer = PacketWriter()
        writer.add_int(-123)
        writer.add_long(456)
        writer.add_bytes(b"hello")

        reader = PacketReader(writer.to_bytes())

        assert _call_method(reader, "_parse_int") == DataSize.INT
        assert _call_method(reader, "_parse_int") == -123

        assert _call_method(reader, "_parse_int") == DataSize.LONG
        assert _call_method(reader, "_parse_long") == 456

        byte_size = cast(int, _call_method(reader, "_parse_int"))
        assert byte_size == 5
        assert _call_method(reader, "_parse_bytes", byte_size) == b"hello"


class TestPackedLobHandleFraming:
    """The packed LOB handle inside a declared cell size (#933).

    cas_execute.c (10.2-11.4) ``add_res_data_lob_handle`` declares exactly
    ``4 + 8 + 4 + locator_size`` bytes and ``dblob_to_caslob`` sets the
    server DB_TYPE (BLOB 33, CLOB 34), a NUL-terminated locator, or size -1
    with no locator for a value without an ELO. Live 11.4 and 10.2 handles
    have this shape, with no padding.
    """

    # The shape of a handle fetched from a CUBRID 11.4 BLOB column.
    LIVE_BLOB = _build_lob_handle(33, 3, "file:/var/lib/cubrid/testdb/lob/ces_334/fx933.0001_1")

    @pytest.mark.parametrize(("read", "db_type"), [("read_blob", 33), ("read_clob", 34)])
    def test_well_formed_handle_keeps_following_bytes(self, read: str, db_type: int) -> None:
        handle = _build_lob_handle(db_type, 3, "file:/x/ces_1/t.0001_1")
        reader = PacketReader(handle + b"NEXT-FIELD")

        result = getattr(reader, read)(len(handle))

        assert result["packed_lob_handle"] == handle
        assert result["file_locator"] == "file:/x/ces_1/t.0001_1"
        assert reader._parse_bytes(reader.bytes_remaining()) == b"NEXT-FIELD"

    def test_value_without_elo_is_accepted(self) -> None:
        handle = struct.pack(">iqi", 34, -1, 0)
        result = PacketReader(handle).read_clob(len(handle))
        assert (result["lob_length"], result["file_locator"]) == (-1, "")

    @pytest.mark.parametrize("size", [0, 4, 12, 15])
    def test_shortened_header_is_malformed(self, size: int) -> None:
        reader = PacketReader(self.LIVE_BLOB[:size] + b"\x00" * 32)
        with pytest.raises(ValueError, match="shorter than its header"):
            reader.read_blob(size)

    def test_locator_past_the_handle_is_malformed(self) -> None:
        # The next field's bytes must not be read as the rest of the locator.
        handle = struct.pack(">iqi", 33, 3, 20) + b"file:/x\x00"
        reader = PacketReader(handle + b"\x01" * 32)
        with pytest.raises(ValueError, match="past the end of the broker reply"):
            reader.read_blob(len(handle))

    def test_negative_locator_size_is_malformed(self) -> None:
        handle = struct.pack(">iqi", 33, 3, -1) + b"file:/x\x00"
        with pytest.raises(ValueError, match="negative length"):
            PacketReader(handle).read_blob(len(handle))

    def test_locator_shorter_than_the_handle_is_malformed(self) -> None:
        # Bytes inside the declared handle that its locator does not cover.
        handle = struct.pack(">iqi", 33, 3, 8) + b"file:/x\x00" + b"PAD"
        with pytest.raises(ValueError, match="locator does not fill the handle"):
            PacketReader(handle).read_blob(len(handle))

    @pytest.mark.parametrize(
        ("read", "db_type"),
        [
            ("read_blob", 34),  # a CLOB handle in a BLOB cell
            ("read_clob", 33),
            ("read_blob", int(CUBRIDDataType.BLOB)),  # the CCI type 23, not DB_TYPE
            ("read_clob", int(CUBRIDDataType.CLOB)),
            ("read_blob", 0),
            ("read_blob", 9),  # the obsolete DB_TYPE_ELO
        ],
    )
    def test_mismatched_or_unknown_embedded_type_is_malformed(
        self, read: str, db_type: int
    ) -> None:
        handle = _build_lob_handle(db_type, 3, "file:/x")
        with pytest.raises(ValueError, match="embedded type does not match the column"):
            getattr(PacketReader(handle), read)(len(handle))
