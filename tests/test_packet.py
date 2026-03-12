from __future__ import annotations

import datetime
import struct
from decimal import Decimal
from typing import Callable, cast

import pytest

from pycubrid.constants import CUBRIDDataType, DataSize
from pycubrid.packet import (
    DEFAULT_CAS_INFO,
    PacketReader,
    PacketWriter,
    build_protocol_header,
    parse_protocol_header,
)


def _build_lob_handle(db_type: int, lob_length: int, file_locator: str) -> bytes:
    locator = file_locator.encode("utf-8") + b"\x00"
    return struct.pack(">iqi", db_type, lob_length, len(locator)) + locator


def _call_method(obj: object, method_name: str, *args: object) -> object:
    method = cast(Callable[..., object], getattr(obj, method_name))
    return method(*args)


class TestProtocolHeader:
    def test_default_cas_info_constant(self) -> None:
        assert DEFAULT_CAS_INFO == b"\x00\x00\x00\x00"

    def test_build_and_parse_protocol_header(self) -> None:
        cas_info = b"\x01\x02\x03\x04"
        header = build_protocol_header(12345, cas_info)

        assert header == struct.pack(">i", 12345) + cas_info

        length, parsed_cas_info = parse_protocol_header(header)
        assert length == 12345
        assert parsed_cas_info == cas_info

    def test_parse_protocol_header_uses_first_8_bytes(self) -> None:
        cas_info = b"ABCD"
        header = build_protocol_header(99, cas_info)
        payload = header + b"EXTRA"

        length, parsed_cas_info = parse_protocol_header(payload)
        assert length == 99
        assert parsed_cas_info == cas_info


class TestPacketWriterAddMethods:
    @pytest.mark.parametrize(
        ("method_name", "value", "expected"),
        [
            ("add_byte", 0x7F, struct.pack(">iB", DataSize.BYTE, 0x7F)),
            ("add_short", -123, struct.pack(">ih", DataSize.SHORT, -123)),
            ("add_int", -2147483648, struct.pack(">ii", DataSize.INT, -2147483648)),
            (
                "add_long",
                9223372036854775807,
                struct.pack(">iq", DataSize.LONG, 9223372036854775807),
            ),
            ("add_float", 1.5, struct.pack(">if", DataSize.FLOAT, 1.5)),
            ("add_double", -2.5, struct.pack(">id", DataSize.DOUBLE, -2.5)),
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

    def test_add_datetime_exact_bytes(self) -> None:
        writer = PacketWriter()
        writer.add_datetime(2026, 12, 15, 10, 20, 30, 400)

        expected = struct.pack(">i", DataSize.DATETIME) + struct.pack(
            ">hhhhhhh", 2026, 12, 15, 10, 20, 30, 400
        )
        assert writer.to_bytes() == expected

    def test_add_date_delegates_to_datetime(self) -> None:
        writer = PacketWriter()
        writer.add_date(2026, 6, 7)

        expected = struct.pack(">i", DataSize.DATETIME) + struct.pack(
            ">hhhhhhh", 2026, 6, 7, 0, 0, 0, 0
        )
        assert writer.to_bytes() == expected

    def test_add_time_delegates_to_datetime(self) -> None:
        writer = PacketWriter()
        writer.add_time(8, 9, 10)

        expected = struct.pack(">i", DataSize.DATETIME) + struct.pack(
            ">hhhhhhh", 0, 0, 0, 8, 9, 10, 0
        )
        assert writer.to_bytes() == expected

    def test_add_timestamp_exact_bytes(self) -> None:
        writer = PacketWriter()
        writer.add_timestamp(2027, 4, 5, 11, 12, 13)

        expected = struct.pack(">i", DataSize.DATETIME) + struct.pack(
            ">hhhhhhh", 2027, 4, 5, 11, 12, 13, 0
        )
        assert writer.to_bytes() == expected

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
        _ = _call_method(writer, "_write_float", 1.25)
        _ = _call_method(writer, "_write_double", -3.5)
        _ = _call_method(writer, "_write_bytes", b"XY")

        expected = (
            struct.pack(">B", 255)
            + struct.pack(">h", -2)
            + struct.pack(">i", 2147483647)
            + struct.pack(">q", -9223372036854775808)
            + struct.pack(">f", 1.25)
            + struct.pack(">d", -3.5)
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

    def test_parse_buffer_alias(self) -> None:
        reader = PacketReader(b"abcd")
        assert _call_method(reader, "_parse_buffer", 2) == b"ab"
        assert _call_method(reader, "_parse_buffer", 2) == b"cd"
        assert reader.bytes_remaining() == 0

    def test_bytes_remaining_decreases_while_parsing(self) -> None:
        reader = PacketReader(b"\x01\x02\x03\x04")
        assert reader.bytes_remaining() == 4

        _ = _call_method(reader, "_parse_byte")
        assert reader.bytes_remaining() == 3

        _ = _call_method(reader, "_parse_bytes", 2)
        assert reader.bytes_remaining() == 1


class TestPacketReaderComplexPackets:
    def test_read_blob(self) -> None:
        handle = _build_lob_handle(int(CUBRIDDataType.BLOB), 123456789, "file:/tmp/blob1")
        reader = PacketReader(handle)

        result = reader.read_blob(len(handle))

        assert result["lob_type"] == CUBRIDDataType.BLOB
        assert result["lob_length"] == 123456789
        assert result["file_locator"] == "file:/tmp/blob1"
        assert result["packed_lob_handle"] == handle
        assert reader.bytes_remaining() == 0

    def test_read_clob(self) -> None:
        handle = _build_lob_handle(int(CUBRIDDataType.CLOB), 987654321, "file:/tmp/clob1")
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
        writer.add_float(3.25)
        writer.add_double(-4.5)
        writer.add_bytes(b"hello")

        reader = PacketReader(writer.to_bytes())

        assert _call_method(reader, "_parse_int") == DataSize.INT
        assert _call_method(reader, "_parse_int") == -123

        assert _call_method(reader, "_parse_int") == DataSize.LONG
        assert _call_method(reader, "_parse_long") == 456

        assert _call_method(reader, "_parse_int") == DataSize.FLOAT
        assert _call_method(reader, "_parse_float") == 3.25

        assert _call_method(reader, "_parse_int") == DataSize.DOUBLE
        assert _call_method(reader, "_parse_double") == -4.5

        byte_size = cast(int, _call_method(reader, "_parse_int"))
        assert byte_size == 5
        assert _call_method(reader, "_parse_bytes", byte_size) == b"hello"

    def test_round_trip_datetime_variants(self) -> None:
        writer = PacketWriter()
        writer.add_date(2030, 6, 7)
        writer.add_time(8, 9, 10)
        writer.add_timestamp(2031, 7, 8, 11, 12, 13)
        writer.add_datetime(2032, 8, 9, 14, 15, 16, 123)

        reader = PacketReader(writer.to_bytes())

        # DATE: add_date writes 14 bytes (7 shorts via add_datetime),
        # but _parse_date reads only 6 bytes (3 shorts: year, month, day).
        # Skip the remaining 8 bytes (4 zero shorts: h,m,s,ms).
        assert _call_method(reader, "_parse_int") == DataSize.DATETIME
        assert _call_method(reader, "_parse_date") == datetime.date(2030, 6, 7)
        _call_method(reader, "_parse_bytes", 8)  # skip 4 zero shorts

        # TIME: same write size (14 bytes), _parse_time reads 6 bytes (3 shorts).
        # Skip 8 bytes (year=0, month=0, day=0 before + ms=0 after — but
        # add_time writes (0, 0, 0, h, m, s, 0), so h/m/s are at offset 3-5).
        # Actually add_time calls add_datetime(0, 0, 0, h, m, s, 0) which writes:
        #   year=0, month=0, day=0, hour=8, minute=9, second=10, ms=0
        # _parse_time reads the first 3 shorts as hour, minute, second — but
        # the first 3 shorts are actually year=0, month=0, day=0!
        # This means the round-trip test can't simply call _parse_time on
        # add_time output. Instead, we skip the 3 date shorts, parse time,
        # then skip the trailing ms short.
        assert _call_method(reader, "_parse_int") == DataSize.DATETIME
        _call_method(reader, "_parse_bytes", 6)  # skip 3 date shorts (year=0, month=0, day=0)
        assert _call_method(reader, "_parse_time") == datetime.time(8, 9, 10)
        _call_method(reader, "_parse_bytes", 2)  # skip ms=0 short

        # TIMESTAMP: add_timestamp writes 14 bytes, _parse_timestamp reads 12 (6 shorts).
        # Skip remaining 2 bytes (ms=0 short).
        assert _call_method(reader, "_parse_int") == DataSize.DATETIME
        assert _call_method(reader, "_parse_timestamp") == datetime.datetime(2031, 7, 8, 11, 12, 13)
        _call_method(reader, "_parse_bytes", 2)  # skip ms=0 short

        # DATETIME: add_datetime writes 14 bytes, _parse_datetime reads 14. Exact match.
        assert _call_method(reader, "_parse_int") == DataSize.DATETIME
        assert _call_method(reader, "_parse_datetime") == datetime.datetime(
            2032, 8, 9, 14, 15, 16, 123000
        )
