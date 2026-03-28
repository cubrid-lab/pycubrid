from __future__ import annotations

import datetime
import struct
from decimal import Decimal

from .constants import CUBRIDDataType, DataSize

# Pre-compiled struct objects — avoids format-string parsing on every call.
_STRUCT_SHORT = struct.Struct(">h")
_STRUCT_INT = struct.Struct(">i")
_STRUCT_LONG = struct.Struct(">q")
_STRUCT_FLOAT = struct.Struct(">f")
_STRUCT_DOUBLE = struct.Struct(">d")
_STRUCT_BYTE = struct.Struct(">B")
_STRUCT_3H = struct.Struct(">3h")
_STRUCT_6H = struct.Struct(">6h")
_STRUCT_7H = struct.Struct(">7h")

DEFAULT_CAS_INFO: bytes = b"\x00\x00\x00\x00"


def build_protocol_header(data_length: int, cas_info: bytes) -> bytes:
    """Build an 8-byte protocol header."""
    return struct.pack(">i", data_length) + cas_info


def parse_protocol_header(data: bytes) -> tuple[int, bytes]:
    """Parse an 8-byte protocol header."""
    data_length: int = _STRUCT_INT.unpack(data[: DataSize.DATA_LENGTH])[0]
    cas_info = data[DataSize.DATA_LENGTH : DataSize.DATA_LENGTH + DataSize.CAS_INFO]
    return data_length, cas_info


class PacketWriter:
    def __init__(self) -> None:
        self._buffer: bytearray = bytearray()

    def add_byte(self, value: int) -> None:
        """Write a length-prefixed byte value."""
        self._write_int(DataSize.BYTE)
        self._write_byte(value)

    def add_short(self, value: int) -> None:
        """Write a length-prefixed short value."""
        self._write_int(DataSize.SHORT)
        self._write_short(value)

    def add_int(self, value: int) -> None:
        """Write a length-prefixed int value."""
        self._write_int(DataSize.INT)
        self._write_int(value)

    def add_long(self, value: int) -> None:
        """Write a length-prefixed long value."""
        self._write_int(DataSize.LONG)
        self._write_long(value)

    def add_float(self, value: float) -> None:
        """Write a length-prefixed float value."""
        self._write_int(DataSize.FLOAT)
        self._write_float(value)

    def add_double(self, value: float) -> None:
        """Write a length-prefixed double value."""
        self._write_int(DataSize.DOUBLE)
        self._write_double(value)

    def add_bytes(self, value: bytes) -> None:
        """Write length-prefixed raw bytes."""
        self._write_int(len(value))
        self._write_bytes(value)

    def add_null(self) -> None:
        """Write a null marker (zero length)."""
        self._write_int(DataSize.UNSPECIFIED)

    def add_date(self, year: int, month: int, day: int) -> None:
        """Write a length-prefixed date (time fields zeroed)."""
        self.add_datetime(year, month, day, 0, 0, 0, 0)

    def add_time(self, hour: int, minute: int, second: int) -> None:
        """Write a length-prefixed time (date fields zeroed)."""
        self.add_datetime(0, 0, 0, hour, minute, second, 0)

    def add_timestamp(
        self,
        year: int,
        month: int,
        day: int,
        hour: int,
        minute: int,
        second: int,
    ) -> None:
        """Write a length-prefixed timestamp (millisecond zeroed)."""
        self.add_datetime(year, month, day, hour, minute, second, 0)

    def add_datetime(
        self,
        year: int,
        month: int,
        day: int,
        hour: int,
        minute: int,
        second: int,
        millisecond: int,
    ) -> None:
        """Write a length-prefixed datetime with seven shorts."""
        self._write_int(DataSize.DATETIME)
        self._write_short(year)
        self._write_short(month)
        self._write_short(day)
        self._write_short(hour)
        self._write_short(minute)
        self._write_short(second)
        self._write_short(millisecond)

    def add_cache_time(self) -> None:
        """Write a length-prefixed cache time value (two zero ints)."""
        self._write_int(DataSize.LONG)
        self._write_int(0)
        self._write_int(0)

    def _write_byte(self, value: int) -> None:
        self._buffer.extend(_STRUCT_BYTE.pack(value & 0xFF))

    def _write_short(self, value: int) -> None:
        self._buffer.extend(_STRUCT_SHORT.pack(value))

    def _write_int(self, value: int) -> None:
        self._buffer.extend(_STRUCT_INT.pack(value))

    def _write_long(self, value: int) -> None:
        self._buffer.extend(_STRUCT_LONG.pack(value))

    def _write_float(self, value: float) -> None:
        self._buffer.extend(_STRUCT_FLOAT.pack(value))

    def _write_double(self, value: float) -> None:
        self._buffer.extend(_STRUCT_DOUBLE.pack(value))

    def _write_bytes(self, value: bytes) -> None:
        self._buffer.extend(value)

    def _write_filler(self, count: int, value: int = 0) -> None:
        if count <= 0:
            return
        self._buffer.extend(bytes([value & 0xFF]) * count)

    def _write_null_terminated_string(self, value: str) -> None:
        encoded = value.encode("utf-8")
        self._write_int(len(encoded) + 1)
        self._write_bytes(encoded)
        self._write_byte(0)

    def _write_fixed_length_string(self, value: str, length: int, filler: int = 0) -> None:
        if length <= 0:
            return

        encoded = value.encode("utf-8")
        fixed = encoded[:length]
        self._write_bytes(fixed)
        if len(fixed) < length:
            self._write_filler(length - len(fixed), filler)

    def to_bytes(self) -> bytes:
        """Return all bytes currently written."""
        return bytes(self._buffer)

    def __len__(self) -> int:
        """Return current buffer size."""
        return len(self._buffer)


class PacketReader:
    __slots__ = ("_buffer", "_offset")

    def __init__(self, data: bytes | bytearray) -> None:
        self._buffer: memoryview = memoryview(data)
        self._offset: int = 0

    def _parse_byte(self) -> int:
        value = self._buffer[self._offset]
        self._offset += DataSize.BYTE
        return value

    def _parse_short(self, size: int = 0) -> int:
        value: int = _STRUCT_SHORT.unpack_from(self._buffer, self._offset)[0]
        self._offset += DataSize.SHORT
        return value

    def _parse_int(self, size: int = 0) -> int:
        value: int = _STRUCT_INT.unpack_from(self._buffer, self._offset)[0]
        self._offset += DataSize.INT
        return value

    def _parse_long(self, size: int = 0) -> int:
        value: int = _STRUCT_LONG.unpack_from(self._buffer, self._offset)[0]
        self._offset += DataSize.LONG
        return value

    def _parse_float(self, size: int = 0) -> float:
        value: float = _STRUCT_FLOAT.unpack_from(self._buffer, self._offset)[0]
        self._offset += DataSize.FLOAT
        return value

    def _parse_double(self, size: int = 0) -> float:
        value: float = _STRUCT_DOUBLE.unpack_from(self._buffer, self._offset)[0]
        self._offset += DataSize.DOUBLE
        return value

    def _parse_bytes(self, count: int) -> bytes:
        start = self._offset
        end = start + count
        self._offset = end
        return bytes(self._buffer[start:end])

    def _parse_null_terminated_string(self, length: int) -> str:
        if length <= 0:
            return ""

        start = self._offset
        end = start + length
        self._offset = end
        view = self._buffer[start:end]
        if view[-1] == 0:
            view = view[:-1]
        return bytes(view).decode("utf-8")

    def _parse_date(self, size: int = 0) -> datetime.date:
        year, month, day = _STRUCT_3H.unpack_from(self._buffer, self._offset)
        self._offset += 6
        return datetime.date(year, month, day)

    def _parse_time(self, size: int = 0) -> datetime.time:
        hour, minute, second = _STRUCT_3H.unpack_from(self._buffer, self._offset)
        self._offset += 6
        return datetime.time(hour, minute, second)

    def _parse_datetime(self, size: int = 0) -> datetime.datetime:
        y, mo, d, h, mi, s, ms = _STRUCT_7H.unpack_from(self._buffer, self._offset)
        self._offset += 14
        return datetime.datetime(y, mo, d, h, mi, s, ms * 1000)

    def _parse_timestamp(self, size: int = 0) -> datetime.datetime:
        y, mo, d, h, mi, s = _STRUCT_6H.unpack_from(self._buffer, self._offset)
        self._offset += 12
        return datetime.datetime(y, mo, d, h, mi, s, 0)

    def _parse_numeric(self, size: int) -> Decimal:
        value = self._parse_null_terminated_string(size)
        return Decimal(value)

    def _parse_object(self, size: int = 0) -> str:
        page = self._parse_int()
        slot = self._parse_short()
        volume = self._parse_short()
        return f"OID:@{page}|{slot}|{volume}"

    def read_blob(self, size: int) -> dict[str, object]:
        """Read a packed BLOB handle from the buffer."""
        return self._read_lob(size, CUBRIDDataType.BLOB)

    def read_clob(self, size: int) -> dict[str, object]:
        """Read a packed CLOB handle from the buffer."""
        return self._read_lob(size, CUBRIDDataType.CLOB)

    def read_error(self, response_length: int) -> tuple[int, str]:
        """Read an error packet body as ``(error_code, message)``."""
        error_code = self._parse_int()
        message_size = response_length - DataSize.INT
        error_message = self._parse_null_terminated_string(message_size)
        return error_code, error_message

    def bytes_remaining(self) -> int:
        """Return unread byte count."""
        return len(self._buffer) - self._offset

    def _parse_buffer(self, count: int) -> bytes:
        return self._parse_bytes(count)

    def _read_lob(self, size: int, lob_type: CUBRIDDataType) -> dict[str, object]:
        packed_lob_handle = self._parse_buffer(size)
        lob_reader = PacketReader(packed_lob_handle)
        _ = lob_reader._parse_int()
        lob_length = lob_reader._parse_long()
        locator_size = lob_reader._parse_int()
        file_locator = lob_reader._parse_null_terminated_string(locator_size)

        return {
            "lob_type": lob_type,
            "lob_length": lob_length,
            "file_locator": file_locator,
            "packed_lob_handle": packed_lob_handle,
        }
