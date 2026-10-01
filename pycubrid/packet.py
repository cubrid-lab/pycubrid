from __future__ import annotations

import datetime
import json
import struct
from decimal import Decimal, InvalidOperation
from typing import Any

from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from .constants import CUBRIDDataType, DataSize
from .exceptions import DataError


def _codec_label(encoding: str) -> str:
    """Return the codec name used in driver error messages."""
    return "UTF-8" if encoding == "utf-8" else encoding


# Python's EUC-KR codec encodes a Hangul syllable outside KS X 1001 (e.g. 똠,
# 뷁) as an 8-byte KS X 1001 "makeup" sequence starting with the Hangul filler
# U+3164 (A4 D4). CUBRID's euckr charset stores those bytes as four separate
# jamo, so such a character is treated as unencodable (#86).
_EUC_KR_FILLER = b"\xa4\xd4"


def _encode_text(value: str, encoding: str) -> tuple[bytes | None, int]:
    """Encode ``value``; return ``(bytes, -1)`` or ``(None, bad_position)``.

    Returns instead of raising so callers can raise ``DataError`` outside any
    ``except`` block: no chained exception then keeps the (possibly secret)
    text.
    """
    try:
        encoded = value.encode(encoding)
    except UnicodeEncodeError as exc:
        return None, exc.start
    if encoding == "euc_kr" and _EUC_KR_FILLER in encoded:
        for position, char in enumerate(value):
            if char != "\u3164" and len(char.encode(encoding)) > 2:
                return None, position
    return encoded, -1


def _decode_text(raw: bytes, encoding: str) -> str:
    """Strictly decode ``raw`` with ``encoding`` the way the CUBRID server reads it.

    CPython's ``euc_kr`` reads ``A4 D4`` (the Hangul filler U+3164) as the
    start of an 8-byte makeup sequence, so a stored lone U+3164 fails to
    decode and stored filler+jamo reads back as one syllable. CUBRID's euckr
    treats each pair as one KS X 1001 character, as ``cp949`` does; any
    CP949-only extension character is still rejected as invalid EUC-KR.
    """
    if encoding != "euc_kr" or _EUC_KR_FILLER not in raw:
        return raw.decode(encoding)
    text = raw.decode("cp949")
    offset = 0
    for char in text:
        width = len(char.encode("cp949"))
        if len(char.encode("euc_kr")) > 2:
            raise UnicodeDecodeError(
                "euc_kr", raw, offset, offset + width, "not a KS X 1001 character"
            )
        offset += width
    return text


def _unencodable_message(what: str, encoding: str, position: int) -> str:
    return (
        f"{what} cannot be encoded as {_codec_label(encoding)} "
        f"(unencodable character at position {position})"
    )


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


_HEADER_SIZE = DataSize.DATA_LENGTH + DataSize.CAS_INFO


def _attach_timezone(dt: datetime.datetime, tz_str: str) -> datetime.datetime:
    """Attach timezone info to a naive datetime from a CUBRID TZ string.

    Handles IANA region names (``Asia/Seoul``), UTC offsets in forms
    ``±HH``, ``±HH:MM``, ``±HH:MM:SS``, and region names followed by
    an abbreviation token (e.g. ``America/New_York EST``).

    An empty string returns ``dt`` unchanged. A nonempty token that
    cannot be resolved (an unknown region, or an offset that is malformed
    or outside ±24 hours) raises ``DataError`` instead of dropping the
    timezone (#413); the caller holds the complete reply, so the session
    stays usable.
    """
    import re

    tz_str = tz_str.strip()
    if not tz_str:
        return dt

    tokens = tz_str.split()
    timezone_token = tokens[0]

    if timezone_token[0] in "+-":
        # ±HH, ±HH:MM or ±HH:MM:SS
        offset_match = re.match(r"^([+-])(\d{2})(?::([0-5]\d))?(?::([0-5]\d))?$", timezone_token)
        try:
            if offset_match is None:
                raise ValueError("not in ±HH[:MM[:SS]] form")
            sign = 1 if offset_match.group(1) == "+" else -1
            hours = int(offset_match.group(2))
            minutes = int(offset_match.group(3) or "0")
            seconds = int(offset_match.group(4) or "0")
            offset = datetime.timedelta(hours=hours, minutes=minutes, seconds=seconds) * sign
            return dt.replace(tzinfo=datetime.timezone(offset))
        except ValueError as exc:
            raise DataError(
                f"cannot resolve CUBRID timezone offset {timezone_token!r}: {exc}"
            ) from exc

    try:
        zone = ZoneInfo(timezone_token)
    except (ZoneInfoNotFoundError, ValueError) as exc:
        raise DataError(
            f"cannot resolve CUBRID timezone {timezone_token!r}: it is not in the "
            "client's IANA time zone database (install the 'tzdata' package or "
            "update the system zoneinfo)"
        ) from exc

    aware = dt.replace(tzinfo=zone)
    # CUBRID sends the abbreviation in effect. When fold=0 does not carry it
    # but fold=1 does, take fold=1: the second occurrence of a repeated wall
    # time (01:30 EST, not EDT, as DST ends) or the post-transition offset of
    # a skipped one. A missing or unknown abbreviation, or one both folds
    # share (Europe/Moscow MSK on 2014-10-26), keeps fold=0.
    if len(tokens) > 1 and aware.tzname() != tokens[1]:
        later = aware.replace(fold=1)
        if later.tzname() == tokens[1] and later.utcoffset() != aware.utcoffset():
            return later
    return aware


class PacketWriter:
    def __init__(self, *, reserve_header: bool = True, encoding: str = "utf-8") -> None:
        self._header_size: int = _HEADER_SIZE if reserve_header else 0
        self._buffer: bytearray = bytearray(self._header_size)
        # Python codec for SQL text and credentials: the connection charset
        # (#86). The CAS applies no conversion, so these bytes must already be
        # in the database charset.
        self._encoding: str = encoding

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

    def _encode(self, value: str) -> bytes:
        """Encode request text with the connection codec, before any I/O.

        An unencodable character is a data problem: raise ``DataError``
        without echoing the text (it may hold bound values), so nothing of
        the request reaches the socket and the session stays usable.
        """
        encoded, position = _encode_text(value, self._encoding)
        if encoded is None:
            raise DataError(_unencodable_message("text", self._encoding, position))
        return encoded

    def _write_null_terminated_string(self, value: str) -> None:
        encoded = self._encode(value)
        self._write_int(len(encoded) + 1)
        self._write_bytes(encoded)
        self._write_byte(0)

    def _write_fixed_length_string(self, value: str, length: int, filler: int = 0) -> None:
        if length <= 0:
            return

        fixed = self._encode(value)
        if len(fixed) > length:
            # Keep whole characters: never send a truncated multibyte
            # sequence the CAS would read as a different name (#86).
            kept = bytearray()
            for char in value:
                encoded_char = char.encode(self._encoding)
                if len(kept) + len(encoded_char) > length:
                    break
                kept += encoded_char
            fixed = bytes(kept)
        self._write_bytes(fixed)
        if len(fixed) < length:
            self._write_filler(length - len(fixed), filler)

    def to_bytes(self) -> bytes:
        return bytes(self._buffer[self._header_size :])

    def finalize(self, cas_info: bytes | bytearray) -> bytes:
        payload_len = len(self._buffer) - _HEADER_SIZE
        struct.pack_into(">i", self._buffer, 0, payload_len)
        self._buffer[4:8] = cas_info
        return bytes(self._buffer)

    def __len__(self) -> int:
        return len(self._buffer) - self._header_size


_COLLECTION_ELEMENT_METHOD_NAMES: dict[int, str] = {
    CUBRIDDataType.CHAR: "_parse_text_value",
    CUBRIDDataType.STRING: "_parse_text_value",
    CUBRIDDataType.NCHAR: "_parse_text_value",
    CUBRIDDataType.VARNCHAR: "_parse_text_value",
    CUBRIDDataType.ENUM: "_parse_text_value",
    CUBRIDDataType.SHORT: "_parse_short",
    CUBRIDDataType.INT: "_parse_int",
    CUBRIDDataType.BIGINT: "_parse_long",
    CUBRIDDataType.FLOAT: "_parse_float",
    CUBRIDDataType.DOUBLE: "_parse_double",
    CUBRIDDataType.MONETARY: "_parse_double",
    CUBRIDDataType.NUMERIC: "_parse_numeric",
    CUBRIDDataType.DATE: "_parse_date",
    CUBRIDDataType.TIME: "_parse_time",
    CUBRIDDataType.DATETIME: "_parse_datetime",
    CUBRIDDataType.TIMESTAMP: "_parse_timestamp",
    CUBRIDDataType.OBJECT: "_parse_object",
    CUBRIDDataType.BIT: "_parse_bytes",
    CUBRIDDataType.VARBIT: "_parse_bytes",
    CUBRIDDataType.BLOB: "read_blob",
    CUBRIDDataType.CLOB: "read_clob",
}


def _unrepresentable_temporal(
    type_name: str, fields: tuple[int, ...], exc: ValueError, size_ok: bool
) -> DataError | ValueError:
    """Classify a temporal value Python cannot hold (#512).

    CUBRID stores zero dates such as ``DATE'0000-00-00'``, but ``datetime``
    has no year 0. When the field has the declared size of its type, this is a
    data problem: return ``DataError`` so the connection stays usable, as for
    invalid UTF-8 (#492); ``_parse_row_data`` still checks the rest of the
    reply before re-raising it. A field of the wrong size means the decoder
    read outside it, so return a ``ValueError`` that the connection reports as
    a malformed response (#383).
    """
    if not size_ok:
        return ValueError(f"malformed CUBRID {type_name} value: wrong field size")
    return DataError(f"CUBRID {type_name} value {fields!r} cannot be represented in Python: {exc}")


def _out_of_bounds(count: int, offset: int, buffer_size: int) -> ValueError:
    """A length that cannot be read from the reply: a malformed response (#383)."""
    if count < 0:
        return ValueError(f"negative length {count} in broker reply")
    return ValueError(
        f"read of {count} bytes at offset {offset} runs past the end of the "
        f"broker reply ({buffer_size - offset} bytes left)"
    )


class PacketReader:
    """Read CAS values from one complete broker reply.

    Every read that consumes bytes stays inside the reply (#383): a
    length-prefixed read checks ``0 <= length <= bytes_remaining()`` before it
    moves and raises ``ValueError`` otherwise, and a fixed-width read past the
    end raises ``struct.error`` or ``IndexError``. The connection reports all
    three as ``OperationalError("malformed response from broker")`` and closes,
    while ``DataError`` stays for a complete reply holding a value Python cannot
    represent (#492, #512). A failed read leaves the offset where it was.
    Text readers treat a non-positive length as empty without moving.

    Bytes left unread after a reply is parsed are not checked: only values whose
    size the protocol states exactly (a length word, a collection's size) must
    match it.
    """

    __slots__ = ("_buffer", "_offset", "_decode_collections", "_json_deserializer", "_encoding")

    def __init__(
        self,
        data: bytes | bytearray,
        *,
        decode_collections: bool = False,
        json_deserializer: Any = None,
        encoding: str = "utf-8",
    ) -> None:
        self._buffer: memoryview = memoryview(data)
        self._offset: int = 0
        self._decode_collections: bool = decode_collections
        self._json_deserializer: Any = json_deserializer
        # Connection charset (#86) for character values, metadata names,
        # error text and LOB locators (lenient: they embed the table name).
        # Protocol text (NUMERIC, timezone names, version) and JSON stay UTF-8.
        self._encoding: str = encoding

    def mark(self) -> int:
        """Return the current offset for a subsequent bounds-only re-walk."""
        return self._offset

    def seek(self, position: int) -> None:
        """Restore an offset inside this reply; rejection leaves it unchanged."""
        if (
            isinstance(position, bool)
            or not isinstance(position, int)
            or not 0 <= position <= len(self._buffer)
        ):
            raise ValueError("invalid position in broker reply")
        self._offset = position

    def _parse_byte(self) -> int:
        value = self._buffer[self._offset]
        self._offset += DataSize.BYTE
        return value

    def _parse_short(self, size: int = 0) -> int:
        """Parse a 2-byte short. ``size`` is ignored (uniform dispatch signature)."""
        value: int = _STRUCT_SHORT.unpack_from(self._buffer, self._offset)[0]
        self._offset += DataSize.SHORT
        return value

    def _parse_int(self, size: int = 0) -> int:
        """Parse a 4-byte int. ``size`` is ignored (uniform dispatch signature)."""
        value: int = _STRUCT_INT.unpack_from(self._buffer, self._offset)[0]
        self._offset += DataSize.INT
        return value

    def _parse_long(self, size: int = 0) -> int:
        """Parse an 8-byte long. ``size`` is ignored (uniform dispatch signature)."""
        value: int = _STRUCT_LONG.unpack_from(self._buffer, self._offset)[0]
        self._offset += DataSize.LONG
        return value

    def _parse_float(self, size: int = 0) -> float:
        """Parse a 4-byte float. ``size`` is ignored (uniform dispatch signature)."""
        value: float = _STRUCT_FLOAT.unpack_from(self._buffer, self._offset)[0]
        self._offset += DataSize.FLOAT
        return value

    def _parse_double(self, size: int = 0) -> float:
        """Parse an 8-byte double. ``size`` is ignored (uniform dispatch signature)."""
        value: float = _STRUCT_DOUBLE.unpack_from(self._buffer, self._offset)[0]
        self._offset += DataSize.DOUBLE
        return value

    def _parse_bytes(self, count: int) -> bytes:
        start = self._offset
        end = start + count
        if count < 0 or end > len(self._buffer):
            raise _out_of_bounds(count, start, len(self._buffer))
        self._offset = end
        return bytes(self._buffer[start:end])

    def _skip_bytes(self, count: int) -> None:
        end = self._offset + count
        if count < 0 or end > len(self._buffer):
            raise _out_of_bounds(count, self._offset, len(self._buffer))
        self._offset = end

    def _parse_null_terminated_string(self, length: int) -> str:
        """Decode protocol text (NUMERIC, timezone names, version) as UTF-8."""
        if length <= 0:
            return ""

        start = self._offset
        end = start + length
        if end > len(self._buffer):
            raise _out_of_bounds(length, start, len(self._buffer))
        self._offset = end
        if self._buffer[end - 1] == 0:
            return bytes(self._buffer[start : end - 1]).decode("utf-8")
        return bytes(self._buffer[start:end]).decode("utf-8")

    def _parse_charset_text(self, length: int, what: str, encoding: str | None = None) -> str:
        """Decode strictly with the connection codec (or ``encoding``).

        The caller already holds the complete response, so undecodable bytes
        are a data problem, not a framing problem: raise ``DataError`` naming
        the codec and leave the connection usable (#492, #86).
        """
        if length <= 0:
            return ""
        codec = self._encoding if encoding is None else encoding
        start = self._offset
        end = start + length
        if end > len(self._buffer):
            raise _out_of_bounds(length, start, len(self._buffer))
        self._offset = end
        if self._buffer[end - 1] == 0:
            end -= 1
        try:
            return _decode_text(bytes(self._buffer[start:end]), codec)
        except UnicodeDecodeError as exc:
            raise DataError(
                f"{what} is not valid {_codec_label(codec)} (invalid byte at offset {exc.start})"
            ) from exc

    def _parse_text_value(self, length: int) -> str:
        """Decode a character column value with the connection codec (hot path)."""
        if length <= 0:
            return ""
        start = self._offset
        end = start + length
        if end > len(self._buffer):
            raise _out_of_bounds(length, start, len(self._buffer))
        self._offset = end
        if self._buffer[end - 1] == 0:
            end -= 1
        try:
            return _decode_text(bytes(self._buffer[start:end]), self._encoding)
        except UnicodeDecodeError as exc:
            raise DataError(
                f"column value is not valid {_codec_label(self._encoding)} "
                f"(invalid byte at offset {exc.start})"
            ) from exc

    def _parse_metadata_text(self, length: int) -> str:
        """Decode a column/table name or default value with the connection codec."""
        return self._parse_charset_text(length, "column metadata")

    def _parse_date(self, size: int = 0) -> datetime.date:
        year, month, day = _STRUCT_3H.unpack_from(self._buffer, self._offset)
        self._offset += 6
        try:
            return datetime.date(year, month, day)
        except ValueError as exc:
            raise _unrepresentable_temporal("DATE", (year, month, day), exc, size == 6) from exc

    def _parse_time(self, size: int = 0) -> datetime.time:
        hour, minute, second = _STRUCT_3H.unpack_from(self._buffer, self._offset)
        self._offset += 6
        try:
            return datetime.time(hour, minute, second)
        except ValueError as exc:
            raise _unrepresentable_temporal("TIME", (hour, minute, second), exc, size == 6) from exc

    def _parse_datetime(self, size: int = 0) -> datetime.datetime:
        y, mo, d, h, mi, s, ms = _STRUCT_7H.unpack_from(self._buffer, self._offset)
        self._offset += 14
        try:
            return datetime.datetime(y, mo, d, h, mi, s, ms * 1000)
        except ValueError as exc:
            raise _unrepresentable_temporal(
                "DATETIME", (y, mo, d, h, mi, s, ms), exc, size == 14
            ) from exc

    def _parse_timestamp(self, size: int = 0) -> datetime.datetime:
        y, mo, d, h, mi, s = _STRUCT_6H.unpack_from(self._buffer, self._offset)
        self._offset += 12
        try:
            return datetime.datetime(y, mo, d, h, mi, s, 0)
        except ValueError as exc:
            raise _unrepresentable_temporal(
                "TIMESTAMP", (y, mo, d, h, mi, s), exc, size == 12
            ) from exc

    def _parse_timestamptz(self, size: int) -> datetime.datetime:
        # TIMESTAMPTZ / TIMESTAMPLTZ are second-precision: 6 shorts (12 bytes,
        # no millisecond field) followed by the timezone string. Reading the
        # 7-short / 14-byte DATETIMETZ layout here over-read the first 2 bytes
        # of the timezone string as ``ms`` and then overflowed ``ms * 1000``
        # (raising "microsecond must be in 0..999999"), which surfaced as
        # "malformed response from broker" (#289).
        y, mo, d, h, mi, s = _STRUCT_6H.unpack_from(self._buffer, self._offset)
        self._offset += 12
        try:
            dt = datetime.datetime(y, mo, d, h, mi, s, 0)
        except ValueError as exc:
            raise _unrepresentable_temporal(
                "TIMESTAMPTZ/TIMESTAMPLTZ", (y, mo, d, h, mi, s), exc, size >= 12
            ) from exc
        return self._attach_timezone_suffix(dt, size - 12)

    def _parse_datetimetz(self, size: int) -> datetime.datetime:
        # DATETIMETZ / DATETIMELTZ carry a millisecond field: 7 shorts
        # (14 bytes) followed by the timezone string.
        y, mo, d, h, mi, s, ms = _STRUCT_7H.unpack_from(self._buffer, self._offset)
        self._offset += 14
        try:
            dt = datetime.datetime(y, mo, d, h, mi, s, ms * 1000)
        except ValueError as exc:
            raise _unrepresentable_temporal(
                "DATETIMETZ/DATETIMELTZ", (y, mo, d, h, mi, s, ms), exc, size >= 14
            ) from exc
        return self._attach_timezone_suffix(dt, size - 14)

    def _attach_timezone_suffix(
        self, dt: datetime.datetime, tz_bytes_len: int
    ) -> datetime.datetime:
        if tz_bytes_len > 0:
            tz_str = self._parse_null_terminated_string(tz_bytes_len)
        else:
            tz_str = ""
        return _attach_timezone(dt, tz_str)

    def _parse_numeric(self, size: int) -> Decimal:
        value = self._parse_null_terminated_string(size)
        try:
            return Decimal(value)
        except InvalidOperation as exc:
            raise ValueError(f"malformed NUMERIC value: {value!r}") from exc

    def _parse_json(self, size: int) -> Any:
        # The CAS always sends JSON as UTF-8, whatever the database charset.
        value = self._parse_charset_text(size, "JSON value", "utf-8")
        if self._json_deserializer is None:
            return value
        if self._json_deserializer is json.loads:
            # Invalid JSON text in a complete reply is a data problem, not a
            # framing problem (#543): raise ``DataError`` so ``_parse_row_data``
            # applies the same complete-reply check as for invalid UTF-8
            # (#492) and unrepresentable temporal values (#512), and the
            # connection stays usable. A caller-supplied ``json_deserializer``
            # is not wrapped here: its failures are between the application
            # and its own deserializer.
            try:
                return json.loads(value)
            except json.JSONDecodeError as exc:
                raise DataError(f"JSON value is not valid JSON: {exc}") from exc
        return self._json_deserializer(value)

    def _parse_collection(self, size: int) -> object:
        # ``element_count`` below is bounded by ``size`` (validated upstream by
        # ``_validate_data_length`` to max 256 MB). A malicious ``element_count``
        # cannot cause unbounded iteration: once ``_offset`` exceeds the buffer,
        # ``_parse_int()`` raises ``struct.error``. See Oracle review (GAP-3).
        if not self._decode_collections:
            return self._parse_bytes(size)

        start_offset = self._offset
        element_type = self._parse_byte()
        element_count = self._parse_int()
        if element_type == CUBRIDDataType.NULL:
            # CUBRID 10.2/11.4 send element type NULL for an empty collection
            # and when every element is SQL NULL: the count is followed by one
            # ``-1`` length word per element and no payload (#483). Check the
            # exact size before allocating.
            if (
                element_count < 0
                or element_count * DataSize.INT != size - DataSize.BYTE - DataSize.INT
            ):
                raise ValueError("malformed NULL-only collection: count does not match size")
            for _ in range(element_count):
                if self._parse_int() not in (-1, 0):
                    raise ValueError("malformed NULL-only collection: invalid element length")
            return [None] * element_count
        if element_count < 0:
            raise ValueError("negative collection element count")
        if element_type in (
            CUBRIDDataType.SET,
            CUBRIDDataType.MULTISET,
            CUBRIDDataType.SEQUENCE,
        ):
            self._offset = start_offset
            return self._parse_bytes(size)

        method_name = _COLLECTION_ELEMENT_METHOD_NAMES.get(element_type)
        if method_name is None:
            self._offset = start_offset
            return self._parse_bytes(size)

        parser = getattr(self, method_name)
        values: list[object] = []
        for index in range(element_count):
            element_size = self._parse_int()
            if element_size <= 0:
                values.append(None)
                continue
            element_start = self._offset
            try:
                values.append(parser(element_size))
            except DataError:
                # Report an unrepresentable element (#492, #512) only when the
                # remaining elements fit the collection's declared size.
                self._offset = element_start + element_size
                for _ in range(index + 1, element_count):
                    element_size = self._parse_int()
                    if element_size > 0:
                        self._offset += element_size
                if self._offset > start_offset + size:
                    raise ValueError("malformed collection: elements exceed its size") from None
                if self._offset != start_offset + size:
                    raise ValueError(
                        "malformed collection: elements do not match its size"
                    ) from None
                raise
        if self._offset != start_offset + size:
            # The elements must fill the declared size exactly, or the next
            # value in the row would be read from the wrong place (#383).
            raise ValueError("malformed collection: elements do not match its size")
        return values

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
        error_message = self._parse_lenient_text(message_size)
        return error_code, error_message

    def _parse_lenient_text(self, length: int) -> str:
        """Decode server text (error messages, LOB locators) with the connection
        codec, replacing undecodable bytes.

        CUBRID can cut a message mid-character (e.g. when echoing an
        oversized value), and the real error must still surface (#492).
        """
        if length <= 0:
            return ""
        start = self._offset
        end = start + length
        if end > len(self._buffer):
            raise _out_of_bounds(length, start, len(self._buffer))
        self._offset = end
        if self._buffer[end - 1] == 0:
            end -= 1
        return bytes(self._buffer[start:end]).decode(self._encoding, errors="replace")

    def bytes_remaining(self) -> int:
        """Return unread byte count."""
        return len(self._buffer) - self._offset

    def _parse_buffer(self, count: int) -> bytes:
        return self._parse_bytes(count)

    def _read_lob(self, size: int, lob_type: CUBRIDDataType) -> dict[str, object]:
        packed_lob_handle = self._parse_buffer(size)
        lob_reader = PacketReader(packed_lob_handle, encoding=self._encoding)
        _ = lob_reader._parse_int()
        lob_length = lob_reader._parse_long()
        locator_size = lob_reader._parse_int()
        # The locator is a server file path that embeds the table name, in the
        # database charset. It is informational (``packed_lob_handle`` is what
        # goes back to the server), so decode it leniently: a mismatch must not
        # fail the fetch (#86).
        file_locator = lob_reader._parse_lenient_text(locator_size)

        return {
            "lob_type": lob_type,
            "lob_length": lob_length,
            "file_locator": file_locator,
            "packed_lob_handle": packed_lob_handle,
        }
