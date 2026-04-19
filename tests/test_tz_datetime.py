from __future__ import annotations

import datetime
import struct

import pytest

from pycubrid.constants import CUBRIDDataType
from pycubrid.packet import PacketReader, _attach_timezone
from pycubrid.protocol import _TYPE_METHOD_NAMES, _resolve_reader


def _build_tz_payload(
    year: int,
    month: int,
    day: int,
    hour: int,
    minute: int,
    second: int,
    millisecond: int,
    timezone: bytes,
) -> bytes:
    return struct.pack(">7h", year, month, day, hour, minute, second, millisecond) + timezone


def test_timestamptz_with_iana_timezone() -> None:
    payload = _build_tz_payload(2026, 4, 19, 12, 34, 56, 0, b"Asia/Seoul\x00")
    reader = PacketReader(payload)

    value = reader._parse_timestamptz(len(payload))

    assert value == datetime.datetime(2026, 4, 19, 12, 34, 56, tzinfo=value.tzinfo)
    assert value.tzinfo is not None
    assert value.tzinfo.tzname(value) == "KST"


def test_timestamptz_with_utc_offset() -> None:
    payload = _build_tz_payload(2026, 4, 19, 12, 34, 56, 0, b"+09:00\x00")
    reader = PacketReader(payload)

    value = reader._parse_timestamptz(len(payload))

    assert value.utcoffset() == datetime.timedelta(hours=9)


def test_datetimetz_with_milliseconds_and_timezone() -> None:
    payload = _build_tz_payload(2026, 4, 19, 12, 34, 56, 789, b"Europe/Paris\x00")
    reader = PacketReader(payload)

    value = reader._parse_datetimetz(len(payload))

    assert value == datetime.datetime(2026, 4, 19, 12, 34, 56, 789000, tzinfo=value.tzinfo)
    assert value.tzinfo is not None


def test_tz_datetime_empty_timezone_falls_back_to_naive() -> None:
    payload = _build_tz_payload(2026, 4, 19, 12, 34, 56, 321, b"")
    reader = PacketReader(payload)

    value = reader._parse_timestamptz(len(payload))

    assert value == datetime.datetime(2026, 4, 19, 12, 34, 56, 321000)
    assert value.tzinfo is None


def test_tz_datetime_trailing_space_and_null_is_tolerated() -> None:
    payload = _build_tz_payload(2026, 4, 19, 12, 34, 56, 0, b"+09:00 \x00")
    reader = PacketReader(payload)

    value = reader._parse_timestamptz(len(payload))

    assert value.utcoffset() == datetime.timedelta(hours=9)


def test_protocol_type_dispatch_maps_timezone_types() -> None:
    payload = _build_tz_payload(2026, 4, 19, 12, 34, 56, 0, b"Asia/Seoul\x00")
    reader = PacketReader(payload)

    assert _TYPE_METHOD_NAMES[CUBRIDDataType.TIMESTAMPTZ] == "_parse_timestamptz"
    assert _TYPE_METHOD_NAMES[CUBRIDDataType.TIMESTAMPLTZ] == "_parse_timestamptz"
    assert _TYPE_METHOD_NAMES[CUBRIDDataType.DATETIMETZ] == "_parse_datetimetz"
    assert _TYPE_METHOD_NAMES[CUBRIDDataType.DATETIMELTZ] == "_parse_datetimetz"
    assert _resolve_reader(reader, CUBRIDDataType.TIMESTAMPTZ)(len(payload)).tzinfo is not None


def test_attach_timezone_offset_hh_only() -> None:
    dt = datetime.datetime(2026, 1, 1, 0, 0, 0)
    result = _attach_timezone(dt, "+09")
    assert result.utcoffset() == datetime.timedelta(hours=9)


def test_attach_timezone_offset_hh_mm_ss() -> None:
    dt = datetime.datetime(2026, 1, 1, 0, 0, 0)
    result = _attach_timezone(dt, "+05:30:15")
    assert result.utcoffset() == datetime.timedelta(hours=5, minutes=30, seconds=15)


def test_attach_timezone_invalid_raises_valueerror() -> None:
    dt = datetime.datetime(2026, 1, 1, 0, 0, 0)
    with pytest.raises(ValueError, match="Unrecognized CUBRID timezone"):
        _attach_timezone(dt, "Not/A/Real/Zone")


def test_timestamptz_invalid_tz_falls_back_to_naive() -> None:
    payload = _build_tz_payload(2026, 4, 19, 12, 0, 0, 0, b"Invalid/Zone\x00")
    reader = PacketReader(payload)

    value = reader._parse_timestamptz(len(payload))

    assert value.tzinfo is None
