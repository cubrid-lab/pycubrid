from __future__ import annotations

import datetime
import struct
import sys
import zoneinfo
from collections.abc import Iterator
from unittest.mock import MagicMock

import pytest

from pycubrid.constants import CUBRIDDataType
from pycubrid.exceptions import DataError
from pycubrid.packet import PacketReader, _attach_timezone
from pycubrid.protocol import _TYPE_METHOD_NAMES, PrepareAndExecutePacket, _resolve_reader
from tests.test_connection import socket_queue  # noqa: F401
from tests.test_invalid_utf8_response import (
    _async_connection_with_reply,
    _connection_with_reply,
)
from tests.test_json_decode import _build_select_response


def _build_timestamptz_payload(
    year: int,
    month: int,
    day: int,
    hour: int,
    minute: int,
    second: int,
    timezone: bytes,
) -> bytes:
    # TIMESTAMPTZ / TIMESTAMPLTZ: second-precision, 6 shorts (12 bytes,
    # no millisecond field) followed by the timezone string.
    return struct.pack(">6h", year, month, day, hour, minute, second) + timezone


def _build_datetimetz_payload(
    year: int,
    month: int,
    day: int,
    hour: int,
    minute: int,
    second: int,
    millisecond: int,
    timezone: bytes,
) -> bytes:
    # DATETIMETZ / DATETIMELTZ: 7 shorts (14 bytes, with millisecond)
    # followed by the timezone string.
    return struct.pack(">7h", year, month, day, hour, minute, second, millisecond) + timezone


def test_timestamptz_with_iana_timezone() -> None:
    payload = _build_timestamptz_payload(2026, 4, 19, 12, 34, 56, b"Asia/Seoul\x00")
    reader = PacketReader(payload)

    value = reader._parse_timestamptz(len(payload))

    assert value == datetime.datetime(2026, 4, 19, 12, 34, 56, tzinfo=value.tzinfo)
    assert value.tzinfo is not None
    assert value.tzinfo.tzname(value) == "KST"
    assert reader.bytes_remaining() == 0


def test_timestamptz_with_utc_offset() -> None:
    payload = _build_timestamptz_payload(2026, 4, 19, 12, 34, 56, b"+09:00\x00")
    reader = PacketReader(payload)

    value = reader._parse_timestamptz(len(payload))

    assert value.utcoffset() == datetime.timedelta(hours=9)


def test_datetimetz_with_milliseconds_and_timezone() -> None:
    payload = _build_datetimetz_payload(2026, 4, 19, 12, 34, 56, 789, b"Europe/Paris\x00")
    reader = PacketReader(payload)

    value = reader._parse_datetimetz(len(payload))

    assert value == datetime.datetime(2026, 4, 19, 12, 34, 56, 789000, tzinfo=value.tzinfo)
    assert value.tzinfo is not None


def test_timestamptz_empty_timezone_falls_back_to_naive() -> None:
    payload = _build_timestamptz_payload(2026, 4, 19, 12, 34, 56, b"")
    reader = PacketReader(payload)

    value = reader._parse_timestamptz(len(payload))

    assert value == datetime.datetime(2026, 4, 19, 12, 34, 56)
    assert value.tzinfo is None


def test_datetimetz_empty_timezone_falls_back_to_naive() -> None:
    payload = _build_datetimetz_payload(2026, 4, 19, 12, 34, 56, 321, b"")
    reader = PacketReader(payload)

    value = reader._parse_datetimetz(len(payload))

    assert value == datetime.datetime(2026, 4, 19, 12, 34, 56, 321000)
    assert value.tzinfo is None


def test_timestamptz_second_precision_does_not_overflow_microseconds() -> None:
    # Regression for #289: the timezone string's leading bytes ("As" =
    # 0x4173 = 16755) were previously misread as a millisecond field and
    # 16755 * 1000 overflowed "microsecond must be in 0..999999", surfacing
    # as "malformed response from broker". A second-precision parse must not
    # consume any millisecond field.
    payload = _build_timestamptz_payload(2026, 1, 15, 10, 30, 0, b"Asia/Seoul\x00")
    reader = PacketReader(payload)

    value = reader._parse_timestamptz(len(payload))

    assert value == datetime.datetime(2026, 1, 15, 10, 30, 0, tzinfo=value.tzinfo)
    assert value.microsecond == 0
    assert value.tzinfo is not None
    assert value.tzinfo.tzname(value) == "KST"


def test_tz_datetime_trailing_space_and_null_is_tolerated() -> None:
    payload = _build_timestamptz_payload(2026, 4, 19, 12, 34, 56, b"+09:00 \x00")
    reader = PacketReader(payload)

    value = reader._parse_timestamptz(len(payload))

    assert value.utcoffset() == datetime.timedelta(hours=9)


def test_protocol_type_dispatch_maps_timezone_types() -> None:
    payload = _build_timestamptz_payload(2026, 4, 19, 12, 34, 56, b"Asia/Seoul\x00")
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


def test_attach_timezone_invalid_raises_data_error() -> None:
    dt = datetime.datetime(2026, 1, 1, 0, 0, 0)
    with pytest.raises(DataError, match="cannot resolve CUBRID timezone 'Not/A/Real/Zone'"):
        _attach_timezone(dt, "Not/A/Real/Zone")


def test_attach_timezone_malformed_key_raises_data_error() -> None:
    # ZoneInfo raises ValueError (not KeyError) for keys like this one.
    dt = datetime.datetime(2026, 1, 1, 0, 0, 0)
    with pytest.raises(DataError, match="install the 'tzdata' package"):
        _attach_timezone(dt, "../etc/passwd")


@pytest.mark.parametrize(
    "timezone", [b"Not/A_Zone\x00", b"Invalid/Zone KST\x00"], ids=["bare", "with-abbrev"]
)
def test_timestamptz_unknown_zone_raises_data_error(timezone: bytes) -> None:
    # #413: a nonempty token is never silently dropped to a naive datetime.
    payload = _build_timestamptz_payload(2026, 4, 19, 12, 0, 0, timezone)
    reader = PacketReader(payload)

    with pytest.raises(DataError, match="cannot resolve CUBRID timezone"):
        reader._parse_timestamptz(len(payload))


def test_datetimetz_unknown_zone_raises_data_error() -> None:
    payload = _build_datetimetz_payload(2026, 4, 19, 12, 0, 0, 5, b"Not/A_Zone\x00")
    reader = PacketReader(payload)

    with pytest.raises(DataError, match="'Not/A_Zone'"):
        reader._parse_datetimetz(len(payload))


# --- captured CUBRID 11.4 / 10.2 wire values (identical on both) -------------

# SELECT TIMESTAMPTZ'2026-01-15 10:30:00 Asia/Seoul'
WIRE_TIMESTAMPTZ_SEOUL = b"\x07\xea\x00\x01\x00\x0f\x00\n\x00\x1e\x00\x00Asia/Seoul KST\x00"
# SELECT TIMESTAMPTZ'2026-01-15 10:30:00 +05:30'
WIRE_TIMESTAMPTZ_OFFSET = b"\x07\xea\x00\x01\x00\x0f\x00\n\x00\x1e\x00\x00+05:30\x00"
# SELECT TIMESTAMPLTZ'2026-01-15 10:30:00' (session timezone UTC)
WIRE_TIMESTAMPLTZ_UTC = b"\x07\xea\x00\x01\x00\x0f\x00\n\x00\x1e\x00\x00UTC UTC\x00"
# SELECT DATETIMELTZ'2026-01-15 10:30:00.500' (session timezone UTC)
WIRE_DATETIMELTZ_UTC = b"\x07\xea\x00\x01\x00\x0f\x00\n\x00\x1e\x00\x00\x01\xf4UTC UTC\x00"
# SELECT DATETIMETZ'2026-11-01 01:30:00.250 America/New_York EDT' / ... EST'
WIRE_DATETIMETZ_NY_EDT = (
    b"\x07\xea\x00\x0b\x00\x01\x00\x01\x00\x1e\x00\x00\x00\xfaAmerica/New_York EDT\x00"
)
WIRE_DATETIMETZ_NY_EST = (
    b"\x07\xea\x00\x0b\x00\x01\x00\x01\x00\x1e\x00\x00\x00\xfaAmerica/New_York EST\x00"
)
# SELECT TIMESTAMPTZ'2026-11-01 01:30:00 America/New_York EST'
WIRE_TIMESTAMPTZ_NY_EST = (
    b"\x07\xea\x00\x0b\x00\x01\x00\x01\x00\x1e\x00\x00America/New_York EST\x00"
)


@pytest.fixture
def no_tz_database(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Hide both the system zoneinfo and the ``tzdata`` package."""
    monkeypatch.setitem(sys.modules, "tzdata", None)
    zoneinfo.reset_tzpath(to=[])
    zoneinfo.ZoneInfo.clear_cache()
    try:
        yield
    finally:
        zoneinfo.reset_tzpath()
        zoneinfo.ZoneInfo.clear_cache()


def _decode(method: str, wire: bytes) -> datetime.datetime:
    reader = PacketReader(wire)
    value: datetime.datetime = getattr(reader, method)(len(wire))
    assert reader.bytes_remaining() == 0
    return value


def test_captured_region_and_offset_values() -> None:
    seoul = _decode("_parse_timestamptz", WIRE_TIMESTAMPTZ_SEOUL)
    assert seoul == datetime.datetime(2026, 1, 15, 1, 30, tzinfo=datetime.timezone.utc)
    assert seoul.tzinfo == zoneinfo.ZoneInfo("Asia/Seoul")

    offset = _decode("_parse_timestamptz", WIRE_TIMESTAMPTZ_OFFSET)
    assert offset.utcoffset() == datetime.timedelta(hours=5, minutes=30)

    ltz = _decode("_parse_timestamptz", WIRE_TIMESTAMPLTZ_UTC)
    assert ltz.tzinfo == zoneinfo.ZoneInfo("UTC")

    dltz = _decode("_parse_datetimetz", WIRE_DATETIMELTZ_UTC)
    assert dltz == datetime.datetime(2026, 1, 15, 10, 30, 0, 500000, tzinfo=datetime.timezone.utc)


def test_captured_ambiguous_hour_uses_abbreviation() -> None:
    # 01:30 on 2026-11-01 happens twice in New York; CUBRID names which one.
    edt = _decode("_parse_datetimetz", WIRE_DATETIMETZ_NY_EDT)
    est = _decode("_parse_datetimetz", WIRE_DATETIMETZ_NY_EST)
    ts_est = _decode("_parse_timestamptz", WIRE_TIMESTAMPTZ_NY_EST)

    assert (edt.fold, edt.utcoffset(), edt.tzname()) == (0, datetime.timedelta(hours=-4), "EDT")
    assert (est.fold, est.utcoffset(), est.tzname()) == (1, datetime.timedelta(hours=-5), "EST")
    assert ts_est.utcoffset() == datetime.timedelta(hours=-5)
    utc = datetime.timezone.utc
    assert est.astimezone(utc) - edt.astimezone(utc) == datetime.timedelta(hours=1)
    assert est.microsecond == 250000


def test_abbreviation_does_not_change_unambiguous_time() -> None:
    dt = datetime.datetime(2026, 1, 15, 10, 30)
    for abbrev in ("EST", "EDT", "XYZ"):
        value = _attach_timezone(dt, f"America/New_York {abbrev}")
        assert (value.fold, value.utcoffset()) == (0, datetime.timedelta(hours=-5))


def test_unknown_abbreviation_in_ambiguous_hour_keeps_first_occurrence() -> None:
    value = _attach_timezone(datetime.datetime(2026, 11, 1, 1, 30), "America/New_York XYZ")
    assert (value.fold, value.utcoffset()) == (0, datetime.timedelta(hours=-4))


@pytest.mark.usefixtures("no_tz_database")
@pytest.mark.parametrize(
    ("method", "wire", "token"),
    [
        ("_parse_timestamptz", WIRE_TIMESTAMPTZ_SEOUL, "Asia/Seoul"),
        ("_parse_timestamptz", WIRE_TIMESTAMPLTZ_UTC, "UTC"),
        ("_parse_datetimetz", WIRE_DATETIMELTZ_UTC, "UTC"),
        ("_parse_datetimetz", WIRE_DATETIMETZ_NY_EST, "America/New_York"),
    ],
)
def test_missing_tz_database_raises_data_error(method: str, wire: bytes, token: str) -> None:
    with pytest.raises(DataError, match=f"'{token}'.*install the 'tzdata' package"):
        _decode(method, wire)


@pytest.mark.usefixtures("no_tz_database")
def test_missing_tz_database_keeps_offsets_and_empty_suffix() -> None:
    assert _decode("_parse_timestamptz", WIRE_TIMESTAMPTZ_OFFSET).utcoffset() == (
        datetime.timedelta(hours=5, minutes=30)
    )
    empty = _decode("_parse_timestamptz", WIRE_TIMESTAMPTZ_OFFSET[:12])
    assert empty.tzinfo is None


# --- connection level: the session survives the DataError --------------------


def _tz_select_body(wire: bytes) -> bytes:
    return _build_select_response([(CUBRIDDataType.TIMESTAMPTZ, "v")], [wire])


@pytest.mark.usefixtures("no_tz_database")
def test_sync_unresolved_zone_keeps_connection(
    socket_queue: list[MagicMock],  # noqa: F811
) -> None:
    conn, _ = _connection_with_reply(socket_queue, _tz_select_body(WIRE_TIMESTAMPTZ_SEOUL))
    with pytest.raises(DataError, match="'Asia/Seoul'"):
        conn._send_and_receive(PrepareAndExecutePacket("SELECT v FROM t"))
    assert conn._connected is True
    assert conn._socket is not None


@pytest.mark.asyncio
@pytest.mark.usefixtures("no_tz_database")
async def test_async_unresolved_zone_keeps_connection() -> None:
    conn = _async_connection_with_reply(_tz_select_body(WIRE_TIMESTAMPTZ_SEOUL))
    with pytest.raises(DataError, match="'Asia/Seoul'"):
        await conn._send_and_receive(PrepareAndExecutePacket("SELECT v FROM t"))
    assert conn._connected is True
    assert conn._writer is not None
