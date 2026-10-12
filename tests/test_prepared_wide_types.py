"""Wider typed prepared binds: BIGINT, DOUBLE, NUMERIC, temporal, VARBIT (#840)."""

from __future__ import annotations

import datetime
import enum
import struct
from decimal import Decimal
from typing import Any
from zoneinfo import ZoneInfo

import pytest

from pycubrid import _cursor_common, _prepared_binding, protocol
from pycubrid.constants import CUBRIDDataType, CUBRIDStatementType
from pycubrid.exceptions import DataError, NotSupportedError, ProgrammingError
from pycubrid.protocol import _encode_prepared_scalar, _PreparedScalar

_CAS_INFO = b"\x01\x00\x00\x00"
_T = struct.Struct(">7h")


def _pair(value: Any) -> tuple[int, bytes]:
    scalar = _encode_prepared_scalar(value)
    return int(scalar.type_code), scalar.payload


# -- encoding per type ------------------------------------------------------


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (True, (8, struct.pack(">i", 1))),
        (False, (8, struct.pack(">i", 0))),
        (2**31 - 1, (8, struct.pack(">i", 2**31 - 1))),
        (2**31, (21, struct.pack(">q", 2**31))),
        (-(2**31) - 1, (21, struct.pack(">q", -(2**31) - 1))),
        (2**63 - 1, (21, struct.pack(">q", 2**63 - 1))),
        (-(2**63), (21, struct.pack(">q", -(2**63)))),
        (2**63, (7, b"9223372036854775808\x00")),
        (-(10**38) + 1, (7, b"-" + b"9" * 38 + b"\x00")),
        (1.5, (12, struct.pack(">d", 1.5))),
        (-0.0, (12, struct.pack(">d", -0.0))),
        (1e300, (12, struct.pack(">d", 1e300))),
    ],
    ids=repr,
)
def test_numeric_python_types_encode_exact_pairs(value: Any, expected: tuple[int, bytes]) -> None:
    assert _pair(value) == expected


@pytest.mark.parametrize(
    ("value", "text"),
    [
        (Decimal("123.4500"), b"123.4500"),
        (Decimal("-123.45"), b"-123.45"),
        (Decimal("0.5"), b".5"),
        (Decimal("-0.05"), b"-.05"),
        (Decimal("1E-7"), b".0000001"),
        (Decimal("0E-3"), b".000"),
        (Decimal("0E+5"), b"0"),
        (Decimal("-0"), b"-0"),
        (Decimal("1E+3"), b"1000"),
        (Decimal("9" * 38), b"9" * 38),
        (Decimal("0." + "1" * 38), b"." + b"1" * 38),
    ],
    ids=str,
)
def test_decimal_is_fixed_point_numeric_text(value: Decimal, text: bytes) -> None:
    # The broker counts every byte but the sign and point as precision, so a
    # lone leading zero is dropped to keep the literal path's 38-digit limit.
    assert _pair(value) == (7, text + b"\x00")


@pytest.mark.parametrize(
    ("value", "error"),
    [
        (Decimal("NaN"), ProgrammingError),
        (Decimal("sNaN"), ProgrammingError),
        (Decimal("Infinity"), ProgrammingError),
        (Decimal("1" * 39), DataError),
        (Decimal("1E+38"), DataError),
        (Decimal("1E-39"), DataError),
        (float("nan"), ProgrammingError),
        (float("inf"), ProgrammingError),
        (float("-inf"), ProgrammingError),
        (10**38, DataError),
        (-(10**38), DataError),
        (10**5000, DataError),
    ],
    ids=lambda value: type(value).__name__,
)
def test_numeric_values_the_literal_path_rejects_are_rejected(
    value: Any, error: type[Exception]
) -> None:
    with pytest.raises(error):
        _encode_prepared_scalar(value)


def test_temporal_types_use_the_seven_short_layout() -> None:
    assert _pair(datetime.date(1, 2, 3)) == (13, _T.pack(1, 2, 3, 0, 0, 0, 0))
    # TIME is read from offset 6; microseconds and tzinfo are dropped.
    tz = datetime.timezone(datetime.timedelta(hours=9))
    assert _pair(datetime.time(23, 59, 58, 999999, tzinfo=tz)) == (
        14,
        _T.pack(0, 0, 0, 23, 59, 58, 0),
    )
    # Microseconds truncate to milliseconds, as in the literal path.
    assert _pair(datetime.datetime(9999, 12, 31, 1, 2, 3, 456789)) == (
        22,
        _T.pack(9999, 12, 31, 1, 2, 3, 456),
    )


@pytest.mark.parametrize(
    ("value", "zone"),
    [
        (
            datetime.datetime(2024, 1, 2, 3, 4, 5, 6000, tzinfo=ZoneInfo("Asia/Seoul")),
            b"Asia/Seoul",
        ),
        (
            datetime.datetime(2024, 1, 2, 3, 4, 5, 6000, tzinfo=datetime.UTC),
            b"+00:00",
        ),
        (
            datetime.datetime(
                2024, 1, 2, 3, 4, 5, 6000, tzinfo=datetime.timezone(-datetime.timedelta(hours=5.5))
            ),
            b"-05:30",
        ),
        # fold=1 in a repeated hour names its occurrence, as the literal path.
        (
            datetime.datetime(2024, 1, 2, 3, 4, 5, 6000, tzinfo=ZoneInfo("America/New_York")),
            b"America/New_York",
        ),
    ],
    ids=["region", "utc", "offset", "region-standard"],
)
def test_aware_datetime_is_datetimetz_with_the_literal_zone(
    value: datetime.datetime, zone: bytes
) -> None:
    code, payload = _pair(value)
    assert code == CUBRIDDataType.DATETIMETZ
    assert payload == _T.pack(2024, 1, 2, 3, 4, 5, 6) + zone
    # The zone text is the literal path's own, without a terminal NUL.
    assert zone.decode() in _cursor_common.format_parameter(value)


def test_repeated_hour_fold_names_the_second_occurrence() -> None:
    first = datetime.datetime(2024, 11, 3, 1, 30, tzinfo=ZoneInfo("America/New_York"))
    second = first.replace(fold=1)
    assert _pair(first)[1][14:] == b"America/New_York"
    assert _pair(second)[1][14:] == b"America/New_York EST"


def test_tzinfo_without_offset_binds_datetime() -> None:
    class NoOffset(datetime.tzinfo):
        def utcoffset(self, dt: datetime.datetime | None) -> None:
            return None

    value = datetime.datetime(2024, 1, 2, 3, 4, 5, tzinfo=NoOffset())
    assert _pair(value) == (22, _T.pack(2024, 1, 2, 3, 4, 5, 0))


def test_broken_tzinfo_raises_programming_error() -> None:
    class Broken(datetime.tzinfo):
        def utcoffset(self, dt: datetime.datetime | None) -> datetime.timedelta:
            raise RuntimeError("boom")

    with pytest.raises(ProgrammingError):
        _encode_prepared_scalar(datetime.datetime(2024, 1, 2, tzinfo=Broken()))


def test_bytes_and_bytearray_are_varbit() -> None:
    assert _pair(b"\x00\x01\xff") == (6, b"\x00\x01\xff")
    assert _pair(bytearray(b"ab")) == (6, b"ab")
    assert type(_encode_prepared_scalar(bytearray(b"ab")).payload) is bytes


@pytest.mark.parametrize("value", [b"", bytearray()])
def test_empty_bytes_cannot_be_sent(value: bytes | bytearray) -> None:
    # X'' is valid SQL, but a zero-length bind arrives as NULL.
    with pytest.raises(NotSupportedError):
        _encode_prepared_scalar(value)


# -- subclasses and forged types --------------------------------------------


class _Color(enum.IntEnum):
    BIG = 2**40


class _LyingInt(int):
    def __lt__(self, other: object) -> bool:
        return True

    def __le__(self, other: object) -> bool:
        return True

    def __index__(self) -> int:
        return 1


class _LyingBytes(bytes):
    def __bytes__(self) -> bytes:
        return b"EVIL"

    def __buffer__(self, flags: int) -> memoryview:
        return memoryview(b"EVIL")


class _LyingBytearray(bytearray):
    def __buffer__(self, flags: int) -> memoryview:
        return memoryview(bytearray(b"EVIL"))


class _LyingStr(str):
    def __contains__(self, item: object) -> bool:
        return False

    def encode(self, encoding: str = "utf-8", errors: str = "strict") -> bytes:
        return b"EVIL"


class _LyingFloat(float):
    def __float__(self) -> float:
        return 99.0


class _LyingDatetime(datetime.datetime):
    @property
    def year(self) -> int:
        return 1999


def test_subclasses_bind_by_their_stored_value() -> None:
    assert _pair(_Color.BIG) == (21, struct.pack(">q", 2**40))
    # int.__index__ ignores the override and the overridden comparisons.
    assert _pair(_LyingInt(2**40)) == (21, struct.pack(">q", 2**40))
    assert _pair(_LyingInt(2**70)) == (7, b"1180591620717411303424\x00")
    assert _pair(_LyingBytes(b"real")) == (6, b"real")
    assert _pair(_LyingBytearray(b"real")) == (6, b"real")
    assert _pair(_LyingFloat(1.25)) == (12, struct.pack(">d", 1.25))
    assert _pair(_LyingStr("real")) == (1, b"real\x00")
    with pytest.raises(ProgrammingError):
        _encode_prepared_scalar(_LyingStr("a\x00b"))
    assert _pair(_LyingDatetime(2024, 1, 2)) == (22, _T.pack(2024, 1, 2, 0, 0, 0, 0))


@pytest.mark.parametrize("claimed", [int, str, float, bytes, Decimal, datetime.date])
def test_overridden_class_attribute_is_not_trusted(claimed: type) -> None:
    class Forged:
        __class__ = claimed

    with pytest.raises(ProgrammingError, match="unsupported prepared parameter type"):
        _encode_prepared_scalar(Forged())


def test_decimal_subclass_needs_the_c_module(monkeypatch: pytest.MonkeyPatch) -> None:
    _pydecimal = pytest.importorskip("_pydecimal")
    monkeypatch.setattr(_cursor_common, "Decimal", _pydecimal.Decimal)
    monkeypatch.setattr(_prepared_binding, "Decimal", _pydecimal.Decimal)
    sub = type("Sub", (_pydecimal.Decimal,), {})
    with pytest.raises(ProgrammingError, match="Decimal subclass"):
        _encode_prepared_scalar(sub("1"))
    assert _pair(_pydecimal.Decimal("0.5")) == (7, b".5\x00")


def test_temporal_subclass_needs_the_c_module(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(_cursor_common, "_CDateTime", None)
    with pytest.raises(ProgrammingError, match="C datetime module"):
        _encode_prepared_scalar(_LyingDatetime(2024, 1, 2))
    assert _pair(datetime.date(2024, 1, 2))[0] == CUBRIDDataType.DATE


# -- _PreparedScalar shape validation ---------------------------------------


@pytest.mark.parametrize(
    ("code", "payload"),
    [
        (CUBRIDDataType.BIGINT, b"\x00" * 4),
        (CUBRIDDataType.DOUBLE, b"\x00" * 4),
        (CUBRIDDataType.DOUBLE, struct.pack(">d", float("nan"))),
        (CUBRIDDataType.DOUBLE, struct.pack(">d", float("-inf"))),
        (CUBRIDDataType.NUMERIC, b"1.5"),
        (CUBRIDDataType.NUMERIC, b"\x00"),
        (CUBRIDDataType.NUMERIC, b".\x00"),
        (CUBRIDDataType.NUMERIC, b"-\x00"),
        (CUBRIDDataType.NUMERIC, b"1E5\x00"),
        (CUBRIDDataType.NUMERIC, b" 1\x00"),
        (CUBRIDDataType.NUMERIC, b"1" * 39 + b"\x00"),
        (CUBRIDDataType.DATE, b"\x00" * 6),
        (CUBRIDDataType.DATE, _T.pack(2024, 1, 2, 3, 0, 0, 0)),
        (CUBRIDDataType.TIME, _T.pack(2024, 0, 0, 1, 2, 3, 0)),
        (CUBRIDDataType.TIME, _T.pack(0, 0, 0, 1, 2, 3, 4)),
        (CUBRIDDataType.DATETIME, b"\x00" * 15),
        (CUBRIDDataType.DATETIMETZ, _T.pack(2024, 1, 2, 0, 0, 0, 0)),
        (CUBRIDDataType.DATETIMETZ, _T.pack(2024, 1, 2, 0, 0, 0, 0) + b"Asia/Seoul\x00"),
        (CUBRIDDataType.DATETIMETZ, _T.pack(2024, 1, 2, 0, 0, 0, 0) + b"A" * 64),
        (CUBRIDDataType.VARBIT, b""),
        (CUBRIDDataType.TIMESTAMP, _T.pack(2024, 1, 2, 0, 0, 0, 0)),
    ],
    ids=repr,
)
def test_scalar_rejects_malformed_payloads(code: int, payload: bytes) -> None:
    with pytest.raises(ProgrammingError):
        _PreparedScalar(code, payload)


@pytest.mark.parametrize(
    "payload", [b"0\x00", b"-12\x00", b".5\x00", b"-.5\x00", b"12.\x00", b"9" * 38 + b"\x00"]
)
def test_scalar_accepts_plain_numeric_text(payload: bytes) -> None:
    assert _PreparedScalar(CUBRIDDataType.NUMERIC, payload).payload == payload


# -- FC3 frame ----------------------------------------------------------------


def test_execute_frame_carries_each_type_value_pair() -> None:
    values = (
        True,
        2**40,
        0.25,
        Decimal("-0.5"),
        datetime.date(2024, 2, 29),
        datetime.time(1, 2, 3),
        datetime.datetime(2024, 2, 29, 1, 2, 3, 4000),
        datetime.datetime(2024, 2, 29, tzinfo=ZoneInfo("UTC")),
        b"\x01",
    )
    bindings = tuple(_encode_prepared_scalar(value, "euc_kr") for value in values)
    packet = protocol.ExecutePacket(
        3, CUBRIDStatementType.INSERT, bindings=bindings, bind_count=len(values)
    )
    frame = packet.write(_CAS_INFO)
    payload = memoryview(frame)[9:]
    args: list[bytes] = []
    offset = 0
    while offset < len(payload):
        (length,) = struct.unpack_from(">i", payload, offset)
        args.append(bytes(payload[offset + 4 : offset + 4 + length]))
        offset += 4 + length
    assert args[10:] == [
        b"\x08",
        struct.pack(">i", 1),
        b"\x15",
        struct.pack(">q", 2**40),
        b"\x0c",
        struct.pack(">d", 0.25),
        b"\x07",
        b"-.5\x00",
        b"\x0d",
        _T.pack(2024, 2, 29, 0, 0, 0, 0),
        b"\x0e",
        _T.pack(0, 0, 0, 1, 2, 3, 0),
        b"\x16",
        _T.pack(2024, 2, 29, 1, 2, 3, 4),
        b"\x1f",
        _T.pack(2024, 2, 29, 0, 0, 0, 0) + b"UTC",
        b"\x06",
        b"\x01",
    ]
