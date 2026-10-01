"""Tests for hardened parameter binding security in Cursor._format_parameter."""

from __future__ import annotations

import datetime
import enum
from collections.abc import Iterator
from decimal import Decimal, DecimalTuple
from typing import cast

import pytest

from pycubrid.exceptions import DataError, ProgrammingError

_INJECTED = "1; DROP TABLE t"


class _Color(enum.IntEnum):
    RED = 1


class _Perm(enum.IntFlag):
    R = 4
    W = 2


class _HostileInt(int):
    def __str__(self) -> str:
        return _INJECTED

    def __repr__(self) -> str:
        return _INJECTED

    def __format__(self, spec: str) -> str:
        return _INJECTED

    def __int__(self) -> int:
        return 666

    def __index__(self) -> int:
        return 666


class _HostileFloat(float):
    def __str__(self) -> str:
        return _INJECTED

    def __repr__(self) -> str:
        return _INJECTED

    def __format__(self, spec: str) -> str:
        return _INJECTED

    def __float__(self) -> float:
        return 666.0


class _HostileDecimal(Decimal):
    def __str__(self) -> str:
        return _INJECTED

    def __repr__(self) -> str:
        return _INJECTED

    def __format__(self, spec: str, *args: object) -> str:
        return _INJECTED

    def is_nan(self) -> bool:
        return False

    def is_infinite(self) -> bool:
        return False

    def as_tuple(self) -> DecimalTuple:
        return Decimal("1").as_tuple()


class TestEscapeString:
    @pytest.fixture
    def cursor(self) -> object:
        from unittest.mock import MagicMock

        from pycubrid.cursor import Cursor

        conn = MagicMock()
        conn._timing = None
        conn._cursors = set()
        conn.autocommit = False
        conn._no_backslash_escapes = False
        return Cursor(conn)

    def test_single_quote_escaped(self, cursor: object) -> None:
        result = cursor._format_parameter("it's a test")
        assert result == "'it''s a test'"

    def test_backslash_escaped(self, cursor: object) -> None:
        result = cursor._format_parameter("path\\to\\file")
        assert result == "'path\\\\to\\\\file'"

    def test_null_byte_rejected(self, cursor: object) -> None:
        with pytest.raises(ProgrammingError, match="null byte"):
            cursor._format_parameter("hello\x00world")

    def test_carriage_return_escaped(self, cursor: object) -> None:
        result = cursor._format_parameter("line1\rline2")
        assert "\\r" in result or "\\\r" in result

    def test_newline_escaped(self, cursor: object) -> None:
        result = cursor._format_parameter("line1\nline2")
        assert "\\n" in result or "\\\n" in result

    def test_ctrl_z_rejected(self, cursor: object) -> None:
        # CUBRID's SQL grammar defines no safe literal escape for 0x1A
        # (no MySQL-style \Z), so it is rejected like the null byte rather
        # than emitted as a raw control byte.
        with pytest.raises(ProgrammingError, match="Ctrl-Z"):
            cursor._format_parameter("data\x1amore")

    def test_combined_escaping(self, cursor: object) -> None:
        result = cursor._format_parameter("O'Reilly\\path\nnewline")
        assert "''" in result
        assert "\\\\" in result

    def test_empty_string(self, cursor: object) -> None:
        assert cursor._format_parameter("") == "''"

    def test_unicode_passthrough(self, cursor: object) -> None:
        result = cursor._format_parameter("한국어 テスト")
        assert "한국어" in result
        assert result.startswith("'")
        assert result.endswith("'")

    def test_unicode_non_bmp_passthrough(self, cursor: object) -> None:
        # Pins the PARAMETER_BINDING contract claim that non-BMP code points
        # (here U+1F389 PARTY POPPER and U+1F1F0 U+1F1F7 KR flag, encoded as
        # a surrogate pair in UTF-16) pass through unchanged.
        text = "tada \U0001f389 flag \U0001f1f0\U0001f1f7"
        result = cursor._format_parameter(text)
        assert "\U0001f389" in result
        assert "\U0001f1f0\U0001f1f7" in result
        assert result.startswith("'")
        assert result.endswith("'")

    def test_backslash_then_quote(self, cursor: object) -> None:
        result = cursor._format_parameter("test\\'end")
        assert "\\\\'" in result


class TestFormatParameterTypes:
    @pytest.fixture
    def cursor(self) -> object:
        from unittest.mock import MagicMock

        from pycubrid.cursor import Cursor

        conn = MagicMock()
        conn._timing = None
        conn._cursors = set()
        conn.autocommit = False
        conn._no_backslash_escapes = False
        return Cursor(conn)

    def test_none(self, cursor: object) -> None:
        assert cursor._format_parameter(None) == "NULL"

    def test_bool_true(self, cursor: object) -> None:
        assert cursor._format_parameter(True) == "1"

    def test_bool_false(self, cursor: object) -> None:
        assert cursor._format_parameter(False) == "0"

    def test_bytes_hex(self, cursor: object) -> None:
        assert cursor._format_parameter(b"\xde\xad") == "X'dead'"

    @pytest.mark.parametrize("value, expected", [(42, "42"), (0, "0"), (-42, "-42")])
    def test_int(self, cursor: object, value: int, expected: str) -> None:
        assert cursor._format_parameter(value) == expected

    @pytest.mark.parametrize("sign", [1, -1], ids=["positive", "negative"])
    def test_large_int(self, cursor: object, sign: int) -> None:
        value = sign * 10**1000
        expected = ("-" if sign < 0 else "") + "1" + "0" * 1000
        assert cursor._format_parameter(value) == expected

    @pytest.mark.parametrize("sign", [1, -1], ids=["positive", "negative"])
    def test_bind_large_int(self, cursor: object, sign: int) -> None:
        value = sign * 10**1000
        expected = ("-" if sign < 0 else "") + "1" + "0" * 1000
        assert cursor._bind_parameters("SELECT ?", (value,)) == "SELECT " + expected

    def test_float(self, cursor: object) -> None:
        assert cursor._format_parameter(3.14) == "3.14"

    def test_float_large_scientific_notation(self, cursor: object) -> None:
        # CUBRID accepts scientific/exponential notation in numeric literals
        # (an approximate number written with E is parsed as DOUBLE), so
        # str()'s exponent form is a valid literal and is emitted as-is.
        assert cursor._format_parameter(1e20) == "1e+20"

    def test_decimal(self, cursor: object) -> None:
        assert cursor._format_parameter(Decimal("99.99")) == "99.99"

    @pytest.mark.parametrize(
        "value, expected",
        [
            ("0.0000001", "0.0000001"),
            ("1E-7", "0.0000001"),
            ("1.23456789012345678901234E-7", "0.000000123456789012345678901234"),
            ("-1.5E-3", "-0.0015"),
            ("1.10", "1.10"),
            ("1.10E-6", "0.00000110"),
            ("0E-3", "0.000"),
            ("-0", "-0"),
            ("-0.00", "-0.00"),
            ("0E+5", "0"),
            ("1E+5", "100000"),
            ("-1.2E+3", "-1200"),
            ("123.456E+2", "12345.6"),
            ("12345678901234567890", "12345678901234567890"),
        ],
    )
    def test_decimal_plain_notation(self, cursor: object, value: str, expected: str) -> None:
        # CUBRID parses an E-notation literal as DOUBLE, so a Decimal must be
        # rendered in plain fixed-point notation with its sign and scale (#517).
        assert cursor._format_parameter(Decimal(value)) == expected

    @pytest.mark.parametrize(
        "value, expected",
        [
            ("1E-38", "0." + "0" * 37 + "1"),
            ("-1E-38", "-0." + "0" * 37 + "1"),
            ("0E-38", "0." + "0" * 38),
            ("9" * 38, "9" * 38),
            ("1E+37", "1" + "0" * 37),
            ("0." + "1" * 38, "0." + "1" * 38),
            ("1" * 37 + ".1", "1" * 37 + ".1"),
        ],
        ids=[
            "scale-38",
            "negative-scale-38",
            "zero-scale-38",
            "integer-38",
            "exponent-integer-38",
            "fraction-38",
            "mixed-38",
        ],
    )
    def test_decimal_precision_38_accepted(self, cursor: object, value: str, expected: str) -> None:
        assert cursor._format_parameter(Decimal(value)) == expected

    @pytest.mark.parametrize(
        "value",
        [
            "1E-39",
            "-1E-39",
            "0E-39",
            "9" * 39,
            "1E+38",
            "0." + "1" * 39,
            "1" * 38 + ".1",
            "1.00000000000000000000000000000000000001",
            "1E+999999999",
            "1E-999999999",
        ],
        ids=[
            "scale-39",
            "negative-scale-39",
            "zero-scale-39",
            "integer-39",
            "exponent-integer-39",
            "fraction-39",
            "mixed-39",
            "significant-39",
            "huge-positive-exponent",
            "huge-negative-exponent",
        ],
    )
    def test_decimal_precision_over_38_raises(self, cursor: object, value: str) -> None:
        with pytest.raises(DataError, match="at most 38 digits"):
            cursor._format_parameter(Decimal(value))

    def test_bind_decimal_plain_notation(self, cursor: object) -> None:
        assert cursor._bind_parameters("SELECT ?", (Decimal("1E-7"),)) == "SELECT 0.0000001"

    def test_date(self, cursor: object) -> None:
        result = cursor._format_parameter(datetime.date(2026, 1, 15))
        assert result == "DATE'2026-01-15'"

    def test_time(self, cursor: object) -> None:
        result = cursor._format_parameter(datetime.time(13, 45, 30))
        assert result == "TIME'13:45:30'"

    def test_time_microseconds_truncated(self, cursor: object) -> None:
        # CUBRID TIME has second resolution (literal grammar allows only
        # 'HH:MI:SS'), so sub-second precision is intentionally dropped.
        result = cursor._format_parameter(datetime.time(13, 45, 30, 123456))
        assert result == "TIME'13:45:30'"

    def test_datetime(self, cursor: object) -> None:
        result = cursor._format_parameter(datetime.datetime(2026, 1, 15, 13, 45, 30, 123000))
        assert result == "DATETIME'2026-01-15 13:45:30.123'"

    def test_unsupported_type(self, cursor: object) -> None:
        with pytest.raises(ProgrammingError, match="unsupported parameter type"):
            cursor._format_parameter(object())

    @pytest.mark.parametrize("value", [[1, 2], (1, 2), {1, 2}, frozenset({1, 2}), {"a": 1}])
    def test_collection_raises_actionable_message(self, cursor: object, value: object) -> None:
        with pytest.raises(ProgrammingError, match="cannot bind a collection"):
            cursor._format_parameter(value)

    def test_float_nan_raises(self, cursor: object) -> None:
        with pytest.raises(ProgrammingError, match="nan and inf"):
            cursor._format_parameter(float("nan"))

    def test_float_inf_raises(self, cursor: object) -> None:
        with pytest.raises(ProgrammingError, match="nan and inf"):
            cursor._format_parameter(float("inf"))

    def test_float_neg_inf_raises(self, cursor: object) -> None:
        with pytest.raises(ProgrammingError, match="nan and inf"):
            cursor._format_parameter(float("-inf"))

    def test_decimal_nan_raises(self, cursor: object) -> None:
        with pytest.raises(ProgrammingError, match="nan and inf"):
            cursor._format_parameter(Decimal("NaN"))

    def test_decimal_inf_raises(self, cursor: object) -> None:
        with pytest.raises(ProgrammingError, match="nan and inf"):
            cursor._format_parameter(Decimal("Infinity"))

    def test_decimal_neg_inf_raises(self, cursor: object) -> None:
        with pytest.raises(ProgrammingError, match="nan and inf"):
            cursor._format_parameter(Decimal("-Infinity"))

    @pytest.mark.parametrize(
        "value, expected",
        [
            (_Color.RED, "1"),
            (_Perm.R | _Perm.W, "6"),
            (_Perm(0), "0"),
            (_HostileInt(1), "1"),
            (_HostileInt(-(10**40)), "-1" + "0" * 40),
            (_HostileFloat(2.5), "2.5"),
            (_HostileFloat(1e20), "1e+20"),
            (_HostileDecimal("1E-7"), "0.0000001"),
            (_HostileDecimal("-1.10"), "-1.10"),
        ],
        ids=[
            "IntEnum",
            "IntFlag",
            "IntFlag-zero",
            "int-subclass",
            "int-subclass-large",
            "float-subclass",
            "float-subclass-exponent",
            "decimal-subclass",
            "decimal-subclass-scale",
        ],
    )
    def test_numeric_subclass_renders_by_value(
        self, cursor: object, value: object, expected: str
    ) -> None:
        # Subclasses (IntEnum/IntFlag and user types) must not reach SQL
        # through an overridable __str__/__repr__/__format__ (#518).
        assert cursor._format_parameter(value) == expected
        assert cursor._bind_parameters("SELECT ?", (value,)) == "SELECT " + expected

    def test_float_subclass_nan_raises(self, cursor: object) -> None:
        with pytest.raises(ProgrammingError, match="nan and inf"):
            cursor._format_parameter(_HostileFloat("nan"))

    def test_decimal_subclass_nan_raises(self, cursor: object) -> None:
        with pytest.raises(ProgrammingError, match="nan and inf"):
            cursor._format_parameter(_HostileDecimal("NaN"))

    def test_decimal_subclass_precision_check_uses_value(self, cursor: object) -> None:
        # A lying as_tuple() override must not bypass the 38-digit check.
        with pytest.raises(DataError, match="at most 38 digits"):
            cursor._format_parameter(_HostileDecimal("1E-39"))

    def test_bool_cannot_be_subclassed(self) -> None:
        # bool is final, so the bool -> 0/1 branch cannot be spoofed.
        with pytest.raises(TypeError):
            type("B", (bool,), {})

    def test_bytearray_hex(self, cursor: object) -> None:
        assert cursor._format_parameter(bytearray(b"\xca\xfe")) == "X'cafe'"

    def test_datetime_tz_iana(self, cursor: object) -> None:
        from zoneinfo import ZoneInfo

        dt = datetime.datetime(2026, 1, 15, 10, 30, 0, 123000, tzinfo=ZoneInfo("Asia/Seoul"))
        result = cursor._format_parameter(dt)
        assert result == "DATETIMETZ'2026-01-15 10:30:00.123 Asia/Seoul'"

    def test_datetime_tz_utc(self, cursor: object) -> None:
        dt = datetime.datetime(2026, 1, 15, 10, 30, 0, tzinfo=datetime.timezone.utc)
        result = cursor._format_parameter(dt)
        assert result == "DATETIMETZ'2026-01-15 10:30:00.000 +00:00'"

    def test_datetime_tz_fixed_offset(self, cursor: object) -> None:
        tz = datetime.timezone(datetime.timedelta(hours=5, minutes=30))
        dt = datetime.datetime(2026, 1, 15, 10, 30, 0, tzinfo=tz)
        result = cursor._format_parameter(dt)
        assert result == "DATETIMETZ'2026-01-15 10:30:00.000 +05:30'"

    def test_datetime_tz_negative_offset(self, cursor: object) -> None:
        tz = datetime.timezone(datetime.timedelta(hours=-5))
        dt = datetime.datetime(2026, 1, 15, 10, 30, 0, tzinfo=tz)
        result = cursor._format_parameter(dt)
        assert result == "DATETIMETZ'2026-01-15 10:30:00.000 -05:00'"


# ---- #528 / #519: str, bytes, date and time literals ------------------------

_INJECTED_SQL = "x'; DROP TABLE users; --"


def _inject(*args: object, **kwargs: object) -> str:
    return _INJECTED_SQL


class _HostileStr(str):
    """A str subclass whose every overridable text method lies (#528)."""

    replace = _inject
    translate = _inject
    encode = _inject
    join = _inject
    __str__ = _inject
    __repr__ = _inject
    __format__ = _inject
    __mod__ = _inject
    __rmod__ = _inject
    __add__ = _inject
    __radd__ = _inject
    __getitem__ = _inject

    def __contains__(self, item: object) -> bool:
        return False

    def find(self, *args: object) -> int:
        return -1

    def __iter__(self) -> Iterator[str]:
        return iter(_INJECTED_SQL)

    def __len__(self) -> int:
        return 0

    def __bool__(self) -> bool:
        return False

    def __eq__(self, other: object) -> bool:
        return True

    __hash__ = str.__hash__


class _HostileBytes(bytes):
    hex = _inject
    decode = _inject
    __str__ = _inject
    __repr__ = _inject
    __format__ = _inject
    __getitem__ = _inject

    def __bytes__(self) -> bytes:
        return b"'; DROP TABLE users; --"

    def __iter__(self) -> Iterator[int]:
        return iter(b"'; DROP")

    def __len__(self) -> int:
        return 0


class _HostileByteArray(bytearray):
    hex = _inject
    decode = _inject
    __str__ = _inject
    __repr__ = _inject
    __format__ = _inject

    def __len__(self) -> int:
        return 0


def _lie(self: object) -> int:
    return 7


_HOSTILE_TEMPORAL = {
    "strftime": _inject,
    "isoformat": _inject,
    "ctime": _inject,
    "__str__": _inject,
    "__repr__": _inject,
    "__format__": _inject,
    "timetuple": _inject,
    "replace": _inject,
    "utcoffset": _inject,
    "year": property(_lie),
    "month": property(_lie),
    "day": property(_lie),
    "hour": property(_lie),
    "minute": property(_lie),
    "second": property(_lie),
    "microsecond": property(_lie),
    "tzinfo": property(lambda self: None),
}

_HostileDate = type("_HostileDate", (datetime.date,), dict(_HOSTILE_TEMPORAL))
_HostileDateTime = type("_HostileDateTime", (datetime.datetime,), dict(_HOSTILE_TEMPORAL))
_HostileTime = type("_HostileTime", (datetime.time,), dict(_HOSTILE_TEMPORAL))


class _HostileTimedelta(datetime.timedelta):
    def total_seconds(self) -> float:
        return 0.0


# Shadow the timedelta fields after class creation (a class-body assignment
# would conflict with the base-class attribute types).
setattr(_HostileTimedelta, "days", property(_lie))
setattr(_HostileTimedelta, "seconds", property(_lie))


class _KeyedTZ(datetime.tzinfo):
    """A tzinfo with a caller-chosen ``key`` and a fixed +09:00 offset."""

    def __init__(self, key: object, offset: datetime.timedelta | None = None) -> None:
        self.key = key
        self._offset = datetime.timedelta(hours=9) if offset is None else offset

    def utcoffset(self, dt: datetime.datetime | None) -> datetime.timedelta:
        return self._offset

    def dst(self, dt: datetime.datetime | None) -> datetime.timedelta:
        return datetime.timedelta(0)

    def tzname(self, dt: datetime.datetime | None) -> str:
        return "X"


def _spoof(cls: type) -> object:
    """Return an object whose ``__class__`` claims to be *cls*."""
    return type("_Spoof", (), {"__class__": property(lambda self: cls)})()


# Plain values and their exact literals on main before #528 (golden output).
_GOLDEN_PLAIN = [
    (None, "NULL", None),
    (True, "1", None),
    (False, "0", None),
    (42, "42", None),
    (2.5, "2.5", None),
    (Decimal("3.14"), "3.14", None),
    ("", "''", "''"),
    ("hello", "'hello'", "'hello'"),
    ("it's", "'it''s'", "'it''s'"),
    ("back\\slash", "'back\\slash'", "'back\\\\slash'"),
    ("line\nbreak\r", "'line\nbreak\r'", "'line\\\nbreak\\\r'"),
    ("\\'", "'\\'''", "'\\\\'''"),
    ("유니코드 ✓", "'유니코드 ✓'", "'유니코드 ✓'"),
    (b"", "X''", "X''"),
    (b"\x00\xff", "X'00ff'", "X'00ff'"),
    (bytearray(b"\xca\xfe"), "X'cafe'", "X'cafe'"),
    (datetime.date(2026, 1, 15), "DATE'2026-01-15'", None),
    (datetime.date(9999, 12, 31), "DATE'9999-12-31'", None),
    (datetime.time(13, 45, 30, 123456), "TIME'13:45:30'", None),
    (datetime.time(1, 2, 3, tzinfo=datetime.timezone.utc), "TIME'01:02:03'", None),
    (datetime.datetime(2026, 1, 15, 13, 45, 30, 999999), "DATETIME'2026-01-15 13:45:30.999'", None),
    (datetime.datetime(1000, 1, 1), "DATETIME'1000-01-01 00:00:00.000'", None),
    (
        datetime.datetime(2026, 1, 15, 10, 30, tzinfo=datetime.timezone.utc),
        "DATETIMETZ'2026-01-15 10:30:00.000 +00:00'",
        None,
    ),
    (
        datetime.datetime(
            2026, 1, 15, 10, 30, tzinfo=datetime.timezone(-datetime.timedelta(hours=3, minutes=30))
        ),
        "DATETIMETZ'2026-01-15 10:30:00.000 -03:30'",
        None,
    ),
    (
        datetime.datetime(
            2026, 1, 15, 10, 30, tzinfo=datetime.timezone(datetime.timedelta(seconds=-1))
        ),
        "DATETIMETZ'2026-01-15 10:30:00.000 -00:00'",
        None,
    ),
    (
        datetime.datetime(
            2026, 1, 15, 10, 30, tzinfo=datetime.timezone(datetime.timedelta(microseconds=-1))
        ),
        "DATETIMETZ'2026-01-15 10:30:00.000 +00:00'",
        None,
    ),
    (
        datetime.datetime(
            2026,
            1,
            15,
            10,
            30,
            tzinfo=datetime.timezone(
                -datetime.timedelta(hours=23, minutes=59, seconds=59, microseconds=999999)
            ),
        ),
        "DATETIMETZ'2026-01-15 10:30:00.000 -23:59'",
        None,
    ),
]


class TestPlainLiteralsUnchanged:
    """Plain values render byte-identically to main before #528."""

    @pytest.mark.parametrize("value, strict, legacy", _GOLDEN_PLAIN)
    def test_golden(self, value: object, strict: str, legacy: str | None) -> None:
        from pycubrid._cursor_common import format_parameter

        assert format_parameter(value, no_backslash_escapes=True) == strict
        assert format_parameter(value, no_backslash_escapes=False) == (legacy or strict)

    @pytest.mark.parametrize(
        "key", ["Asia/Seoul", "America/Port-au-Prince", "Etc/GMT+5", "Etc/GMT-14", "UTC"]
    )
    def test_zoneinfo_keys_unchanged(self, key: str) -> None:
        from zoneinfo import ZoneInfo

        from pycubrid._cursor_common import format_parameter

        value = datetime.datetime(2026, 7, 1, 10, 30, 0, 5000, tzinfo=ZoneInfo(key))
        assert format_parameter(value) == "DATETIMETZ'2026-07-01 10:30:00.005 %s'" % key


class TestStrSubclassEscaping:
    """str subclasses are escaped from their characters, not their methods (#528)."""

    @pytest.mark.parametrize("no_backslash_escapes", [True, False])
    def test_hostile_str_is_escaped(self, no_backslash_escapes: bool) -> None:
        from pycubrid._cursor_common import bind_parameters, escape_string, format_parameter

        value = _HostileStr("it's")
        assert format_parameter(value, no_backslash_escapes=no_backslash_escapes) == "'it''s'"
        assert escape_string(value, no_backslash_escapes=no_backslash_escapes) == "'it''s'"
        assert (
            bind_parameters("SELECT ?", (value,), no_backslash_escapes=no_backslash_escapes)
            == "SELECT 'it''s'"
        )

    def test_hostile_str_backslash_mode(self) -> None:
        from pycubrid._cursor_common import format_parameter

        value = _HostileStr("a\\'\nb")
        assert format_parameter(value, no_backslash_escapes=False) == "'a\\\\''\\\nb'"

    @pytest.mark.parametrize(
        "raw, message", [("a\x00b", "null byte"), ("a\x1ab", "Ctrl-Z")], ids=["nul", "ctrl-z"]
    )
    def test_hostile_str_guards_still_apply(self, raw: str, message: str) -> None:
        from pycubrid._cursor_common import escape_string, format_parameter

        with pytest.raises(ProgrammingError, match=message):
            format_parameter(_HostileStr(raw))
        with pytest.raises(ProgrammingError, match=message):
            escape_string(_HostileStr(raw))

    def test_escape_string_rejects_non_str(self) -> None:
        from pycubrid._cursor_common import escape_string

        with pytest.raises(ProgrammingError):
            escape_string(cast(str, _spoof(str)))


class TestBinarySubclassRendering:
    @pytest.mark.parametrize(
        "value", [_HostileBytes(b"A'"), _HostileByteArray(b"A'")], ids=["bytes", "bytearray"]
    )
    def test_hostile_binary_renders_real_bytes(self, value: object) -> None:
        from pycubrid._cursor_common import format_parameter

        assert format_parameter(value) == "X'4127'"


class TestTemporalSubclassRendering:
    """date/time subclasses render from base-class fields (#528)."""

    def test_hostile_date(self) -> None:
        from pycubrid._cursor_common import format_parameter

        assert format_parameter(_HostileDate(2024, 2, 29)) == "DATE'2024-02-29'"

    def test_hostile_time(self) -> None:
        from pycubrid._cursor_common import format_parameter

        assert format_parameter(_HostileTime(1, 2, 3, 456789)) == "TIME'01:02:03'"

    def test_hostile_datetime(self) -> None:
        from pycubrid._cursor_common import format_parameter

        value = _HostileDateTime(2024, 1, 1, 0, 0, 0, 123999)
        assert format_parameter(value) == "DATETIME'2024-01-01 00:00:00.123'"

    def test_hostile_datetime_tz(self) -> None:
        from pycubrid._cursor_common import format_parameter

        tz = datetime.timezone(datetime.timedelta(hours=-5))
        value = _HostileDateTime(2024, 1, 1, 12, 0, 0, tzinfo=tz)
        assert format_parameter(value) == "DATETIMETZ'2024-01-01 12:00:00.000 -05:00'"

    def test_tzinfo_returning_hostile_timedelta(self) -> None:
        from pycubrid._cursor_common import format_parameter

        offset = _HostileTimedelta(hours=5, minutes=30)
        value = datetime.datetime(2024, 1, 1, tzinfo=_KeyedTZ(None, offset))
        assert format_parameter(value) == "DATETIMETZ'2024-01-01 00:00:00.000 +05:30'"

    @pytest.mark.parametrize(
        "value, expected",
        [
            (datetime.date(1, 1, 2), "DATE'0001-01-02'"),
            (datetime.date(99, 1, 2), "DATE'0099-01-02'"),
            (datetime.date(999, 1, 2), "DATE'0999-01-02'"),
            (datetime.date(1000, 1, 2), "DATE'1000-01-02'"),
            (datetime.datetime(99, 1, 2, 3, 4, 5, 6000), "DATETIME'0099-01-02 03:04:05.006'"),
            (
                datetime.datetime(999, 1, 2, tzinfo=datetime.timezone.utc),
                "DATETIMETZ'0999-01-02 00:00:00.000 +00:00'",
            ),
            (_HostileDate(99, 1, 2), "DATE'0099-01-02'"),
        ],
        ids=["y1", "y99", "y999", "y1000", "datetime-y99", "datetimetz-y999", "subclass-y99"],
    )
    def test_year_zero_padded(self, value: object, expected: str) -> None:
        # #519: %Y does not pad years below 1000, and CUBRID reads '99-01-02'
        # as 1999-01-02.
        from pycubrid._cursor_common import format_parameter

        assert format_parameter(value) == expected


class TestTzinfoKey:
    @pytest.mark.parametrize(
        "key",
        [
            "Asia/Seoul' ; DROP TABLE users; --",
            "UTC'",
            "Asia/Seoul\n",
            "Asia Seoul",
            "Europe/Zürich",
            _HostileStr("Asia/Seoul"),
            42,
            b"UTC",
        ],
        ids=[
            "quote-injection",
            "trailing-quote",
            "newline",
            "space",
            "non-ascii",
            "str-subclass",
            "int",
            "bytes",
        ],
    )
    def test_invalid_key_rejected(self, key: object) -> None:
        from pycubrid._cursor_common import format_parameter

        value = datetime.datetime(2024, 1, 1, tzinfo=_KeyedTZ(key))
        with pytest.raises(ProgrammingError, match="time zone"):
            format_parameter(value)

    @pytest.mark.parametrize("key", [None, ""], ids=["none", "empty"])
    def test_missing_key_uses_offset(self, key: object) -> None:
        from pycubrid._cursor_common import format_parameter

        value = datetime.datetime(2024, 1, 1, tzinfo=_KeyedTZ(key))
        assert format_parameter(value) == "DATETIMETZ'2024-01-01 00:00:00.000 +09:00'"

    def test_tzinfo_without_offset_renders_naive(self) -> None:
        # Unchanged from main: a tzinfo whose utcoffset() is None is naive.
        from pycubrid._cursor_common import format_parameter

        class _NoOffset(datetime.tzinfo):
            def utcoffset(self, dt: datetime.datetime | None) -> None:
                return None

            def dst(self, dt: datetime.datetime | None) -> None:
                return None

        value = datetime.datetime(2024, 1, 1, tzinfo=_NoOffset())
        assert format_parameter(value) == "DATETIME'2024-01-01 00:00:00.000'"

    def test_valid_custom_key_used(self) -> None:
        from pycubrid._cursor_common import format_parameter

        value = datetime.datetime(2024, 1, 1, tzinfo=_KeyedTZ("Asia/Tokyo"))
        assert format_parameter(value) == "DATETIMETZ'2024-01-01 00:00:00.000 Asia/Tokyo'"


class TestClassSpoofing:
    @pytest.mark.parametrize(
        "claimed",
        [
            bool,
            int,
            float,
            Decimal,
            str,
            bytes,
            bytearray,
            datetime.datetime,
            datetime.date,
            datetime.time,
            list,
        ],
    )
    def test_spoofed_class_raises_programming_error(self, claimed: type) -> None:
        from pycubrid._cursor_common import bind_parameters, format_parameter

        value = _spoof(claimed)
        assert isinstance(value, claimed)
        with pytest.raises(ProgrammingError):
            format_parameter(value)
        with pytest.raises(ProgrammingError):
            bind_parameters("SELECT ?", (value,))


class TestPureDecimalFallback:
    """Decimal subclasses cannot be copied safely by the pure-Python module."""

    @staticmethod
    def _forging_subclass(base: type) -> type:
        # _pydecimal's Decimal(x) copies these attributes with plain reads.
        forged = {"_int": "1; DROP TABLE t", "_exp": 0, "_sign": 0, "_is_special": False}

        def __getattribute__(self: object, name: str) -> object:
            if name in forged:
                return forged[name]
            return base.__getattribute__(self, name)

        return type("_Forged", (base,), {"__getattribute__": __getattribute__})

    def test_pydecimal_copy_is_forgeable(self) -> None:
        # Documents why the guard exists: the pure-Python copy trusts the
        # subclass's attributes.
        _pydecimal = pytest.importorskip("_pydecimal")
        forged = self._forging_subclass(_pydecimal.Decimal)("1")
        assert "DROP" in format(_pydecimal.Decimal(forged), "f")

    def test_subclass_rejected_without_c_decimal(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from pycubrid import _cursor_common

        _pydecimal = pytest.importorskip("_pydecimal")
        monkeypatch.setattr(_cursor_common, "Decimal", _pydecimal.Decimal)
        forged = self._forging_subclass(_pydecimal.Decimal)("1")
        with pytest.raises(ProgrammingError, match="Decimal subclass"):
            _cursor_common.format_parameter(forged)
        # A plain pure-Python Decimal still renders.
        assert _cursor_common.format_parameter(_pydecimal.Decimal("1E-7")) == "0.0000001"

    def test_c_decimal_subclass_is_forge_proof(self) -> None:
        from pycubrid._cursor_common import format_parameter

        forged = self._forging_subclass(Decimal)("1.5")
        assert format_parameter(forged) == "1.5"
