"""Tests for hardened parameter binding security in Cursor._format_parameter."""

from __future__ import annotations

import datetime
import enum
from decimal import Decimal, DecimalTuple

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
