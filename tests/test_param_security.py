"""Tests for hardened parameter binding security in Cursor._format_parameter."""

from __future__ import annotations

import datetime
from decimal import Decimal

import pytest

from pycubrid.exceptions import ProgrammingError


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

    def test_int(self, cursor: object) -> None:
        assert cursor._format_parameter(42) == "42"

    def test_float(self, cursor: object) -> None:
        assert cursor._format_parameter(3.14) == "3.14"

    def test_float_large_scientific_notation(self, cursor: object) -> None:
        # CUBRID accepts scientific/exponential notation in numeric literals
        # (an approximate number written with E is parsed as DOUBLE), so
        # str()'s exponent form is a valid literal and is emitted as-is.
        assert cursor._format_parameter(1e20) == "1e+20"

    def test_decimal(self, cursor: object) -> None:
        assert cursor._format_parameter(Decimal("99.99")) == "99.99"

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
