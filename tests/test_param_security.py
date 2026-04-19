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

    def test_ctrl_z_escaped(self, cursor: object) -> None:
        result = cursor._format_parameter("data\x1amore")
        assert "\\\x1a" in result

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

    def test_decimal(self, cursor: object) -> None:
        assert cursor._format_parameter(Decimal("99.99")) == "99.99"

    def test_date(self, cursor: object) -> None:
        result = cursor._format_parameter(datetime.date(2026, 1, 15))
        assert result == "DATE'2026-01-15'"

    def test_time(self, cursor: object) -> None:
        result = cursor._format_parameter(datetime.time(13, 45, 30))
        assert result == "TIME'13:45:30'"

    def test_datetime(self, cursor: object) -> None:
        result = cursor._format_parameter(datetime.datetime(2026, 1, 15, 13, 45, 30, 123000))
        assert result == "DATETIME'2026-01-15 13:45:30.123'"

    def test_unsupported_type(self, cursor: object) -> None:
        with pytest.raises(ProgrammingError, match="unsupported parameter type"):
            cursor._format_parameter(object())
