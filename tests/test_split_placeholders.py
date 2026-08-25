"""Tests for _split_on_placeholders() safe parameter binding."""

from __future__ import annotations

import pytest

from pycubrid.cursor import _split_on_placeholders


class TestSplitOnPlaceholders:
    """Verify ? splitting respects quotes and comments."""

    def test_simple(self):
        assert _split_on_placeholders("SELECT * FROM t WHERE id = ?") == [
            "SELECT * FROM t WHERE id = ",
            "",
        ]

    def test_multiple_placeholders(self):
        assert _split_on_placeholders("INSERT INTO t (a, b) VALUES (?, ?)") == [
            "INSERT INTO t (a, b) VALUES (",
            ", ",
            ")",
        ]

    def test_no_placeholders(self):
        assert _split_on_placeholders("SELECT 1") == ["SELECT 1"]

    def test_question_in_single_quoted_string(self):
        parts = _split_on_placeholders("SELECT * FROM t WHERE name = 'what?' AND id = ?")
        assert len(parts) == 2  # only 1 real placeholder
        assert "what?" in parts[0]

    def test_doubled_quote_escape(self):
        parts = _split_on_placeholders(
            "SELECT * FROM t WHERE name = 'it''s a question?' AND id = ?"
        )
        assert len(parts) == 2
        assert "it''s a question?" in parts[0]

    def test_question_in_double_quoted_identifier(self):
        parts = _split_on_placeholders('SELECT "col?" FROM t WHERE id = ?')
        assert len(parts) == 2
        assert "col?" in parts[0]

    def test_question_in_line_comment(self):
        parts = _split_on_placeholders("-- why?\nSELECT * FROM t WHERE id = ?")
        assert len(parts) == 2
        assert "why?" in parts[0]

    def test_question_in_block_comment(self):
        parts = _split_on_placeholders("/* why? */ SELECT * FROM t WHERE id = ?")
        assert len(parts) == 2
        assert "why?" in parts[0]

    def test_mixed_comments_and_strings(self):
        sql = "/* hint? */ SELECT * FROM t WHERE name = 'q?' AND -- comment?\nid = ?"
        parts = _split_on_placeholders(sql)
        assert len(parts) == 2

    def test_optimizer_hint_with_question(self):
        parts = _split_on_placeholders("/*+ hint? */ SELECT * FROM t WHERE id = ?")
        assert len(parts) == 2

    def test_empty_string(self):
        assert _split_on_placeholders("") == [""]

    def test_only_placeholder(self):
        assert _split_on_placeholders("?") == ["", ""]

    def test_unterminated_string_is_lenient(self):
        # Malformed SQL - don't crash, let CUBRID reject it
        parts = _split_on_placeholders("SELECT 'unterminated")
        assert len(parts) == 1  # no placeholder found

    def test_unterminated_block_comment(self):
        parts = _split_on_placeholders("/* unterminated SELECT ? FROM t")
        assert len(parts) == 1  # ? is inside comment, no placeholder


class TestDialectAwareTokenizer:
    """Verify CUBRID dialect additions: backslash escapes, // comments,
    backtick and bracket identifiers."""

    # --- backslash escapes in single-quoted strings ---------------------

    def test_backslash_escaped_quote_no_backslash_escapes_off(self):
        # no_backslash_escapes=no -> \' does NOT terminate the string,
        # so the ? stays inside the literal (1 real placeholder).
        sql = r"SELECT * FROM t WHERE name = 'a\'? b' AND id = ?"
        parts = _split_on_placeholders(sql, no_backslash_escapes=False)
        assert len(parts) == 2
        assert "?" in parts[0]  # the quoted ? is retained in part 0

    def test_backslash_escaped_quote_no_backslash_escapes_on(self):
        # no_backslash_escapes=yes (default) -> backslash is ordinary, the
        # first ' after it terminates the string, exposing the next ? .
        sql = r"SELECT 'x\' , ?, ?"
        parts = _split_on_placeholders(sql, no_backslash_escapes=True)
        assert len(parts) == 3  # both ? are real placeholders

    def test_default_is_no_backslash_escapes_on(self):
        # Standalone default must preserve legacy behaviour (backslash
        # ordinary), matching CUBRID's default no_backslash_escapes=yes.
        sql = r"SELECT 'x\' , ?, ?"
        assert _split_on_placeholders(sql) == _split_on_placeholders(sql, no_backslash_escapes=True)

    def test_double_backslash_then_quote(self):
        # \\ is a literal backslash, so the following ' terminates.
        sql = r"SELECT '\\' , ?"
        parts = _split_on_placeholders(sql, no_backslash_escapes=False)
        assert len(parts) == 2

    def test_trailing_backslash_at_eof_is_lenient(self):
        parts = _split_on_placeholders("SELECT 'oops\\", no_backslash_escapes=False)
        assert len(parts) == 1  # unterminated, no crash, no placeholder

    def test_doubling_still_works_with_escapes_on(self):
        # '' doubling stays valid even when backslash escapes are active.
        sql = "SELECT 'it''s a q?' AND id = ?"
        parts = _split_on_placeholders(sql, no_backslash_escapes=False)
        assert len(parts) == 2
        assert "it''s a q?" in parts[0]

    # --- // line comments -----------------------------------------------

    def test_question_in_cpp_line_comment(self):
        parts = _split_on_placeholders("// why?\nSELECT * FROM t WHERE id = ?")
        assert len(parts) == 2
        assert "why?" in parts[0]

    # --- backtick identifiers -------------------------------------------

    def test_question_in_backtick_identifier(self):
        parts = _split_on_placeholders("SELECT `col?` FROM t WHERE id = ?")
        assert len(parts) == 2
        assert "col?" in parts[0]

    def test_unterminated_backtick_is_lenient(self):
        parts = _split_on_placeholders("SELECT `col? FROM t")
        assert len(parts) == 1

    # --- bracket identifiers --------------------------------------------

    def test_question_in_bracket_identifier(self):
        parts = _split_on_placeholders("SELECT [col?] FROM t WHERE id = ?")
        assert len(parts) == 2
        assert "col?" in parts[0]

    def test_unterminated_bracket_is_lenient(self):
        parts = _split_on_placeholders("SELECT [col? FROM t")
        assert len(parts) == 1

    # --- signature / compatibility --------------------------------------

    def test_keyword_only_param(self):
        with pytest.raises(TypeError):
            _split_on_placeholders("SELECT ?", False)  # type: ignore[misc]


class TestBindParametersIntegration:
    """End-to-end _bind_parameters tests through Cursor path."""

    def test_bind_skips_quoted_question_mark(self):
        """Verify full bind path handles ? in string literals."""
        from unittest.mock import MagicMock
        from pycubrid.cursor import Cursor

        conn = MagicMock()
        conn._no_backslash_escapes = False
        conn._decode_collections = False
        conn._json_deserializer = None
        conn._fetch_size = 100
        conn._timing = None
        cur = Cursor(conn)

        result = cur._bind_parameters("SELECT * FROM t WHERE name = 'what?' AND id = ?", [42])
        assert result == "SELECT * FROM t WHERE name = 'what?' AND id = 42"

    def test_bind_multiple_with_comments(self):
        from unittest.mock import MagicMock
        from pycubrid.cursor import Cursor

        conn = MagicMock()
        conn._no_backslash_escapes = False
        conn._decode_collections = False
        conn._json_deserializer = None
        conn._fetch_size = 100
        conn._timing = None
        cur = Cursor(conn)

        result = cur._bind_parameters(
            "/* hint? */ INSERT INTO t (a, b) VALUES (?, ?)", ["hello", 99]
        )
        assert "hint?" in result
        assert "'hello'" in result
        assert "99" in result

    def test_bind_wrong_count_with_quoted_question(self):
        from unittest.mock import MagicMock
        from pycubrid.cursor import Cursor
        from pycubrid.exceptions import ProgrammingError

        conn = MagicMock()
        conn._no_backslash_escapes = False
        conn._decode_collections = False
        conn._json_deserializer = None
        conn._fetch_size = 100
        conn._timing = None
        cur = Cursor(conn)

        # 'what?' doesn't count as placeholder, so only 1 real placeholder
        with pytest.raises(ProgrammingError):
            cur._bind_parameters("SELECT * FROM t WHERE name = 'what?' AND id = ?", [1, 2])


class TestAsyncBindParity:
    """Verify async cursor uses same placeholder logic."""

    def test_async_bind_skips_quoted_question(self):
        from unittest.mock import MagicMock
        from pycubrid.aio.cursor import AsyncCursor

        conn = MagicMock()
        conn._no_backslash_escapes = False
        conn._decode_collections = False
        conn._json_deserializer = None
        conn._fetch_size = 100
        conn._timing = None
        cur = AsyncCursor(conn)

        result = cur._bind_parameters("SELECT * FROM t WHERE name = 'what?' AND id = ?", [42])
        assert result == "SELECT * FROM t WHERE name = 'what?' AND id = 42"


class TestEscapeModeResolution:
    """Verify the None-guard on the negotiated escape mode (#267)."""

    def _make_cursor(self, escape_mode):
        from unittest.mock import MagicMock
        from pycubrid.cursor import Cursor

        conn = MagicMock()
        conn._no_backslash_escapes = escape_mode
        conn._decode_collections = False
        conn._json_deserializer = None
        conn._fetch_size = 100
        conn._timing = None
        return Cursor(conn)

    def test_bind_raises_when_escape_mode_unnegotiated(self):
        from pycubrid.exceptions import InterfaceError

        cur = self._make_cursor(None)
        with pytest.raises(InterfaceError):
            cur._bind_parameters("SELECT ?", [1])

    def test_format_parameter_raises_when_escape_mode_unnegotiated(self):
        from pycubrid.exceptions import InterfaceError

        cur = self._make_cursor(None)
        with pytest.raises(InterfaceError):
            cur._format_parameter("x")

    def test_resolve_returns_concrete_bool(self):
        assert self._make_cursor(True)._resolve_escape_mode() is True
        assert self._make_cursor(False)._resolve_escape_mode() is False
