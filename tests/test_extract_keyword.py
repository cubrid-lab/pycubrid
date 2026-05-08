"""Tests for _extract_first_keyword() comment-stripping SQL parser."""

from __future__ import annotations


from pycubrid.cursor import _DML_BATCH_VERBS, _extract_first_keyword


class TestExtractFirstKeyword:
    """Verify SQL keyword extraction handles comments and edge cases."""

    def test_simple_insert(self):
        assert _extract_first_keyword("INSERT INTO t VALUES (1)") == "INSERT"

    def test_simple_select(self):
        assert _extract_first_keyword("SELECT 1") == "SELECT"

    def test_lowercase(self):
        assert _extract_first_keyword("insert into t values (1)") == "INSERT"

    def test_mixed_case(self):
        assert _extract_first_keyword("Insert INTO t") == "INSERT"

    def test_leading_whitespace(self):
        assert _extract_first_keyword("   \n\t  DELETE FROM t") == "DELETE"

    def test_block_comment(self):
        assert _extract_first_keyword("/* hint */ INSERT INTO t") == "INSERT"

    def test_nested_block_comments(self):
        assert _extract_first_keyword("/* a */ /* b */ UPDATE t SET x=1") == "UPDATE"

    def test_line_comment(self):
        assert _extract_first_keyword("-- this is a comment\nINSERT INTO t") == "INSERT"

    def test_multiple_line_comments(self):
        assert _extract_first_keyword("-- c1\n-- c2\nDELETE FROM t") == "DELETE"

    def test_mixed_comments(self):
        assert _extract_first_keyword("/* block */ -- line\nMERGE INTO t") == "MERGE"

    def test_empty_string(self):
        assert _extract_first_keyword("") == ""

    def test_whitespace_only(self):
        assert _extract_first_keyword("   \n\t  ") == ""

    def test_comment_only(self):
        # Comment with no SQL after it
        assert _extract_first_keyword("/* only comment */") == ""

    def test_line_comment_no_trailing_newline(self):
        """Line comment at EOF without trailing newline."""
        assert _extract_first_keyword("-- only comment") == ""

    def test_replace_not_in_batch_verbs(self):
        """CUBRID does not support REPLACE; verify it's excluded from DML verbs."""
        assert "REPLACE" not in _DML_BATCH_VERBS
        assert _extract_first_keyword("REPLACE INTO t") == "REPLACE"

    def test_merge_in_batch_verbs(self):
        assert "MERGE" in _DML_BATCH_VERBS

    def test_select_not_in_batch_verbs(self):
        assert "SELECT" not in _DML_BATCH_VERBS
