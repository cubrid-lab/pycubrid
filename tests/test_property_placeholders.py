"""Property-based fuzzing of the ``?`` placeholder tokenizer (issue #338).

These tests exercise :func:`pycubrid._cursor_common.split_on_placeholders` and
:func:`pycubrid._cursor_common.bind_parameters` with generated SQL fragments that
mix real bind placeholders with ``?`` characters buried inside string literals,
quoted/bracketed/backticked identifiers, and line/block comments.

The generator is *structural*: it builds SQL from tokens whose placeholder
contribution is known by construction, so the expected placeholder count and the
original text are both known exactly. That lets us assert:

* placeholders inside literals / identifiers / comments are never counted;
* real bind placeholders are counted exactly once each;
* ``split_on_placeholders`` parts rejoined with ``?`` reproduce the input;
* ``bind_parameters`` never miscounts for a valid generated statement.

Both CUBRID escaping modes are covered (``no_backslash_escapes`` True/False).

Scope note: this is intentionally *not* a full SQL parser. Only the quoting and
comment forms that ``split_on_placeholders`` already models are generated.
"""

from __future__ import annotations

import sys

import pytest
from hypothesis import given, settings, strategies as st

from pycubrid._cursor_common import (
    bind_parameters,
    split_on_placeholders,
)

# ---------------------------------------------------------------------------
# Token model
#
# Each token carries the SQL text it contributes and the number of REAL
# placeholders it contributes (0 for everything except a bare "?"). By summing
# the per-token counts we know the ground-truth placeholder count for the whole
# statement without re-deriving it from the tokenizer under test.
# ---------------------------------------------------------------------------


# Characters that are safe to drop into "plain" (out-of-literal) SQL text
# without accidentally opening a literal/identifier/comment context. Notably
# excludes: ' " ` [ ] ? / -  (all context-significant to the tokenizer).
_PLAIN_ALPHABET = "abcXYZ_0129 =<>().,;+*:@#$&!|~^ \t\n"

plain_text = st.text(alphabet=_PLAIN_ALPHABET, min_size=0, max_size=8)

# Body text allowed inside single-quoted string literals. We separately inject
# the escape forms (doubled quote, backslash escapes) via dedicated tokens, so
# here we exclude the raw quote and backslash to keep bodies unambiguous.
_LITERAL_BODY = "abc XYZ 019 ?-- /* */ [](){}=<>@#`\t\n"
literal_body = st.text(alphabet=_LITERAL_BODY, min_size=0, max_size=8)

# Body allowed inside "..." / `...` / [...] identifiers and inside comments.
_IDENT_BODY = "abc XYZ 019 ?'-- /* @#$\t"
ident_body = st.text(alphabet=_IDENT_BODY, min_size=0, max_size=8)

# Comment body: must not contain the closing sequence "*/" for block comments
# or a newline for line comments; we handle those per-token below.
_COMMENT_BODY = "abc XYZ 019 ?'\"`[]-=+@#$ \t"
comment_body = st.text(alphabet=_COMMENT_BODY, min_size=0, max_size=10)


def _placeholder_token() -> st.SearchStrategy[tuple[str, int]]:
    return st.just(("?", 1))


def _plain_token() -> st.SearchStrategy[tuple[str, int]]:
    return plain_text.map(lambda s: (s, 0))


@st.composite
def _string_literal_token(draw: st.DrawFn, *, no_backslash_escapes: bool) -> tuple[str, int]:
    """A single-quoted string literal that may embed ``?`` and escapes."""
    pieces: list[str] = []
    n = draw(st.integers(min_value=0, max_value=4))
    for _ in range(n):
        kind = draw(st.sampled_from(["body", "doubled_quote", "question", "backslash"]))
        if kind == "body":
            pieces.append(draw(literal_body))
        elif kind == "doubled_quote":
            pieces.append("''")
        elif kind == "question":
            pieces.append("?")
        else:  # backslash
            # A backslash followed by a benign char. Under
            # no_backslash_escapes=False this escapes the next char (so we must
            # not let it escape the terminating quote); under True it is an
            # ordinary character. Either way it contributes zero placeholders.
            if no_backslash_escapes:
                # Backslash is ordinary; still fine to include, but avoid a
                # trailing backslash immediately before the closing quote from
                # producing "\'" which would be read as an escaped quote when
                # the *same* SQL is later reused — keep it simple and pair it.
                pieces.append("\\\\")
            else:
                pieces.append("\\?")  # escaped '?', still zero real placeholders
    return "'" + "".join(pieces) + "'", 0


@st.composite
def _dquote_ident_token(draw: st.DrawFn) -> tuple[str, int]:
    """A double-quoted identifier that may embed ``?`` and doubled quotes."""
    pieces: list[str] = []
    n = draw(st.integers(min_value=0, max_value=3))
    for _ in range(n):
        kind = draw(st.sampled_from(["body", "doubled", "question"]))
        if kind == "body":
            pieces.append(draw(ident_body))
        elif kind == "doubled":
            pieces.append('""')
        else:
            pieces.append("?")
    return '"' + "".join(pieces) + '"', 0


@st.composite
def _backtick_ident_token(draw: st.DrawFn) -> tuple[str, int]:
    body = draw(ident_body)
    return "`" + body + "`", 0


@st.composite
def _bracket_ident_token(draw: st.DrawFn) -> tuple[str, int]:
    # Bracket identifiers terminate on the first ']', so the body must not
    # contain ']'. ident_body already excludes ']'.
    body = draw(ident_body)
    return "[" + body + "]", 0


@st.composite
def _line_comment_token(draw: st.DrawFn) -> tuple[str, int]:
    lead = draw(st.sampled_from(["--", "//"]))
    body = draw(comment_body)
    # Line comments end at newline; always terminate with one so following
    # tokens are back in plain context.
    return lead + body + "\n", 0


@st.composite
def _block_comment_token(draw: st.DrawFn) -> tuple[str, int]:
    body = draw(comment_body)
    # comment_body cannot contain '*' followed by '/', but to be safe strip any
    # accidental "*/" the alphabet can't actually produce (no '/').
    return "/*" + body + "*/", 0


def _token_strategies(*, no_backslash_escapes: bool) -> st.SearchStrategy[tuple[str, int]]:
    return st.one_of(
        _placeholder_token(),
        _plain_token(),
        _string_literal_token(no_backslash_escapes=no_backslash_escapes),
        _dquote_ident_token(),
        _backtick_ident_token(),
        _bracket_ident_token(),
        _line_comment_token(),
        _block_comment_token(),
    )


@st.composite
def sql_and_expected(draw: st.DrawFn, *, no_backslash_escapes: bool) -> tuple[str, int]:
    """Build a SQL string and its ground-truth real-placeholder count."""
    tokens = draw(
        st.lists(
            _token_strategies(no_backslash_escapes=no_backslash_escapes),
            min_size=0,
            max_size=12,
        )
    )
    sql = "".join(text for text, _ in tokens)
    count = sum(c for _, c in tokens)
    return sql, count


# ---------------------------------------------------------------------------
# Properties
# ---------------------------------------------------------------------------

_MODES = st.booleans()


class TestPlaceholderTokenizer:
    @given(mode=_MODES, data=st.data())
    @settings(deadline=None)
    def test_placeholder_count_matches_ground_truth(self, mode: bool, data: st.DataObject) -> None:
        """split_on_placeholders counts exactly the real bind placeholders."""
        sql, expected = data.draw(sql_and_expected(no_backslash_escapes=mode))
        parts = split_on_placeholders(sql, no_backslash_escapes=mode)
        assert len(parts) - 1 == expected

    @given(mode=_MODES, data=st.data())
    @settings(deadline=None)
    def test_split_reconstruction_is_lossless(self, mode: bool, data: st.DataObject) -> None:
        """Rejoining the parts with '?' reproduces the original SQL exactly.

        This holds because split only ever cuts at a real placeholder: the
        non-placeholder text is preserved verbatim in the parts.
        """
        sql, _ = data.draw(sql_and_expected(no_backslash_escapes=mode))
        parts = split_on_placeholders(sql, no_backslash_escapes=mode)
        assert "?".join(parts) == sql

    @given(mode=_MODES, data=st.data())
    @settings(deadline=None)
    def test_bind_parameters_matches_count(self, mode: bool, data: st.DataObject) -> None:
        """bind_parameters accepts exactly `expected` params and no other count.

        Uses ``None`` values so formatting never rejects a value for a reason
        unrelated to counting.
        """
        sql, expected = data.draw(sql_and_expected(no_backslash_escapes=mode))
        params = [None] * expected
        # Correct count binds without a count error.
        bound = bind_parameters(sql, params, no_backslash_escapes=mode)
        assert bound.count("NULL") >= expected  # every placeholder became NULL

        # One too few / one too many must raise a count error.
        from pycubrid.exceptions import ProgrammingError

        with pytest.raises(ProgrammingError):
            bind_parameters(sql, [None] * (expected + 1), no_backslash_escapes=mode)
        if expected > 0:
            with pytest.raises(ProgrammingError):
                bind_parameters(sql, [None] * (expected - 1), no_backslash_escapes=mode)

    @given(mode=_MODES, data=st.data())
    @settings(deadline=None)
    def test_no_placeholder_inside_contexts(self, mode: bool, data: st.DataObject) -> None:
        """A statement whose only ``?`` live inside literals/idents/comments has 0.

        Built from non-placeholder tokens only, so the tokenizer must report a
        single part (zero placeholders) regardless of how many ``?`` characters
        the literals/comments contain.
        """
        non_ph_tokens = data.draw(
            st.lists(
                st.one_of(
                    _plain_token(),
                    _string_literal_token(no_backslash_escapes=mode),
                    _dquote_ident_token(),
                    _backtick_ident_token(),
                    _bracket_ident_token(),
                    _line_comment_token(),
                    _block_comment_token(),
                ),
                min_size=0,
                max_size=10,
            )
        )
        sql = "".join(t for t, _ in non_ph_tokens)
        parts = split_on_placeholders(sql, no_backslash_escapes=mode)
        assert len(parts) == 1
        assert parts[0] == sql


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
