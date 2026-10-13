"""A negative literal after a minus sign never forms a ``--`` comment (#893).

The literal binding path pastes each rendered value into the SQL text. A
negative number placed right after a ``-`` (``x-?``) used to produce ``x--5``,
which CUBRID reads as a line comment, silently dropping the rest of the
statement (a ``WHERE`` clause, for example). The renderer now separates the
two minus signs with one space, and changes nothing else.
"""

from __future__ import annotations

import asyncio
import enum
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from hypothesis import given, settings, strategies as st

from pycubrid._cursor_common import bind_parameters, format_parameter
from pycubrid.aio.cursor import AsyncCursor
from pycubrid.cursor import Cursor
from pycubrid.protocol import BatchExecutePacket, PrepareAndExecutePacket
from pycubrid.types import Multiset, Sequence, Set

UPDATE = "UPDATE a SET x = x-? WHERE id = ?"


class _Signed(enum.IntEnum):
    MINUS = -3


class _Int(int):
    pass


NEGATIVES: list[tuple[Any, str]] = [
    (-5, "-5"),
    (-(2**70), "-1180591620717411303424"),
    (_Int(-7), "-7"),
    (_Signed.MINUS, "-3"),
    (-2.5, "-2.5"),
    (-1e-07, "-1e-07"),
    (-1e20, "-1e+20"),
    (-0.0, "-0.0"),
    (Decimal("-1.5"), "-1.5"),
    (Decimal("-0.0001"), "-0.0001"),
    # Exponent forms are expanded by format(value, "f").
    (Decimal("-1E+3"), "-1000"),
    (Decimal("-1E-7"), "-0.0000001"),
]


@pytest.mark.parametrize(("value", "literal"), NEGATIVES, ids=[n[1] for n in NEGATIVES])
@pytest.mark.parametrize("mode", [True, False], ids=["nbe", "bse"])
def test_negative_after_minus_is_separated(value: Any, literal: str, mode: bool) -> None:
    assert format_parameter(value, no_backslash_escapes=mode) == literal
    sql = bind_parameters(UPDATE, (value, 1), no_backslash_escapes=mode)
    assert sql == "UPDATE a SET x = x- %s WHERE id = 1" % literal
    assert "--" not in sql


@pytest.mark.parametrize(
    ("operation", "values", "expected"),
    [
        ("SELECT 1-?", (-2,), "SELECT 1- -2"),
        ("SELECT -?", (-2,), "SELECT - -2"),
        ("SELECT ?-?", (-1, -1), "SELECT -1- -1"),
        ("SELECT 1 -?", (-2,), "SELECT 1 - -2"),
        # Not after a minus sign: unchanged.
        ("SELECT 1 - ?", (-2,), "SELECT 1 - -2"),
        ("SELECT ?", (-2,), "SELECT -2"),
        ("SELECT * FROM t WHERE a IN (?, ?)", (-1, -2), "SELECT * FROM t WHERE a IN (-1, -2)"),
        ("SELECT * FROM t LIMIT ?", (-1,), "SELECT * FROM t LIMIT -1"),
        ("SELECT 1+?", (-2,), "SELECT 1+-2"),
        ("SELECT 1/?", (-2,), "SELECT 1/-2"),
        ("SELECT 1*?", (-2,), "SELECT 1*-2"),
        ("SELECT ??", (-1, -2), "SELECT -1-2"),
        # Non-negative values and non-numeric values after a minus: unchanged.
        ("SELECT 1-?", (2,), "SELECT 1-2"),
        ("SELECT 1-?", (0,), "SELECT 1-0"),
        ("SELECT 1-?", (True,), "SELECT 1-1"),
        ("SELECT 1-?", (None,), "SELECT 1-NULL"),
        ("SELECT 'a'-?", ("-1",), "SELECT 'a'-'-1'"),
        # A minus inside a string or a comment is not SQL text before the value.
        ("SELECT '-', ?", (-1,), "SELECT '-', -1"),
        ("SELECT /* - */ ?", (-1,), "SELECT /* - */ -1"),
        # Collection elements follow "{" or ", ", never a minus sign.
        (
            "SELECT 1-?, ?, ?",
            (Set([-1, -2]), Multiset([-1.5]), Sequence([Decimal("-3")])),
            "SELECT 1-SET{-1, -2}, MULTISET{-1.5}, SEQUENCE{-3}",
        ),
    ],
)
def test_rendering_around_minus(operation: str, values: tuple[Any, ...], expected: str) -> None:
    assert bind_parameters(operation, values) == expected


_NUMBERS = st.one_of(
    st.integers(max_value=-1),
    st.floats(max_value=-0.0, allow_nan=False, allow_infinity=False),
    st.decimals(max_value=0, allow_nan=False, allow_infinity=False, places=6),
)


@settings(deadline=None, max_examples=300)
@given(
    prefix=st.sampled_from(["SELECT 1-", "SELECT - -", "SELECT 1 -", "SELECT x--y\n-"]),
    value=_NUMBERS,
)
def test_no_new_double_minus(prefix: str, value: Any) -> None:
    operation = prefix + "? AND 1"
    sql = bind_parameters(operation, (value,))
    # The value is never glued onto the minus signs written in the SQL.
    head, _, _ = operation.partition("?")
    assert sql.startswith(head)
    rest = sql[len(head) :]
    literal = format_parameter(value)
    if literal.startswith("-"):
        assert rest == " " + literal + " AND 1"
    else:
        assert rest == literal + " AND 1"


def _conn() -> MagicMock:
    conn = MagicMock()
    conn._no_backslash_escapes = True
    conn._physical_generation = 1
    conn._defer_close = MagicMock(return_value=False)
    conn._generation_for_binding = MagicMock(return_value=1)
    conn._wait_for_setup_if_needed = AsyncMock()
    conn.autocommit = False
    conn._protocol_version = 8
    conn._decode_collections = True
    conn._json_deserializer = None
    return conn


class _Sent(Exception):
    pass


def _capture(sent: list[Any]) -> Any:
    def side_effect(packet: object, **_: object) -> object:
        if isinstance(packet, PrepareAndExecutePacket):
            sent.append(packet.sql)
            raise _Sent
        if isinstance(packet, BatchExecutePacket):
            sent.append(list(packet.sql_list))
            packet.results = [(20, 1)] * len(packet.sql_list)
            packet.errors = []
        return packet

    return side_effect


EXPECTED = "UPDATE a SET x = x- -5 WHERE id = 1"


def test_sync_execute() -> None:
    sent: list[Any] = []
    conn = _conn()
    conn._send_and_receive = MagicMock(side_effect=_capture(sent))
    with pytest.raises(_Sent):
        Cursor(conn).execute(UPDATE, (-5, 1))
    assert sent == [EXPECTED]


def test_async_execute() -> None:
    sent: list[Any] = []
    conn = _conn()
    conn._send_and_receive = AsyncMock(side_effect=_capture(sent))
    conn._generation_for_binding = AsyncMock(return_value=1)
    with pytest.raises(_Sent):
        asyncio.run(AsyncCursor(conn).execute(UPDATE, (-5, 1)))
    assert sent == [EXPECTED]


ROWS = [(-5, 1), (-2.5, 2), (Decimal("-1.5"), 3)]
EXPECTED_BATCH = [
    "UPDATE a SET x = x- -5 WHERE id = 1",
    "UPDATE a SET x = x- -2.5 WHERE id = 2",
    "UPDATE a SET x = x- -1.5 WHERE id = 3",
]


def test_sync_executemany() -> None:
    sent: list[Any] = []
    conn = _conn()
    conn._send_and_receive = MagicMock(side_effect=_capture(sent))
    Cursor(conn).executemany(UPDATE, ROWS)
    assert sent == [EXPECTED_BATCH]


def test_async_executemany() -> None:
    sent: list[Any] = []
    conn = _conn()
    conn._send_and_receive = AsyncMock(side_effect=_capture(sent))
    conn._generation_for_binding = AsyncMock(return_value=1)
    asyncio.run(AsyncCursor(conn).executemany(UPDATE, ROWS))
    assert sent == [EXPECTED_BATCH]
