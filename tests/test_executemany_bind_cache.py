"""``executemany()`` renders the same SQL with one placeholder split per call (#837).

The literal batch path tokenises the operation once per ``executemany()``
call instead of once per parameter set. The documented literal-binding
contract (``docs/PARAMETER_BINDING.md``) must not change by a single byte:
``tests/fixtures/executemany_rendered_sql.json`` holds the SQL that
``executemany()`` sent for the corpus below, generated with the per-row
renderer before the change, and every case must still render exactly that.
"""

from __future__ import annotations

import datetime
import enum
import json
import pathlib
import typing
from collections.abc import Sequence as AbcSequence
from decimal import Decimal
from typing import Any, NamedTuple
from zoneinfo import ZoneInfo
from unittest.mock import AsyncMock, MagicMock

import pytest
from hypothesis import given, settings, strategies as st

from pycubrid._cursor_common import bind_parameters, format_parameter, split_on_placeholders
from pycubrid.aio.cursor import AsyncCursor
from pycubrid.cursor import Cursor
from pycubrid.exceptions import DataError, ProgrammingError
from pycubrid.protocol import BatchExecutePacket
from pycubrid.types import Multiset, Sequence, Set

FIXTURE = pathlib.Path(__file__).parent / "fixtures" / "executemany_rendered_sql.json"
KST = datetime.timezone(datetime.timedelta(hours=9))
MINUS_0330 = datetime.timezone(datetime.timedelta(hours=-3, minutes=-30))


class _Color(enum.IntEnum):
    RED = 1


class _Perm(enum.IntFlag):
    READ = 4


class _Text(str):
    pass


class _Int(int):
    pass


class _Float(float):
    pass


class _Dec(Decimal):
    pass


class _Bytes(bytes):
    pass


class _Stamp(datetime.datetime):
    pass


class _Pair(NamedTuple):
    a: Any
    b: Any


class _Row(AbcSequence[Any]):
    """A sequence that is neither a tuple nor a list."""

    def __init__(self, *values: Any) -> None:
        self._values = values

    def __getitem__(self, index: Any) -> Any:
        return self._values[index]

    def __len__(self) -> int:
        return len(self._values)


SCALARS: list[Any] = [
    None,
    True,
    False,
    0,
    -1,
    2**70,
    _Color.RED,
    1.5,
    1e20,
    -0.0,
    Decimal("1.2300"),
    Decimal("-0.0001"),
    Decimal("1E+5"),
    "plain",
    "o'quote",
    "back\\slash",
    "both\\'mix",
    "한글 ?",
    "-- not a comment",
    _Text("sub'class"),
    b"\x00\xff",
    b"",
    bytearray(b"ab"),
    datetime.date(1, 2, 3),
    datetime.time(4, 5, 6, 789),
    datetime.datetime(2026, 10, 12, 1, 2, 3, 456789),
    datetime.datetime(2026, 10, 12, 1, 2, 3, 999, tzinfo=KST),
    datetime.datetime(2026, 1, 1, tzinfo=MINUS_0330),
    datetime.datetime(2026, 10, 12, 9, 30, tzinfo=ZoneInfo("Asia/Seoul")),
    # The repeated wall-clock hour of a DST change, second occurrence (#819).
    datetime.datetime(2026, 11, 1, 1, 30, fold=1, tzinfo=ZoneInfo("America/New_York")),
    datetime.time(7, 8, 9, tzinfo=KST),
    _Stamp(2026, 2, 3, 4, 5, 6, 7000),
    _Int(-42),
    _Float(2.25),
    _Bytes(b"\x01\x02"),
    _Perm.READ,
    Set([1, "a", None]),
    Multiset([]),
    Sequence(["x'y", 2]),
]

OPERATIONS: dict[str, str] = {
    "insert": "INSERT INTO t (a, b) VALUES (?, ?)",
    "quoted": "INSERT INTO t (a, b) VALUES ('?', ?) /* ? */ -- ?\n, (?)",
    "identifiers": "UPDATE \"q?\" SET `b?` = ?, [c?] = ? WHERE x = 'it''s ?'",
    "backslash": "DELETE FROM t WHERE a = 'x\\' OR ? = ?' AND b = ? // ?\n AND c = ?",
    "merge": "MERGE INTO t USING db_root ON (t.id = ?) WHEN MATCHED THEN UPDATE SET v = ?",
}


def _rows(operation: str, no_backslash_escapes: bool) -> list[Any]:
    """Every scalar in every position, as tuples, lists, NamedTuples and other sequences."""
    count = len(_split(operation, no_backslash_escapes)) - 1
    rows: list[Any] = []
    for index, value in enumerate(SCALARS):
        other = SCALARS[(index * 7 + 3) % len(SCALARS)]
        values = [value if i % 2 == 0 else other for i in range(count)]
        kind = index % 4
        if kind == 0:
            rows.append(tuple(values))
        elif kind == 1:
            rows.append(list(values))
        elif kind == 2 and count == 2:
            rows.append(_Pair(*values))
        else:
            rows.append(_Row(*values))
    return rows


def _split(operation: str, no_backslash_escapes: bool) -> list[str]:
    return split_on_placeholders(operation, no_backslash_escapes=no_backslash_escapes)


def _legacy_bind_parameters(
    operation: str, parameters: Any, *, no_backslash_escapes: bool = True
) -> str:
    """A frozen copy of ``bind_parameters`` before #837, the reference renderer."""
    if isinstance(parameters, typing.Sequence) and not isinstance(
        parameters, (str, bytes, bytearray)
    ):
        values = list(parameters)
    else:
        raise ProgrammingError("parameters must be a sequence")
    parts = split_on_placeholders(operation, no_backslash_escapes=no_backslash_escapes)
    if len(parts) - 1 != len(values):
        raise ProgrammingError("wrong number of parameters")
    result = [parts[0]]
    for index, value in enumerate(values, start=1):
        result.append(format_parameter(value, no_backslash_escapes=no_backslash_escapes))
        result.append(parts[index])
    return "".join(result)


def _cases() -> list[tuple[str, str, bool]]:
    return [
        (f"{name}-{'nbe' if mode else 'bse'}", operation, mode)
        for name, operation in OPERATIONS.items()
        for mode in (True, False)
    ]


def _conn(no_backslash_escapes: bool) -> MagicMock:
    conn = MagicMock()
    conn._no_backslash_escapes = no_backslash_escapes
    conn._physical_generation = 1
    conn._defer_close = MagicMock(return_value=False)
    conn._generation_for_binding = MagicMock(return_value=1)
    conn._wait_for_setup_if_needed = AsyncMock()
    conn.autocommit = False
    conn._protocol_version = 8
    return conn


def _sent_sql(conn: MagicMock) -> list[list[str]]:
    return [
        call.args[0].sql_list
        for call in conn._send_and_receive.call_args_list
        if isinstance(call.args[0], BatchExecutePacket)
    ]


def _fill(packet: object, **_: object) -> object:
    if isinstance(packet, BatchExecutePacket):
        packet.results = [(20, 1)] * len(packet.sql_list)
        packet.errors = []
    return packet


def render_with_cursor(
    operation: str, rows: list[Any], no_backslash_escapes: bool, asynchronous: bool
) -> list[str]:
    """Return the SQL list ``executemany()`` sends for *rows*."""
    conn = _conn(no_backslash_escapes)
    if asynchronous:
        import asyncio

        conn._send_and_receive = AsyncMock(side_effect=_fill)
        conn._generation_for_binding = AsyncMock(return_value=1)
        asyncio.run(AsyncCursor(conn).executemany(operation, rows))
    else:
        conn._send_and_receive = MagicMock(side_effect=_fill)
        Cursor(conn).executemany(operation, rows)
    (sent,) = _sent_sql(conn)
    return sent


def generate() -> dict[str, list[str]]:
    """Render the corpus through ``Cursor.executemany()`` of the imported ``pycubrid``.

    The fixture was written by running this against an unmodified copy of the
    package from before #837 (``PYTHONPATH`` pointing at a ``git archive`` of
    the parent commit).
    """
    return {
        case_id: render_with_cursor(op, _rows(op, mode), mode, asynchronous=False)
        for case_id, op, mode in _cases()
    }


# ---------------------------------------------------------------------------
# Byte identity with the SQL rendered before the change
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("asynchronous", [False, True], ids=["sync", "async"])
@pytest.mark.parametrize(("case_id", "operation", "mode"), _cases(), ids=[c[0] for c in _cases()])
def test_rendered_sql_is_byte_identical_to_the_fixture(
    case_id: str, operation: str, mode: bool, asynchronous: bool
) -> None:
    expected = json.loads(FIXTURE.read_text(encoding="utf-8"))[case_id]
    rendered = render_with_cursor(operation, _rows(operation, mode), mode, asynchronous)
    assert [sql.encode("utf-8") for sql in rendered] == [sql.encode("utf-8") for sql in expected]


def test_fixture_covers_every_case() -> None:
    fixture = json.loads(FIXTURE.read_text(encoding="utf-8"))
    assert sorted(fixture) == sorted(case_id for case_id, _op, _mode in _cases())
    assert all(len(fixture[c]) == len(SCALARS) for c in fixture)


_VALUES = st.one_of(
    st.none(),
    st.booleans(),
    st.integers(),
    st.floats(allow_nan=False, allow_infinity=False),
    st.decimals(allow_nan=False, allow_infinity=False, places=4),
    st.text(),
    st.binary(),
    st.datetimes(),
    st.dates(),
    st.times(),
)


@st.composite
def _parameter_sets(draw: Any, count: int) -> list[Any]:
    sets: list[Any] = []
    for _ in range(draw(st.integers(min_value=1, max_value=6))):
        size = count + draw(st.sampled_from([0, 0, 0, 0, -1, 1]))
        values = draw(st.lists(_VALUES, min_size=max(size, 0), max_size=max(size, 0)))
        kind = draw(st.sampled_from(["tuple", "list", "row", "pair", "str", "dict"]))
        if kind == "tuple":
            sets.append(tuple(values))
        elif kind == "list":
            sets.append(values)
        elif kind == "row":
            sets.append(_Row(*values))
        elif kind == "pair" and len(values) == 2:
            sets.append(_Pair(*values))
        elif kind == "str":
            sets.append("".join(map(str, values)))
        elif kind == "dict":
            sets.append(dict(enumerate(values)))
        else:
            sets.append(tuple(values))
    return sets


@settings(deadline=None, max_examples=300)
@given(data=st.data(), name=st.sampled_from(sorted(OPERATIONS)), mode=st.booleans())
def test_matches_the_frozen_per_row_renderer(data: Any, name: str, mode: bool) -> None:
    operation = OPERATIONS[name]
    sets = data.draw(_parameter_sets(len(_split(operation, mode)) - 1))
    try:
        expected: Any = [
            _legacy_bind_parameters(operation, row, no_backslash_escapes=mode) for row in sets
        ]
    except (ProgrammingError, DataError) as exc:
        expected = (type(exc), str(exc))
    try:
        actual: Any = Cursor(_conn(mode))._bind_parameter_sets(operation, sets)
    except (ProgrammingError, DataError) as exc:
        actual = (type(exc), str(exc))
    assert actual == expected
    # The single-statement path shares the renderer and must agree as well.
    if isinstance(expected, list):
        assert [bind_parameters(operation, r, no_backslash_escapes=mode) for r in sets] == expected


# ---------------------------------------------------------------------------
# One split per call, and the same errors in the same order
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("asynchronous", [False, True], ids=["sync", "async"])
def test_operation_is_split_once_per_call(
    monkeypatch: pytest.MonkeyPatch, asynchronous: bool
) -> None:
    calls: list[str] = []

    def counting(sql: str, *, no_backslash_escapes: bool = True) -> list[str]:
        calls.append(sql)
        return split_on_placeholders(sql, no_backslash_escapes=no_backslash_escapes)

    monkeypatch.setattr("pycubrid._cursor_common.split_on_placeholders", counting)
    rows = [(i, str(i)) for i in range(50)]
    sent = render_with_cursor(OPERATIONS["insert"], rows, True, asynchronous)
    assert len(sent) == 50
    assert calls == [OPERATIONS["insert"]]


def _old_error(operation: str, rows: list[Any], mode: bool) -> tuple[type, str]:
    with pytest.raises((ProgrammingError, DataError)) as raised:
        for row in rows:
            _legacy_bind_parameters(operation, row, no_backslash_escapes=mode)
    return type(raised.value), str(raised.value)


# Claims to be a tuple through ``__class__`` only; the type check must still reject it.
_SPOOF = type(
    "_Spoof",
    (),
    {
        "__class__": property(lambda self: tuple),
        "__iter__": lambda self: iter((1, 2)),
        "__len__": lambda self: 2,
    },
)()


@pytest.mark.parametrize("asynchronous", [False, True], ids=["sync", "async"])
@pytest.mark.parametrize(
    "bad",
    [
        "ab",  # a string is not a parameter set
        b"ab",
        {"a": 1, "b": 2},
        _SPOOF,
        (1,),  # wrong count
        [1, 2, 3],
        (object(), 1),  # unsupported value
        (float("nan"), 1),
        (Decimal("1" * 39), 1),
        ("nul\x00", 1),
    ],
    ids=["str", "bytes", "dict", "spoof", "short", "long", "object", "nan", "digits", "nul"],
)
def test_first_bad_set_raises_what_it_raised_before(bad: Any, asynchronous: bool) -> None:
    operation = OPERATIONS["insert"]
    rows: list[Any] = [(1, "a"), [2, "b"], bad, ("never", "reached")]
    expected = _old_error(operation, rows, True)
    conn = _conn(True)
    with pytest.raises((ProgrammingError, DataError)) as raised:
        if asynchronous:
            import asyncio

            conn._send_and_receive = AsyncMock(side_effect=_fill)
            conn._generation_for_binding = AsyncMock(return_value=1)
            asyncio.run(AsyncCursor(conn).executemany(operation, rows))
        else:
            conn._send_and_receive = MagicMock(side_effect=_fill)
            Cursor(conn).executemany(operation, rows)
    assert (type(raised.value), str(raised.value)) == expected
    assert _sent_sql(conn) == []


def test_generator_is_consumed_once() -> None:
    consumed: list[int] = []

    def rows() -> Any:
        for i in range(3):
            consumed.append(i)
            yield (i, "x")

    conn = _conn(True)
    conn._send_and_receive = MagicMock(side_effect=_fill)
    Cursor(conn).executemany(OPERATIONS["insert"], rows())
    assert consumed == [0, 1, 2]
    assert _sent_sql(conn) == [[f"INSERT INTO t (a, b) VALUES ({i}, 'x')" for i in range(3)]]


def test_escape_mode_is_resolved_only_for_a_first_set() -> None:
    # Unnegotiated mode: an empty generator renders nothing and raises nothing,
    # as the per-row comprehension did; the first set raises InterfaceError first.
    from pycubrid.exceptions import InterfaceError

    cursor = Cursor(_conn(True))
    cursor._connection._no_backslash_escapes = None
    assert cursor._bind_parameter_sets(OPERATIONS["insert"], iter(())) == []
    with pytest.raises(InterfaceError, match="escape mode not negotiated"):
        cursor._bind_parameter_sets(OPERATIONS["insert"], ["not a sequence"])


def test_decimal_subclass_matches_the_frozen_renderer() -> None:
    # Kept out of the golden corpus: without the C decimal module (some CPython
    # builds) a Decimal subclass parameter raises ProgrammingError by contract.
    pytest.importorskip("_decimal")
    rows = [(_Dec("-12.50"), 1), [_Dec("1E+3"), _Dec("0.000")]]
    expected = [_legacy_bind_parameters(OPERATIONS["insert"], row) for row in rows]
    assert Cursor(_conn(True))._bind_parameter_sets(OPERATIONS["insert"], rows) == expected
    assert expected == [
        "INSERT INTO t (a, b) VALUES (-12.50, 1)",
        "INSERT INTO t (a, b) VALUES (1000, 0.000)",
    ]
