"""CUBRIDdb wrapper collection call shapes: execute set_type and executemany (#610)."""

from __future__ import annotations

from collections import namedtuple
from collections.abc import Iterator
from datetime import date, datetime, time
from decimal import Decimal
from typing import Any

import pytest

from pycubrid.compat import cubriddb, native
from pycubrid.constants import CUBRIDDataType, CUBRIDStatementType
from pycubrid.exceptions import InterfaceError, NotSupportedError, ProgrammingError
from pycubrid.protocol import ColumnMetaData, ExecutePacket, PreparePacket

from .test_compat_collections import OFFICIAL_BIND_PAIRS, _bind_pair
from .test_compat_prepared import DSN
from .test_compat_wrapper_rows import RowsDriver

INT, STRING, NUMERIC = CUBRIDDataType.INT, CUBRIDDataType.STRING, CUBRIDDataType.NUMERIC
FLOAT, DATE, TIME = CUBRIDDataType.FLOAT, CUBRIDDataType.DATE, CUBRIDDataType.TIME
INSERT3 = "INSERT INTO t VALUES (?, ?, ?)"


@pytest.fixture
def wrapped(monkeypatch: pytest.MonkeyPatch) -> Iterator[cubriddb.Connection]:
    RowsDriver.created.clear()
    monkeypatch.setattr(native, "_DriverConnection", RowsDriver)
    conn = cubriddb.Connection(DSN)
    driver = conn.connection._driver
    driver.columns = [ColumnMetaData(name="v", column_type=INT, precision=10, scale=0)]
    driver.result = [(1,), (2,)]
    try:
        yield conn
    finally:
        conn.close()


@pytest.fixture
def imported(monkeypatch: pytest.MonkeyPatch) -> list[tuple[tuple[Any, ...], int]]:
    """Record every native imports(data, type) call the wrapper makes."""
    calls: list[tuple[tuple[Any, ...], int]] = []
    original = native.set.imports

    def spy(self: Any, data: tuple[Any, ...], type_: int, /, **kwargs: Any) -> None:
        assert kwargs == {}  # the wrapper keeps the official SET kind
        calls.append((data, type_))
        original(self, data, type_)

    monkeypatch.setattr(native.set, "imports", spy)
    return calls


def _driver(conn: cubriddb.Connection) -> RowsDriver:
    driver = conn.connection._driver
    assert isinstance(driver, RowsDriver)
    return driver


def _executed(conn: cubriddb.Connection) -> list[ExecutePacket]:
    return [p for p, _ in _driver(conn).requests if isinstance(p, ExecutePacket)]


def _prepared(conn: cubriddb.Connection) -> list[PreparePacket]:
    return [p for p, _ in _driver(conn).requests if isinstance(p, PreparePacket)]


def test_explicit_set_type_applies_to_every_collection(
    wrapped: cubriddb.Connection, imported: list[Any]
) -> None:
    cur = wrapped.cursor()
    result = cur.execute(INSERT3, (1, ("a", "b"), ["1", "2"]), set_type=NUMERIC)
    assert result == 1 and cur.rowcount == 1 and cur.description is None
    assert imported == [(("a", "b"), NUMERIC), (("1", "2"), NUMERIC)]
    (packet,) = _executed(wrapped)
    assert [b.type_code for b in packet.bindings] == [INT, CUBRIDDataType.SET, CUBRIDDataType.SET]


@pytest.mark.parametrize(
    ("set_type", "codes"),
    [
        ([None, STRING, INT], [STRING, INT]),
        ((None, None, NUMERIC), [STRING, NUMERIC]),  # a None entry is inferred
        ([INT], [STRING, INT]),  # positions past the list are inferred
        ([INT, NUMERIC], [NUMERIC, INT]),  # entry 1 belongs to the scalar
    ],
)
def test_per_position_set_type(
    wrapped: cubriddb.Connection, imported: list[Any], set_type: Any, codes: list[int]
) -> None:
    wrapped.cursor().execute(INSERT3, (1, ("g", "h"), (5, 6)), set_type=set_type)
    assert [code for _data, code in imported] == codes


@pytest.mark.parametrize(
    ("elements", "code", "data"),
    [
        ((1, 2), INT, ("1", "2")),
        ((True, 0), INT, ("True", "0")),  # bool is an int, sent as its str() text
        ((1.5,), FLOAT, ("1.5",)),
        ((Decimal("1.25"),), NUMERIC, ("1.25",)),
        ((date(2024, 1, 15),), DATE, ("2024-01-15",)),
        ((datetime(2024, 1, 15, 13, 30, 45),), DATE, ("2024-01-15 13:30:45",)),
        ((time(13, 30, 45),), TIME, ("13:30:45",)),
        (("a", "한"), STRING, ("a", "한")),
        ((), STRING, ()),
        ((None,), STRING, (None,)),
        ((None, 3, None), INT, (None, "3", None)),  # inference skips None
    ],
)
def test_inferred_codes_and_str_adaptation(
    wrapped: cubriddb.Connection,
    imported: list[Any],
    elements: tuple[Any, ...],
    code: int,
    data: tuple[Any, ...],
) -> None:
    wrapped.cursor().execute("INSERT INTO t VALUES (?)", (elements,))
    assert imported == [(data, code)]


@pytest.mark.parametrize("container", [list, tuple, set, frozenset])
def test_every_collection_container(
    wrapped: cubriddb.Connection, imported: list[Any], container: type
) -> None:
    wrapped.cursor().execute("INSERT INTO t VALUES (?)", [container([7])])
    assert imported == [(("7",), INT)]


def test_explicit_code_still_adapts_with_str(
    wrapped: cubriddb.Connection, imported: list[Any]
) -> None:
    wrapped.cursor().execute("INSERT INTO t VALUES (?)", ((1, 2.5, None),), set_type=STRING)
    assert imported == [(("1", "2.5", None), STRING)]


@pytest.mark.parametrize(("data", "type_", "pair"), OFFICIAL_BIND_PAIRS)
def test_wire_bytes_equal_native_imports_and_official_capture(
    wrapped: cubriddb.Connection, data: tuple[Any, ...], type_: int, pair: str
) -> None:
    wrapped.cursor().execute("INSERT INTO t VALUES (?)", (data,), set_type=type_)
    (packet,) = _executed(wrapped)
    assert _bind_pair(packet.bindings[0]) == bytes.fromhex(pair)
    native_cur = wrapped.connection.cursor()
    native_cur.prepare("INSERT INTO t VALUES (?)")
    s = wrapped.connection.set()
    s.imports(data, type_)
    native_cur.bind_set(1, s)
    native_cur.execute()
    assert _executed(wrapped)[1].bindings == packet.bindings


@pytest.mark.parametrize(
    ("args", "set_type", "error"),
    [
        (((1, "z"),), None, TypeError),
        (((Decimal(1), 1.5),), None, TypeError),
        (((date(2024, 1, 1), time(1)),), None, TypeError),
        (((b"\x14",),), None, NotSupportedError),
        (((bytearray(b"\x14"),),), None, NotSupportedError),
        ((("1",),), CUBRIDDataType.BIT, NotSupportedError),
        ((("1",),), CUBRIDDataType.VARBIT, NotSupportedError),
        (((b"\x14",),), STRING, NotSupportedError),
        (((object(),),), None, ProgrammingError),
        (((object(),),), STRING, ProgrammingError),
        (((("nested",),),), None, ProgrammingError),
        ((("1",),), "8", InterfaceError),
        ((("1",),), True, InterfaceError),
        ((("1",),), 8.0, InterfaceError),
        ((("1",),), ["8"], InterfaceError),
        ((("1",),), {1: 8}, InterfaceError),
        ((("a\x00b",),), None, ProgrammingError),
        (({"k": 1},), None, ProgrammingError),
        (((x for x in (1,)),), None, ProgrammingError),
        ((b"12",), None, ProgrammingError),
        ((range(2),), None, ProgrammingError),
        ((1.5,), None, ProgrammingError),
        ((True,), None, ProgrammingError),
        ({1, 2}, None, ProgrammingError),  # a top-level set is not positional
    ],
)
def test_argument_errors_before_any_io(
    wrapped: cubriddb.Connection, args: Any, set_type: Any, error: type[Exception]
) -> None:
    driver = _driver(wrapped)
    cur = wrapped.cursor()
    cur.execute("SELECT ?", (1,))
    assert cur.rowcount == 2 and cur.description is not None
    before = len(driver.requests)
    with pytest.raises(error):
        cur.execute("INSERT INTO t VALUES (?)", args, set_type=set_type)
    assert len(driver.requests) == before
    # Like upstream's prepare-then-bind, the old result and snapshot are gone.
    assert (cur.rowcount, cur.description, cur._native_description) == (-1, None, None)
    with pytest.raises(InterfaceError, match="invalidated"):
        cur.fetchone()
    assert len(driver.requests) == before
    assert cur.execute("SELECT ?", (1,)) == 2 and cur.fetchone() == (1,)


def test_set_type_sequence_subclasses_are_per_position(
    wrapped: cubriddb.Connection, imported: list[Any]
) -> None:
    Codes = namedtuple("Codes", "id tags")
    wrapped.cursor().execute(
        "INSERT INTO t VALUES (?, ?)", (1, ("1",)), set_type=Codes(None, NUMERIC)
    )
    assert imported == [(("1",), NUMERIC)]


def test_unused_bad_set_type_is_ignored_like_upstream(wrapped: cubriddb.Connection) -> None:
    assert wrapped.cursor().execute("SELECT ?", (1,), set_type="not a code") == 2


def test_failed_execute_does_not_leave_bindings_for_the_next_call(
    wrapped: cubriddb.Connection,
) -> None:
    cur = wrapped.cursor()
    cur.execute("INSERT INTO t VALUES (?, ?)", (1, (2,)))
    with pytest.raises(ProgrammingError, match="out of range"):
        cur.execute("INSERT INTO t VALUES (?)", (1, (2,)))
    with pytest.raises(ProgrammingError, match="unbound"):
        cur.execute("INSERT INTO t VALUES (?, ?)", (1,))
    assert len(_executed(wrapped)) == 1


def test_executemany_prepares_once_and_keeps_the_last_snapshot(
    wrapped: cubriddb.Connection, imported: list[Any]
) -> None:
    cur = wrapped.cursor()
    rows = [(1, (3, 1, 3), ["a"]), [2, frozenset({"x"}), ()], (3, None, None)]
    assert cur.executemany(INSERT3, rows) is None
    assert len(_prepared(wrapped)) == 1
    executed = _executed(wrapped)
    assert [p.query_handle for p in executed] == [41, 41, 41]
    assert [b.type_code for b in executed[2].bindings] == [INT, 0, 0]
    assert imported == [(("3", "1", "3"), INT), (("a",), STRING), (("x",), STRING), ((), STRING)]
    assert cur.rowcount == 1 and cur.description is None


def test_executemany_accepts_any_iterable_and_select_snapshot(
    wrapped: cubriddb.Connection,
) -> None:
    cur = wrapped.cursor()
    cur.executemany("SELECT ? FROM t", (args for args in [(1,), 2]))
    assert len(_executed(wrapped)) == 2
    assert cur.rowcount == 2 and cur.description == (("v", INT, 0, 0, 10, 0, 0),)
    assert cur.fetchall() == [(1,), (2,)]


def test_executemany_empty_prepares_only(wrapped: cubriddb.Connection) -> None:
    cur = wrapped.cursor()
    cur.execute("SELECT ? FROM t", (1,))
    assert cur.rowcount == 2 and cur.description is not None
    assert cur.executemany("INSERT INTO t VALUES (?)", []) is None
    assert len(_prepared(wrapped)) == 2 and len(_executed(wrapped)) == 1
    assert cur.rowcount == -1 and cur.description is None


def test_executemany_checks_every_group_before_prepare(wrapped: cubriddb.Connection) -> None:
    driver = _driver(wrapped)
    cur = wrapped.cursor()
    cur.execute("SELECT ? FROM t", (1,))
    before = len(driver.requests)
    with pytest.raises(TypeError):
        cur.executemany("INSERT INTO t VALUES (?)", [((1,),), ((1, "z"),)])
    assert (cur.rowcount, cur.description) == (-1, None)
    with pytest.raises(InterfaceError, match="invalidated"):
        cur.fetchone()
    with pytest.raises(ProgrammingError):
        cur.executemany("INSERT INTO t VALUES (?)", [(1,), {"k": 1}])
    assert len(driver.requests) == before


@pytest.mark.parametrize(
    ("rows", "message"),
    [
        ([(1, (2,)), (3,)], "group 2 has 1 values for 2 placeholders"),
        ([(1, (2,)), (3, (4,), 5)], "group 2 has 3 values for 2 placeholders"),
        ([(1,), (2, (3,))], "group 1 has 1 values for 2 placeholders"),
    ],
)
def test_executemany_count_mismatch_runs_no_group(
    wrapped: cubriddb.Connection, rows: list[Any], message: str
) -> None:
    cur = wrapped.cursor()
    cur.execute("SELECT ? FROM t", (1,))
    rowcount, description = cur.rowcount, cur.description
    with pytest.raises(ProgrammingError, match=message):
        cur.executemany("INSERT INTO t VALUES (?, ?)", rows)
    assert len(_executed(wrapped)) == 1  # only the earlier SELECT
    assert len(_prepared(wrapped)) == 2
    assert (cur.rowcount, cur.description) == (rowcount, description)


def test_executemany_server_error_keeps_earlier_groups_and_snapshot(
    wrapped: cubriddb.Connection, monkeypatch: pytest.MonkeyPatch
) -> None:
    driver = _driver(wrapped)
    cur = wrapped.cursor()
    cur.execute("SELECT ? FROM t", (1,))
    rowcount, description = cur.rowcount, cur.description
    original = driver._send_and_receive
    executes = 0

    def failing(packet: Any, **kwargs: Any) -> Any:
        nonlocal executes
        if (
            isinstance(packet, ExecutePacket)
            and packet.statement_type == CUBRIDStatementType.INSERT
        ):
            executes += 1
            if executes == 2:
                driver.requests.append((packet, kwargs.get("expected_generation")))
                error = ProgrammingError("server rejected", code=-494)
                setattr(error, "_cas_server_error", True)  # a complete broker error
                raise error
        return original(packet, **kwargs)

    monkeypatch.setattr(driver, "_send_and_receive", failing)
    with pytest.raises(ProgrammingError, match="failed on server"):
        cur.executemany("INSERT INTO t VALUES (?)", [(1,), (2,), (3,)])
    inserts = [p for p in _executed(wrapped) if p.statement_type == CUBRIDStatementType.INSERT]
    assert len(inserts) == 2  # group 1 ran, group 2 failed, group 3 never ran
    assert (cur.rowcount, cur.description) == (rowcount, description)
    assert cur.execute("SELECT ? FROM t", (1,)) == 2
    assert cur.fetchall() == [(1,), (2,)]


def test_closed_cursor_rejects_executemany(wrapped: cubriddb.Connection) -> None:
    cur = wrapped.cursor()
    cur.close()
    with pytest.raises(InterfaceError):
        cur.executemany("INSERT INTO t VALUES (?)", [(1,)])
