"""Live ``compat.native`` collection binding (#440): ``set()``/``imports()``/``bind_set()``."""

from __future__ import annotations

from collections.abc import Generator
from datetime import date
from decimal import Decimal
from typing import Any

import pytest

import pycubrid
from pycubrid.compat import native
from pycubrid.connection import Connection
from pycubrid.constants import CUBRIDDataType
from pycubrid.exceptions import DatabaseError

from ._cubrid_endpoint import TEST_DB, TEST_HOST, TEST_PASSWORD, TEST_PORT, TEST_USER
from ._parity_helpers import connect_kwargs, table_name

pytestmark = [pytest.mark.integration, pytest.mark.no_escape_pin]

SET, MULTISET, SEQUENCE = CUBRIDDataType.SET, CUBRIDDataType.MULTISET, CUBRIDDataType.SEQUENCE
STRING, INT = CUBRIDDataType.STRING, CUBRIDDataType.INT


def _connect() -> native.connection:
    return native.connect(f"CUBRID:{TEST_HOST}:{TEST_PORT}:{TEST_DB}:::", TEST_USER, TEST_PASSWORD)


@pytest.fixture
def observer() -> Generator[Connection, None, None]:
    conn = pycubrid.connect(**connect_kwargs(), autocommit=True, decode_collections=True)
    yield conn
    conn.close()


@pytest.fixture
def table(observer: Connection) -> Generator[str, None, None]:
    name = table_name("p440")
    try:
        yield name
    finally:
        cursor = observer.cursor()
        cursor.execute(f"DROP TABLE IF EXISTS {name}")
        cursor.close()


def _sql(observer: Connection, sql: str) -> None:
    cursor = observer.cursor()
    try:
        cursor.execute(sql)
    finally:
        cursor.close()


def _stored(observer: Connection, table: str) -> list[tuple[int, Any]]:
    cursor = observer.cursor()
    try:
        cursor.execute(f"SELECT id, c FROM {table} ORDER BY id")
        return [(int(row[0]), row[1]) for row in cursor.fetchall()]
    finally:
        cursor.close()


@pytest.mark.parametrize(
    ("column", "type_", "kind", "cases", "expected"),
    [
        (
            "SET(INTEGER)",
            INT,
            SET,
            [(3, 1, 3), ("9", "-2", "9"), (), (None, 5)],
            [frozenset({1, 3}), frozenset({9, -2}), frozenset(), frozenset({None, 5})],
        ),
        (
            "MULTISET(INTEGER)",
            INT,
            MULTISET,
            [(3, 1, 3), ("2", "2"), (), (None, 5, 5)],
            [[1, 3, 3], [2, 2], [], [5, 5, None]],
        ),
        (
            "SEQUENCE(INTEGER)",
            INT,
            SEQUENCE,
            [(3, 1, 3, 2), ("7", "-8"), (), (None, 4, None)],
            [[3, 1, 3, 2], [7, -8], [], [None, 4, None]],
        ),
        (
            "SET(VARCHAR(20))",
            STRING,
            SET,
            [("b", "a", "b"), ("한글", ""), (), (None, "NULL")],
            [
                frozenset({"a", "b"}),
                frozenset({"한글", ""}),
                frozenset(),
                frozenset({None, "NULL"}),
            ],
        ),
        (
            "MULTISET(VARCHAR(20))",
            STRING,
            MULTISET,
            [("b", "a", "b"), ("", "")],
            [["a", "b", "b"], ["", ""]],
        ),
        (
            "SEQUENCE(VARCHAR(20))",
            STRING,
            SEQUENCE,
            [("b", "a", "b"), ("한", None, "")],
            [["b", "a", "b"], ["한", None, ""]],
        ),
    ],
    ids=[
        "set-int",
        "multiset-int",
        "sequence-int",
        "set-varchar",
        "multiset-varchar",
        "seq-varchar",
    ],
)
def test_bind_set_stores_exact_values_on_one_handle(
    observer: Connection,
    table: str,
    column: str,
    type_: int,
    kind: int,
    cases: list[tuple[Any, ...]],
    expected: list[Any],
) -> None:
    _sql(observer, f"CREATE TABLE {table} (id INTEGER, c {column})")
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.prepare(f"INSERT INTO {table} VALUES (?, ?)")
        handle = cur._handle
        s = conn.set()
        for row_id, values in enumerate(cases):
            s.imports(values, type_, kind=kind)
            cur.bind_param(1, row_id)
            cur.bind_set(2, s)
            assert cur.execute() == 1
        # Whole SQL NULL: the scalar NULL bind, and a set that was never imported.
        cur.bind_param(1, len(cases))
        cur.bind_param(2, None)
        assert cur.execute() == 1
        cur.bind_param(1, len(cases) + 1)
        cur.bind_set(2, conn.set())
        assert cur.execute() == 1
        assert cur._handle == handle
        cur.close()
    finally:
        conn.close()
    assert _stored(observer, table) == [
        *enumerate(expected),
        (len(cases), None),
        (len(cases) + 1, None),
    ]


def test_default_kind_into_multiset_and_sequence_columns_keeps_set_semantics(
    observer: Connection, table: str
) -> None:
    # The official bytes: SET drops duplicates and does not keep order. Use
    # kind=MULTISET or kind=SEQUENCE to keep them.
    _sql(observer, f"CREATE TABLE {table} (id INTEGER, c SEQUENCE(INTEGER))")
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.prepare(f"INSERT INTO {table} VALUES (?, ?)")
        s = conn.set()
        s.imports((3, 1, 3, 2), INT)
        cur.bind_param(1, 1)
        cur.bind_set(2, s)
        cur.execute()
        cur.close()
    finally:
        conn.close()
    assert _stored(observer, table) == [(1, [1, 2, 3])]


@pytest.mark.parametrize(
    ("column", "expected"),
    [("SET(INTEGER)", frozenset({1, 3})), ("SEQUENCE(INTEGER)", [3, 1, 3])],
    ids=["set-column", "sequence-column"],
)
def test_multiset_kind_follows_the_column(
    observer: Connection, table: str, column: str, expected: Any
) -> None:
    # kind=MULTISET is sent as SEQUENCE: a SET column still deduplicates and
    # a SEQUENCE column keeps the order and duplicates.
    _sql(observer, f"CREATE TABLE {table} (id INTEGER, c {column})")
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.prepare(f"INSERT INTO {table} VALUES (?, ?)")
        s = conn.set()
        s.imports((3, 1, 3), INT, kind=MULTISET)
        cur.bind_param(1, 1)
        cur.bind_set(2, s)
        cur.execute()
        cur.close()
    finally:
        conn.close()
    assert _stored(observer, table) == [(1, expected)]


def test_other_element_type_codes_are_sent_as_strings(observer: Connection, table: str) -> None:
    # As in the official driver, a NUMERIC/DATE type code only labels the
    # import; the STRING elements are converted by the server.
    _sql(observer, f"CREATE TABLE {table} (id INTEGER, c SET(NUMERIC(5,2)), d SET(DATE))")
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.prepare(f"INSERT INTO {table} VALUES (?, ?, ?)")
        numbers = conn.set()
        numbers.imports(("1.5", "2"), CUBRIDDataType.NUMERIC)
        dates = conn.set()
        dates.imports(("2024-01-15",), CUBRIDDataType.DATE)
        cur.bind_param(1, 1)
        cur.bind_set(2, numbers)
        cur.bind_set(3, dates)
        cur.execute()
        cur.close()
    finally:
        conn.close()
    cursor = observer.cursor()
    try:
        cursor.execute(f"SELECT c, d FROM {table}")
        assert cursor.fetchall() == [
            (frozenset({Decimal("1.50"), Decimal("2.00")}), frozenset({date(2024, 1, 15)}))
        ]
    finally:
        cursor.close()


def test_elements_are_strings_on_the_wire_like_the_official_driver(
    observer: Connection, table: str
) -> None:
    # An untyped SET column stores each element with its bound type, so INT
    # imports arrive as text, exactly as the official imports() sends them.
    _sql(observer, f"CREATE TABLE {table} (id INTEGER, c SET)")
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.prepare(f"INSERT INTO {table} VALUES (?, ?)")
        s = conn.set()
        s.imports((1, 2), INT)
        cur.bind_param(1, 1)
        cur.bind_set(2, s)
        cur.execute()
        cur.close()
    finally:
        conn.close()
    assert _stored(observer, table) == [(1, frozenset({"1", "2"}))]


def test_server_rejection_keeps_the_handle_and_connection_usable(
    observer: Connection, table: str
) -> None:
    _sql(observer, f"CREATE TABLE {table} (id INTEGER, c SET(INTEGER))")
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.prepare(f"INSERT INTO {table} VALUES (?, ?)")
        handle = cur._handle
        for attempt, bad in enumerate((("x",), (2**40,))):
            s = conn.set()
            s.imports(bad, INT)
            cur.bind_param(1, 1)
            cur.bind_set(2, s)
            with pytest.raises(DatabaseError) as caught:
                cur.execute()
            if attempt == 0:
                assert caught.value.errno == -494  # Cannot coerce host var to type set.
            else:
                # A reused handle's next failure reports -1024 (#611).
                assert caught.value.errno is not None and caught.value.errno < 0
            assert "x" not in str(caught.value)
        s = conn.set()
        s.imports((4, 4), INT)
        cur.bind_param(1, 2)
        cur.bind_set(2, s)
        assert cur.execute() == 1
        assert cur._handle == handle
        cur.close()
    finally:
        conn.close()
    assert _stored(observer, table) == [(2, frozenset({4}))]


def test_collection_bind_in_a_select_predicate(observer: Connection, table: str) -> None:
    _sql(observer, f"CREATE TABLE {table} (id INTEGER, c SET(INTEGER))")
    _sql(observer, f"INSERT INTO {table} VALUES (1, {{1, 2}}), (2, {{3}})")
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.prepare(f"SELECT id FROM {table} WHERE c SUBSETEQ ? ORDER BY id")
        for values, rows in (((1, 2, 3), [(1,), (2,)]), (("3",), [(2,)]), ((), [])):
            s = conn.set()
            s.imports(values, INT)
            cur.bind_set(1, s)
            assert cur.execute() == len(rows)
            fetched = []
            while (row := cur.fetch_row()) is not None:
                fetched.append(row)
            assert fetched == rows
        cur.close()
    finally:
        conn.close()
