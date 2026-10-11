"""Live CUBRIDdb wrapper collection call shapes: execute set_type and executemany (#610)."""

from __future__ import annotations

from collections.abc import Generator
from contextlib import closing
from datetime import date, datetime, time
from decimal import Decimal
from typing import Any

import pytest

import pycubrid
from pycubrid.compat import cubriddb
from pycubrid.connection import Connection
from pycubrid.constants import CUBRIDDataType

from ._cubrid_endpoint import TEST_DB, TEST_HOST, TEST_PASSWORD, TEST_PORT, TEST_USER
from ._parity_helpers import connect_kwargs, table_name

pytestmark = pytest.mark.integration


@pytest.fixture
def observer() -> Generator[Connection, None, None]:
    with closing(
        pycubrid.connect(**connect_kwargs(), autocommit=True, decode_collections=True)
    ) as conn:
        yield conn


def _rows(conn: Connection, sql: str) -> list[tuple[Any, ...]]:
    with closing(conn.cursor()) as cursor:
        cursor.execute(sql)
        return [tuple(row) for row in cursor.fetchall()]


def _ddl(conn: Connection, sql: str) -> None:
    with closing(conn.cursor()) as cursor:
        cursor.execute(sql)


@pytest.fixture
def table(observer: Connection, request: pytest.FixtureRequest) -> Generator[str, None, None]:
    name = table_name("p610")
    _ddl(observer, f"CREATE TABLE {name} ({request.param})")
    try:
        yield name
    finally:
        _ddl(observer, f"DROP TABLE IF EXISTS {name}")


def _wrapper() -> cubriddb.Connection:
    return cubriddb.Connection(
        f"CUBRID:{TEST_HOST}:{TEST_PORT}:{TEST_DB}:::", TEST_USER, TEST_PASSWORD
    )


@pytest.mark.parametrize("table", ["id INT, s SET(VARCHAR(20)), n SET(INTEGER)"], indirect=True)
def test_set_type_forms(observer: Connection, table: str) -> None:
    sql = f"INSERT INTO {table} VALUES (?, ?, ?)"
    with closing(_wrapper()) as conn, closing(conn.cursor()) as cur:
        assert cur.execute(sql, (1, ("a", "b"), ("1", "2")), set_type=CUBRIDDataType.STRING) == 1
        assert cur.rowcount == 1 and cur.description is None
        cur.execute(sql, (2, ["c", "d"], (3, 4)), set_type=[None, None, CUBRIDDataType.INT])
        cur.execute(sql, (3, {"g", "h"}, frozenset({5})), set_type=[CUBRIDDataType.INT])
        cur.execute(sql, (4, ("a", None), ()))
    assert _rows(observer, f"SELECT id, s, n FROM {table} ORDER BY id") == [
        (1, {"a", "b"}, {1, 2}),
        (2, {"c", "d"}, {3, 4}),
        (3, {"g", "h"}, {5}),
        (4, {"a", None}, set()),
    ]


@pytest.mark.parametrize(
    "table",
    [
        "i SET(INTEGER), f SET(DOUBLE), d SET(NUMERIC(5,2)), dt SET(DATE), t SET(TIME),"
        " dtm SET(DATETIME), v SET(VARCHAR(20))"
    ],
    indirect=True,
)
def test_inferred_element_types_are_stored(observer: Connection, table: str) -> None:
    args = (
        (2, 1),
        (1.5,),
        (Decimal("1.25"),),
        (date(2024, 1, 15),),
        (time(13, 30, 45),),
        (datetime(2024, 1, 15, 13, 30, 45),),
        ("b", "한"),
    )
    with closing(_wrapper()) as conn, closing(conn.cursor()) as cur:
        assert cur.execute(f"INSERT INTO {table} VALUES (?, ?, ?, ?, ?, ?, ?)", args) == 1
    assert _rows(observer, f"SELECT * FROM {table}") == [
        (
            {1, 2},
            {1.5},
            {Decimal("1.25")},
            {date(2024, 1, 15)},
            {time(13, 30, 45)},
            {datetime(2024, 1, 15, 13, 30, 45)},
            {"b", "한"},
        )
    ]


@pytest.mark.parametrize(
    "table",
    ["id INT, s SET(INTEGER), m MULTISET(INTEGER), q SEQUENCE(INTEGER)"],
    indirect=True,
)
def test_executemany_on_every_collection_kind(observer: Connection, table: str) -> None:
    rows = [(1, (3, 1, 3), [3, 1, 3], (3, 1, 3, 2)), (2, (), (), ())]
    with closing(_wrapper()) as conn, closing(conn.cursor()) as cur:
        assert cur.executemany(f"INSERT INTO {table} VALUES (?, ?, ?, ?)", rows) is None
        assert cur.rowcount == 1 and cur.description is None
        # Like the official driver, every collection binds as a SET: MULTISET and
        # SEQUENCE columns receive the deduplicated, server-ordered elements.
        assert cur.execute(f"SELECT id FROM {table} ORDER BY id") == 2
        assert cur.fetchall() == [(1,), (2,)]
    assert _rows(observer, f"SELECT id, s, m, q FROM {table} ORDER BY id") == [
        (1, {1, 3}, [1, 3], [1, 2, 3]),
        (2, set(), [], []),
    ]


@pytest.mark.parametrize("table", ["id INT, s SET(INTEGER)"], indirect=True)
def test_errors_leave_the_table_and_cursor_usable(observer: Connection, table: str) -> None:
    sql = f"INSERT INTO {table} VALUES (?, ?)"
    with closing(_wrapper()) as conn, closing(conn.cursor()) as cur:
        with pytest.raises(TypeError):
            cur.execute(sql, (1, (1, "z")))
        with pytest.raises(pycubrid.NotSupportedError):
            cur.execute(sql, (1, (b"\x14",)))
        with pytest.raises(pycubrid.DatabaseError):
            cur.execute(sql, (1, ("x", "y")), set_type=CUBRIDDataType.INT)
        with pytest.raises(TypeError):
            cur.executemany(sql, [(1, (1,)), (2, (1, "z"))])
        cur.execute(sql, (9, (9,)))
    assert _rows(observer, f"SELECT id, s FROM {table}") == [(9, {9})]
