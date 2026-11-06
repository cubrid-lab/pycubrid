"""Live ``executemany(prepared=True)`` (PREPARE + EXECUTE_ARRAY) semantics (#839).

Each check compares the prepared batch with the shipped literal batch or with
the #836 probes: every row is executed and only a failing row is undone,
autocommit commits each successful row, and the per-row counts add up to
``rowcount``.
"""

from __future__ import annotations

import asyncio
import contextlib
import datetime
from collections.abc import Generator
from decimal import Decimal
from typing import Any

import pytest

import pycubrid
import pycubrid.aio
from pycubrid.connection import Connection
from pycubrid.constants import CUBRIDDataType
from pycubrid.exceptions import (
    DatabaseError,
    IntegrityError,
    NotSupportedError,
    ProgrammingError,
)
from pycubrid.types import Multiset, Sequence, Set

from ._parity_helpers import connect_kwargs, table_name
from ._prepared_wide_values import COLUMNS, ROWS

pytestmark = [pytest.mark.integration, pytest.mark.no_escape_pin]

AUTOCOMMIT = pytest.mark.parametrize("autocommit", [False, True], ids=["manual", "autocommit"])


@pytest.fixture
def admin() -> Generator[Connection, None, None]:
    connection = pycubrid.connect(**connect_kwargs(), autocommit=True)
    yield connection
    connection.close()


@pytest.fixture
def table(admin: Connection) -> Generator[str, None, None]:
    name = table_name("p839")
    with admin.cursor() as cursor:
        cursor.execute(
            f"CREATE TABLE {name} (id INT PRIMARY KEY, v VARCHAR(20), n NUMERIC(38,2), k INT)"
        )
    try:
        yield name
    finally:
        with admin.cursor() as cursor:
            cursor.execute(f"DROP TABLE IF EXISTS {name}")


def _rows(admin: Connection, sql: str) -> list[tuple[Any, ...]]:
    with admin.cursor() as cursor:
        cursor.execute(sql)
        return [tuple(row) for row in cursor.fetchall()]


def _ids(admin: Connection, table: str) -> list[int]:
    return [row[0] for row in _rows(admin, f"SELECT id FROM {table} ORDER BY id")]


def test_wide_types_store_the_same_values_as_the_literal_batch(admin: Connection) -> None:
    name = table_name("p839w")
    columns = ", ".join(f"{column} {sql_type}" for column, sql_type in COLUMNS)
    names = ", ".join(column for column, _sql_type in COLUMNS)
    markers = ", ".join("?" * (len(COLUMNS) + 2))
    sql = f"INSERT INTO {name} VALUES ({markers})"
    with admin.cursor() as cursor:
        cursor.execute(f"CREATE TABLE {name} (id INTEGER, path VARCHAR(8), {columns})")
        try:
            cursor.executemany(sql, [(i, "literal", *row) for i, row in enumerate(ROWS)])
            cursor.executemany(
                sql, [(i, "prepared", *row) for i, row in enumerate(ROWS)], prepared=True
            )
            assert cursor.rowcount == len(ROWS)
            select = f"SELECT id, {names} FROM {name} WHERE path = '%s' ORDER BY id"
            literal = _rows(admin, select % "literal")
            prepared = _rows(admin, select % "prepared")
            assert prepared == literal
            assert len(literal) == len(ROWS)
            # Spot-check the values themselves, not only the parity.
            first = prepared[0]
            assert first[1:4] == (1, 2**63 - 1, Decimal(10**38 - 1))
            assert first[6] == Decimal("0." + "1" * 38)
            assert first[9] == datetime.datetime(2024, 2, 29, 23, 59, 59, 999000)
            assert first[11] == b"\x00\xff"
            repeated = prepared[1][10]
            assert repeated is not None
            assert repeated.utcoffset() == datetime.timedelta(hours=-5)
        finally:
            cursor.execute(f"DROP TABLE IF EXISTS {name}")


def test_mixed_types_and_nulls_per_column(admin: Connection, table: str) -> None:
    rows = [
        (1, "a", 1, 7),
        (2, None, 2**40, None),
        (3, "c", Decimal("12.5"), 2**31 - 1),
        (4, "d", None, True),
        (5, "e", 1.25, -(2**31)),
    ]
    with admin.cursor() as cursor:
        cursor.executemany(f"INSERT INTO {table} VALUES (?, ?, ?, ?)", rows, prepared=True)
        assert cursor.rowcount == 5
    assert _rows(admin, f"SELECT * FROM {table} ORDER BY id") == [
        (1, "a", Decimal("1.00"), 7),
        (2, None, Decimal("1099511627776.00"), None),
        (3, "c", Decimal("12.50"), 2**31 - 1),
        (4, "d", None, 1),
        (5, "e", Decimal("1.25"), -(2**31)),
    ]


@AUTOCOMMIT
@pytest.mark.parametrize("ending", ["commit", "rollback"])
def test_duplicate_key_in_the_middle_undoes_only_that_row(
    admin: Connection, table: str, autocommit: bool, ending: str
) -> None:
    with admin.cursor() as cursor:
        cursor.execute(f"INSERT INTO {table} (id) VALUES (3)")
    with contextlib.closing(pycubrid.connect(**connect_kwargs(), autocommit=autocommit)) as conn:
        with conn.cursor() as cursor:
            with pytest.raises(IntegrityError) as raised:
                cursor.executemany(
                    f"INSERT INTO {table} (id, v) VALUES (?, ?)",
                    [(i, "row") for i in (1, 2, 3, 4, 5)],
                    prepared=True,
                )
            assert raised.value.code == -670
            assert cursor.rowcount == -1
            results = cursor._batch_row_results
            assert [(count, code) for count, code, _message in results] == [
                (1, 0),
                (1, 0),
                (-1, -670),
                (1, 0),
                (1, 0),
            ]
        getattr(conn, ending)()
    kept = [1, 2, 3, 4, 5] if autocommit or ending == "commit" else [3]
    assert _ids(admin, table) == kept


@AUTOCOMMIT
def test_conversion_error_matches_the_literal_batch(
    admin: Connection, table: str, autocommit: bool
) -> None:
    rows = [(1, 1), (2, "abc"), (3, 3)]
    sql = f"INSERT INTO {table} (id, k) VALUES (?, ?)"
    with contextlib.closing(pycubrid.connect(**connect_kwargs(), autocommit=autocommit)) as conn:
        with conn.cursor() as cursor:
            with pytest.raises(DatabaseError) as literal:
                cursor.executemany(sql, rows)
            conn.rollback()
            with pytest.raises(DatabaseError) as prepared:
                cursor.executemany(sql, [(i + 10, k) for i, k in rows], prepared=True)
            assert type(prepared.value) is type(literal.value)
            assert prepared.value.code == literal.value.code
            assert [code for _count, code, _m in cursor._batch_row_results][::2] == [0, 0]
        conn.commit()
    assert _ids(admin, table) == ([1, 3, 11, 13] if autocommit else [11, 13])


def test_insert_host_variable_coercion_error_keeps_the_session_usable(
    admin: Connection, table: str
) -> None:
    # 2**40 is typed BIGINT; storing it in INT overflows on the server.
    sql = f"INSERT INTO {table} (id, k) VALUES (?, ?)"
    with admin.cursor() as cursor:
        with pytest.raises(ProgrammingError) as raised:
            cursor.executemany(sql, [(1, 2**40)], prepared=True)
        assert isinstance(raised.value, DatabaseError)
        assert raised.value.errno == -494
        cursor.executemany(sql, [(2, 7)], prepared=True)
        assert cursor.rowcount == 1
    assert _rows(admin, f"SELECT id, k FROM {table}") == [(2, 7)]


@AUTOCOMMIT
def test_update_and_delete_rowcount_is_the_sum(
    admin: Connection, table: str, autocommit: bool
) -> None:
    with admin.cursor() as cursor:
        cursor.executemany(
            f"INSERT INTO {table} (id, k) VALUES (?, ?)",
            [(1, 1), (2, 1), (3, 1), (4, 3), (5, 3)],
        )
    with contextlib.closing(pycubrid.connect(**connect_kwargs(), autocommit=autocommit)) as conn:
        with conn.cursor() as cursor:
            update = f"UPDATE {table} SET v = ? WHERE k = ?"
            cursor.executemany(update, [("x", 1), ("y", 2), ("z", 3)], prepared=True)
            assert cursor.rowcount == 5
            assert [r[0] for r in cursor._batch_row_results] == [3, 0, 2]
            delete = f"DELETE FROM {table} WHERE id = ?"
            cursor.executemany(delete, [(1,), (99,), (2,)], prepared=True)
            assert cursor.rowcount == 2
            assert [r[0] for r in cursor._batch_row_results] == [1, 0, 1]
        conn.commit()
    assert _rows(admin, f"SELECT id, v FROM {table} ORDER BY id") == [
        (3, "x"),
        (4, "z"),
        (5, "z"),
    ]


@AUTOCOMMIT
def test_merge(admin: Connection, table: str, autocommit: bool) -> None:
    with admin.cursor() as cursor:
        cursor.execute(f"INSERT INTO {table} (id, v) VALUES (1, 'old')")
    merge = (
        f"MERGE INTO {table} t USING db_root ON (t.id = CAST(? AS INT)) "
        "WHEN MATCHED THEN UPDATE SET t.v = CAST(? AS VARCHAR(20)) "
        "WHEN NOT MATCHED THEN INSERT (id, v) VALUES (CAST(? AS INT), CAST(? AS VARCHAR(20)))"
    )
    with contextlib.closing(pycubrid.connect(**connect_kwargs(), autocommit=autocommit)) as conn:
        with conn.cursor() as cursor:
            cursor.executemany(merge, [(1, "new", 1, "new"), (2, "ins", 2, "ins")], prepared=True)
            assert cursor.rowcount == 2
        conn.commit()
    assert _rows(admin, f"SELECT id, v FROM {table} ORDER BY id") == [(1, "new"), (2, "ins")]


def test_collections_store_the_same_values_as_literals(admin: Connection) -> None:
    name = table_name("p839c")
    rows = [
        (1, Set([1, 2, None]), Multiset(["a", "a"]), Sequence([3, 1])),
        (2, Set([]), Multiset([]), Sequence([])),
        (3, None, Multiset(["b", None]), Sequence([2**31 - 1])),
    ]
    sql = f"INSERT INTO {name} VALUES (?, ?, ?, ?, ?)"
    with admin.cursor() as cursor:
        cursor.execute(
            f"CREATE TABLE {name} (id INT, path VARCHAR(8), s SET(INT), "
            "m MULTISET(VARCHAR(10)), q SEQUENCE(INT))"
        )
        try:
            cursor.executemany(sql, [(i, "literal", *rest) for i, *rest in rows])
            cursor.executemany(sql, [(i, "prepared", *rest) for i, *rest in rows], prepared=True)
            select = f"SELECT id, s, m, q FROM {name} WHERE path = '%s' ORDER BY id"
            literal = _rows(admin, select % "literal")
            assert _rows(admin, select % "prepared") == literal
            assert len(literal) == 3
        finally:
            cursor.execute(f"DROP TABLE IF EXISTS {name}")


def test_lob_values_with_autocommit_off(admin: Connection) -> None:
    name = table_name("p839l")
    with admin.cursor() as cursor:
        cursor.execute(f"CREATE TABLE {name} (id INT, c CLOB, b BLOB)")
    try:
        with contextlib.closing(pycubrid.connect(**connect_kwargs(), autocommit=False)) as conn:
            rows = []
            for i in range(3):
                clob = conn.create_lob(CUBRIDDataType.CLOB)
                clob.write(f"text {i}".encode())
                blob = conn.create_lob(CUBRIDDataType.BLOB)
                blob.write(bytes([i, 255]))
                rows.append((i, clob, blob))
            with conn.cursor() as cursor:
                cursor.executemany(f"INSERT INTO {name} VALUES (?, ?, ?)", rows, prepared=True)
                assert cursor.rowcount == 3
            conn.commit()
        assert _rows(
            admin, f"SELECT id, CLOB_TO_CHAR(c), BLOB_TO_BIT(b) FROM {name} ORDER BY id"
        ) == [(i, f"text {i}", bytes([i, 255])) for i in range(3)]
    finally:
        with admin.cursor() as cursor:
            cursor.execute(f"DROP TABLE IF EXISTS {name}")


def test_fetched_lob_rebinds_on_the_same_connection(admin: Connection) -> None:
    name = table_name("p839r")
    with admin.cursor() as cursor:
        cursor.execute(f"CREATE TABLE {name} (id INT, b BLOB)")
    try:
        with contextlib.closing(pycubrid.connect(**connect_kwargs(), autocommit=False)) as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"INSERT INTO {name} VALUES (?, ?)", (1, b"stored"))
                conn.commit()
                cursor.execute(f"SELECT b FROM {name} WHERE id = 1")
                cell = cursor.fetchone()[0]
                lob = pycubrid.Lob(conn, cell["lob_type"], cell["packed_lob_handle"])
                insert = f"INSERT INTO {name} VALUES (?, ?)"
                cursor.executemany(insert, [(2, lob)], prepared=True)
                assert cursor.rowcount == 1
            conn.commit()
        assert _rows(admin, f"SELECT id, BLOB_TO_BIT(b) FROM {name} ORDER BY id") == [
            (1, b"stored"),
            (2, b"stored"),
        ]
    finally:
        with admin.cursor() as cursor:
            cursor.execute(f"DROP TABLE IF EXISTS {name}")


def test_lob_values_with_autocommit_on_are_rejected(admin: Connection) -> None:
    name = table_name("p839l")
    with admin.cursor() as cursor:
        cursor.execute(f"CREATE TABLE {name} (id INT, c CLOB)")
    try:
        with contextlib.closing(pycubrid.connect(**connect_kwargs(), autocommit=True)) as conn:
            clob = conn.create_lob(CUBRIDDataType.CLOB)
            clob.write(b"x")
            with conn.cursor() as cursor:
                with pytest.raises(NotSupportedError, match="autocommit off"):
                    cursor.executemany(
                        f"INSERT INTO {name} VALUES (?, ?)", [(1, clob)], prepared=True
                    )
        assert _rows(admin, f"SELECT COUNT(*) FROM {name}") == [(0,)]
    finally:
        with admin.cursor() as cursor:
            cursor.execute(f"DROP TABLE IF EXISTS {name}")


def test_prepare_error_is_a_programming_error(admin: Connection, table: str) -> None:
    with admin.cursor() as cursor:
        with pytest.raises(ProgrammingError):
            cursor.executemany(f"INSERT INTO {table} (nope) VALUES (?)", [(1,)], prepared=True)
        assert cursor.rowcount == -1
        cursor.execute("SELECT 1")
        assert cursor.fetchall() == [(1,)]


def test_many_batches_do_not_leak_handles(admin: Connection, table: str) -> None:
    with admin.cursor() as cursor:
        for i in range(1000):
            cursor.executemany(
                f"UPDATE {table} SET k = ? WHERE id = ?", [(i, 1), (i, 2)], prepared=True
            )
            # Each PREPARE carries the previous batch's handle (#488), so the
            # deferred-close queue never grows.
            assert len(admin._deferred_closes) <= 1
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        assert cursor.fetchall() == [(0,)]


def test_ten_thousand_rows_in_one_request(admin: Connection, table: str) -> None:
    # Autocommit off: with autocommit on the server commits every row.
    rows = [(i, "v%d" % i, i, i) for i in range(10_000)]
    with contextlib.closing(pycubrid.connect(**connect_kwargs(), autocommit=False)) as conn:
        with conn.cursor() as cursor:
            cursor.executemany(f"INSERT INTO {table} VALUES (?, ?, ?, ?)", rows, prepared=True)
            assert cursor.rowcount == 10_000
        conn.commit()
    assert _rows(admin, f"SELECT COUNT(*), SUM(k) FROM {table}") == [(10_000, sum(range(10_000)))]


@AUTOCOMMIT
def test_async_cursor(admin: Connection, table: str, autocommit: bool) -> None:
    async def run() -> None:
        conn = await pycubrid.aio.connect(**connect_kwargs(), autocommit=autocommit)
        try:
            cursor = conn.cursor()
            await cursor.executemany(
                f"INSERT INTO {table} (id, n) VALUES (?, ?)",
                [(1, 1), (2, Decimal("2.5")), (3, None)],
                prepared=True,
            )
            assert cursor.rowcount == 3
            with pytest.raises(IntegrityError):
                await cursor.executemany(
                    f"INSERT INTO {table} (id) VALUES (?)", [(4,), (1,), (5,)], prepared=True
                )
            assert [r[1] for r in cursor._batch_row_results] == [0, -670, 0]
            await conn.commit()
            await cursor.close()
        finally:
            await conn.close()

    asyncio.run(run())
    assert _ids(admin, table) == [1, 2, 3, 4, 5]
