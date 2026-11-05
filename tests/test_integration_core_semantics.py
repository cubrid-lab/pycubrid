"""Core sync/async DB-API semantics, stated without any compatibility layer (#1123).

Each test fixes its own SQL, inputs and expected Python values, so correctness
does not rest on a comparison with another driver. Expected values were measured
on CUBRID 10.2, 11.2 and 11.4. Every test owns a uniquely named table, sets
autocommit explicitly and observes visibility from a separate autocommit
connection.
"""

from __future__ import annotations

import datetime
import gc
import hashlib
import inspect
from collections.abc import AsyncIterator, Awaitable, Callable
from decimal import Decimal
from typing import Any

import pytest
import pytest_asyncio

import pycubrid
import pycubrid.aio
from pycubrid.constants import CUBRIDDataType
from pycubrid.lob import Lob
from pycubrid.types import Set

from ._parity_helpers import connect_kwargs, table_name

pytestmark = [pytest.mark.integration, pytest.mark.no_escape_pin]

MODES = pytest.mark.parametrize("mode", ["sync", "async"])
Connect = Callable[..., Awaitable[Any]]


async def _call(value: Any) -> Any:
    return await value if inspect.isawaitable(value) else value


@pytest_asyncio.fixture
async def connect(mode: str) -> AsyncIterator[Connect]:
    """Open connections in ``mode``; each closes when the test ends."""
    opened: list[Any] = []

    async def open_connection(*, autocommit: bool, **options: Any) -> Any:
        driver: Any = pycubrid.aio if mode == "async" else pycubrid
        conn = await _call(driver.connect(**connect_kwargs(), autocommit=autocommit, **options))
        opened.append(conn)
        return conn

    yield open_connection
    for conn in reversed(opened):
        await _call(conn.close())


async def _run(conn: Any, sql: str, params: tuple[Any, ...] | None = None) -> list[tuple[Any, ...]]:
    cursor = conn.cursor()
    try:
        await _call(cursor.execute(sql) if params is None else cursor.execute(sql, params))
        if cursor.description is None:
            return []
        return [tuple(row) for row in await _call(cursor.fetchall())]
    finally:
        await _call(cursor.close())


class _Table:
    """Create a uniquely named table; drop it through autocommit ``admin`` on exit.

    ``actors`` roll back first, so a failed test cannot leave a lock on the table.
    """

    def __init__(self, admin: Any, prefix: str, columns: str, *actors: Any) -> None:
        self.admin = admin
        self.name = table_name(prefix)
        self.columns = columns
        self.actors = actors

    async def __aenter__(self) -> str:
        await _run(self.admin, f"CREATE TABLE {self.name} ({self.columns})")
        return self.name

    async def __aexit__(self, *_exc: object) -> None:
        for actor in self.actors:
            await _call(actor.rollback())
        await _run(self.admin, f"DROP TABLE IF EXISTS {self.name}")


@MODES
@pytest.mark.asyncio
async def test_scalar_expression_row_and_description(connect: Connect) -> None:
    conn = await connect(autocommit=True)
    cursor = conn.cursor()
    await _call(
        cursor.execute(
            "SELECT CAST(1 AS INTEGER) AS i, CAST(2 AS BIGINT) AS b,"
            " CAST(1.5 AS NUMERIC(10,2)) AS n, CAST(2.5 AS DOUBLE) AS d,"
            " CAST('ab' AS VARCHAR(20)) AS v, CAST('ab' AS CHAR(5)) AS c,"
            " DATE'2024-01-15' AS dt, CAST(NULL AS INTEGER) AS z"
        )
    )
    row = tuple(await _call(cursor.fetchone()))
    expected = (1, 2, Decimal("1.50"), 2.5, "ab", "ab   ", datetime.date(2024, 1, 15), None)
    assert row == expected
    assert [type(value) for value in row] == [type(value) for value in expected]
    assert [tuple(column) for column in cursor.description] == [
        ("i", 8, None, None, 10, 0, True),
        ("b", 21, None, None, 19, 0, True),
        ("n", 7, None, None, 10, 2, True),
        ("d", 12, None, None, 15, 0, True),
        ("v", 2, None, None, 20, 0, True),
        ("c", 1, None, None, 5, 0, True),
        ("dt", 13, None, None, 10, 0, True),
        ("z", 0, None, None, 0, 0, True),
    ]
    await _call(cursor.close())


@MODES
@pytest.mark.asyncio
async def test_alias_case_duplicates_hangul_null_and_empty(connect: Connect) -> None:
    conn = await connect(autocommit=True)
    columns = "id INTEGER NOT NULL, txt VARCHAR(40), opt VARCHAR(40)"
    async with _Table(conn, "core_alias", columns) as table:
        insert = f"INSERT INTO {table} VALUES (?, ?, ?)"
        await _run(conn, insert, (1, "한", None))
        await _run(conn, insert, (2, "", ""))
        cursor = conn.cursor()
        await _call(
            cursor.execute(f'SELECT id AS "MiXeD", txt AS dup, opt AS dup FROM {table} ORDER BY id')
        )
        rows = [tuple(row) for row in await _call(cursor.fetchall())]
        assert rows == [(1, "한", None), (2, "", "")]
        assert tuple(column[0] for column in cursor.description) == ("MiXeD", "dup", "dup")
        assert tuple(column[6] for column in cursor.description) == (False, True, True)
        assert tuple(column[4] for column in cursor.description) == (10, 40, 40)
        await _call(cursor.close())


@MODES
@pytest.mark.asyncio
async def test_commit_then_rollback_visibility_sequence(connect: Connect) -> None:
    observer = await connect(autocommit=True)
    actor = await connect(autocommit=False)
    async with _Table(observer, "core_tx", "id INTEGER PRIMARY KEY", actor) as table:
        select = f"SELECT id FROM {table} ORDER BY id"
        insert = f"INSERT INTO {table} VALUES (?)"

        await _run(actor, insert, (1,))
        assert await _run(observer, select) == []
        assert await _call(actor.commit()) is None
        assert await _run(observer, select) == [(1,)]

        await _run(actor, insert, (2,))
        assert await _run(actor, select) == [(1,), (2,)]
        assert await _run(observer, select) == [(1,)]
        assert await _call(actor.rollback()) is None
        assert await _run(actor, select) == [(1,)]
        await _call(actor.commit())
        assert await _run(observer, select) == [(1,)]


@MODES
@pytest.mark.asyncio
async def test_abandoned_cursor_does_not_commit_manual_dml(connect: Connect) -> None:
    observer = await connect(autocommit=True)
    actor = await connect(autocommit=False)
    async with _Table(observer, "core_drop", "id INTEGER", actor) as table:
        select = f"SELECT id FROM {table} ORDER BY id"
        cursor = actor.cursor()
        await _call(cursor.execute(f"INSERT INTO {table} VALUES (?)", (1,)))
        del cursor
        gc.collect()
        assert await _run(observer, select) == []
        await _call(actor.rollback())
        assert await _run(observer, select) == []
        assert await _run(actor, select) == []


@MODES
@pytest.mark.parametrize(
    ("level", "counts"),
    [(4, [0, 1, 1]), (5, [0, 0, 1])],
    ids=["read-committed", "repeatable-read"],
)
@pytest.mark.asyncio
async def test_isolation_level_controls_visibility(
    connect: Connect, level: int, counts: list[int]
) -> None:
    observer = await connect(autocommit=True)
    actor = await connect(autocommit=False)
    async with _Table(observer, "core_iso", "id INTEGER", actor) as table:
        count = f"SELECT COUNT(*) FROM {table}"
        await _run(actor, f"SET TRANSACTION ISOLATION LEVEL {level:d}")
        await _call(actor.commit())

        first = await _run(actor, count)
        await _run(observer, f"INSERT INTO {table} VALUES (?)", (1,))
        second = await _run(actor, count)
        await _call(actor.commit())
        third = await _run(actor, count)
        await _call(actor.commit())
        assert [first[0][0], second[0][0], third[0][0]] == counts


@MODES
@pytest.mark.parametrize("prepared", [False, True], ids=["literal", "prepared"])
@pytest.mark.asyncio
async def test_text_elements_keep_null_word_empty_and_hangul(
    connect: Connect, prepared: bool
) -> None:
    conn = await connect(autocommit=True, decode_collections=True)
    rows = [
        (1, Set(["b", "a", "한", "b"])),
        (2, Set(["NULL", "a"])),
        (3, Set(["", "a"])),
        (4, Set(["a", None])),
        (5, Set()),
    ]
    async with _Table(conn, "core_set", "id INT, s SET(VARCHAR(20))") as table:
        cursor = conn.cursor()
        insert = f"INSERT INTO {table} VALUES (?, ?)"
        if prepared:
            await _call(cursor.executemany(insert, rows, prepared=True))
        else:
            await _call(cursor.executemany(insert, rows))
        assert cursor.rowcount == len(rows)
        await _call(cursor.close())
        assert await _run(conn, f"SELECT s FROM {table} ORDER BY id") == [
            (frozenset({"a", "b", "한"}),),
            (frozenset({"NULL", "a"}),),
            (frozenset({"", "a"}),),
            (frozenset({"a", None}),),
            (frozenset(),),
        ]


@MODES
@pytest.mark.asyncio
async def test_async_lob_contents_null_and_empty(connect: Connect) -> None:
    # Async cursors have no Lob API, so SQL conversion is the content check;
    # the sync run pins the same results.
    conn = await connect(autocommit=True)
    blob, clob = bytes(range(256)), "clob 한글"
    async with _Table(conn, "core_lob", "id INT, b BLOB, c CLOB") as table:
        insert = f"INSERT INTO {table} VALUES (?, ?, ?)"
        await _run(conn, insert, (1, blob, clob))
        await _run(conn, insert, (2, None, None))
        await _run(conn, insert, (3, b"", ""))
        assert await _run(
            conn, f"SELECT id, BLOB_TO_BIT(b), CLOB_TO_CHAR(c) FROM {table} WHERE id = 1"
        ) == [(1, blob, clob)]
        assert await _run(conn, f"SELECT id FROM {table} WHERE b IS NULL AND c IS NULL") == [(2,)]
        assert await _run(
            conn, f"SELECT id FROM {table} WHERE b IS NOT NULL AND c IS NOT NULL ORDER BY id"
        ) == [(1,), (3,)]


@pytest.mark.parametrize("mode", ["sync"])
@pytest.mark.asyncio
async def test_large_multibyte_clob_reads_exact_bytes(connect: Connect) -> None:
    admin = await connect(autocommit=True)
    conn = await connect(autocommit=False)
    text = "A한éB" * 25000
    async with _Table(admin, "core_clob", "c CLOB", conn) as table:
        await _run(conn, f"INSERT INTO {table} VALUES (?)", (text,))
        conn.commit()
        ((handle,),) = await _run(conn, f"SELECT c FROM {table}")
        assert handle["lob_length"] == 175000
        with Lob(conn, CUBRIDDataType.CLOB, handle["packed_lob_handle"]) as lob:
            content = lob.read(handle["lob_length"])
    assert len(content) == 175000
    assert hashlib.sha256(content).hexdigest() == (
        "1231f878b5a7c0713adf540d294cecc11b6325e466279795ac9fe4b4404f39e2"
    )
