"""An empty BIT VARYING value is fetched as b"", not as NULL (#994).

CAS writes a NULL cell as size -1 and an empty bit string as size 0, but the
row decoder read every size <= 0 as NULL, so ``X''`` (``IS NULL`` 0,
``BIT_LENGTH`` 0) came back as ``None`` next to a real NULL. Sync and async,
for a stored column, a constant and a NULL/value UNION column.
"""

from __future__ import annotations

from typing import cast

import pytest

from tests._parity_helpers import ADAPTERS, ParityAdapter, table_name

pytestmark = [pytest.mark.integration]


@pytest.fixture(params=ADAPTERS, ids=[adapter.kind for adapter in ADAPTERS])
def adapter(request: pytest.FixtureRequest) -> ParityAdapter:
    return cast(ParityAdapter, request.param)


@pytest.mark.asyncio
async def test_empty_bit_string_is_not_null(adapter: ParityAdapter) -> None:
    table = table_name("bit994")
    connection = await adapter.connect()
    cursor = adapter.cursor(connection)
    try:
        await adapter.set_autocommit(connection, True)
        await adapter.execute(
            cursor, "CREATE TABLE %s (id INT PRIMARY KEY, v BIT VARYING(64), b BIT(8))" % table
        )
        try:
            await adapter.execute(
                cursor,
                "INSERT INTO %s VALUES (1, X'', X'01'), (2, NULL, NULL), (3, X'0F', X'0F')" % table,
            )
            # b"" on the literal path renders X'', an empty bit string.
            await adapter.execute(cursor, "INSERT INTO %s (id, v) VALUES (4, ?)" % table, (b"",))
            await adapter.execute(
                cursor, "SELECT id, v, v IS NULL, BIT_LENGTH(v), b FROM %s ORDER BY id" % table
            )
            rows = [tuple(row) for row in await adapter.fetchall(cursor)]
            assert rows == [
                (1, b"", 0, 0, b"\x01"),
                (2, None, 1, None, None),
                (3, b"\x0f", 0, 8, b"\x0f"),
                (4, b"", 0, 0, None),
            ]

            await adapter.execute(
                cursor,
                "SELECT X'', CAST(NULL AS BIT VARYING), CAST(X'' AS BIT VARYING(8)) FROM db_root",
            )
            assert [tuple(row) for row in await adapter.fetchall(cursor)] == [(b"", None, b"")]

            await adapter.execute(
                cursor,
                "SELECT v FROM (SELECT 1 AS k, NULL AS v FROM db_root"
                " UNION ALL SELECT 2, X'' FROM db_root) t ORDER BY k",
            )
            assert [tuple(row) for row in await adapter.fetchall(cursor)] == [(None,), (b"",)]
        finally:
            await adapter.execute(cursor, "DROP TABLE %s" % table)
    finally:
        await adapter.close_cursor(cursor)
        await adapter.close_connection(connection)


@pytest.mark.asyncio
async def test_empty_bit_string_across_fetch_pages(adapter: ParityAdapter) -> None:
    """Rows past the first FETCH page decode b"" and NULL the same way."""
    table = table_name("bit994p")
    connection = await adapter.connect()
    cursor = adapter.cursor(connection)
    try:
        await adapter.set_autocommit(connection, True)
        await adapter.execute(
            cursor, "CREATE TABLE %s (id INT PRIMARY KEY, v BIT VARYING(8))" % table
        )
        try:
            await adapter.execute(
                cursor,
                "INSERT INTO %s SELECT ROWNUM, CASE WHEN MOD(ROWNUM, 2) = 0 THEN X'' END"
                " FROM db_attribute a, db_attribute b WHERE ROWNUM <= 1000" % table,
            )
            await adapter.execute(cursor, "SELECT id, v FROM %s ORDER BY id" % table)
            rows = [tuple(row) for row in await adapter.fetchall(cursor)]
            assert len(rows) == 1000
            assert rows == [(i, b"" if i % 2 == 0 else None) for i in range(1, 1001)]
        finally:
            await adapter.execute(cursor, "DROP TABLE %s" % table)
    finally:
        await adapter.close_cursor(cursor)
        await adapter.close_connection(connection)


@pytest.mark.asyncio
async def test_evaluate_empty_bit_string(adapter: ParityAdapter) -> None:
    """EVALUATE sends a self-typed cell; X'' is b"" and NULL is None."""
    connection = await adapter.connect()
    cursor = adapter.cursor(connection)
    try:
        await adapter.execute(cursor, "EVALUATE X''")
        assert [tuple(row) for row in await adapter.fetchall(cursor)] == [(b"",)]
        await adapter.execute(cursor, "EVALUATE CAST(NULL AS BIT VARYING)")
        assert [tuple(row) for row in await adapter.fetchall(cursor)] == [(None,)]
    finally:
        await adapter.close_cursor(cursor)
        await adapter.close_connection(connection)
