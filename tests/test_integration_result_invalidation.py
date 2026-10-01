"""Exercise actual wide-result FETCH boundaries without shared tables (#395)."""

from __future__ import annotations

from typing import cast

import pytest

import pycubrid
import pycubrid.aio
from pycubrid.exceptions import InterfaceError, ProgrammingError
from tests._parity_helpers import ADAPTERS, ParityAdapter, connect_kwargs, table_name

pytestmark = pytest.mark.integration


@pytest.fixture(params=ADAPTERS, ids=[adapter.kind for adapter in ADAPTERS])
def adapter(request: pytest.FixtureRequest) -> ParityAdapter:
    return cast(ParityAdapter, request.param)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["sql", "binding"])
async def test_failed_execute_discards_previous_rows(adapter: ParityAdapter, failure: str) -> None:
    connection = await adapter.connect()
    cursor = adapter.cursor(connection)
    try:
        await adapter.execute(cursor, "SELECT 1 AS id UNION ALL SELECT 2 UNION ALL SELECT 3")
        assert await adapter.fetchone(cursor) == (1,)
        with pytest.raises(ProgrammingError):
            if failure == "sql":
                await adapter.execute(cursor, "SELECT missing_column FROM db_root")
            else:
                await adapter.execute(cursor, "SELECT ?", (1, 2))
        assert cursor.description is None
        assert cursor.rowcount == -1
        assert cursor.lastrowid is None
        with pytest.raises(InterfaceError, match="No result set available"):
            await adapter.fetchone(cursor)
        with pytest.raises(InterfaceError, match="No result set available"):
            await adapter.fetchmany(cursor, 2)
        with pytest.raises(InterfaceError, match="No result set available"):
            await adapter.fetchall(cursor)
        await adapter.execute(cursor, "SELECT 42")
        assert await adapter.fetchall(cursor) == [(42,)]
    finally:
        await adapter.close_cursor(cursor)
        await adapter.close_connection(connection)


@pytest.mark.asyncio
@pytest.mark.parametrize("configuration", ["warm", "pinned"])
@pytest.mark.parametrize("boundary", ["commit", "rollback", "none"])
@pytest.mark.parametrize("method", ["one", "many", "all"])
async def test_wide_select_transaction_boundary(
    adapter: ParityAdapter, configuration: str, boundary: str, method: str
) -> None:
    if configuration == "pinned":
        if adapter.kind == "sync":
            connection = pycubrid.connect(
                **connect_kwargs(fetch_size=17), no_backslash_escapes=True
            )
        else:
            connection = await pycubrid.aio.connect(
                **connect_kwargs(fetch_size=17), no_backslash_escapes=True
            )
    else:
        connection = await adapter.connect(fetch_size=17)
    cursor = adapter.cursor(connection)
    table = table_name("er395")
    created = False
    try:
        await adapter.set_autocommit(connection, False)
        await adapter.execute(
            cursor, "CREATE TABLE %s (id INTEGER PRIMARY KEY, payload VARCHAR(1000))" % table
        )
        created = True
        await adapter.executemany(
            cursor, "INSERT INTO %s VALUES (?, ?)" % table, [(i, "x" * 1000) for i in range(500)]
        )
        await adapter.commit(connection)
        await adapter.execute(cursor, "SELECT 1")
        assert await adapter.fetchall(cursor) == [(1,)]
        await adapter.execute(cursor, "SELECT id, payload FROM %s ORDER BY id" % table)
        assert cursor._total_tuple_count == 500
        assert 0 < cursor._fetched_count < 500
        assert cursor._invalidated_by_reconnect is False
        first = await adapter.fetchone(cursor)
        assert first is not None and first[0] == 0
        if boundary == "commit":
            await adapter.commit(connection)
        elif boundary == "rollback":
            await adapter.rollback(connection)

        async def fetch_remaining() -> list[int]:
            if method == "all":
                return [int(row[0]) for row in await adapter.fetchall(cursor)]
            rows: list[int] = []
            while True:
                if method == "many":
                    part = await adapter.fetchmany(cursor, 17)
                    if not part:
                        return rows
                    rows.extend(int(row[0]) for row in part)
                else:
                    row = await adapter.fetchone(cursor)
                    if row is None:
                        return rows
                    rows.append(int(row[0]))

        if boundary == "none":
            assert await fetch_remaining() == list(range(1, 500))
            assert cursor._fetched_count == 500  # a real FETCH was required
        else:
            assert cursor._query_handle is None
            assert cursor._fetched_count < cursor._total_tuple_count
            with pytest.raises(InterfaceError, match="invalidated"):
                await fetch_remaining()
        await adapter.execute(cursor, "SELECT 1")
        assert await adapter.fetchall(cursor) == [(1,)]
    finally:
        await adapter.rollback(connection)
        if created:
            await adapter.execute(cursor, "DROP TABLE %s" % table)
            await adapter.commit(connection)
        await adapter.close_cursor(cursor)
        await adapter.close_connection(connection)
