"""Real native constraint failures remain usable after rollback (#390)."""

from __future__ import annotations

from typing import cast

import pytest

from pycubrid.exceptions import IntegrityError
from tests._parity_helpers import ADAPTERS, ParityAdapter, table_name

pytestmark = pytest.mark.integration


@pytest.fixture(params=ADAPTERS, ids=[adapter.kind for adapter in ADAPTERS])
def adapter(request: pytest.FixtureRequest) -> ParityAdapter:
    return cast(ParityAdapter, request.param)


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["execute", "executemany", "batch"])
@pytest.mark.parametrize("code", [-631, -922], ids=["not-null", "foreign-key"])
async def test_native_integrity_failure_and_rollback_reuse(
    adapter: ParityAdapter, operation: str, code: int
) -> None:
    connection = await adapter.connect()
    cursor = adapter.cursor(connection)
    table = table_name("er390")
    created = False
    try:
        await adapter.set_autocommit(connection, False)
        await adapter.execute(
            cursor,
            "CREATE TABLE %s (id INTEGER PRIMARY KEY, parent_id INTEGER, n INTEGER NOT NULL, "
            "FOREIGN KEY(parent_id) REFERENCES %s(id))" % (table, table),
        )
        created = True
        await adapter.commit(connection)
        values = (1, None, None) if code == -631 else (1, 999, 1)
        sql = "INSERT INTO %s VALUES (?, ?, ?)" % table
        with pytest.raises(IntegrityError) as raised:
            if operation == "execute":
                await adapter.execute(cursor, sql, values)
            elif operation == "executemany":
                await adapter.executemany(cursor, sql, [values])
            else:
                literal = "1, NULL, NULL" if code == -631 else "1, 999, 1"
                await adapter.executemany_batch(
                    cursor, ["INSERT INTO %s VALUES (%s)" % (table, literal)]
                )
        assert raised.value.code == code
        assert raised.value.errno == code
        assert raised.value.sqlstate == "23000"
        await adapter.rollback(connection)
        await adapter.execute(cursor, "SELECT 1")
        assert await adapter.fetchone(cursor) == (1,)
        await adapter.execute(cursor, sql, (2, None, 7))
        await adapter.execute(cursor, "SELECT COUNT(*) FROM %s" % table)
        assert await adapter.fetchone(cursor) == (1,)
    finally:
        await adapter.rollback(connection)
        if created:
            await adapter.execute(cursor, "DROP TABLE %s" % table)
            await adapter.commit(connection)
        await adapter.close_cursor(cursor)
        await adapter.close_connection(connection)
