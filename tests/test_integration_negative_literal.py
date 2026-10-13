"""A negative value bound after a minus sign updates only its row (#893).

``x-?`` with ``-5`` used to render ``x--5``, a line comment that dropped the
``WHERE`` clause, so the UPDATE changed every row. Sync and async, through
``execute()`` and the default (FC20) ``executemany()``.
"""

from __future__ import annotations

from decimal import Decimal
from typing import cast

import pytest

from tests._parity_helpers import ADAPTERS, ParityAdapter, table_name

pytestmark = [pytest.mark.integration, pytest.mark.no_escape_pin]


@pytest.fixture(params=ADAPTERS, ids=[adapter.kind for adapter in ADAPTERS])
def adapter(request: pytest.FixtureRequest) -> ParityAdapter:
    return cast(ParityAdapter, request.param)


@pytest.mark.asyncio
@pytest.mark.parametrize("batch", [False, True], ids=["execute", "executemany"])
async def test_negative_after_minus_updates_only_its_row(
    adapter: ParityAdapter, batch: bool
) -> None:
    table = table_name("neg893")
    connection = await adapter.connect()
    cursor = adapter.cursor(connection)
    try:
        await adapter.set_autocommit(connection, True)
        await adapter.execute(
            cursor,
            "CREATE TABLE %s (id INT PRIMARY KEY, x INT, d DOUBLE, n NUMERIC(10,2))" % table,
        )
        try:
            await adapter.executemany(
                cursor,
                "INSERT INTO %s VALUES (?, 10, 10, 10)" % table,
                [(1,), (2,), (3,)],
            )
            update = "UPDATE %s SET x = x-?, d = d-?, n = n-? WHERE id = ?" % table
            params = (-5, -2.5, Decimal("-1.5"), 2)
            if batch:
                await adapter.executemany(cursor, update, [params])
            else:
                await adapter.execute(cursor, update, params)
            await adapter.execute(cursor, "SELECT id, x, d, n FROM %s ORDER BY id" % table)
            rows = [tuple(row) for row in await adapter.fetchall(cursor)]
            assert rows == [
                (1, 10, 10.0, Decimal("10.00")),
                (2, 15, 12.5, Decimal("11.50")),
                (3, 10, 10.0, Decimal("10.00")),
            ]
            await adapter.execute(cursor, "SELECT 1-?, -?", (-2, -3))
            assert [tuple(row) for row in await adapter.fetchall(cursor)] == [(3, 3)]
        finally:
            await adapter.execute(cursor, "DROP TABLE %s" % table)
    finally:
        await adapter.close_cursor(cursor)
        await adapter.close_connection(connection)
