"""Subnormal floats and -0.0 round-trip through the literal path (#988).

CUBRID rejected every subnormal literal with -493 "exceeds limit of double",
and read the literal ``-0.0`` as NUMERIC, so a DOUBLE column stored ``0.0``.
Sync and async, through ``execute()`` and the default (FC20) ``executemany()``.
"""

from __future__ import annotations

import math
from typing import cast

import pytest

from tests._parity_helpers import ADAPTERS, ParityAdapter, table_name

pytestmark = [pytest.mark.integration]

VALUES = [5e-324, -5e-324, 1e-320, 2.225073858507201e-308, 2.2250738585072014e-308, -0.0, 0.0]


@pytest.fixture(params=ADAPTERS, ids=[adapter.kind for adapter in ADAPTERS])
def adapter(request: pytest.FixtureRequest) -> ParityAdapter:
    return cast(ParityAdapter, request.param)


@pytest.mark.asyncio
@pytest.mark.parametrize("batch", [False, True], ids=["execute", "executemany"])
async def test_double_keeps_subnormals_and_the_sign_of_zero(
    adapter: ParityAdapter, batch: bool
) -> None:
    table = table_name("flt988")
    connection = await adapter.connect()
    cursor = adapter.cursor(connection)
    try:
        await adapter.set_autocommit(connection, True)
        await adapter.execute(cursor, "CREATE TABLE %s (id INT PRIMARY KEY, d DOUBLE)" % table)
        try:
            insert = "INSERT INTO %s VALUES (?, ?)" % table
            rows = list(enumerate(VALUES))
            if batch:
                await adapter.executemany(cursor, insert, rows)
            else:
                for row in rows:
                    await adapter.execute(cursor, insert, row)
            await adapter.execute(cursor, "SELECT id, d FROM %s ORDER BY id" % table)
            fetched = [tuple(row) for row in await adapter.fetchall(cursor)]
            assert [value for _, value in fetched] == VALUES
            for (_, got), expected in zip(fetched, VALUES):
                assert math.copysign(1.0, got) == math.copysign(1.0, expected)

            await adapter.execute(cursor, "SELECT 1-?, ?", (-5e-324, -0.0))
            ((difference, zero),) = [tuple(row) for row in await adapter.fetchall(cursor)]
            assert difference == 1.0
            assert zero == 0.0 and math.copysign(1.0, zero) == -1.0
        finally:
            await adapter.execute(cursor, "DROP TABLE %s" % table)
    finally:
        await adapter.close_cursor(cursor)
        await adapter.close_connection(connection)
