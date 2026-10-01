"""Syntax and absent-class failures share a generic native category (#391)."""

from __future__ import annotations

from typing import cast

import pytest

from pycubrid.exceptions import ProgrammingError
from tests._parity_helpers import ADAPTERS, ParityAdapter, table_name

pytestmark = [pytest.mark.integration, pytest.mark.no_escape_pin]


@pytest.fixture(params=ADAPTERS, ids=[adapter.kind for adapter in ADAPTERS])
def adapter(request: pytest.FixtureRequest) -> ParityAdapter:
    return cast(ParityAdapter, request.param)


@pytest.mark.asyncio
@pytest.mark.parametrize("missing_table", [False, True], ids=["syntax", "missing-class"])
async def test_native_syntax_category_and_reuse(
    adapter: ParityAdapter, missing_table: bool
) -> None:
    connection = await adapter.connect()
    cursor = adapter.cursor(connection)
    sql = "SELECT * FROM %s" % table_name("absent391") if missing_table else "SELEC 1"
    try:
        with pytest.raises(ProgrammingError) as raised:
            await adapter.execute(cursor, sql)
        assert raised.value.code == raised.value.errno == -493
        assert raised.value.sqlstate == "42000"
        assert "description='Syntax error'" in str(raised.value)
        assert "Table not found" not in str(raised.value)
        if missing_table:
            assert "Unknown class" in raised.value.msg
        else:
            assert "Syntax error" in raised.value.msg
        await adapter.rollback(connection)
        await adapter.execute(cursor, "SELECT 1")
        assert await adapter.fetchone(cursor) == (1,)
    finally:
        await adapter.close_cursor(cursor)
        await adapter.close_connection(connection)
