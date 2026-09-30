"""A zero DATE/DATETIME/TIMESTAMP from a live broker keeps the session usable (#512)."""

from __future__ import annotations

import uuid
from typing import cast

import pytest

import pycubrid
import pycubrid.aio
from pycubrid.exceptions import DataError
from tests._parity_helpers import ADAPTERS, ParityAdapter, connect_kwargs

pytestmark = pytest.mark.integration

ROWS_BEFORE_ZERO = 1000

ZERO_LITERALS = [
    "DATE'0000-00-00'",
    "DATETIME'0000-00-00 00:00:00'",
    "TIMESTAMP'0000-00-00 00:00:00'",
    "CAST('0000-00-00' AS DATE)",
    "DATETIMETZ'0000-00-00 00:00:00 +09:00'",
    "DATETIMELTZ'0000-00-00 00:00:00'",
    "TIMESTAMPTZ'0000-00-00 00:00:00 +09:00'",
    "TIMESTAMPLTZ'0000-00-00 00:00:00'",
]


@pytest.fixture(params=ADAPTERS, ids=[adapter.kind for adapter in ADAPTERS])
def adapter(request: pytest.FixtureRequest) -> ParityAdapter:
    return cast(ParityAdapter, request.param)


@pytest.mark.asyncio
@pytest.mark.parametrize("literal", ZERO_LITERALS)
async def test_zero_literal_raises_data_error_and_keeps_session(
    adapter: ParityAdapter, literal: str
) -> None:
    connection = await adapter.connect()
    cursor = adapter.cursor(connection)
    token = adapter.transport_token(connection)
    try:
        with pytest.raises(DataError, match="cannot be represented"):
            await adapter.execute(cursor, "SELECT %s" % literal)
        assert adapter.transport_token(connection) is token
        await adapter.execute(cursor, "SELECT 1")
        assert await adapter.fetchone(cursor) == (1,)
    finally:
        await adapter.close_cursor(cursor)
        await adapter.close_connection(connection)


ZERO_DATE_SEQUENCE = "SELECT CAST({DATE'0000-00-00', DATE'2024-01-02'} AS SEQUENCE OF DATE)"


def test_zero_date_in_decoded_collection_keeps_session_sync() -> None:
    with pycubrid.connect(**connect_kwargs(), decode_collections=True) as connection:
        cursor = connection.cursor()
        with pytest.raises(DataError, match="cannot be represented"):
            cursor.execute(ZERO_DATE_SEQUENCE)
        cursor.execute("SELECT 1")
        assert cursor.fetchone() == (1,)


@pytest.mark.asyncio
async def test_zero_date_in_decoded_collection_keeps_session_async() -> None:
    connection = await pycubrid.aio.connect(**connect_kwargs(), decode_collections=True)
    try:
        cursor = connection.cursor()
        with pytest.raises(DataError, match="cannot be represented"):
            await cursor.execute(ZERO_DATE_SEQUENCE)
        await cursor.execute("SELECT 1")
        assert await cursor.fetchone() == (1,)
    finally:
        await connection.close()


@pytest.mark.asyncio
async def test_documented_sql_workaround_reads_zero_dates(adapter: ParityAdapter) -> None:
    connection = await adapter.connect()
    cursor = adapter.cursor(connection)
    try:
        await adapter.execute(
            cursor,
            "SELECT NULLIF(DATE'0000-00-00', DATE'0000-00-00'),"
            " NULLIF(DATE'2024-01-02', DATE'0000-00-00'),"
            " TO_CHAR(DATE'0000-00-00', 'YYYY-MM-DD')",
        )
        row = await adapter.fetchone(cursor)
        assert row is not None
        assert row[0] is None
        assert str(row[1]) == "2024-01-02"
        assert row[2] == "0000-00-00"
    finally:
        await adapter.close_cursor(cursor)
        await adapter.close_connection(connection)


@pytest.mark.asyncio
async def test_zero_date_on_later_fetch_page_keeps_session(adapter: ParityAdapter) -> None:
    connection = await adapter.connect(fetch_size=10)
    await adapter.set_autocommit(connection, True)
    cursor = adapter.cursor(connection)
    token = adapter.transport_token(connection)
    table = "zd_%s" % uuid.uuid4().hex[:8]
    try:
        await adapter.execute(cursor, "CREATE TABLE %s (id INT, d DATE)" % table)
        # The first execute reply carries a byte-limited page (a few hundred
        # rows), so put the zero date well past it to reach it through FETCH.
        await adapter.execute(
            cursor,
            "INSERT INTO %s SELECT ROWNUM, DATE'2024-01-01' FROM db_class a, db_class b"
            " WHERE ROWNUM <= %d" % (table, ROWS_BEFORE_ZERO),
        )
        await adapter.execute(cursor, "INSERT INTO %s VALUES (-1, DATE'0000-00-00')" % table)
        await adapter.execute(cursor, "SELECT id, d FROM %s ORDER BY id DESC" % table)
        first_page = await adapter.fetchmany(cursor, 10)
        assert [row[0] for row in first_page] == list(
            range(ROWS_BEFORE_ZERO, ROWS_BEFORE_ZERO - 10, -1)
        )
        with pytest.raises(DataError, match="cannot be represented"):
            await adapter.fetchall(cursor)
        assert adapter.transport_token(connection) is token
        await adapter.execute(cursor, "SELECT COUNT(*) FROM %s" % table)
        assert await adapter.fetchone(cursor) == (ROWS_BEFORE_ZERO + 1,)
    finally:
        await adapter.execute(cursor, "DROP TABLE IF EXISTS %s" % table)
        await adapter.close_cursor(cursor)
        await adapter.close_connection(connection)
