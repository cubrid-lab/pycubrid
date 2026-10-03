"""CALL / EVALUATE results and NULL-typed columns decode their values (#542).

Under CAS protocol 8 each cell of a CALL or EVALUATE result, and of a
NULL-typed column, starts with the two-byte type header ``0x80 | collection
bits | charset, type``. The driver used to read one byte and returned the rest
of the cell as raw bytes. These statements need no Java stored procedure, so
they run on every CUBRID of the matrix, sync and async.
"""

from __future__ import annotations

import datetime
import re
from typing import cast

import pytest

import pycubrid
import pycubrid.aio
from tests._parity_helpers import ADAPTERS, ParityAdapter, connect_kwargs

pytestmark = [pytest.mark.integration, pytest.mark.no_escape_pin]

_OID = re.compile(r"OID:@-?\d+\|-?\d+\|-?\d+")


@pytest.fixture(params=ADAPTERS, ids=[adapter.kind for adapter in ADAPTERS])
def adapter(request: pytest.FixtureRequest) -> ParityAdapter:
    return cast(ParityAdapter, request.param)


async def _fetch_all(adapter: ParityAdapter, sql: str) -> list[tuple[object, ...]]:
    connection = await adapter.connect()
    cursor = adapter.cursor(connection)
    try:
        await adapter.execute(cursor, sql)
        return list(await adapter.fetchall(cursor))
    finally:
        await adapter.close_cursor(cursor)
        await adapter.close_connection(connection)


@pytest.mark.asyncio
async def test_call_method_returns_the_oid_string(adapter: ParityAdapter) -> None:
    rows = await _fetch_all(adapter, "CALL find_user('dba') ON CLASS db_user")
    assert len(rows) == 1
    value = rows[0][0]
    assert isinstance(value, str) and _OID.fullmatch(value), value


@pytest.mark.asyncio
async def test_call_method_returning_null_is_none(adapter: ParityAdapter) -> None:
    rows = await _fetch_all(adapter, "CALL find_user('no_such_user_542') ON CLASS db_user")
    assert rows == [(None,)]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("expression", "expected"),
    [
        pytest.param("42", 42, id="int"),
        pytest.param("'hello'", "hello", id="char"),
        pytest.param("CAST('hello' AS VARCHAR(10))", "hello", id="varchar"),
        pytest.param(
            "DATETIME'2026-09-30 12:34:56.789'",
            datetime.datetime(2026, 9, 30, 12, 34, 56, 789000),
            id="datetime",
        ),
        pytest.param("NULL", None, id="null"),
    ],
)
async def test_evaluate_returns_the_decoded_value(
    adapter: ParityAdapter, expression: str, expected: object
) -> None:
    rows = await _fetch_all(adapter, "EVALUATE %s" % expression)
    assert rows == [(expected,)]


@pytest.mark.asyncio
async def test_null_typed_columns_decode_beside_typed_columns(adapter: ParityAdapter) -> None:
    rows = await _fetch_all(adapter, "SELECT NULL, 1, NULL, 'x'")
    assert rows == [(None, 1, None, "x")]


def test_evaluate_collection_decodes_with_decode_collections_sync() -> None:
    with pycubrid.connect(**connect_kwargs(), decode_collections=True) as connection:
        cursor = connection.cursor()
        cursor.execute("EVALUATE {1, 2}")
        assert cursor.fetchall() == [([1, 2],)]


@pytest.mark.asyncio
async def test_evaluate_collection_decodes_with_decode_collections_async() -> None:
    connection = await pycubrid.aio.connect(**connect_kwargs(), decode_collections=True)
    try:
        cursor = connection.cursor()
        await cursor.execute("EVALUATE {1, 2}")
        assert await cursor.fetchall() == [([1, 2],)]
    finally:
        await connection.close()
