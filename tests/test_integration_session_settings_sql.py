"""The SQL read of session settings that the Compat migration guide documents (#1129)."""

from __future__ import annotations

import inspect
import socket
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Any

import pytest
import pytest_asyncio

import pycubrid
import pycubrid.aio

from ._parity_helpers import connect_kwargs, table_name

pytestmark = [pytest.mark.integration, pytest.mark.no_escape_pin]

MODES = pytest.mark.parametrize("mode", ["sync", "async"])
Connect = Callable[..., Awaitable[Any]]


async def _call(value: Any) -> Any:
    return await value if inspect.isawaitable(value) else value


async def _run(conn: Any, *statements: str) -> list[tuple[Any, ...]]:
    cursor = conn.cursor()
    rows: list[tuple[Any, ...]] = []
    try:
        for sql in statements:
            await _call(cursor.execute(sql))
            rows = [] if cursor.description is None else list(await _call(cursor.fetchall()))
        return rows
    finally:
        await _call(cursor.close())


async def _settings(conn: Any) -> tuple[int, float]:
    level = await _run(conn, "GET TRANSACTION ISOLATION LEVEL TO iso", "SELECT iso")
    timeout = await _run(conn, "GET TRANSACTION LOCK TIMEOUT TO lk", "SELECT lk")
    return level[0][0], timeout[0][0]


@pytest_asyncio.fixture
async def connect(mode: str) -> AsyncIterator[Connect]:
    opened: list[Any] = []

    async def open_connection(*, autocommit: bool) -> Any:
        driver: Any = pycubrid.aio if mode == "async" else pycubrid
        conn = await _call(driver.connect(**connect_kwargs(), autocommit=autocommit))
        opened.append(conn)
        return conn

    yield open_connection
    for conn in reversed(opened):
        await _call(conn.close())


@MODES
@pytest.mark.asyncio
async def test_reads_the_settings_made_with_sql(connect: Connect) -> None:
    conn = await connect(autocommit=False)
    assert await _settings(conn) == (4, -1.0)
    await _run(conn, "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
    await _run(conn, "SET TRANSACTION LOCK TIMEOUT 5")
    assert await _settings(conn) == (5, 5.0)


@MODES
@pytest.mark.asyncio
async def test_set_and_read_keep_the_open_transaction(connect: Connect) -> None:
    observer = await connect(autocommit=True)
    actor = await connect(autocommit=False)
    table = table_name("sql_settings")
    await _run(observer, f"CREATE TABLE {table} (id INTEGER)")
    try:
        await _run(actor, f"INSERT INTO {table} VALUES (1)")
        await _run(actor, "SET TRANSACTION LOCK TIMEOUT 5")
        await _run(actor, "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        await _settings(actor)
        assert await _run(actor, f"SELECT id FROM {table}") == [(1,)]
        assert await _run(observer, f"SELECT id FROM {table}") == []
        await _call(actor.rollback())
        assert await _run(actor, f"SELECT id FROM {table}") == []
        assert await _run(observer, f"SELECT id FROM {table}") == []
    finally:
        await _call(actor.rollback())
        await _run(observer, f"DROP TABLE IF EXISTS {table}")


@MODES
@pytest.mark.asyncio
async def test_replaced_session_shows_the_defaults(connect: Connect, mode: str) -> None:
    conn = await connect(autocommit=False)
    await _run(conn, "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
    await _run(conn, "SET TRANSACTION LOCK TIMEOUT 5")
    await _call(conn.commit())
    assert await _settings(conn) == (5, 5.0)
    await _call(conn.commit())

    # The transport of an out-of-transaction session dies, as after a CAS restart.
    if mode == "sync":
        conn._socket.shutdown(socket.SHUT_RDWR)
    else:
        conn._writer.transport.abort()

    assert await _settings(conn) == (4, -1.0)
