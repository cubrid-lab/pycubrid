"""Live effective isolation level and lock timeout reads, sync and async (#1129)."""

from __future__ import annotations

import inspect
import socket
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Any

import pytest
import pytest_asyncio

import pycubrid
import pycubrid.aio
from pycubrid.constants import CUBRIDIsolationLevel

from ._parity_helpers import connect_kwargs, table_name

pytestmark = [pytest.mark.integration, pytest.mark.no_escape_pin]

MODES = pytest.mark.parametrize("mode", ["sync", "async"])
Connect = Callable[..., Awaitable[Any]]


async def _call(value: Any) -> Any:
    return await value if inspect.isawaitable(value) else value


async def _run(conn: Any, sql: str) -> list[tuple[Any, ...]]:
    cursor = conn.cursor()
    try:
        await _call(cursor.execute(sql))
        return [] if cursor.description is None else list(await _call(cursor.fetchall()))
    finally:
        await _call(cursor.close())


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
async def test_fresh_session_reports_the_server_defaults(connect: Connect) -> None:
    conn = await connect(autocommit=True)
    assert await _call(conn.get_isolation_level()) == CUBRIDIsolationLevel.REP_CLASS_COMMIT_INSTANCE
    assert await _call(conn.get_lock_timeout()) == -1


@MODES
@pytest.mark.parametrize(
    ("clause", "level"),
    [("READ COMMITTED", 4), ("REPEATABLE READ", 5), ("SERIALIZABLE", 6)],
)
@pytest.mark.asyncio
async def test_reads_the_level_set_with_sql(connect: Connect, clause: str, level: int) -> None:
    conn = await connect(autocommit=False)
    await _run(conn, f"SET TRANSACTION ISOLATION LEVEL {clause}")
    assert await _call(conn.get_isolation_level()) == level
    await _call(conn.commit())
    assert await _call(conn.get_isolation_level()) == level


@MODES
@pytest.mark.parametrize(
    ("clause", "milliseconds"),
    [("5", 5000), ("1.5", 1500), ("0", 0), ("INFINITE", -1)],
)
@pytest.mark.asyncio
async def test_reads_the_lock_timeout_set_with_sql_in_milliseconds(
    connect: Connect, clause: str, milliseconds: int
) -> None:
    conn = await connect(autocommit=False)
    await _run(conn, f"SET TRANSACTION LOCK TIMEOUT {clause}")
    assert await _call(conn.get_lock_timeout()) == milliseconds
    await _call(conn.commit())
    assert await _call(conn.get_lock_timeout()) == milliseconds


@MODES
@pytest.mark.asyncio
async def test_set_and_read_keep_the_open_transaction(connect: Connect) -> None:
    observer = await connect(autocommit=True)
    actor = await connect(autocommit=False)
    table = table_name("core_param")
    await _run(observer, f"CREATE TABLE {table} (id INTEGER)")
    try:
        await _run(actor, f"INSERT INTO {table} VALUES (1)")
        await _run(actor, "SET TRANSACTION LOCK TIMEOUT 5")
        await _run(actor, "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        await _call(actor.get_isolation_level())
        await _call(actor.get_lock_timeout())
        assert await _run(observer, f"SELECT id FROM {table}") == []
        assert await _run(actor, f"SELECT id FROM {table}") == [(1,)]
        await _call(actor.rollback())
        assert await _run(observer, f"SELECT id FROM {table}") == []
    finally:
        await _call(actor.rollback())
        await _run(observer, f"DROP TABLE IF EXISTS {table}")


@MODES
@pytest.mark.asyncio
async def test_replaced_session_reports_its_own_values(connect: Connect, mode: str) -> None:
    conn = await connect(autocommit=False)
    await _run(conn, "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
    await _run(conn, "SET TRANSACTION LOCK TIMEOUT 5")
    await _call(conn.commit())
    assert await _call(conn.get_isolation_level()) == 5
    assert await _call(conn.get_lock_timeout()) == 5000
    generation = conn._physical_generation

    # The transport of an out-of-transaction session dies, as after a CAS restart.
    if mode == "sync":
        conn._socket.shutdown(socket.SHUT_RDWR)
    else:
        conn._writer.transport.abort()

    assert await _call(conn.get_isolation_level()) == 4
    assert await _call(conn.get_lock_timeout()) == -1
    assert conn._physical_generation == generation + 1
