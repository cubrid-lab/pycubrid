"""Unfinished invalidated results must not report normal EOF (#395)."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from pycubrid.aio.connection import AsyncConnection
from pycubrid.aio.cursor import AsyncCursor
from pycubrid.connection import Connection
from pycubrid.constants import CUBRIDStatementType
from pycubrid.cursor import Cursor
from pycubrid.exceptions import InterfaceError, OperationalError
from pycubrid.protocol import (
    CloseQueryPacket,
    CommitPacket,
    PrepareAndExecutePacket,
    RollbackPacket,
)
from tests.test_cursor import _set_prepare_packet
from tests.test_network_edge_cases import make_connected_connection


def _cursor(
    asynchronous: bool, total: int = 5
) -> tuple[Connection | AsyncConnection, Cursor | AsyncCursor]:
    if asynchronous:
        connection: Connection | AsyncConnection = AsyncConnection(
            "localhost", 33000, "testdb", "dba", "", no_backslash_escapes=True
        )
        connection._connected = True
    else:
        connection, _ = make_connected_connection()

    def send(packet: object) -> None:
        if isinstance(packet, PrepareAndExecutePacket):
            _set_prepare_packet(
                packet, stmt_type=CUBRIDStatementType.SELECT, rows=[(9,)], total_count=1
            )
        else:
            assert isinstance(packet, (CloseQueryPacket, CommitPacket, RollbackPacket))

    connection._send_and_receive = (
        AsyncMock(side_effect=send) if asynchronous else MagicMock(side_effect=send)
    )
    if isinstance(connection, AsyncConnection):
        connection._send_and_receive_locked = connection._send_and_receive
    cursor = connection.cursor()
    cursor._description = (("id", 8, None, None, 10, 0, False),)
    cursor._rows = [(0,), (1,), (2,)]
    cursor._row_index = 1
    cursor._fetched_count = 3
    cursor._total_tuple_count = total
    cursor._query_handle = 42
    return connection, cursor


async def _boundary(connection: Connection | AsyncConnection, operation: str) -> None:
    if isinstance(connection, AsyncConnection):
        if operation == "commit":
            await connection.commit()
        else:
            await connection.rollback()
    elif operation == "commit":
        connection.commit()
    else:
        connection.rollback()


async def _fetch(cursor: Cursor | AsyncCursor, method: str) -> object:
    if isinstance(cursor, AsyncCursor):
        if method == "one":
            return await cursor.fetchone()
        if method == "many":
            return await cursor.fetchmany(4)
        return await cursor.fetchall()
    if method == "one":
        return cursor.fetchone()
    if method == "many":
        return cursor.fetchmany(4)
    return cursor.fetchall()


@pytest.mark.asyncio
@pytest.mark.parametrize("asynchronous", [False, True], ids=["sync", "async"])
@pytest.mark.parametrize("operation", ["commit", "rollback"])
@pytest.mark.parametrize("method", ["one", "many", "all"])
async def test_unfinished_result_raises_at_missing_fetch(
    asynchronous: bool, operation: str, method: str
) -> None:
    connection, cursor = _cursor(asynchronous)
    await _boundary(connection, operation)
    assert cursor._query_handle is None
    assert cursor._fetched_count == 3 < cursor._total_tuple_count
    if method == "one":
        assert await _fetch(cursor, "one") == (1,)
        assert await _fetch(cursor, "one") == (2,)
    with pytest.raises(InterfaceError, match="invalidated"):
        await _fetch(cursor, method)
    assert connection._send_and_receive.call_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("asynchronous", [False, True], ids=["sync", "async"])
@pytest.mark.parametrize("operation", ["commit", "rollback"])
async def test_fully_buffered_and_exhausted_remain_normal(
    asynchronous: bool, operation: str
) -> None:
    connection, cursor = _cursor(asynchronous, total=3)
    await _boundary(connection, operation)
    assert await _fetch(cursor, "all") == [(1,), (2,)]
    assert await _fetch(cursor, "one") is None
    assert await _fetch(cursor, "many") == []
    assert await _fetch(cursor, "all") == []
    assert connection._send_and_receive.call_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("asynchronous", [False, True], ids=["sync", "async"])
async def test_reconnect_retains_operational_error(asynchronous: bool) -> None:
    connection, cursor = _cursor(asynchronous)
    connection._invalidate_query_handles_for_reconnect()
    assert await _fetch(cursor, "one") == (1,)
    assert await _fetch(cursor, "one") == (2,)
    with pytest.raises(OperationalError, match="result set lost due to broker reconnect mid-fetch"):
        await _fetch(cursor, "one")
    connection._send_and_receive.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("asynchronous", [False, True], ids=["sync", "async"])
async def test_size_zero_fresh_execute_and_empty_reset(asynchronous: bool) -> None:
    connection, cursor = _cursor(asynchronous)
    await _boundary(connection, "commit")
    if isinstance(cursor, AsyncCursor):
        assert await cursor.fetchmany(0) == []
        await cursor.execute("SELECT 9")
    else:
        assert cursor.fetchmany(0) == []
        cursor.execute("SELECT 9")
    assert await _fetch(cursor, "all") == [(9,)]
    if isinstance(cursor, AsyncCursor):
        await cursor.executemany("SELECT ?", [])
    else:
        cursor.executemany("SELECT ?", [])
    with pytest.raises(InterfaceError, match="No result set available"):
        await _fetch(cursor, "one")
    await cursor.close() if isinstance(cursor, AsyncCursor) else cursor.close()
    with pytest.raises(InterfaceError, match="closed"):
        await _fetch(cursor, "one")
