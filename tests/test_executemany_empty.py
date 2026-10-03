"""Regression tests for empty executemany() result-state reset (issue #376)."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from pycubrid.aio.cursor import AsyncCursor
from pycubrid.cursor import Cursor
from pycubrid.exceptions import InterfaceError, OperationalError
from pycubrid.protocol import CloseQueryPacket, ColumnMetaData


@pytest.fixture
def mock_connection() -> MagicMock:
    conn = MagicMock()
    # A pooling-off broker: CLOSE_REQ is sent, never deferred (#488).
    conn._defer_close = MagicMock(return_value=False)
    conn.autocommit = False
    conn._connected = True
    conn._record_reply_cas_info(b"\x01\x01\x02\x03")
    conn._cursors = set()
    conn._ensure_connected = MagicMock()
    conn._no_backslash_escapes = False
    conn._fetch_size = 100
    conn._timing = None
    conn._send_and_receive = MagicMock(side_effect=lambda packet: packet)
    return conn


def test_executemany_empty_resets_stale_result_state(mock_connection: MagicMock) -> None:
    cursor = Cursor(mock_connection)
    cursor._description = (("id", 8, None, None, 10, 0, False),)
    cursor._rows = [(1,), (2,), (3,)]
    cursor._row_index = 1
    cursor._fetched_count = 1
    cursor._query_handle = 42
    cursor._rowcount = 3
    cursor._lastrowid = 123

    result = cursor.executemany("INSERT INTO t (id) VALUES (?)", [])

    assert result is cursor
    assert cursor.description is None
    assert cursor.rowcount == 0
    assert cursor._rows == []
    assert cursor._row_index == 0
    assert cursor._query_handle is None
    assert cursor.lastrowid is None
    packet = mock_connection._send_and_receive.call_args.args[0]
    assert isinstance(packet, CloseQueryPacket)
    assert packet.query_handle == 42
    with pytest.raises(InterfaceError, match="No result set available"):
        cursor.fetchone()


@pytest.mark.asyncio
async def test_async_executemany_empty_resets_stale_result_state() -> None:
    conn = MagicMock()
    # A pooling-off broker: CLOSE_REQ is sent, never deferred (#488).
    conn._defer_close = MagicMock(return_value=False)
    conn.autocommit = False
    conn._connected = True
    conn._record_reply_cas_info(b"\x01\x01\x02\x03")
    conn._cursors = set()
    conn._ensure_connected = MagicMock()
    conn._no_backslash_escapes = False
    conn._fetch_size = 100
    conn._timing = None
    conn._send_and_receive = AsyncMock()
    cursor = AsyncCursor(conn)
    cursor._description = (("id", 8, None, None, 10, 0, False),)
    cursor._rows = [(1,), (2,), (3,)]
    cursor._row_index = 1
    cursor._fetched_count = 1
    cursor._query_handle = 42
    cursor._rowcount = 3
    cursor._lastrowid = 123

    result = await cursor.executemany("INSERT INTO t (id) VALUES (?)", [])

    assert result is cursor
    assert cursor.description is None
    assert cursor.rowcount == 0
    assert cursor._rows == []
    assert cursor._row_index == 0
    assert cursor._query_handle is None
    assert cursor.lastrowid is None
    conn._send_and_receive.assert_awaited_once()
    packet = conn._send_and_receive.call_args.args[0]
    assert isinstance(packet, CloseQueryPacket)
    assert packet.query_handle == 42
    with pytest.raises(InterfaceError, match="No result set available"):
        await cursor.fetchone()


@pytest.mark.asyncio
@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("operation", ["INSERT INTO t VALUES (?)", "SELECT ?"])
@pytest.mark.parametrize("has_handle", [False, True])
async def test_empty_executemany_lifecycle(
    mock_connection: MagicMock, asynchronous: bool, operation: str, has_handle: bool
) -> None:
    conn = mock_connection
    if asynchronous:
        conn._send_and_receive = AsyncMock()
    cursor = AsyncCursor(conn) if asynchronous else Cursor(conn)
    cursor._rows = [(1,)]
    cursor._columns = [ColumnMetaData(name="id")]
    cursor._query_handle = 42 if has_handle else None
    cursor._rowcount = 10
    cursor._lastrowid = 123
    cursor._statement_type = 21
    cursor._total_tuple_count = 20
    cursor._invalidated_by_reconnect = True

    if asynchronous:
        result = await cursor.executemany(operation, [])
    else:
        result = cursor.executemany(operation, [])

    assert result is cursor
    assert cursor.rowcount == 0
    assert cursor.lastrowid is None
    assert cursor._rows == []
    assert cursor._columns == []
    assert cursor._statement_type == 0
    assert cursor._total_tuple_count == 0
    assert cursor._fetched_count == 0
    assert cursor._invalidated_by_reconnect is False
    assert cursor._connection is conn
    assert conn._send_and_receive.call_count == int(has_handle)


@pytest.mark.asyncio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_empty_executemany_close_failure_preserves_handle(
    mock_connection: MagicMock, asynchronous: bool
) -> None:
    conn = mock_connection
    error = OperationalError("query close failed")
    conn._send_and_receive = (
        AsyncMock(side_effect=error) if asynchronous else MagicMock(side_effect=error)
    )
    cursor = AsyncCursor(conn) if asynchronous else Cursor(conn)
    cursor._query_handle = 42
    cursor._rows = [(1,)]
    cursor._description = (("id", 8, None, None, 10, 0, False),)
    cursor._rowcount = 10
    cursor._lastrowid = 123
    with pytest.raises(OperationalError) as raised:
        if asynchronous:
            await cursor.executemany("INSERT INTO t VALUES (?)", [])
        else:
            cursor.executemany("INSERT INTO t VALUES (?)", [])

    assert raised.value is error
    assert cursor._query_handle == 42
    assert cursor._rows == [(1,)]
    assert cursor.description is not None
    assert cursor.rowcount == 10
    assert cursor.lastrowid == 123
    assert conn._send_and_receive.call_count == 1
