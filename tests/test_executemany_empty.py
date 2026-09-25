"""Regression tests for empty executemany() result-state reset (issue #376)."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from pycubrid.aio.cursor import AsyncCursor
from pycubrid.cursor import Cursor
from pycubrid.exceptions import InterfaceError


@pytest.fixture
def mock_connection() -> MagicMock:
    conn = MagicMock()
    conn.autocommit = False
    conn._connected = True
    conn._cas_info = b"\x01\x01\x02\x03"
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

    result = cursor.executemany("INSERT INTO t (id) VALUES (?)", [])

    assert result is cursor
    assert cursor.description is None
    assert cursor.rowcount == 0
    assert cursor._rows == []
    assert cursor._row_index == 0
    assert cursor._query_handle is None
    with pytest.raises(InterfaceError, match="No result set available"):
        cursor.fetchone()


@pytest.mark.asyncio
async def test_async_executemany_empty_resets_stale_result_state() -> None:
    conn = MagicMock()
    conn.autocommit = False
    conn._connected = True
    conn._cas_info = b"\x01\x01\x02\x03"
    conn._cursors = set()
    conn._ensure_connected = MagicMock()
    conn._no_backslash_escapes = False
    conn._fetch_size = 100
    conn._timing = None
    cursor = AsyncCursor(conn)
    cursor._description = (("id", 8, None, None, 10, 0, False),)
    cursor._rows = [(1,), (2,), (3,)]
    cursor._row_index = 1
    cursor._fetched_count = 1
    cursor._query_handle = 42
    cursor._rowcount = 3

    result = await cursor.executemany("INSERT INTO t (id) VALUES (?)", [])

    assert result is cursor
    assert cursor.description is None
    assert cursor.rowcount == 0
    assert cursor._rows == []
    assert cursor._row_index == 0
    assert cursor._query_handle is None
    with pytest.raises(InterfaceError, match="No result set available"):
        await cursor.fetchone()
