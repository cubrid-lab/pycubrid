"""Batch failures must not expose the previous operation's results (#375)."""

from __future__ import annotations

import struct
from unittest.mock import AsyncMock, MagicMock

import pytest

from pycubrid.aio.cursor import AsyncCursor
from pycubrid.cursor import Cursor
from pycubrid.exceptions import InterfaceError, OperationalError
from pycubrid.protocol import BatchExecutePacket, CloseQueryPacket


@pytest.mark.asyncio
@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("failure", ["transport", "parse", "close"])
async def test_failed_batch_replaces_stale_state(asynchronous: bool, failure: str) -> None:
    conn = MagicMock()
    conn._no_backslash_escapes = False
    conn.autocommit = False
    cursor = AsyncCursor(conn) if asynchronous else Cursor(conn)
    cursor._description = (("id", 8, None, None, 10, 0, False),)
    cursor._rows = [(1,), (2,)]
    cursor._row_index = 1
    cursor._fetched_count = 2
    cursor._query_handle = 42
    cursor._rowcount = 10
    cursor._lastrowid = 123

    error = OperationalError("malformed response" if failure == "parse" else "connection lost")
    error.__cause__ = struct.error("truncated packet") if failure == "parse" else OSError("reset")

    def send(packet: object) -> object:
        if isinstance(packet, CloseQueryPacket):
            if failure == "close":
                raise error
        else:
            assert isinstance(packet, BatchExecutePacket)
            raise error
        return packet

    conn._send_and_receive = (
        AsyncMock(side_effect=send) if asynchronous else MagicMock(side_effect=send)
    )
    with pytest.raises(OperationalError) as raised:
        if asynchronous:
            await cursor.executemany_batch(["INSERT INTO t VALUES (1)"])
        else:
            cursor.executemany_batch(["INSERT INTO t VALUES (1)"])
    assert raised.value is error
    assert isinstance(conn._send_and_receive.call_args_list[0].args[0], CloseQueryPacket)

    if failure == "close":
        assert conn._send_and_receive.call_count == 1
        assert cursor._query_handle == 42
        assert cursor.description is not None
        assert cursor._rows == [(1,), (2,)]
        assert cursor.rowcount == 10
        assert cursor.lastrowid == 123
        return

    assert conn._send_and_receive.call_count == 2
    assert cursor._query_handle is None
    assert cursor.description is None
    assert cursor._rows == []
    assert cursor._row_index == 0
    assert cursor._fetched_count == 0
    assert cursor.rowcount == -1
    assert cursor.lastrowid is None
    with pytest.raises(InterfaceError, match="No result set available"):
        if asynchronous:
            await cursor.fetchone()
        else:
            cursor.fetchone()
