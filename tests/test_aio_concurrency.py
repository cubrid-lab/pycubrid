from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from pycubrid.aio.connection import AsyncConnection
from pycubrid.constants import CUBRIDStatementType
from pycubrid.exceptions import InterfaceError, OperationalError
from pycubrid.protocol import CloseDatabasePacket


def make_connected_async_connection() -> AsyncConnection:
    conn = AsyncConnection("localhost", 33000, "testdb", "dba", "")
    conn._connected = True
    conn._cas_info = b"\x01\x01\x02\x03"
    conn._reader = MagicMock()
    conn._writer = MagicMock()
    conn._writer.close = MagicMock()
    conn._writer.wait_closed = AsyncMock()
    return conn


@pytest.mark.asyncio
async def test_concurrent_execute_calls_are_serialized_per_connection() -> None:
    conn = make_connected_async_connection()
    cur1 = conn.cursor()
    cur2 = conn.cursor()

    first_started = asyncio.Event()
    release_first = asyncio.Event()
    in_flight = 0
    max_in_flight = 0
    query_handle = 0

    async def fake_do_send_and_receive(packet: Any) -> Any:
        nonlocal in_flight, max_in_flight, query_handle

        in_flight += 1
        max_in_flight = max(max_in_flight, in_flight)
        try:
            if max_in_flight > 1:
                raise AssertionError("request/response sections overlapped")

            if not first_started.is_set():
                first_started.set()
                await release_first.wait()

            await asyncio.sleep(0)

            query_handle += 1
            packet.query_handle = query_handle
            packet.statement_type = CUBRIDStatementType.SELECT
            packet.column_count = 0
            packet.columns = []
            packet.total_tuple_count = 0
            packet.rows = []
            packet.result_infos = []
            return packet
        finally:
            in_flight -= 1

    conn._do_send_and_receive = AsyncMock(side_effect=fake_do_send_and_receive)

    task1 = asyncio.create_task(cur1.execute("SELECT 1"))
    await first_started.wait()

    task2 = asyncio.create_task(cur2.execute("SELECT 2"))
    await asyncio.sleep(0)
    assert not task2.done()

    release_first.set()
    await asyncio.gather(task1, task2)

    assert max_in_flight == 1
    assert cur1._query_handle == 1
    assert cur2._query_handle == 2


@pytest.mark.asyncio
async def test_close_waits_for_in_flight_send_and_receive() -> None:
    conn = make_connected_async_connection()
    started = asyncio.Event()
    release_request = asyncio.Event()
    events: list[str] = []

    async def fake_do_send_and_receive(packet: Any) -> Any:
        if isinstance(packet, CloseDatabasePacket):
            events.append("close")
            return packet

        events.append("request-start")
        started.set()
        await release_request.wait()
        events.append("request-finish")
        return packet

    conn._do_send_and_receive = AsyncMock(side_effect=fake_do_send_and_receive)
    request = MagicMock()

    request_task = asyncio.create_task(conn._send_and_receive(request))
    await started.wait()

    close_task = asyncio.create_task(conn.close())
    await asyncio.sleep(0)
    assert not close_task.done()

    release_request.set()

    try:
        result = await request_task
        assert result is request
    except (InterfaceError, OperationalError):
        pass

    await close_task

    assert events == ["request-start", "request-finish", "close"]
    assert conn._connected is False
    assert conn._writer is None
