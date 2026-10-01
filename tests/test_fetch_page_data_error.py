"""Rows fetched before a failing page are kept, and the page is not skipped (#507).

A later FETCH page can raise a data-level ``DataError`` (invalid text #492,
unresolved zone #413, zero date #512) after the reply was read in full, so the
session stays usable and the cursor keeps its handle. The fetch call that
reaches the page raises; rows it had already collected stay buffered and the
next fetch calls return them. After that every fetch raises the same
``DataError`` without requesting the page again, so no later row is returned
and retries cannot loop on the server.
"""

from __future__ import annotations

import asyncio
import struct
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from pycubrid.aio.cursor import AsyncCursor
from pycubrid.constants import CUBRIDDataType
from pycubrid.cursor import Cursor
from pycubrid.exceptions import DataError, ProgrammingError
from pycubrid.protocol import CloseQueryPacket, FetchPacket, PrepareAndExecutePacket
from tests.test_execute_failure import _assert_no_result, _call
from tests.test_json_decode import _build_select_response
from tests.test_zero_temporal_values import _ZERO_DATE, _fetch_body

# Five rows in three pages: the execute reply carries row 1, FETCH pages carry
# rows 2-3 and rows 4-5, and row 5 is a zero date Python cannot represent.
_TOTAL = 5


def _date(day: int) -> bytes:
    return struct.pack(">3h", 2024, 1, day)


_PAGES = {1: [_date(2), _date(3)], 3: [_date(4), _ZERO_DATE]}
_GOOD_PAGES = {1: [_date(2), _date(3)], 3: [_date(4), _date(5)]}


def _day(row: tuple[Any, ...]) -> int:
    return int(row[0].day)


class _Broker:
    """Scripted replies; with ``bad_fetches`` page 3 decodes after that many failures."""

    def __init__(self, bad_fetches: int | None = None) -> None:
        self.positions: list[int] = []
        self.bad_fetches = bad_fetches

    def reply(self, packet: object, **_kwargs: object) -> object:
        if isinstance(packet, PrepareAndExecutePacket):
            packet.parse(_build_select_response([(CUBRIDDataType.DATE, "d")], [_date(1)]))
            packet.total_tuple_count = _TOTAL
        elif isinstance(packet, FetchPacket):
            position = packet.current_tuple_count
            self.positions.append(position)
            pages = _PAGES
            if self.bad_fetches is not None:
                if self.bad_fetches <= 0:
                    pages = _GOOD_PAGES
                elif position == 3:
                    self.bad_fetches -= 1
            packet.parse(_fetch_body(pages[position]))
        return packet


def _connection(broker: _Broker, asynchronous: bool) -> MagicMock:
    connection = MagicMock()
    connection._timing = None
    connection._cursors = set()
    connection.autocommit = True
    connection._protocol_version = 8
    connection._decode_collections = False
    connection._json_deserializer = None
    connection._fetch_size = 2
    if asynchronous:
        connection._send_and_receive = AsyncMock(side_effect=broker.reply)
        connection._wait_for_setup_if_needed = AsyncMock()
        connection._generation_for_binding = AsyncMock(return_value=1)
    else:
        connection._send_and_receive = MagicMock(side_effect=broker.reply)
        connection._generation_for_binding = MagicMock(return_value=1)
    return connection


class _Sync:
    """Run the same scenario against the sync cursor."""

    def __init__(self, broker: _Broker) -> None:
        self.cursor = Cursor(_connection(broker, asynchronous=False))

    async def execute(self) -> None:
        self.cursor.execute("SELECT d FROM t")

    async def fetchone(self) -> tuple[Any, ...] | None:
        return self.cursor.fetchone()

    async def fetchmany(self, size: int) -> list[tuple[Any, ...]]:
        return self.cursor.fetchmany(size)

    async def fetchall(self) -> list[tuple[Any, ...]]:
        return self.cursor.fetchall()

    async def iterate(self, seen: list[tuple[Any, ...]]) -> None:
        for row in self.cursor:
            seen.append(row)


class _Async:
    """Run the same scenario against the async cursor."""

    def __init__(self, broker: _Broker) -> None:
        self.cursor = AsyncCursor(_connection(broker, asynchronous=True))

    async def execute(self) -> None:
        await self.cursor.execute("SELECT d FROM t")

    async def fetchone(self) -> tuple[Any, ...] | None:
        return await self.cursor.fetchone()

    async def fetchmany(self, size: int) -> list[tuple[Any, ...]]:
        return await self.cursor.fetchmany(size)

    async def fetchall(self) -> list[tuple[Any, ...]]:
        return await self.cursor.fetchall()

    async def iterate(self, seen: list[tuple[Any, ...]]) -> None:
        async for row in self.cursor:
            seen.append(row)


CURSORS = [pytest.param(_Sync, id="sync"), pytest.param(_Async, id="async")]


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", CURSORS)
async def test_fetchall_keeps_rows_before_failing_page(kind: type) -> None:
    broker = _Broker()
    cur = kind(broker)
    await cur.execute()
    with pytest.raises(DataError, match="cannot be represented"):
        await cur.fetchall()
    # Rows 1-3 were collected before page 3 failed; the next call returns them.
    assert [_day(row) for row in await cur.fetchall()] == [1, 2, 3]
    # Nothing left to return: every retry raises the same error without
    # requesting the page again.
    for _ in range(2):
        with pytest.raises(DataError, match="cannot be represented"):
            await cur.fetchall()
    assert broker.positions == [1, 3]
    assert cur.cursor._query_handle == 1
    assert cur.cursor.description is not None


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", CURSORS)
@pytest.mark.parametrize(
    ("size", "before", "held"),
    [
        # fetchmany(2): [1, 2], then [3] + page 3 fails; row 3 is held.
        (2, [[1, 2]], [3]),
        # fetchmany(4): rows 1-3 collected across pages, then page 3 fails.
        (4, [], [1, 2, 3]),
        # fetchmany(3) ends exactly at the page boundary; the next call fails
        # with nothing collected.
        (3, [[1, 2, 3]], []),
    ],
)
async def test_fetchmany_spanning_failing_page_keeps_rows(
    kind: type, size: int, before: list[list[int]], held: list[int]
) -> None:
    broker = _Broker()
    cur = kind(broker)
    await cur.execute()
    for expected in before:
        assert [_day(row) for row in await cur.fetchmany(size)] == expected
    with pytest.raises(DataError, match="cannot be represented"):
        await cur.fetchmany(size)
    if held:
        assert [_day(row) for row in await cur.fetchmany(size)] == held
    with pytest.raises(DataError, match="cannot be represented"):
        await cur.fetchmany(size)
    assert broker.positions == [1, 3]


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", CURSORS)
async def test_held_rows_are_returned_by_any_fetch_method(kind: type) -> None:
    broker = _Broker()
    cur = kind(broker)
    await cur.execute()
    with pytest.raises(DataError, match="cannot be represented"):
        await cur.fetchall()
    assert _day(await cur.fetchone()) == 1
    assert [_day(row) for row in await cur.fetchmany(5)] == [2, 3]
    with pytest.raises(DataError, match="cannot be represented"):
        await cur.fetchone()
    assert broker.positions == [1, 3]


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", CURSORS)
async def test_iteration_yields_rows_before_failing_page(kind: type) -> None:
    broker = _Broker()
    cur = kind(broker)
    await cur.execute()
    seen: list[tuple[Any, ...]] = []
    with pytest.raises(DataError, match="cannot be represented"):
        await cur.iterate(seen)
    assert [_day(row) for row in seen] == [1, 2, 3]
    with pytest.raises(DataError, match="cannot be represented"):
        await cur.iterate(seen)
    assert len(seen) == 3


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", CURSORS)
async def test_failing_page_is_not_skipped(kind: type) -> None:
    # Even a broker that would send the page again in a decodable form never
    # gets asked: after the held rows every fetch raises, so no row of or past
    # the failing page is returned out of order.
    broker = _Broker(bad_fetches=1)
    cur = kind(broker)
    await cur.execute()
    with pytest.raises(DataError, match="cannot be represented") as first:
        await cur.fetchall()
    assert [_day(row) for row in await cur.fetchall()] == [1, 2, 3]
    for fetch in (cur.fetchall, cur.fetchone, lambda: cur.fetchmany(1)):
        with pytest.raises(DataError) as again:
            await fetch()
        assert again.value is first.value
    assert broker.positions == [1, 3]
    # Re-executing starts a new result set that reads every page.
    await cur.execute()
    assert [_day(row) for row in await cur.fetchall()] == [1, 2, 3, 4, 5]
    assert broker.positions == [1, 3, 1, 3]


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", CURSORS)
async def test_execute_clears_held_page_error(kind: type) -> None:
    broker = _Broker()
    cur = kind(broker)
    await cur.execute()
    with pytest.raises(DataError, match="cannot be represented"):
        await cur.fetchmany(4)
    # A new result set must fetch across pages normally, not stop early.
    await cur.execute()
    assert [_day(row) for row in await cur.fetchmany(3)] == [1, 2, 3]


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", CURSORS)
@pytest.mark.parametrize("failure", ["binding", "sql"])
async def test_failed_execute_clears_held_page_error(kind: type, failure: str) -> None:
    broker = _Broker(bad_fetches=1)
    cur = kind(broker)
    await cur.execute()
    with pytest.raises(DataError) as previous:
        await cur.fetchall()
    assert _day(await cur.fetchone()) == 1
    cursor = cur.cursor
    assert cursor._page_error is previous.value
    assert cursor._row_index == 1
    connection = cursor._connection
    error = ProgrammingError("replacement failed")

    def fail_replacement(packet: object, **kwargs: object) -> object:
        if isinstance(packet, PrepareAndExecutePacket):
            raise error
        return broker.reply(packet, **kwargs)

    connection._send_and_receive.reset_mock()
    connection._send_and_receive.side_effect = fail_replacement
    with pytest.raises(ProgrammingError) as raised:
        await _call(
            cursor,
            "execute",
            "SELECT ?" if failure == "binding" else "INVALID SQL",
            (1, 2) if failure == "binding" else None,
        )
    assert raised.value is not previous.value
    if failure == "sql":
        assert raised.value is error
    await _assert_no_result(cursor)
    assert cursor._query_handle is None
    packets = [call.args[0] for call in connection._send_and_receive.call_args_list]
    assert isinstance(packets[0], CloseQueryPacket)
    assert packets[0].query_handle == 1
    assert len(packets) == (1 if failure == "binding" else 2)
    assert broker.positions == [1, 3]

    connection._send_and_receive.side_effect = broker.reply
    await cur.execute()
    assert [_day(row) for row in await cur.fetchall()] == [1, 2, 3, 4, 5]
    assert broker.positions == [1, 3, 1, 3]


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", CURSORS)
async def test_close_failure_preserves_held_page_error(kind: type) -> None:
    broker = _Broker(bad_fetches=1)
    cur = kind(broker)
    await cur.execute()
    with pytest.raises(DataError) as previous:
        await cur.fetchall()
    assert _day(await cur.fetchone()) == 1
    cursor = cur.cursor
    before = {
        "_description": cursor._description,
        "_columns": list(cursor._columns),
        "_rows": list(cursor._rows),
        "_row_index": cursor._row_index,
        "_fetched_count": cursor._fetched_count,
        "_total_tuple_count": cursor._total_tuple_count,
        "_rowcount": cursor._rowcount,
        "_lastrowid": cursor._lastrowid,
        "_query_handle": cursor._query_handle,
    }
    connection = cursor._connection
    error = ProgrammingError("close failed")
    connection._send_and_receive.reset_mock()
    connection._generation_for_binding.reset_mock()
    connection._send_and_receive.side_effect = error
    with pytest.raises(ProgrammingError) as raised:
        await _call(cursor, "execute", "SELECT ?", (99,))
    assert raised.value is error
    assert {name: getattr(cursor, name) for name in before} == before
    assert cursor._page_error is previous.value
    connection._generation_for_binding.assert_not_called()
    assert [_day(row) for row in await cur.fetchall()] == [2, 3]
    for fetch in (cur.fetchone, lambda: cur.fetchmany(1), cur.fetchall):
        with pytest.raises(DataError) as again:
            await fetch()
        assert again.value is previous.value
    connection._send_and_receive.assert_called_once()
    assert broker.positions == [1, 3]

    connection._send_and_receive.side_effect = broker.reply
    await cur.execute()
    packets = [call.args[0] for call in connection._send_and_receive.call_args_list]
    assert [packet.query_handle for packet in packets if isinstance(packet, CloseQueryPacket)] == [
        1,
        1,
    ]
    assert [_day(row) for row in await cur.fetchall()] == [1, 2, 3, 4, 5]
    assert broker.positions == [1, 3, 1, 3]


@pytest.mark.asyncio
async def test_async_execute_cancellation_clears_held_page_error() -> None:
    broker = _Broker(bad_fetches=1)
    cur = _Async(broker)
    await cur.execute()
    with pytest.raises(DataError) as previous:
        await cur.fetchall()
    assert _day(await cur.fetchone()) == 1
    cursor = cur.cursor
    assert cursor._page_error is previous.value
    connection = cursor._connection
    connection._send_and_receive.reset_mock()
    started = asyncio.Event()

    async def wait_for_reply(packet: object, **kwargs: object) -> object:
        if isinstance(packet, PrepareAndExecutePacket):
            started.set()
            await asyncio.Future()
        return broker.reply(packet, **kwargs)

    connection._send_and_receive.side_effect = wait_for_reply
    task = asyncio.create_task(cursor.execute("SELECT 99"))
    await asyncio.wait_for(started.wait(), timeout=5)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, timeout=5)
    await _assert_no_result(cursor)
    assert cursor._query_handle is None
    packets = [call.args[0] for call in connection._send_and_receive.call_args_list]
    assert len(packets) == 2
    assert isinstance(packets[0], CloseQueryPacket)
    assert packets[0].query_handle == 1
    assert isinstance(packets[1], PrepareAndExecutePacket)
    assert broker.positions == [1, 3]

    connection._send_and_receive.side_effect = broker.reply
    await cur.execute()
    assert [_day(row) for row in await cur.fetchall()] == [1, 2, 3, 4, 5]
    assert broker.positions == [1, 3, 1, 3]
