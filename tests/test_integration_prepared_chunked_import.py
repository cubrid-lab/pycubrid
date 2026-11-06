"""The large-import recipes in docs/PERFORMANCE.md, on a live server (#1013).

Each ``executemany()`` chunk is its own request, so what these tests pin is what
the caller must account for: rows stay in one transaction until a commit, a
failing row's index restarts in each chunk, ``rowcount`` covers the last chunk,
and resuming after a committed chunk neither loses nor repeats a row.
"""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import AsyncIterator, Coroutine, Iterable, Iterator
from contextlib import suppress
from itertools import islice
from typing import Any

import pytest
import pytest_asyncio

import pycubrid
import pycubrid.aio
from pycubrid.protocol import CommitPacket, ExecuteArrayPacket

from ._parity_helpers import connect_kwargs, table_name

pytestmark = [pytest.mark.integration, pytest.mark.no_escape_pin]

MODES = pytest.mark.parametrize("mode", ["sync", "async"])
SIZE = 10


async def _call(value: Any) -> Any:
    return await value if inspect.isawaitable(value) else value


def chunks(rows: Iterable[Any], size: int) -> Iterator[list[Any]]:
    if size < 1:
        raise ValueError("size must be at least 1")
    iterator = iter(rows)
    while chunk := list(islice(iterator, size)):
        yield chunk


async def import_all_or_nothing(conn: Any, sql: str, rows: Iterable[Any], size: int = SIZE) -> int:
    cursor = conn.cursor()
    total = 0
    try:
        for chunk in chunks(rows, size):
            await _call(cursor.executemany(sql, chunk, prepared=True))
            total += cursor.rowcount
        await _call(conn.commit())
    except BaseException:  # Includes asyncio.CancelledError.
        with suppress(pycubrid.InterfaceError):  # A retired session has nothing to undo.
            await _call(conn.rollback())
        raise
    return total


async def import_in_steps(
    conn: Any, sql: str, rows: Iterable[Any], start: int = 0, size: int = SIZE
) -> AsyncIterator[int]:
    cursor = conn.cursor()
    done = start
    for chunk in chunks(islice(rows, start, None), size):
        try:
            await _call(cursor.executemany(sql, chunk, prepared=True))
            await _call(conn.commit())
        except BaseException:
            with suppress(pycubrid.InterfaceError):
                await _call(conn.rollback())
            raise
        done += len(chunk)
        yield done


@pytest_asyncio.fixture
async def conn(mode: str) -> AsyncIterator[Any]:
    kwargs = connect_kwargs()
    if mode == "sync":
        connection: Any = pycubrid.connect(**kwargs)
    else:
        connection = await pycubrid.aio.connect(**kwargs)
    try:
        yield connection
    finally:
        await _call(connection.close())


@pytest_asyncio.fixture
async def table(conn: Any) -> AsyncIterator[str]:
    name = table_name("chunked")
    cursor = conn.cursor()
    await _call(cursor.execute(f"CREATE TABLE {name} (id INT PRIMARY KEY, v INT)"))
    await _call(conn.commit())
    try:
        yield name
    finally:
        await _call(conn.rollback())
        await _call(cursor.execute(f"DROP TABLE IF EXISTS {name}"))
        await _call(conn.commit())
        await _call(cursor.close())


async def _ids(conn: Any, table: str) -> list[int]:
    cursor = conn.cursor()
    try:
        await _call(cursor.execute(f"SELECT id FROM {table} ORDER BY id"))
        return [row[0] for row in await _call(cursor.fetchall())]
    finally:
        await _call(cursor.close())


def _rows(count: int) -> Iterator[tuple[int, int]]:
    return ((i, i * 2) for i in range(count))


def _insert(table: str) -> str:
    return f"INSERT INTO {table} (id, v) VALUES (?, ?)"


@pytest.mark.asyncio
@MODES
async def test_one_transaction_imports_a_generator(conn: Any, table: str) -> None:
    assert await import_all_or_nothing(conn, _insert(table), _rows(25)) == 25
    await _call(conn.rollback())  # The commit already happened.
    assert await _ids(conn, table) == list(range(25))


@pytest.mark.asyncio
@MODES
async def test_rowcount_covers_only_the_last_chunk(conn: Any, table: str) -> None:
    cursor = conn.cursor()
    try:
        for chunk in chunks(_rows(25), SIZE):
            await _call(cursor.executemany(_insert(table), chunk, prepared=True))
        assert cursor.rowcount == 5
    finally:
        await _call(conn.rollback())
        await _call(cursor.close())


@pytest.mark.asyncio
@MODES
async def test_a_failed_row_in_one_transaction_leaves_nothing(conn: Any, table: str) -> None:
    rows = [(0 if i == 13 else i, 0) for i in range(25)]  # Row 13 repeats id 0.
    with pytest.raises(pycubrid.DatabaseError) as caught:
        await import_all_or_nothing(conn, _insert(table), rows)
    # Chunk-relative: the row is number 13 of the input, in the second chunk.
    assert caught.value.batch_index == 3
    assert await _ids(conn, table) == []


@pytest.mark.asyncio
@MODES
async def test_a_failed_chunk_keeps_earlier_steps_and_resumes(conn: Any, table: str) -> None:
    rows = [(0 if i == 13 else i, 0) for i in range(25)]
    progress = 0
    with pytest.raises(pycubrid.DatabaseError) as caught:
        async for progress in import_in_steps(conn, _insert(table), rows):
            pass
    assert progress == SIZE
    assert progress + caught.value.batch_index == 13
    assert await _ids(conn, table) == list(range(SIZE))

    rows[13] = (13, 0)
    async for progress in import_in_steps(conn, _insert(table), rows, start=progress):
        pass
    assert progress == 25
    assert await _ids(conn, table) == list(range(25))


@pytest.mark.asyncio
@MODES
async def test_a_generator_error_sends_nothing_from_its_chunk(conn: Any, table: str) -> None:
    def broken() -> Iterator[tuple[int, int]]:
        yield from _rows(15)
        raise ValueError("source failed")

    with pytest.raises(ValueError, match="source failed"):
        await import_all_or_nothing(conn, _insert(table), broken())
    assert await _ids(conn, table) == []

    progress = 0
    with pytest.raises(ValueError, match="source failed"):
        async for progress in import_in_steps(conn, _insert(table), broken()):
            pass
    assert progress == SIZE
    assert await _ids(conn, table) == list(range(SIZE))


@pytest.mark.asyncio
@MODES
@pytest.mark.parametrize("size", [0, -1])
async def test_a_nonpositive_size_imports_nothing_and_says_so(
    conn: Any, table: str, size: int
) -> None:
    with pytest.raises(ValueError, match="at least 1"):
        await import_all_or_nothing(conn, _insert(table), _rows(3), size)
    with pytest.raises(ValueError, match="at least 1"):
        async for _ in import_in_steps(conn, _insert(table), _rows(3), size=size):
            pass
    assert await _ids(conn, table) == []


@pytest.mark.asyncio
@MODES
async def test_a_base_exception_from_the_input_leaves_nothing(conn: Any, table: str) -> None:
    # The recipes' cleanup only: nothing is in flight. See the tests that follow.
    def cancelled() -> Iterator[tuple[int, int]]:
        yield from _rows(15)
        raise asyncio.CancelledError

    with pytest.raises(asyncio.CancelledError):
        await import_all_or_nothing(conn, _insert(table), cancelled())
    await _call(conn.commit())
    assert await _ids(conn, table) == []


@pytest_asyncio.fixture
async def sessions() -> AsyncIterator[tuple[Any, Any, str]]:
    """A session to cancel, another that outlives it, and their table."""
    name = table_name("chunked_cancel")
    cancelled = await pycubrid.aio.connect(**connect_kwargs())
    observer = await pycubrid.aio.connect(**connect_kwargs())
    cursor = observer.cursor()
    await cursor.execute(f"CREATE TABLE {name} (id INT PRIMARY KEY, v INT)")
    await observer.commit()
    try:
        yield cancelled, observer, name
    finally:
        await observer.rollback()
        await cursor.execute(f"DROP TABLE IF EXISTS {name}")
        await observer.commit()
        await cursor.close()
        await observer.close()
        await cancelled.close()


async def _cancel_in_flight(
    conn: Any, packet_type: type, nth: int, work: Coroutine[Any, Any, Any]
) -> None:
    """Cancel *work* with the *nth* such request sent and its reply unread."""
    task = asyncio.create_task(work)
    send = conn._do_send_and_receive
    seen = 0

    async def counting_send(packet: Any) -> Any:
        nonlocal seen
        if isinstance(packet, packet_type):
            seen += 1
            if seen == nth:
                drain = conn._writer.drain

                async def drain_then_cancel() -> None:
                    await drain()
                    task.cancel()

                conn._writer.drain = drain_then_cancel
        return await send(packet)

    conn._do_send_and_receive = counting_send
    with pytest.raises(asyncio.CancelledError):
        await task
    assert seen == nth
    with pytest.raises(pycubrid.InterfaceError):  # The session is retired.
        await conn.rollback()


async def _settled_ids(conn: Any, table: str, expected: list[int]) -> list[int]:
    # A commit whose reply was never read can still land after the cancellation.
    ids: list[int] = []
    for _ in range(50):
        ids = await _ids(conn, table)
        await conn.commit()
        if ids == expected:
            break
        await asyncio.sleep(0.1)
    return ids


async def _take_all_steps(conn: Any, sql: str, count: int, done: list[int]) -> None:
    async for progress in import_in_steps(conn, sql, _rows(count), done[-1]):
        done.append(progress)


@pytest.mark.asyncio
async def test_cancelling_a_sent_chunk_discards_the_transaction(
    sessions: tuple[Any, Any, str],
) -> None:
    cancelled, observer, table = sessions
    work = import_all_or_nothing(cancelled, _insert(table), _rows(25))
    await _cancel_in_flight(cancelled, ExecuteArrayPacket, 2, work)
    # These inserts wait for the old transaction's locks and would hit its keys.
    imported = import_all_or_nothing(observer, _insert(table), _rows(25))
    assert await asyncio.wait_for(imported, timeout=60) == 25
    assert await _ids(observer, table) == list(range(25))


@pytest.mark.asyncio
async def test_a_cancelled_commit_is_all_or_nothing_but_uncertain(
    sessions: tuple[Any, Any, str],
) -> None:
    cancelled, observer, table = sessions
    work = import_all_or_nothing(cancelled, _insert(table), _rows(25))
    await _cancel_in_flight(cancelled, CommitPacket, 1, work)
    # Either outcome is valid, and rollback() could not have changed it.
    assert await _settled_ids(observer, table, list(range(25))) in ([], list(range(25)))


@pytest.mark.asyncio
async def test_a_cancelled_step_resumes_from_the_last_commit(
    sessions: tuple[Any, Any, str],
) -> None:
    cancelled, observer, table = sessions
    done = [0]
    work = _take_all_steps(cancelled, _insert(table), 25, done)
    await _cancel_in_flight(cancelled, ExecuteArrayPacket, 2, work)
    assert done == [0, SIZE]
    await asyncio.wait_for(_take_all_steps(observer, _insert(table), 25, done), timeout=60)
    assert done == [0, SIZE, 20, 25]
    assert await _ids(observer, table) == list(range(25))


@pytest.mark.asyncio
async def test_a_cancelled_step_commit_leaves_that_chunk_uncertain(
    sessions: tuple[Any, Any, str],
) -> None:
    cancelled, observer, table = sessions
    done = [0]
    work = _take_all_steps(cancelled, _insert(table), 25, done)
    await _cancel_in_flight(cancelled, CommitPacket, 2, work)
    assert done == [0, SIZE]
    ids = await _settled_ids(observer, table, list(range(20)))
    assert ids in (list(range(SIZE)), list(range(20)))


class _RetiredConnection:
    def cursor(self) -> object:
        return object()

    def rollback(self) -> None:
        raise pycubrid.InterfaceError("connection is closed")


@pytest.mark.asyncio
async def test_a_retired_session_keeps_the_original_error() -> None:
    def broken() -> Iterator[tuple[int, int]]:
        raise ValueError("source failed")
        yield  # pragma: no cover

    with pytest.raises(ValueError, match="source failed"):
        await import_all_or_nothing(_RetiredConnection(), "INSERT", broken())
