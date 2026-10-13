"""A request refused by another task's setup gate keeps that task's session (#894).

Each test reproduces the window deterministically: the caller passes the outer
setup gate while the lock is held, then another task begins setting up a new
session and releases the lock (as recovery does around its escape-mode probe)
before the caller acquires it.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from pycubrid.aio.connection import AsyncConnection
from pycubrid.exceptions import OperationalError


def make_connection(*, out_tran: bool = False) -> tuple[AsyncConnection, MagicMock]:
    conn = AsyncConnection("localhost", 33000, "testdb", "dba", "")
    conn._connected = True
    conn._no_backslash_escapes = False
    # OUT_TRAN and unverified: the next request would normally run CHECK_CAS.
    conn._record_reply_cas_info(b"\x00\x01\x02\x03" if out_tran else b"\x01\x01\x02\x03")
    if not out_tran:
        conn._mark_cas_reply_verified()
    writer = MagicMock()
    writer.close = MagicMock()
    writer.drain = AsyncMock()
    writer.wait_closed = AsyncMock()
    conn._reader = MagicMock()
    conn._writer = writer
    conn._do_send_and_receive = AsyncMock(side_effect=AssertionError("must not send"))
    return conn, writer


async def run_in_setup_window(
    conn: AsyncConnection, operation: Callable[[], Awaitable[Any]]
) -> BaseException | None:
    setup_owner = asyncio.create_task(asyncio.sleep(3600))
    try:
        await conn._lock.acquire()
        task = asyncio.create_task(operation())
        await asyncio.sleep(0)  # Pass the outer gate and queue on the lock.
        assert not task.done()
        conn._setup_owner = setup_owner
        conn._setup_done.clear()
        conn._lock.release()  # Recovery releases the lock around its probe.
        result = (await asyncio.gather(task, return_exceptions=True))[0]
        # Setup never saw its session dropped or replaced.
        assert conn._connected is True
        assert conn._writer is not None
        conn._writer.close.assert_not_called()
        conn._do_send_and_receive.assert_not_awaited()
        return result if isinstance(result, BaseException) else None
    finally:
        setup_owner.cancel()
        conn._setup_owner = None
        conn._setup_done.set()


def assert_refused(error: BaseException | None) -> None:
    assert isinstance(error, OperationalError)
    assert "connection setup in progress" in str(error)


@pytest.mark.asyncio
async def test_get_schema_info_refused_keeps_setup_session() -> None:
    conn, _ = make_connection()

    assert_refused(await run_in_setup_window(conn, lambda: conn.get_schema_info(1, "t")))


@pytest.mark.asyncio
@pytest.mark.parametrize("name", ["commit", "rollback"])
async def test_transaction_end_refused_does_not_replace_setup_session(name: str) -> None:
    conn, _ = make_connection(out_tran=True)
    conn._connect_locked = AsyncMock(side_effect=AssertionError("must not reconnect"))

    assert_refused(await run_in_setup_window(conn, getattr(conn, name)))


@pytest.mark.asyncio
async def test_execute_refused_does_not_replace_setup_session() -> None:
    conn, _ = make_connection(out_tran=True)
    conn._connect_locked = AsyncMock(side_effect=AssertionError("must not reconnect"))
    cursor = conn.cursor()

    assert_refused(await run_in_setup_window(conn, lambda: cursor.execute("SELECT ?", (1,))))


@pytest.mark.asyncio
async def test_fetch_schema_info_refused_keeps_setup_session() -> None:
    conn, _ = make_connection()
    packet = MagicMock()
    conn._active_schema_result = MagicMock(return_value=MagicMock(count=1, handle=7, columns=[]))
    conn._close_schema_info_locked = AsyncMock()

    assert_refused(await run_in_setup_window(conn, lambda: conn.fetch_schema_info(packet)))


@pytest.mark.asyncio
async def test_close_schema_info_refused_keeps_setup_session() -> None:
    conn, _ = make_connection()
    packet = MagicMock()
    conn._owned_schema_result = MagicMock(return_value=MagicMock(handle=7))

    assert_refused(await run_in_setup_window(conn, lambda: conn.close_schema_info(packet)))


@pytest.mark.asyncio
async def test_setup_owner_still_sends_through_the_gate() -> None:
    conn, _ = make_connection()
    conn._do_send_and_receive = AsyncMock(side_effect=lambda packet: packet)
    conn._setup_owner = asyncio.current_task()
    conn._setup_done.clear()
    try:
        assert await conn._check_reconnect() is False
        sentinel = object()
        assert await conn._send_and_receive(sentinel) is sentinel
    finally:
        conn._setup_owner = None
        conn._setup_done.set()


@pytest.mark.asyncio
async def test_ping_recovery_survives_refused_get_schema_info() -> None:
    conn, _ = make_connection()
    conn._connected = False
    conn._writer = None
    replacement = MagicMock()
    replacement.wait_closed = AsyncMock()
    outcomes: list[BaseException | None] = []

    async def reconnect() -> None:
        conn._connected = True
        conn._writer = replacement
        conn._reader = MagicMock()

    async def negotiate() -> None:
        # The schema request passed the outer gate before this setup began and
        # takes the lock while the probe has released it.
        conn._wait_for_setup_if_needed = AsyncMock()
        try:
            schema = asyncio.create_task(conn.get_schema_info(1, "t"))
            result = (await asyncio.gather(schema, return_exceptions=True))[0]
        finally:
            del conn._wait_for_setup_if_needed
        outcomes.append(result if isinstance(result, BaseException) else None)
        if not conn._connected:
            raise OperationalError("probe session was dropped by another task")
        conn._no_backslash_escapes = True

    conn._no_backslash_escapes = None
    conn._connect_locked = AsyncMock(side_effect=reconnect)
    conn._negotiate_backslash_escapes = AsyncMock(side_effect=negotiate)

    assert await conn.ping(reconnect=True) is True
    assert_refused(outcomes[0])
    assert conn._connected is True
    assert conn._writer is replacement
    replacement.close.assert_not_called()
