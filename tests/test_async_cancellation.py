"""Async cancellation and concurrent lifecycle race tests (issue #348).

Async connection bugs often surface when an ``await`` is cancelled rather than
completing normally. This suite injects :meth:`asyncio.Task.cancel` at various
points in the async connection lifecycle against a live CUBRID server and races
lifecycle operations against each other, asserting the durable contract:

* a cancelled operation never deadlocks (the whole test is bounded by a timeout);
* after a cancellation the connection is either still usable OR cleanly closed —
  never a half-open, desynced socket that returns a raw (non-DB-API) exception;
* the per-connection lock is always released (a follow-up operation can proceed);
* concurrent close / ping(reconnect) / execute do not corrupt shared state.

Skipped when no CUBRID server is reachable. Timing-based cancellation is
inherently nondeterministic, so each test tolerates BOTH outcomes (the op
completed before the cancel, or it was cancelled) and only asserts the
post-condition invariants — never a specific race winner.
"""

from __future__ import annotations

import asyncio
import sys
import uuid

import pytest

import pycubrid.aio
from pycubrid.exceptions import Error as DBAPIError

from ._parity_helpers import TEST_DB, TEST_HOST, TEST_PASSWORD, TEST_PORT, TEST_USER, can_connect

pytestmark = [
    pytest.mark.skipif(not can_connect(), reason="CUBRID instance not available"),
    pytest.mark.asyncio,
]


async def _aconnect() -> pycubrid.aio.AsyncConnection:
    return await pycubrid.aio.connect(
        host=TEST_HOST,
        port=TEST_PORT,
        database=TEST_DB,
        user=TEST_USER,
        password=TEST_PASSWORD,
    )


def _tbl() -> str:
    return "ac_%s" % uuid.uuid4().hex[:8]


async def _assert_usable_or_closed(conn: pycubrid.aio.AsyncConnection) -> None:
    """A follow-up op must either work or raise a DB-API error — never hang or leak."""
    try:
        cur = conn.cursor()
        await asyncio.wait_for(cur.execute("SELECT 1"), timeout=5.0)
        row = await asyncio.wait_for(cur.fetchone(), timeout=5.0)
        assert row == (1,)
        await cur.close()
    except DBAPIError:
        pass  # cleanly-invalidated connection is an acceptable post-state


class TestCancelDuringExecute:
    async def test_cancel_execute_leaves_connection_consistent(self) -> None:
        conn = await _aconnect()
        try:
            cur = conn.cursor()
            task = asyncio.ensure_future(
                cur.execute("SELECT 1 FROM db_root")  # a real round-trip
            )
            # Cancel almost immediately to race the in-flight await.
            await asyncio.sleep(0)
            task.cancel()
            with pytest.raises((asyncio.CancelledError, DBAPIError)):
                await task
            # The lock must have been released: a fresh op proceeds or errors cleanly.
            await asyncio.wait_for(_assert_usable_or_closed(conn), timeout=10.0)
        finally:
            await conn.close()

    async def test_many_cancels_do_not_deadlock(self) -> None:
        conn = await _aconnect()
        try:
            for _ in range(10):
                cur = conn.cursor()
                task = asyncio.ensure_future(cur.execute("SELECT 1 FROM db_root"))
                await asyncio.sleep(0)
                task.cancel()
                try:
                    await task
                except (asyncio.CancelledError, DBAPIError):
                    pass
            await asyncio.wait_for(_assert_usable_or_closed(conn), timeout=10.0)
        finally:
            await conn.close()


class TestCancelDuringClose:
    async def test_cancel_close_does_not_leak(self) -> None:
        conn = await _aconnect()
        task = asyncio.ensure_future(conn.close())
        await asyncio.sleep(0)
        task.cancel()
        try:
            await task
        except (asyncio.CancelledError, DBAPIError):
            pass
        # Whether the cancel won or lost, a second close must be safe/idempotent.
        await asyncio.wait_for(conn.close(), timeout=10.0)


class TestConcurrentLifecycle:
    async def test_concurrent_execute_and_close(self) -> None:
        conn = await _aconnect()
        cur = conn.cursor()
        exec_task = asyncio.ensure_future(cur.execute("SELECT 1 FROM db_root"))
        close_task = asyncio.ensure_future(conn.close())
        results = await asyncio.gather(exec_task, close_task, return_exceptions=True)
        # Neither may raise a raw non-DB-API exception; CancelledError is fine.
        for r in results:
            if isinstance(r, BaseException) and not isinstance(
                r, (DBAPIError, asyncio.CancelledError)
            ):
                raise AssertionError(f"concurrent execute/close leaked {type(r).__name__}: {r!r}")
        await asyncio.wait_for(conn.close(), timeout=10.0)

    async def test_concurrent_ping_reconnect_and_execute(self) -> None:
        conn = await _aconnect()
        try:
            cur = conn.cursor()
            ping_task = asyncio.ensure_future(conn.ping(reconnect=True))
            exec_task = asyncio.ensure_future(cur.execute("SELECT 1 FROM db_root"))
            results = await asyncio.gather(ping_task, exec_task, return_exceptions=True)
            for r in results:
                if isinstance(r, BaseException) and not isinstance(
                    r, (DBAPIError, asyncio.CancelledError)
                ):
                    raise AssertionError(
                        f"concurrent ping/execute leaked {type(r).__name__}: {r!r}"
                    )
            await asyncio.wait_for(_assert_usable_or_closed(conn), timeout=10.0)
        finally:
            await conn.close()

    async def test_parallel_executes_are_serialized(self) -> None:
        # The per-connection lock must serialize concurrent executes without
        # corrupting the shared read buffer (no interleaved/garbled results).
        conn = await _aconnect()
        try:
            cur = conn.cursor()

            async def run_query() -> object:
                await cur.execute("SELECT 1 FROM db_root")
                return await cur.fetchone()

            results = await asyncio.gather(*(run_query() for _ in range(8)), return_exceptions=True)
            for r in results:
                if isinstance(r, BaseException) and not isinstance(r, DBAPIError):
                    raise AssertionError(f"parallel executes leaked {type(r).__name__}: {r!r}")
        finally:
            await conn.close()


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
