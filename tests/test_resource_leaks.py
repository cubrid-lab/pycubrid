"""Resource-leak detection under repeated lifecycle (issue #349).

Runs many connect/cursor/execute/close cycles against a live CUBRID server and
asserts the process does not steadily accumulate operating-system resources:
open file descriptors and sockets must return to a stable baseline, and asyncio
transports must not pile up.

Leak detection is inherently noisy (allocator caching, lazy GC), so each test
uses a warm-up phase to reach steady state, then asserts the descriptor count
after a large batch of cycles stays within a small tolerance of the post-warm-up
baseline rather than requiring an exact match.

Skipped when no CUBRID server is reachable. Marked slow via the ``integration``
marker; the cycle counts are modest so it stays CI-friendly.
"""

from __future__ import annotations

import asyncio
import gc
import os
import sys

import pytest

import pycubrid
import pycubrid.aio
from pycubrid.exceptions import Error as DBAPIError

from ._parity_helpers import TEST_DB, TEST_HOST, TEST_PASSWORD, TEST_PORT, TEST_USER, can_connect

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(not can_connect(), reason="CUBRID instance not available"),
]


def _fd_count() -> int:
    """Number of open file descriptors for this process (Linux /proc)."""
    try:
        return len(os.listdir("/proc/self/fd"))
    except OSError:
        pytest.skip("cannot count file descriptors on this platform")
        raise  # unreachable; pytest.skip raises, but satisfies the type checker


def _connect() -> pycubrid.Connection:
    return pycubrid.connect(
        host=TEST_HOST,
        port=TEST_PORT,
        database=TEST_DB,
        user=TEST_USER,
        password=TEST_PASSWORD,
    )


async def _aconnect() -> pycubrid.aio.AsyncConnection:
    return await pycubrid.aio.connect(
        host=TEST_HOST,
        port=TEST_PORT,
        database=TEST_DB,
        user=TEST_USER,
        password=TEST_PASSWORD,
    )


# A couple of extra fds is acceptable slack (logging handlers, allocator, etc.).
_FD_TOLERANCE = 4


class TestSyncResourceLifecycle:
    def test_connect_close_cycles_do_not_leak_fds(self) -> None:
        for _ in range(10):  # warm-up to steady state
            _connect().close()
        gc.collect()
        baseline = _fd_count()

        for _ in range(200):
            _connect().close()
        gc.collect()

        assert _fd_count() <= baseline + _FD_TOLERANCE, (
            f"fd count grew from {baseline} after 200 connect/close cycles"
        )

    def test_cursor_execute_cycles_do_not_leak_fds(self) -> None:
        with _connect() as conn:
            for _ in range(10):
                cur = conn.cursor()
                cur.execute("SELECT 1")
                cur.fetchall()
                cur.close()
            gc.collect()
            baseline = _fd_count()

            for _ in range(500):
                cur = conn.cursor()
                cur.execute("SELECT 1")
                cur.fetchall()
                cur.close()
            gc.collect()

            assert _fd_count() <= baseline + _FD_TOLERANCE, (
                f"fd count grew from {baseline} after 500 cursor cycles"
            )

    def test_failed_connect_does_not_leak_fds(self) -> None:
        # Connecting to a closed port must not leak a socket per failed attempt.
        for _ in range(5):
            with pytest.raises(DBAPIError):
                pycubrid.connect(
                    host="127.0.0.1",
                    port=1,  # unassigned/refused
                    database=TEST_DB,
                    user=TEST_USER,
                    password=TEST_PASSWORD,
                    connect_timeout=2,
                )
        gc.collect()
        baseline = _fd_count()

        for _ in range(50):
            with pytest.raises(DBAPIError):
                pycubrid.connect(
                    host="127.0.0.1",
                    port=1,
                    database=TEST_DB,
                    user=TEST_USER,
                    password=TEST_PASSWORD,
                    connect_timeout=2,
                )
        gc.collect()

        assert _fd_count() <= baseline + _FD_TOLERANCE, (
            f"fd count grew from {baseline} after 50 failed connects"
        )


class TestAsyncResourceLifecycle:
    def test_async_connect_close_cycles_do_not_leak_fds(self) -> None:
        async def cycle(n: int) -> None:
            for _ in range(n):
                conn = await _aconnect()
                await conn.close()

        asyncio.run(cycle(10))  # warm-up
        gc.collect()
        baseline = _fd_count()

        asyncio.run(cycle(150))
        gc.collect()

        assert _fd_count() <= baseline + _FD_TOLERANCE, (
            f"fd count grew from {baseline} after 150 async connect/close cycles"
        )

    def test_async_execute_cycles_do_not_leak_fds(self) -> None:
        async def run() -> tuple[int, int]:
            conn = await _aconnect()
            try:
                for _ in range(10):
                    cur = conn.cursor()
                    await cur.execute("SELECT 1")
                    await cur.fetchall()
                    await cur.close()
                gc.collect()
                base = _fd_count()

                for _ in range(300):
                    cur = conn.cursor()
                    await cur.execute("SELECT 1")
                    await cur.fetchall()
                    await cur.close()
                gc.collect()
                return base, _fd_count()
            finally:
                await conn.close()

        baseline, after = asyncio.run(run())
        assert after <= baseline + _FD_TOLERANCE, (
            f"fd count grew from {baseline} to {after} after 300 async cursor cycles"
        )


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
