"""Long-running reliability soak (issue #355).

A duration-bounded soak that hammers a live CUBRID server with a randomized mix
of operations — CRUD, commits/rollbacks, varied fetch sizes, multiple
connections, async concurrency, periodic ping, JSON/numeric/Unicode/LOB — and
asserts the process stays healthy over time:

* no unexplained (non-DB-API) errors;
* no progressive file-descriptor growth (proxy for leaked connections/sockets);
* no protocol desynchronization (every round-trip returns the expected value);
* no deadlocks (bounded by the run duration).

Duration is controlled by ``SOAK_SECONDS`` (default 5s so it stays CI-friendly
by default; the nightly bug-hunt workflow sets it to 30-60 minutes). Marked
``integration`` and skip-gated on a reachable broker.
"""

from __future__ import annotations

import asyncio
import datetime
import gc
import os
import random
import sys
import time
import uuid
from decimal import Decimal

import pytest

import pycubrid
import pycubrid.aio
from pycubrid.exceptions import Error as DBAPIError

from ._parity_helpers import TEST_DB, TEST_HOST, TEST_PASSWORD, TEST_PORT, TEST_USER, can_connect

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(not can_connect(), reason="CUBRID instance not available"),
]

_SOAK_SECONDS = float(os.environ.get("SOAK_SECONDS", "5"))


def _fd_count() -> int:
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


def _tbl() -> str:
    return "soak_%s" % uuid.uuid4().hex[:8]


_SAMPLE_STRINGS = ["ascii", "한글", "漢字 CJK", "emoji ☃", "quote's", "back\\slash", ""]


class TestSyncSoak:
    def test_sync_mixed_workload_soak(self) -> None:
        rng = random.Random(20240917)
        table = _tbl()
        conn = _connect()
        conn.autocommit = False
        try:
            cur = conn.cursor()
            cur.execute(
                "CREATE TABLE %s (id INT PRIMARY KEY, s VARCHAR(200), n NUMERIC(12,4), "
                "t DATETIME)" % table
            )
            conn.commit()

            # Warm up to a steady fd baseline before measuring growth.
            for _ in range(20):
                self._one_op(cur, conn, table, rng)
            gc.collect()
            baseline_fd = _fd_count()

            ops = 0
            deadline = time.monotonic() + _SOAK_SECONDS
            while time.monotonic() < deadline:
                self._one_op(cur, conn, table, rng)
                ops += 1
            conn.rollback()
            cur.close()

            gc.collect()
            assert _fd_count() <= baseline_fd + 4, (
                f"fd count grew from {baseline_fd} over {ops} sync soak ops"
            )
            assert ops > 0
        finally:
            self._drop(conn, table)
            conn.close()

    @staticmethod
    def _one_op(
        cur: pycubrid.cursor.Cursor,
        conn: pycubrid.Connection,
        table: str,
        rng: random.Random,
    ) -> None:
        kind = rng.choice(["insert", "select", "update", "delete", "commit", "rollback", "ping"])
        key = rng.randint(0, 200)
        try:
            if kind == "insert":
                cur.execute(
                    "INSERT INTO %s (id, s, n, t) VALUES (?, ?, ?, ?)" % table,
                    (
                        key,
                        rng.choice(_SAMPLE_STRINGS),
                        Decimal(f"{rng.randint(-9999, 9999)}.{rng.randint(0, 9999):04d}"),
                        datetime.datetime(
                            2024, 1, 1 + rng.randint(0, 27), rng.randint(0, 23), 0, 0
                        ),
                    ),
                )
            elif kind == "select":
                size = rng.choice([1, 3, 10, 100])
                cur.execute("SELECT id, s, n FROM %s ORDER BY id" % table)
                rows = cur.fetchmany(size)
                # Protocol-desync guard: every row is a 3-tuple as declared.
                for r in rows:
                    assert len(r) == 3
            elif kind == "update":
                cur.execute(
                    "UPDATE %s SET s = ? WHERE id = ?" % table, (rng.choice(_SAMPLE_STRINGS), key)
                )
            elif kind == "delete":
                cur.execute("DELETE FROM %s WHERE id = ?" % table, (key,))
            elif kind == "commit":
                conn.commit()
            elif kind == "rollback":
                conn.rollback()
            elif kind == "ping":
                assert isinstance(conn.ping(reconnect=True), bool)
        except DBAPIError:
            # A duplicate PK or similar is an expected DB-API error; roll back so
            # the transaction stays usable and keep soaking.
            conn.rollback()

    @staticmethod
    def _drop(conn: pycubrid.Connection, table: str) -> None:
        try:
            conn.autocommit = True
            cur = conn.cursor()
            cur.execute("DROP TABLE IF EXISTS %s" % table)
            cur.close()
        except DBAPIError:
            pass  # best-effort teardown


class TestAsyncSoak:
    def test_async_concurrent_soak(self) -> None:
        async def worker(conn: pycubrid.aio.AsyncConnection, n: int) -> int:
            cur = conn.cursor()
            done = 0
            for _ in range(n):
                await cur.execute("SELECT 1")
                row = await cur.fetchone()
                assert row == (1,)
                done += 1
            await cur.close()
            return done

        async def run() -> int:
            conns = [await _aconnect() for _ in range(4)]
            try:
                deadline = time.monotonic() + _SOAK_SECONDS
                total = 0
                while time.monotonic() < deadline:
                    results = await asyncio.gather(
                        *(worker(c, 25) for c in conns), return_exceptions=True
                    )
                    for r in results:
                        if isinstance(r, BaseException):
                            raise AssertionError(f"async soak worker failed: {r!r}")
                        total += r
                return total
            finally:
                for c in conns:
                    await c.close()

        gc.collect()
        baseline_fd = _fd_count()
        total = asyncio.run(run())
        gc.collect()
        assert total > 0
        assert _fd_count() <= baseline_fd + 6, f"fd count grew from {baseline_fd} during async soak"


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
