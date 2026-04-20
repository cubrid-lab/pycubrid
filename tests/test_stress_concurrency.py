"""Concurrency stress tests against a live CUBRID instance.

Threading: each thread owns its own Connection (threadsafety=1).
Asyncio: many AsyncConnections invoked via asyncio.gather().

Skipped automatically when no CUBRID instance is available.
"""

from __future__ import annotations

import asyncio
import os
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor

import pycubrid
import pycubrid.aio
import pytest


TEST_HOST = os.environ.get("CUBRID_TEST_HOST", "localhost")
TEST_PORT = int(os.environ.get("CUBRID_TEST_PORT", "33000"))
TEST_DB = os.environ.get("CUBRID_TEST_DB", "testdb")
TEST_USER = os.environ.get("CUBRID_TEST_USER", "dba")
TEST_PASSWORD = os.environ.get("CUBRID_TEST_PASSWORD", "")


def _can_connect() -> bool:
    try:
        conn = pycubrid.connect(
            host=TEST_HOST,
            port=TEST_PORT,
            database=TEST_DB,
            user=TEST_USER,
            password=TEST_PASSWORD,
        )
        conn.close()
        return True
    except Exception:
        return False


pytestmark = pytest.mark.skipif(not _can_connect(), reason="CUBRID instance not available")


def _table() -> str:
    return f"pycubrid_stress_{uuid.uuid4().hex[:8]}"


def _connect_sync():
    return pycubrid.connect(
        host=TEST_HOST,
        port=TEST_PORT,
        database=TEST_DB,
        user=TEST_USER,
        password=TEST_PASSWORD,
    )


async def _connect_async():
    return await pycubrid.aio.connect(
        host=TEST_HOST,
        port=TEST_PORT,
        database=TEST_DB,
        user=TEST_USER,
        password=TEST_PASSWORD,
    )


@pytest.fixture
def shared_table():
    table = _table()
    conn = _connect_sync()
    cur = conn.cursor()
    cur.execute(f"CREATE TABLE {table} (id INT AUTO_INCREMENT PRIMARY KEY, worker INT, n INT)")
    conn.commit()
    cur.close()
    conn.close()
    yield table
    cleanup = _connect_sync()
    cur = cleanup.cursor()
    cur.execute(f"DROP TABLE IF EXISTS {table}")
    cleanup.commit()
    cur.close()
    cleanup.close()


class TestThreadedConcurrency:
    def test_many_threads_each_with_own_connection(self, shared_table: str) -> None:
        n_threads = 16
        per_thread = 25
        errors: list[BaseException] = []
        lock = threading.Lock()

        def worker(idx: int) -> int:
            try:
                conn = _connect_sync()
                cur = conn.cursor()
                for i in range(per_thread):
                    cur.execute(
                        f"INSERT INTO {shared_table} (worker, n) VALUES (?, ?)",
                        (idx, i),
                    )
                conn.commit()
                cur.execute(f"SELECT COUNT(*) FROM {shared_table} WHERE worker = ?", (idx,))
                count = cur.fetchone()[0]
                cur.close()
                conn.close()
                return count
            except BaseException as exc:
                with lock:
                    errors.append(exc)
                return -1

        with ThreadPoolExecutor(max_workers=n_threads) as ex:
            results = list(ex.map(worker, range(n_threads)))

        assert not errors, f"thread errors: {errors!r}"
        assert all(r == per_thread for r in results), results

        verify = _connect_sync()
        vcur = verify.cursor()
        vcur.execute(f"SELECT COUNT(*) FROM {shared_table}")
        total = vcur.fetchone()[0]
        vcur.close()
        verify.close()
        assert total == n_threads * per_thread

    def test_concurrent_select_only_workload(self, shared_table: str) -> None:
        seed = _connect_sync()
        scur = seed.cursor()
        for i in range(100):
            scur.execute(f"INSERT INTO {shared_table} (worker, n) VALUES (?, ?)", (0, i))
        seed.commit()
        scur.close()
        seed.close()

        n_threads = 32
        rounds = 10
        errors: list[BaseException] = []
        lock = threading.Lock()

        def reader() -> int:
            try:
                conn = _connect_sync()
                cur = conn.cursor()
                last = 0
                for _ in range(rounds):
                    cur.execute(f"SELECT COUNT(*) FROM {shared_table}")
                    last = cur.fetchone()[0]
                cur.close()
                conn.close()
                return last
            except BaseException as exc:
                with lock:
                    errors.append(exc)
                return -1

        with ThreadPoolExecutor(max_workers=n_threads) as ex:
            results = list(ex.map(lambda _: reader(), range(n_threads)))

        assert not errors, f"reader errors: {errors!r}"
        assert all(r == 100 for r in results), results


class TestAsyncConcurrency:
    @pytest.mark.asyncio
    async def test_gather_many_independent_connections(self, shared_table: str) -> None:
        n = 16
        per_task = 20

        async def worker(idx: int) -> int:
            conn = await _connect_async()
            cur = conn.cursor()
            try:
                for i in range(per_task):
                    await cur.execute(
                        f"INSERT INTO {shared_table} (worker, n) VALUES (?, ?)",
                        (idx, i),
                    )
                await conn.commit()
                await cur.execute(f"SELECT COUNT(*) FROM {shared_table} WHERE worker = ?", (idx,))
                row = await cur.fetchone()
                return row[0]
            finally:
                await cur.close()
                await conn.close()

        results = await asyncio.gather(*(worker(i) for i in range(n)))
        assert all(r == per_task for r in results), results

        verify = await _connect_async()
        vcur = verify.cursor()
        await vcur.execute(f"SELECT COUNT(*) FROM {shared_table}")
        row = await vcur.fetchone()
        await vcur.close()
        await verify.close()
        assert row[0] == n * per_task

    @pytest.mark.asyncio
    async def test_gather_many_select_only(self, shared_table: str) -> None:
        seed = await _connect_async()
        scur = seed.cursor()
        for i in range(50):
            await scur.execute(f"INSERT INTO {shared_table} (worker, n) VALUES (?, ?)", (0, i))
        await seed.commit()
        await scur.close()
        await seed.close()

        n = 32

        async def reader() -> int:
            conn = await _connect_async()
            cur = conn.cursor()
            try:
                await cur.execute(f"SELECT COUNT(*) FROM {shared_table}")
                row = await cur.fetchone()
                return row[0]
            finally:
                await cur.close()
                await conn.close()

        results = await asyncio.gather(*(reader() for _ in range(n)))
        assert all(r == 50 for r in results), results
