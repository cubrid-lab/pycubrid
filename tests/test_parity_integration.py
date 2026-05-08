"""Sync/async parity integration tests for pycubrid.

Runs identical scenarios through both the sync and async APIs to verify
they produce identical results. Requires a running CUBRID instance.

Set CUBRID_TEST_HOST / CUBRID_TEST_PORT env vars or use defaults.
"""

from __future__ import annotations

import asyncio
import os
import uuid

import pytest

import pycubrid
import pycubrid.aio

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


pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(not _can_connect(), reason="CUBRID instance not available"),
]


def _table_name() -> str:
    return f"cookbook_parity_{uuid.uuid4().hex[:8]}"


# ---------------------------------------------------------------------------
# Sync helpers
# ---------------------------------------------------------------------------


def _sync_scenario(table: str, rows: list[tuple]) -> list:
    """Insert rows and select them back via sync API."""
    conn = pycubrid.connect(
        host=TEST_HOST,
        port=TEST_PORT,
        database=TEST_DB,
        user=TEST_USER,
        password=TEST_PASSWORD,
    )
    conn.autocommit = False
    try:
        cur = conn.cursor()
        cur.execute(f"DROP TABLE IF EXISTS {table}")
        cur.execute(f"CREATE TABLE {table} (id INT, name VARCHAR(200), val DOUBLE)")
        cur.executemany(f"INSERT INTO {table} (id, name, val) VALUES (?, ?, ?)", rows)
        conn.commit()
        cur.execute(f"SELECT id, name, val FROM {table} ORDER BY id")
        result = cur.fetchall()
        cur.execute(f"DROP TABLE IF EXISTS {table}")
        conn.commit()
        return result
    finally:
        conn.close()


async def _async_scenario(table: str, rows: list[tuple]) -> list:
    """Insert rows and select them back via async API."""
    conn = await pycubrid.aio.connect(
        host=TEST_HOST,
        port=TEST_PORT,
        database=TEST_DB,
        user=TEST_USER,
        password=TEST_PASSWORD,
    )
    await conn.set_autocommit(False)
    try:
        cur = conn.cursor()
        await cur.execute(f"DROP TABLE IF EXISTS {table}")
        await cur.execute(f"CREATE TABLE {table} (id INT, name VARCHAR(200), val DOUBLE)")
        await cur.executemany(f"INSERT INTO {table} (id, name, val) VALUES (?, ?, ?)", rows)
        await conn.commit()
        await cur.execute(f"SELECT id, name, val FROM {table} ORDER BY id")
        result = await cur.fetchall()
        await cur.execute(f"DROP TABLE IF EXISTS {table}")
        await conn.commit()
        return result
    finally:
        await conn.close()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestParityBasicTypes:
    """Verify sync and async return identical results for basic types."""

    def test_integers_and_strings(self):
        table = _table_name()
        rows = [(1, "hello", 1.5), (2, "world", 2.5), (3, None, None)]
        sync_result = _sync_scenario(table + "_s", rows)
        async_result = asyncio.run(_async_scenario(table + "_a", rows))
        assert sync_result == async_result

    def test_null_handling(self):
        table = _table_name()
        rows = [(1, None, None), (2, "", 0.0)]
        sync_result = _sync_scenario(table + "_s", rows)
        async_result = asyncio.run(_async_scenario(table + "_a", rows))
        assert sync_result == async_result

    def test_large_string(self):
        table = _table_name()
        big = "x" * 1000
        rows = [(1, big, 3.14)]
        sync_result = _sync_scenario(table + "_s", rows)
        async_result = asyncio.run(_async_scenario(table + "_a", rows))
        assert sync_result == async_result


class TestParityExecutemany:
    """Verify executemany produces same results via sync/async."""

    def test_batch_insert(self):
        table = _table_name()
        rows = [(i, f"row_{i}", float(i) * 1.1) for i in range(50)]
        sync_result = _sync_scenario(table + "_s", rows)
        async_result = asyncio.run(_async_scenario(table + "_a", rows))
        assert sync_result == async_result


class TestParityTransactions:
    """Verify transaction rollback semantics are identical."""

    def test_rollback_discards_inserts(self):
        table = _table_name()

        # Sync: insert then rollback
        conn = pycubrid.connect(
            host=TEST_HOST,
            port=TEST_PORT,
            database=TEST_DB,
            user=TEST_USER,
            password=TEST_PASSWORD,
        )
        conn.autocommit = False
        cur = conn.cursor()
        cur.execute(f"DROP TABLE IF EXISTS {table}")
        cur.execute(f"CREATE TABLE {table} (id INT)")
        conn.commit()
        cur.execute(f"INSERT INTO {table} (id) VALUES (?)", (999,))
        conn.rollback()
        cur.execute(f"SELECT id FROM {table}")
        sync_result = cur.fetchall()
        cur.execute(f"DROP TABLE {table}")
        conn.commit()
        conn.close()

        # Async: same scenario
        async def _async_rollback():
            c = await pycubrid.aio.connect(
                host=TEST_HOST,
                port=TEST_PORT,
                database=TEST_DB,
                user=TEST_USER,
                password=TEST_PASSWORD,
            )
            await c.set_autocommit(False)
            cr = c.cursor()
            await cr.execute(f"DROP TABLE IF EXISTS {table}")
            await cr.execute(f"CREATE TABLE {table} (id INT)")
            await c.commit()
            await cr.execute(f"INSERT INTO {table} (id) VALUES (?)", (999,))
            await c.rollback()
            await cr.execute(f"SELECT id FROM {table}")
            result = await cr.fetchall()
            await cr.execute(f"DROP TABLE {table}")
            await c.commit()
            await c.close()
            return result

        async_result = asyncio.run(_async_rollback())
        assert sync_result == async_result == []


class TestParityFetchMethods:
    """Verify fetchone/fetchmany/fetchall parity."""

    def test_fetchmany_parity(self):
        table = _table_name()
        rows = [(i, f"item_{i}", float(i)) for i in range(10)]

        # Sync
        conn = pycubrid.connect(
            host=TEST_HOST,
            port=TEST_PORT,
            database=TEST_DB,
            user=TEST_USER,
            password=TEST_PASSWORD,
        )
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute(f"DROP TABLE IF EXISTS {table}")
        cur.execute(f"CREATE TABLE {table} (id INT, name VARCHAR(100), val DOUBLE)")
        cur.executemany(f"INSERT INTO {table} (id, name, val) VALUES (?, ?, ?)", rows)
        cur.execute(f"SELECT id, name, val FROM {table} ORDER BY id")
        sync_batch1 = cur.fetchmany(3)
        sync_batch2 = cur.fetchmany(3)
        sync_rest = cur.fetchall()
        cur.execute(f"DROP TABLE {table}")
        conn.close()

        # Async
        async def _async_fetchmany():
            c = await pycubrid.aio.connect(
                host=TEST_HOST,
                port=TEST_PORT,
                database=TEST_DB,
                user=TEST_USER,
                password=TEST_PASSWORD,
            )
            await c.set_autocommit(True)
            cr = c.cursor()
            await cr.execute(f"DROP TABLE IF EXISTS {table}")
            await cr.execute(f"CREATE TABLE {table} (id INT, name VARCHAR(100), val DOUBLE)")
            await cr.executemany(f"INSERT INTO {table} (id, name, val) VALUES (?, ?, ?)", rows)
            await cr.execute(f"SELECT id, name, val FROM {table} ORDER BY id")
            b1 = await cr.fetchmany(3)
            b2 = await cr.fetchmany(3)
            rest = await cr.fetchall()
            await cr.execute(f"DROP TABLE {table}")
            await c.close()
            return b1, b2, rest

        ab1, ab2, arest = asyncio.run(_async_fetchmany())
        assert sync_batch1 == ab1
        assert sync_batch2 == ab2
        assert sync_rest == arest
