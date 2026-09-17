"""executemany / batch failure-semantics property tests (issue #346).

Batch execution previously had a silent-error defect (#186), so it warrants
adversarial testing. This suite generates batches of varying size with a failing
row at generated positions (first / middle / last / multiple) against a live
CUBRID server, and asserts the durable batch contract for both sync and async:

* an all-success batch reports ``rowcount == len(batch)`` and every row lands;
* a batch containing a failing row (duplicate PK) is NOT silently ignored — it
  raises a PEP 249 error (never a raw exception);
* the caller can always tell a batch failed;
* the connection stays usable after a batch failure (a subsequent statement
  succeeds);
* sync and async behave identically.

Skipped when no CUBRID server is reachable. Row visibility is checked from the
same autocommit connection so table state is authoritative.
"""

from __future__ import annotations

import asyncio
import sys
import uuid

import pytest
from hypothesis import given, settings, strategies as st

import pycubrid
import pycubrid.aio
from pycubrid.exceptions import Error as DBAPIError

from ._parity_helpers import TEST_DB, TEST_HOST, TEST_PASSWORD, TEST_PORT, TEST_USER, can_connect

pytestmark = pytest.mark.skipif(not can_connect(), reason="CUBRID instance not available")


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
    return "bt_%s" % uuid.uuid4().hex[:8]


@pytest.fixture(scope="module")
def conn() -> pycubrid.Connection:
    c = _connect()
    c.autocommit = True
    yield c
    c.close()


def _make_table(c: pycubrid.Connection) -> str:
    table = _tbl()
    cur = c.cursor()
    cur.execute("CREATE TABLE %s (id INT PRIMARY KEY)" % table)
    cur.close()
    return table


def _drop_table(c: pycubrid.Connection, table: str) -> None:
    cur = c.cursor()
    try:
        cur.execute("DROP TABLE IF EXISTS %s" % table)
    except DBAPIError:
        pass  # best-effort teardown
    cur.close()


def _count(c: pycubrid.Connection, table: str) -> int:
    cur = c.cursor()
    cur.execute("SELECT COUNT(*) FROM %s" % table)
    row = cur.fetchone()
    cur.close()
    assert row is not None
    return int(row[0])


class TestBatchSuccess:
    @given(size=st.integers(min_value=1, max_value=60))
    @settings(deadline=None, max_examples=15)
    def test_all_success_rowcount_and_landing(self, conn: pycubrid.Connection, size: int) -> None:
        table = _make_table(conn)
        try:
            cur = conn.cursor()
            cur.executemany(
                "INSERT INTO %s (id) VALUES (?)" % table,
                [(i,) for i in range(size)],
            )
            assert cur.rowcount == size
            cur.close()
            assert _count(conn, table) == size
        finally:
            _drop_table(conn, table)

    def test_empty_batch_is_noop(self, conn: pycubrid.Connection) -> None:
        table = _make_table(conn)
        try:
            cur = conn.cursor()
            cur.executemany("INSERT INTO %s (id) VALUES (?)" % table, [])
            cur.close()
            assert _count(conn, table) == 0
        finally:
            _drop_table(conn, table)


class TestBatchFailure:
    @given(
        size=st.integers(min_value=1, max_value=20),
        fail_pos=st.integers(min_value=0, max_value=19),
    )
    @settings(deadline=None, max_examples=25)
    def test_failing_row_is_never_silent(
        self, conn: pycubrid.Connection, size: int, fail_pos: int
    ) -> None:
        fail_pos = fail_pos % size
        table = _make_table(conn)
        try:
            cur = conn.cursor()
            # Pre-seed the PK that the batch's fail_pos row will collide with.
            cur.execute("INSERT INTO %s (id) VALUES (?)" % table, (fail_pos,))
            batch = [(i,) for i in range(size)]  # row at fail_pos duplicates the seed
            with pytest.raises(DBAPIError):
                cur.executemany("INSERT INTO %s (id) VALUES (?)" % table, batch)
            cur.close()
        finally:
            _drop_table(conn, table)

    def test_connection_usable_after_batch_failure(self, conn: pycubrid.Connection) -> None:
        table = _make_table(conn)
        try:
            cur = conn.cursor()
            cur.execute("INSERT INTO %s (id) VALUES (100)" % table)
            with pytest.raises(DBAPIError):
                cur.executemany("INSERT INTO %s (id) VALUES (?)" % table, [(100,)])
            # Connection must remain usable for the next statement.
            cur.execute("INSERT INTO %s (id) VALUES (101)" % table)
            cur.close()
            assert _count(conn, table) == 2
        finally:
            _drop_table(conn, table)


async def _async_all_success(size: int) -> tuple[int, int]:
    c = await _aconnect()
    await c.set_autocommit(True)
    table = _tbl()
    try:
        cur = c.cursor()
        await cur.execute("CREATE TABLE %s (id INT PRIMARY KEY)" % table)
        await cur.executemany("INSERT INTO %s (id) VALUES (?)" % table, [(i,) for i in range(size)])
        rowcount = cur.rowcount
        await cur.execute("SELECT COUNT(*) FROM %s" % table)
        row = await cur.fetchone()
        assert row is not None
        await cur.close()
        return rowcount, int(row[0])
    finally:
        cur2 = c.cursor()
        try:
            await cur2.execute("DROP TABLE IF EXISTS %s" % table)
        except DBAPIError:
            pass  # best-effort teardown
        await cur2.close()
        await c.close()


async def _async_failing_batch_raises(size: int, fail_pos: int) -> bool:
    c = await _aconnect()
    await c.set_autocommit(True)
    table = _tbl()
    try:
        cur = c.cursor()
        await cur.execute("CREATE TABLE %s (id INT PRIMARY KEY)" % table)
        await cur.execute("INSERT INTO %s (id) VALUES (?)" % table, (fail_pos,))
        try:
            await cur.executemany(
                "INSERT INTO %s (id) VALUES (?)" % table, [(i,) for i in range(size)]
            )
            return False
        except DBAPIError:
            return True
        finally:
            await cur.close()
    finally:
        cur2 = c.cursor()
        try:
            await cur2.execute("DROP TABLE IF EXISTS %s" % table)
        except DBAPIError:
            pass  # best-effort teardown
        await cur2.close()
        await c.close()


class TestBatchAsyncParity:
    @given(size=st.integers(min_value=1, max_value=40))
    @settings(deadline=None, max_examples=10)
    def test_async_all_success(self, size: int) -> None:
        rowcount, landed = asyncio.run(_async_all_success(size))
        assert rowcount == size
        assert landed == size

    @given(
        size=st.integers(min_value=1, max_value=15),
        fail_pos=st.integers(min_value=0, max_value=14),
    )
    @settings(deadline=None, max_examples=15)
    def test_async_failing_batch_raises(self, size: int, fail_pos: int) -> None:
        assert asyncio.run(_async_failing_batch_raises(size, fail_pos % size)) is True


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
