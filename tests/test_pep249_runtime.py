"""Runtime DB-API 2.0 behavioral contract suite (issue #353).

The existing PEP 249 tests validate module constants, constructors, and the
exception hierarchy well; this suite pins the *runtime* cursor/connection
semantics as a single explicit contract matrix, verified against a live server
for both the sync and async drivers:

* ``cursor.description`` is ``None`` before execute, a column tuple after a
  SELECT, and ``None`` again after a non-result statement (DDL/DML);
* ``rowcount`` is ``-1`` before execute and after SELECT, and the affected-row
  count after DML;
* ``arraysize`` defaults to 1; ``fetchmany`` honours it and an explicit size;
* ``fetchone`` after exhaustion returns ``None``; ``fetchall`` returns ``[]``;
* fetching before execute / on a closed cursor raises ``InterfaceError``;
* operating on a closed connection raises ``InterfaceError``;
* ``nextset`` raises ``NotSupportedError``;
* the PEP 249 optional extension exposes every exception class as an attribute
  on the connection, identical to the module-level class.

Any intentional deviation is documented inline. Skipped when no CUBRID server is
reachable.
"""

from __future__ import annotations

import asyncio
import sys
import uuid

import pytest

import pycubrid
import pycubrid.aio
from pycubrid.exceptions import InterfaceError, NotSupportedError

from ._parity_helpers import TEST_DB, TEST_HOST, TEST_PASSWORD, TEST_PORT, TEST_USER, can_connect

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(not can_connect(), reason="CUBRID instance not available"),
]

_EXC_NAMES = (
    "Warning",
    "Error",
    "InterfaceError",
    "DatabaseError",
    "DataError",
    "OperationalError",
    "IntegrityError",
    "InternalError",
    "ProgrammingError",
    "NotSupportedError",
)


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
    return "pep_%s" % uuid.uuid4().hex[:8]


@pytest.fixture()
def conn() -> pycubrid.Connection:
    c = _connect()
    c.autocommit = True
    yield c
    c.close()


class TestSyncCursorContract:
    def test_description_lifecycle(self, conn: pycubrid.Connection) -> None:
        cur = conn.cursor()
        assert cur.description is None
        cur.execute("SELECT 1 AS a, 'x' AS b")
        assert cur.description is not None
        assert [d[0] for d in cur.description] == ["a", "b"]
        # A non-result statement clears description back to None.
        table = _tbl()
        cur.execute("CREATE TABLE %s (id INT)" % table)
        assert cur.description is None
        cur.execute("DROP TABLE %s" % table)
        cur.close()

    def test_rowcount_lifecycle(self, conn: pycubrid.Connection) -> None:
        cur = conn.cursor()
        assert cur.rowcount == -1
        cur.execute("SELECT 1")
        assert cur.rowcount == -1  # SELECT rowcount is unknown -> -1
        table = _tbl()
        cur.execute("CREATE TABLE %s (id INT)" % table)
        cur.execute("INSERT INTO %s VALUES (1), (2), (3)" % table)
        assert cur.rowcount == 3
        cur.execute("DROP TABLE %s" % table)
        cur.close()

    def test_arraysize_and_fetchmany(self, conn: pycubrid.Connection) -> None:
        table = _tbl()
        cur = conn.cursor()
        cur.execute("CREATE TABLE %s (id INT)" % table)
        cur.executemany("INSERT INTO %s VALUES (?)" % table, [(i,) for i in range(10)])
        cur.execute("SELECT id FROM %s ORDER BY id" % table)
        assert cur.arraysize == 1
        assert len(cur.fetchmany()) == 1  # honours arraysize
        assert len(cur.fetchmany(3)) == 3  # honours explicit size
        cur.arraysize = 5
        assert len(cur.fetchmany()) == 5
        cur.execute("DROP TABLE %s" % table)
        cur.close()

    def test_fetch_after_exhaustion(self, conn: pycubrid.Connection) -> None:
        cur = conn.cursor()
        cur.execute("SELECT 1")
        assert cur.fetchone() == (1,)
        assert cur.fetchone() is None
        assert cur.fetchall() == []
        cur.close()

    def test_fetch_before_execute_raises(self, conn: pycubrid.Connection) -> None:
        cur = conn.cursor()
        with pytest.raises(InterfaceError):
            cur.fetchone()
        cur.close()

    def test_closed_cursor_raises(self, conn: pycubrid.Connection) -> None:
        cur = conn.cursor()
        cur.close()
        with pytest.raises(InterfaceError):
            cur.execute("SELECT 1")
        with pytest.raises(InterfaceError):
            cur.fetchone()

    def test_nextset_not_supported(self, conn: pycubrid.Connection) -> None:
        cur = conn.cursor()
        cur.execute("SELECT 1")
        with pytest.raises(NotSupportedError):
            cur.nextset()
        cur.close()


class TestSyncConnectionContract:
    def test_closed_connection_raises(self) -> None:
        c = _connect()
        c.close()
        with pytest.raises(InterfaceError):
            c.cursor()

    def test_exception_classes_exposed_on_connection(self) -> None:
        with _connect() as c:
            for name in _EXC_NAMES:
                assert getattr(c, name) is getattr(pycubrid, name), name


class TestAsyncCursorContract:
    def test_async_description_and_rowcount(self) -> None:
        async def run() -> None:
            c = await _aconnect()
            await c.set_autocommit(True)
            table = _tbl()
            try:
                cur = c.cursor()
                assert cur.description is None
                assert cur.rowcount == -1
                await cur.execute("SELECT 1 AS a")
                assert cur.description is not None
                assert cur.rowcount == -1
                await cur.execute("CREATE TABLE %s (id INT)" % table)
                assert cur.description is None
                await cur.execute("INSERT INTO %s VALUES (1), (2)" % table)
                assert cur.rowcount == 2
                await cur.close()
            finally:
                cur2 = c.cursor()
                await cur2.execute("DROP TABLE IF EXISTS %s" % table)
                await cur2.close()
                await c.close()

        asyncio.run(run())

    def test_async_fetch_after_exhaustion(self) -> None:
        async def run() -> None:
            c = await _aconnect()
            try:
                cur = c.cursor()
                await cur.execute("SELECT 1")
                assert await cur.fetchone() == (1,)
                assert await cur.fetchone() is None
                assert await cur.fetchall() == []
                await cur.close()
            finally:
                await c.close()

        asyncio.run(run())

    def test_async_closed_cursor_raises(self) -> None:
        async def run() -> None:
            c = await _aconnect()
            try:
                cur = c.cursor()
                await cur.close()
                with pytest.raises(InterfaceError):
                    await cur.execute("SELECT 1")
            finally:
                await c.close()

        asyncio.run(run())


class TestSyncAsyncContractParity:
    def test_rowcount_matches(self) -> None:
        table = _tbl()

        def sync_rowcount() -> int:
            with _connect() as c:
                c.autocommit = True
                cur = c.cursor()
                cur.execute("CREATE TABLE %s (id INT)" % table)
                cur.execute("INSERT INTO %s VALUES (1), (2), (3)" % table)
                rc = cur.rowcount
                cur.execute("DROP TABLE %s" % table)
                cur.close()
                return rc

        async def async_rowcount() -> int:
            atable = _tbl()
            c = await _aconnect()
            await c.set_autocommit(True)
            try:
                cur = c.cursor()
                await cur.execute("CREATE TABLE %s (id INT)" % atable)
                await cur.execute("INSERT INTO %s VALUES (1), (2), (3)" % atable)
                rc = cur.rowcount
                await cur.execute("DROP TABLE %s" % atable)
                await cur.close()
                return rc
            finally:
                await c.close()

        assert sync_rowcount() == asyncio.run(async_rowcount())


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
