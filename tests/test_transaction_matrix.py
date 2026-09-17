"""Transaction and autocommit contract matrix (issue #343).

Exercises the interaction between autocommit mode, commit/rollback, failed
statements, and a second observer connection against a live CUBRID server, for
both the sync and async drivers. The matrix asserts the durable contract:

* a fresh connection starts in a clean, non-committing state
  (``autocommit=False`` by default; uncommitted writes are invisible to a
  separate connection);
* ``commit()`` makes writes durable/visible; ``rollback()`` discards them;
* a failed statement does not implicitly commit prior uncommitted work;
* toggling ``autocommit`` flushes the current transaction (writes before the
  toggle become visible) and subsequent statements self-commit;
* sync and async behave identically for every cell.

Skipped when no CUBRID server is reachable (same gate as the integration
tests). Visibility is checked from an independent connection so we test real
transaction isolation, not just local cursor state.
"""

from __future__ import annotations

import asyncio
import sys
import uuid

import pytest

import pycubrid
import pycubrid.aio
from pycubrid.exceptions import Error as DBAPIError

from ._parity_helpers import TEST_DB, TEST_HOST, TEST_PASSWORD, TEST_PORT, TEST_USER, can_connect

pytestmark = pytest.mark.skipif(not can_connect(), reason="CUBRID instance not available")


def _tbl() -> str:
    return "tx_%s" % uuid.uuid4().hex[:8]


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


def _rows_visible_from_new_conn(table: str) -> int:
    """Row count as seen by a separate, autocommit connection."""
    observer = _connect()
    observer.autocommit = True
    try:
        cur = observer.cursor()
        cur.execute("SELECT COUNT(*) FROM %s" % table)
        row = cur.fetchone()
        cur.close()
        assert row is not None
        return int(row[0])
    finally:
        observer.close()


@pytest.fixture()
def table() -> str:
    name = _tbl()
    setup = _connect()
    setup.autocommit = True
    cur = setup.cursor()
    cur.execute("CREATE TABLE %s (id INT PRIMARY KEY)" % name)
    cur.close()
    yield name
    cur = setup.cursor()
    try:
        cur.execute("DROP TABLE IF EXISTS %s" % name)
    except DBAPIError:
        pass  # best-effort teardown; ignore if the server already dropped it
    cur.close()
    setup.close()


class TestSyncTransactionContract:
    def test_default_is_not_autocommit(self, table: str) -> None:
        conn = _connect()
        try:
            assert conn.autocommit is False
            cur = conn.cursor()
            cur.execute("INSERT INTO %s (id) VALUES (1)" % table)
            cur.close()
            # Uncommitted: invisible to a separate connection.
            assert _rows_visible_from_new_conn(table) == 0
        finally:
            conn.close()

    def test_commit_makes_visible(self, table: str) -> None:
        conn = _connect()
        try:
            cur = conn.cursor()
            cur.execute("INSERT INTO %s (id) VALUES (1)" % table)
            conn.commit()
            cur.close()
            assert _rows_visible_from_new_conn(table) == 1
        finally:
            conn.close()

    def test_rollback_discards(self, table: str) -> None:
        conn = _connect()
        try:
            cur = conn.cursor()
            cur.execute("INSERT INTO %s (id) VALUES (1)" % table)
            conn.rollback()
            cur.close()
            assert _rows_visible_from_new_conn(table) == 0
        finally:
            conn.close()

    def test_failed_statement_does_not_commit_prior_work(self, table: str) -> None:
        conn = _connect()
        try:
            cur = conn.cursor()
            cur.execute("INSERT INTO %s (id) VALUES (1)" % table)
            # Duplicate PK -> IntegrityError; must not commit the prior INSERT.
            with pytest.raises(DBAPIError):
                cur.execute("INSERT INTO %s (id) VALUES (1)" % table)
            conn.rollback()
            cur.close()
            assert _rows_visible_from_new_conn(table) == 0
        finally:
            conn.close()

    def test_autocommit_toggle_flushes_and_self_commits(self, table: str) -> None:
        conn = _connect()
        try:
            cur = conn.cursor()
            cur.execute("INSERT INTO %s (id) VALUES (1)" % table)
            # Turning autocommit ON flushes the open transaction (row 1 commits).
            conn.autocommit = True
            assert _rows_visible_from_new_conn(table) == 1
            # Subsequent statements now self-commit.
            cur.execute("INSERT INTO %s (id) VALUES (2)" % table)
            assert _rows_visible_from_new_conn(table) == 2
            cur.close()
        finally:
            conn.close()


def _run_async_scenario(scenario: str, table: str) -> int:
    """Run one async scenario, returning rows visible from a fresh connection."""

    async def _go() -> None:
        conn = await _aconnect()
        try:
            cur = conn.cursor()
            if scenario == "commit":
                await cur.execute("INSERT INTO %s (id) VALUES (1)" % table)
                await conn.commit()
            elif scenario == "rollback":
                await cur.execute("INSERT INTO %s (id) VALUES (1)" % table)
                await conn.rollback()
            elif scenario == "uncommitted":
                await cur.execute("INSERT INTO %s (id) VALUES (1)" % table)
            elif scenario == "toggle":
                await cur.execute("INSERT INTO %s (id) VALUES (1)" % table)
                await conn.set_autocommit(True)
                await cur.execute("INSERT INTO %s (id) VALUES (2)" % table)
            await cur.close()
        finally:
            await conn.close()

    asyncio.run(_go())
    return _rows_visible_from_new_conn(table)


class TestAsyncTransactionContract:
    def test_async_default_uncommitted_invisible(self, table: str) -> None:
        assert _run_async_scenario("uncommitted", table) == 0

    def test_async_commit_visible(self, table: str) -> None:
        assert _run_async_scenario("commit", table) == 1

    def test_async_rollback_discards(self, table: str) -> None:
        assert _run_async_scenario("rollback", table) == 0

    def test_async_toggle_flushes_and_self_commits(self, table: str) -> None:
        assert _run_async_scenario("toggle", table) == 2


class TestSyncAsyncTransactionParity:
    @pytest.mark.parametrize("scenario", ["uncommitted", "commit", "rollback", "toggle"])
    def test_sync_and_async_agree(self, scenario: str) -> None:
        sync_table = _tbl()
        async_table = _tbl()
        setup = _connect()
        setup.autocommit = True
        cur = setup.cursor()
        cur.execute("CREATE TABLE %s (id INT PRIMARY KEY)" % sync_table)
        cur.execute("CREATE TABLE %s (id INT PRIMARY KEY)" % async_table)
        cur.close()
        try:
            sync_val = self._run_sync_scenario(scenario, sync_table)
            async_val = _run_async_scenario(scenario, async_table)
            assert sync_val == async_val
        finally:
            cur = setup.cursor()
            for t in (sync_table, async_table):
                try:
                    cur.execute("DROP TABLE IF EXISTS %s" % t)
                except DBAPIError:
                    pass  # best-effort teardown
            cur.close()
            setup.close()

    @staticmethod
    def _run_sync_scenario(scenario: str, table: str) -> int:
        conn = _connect()
        try:
            cur = conn.cursor()
            if scenario == "commit":
                cur.execute("INSERT INTO %s (id) VALUES (1)" % table)
                conn.commit()
            elif scenario == "rollback":
                cur.execute("INSERT INTO %s (id) VALUES (1)" % table)
                conn.rollback()
            elif scenario == "uncommitted":
                cur.execute("INSERT INTO %s (id) VALUES (1)" % table)
            elif scenario == "toggle":
                cur.execute("INSERT INTO %s (id) VALUES (1)" % table)
                conn.autocommit = True
                cur.execute("INSERT INTO %s (id) VALUES (2)" % table)
            cur.close()
        finally:
            conn.close()
        return _rows_visible_from_new_conn(table)


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
