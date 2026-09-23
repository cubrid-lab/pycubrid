"""SET / MULTISET / SEQUENCE live CRUD + decode parity (#403).

Tests collection types via SQL literal INSERT (not parameter binding).
pycubrid explicitly rejects collection parameter binding
(``_cursor_common.py`` raises ``ProgrammingError``); that is tracked
as a separate feature issue.
"""

from __future__ import annotations

import os
import uuid
from collections.abc import Generator
from typing import Any

import pycubrid
import pytest
from pycubrid.connection import Connection
from pycubrid.cursor import Cursor

TEST_HOST = os.environ.get("CUBRID_TEST_HOST", "localhost")
TEST_PORT = int(os.environ.get("CUBRID_TEST_PORT", "33000"))
TEST_DB = os.environ.get("CUBRID_TEST_DB", "testdb")
TEST_USER = os.environ.get("CUBRID_TEST_USER", "dba")
TEST_PASSWORD = os.environ.get("CUBRID_TEST_PASSWORD", "")


def _can_connect() -> bool:
    try:
        c = pycubrid.connect(
            host=TEST_HOST, port=TEST_PORT, database=TEST_DB,
            user=TEST_USER, password=TEST_PASSWORD,
        )
        c.close()
        return True
    except Exception:
        return False


pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(not _can_connect(), reason="CUBRID instance not available"),
]


def _connect(**kwargs: Any) -> Connection:
    return pycubrid.connect(
        host=TEST_HOST, port=TEST_PORT, database=TEST_DB,
        user=TEST_USER, password=TEST_PASSWORD, **kwargs,
    )


@pytest.fixture
def conn() -> Generator[Connection, None, None]:
    c = _connect(decode_collections=True)
    yield c
    c.close()


@pytest.fixture
def cursor(conn: Connection) -> Generator[Cursor, None, None]:
    cur = conn.cursor()
    yield cur
    cur.close()


def _tbl() -> str:
    return "pycubrid_coll_%s" % uuid.uuid4().hex[:8]


class TestCollectionCRUD:
    def test_set_of_int(self, cursor: Cursor) -> None:
        table = _tbl()
        try:
            cursor.execute("CREATE TABLE %s (a SET(INT))" % table)
            cursor.execute("INSERT INTO %s VALUES ({1,2,3})" % table)
            cursor.execute("SELECT * FROM %s" % table)
            row = cursor.fetchone()
            assert row is not None
            # SET: no duplicates, order may vary
            assert set(row[0]) == {1, 2, 3} or set(row[0]) == {"1", "2", "3"}
        finally:
            cursor.execute("DROP TABLE IF EXISTS %s" % table)

    def test_multiset_of_int(self, cursor: Cursor) -> None:
        table = _tbl()
        try:
            cursor.execute("CREATE TABLE %s (a MULTISET(INT))" % table)
            cursor.execute("INSERT INTO %s VALUES ({1,1,2})" % table)
            cursor.execute("SELECT * FROM %s" % table)
            row = cursor.fetchone()
            assert row is not None
            # MULTISET preserves duplicates
            result = sorted(row[0])
            assert len(result) == 3
        finally:
            cursor.execute("DROP TABLE IF EXISTS %s" % table)

    def test_sequence_of_int(self, cursor: Cursor) -> None:
        table = _tbl()
        try:
            cursor.execute("CREATE TABLE %s (a SEQUENCE(INT))" % table)
            cursor.execute("INSERT INTO %s VALUES ({1,2,3})" % table)
            cursor.execute("SELECT * FROM %s" % table)
            row = cursor.fetchone()
            assert row is not None
            # SEQUENCE (LIST) preserves order
            assert isinstance(row[0], list)
        finally:
            cursor.execute("DROP TABLE IF EXISTS %s" % table)

    def test_empty_collection(self, cursor: Cursor) -> None:
        table = _tbl()
        try:
            cursor.execute("CREATE TABLE %s (a SET(INT))" % table)
            cursor.execute("INSERT INTO %s VALUES ({})" % table)
            cursor.execute("SELECT * FROM %s" % table)
            row = cursor.fetchone()
            assert row is not None
            # Empty set
            assert len(row[0]) == 0
        finally:
            cursor.execute("DROP TABLE IF EXISTS %s" % table)

    def test_null_collection(self, cursor: Cursor) -> None:
        table = _tbl()
        try:
            cursor.execute("CREATE TABLE %s (a SET(INT))" % table)
            cursor.execute("INSERT INTO %s VALUES (NULL)" % table)
            cursor.execute("SELECT * FROM %s" % table)
            row = cursor.fetchone()
            assert row is not None
            assert row[0] is None
        finally:
            cursor.execute("DROP TABLE IF EXISTS %s" % table)

    def test_mixed_columns(self, cursor: Cursor) -> None:
        table = _tbl()
        try:
            cursor.execute(
                "CREATE TABLE %s (id INT, tags SET(VARCHAR(20)))" % table
            )
            cursor.execute(
                "INSERT INTO %s VALUES (1, {'alpha','beta','gamma'})" % table
            )
            cursor.execute("SELECT id, tags FROM %s" % table)
            row = cursor.fetchone()
            assert row is not None
            assert row[0] == 1
            assert set(row[1]) == {"alpha", "beta", "gamma"}
        finally:
            cursor.execute("DROP TABLE IF EXISTS %s" % table)

    def test_collection_predicate(self, cursor: Cursor) -> None:
        table = _tbl()
        try:
            cursor.execute(
                "CREATE TABLE %s (a SET(INT), b MULTISET(INT), c SEQUENCE(INT))"
                % table
            )
            cursor.execute(
                "INSERT INTO %s VALUES "
                "({},{},{}),(NULL,NULL,NULL),({1,1},{1,1},{1,1}),"
                "({1,2,3},{1,2,3},{1,2,3})" % table
            )
            cursor.execute(
                "SELECT * FROM %s WHERE a SETEQ {'1'} ORDER BY 1" % table
            )
            rows = cursor.fetchall()
            assert len(rows) == 1
        finally:
            cursor.execute("DROP TABLE IF EXISTS %s" % table)


class TestCollectionDecodeFlag:
    def test_decode_collections_false(self) -> None:
        with _connect(decode_collections=False) as conn:
            cur = conn.cursor()
            table = _tbl()
            try:
                cur.execute("CREATE TABLE %s (a SET(INT))" % table)
                cur.execute("INSERT INTO %s VALUES ({1,2,3})" % table)
                cur.execute("SELECT * FROM %s" % table)
                row = cur.fetchone()
                assert row is not None
                # With decode_collections=False, raw bytes are returned
                assert isinstance(row[0], (bytes, bytearray))
            finally:
                cur.execute("DROP TABLE IF EXISTS %s" % table)
                cur.close()
