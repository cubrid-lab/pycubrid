"""BLOB + CLOB live round-trip regression (#405).

Tests LOB data round-trip correctness via pycubrid's existing API
(``create_lob``, ``write``, ``read``).  File import/export and seek
are CUBRIDdb extension APIs not present in pycubrid — see #405 for
the divergence documentation.
"""

from __future__ import annotations

import os
import uuid
from collections.abc import Generator

import pycubrid
import pytest
from pycubrid.connection import Connection
from pycubrid.constants import CUBRIDDataType
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


@pytest.fixture
def conn() -> Generator[Connection, None, None]:
    c = pycubrid.connect(
        host=TEST_HOST, port=TEST_PORT, database=TEST_DB,
        user=TEST_USER, password=TEST_PASSWORD,
    )
    yield c
    c.close()


@pytest.fixture
def cursor(conn: Connection) -> Generator[Cursor, None, None]:
    cur = conn.cursor()
    yield cur
    cur.close()


def _tbl() -> str:
    return "pycubrid_lob_%s" % uuid.uuid4().hex[:8]


class TestBlobRoundTrip:
    def test_small_blob(self, conn: Connection, cursor: Cursor) -> None:
        table = _tbl()
        try:
            cursor.execute("CREATE TABLE %s (data BLOB)" % table)
            data = b"\x00\x01\x02\xff" * 100  # 400 bytes
            lob = conn.create_lob(CUBRIDDataType.BLOB)
            lob.write(data)
            cursor.execute(
                "INSERT INTO %s (data) VALUES (?)" % table, (lob,)
            )
            lob.close()

            cursor.execute("SELECT data FROM %s" % table)
            row = cursor.fetchone()
            assert row is not None
            result_lob = row[0]
            result = result_lob.read(len(data))
            result_lob.close()
            assert result == data
        finally:
            cursor.execute("DROP TABLE IF EXISTS %s" % table)

    def test_large_blob_exceeds_old_cap(self, conn: Connection, cursor: Cursor) -> None:
        """Verify no truncation at the old 81908-byte cap (issue #362)."""
        table = _tbl()
        try:
            cursor.execute("CREATE TABLE %s (data BLOB)" % table)
            data = os.urandom(100_000)  # >81908 bytes
            lob = conn.create_lob(CUBRIDDataType.BLOB)
            lob.write(data)
            cursor.execute(
                "INSERT INTO %s (data) VALUES (?)" % table, (lob,)
            )
            lob.close()

            cursor.execute("SELECT data FROM %s" % table)
            row = cursor.fetchone()
            assert row is not None
            result_lob = row[0]
            result = result_lob.read(len(data))
            result_lob.close()
            assert result == data
        finally:
            cursor.execute("DROP TABLE IF EXISTS %s" % table)


class TestClobRoundTrip:
    def test_clob_string(self, conn: Connection, cursor: Cursor) -> None:
        table = _tbl()
        try:
            cursor.execute("CREATE TABLE %s (content CLOB)" % table)
            text = "hello world — pycubrid CLOB test"
            data = text.encode("utf-8")
            lob = conn.create_lob(CUBRIDDataType.CLOB)
            lob.write(data)
            cursor.execute(
                "INSERT INTO %s (content) VALUES (?)" % table, (lob,)
            )
            lob.close()

            cursor.execute("SELECT content FROM %s" % table)
            row = cursor.fetchone()
            assert row is not None
            result_lob = row[0]
            result = result_lob.read(len(data))
            result_lob.close()
            assert result.decode("utf-8") == text
        finally:
            cursor.execute("DROP TABLE IF EXISTS %s" % table)

    def test_clob_unicode_cjk(self, conn: Connection, cursor: Cursor) -> None:
        table = _tbl()
        try:
            cursor.execute("CREATE TABLE %s (content CLOB)" % table)
            text = "한국어 테스트 日本語テスト 中文测试"
            data = text.encode("utf-8")
            lob = conn.create_lob(CUBRIDDataType.CLOB)
            lob.write(data)
            cursor.execute(
                "INSERT INTO %s (content) VALUES (?)" % table, (lob,)
            )
            lob.close()

            cursor.execute("SELECT content FROM %s" % table)
            row = cursor.fetchone()
            assert row is not None
            result_lob = row[0]
            result = result_lob.read(len(data))
            result_lob.close()
            assert result.decode("utf-8") == text
        finally:
            cursor.execute("DROP TABLE IF EXISTS %s" % table)

    def test_clob_large_text(self, conn: Connection, cursor: Cursor) -> None:
        """Multi-chunk CLOB read (>100KB)."""
        table = _tbl()
        try:
            cursor.execute("CREATE TABLE %s (content CLOB)" % table)
            text = "A" * 120_000
            data = text.encode("utf-8")
            lob = conn.create_lob(CUBRIDDataType.CLOB)
            lob.write(data)
            cursor.execute(
                "INSERT INTO %s (content) VALUES (?)" % table, (lob,)
            )
            lob.close()

            cursor.execute("SELECT content FROM %s" % table)
            row = cursor.fetchone()
            assert row is not None
            result_lob = row[0]
            result = result_lob.read(len(data))
            result_lob.close()
            assert len(result) == len(data)
            assert result == data
        finally:
            cursor.execute("DROP TABLE IF EXISTS %s" % table)


class TestLobMixedTable:
    def test_blob_with_scalar_columns(self, conn: Connection, cursor: Cursor) -> None:
        table = _tbl()
        try:
            cursor.execute(
                "CREATE TABLE %s (id INT, name VARCHAR(100), data BLOB)" % table
            )
            blob_data = b"\xde\xad\xbe\xef" * 50
            lob = conn.create_lob(CUBRIDDataType.BLOB)
            lob.write(blob_data)
            cursor.execute(
                "INSERT INTO %s (id, name, data) VALUES (1, 'test', ?)" % table,
                (lob,),
            )
            lob.close()

            cursor.execute("SELECT id, name, data FROM %s" % table)
            row = cursor.fetchone()
            assert row is not None
            assert row[0] == 1
            assert row[1] == "test"
            result_lob = row[2]
            result = result_lob.read(len(blob_data))
            result_lob.close()
            assert result == blob_data
        finally:
            cursor.execute("DROP TABLE IF EXISTS %s" % table)
