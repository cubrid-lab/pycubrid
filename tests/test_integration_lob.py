"""BLOB + CLOB live round-trip regression (#405).

Tests LOB data round-trip correctness via pycubrid's documented API:
values are inserted as ``bytes``/``str`` literals (``Lob`` objects cannot
be bound as parameters) and read back through ``Lob.read`` using the
fetched handle.  File import/export and seek are CUBRIDdb extension APIs
not present in pycubrid — see #405 for the divergence documentation.
"""

from __future__ import annotations

import os
import uuid
from collections.abc import Generator
from typing import Any

import pycubrid
import pytest
from pycubrid.connection import Connection
from pycubrid.constants import CUBRIDDataType
from pycubrid.cursor import Cursor
from pycubrid.lob import Lob

from ._cubrid_endpoint import TEST_DB, TEST_HOST, TEST_PASSWORD, TEST_PORT, TEST_USER

pytestmark = [pytest.mark.integration, pytest.mark.no_escape_pin]


@pytest.fixture
def conn() -> Generator[Connection, None, None]:
    c = pycubrid.connect(
        host=TEST_HOST,
        port=TEST_PORT,
        database=TEST_DB,
        user=TEST_USER,
        password=TEST_PASSWORD,
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


def _read_lob(conn: Connection, lob_type: int, handle: dict[str, Any]) -> bytes:
    """Read the full content of a fetched LOB handle."""
    assert handle["lob_type"] == lob_type
    lob = Lob(conn, lob_type, handle["packed_lob_handle"])
    try:
        return lob.read(handle["lob_length"])
    finally:
        lob.close()


class TestBlobRoundTrip:
    def test_small_blob(self, conn: Connection, cursor: Cursor) -> None:
        table = _tbl()
        try:
            cursor.execute("CREATE TABLE %s (payload BLOB)" % table)
            data = b"\x00\x01\x02\xff" * 100  # 400 bytes
            cursor.execute("INSERT INTO %s (payload) VALUES (?)" % table, (data,))

            cursor.execute("SELECT payload FROM %s" % table)
            row = cursor.fetchone()
            assert row is not None
            result = _read_lob(conn, CUBRIDDataType.BLOB, row[0])
            assert result == data
        finally:
            cursor.execute("DROP TABLE IF EXISTS %s" % table)

    def test_large_blob_exceeds_old_cap(self, conn: Connection, cursor: Cursor) -> None:
        """Verify no truncation at the old 81908-byte cap (issue #362)."""
        table = _tbl()
        try:
            cursor.execute("CREATE TABLE %s (payload BLOB)" % table)
            data = os.urandom(100_000)  # >81908 bytes
            cursor.execute("INSERT INTO %s (payload) VALUES (?)" % table, (data,))

            cursor.execute("SELECT payload FROM %s" % table)
            row = cursor.fetchone()
            assert row is not None
            result = _read_lob(conn, CUBRIDDataType.BLOB, row[0])
            assert result == data
        finally:
            cursor.execute("DROP TABLE IF EXISTS %s" % table)


class TestClobRoundTrip:
    def test_clob_string(self, conn: Connection, cursor: Cursor) -> None:
        table = _tbl()
        try:
            cursor.execute("CREATE TABLE %s (content CLOB)" % table)
            text = "hello world — pycubrid CLOB test"
            cursor.execute("INSERT INTO %s (content) VALUES (?)" % table, (text,))

            cursor.execute("SELECT content FROM %s" % table)
            row = cursor.fetchone()
            assert row is not None
            result = _read_lob(conn, CUBRIDDataType.CLOB, row[0])
            assert result.decode("utf-8") == text
        finally:
            cursor.execute("DROP TABLE IF EXISTS %s" % table)

    def test_clob_unicode_cjk(self, conn: Connection, cursor: Cursor) -> None:
        table = _tbl()
        try:
            cursor.execute("CREATE TABLE %s (content CLOB)" % table)
            text = "한국어 테스트 日本語テスト 中文测试"
            cursor.execute("INSERT INTO %s (content) VALUES (?)" % table, (text,))

            cursor.execute("SELECT content FROM %s" % table)
            row = cursor.fetchone()
            assert row is not None
            result = _read_lob(conn, CUBRIDDataType.CLOB, row[0])
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
            cursor.execute("INSERT INTO %s (content) VALUES (?)" % table, (text,))

            cursor.execute("SELECT content FROM %s" % table)
            row = cursor.fetchone()
            assert row is not None
            result = _read_lob(conn, CUBRIDDataType.CLOB, row[0])
            assert len(result) == len(data)
            assert result == data
        finally:
            cursor.execute("DROP TABLE IF EXISTS %s" % table)


class TestLobMixedTable:
    def test_blob_with_scalar_columns(self, conn: Connection, cursor: Cursor) -> None:
        table = _tbl()
        try:
            cursor.execute("CREATE TABLE %s (id INT, name VARCHAR(100), payload BLOB)" % table)
            blob_data = b"\xde\xad\xbe\xef" * 50
            cursor.execute(
                "INSERT INTO %s (id, name, payload) VALUES (1, 'test', ?)" % table,
                (blob_data,),
            )

            cursor.execute("SELECT id, name, payload FROM %s" % table)
            row = cursor.fetchone()
            assert row is not None
            assert row[0] == 1
            assert row[1] == "test"
            result = _read_lob(conn, CUBRIDDataType.BLOB, row[2])
            assert result == blob_data
        finally:
            cursor.execute("DROP TABLE IF EXISTS %s" % table)
