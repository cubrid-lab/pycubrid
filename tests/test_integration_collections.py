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
import pycubrid.aio
import pytest
from pycubrid.connection import Connection
from pycubrid.cursor import Cursor

TEST_HOST = os.environ.get("CUBRID_TEST_HOST", "localhost")
TEST_PORT = int(os.environ.get("CUBRID_TEST_PORT", "33000"))
TEST_DB = os.environ.get("CUBRID_TEST_DB", "testdb")
TEST_USER = os.environ.get("CUBRID_TEST_USER", "dba")
TEST_PASSWORD = os.environ.get("CUBRID_TEST_PASSWORD", "")


pytestmark = pytest.mark.integration


def _connect(**kwargs: Any) -> Connection:
    return pycubrid.connect(
        host=TEST_HOST,
        port=TEST_PORT,
        database=TEST_DB,
        user=TEST_USER,
        password=TEST_PASSWORD,
        **kwargs,
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
            assert isinstance(row[0], frozenset)
            assert row[0] == frozenset({1, 2, 3})
            assert cursor.description is not None
            assert cursor.description[0][1] == 16
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
            assert isinstance(row[0], list)
            assert sorted(row[0]) == [1, 1, 2]
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
            assert row[0] == [1, 2, 3]
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
            assert row[0] == frozenset()
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
            cursor.execute("CREATE TABLE %s (id INT, tags SET(VARCHAR(20)))" % table)
            cursor.execute("INSERT INTO %s VALUES (1, {'alpha','beta','gamma'})" % table)
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
            cursor.execute("CREATE TABLE %s (a SET(INT), b MULTISET(INT), c SEQUENCE(INT))" % table)
            cursor.execute(
                "INSERT INTO %s VALUES "
                "({},{},{}),(NULL,NULL,NULL),({1,1},{1,1},{1,1}),"
                "({1,2,3},{1,2,3},{1,2,3})" % table
            )
            cursor.execute("SELECT * FROM %s WHERE a SETEQ {'1'} ORDER BY 1" % table)
            rows = cursor.fetchall()
            assert rows == [(frozenset({1}), [1, 1], [1, 1])]
        finally:
            cursor.execute("DROP TABLE IF EXISTS %s" % table)


class TestNullOnlyCollections:
    """Nonempty collections whose elements are all SQL NULL (#483)."""

    def test_null_only_and_mixed_rows(self, cursor: Cursor) -> None:
        table = _tbl()
        try:
            cursor.execute(
                "CREATE TABLE %s (id INT, s SET(INT), m MULTISET(INT), "
                "q SEQUENCE(INT), l LIST(VARCHAR(10)))" % table
            )
            cursor.execute(
                "INSERT INTO %s VALUES "
                "(1, {NULL}, {NULL}, {NULL}, {NULL}),"
                "(2, {NULL,NULL}, {NULL,NULL}, {NULL,NULL}, {NULL,NULL}),"
                "(3, {1,NULL}, {NULL,1,NULL}, {NULL,2,NULL}, {'a',NULL}),"
                "(4, {}, {}, {}, {})" % table
            )
            cursor.execute("SELECT * FROM %s ORDER BY id" % table)
            assert cursor.fetchall() == [
                (1, frozenset({None}), [None], [None], [None]),
                (2, frozenset({None}), [None, None], [None, None], [None, None]),
                (3, frozenset({1, None}), [1, None, None], [None, 2, None], ["a", None]),
                (4, frozenset(), [], [], []),
            ]
        finally:
            cursor.execute("DROP TABLE IF EXISTS %s" % table)

    def test_null_only_literals(self, cursor: Cursor) -> None:
        cursor.execute(
            "SELECT {NULL}, {NULL,NULL}, CAST({NULL} AS SET(INT)), "
            "CAST({NULL,NULL} AS MULTISET(INT)), "
            "CAST({NULL,NULL} AS SEQUENCE(VARCHAR(5))), {NULL,1}, {}"
        )
        assert cursor.fetchone() == (
            [None],
            [None, None],
            frozenset({None}),
            [None, None],
            [None, None],
            [None, 1],
            [],
        )

    def test_null_only_raw_bytes_unchanged(self) -> None:
        with _connect(decode_collections=False) as conn:
            cur = conn.cursor()
            cur.execute("SELECT {NULL,NULL}, CAST({NULL} AS SET(INT))")
            assert cur.fetchone() == (
                bytes.fromhex("0000000002ffffffffffffffff"),
                bytes.fromhex("0000000001ffffffff"),
            )
            cur.close()

    @pytest.mark.asyncio
    @pytest.mark.parametrize("decode", [False, True])
    async def test_async_null_only_collections(self, decode: bool) -> None:
        conn = await pycubrid.aio.connect(
            host=TEST_HOST,
            port=TEST_PORT,
            database=TEST_DB,
            user=TEST_USER,
            password=TEST_PASSWORD,
            decode_collections=decode,
        )
        async with conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT CAST({NULL,NULL} AS SET(INT)), "
                    "CAST({NULL,NULL} AS MULTISET(INT)), CAST({NULL} AS SEQUENCE(INT))"
                )
                row = await cur.fetchone()
                if decode:
                    assert row == (frozenset({None}), [None, None], [None])
                else:
                    assert row == (
                        bytes.fromhex("0000000002ffffffffffffffff"),
                        bytes.fromhex("0000000002ffffffffffffffff"),
                        bytes.fromhex("0000000001ffffffff"),
                    )


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

    @pytest.mark.asyncio
    @pytest.mark.parametrize("decode", [False, True])
    async def test_async_collection_decode(self, decode: bool) -> None:
        conn = await pycubrid.aio.connect(
            host=TEST_HOST,
            port=TEST_PORT,
            database=TEST_DB,
            user=TEST_USER,
            password=TEST_PASSWORD,
            decode_collections=decode,
        )
        async with conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT CAST({1,2,1} AS SET(INT)), "
                    "CAST({1,1,2} AS MULTISET(INT)), CAST({2,1,2} AS SEQUENCE(INT)), 42"
                )
                row = await cur.fetchone()
                assert row is not None
                assert cur.description is not None
                assert [col[1] for col in cur.description] == [16, 17, 18, 8]
                if decode:
                    assert row == (frozenset({1, 2}), [1, 1, 2], [2, 1, 2], 42)
                else:
                    assert all(isinstance(value, bytes) for value in row[:3])
                    assert row[3] == 42
