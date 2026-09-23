"""PARTITION table DDL + CRUD regression tests (#400).

Mirrors the 5 PARTITION scenarios from CUBRID/cubrid-python
``tests3/test_execute.py``.
"""

from __future__ import annotations

import os
import uuid
from collections.abc import Generator

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


@pytest.fixture
def part_table(cursor: Cursor) -> Generator[str, None, None]:
    table = "pycubrid_part_%s" % uuid.uuid4().hex[:8]
    cursor.execute(
        "CREATE TABLE %s ("
        "id INT, test_char CHAR(50), test_varchar VARCHAR(200), "
        "test_datetime DATETIME"
        ")" % table
    )
    yield table
    cursor.execute("DROP TABLE IF EXISTS %s" % table)


def _add_partition(cursor: Cursor, table: str) -> None:
    cursor.execute(
        "ALTER TABLE %s PARTITION BY LIST (test_char) ("
        "PARTITION p0 VALUES IN ('aaa','bbb','ddd'),"
        "PARTITION p1 VALUES IN ('fff','ggg','hhh',NULL),"
        "PARTITION p2 VALUES IN ('kkk','lll','mmm'))" % table
    )


class TestPartitionTable:
    def test_empty_partitioned_table(self, cursor: Cursor, part_table: str) -> None:
        _add_partition(cursor, part_table)
        cursor.execute("SELECT COUNT(*) FROM %s" % part_table)
        assert cursor.fetchone() == (0,)
        cursor.execute("SELECT MAX(id) FROM %s" % part_table)
        assert cursor.fetchone() == (None,)

    def test_alter_partition(self, cursor: Cursor, part_table: str) -> None:
        _add_partition(cursor, part_table)
        # If ALTER PARTITION succeeds without error, the test passes.

    def test_insert_into_partitions(self, cursor: Cursor, part_table: str) -> None:
        _add_partition(cursor, part_table)
        cursor.execute(
            "INSERT INTO %s VALUES "
            "(1, 'aaa', 'row1', '2024-01-01 09:00:00'),"
            "(5, 'ggg', 'row2', '2024-01-02 09:00:00')" % part_table
        )
        cursor.execute("SELECT COUNT(*) FROM %s" % part_table)
        assert cursor.fetchone() == (2,)

    def test_select_from_partition_subtable(self, cursor: Cursor, part_table: str) -> None:
        _add_partition(cursor, part_table)
        cursor.execute(
            "INSERT INTO %s VALUES "
            "(1, 'aaa', 'row1', '2024-01-01 09:00:00'),"
            "(5, 'ggg', 'row2', '2024-01-02 09:00:00'),"
            "(10, 'kkk', 'row3', '2024-01-03 09:00:00')" % part_table
        )
        # Query the p0 partition sub-table directly
        cursor.execute("SELECT * FROM %s__p__p0 ORDER BY id" % part_table)
        rows = cursor.fetchall()
        assert len(rows) == 1
        assert rows[0][0] == 1  # id=1 with test_char='aaa' -> p0

    def test_delete_from_partitioned_table(self, cursor: Cursor, part_table: str) -> None:
        _add_partition(cursor, part_table)
        cursor.execute(
            "INSERT INTO %s VALUES "
            "(1, 'aaa', 'row1', '2024-01-01 09:00:00'),"
            "(5, 'ggg', 'row2', '2024-01-02 09:00:00'),"
            "(10, 'kkk', 'row3', '2024-01-03 09:00:00')" % part_table
        )
        cursor.execute("DELETE FROM %s WHERE id = 1" % part_table)
        assert cursor.rowcount == 1
        cursor.execute("SELECT COUNT(*) FROM %s WHERE id >= 0" % part_table)
        assert cursor.fetchone() == (2,)
