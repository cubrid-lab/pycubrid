"""ENUM type insert / select / update / cast regression (#404).

Mirrors the 11 ENUM scenarios from CUBRID/cubrid-python
``tests3/test_enum.py``.
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


pytestmark = pytest.mark.integration


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
    return "pycubrid_enum_%s" % uuid.uuid4().hex[:8]


@pytest.fixture
def enum_table(cursor: Cursor) -> Generator[str, None, None]:
    table = _tbl()
    cursor.execute(
        "CREATE TABLE %s ("
        "id INT, "
        "working_days ENUM('Monday','Tuesday','Wednesday','Thursday','Friday'), "
        "answers ENUM('Yes','No','Cancel')"
        ")" % table
    )
    cursor.execute(
        "INSERT INTO %s VALUES "
        "(1, 1, 1), (2, 'Tuesday', 'No'), (3, 'Wednesday', 'Cancel')" % table
    )
    yield table
    cursor.execute("DROP TABLE IF EXISTS %s" % table)


class TestEnumInsertSelect:
    def test_insert_by_ordinal_and_name(self, cursor: Cursor, enum_table: str) -> None:
        cursor.execute("SELECT COUNT(*) FROM %s" % enum_table)
        assert cursor.fetchone() == (3,)

    def test_select_cast_as_int(self, cursor: Cursor, enum_table: str) -> None:
        cursor.execute(
            "SELECT CAST(working_days AS INT), CAST(answers AS INT) "
            "FROM %s ORDER BY id" % enum_table
        )
        rows = cursor.fetchall()
        assert rows == [(1, 1), (2, 2), (3, 3)]

    def test_select_enum_values(self, cursor: Cursor, enum_table: str) -> None:
        cursor.execute(
            "SELECT working_days, answers FROM %s ORDER BY id" % enum_table
        )
        rows = cursor.fetchall()
        assert rows[0] == ("Monday", "Yes")
        assert rows[1] == ("Tuesday", "No")
        assert rows[2] == ("Wednesday", "Cancel")


class TestEnumUpdate:
    def test_update_all_to_first_value(self, cursor: Cursor, enum_table: str) -> None:
        cursor.execute("UPDATE %s SET answers = 1" % enum_table)
        assert cursor.rowcount == 3
        cursor.execute("SELECT answers FROM %s ORDER BY id" % enum_table)
        rows = cursor.fetchall()
        assert all(r[0] == "Yes" for r in rows)

    def test_update_by_name(self, cursor: Cursor) -> None:
        table = _tbl()
        try:
            cursor.execute(
                "CREATE TABLE %s (e1 ENUM('a','b','c'), e2 ENUM('Yes','No','Cancel'))"
                % table
            )
            cursor.execute(
                "INSERT INTO %s VALUES ('a','Yes'),('b','No'),('c','Cancel')" % table
            )
            cursor.execute("UPDATE %s SET e1='b', e2='No'" % table)
            assert cursor.rowcount == 3
            cursor.execute("SELECT * FROM %s" % table)
            rows = cursor.fetchall()
            assert all(r == ("b", "No") for r in rows)
        finally:
            cursor.execute("DROP TABLE IF EXISTS %s" % table)


class TestEnumDescription:
    def test_enum_type_code_is_25(self, cursor: Cursor, enum_table: str) -> None:
        cursor.execute("SELECT working_days FROM %s" % enum_table)
        assert cursor.description is not None
        assert cursor.description[0][1] == 25  # CUBRIDDataType.ENUM
