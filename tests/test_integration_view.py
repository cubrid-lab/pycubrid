"""VIEW create / select / alter regression tests (#401).

Mirrors the 3 VIEW scenarios from CUBRID/cubrid-python
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


def _uid() -> str:
    return uuid.uuid4().hex[:8]


class TestViewOperations:
    def test_create_view_and_select(self, cursor: Cursor) -> None:
        table = "pycubrid_vtbl_%s" % _uid()
        view = "pycubrid_view_%s" % _uid()
        try:
            cursor.execute(
                "CREATE TABLE %s (qty INT, price INT)" % table
            )
            cursor.execute(
                "INSERT INTO %s VALUES (10, 15), (20, 25)" % table
            )
            cursor.execute(
                "CREATE VIEW %s AS SELECT qty, price, qty * price AS total FROM %s"
                % (view, table)
            )
            cursor.execute("SELECT * FROM %s ORDER BY qty" % view)
            # Verify cursor.description exposes view column metadata
            assert cursor.description is not None
            assert len(cursor.description) == 3
            assert cursor.description[0][0] == "qty"
            assert cursor.description[1][0] == "price"
            assert cursor.description[2][0] == "total"
            rows = cursor.fetchall()
            assert len(rows) == 2
            assert rows[0][2] == 150  # 10 * 15
            assert rows[1][2] == 500  # 20 * 25
        finally:
            cursor.execute("DROP VIEW IF EXISTS %s" % view)
            cursor.execute("DROP TABLE IF EXISTS %s" % table)

    def test_show_create_view(self, cursor: Cursor) -> None:
        table = "pycubrid_vtbl_%s" % _uid()
        view = "pycubrid_view_%s" % _uid()
        try:
            cursor.execute("CREATE TABLE %s (qty INT, price INT)" % table)
            cursor.execute(
                "CREATE VIEW %s AS SELECT qty, price, qty * price AS total FROM %s"
                % (view, table)
            )
            cursor.execute("SHOW CREATE VIEW %s" % view)
            row = cursor.fetchone()
            assert row is not None
            # row[0] = view name, row[1] = view definition
            assert view in row[0].lower() or view.lower() in row[0].lower()
            assert "select" in row[1].lower()
        finally:
            cursor.execute("DROP VIEW IF EXISTS %s" % view)
            cursor.execute("DROP TABLE IF EXISTS %s" % table)

    def test_alter_view_add_query(self, cursor: Cursor) -> None:
        table = "pycubrid_vtbl_%s" % _uid()
        view = "pycubrid_view_%s" % _uid()
        try:
            cursor.execute(
                "CREATE TABLE %s (id INT, phone VARCHAR(20))" % table
            )
            cursor.execute(
                "INSERT INTO %s VALUES (1, '111-1111'), (2, '222-2222'), (3, '333-3333')"
                % table
            )
            cursor.execute(
                "CREATE VIEW %s AS SELECT * FROM %s" % (view, table)
            )
            cursor.execute(
                "ALTER VIEW %s ADD QUERY SELECT * FROM %s WHERE id IN (1, 2)"
                % (view, table)
            )
            cursor.execute("SELECT * FROM %s ORDER BY id" % view)
            rows = cursor.fetchall()
            # Original 3 rows + 2 from added query = 5
            assert len(rows) == 5
        finally:
            cursor.execute("DROP VIEW IF EXISTS %s" % view)
            cursor.execute("DROP TABLE IF EXISTS %s" % table)

    def test_update_through_view_raises(self, cursor: Cursor) -> None:
        table = "pycubrid_vtbl_%s" % _uid()
        view = "pycubrid_view_%s" % _uid()
        try:
            cursor.execute(
                "CREATE TABLE %s (id INT, phone VARCHAR(20) NOT NULL)" % table
            )
            cursor.execute(
                "INSERT INTO %s VALUES (1, '111-1111')" % table
            )
            cursor.execute(
                "CREATE VIEW %s AS SELECT * FROM %s" % (view, table)
            )
            with pytest.raises(pycubrid.IntegrityError):
                cursor.execute("UPDATE %s SET phone = NULL" % view)
        finally:
            cursor.execute("DROP VIEW IF EXISTS %s" % view)
            cursor.execute("DROP TABLE IF EXISTS %s" % table)

    def test_drop_view(self, cursor: Cursor) -> None:
        table = "pycubrid_vtbl_%s" % _uid()
        view = "pycubrid_view_%s" % _uid()
        try:
            cursor.execute("CREATE TABLE %s (id INT)" % table)
            cursor.execute(
                "CREATE VIEW %s AS SELECT * FROM %s" % (view, table)
            )
            cursor.execute("DROP VIEW %s" % view)
            with pytest.raises(pycubrid.DatabaseError):
                cursor.execute("SELECT * FROM %s" % view)
        finally:
            cursor.execute("DROP VIEW IF EXISTS %s" % view)
            cursor.execute("DROP TABLE IF EXISTS %s" % table)
