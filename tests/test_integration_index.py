"""INDEX hint SQL regression tests (#399).

Mirrors the 6 INDEX scenarios from CUBRID/cubrid-python
``tests3/test_execute.py``.  Verifies that USE INDEX, FORCE INDEX,
``/*+ recompile */`` hints, and indexed JOINs work correctly through
pycubrid without breaking placeholder parsing or rowcount reporting.
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
    return "pycubrid_idx_%s" % uuid.uuid4().hex[:8]


@pytest.fixture
def index_tables(cursor: Cursor) -> Generator[tuple[str, str], None, None]:
    """Create two tables with indexes and sample data (mirrors official suite)."""
    t = _tbl()
    u = _tbl()
    cursor.execute(
        "CREATE TABLE %s (id INT PRIMARY KEY, val INT, fk INT, text VARCHAR(100))" % t
    )
    cursor.execute("CREATE INDEX _t_id ON %s (id)" % t)
    cursor.execute("CREATE INDEX _t_val ON %s (val)" % t)
    cursor.execute(
        "CREATE TABLE %s (id INT PRIMARY KEY, text VARCHAR(100))" % u
    )
    cursor.execute("CREATE INDEX _u_id ON %s (id)" % u)

    # Seed data
    cursor.execute(
        "INSERT INTO %s (id, val, fk, text) VALUES (1, 10, 1, 'aa'),"
        "(2, 20, 1, 'bb'),(3, 30, 2, 'cc'),(4, 40, 2, 'dd')" % t
    )
    cursor.execute(
        "INSERT INTO %s (id, text) VALUES (1, 'xx'),(2, 'yy')" % u
    )
    yield t, u
    cursor.execute("DROP TABLE IF EXISTS %s" % t)
    cursor.execute("DROP TABLE IF EXISTS %s" % u)


class TestIndexHintSQL:
    def test_use_index_select(self, cursor: Cursor, index_tables: tuple[str, str]) -> None:
        t, _ = index_tables
        cursor.execute("SELECT * FROM %s USE INDEX (_t_id) WHERE id < 2" % t)
        row = cursor.fetchone()
        assert row is not None
        assert row[0] == 1

    def test_recompile_hint_with_use_index(
        self, cursor: Cursor, index_tables: tuple[str, str]
    ) -> None:
        t, _ = index_tables
        cursor.execute(
            "SELECT /*+ recompile */ * FROM %s USE INDEX (_t_id) WHERE id > 1" % t
        )
        rows = cursor.fetchall()
        assert len(rows) == 3

        cursor.execute(
            "SELECT /*+ recompile */ COUNT(*) FROM %s USE INDEX (_t_id) WHERE id > 1" % t
        )
        row = cursor.fetchone()
        assert row is not None
        assert row[0] == 3

    def test_join_with_force_and_use_index(
        self, cursor: Cursor, index_tables: tuple[str, str]
    ) -> None:
        t, u = index_tables
        cursor.execute(
            "SELECT /*+ recompile */ COUNT(*) FROM %s FORCE INDEX (_t_val) "
            "INNER JOIN %s USE INDEX (_u_id) ON %s.fk = %s.id "
            "WHERE RIGHT(%s.text, 2) < 'zz' AND %s.id < 100"
            % (t, u, t, u, u, u)
        )
        row = cursor.fetchone()
        assert row is not None
        assert row[0] == 4  # all 4 rows in t join with u

    def test_subselect_with_force_index(
        self, cursor: Cursor, index_tables: tuple[str, str]
    ) -> None:
        t, u = index_tables
        cursor.execute(
            "SELECT /*+ recompile */ COUNT(*) FROM %s FORCE INDEX (_t_val) "
            "INNER JOIN (SELECT * FROM %s FORCE INDEX (_u_id) "
            "WHERE RIGHT(text, 2) < 'zz') x ON %s.fk = x.id"
            % (t, u, t)
        )
        row = cursor.fetchone()
        assert row is not None
        assert row[0] == 4

    def test_update_with_use_index(
        self, cursor: Cursor, index_tables: tuple[str, str]
    ) -> None:
        t, _ = index_tables
        cursor.execute(
            "UPDATE %s USE INDEX (_t_id, _t_val) SET val = 1000 WHERE id < 4" % t
        )
        assert cursor.rowcount == 3

        cursor.execute("SELECT COUNT(*) FROM %s WHERE val = 1000" % t)
        row = cursor.fetchone()
        assert row is not None
        assert row[0] == 3

    def test_delete_with_use_index_executemany(
        self, cursor: Cursor, index_tables: tuple[str, str]
    ) -> None:
        t, _ = index_tables
        cursor.executemany(
            "DELETE FROM %s USE INDEX (_t_id, _t_val) WHERE id = ?" % t,
            [(1,), (4,), (3,)],
        )
        cursor.execute("SELECT * FROM %s" % t)
        rows = cursor.fetchall()
        assert len(rows) == 1
        assert rows[0][0] == 2  # only id=2 remains
