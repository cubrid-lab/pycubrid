"""TRIGGER fire-on-insert verification (#402).

Mirrors the TRIGGER scenario from CUBRID/cubrid-python
``tests3/test_execute.py``.
"""

from __future__ import annotations

import uuid
from collections.abc import Generator

import pycubrid
import pytest
from pycubrid.connection import Connection
from pycubrid.cursor import Cursor

from ._cubrid_endpoint import TEST_DB, TEST_HOST, TEST_PASSWORD, TEST_PORT, TEST_USER

pytestmark = pytest.mark.integration


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


class TestTrigger:
    def test_trigger_fires_on_insert(self, cursor: Cursor) -> None:
        uid = uuid.uuid4().hex[:8]
        source = "pycubrid_src_%s" % uid
        log = "pycubrid_log_%s" % uid
        trigger_name = "pycubrid_trg_%s" % uid
        trigger_created = False
        try:
            cursor.execute("CREATE TABLE %s (a INT, b STRING)" % log)
            cursor.execute("CREATE TABLE %s (a INT, b STRING)" % source)
            cursor.execute(
                "CREATE TRIGGER %s AFTER INSERT ON %s "
                "EXECUTE INSERT INTO %s (a, b) VALUES (obj.a, TO_CHAR(obj.a))"
                % (trigger_name, source, log)
            )
            trigger_created = True

            cursor.execute("INSERT INTO %s (a, b) VALUES (1, 'test')" % source)

            # Verify trigger-inserted row in log table
            cursor.execute("SELECT * FROM %s" % log)
            rows = cursor.fetchall()
            assert rows == [(1, "1")]

            # Verify original row in source table
            cursor.execute("SELECT * FROM %s" % source)
            rows = cursor.fetchall()
            assert rows == [(1, "test")]
        finally:
            try:
                if trigger_created:
                    cursor.execute("DROP TRIGGER %s" % trigger_name)
            finally:
                cursor.execute("DROP TABLE IF EXISTS %s" % source)
                cursor.execute("DROP TABLE IF EXISTS %s" % log)

    def test_trigger_rowcount(self, cursor: Cursor) -> None:
        """INSERT that fires a trigger should report rowcount for the statement itself."""
        uid = uuid.uuid4().hex[:8]
        source = "pycubrid_src_%s" % uid
        log = "pycubrid_log_%s" % uid
        trigger_name = "pycubrid_trg_%s" % uid
        trigger_created = False
        try:
            cursor.execute("CREATE TABLE %s (a INT, b STRING)" % log)
            cursor.execute("CREATE TABLE %s (a INT, b STRING)" % source)
            cursor.execute(
                "CREATE TRIGGER %s AFTER INSERT ON %s "
                "EXECUTE INSERT INTO %s (a, b) VALUES (obj.a, TO_CHAR(obj.a))"
                % (trigger_name, source, log)
            )
            trigger_created = True
            cursor.execute("INSERT INTO %s (a, b) VALUES (1, 'x')" % source)
            assert cursor.rowcount == 1  # reports the INSERT into source, not log
        finally:
            try:
                if trigger_created:
                    cursor.execute("DROP TRIGGER %s" % trigger_name)
            finally:
                cursor.execute("DROP TABLE IF EXISTS %s" % source)
                cursor.execute("DROP TABLE IF EXISTS %s" % log)
