"""Integration tests against a live CUBRID instance.

These tests require a running CUBRID database. They are skipped
automatically when no CUBRID connection is available.

Set the environment variable ``CUBRID_TEST_HOST`` and ``CUBRID_TEST_PORT``
to configure, or use defaults (localhost:33000).
"""

from __future__ import annotations

import datetime
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
        connection = pycubrid.connect(
            host=TEST_HOST,
            port=TEST_PORT,
            database=TEST_DB,
            user=TEST_USER,
            password=TEST_PASSWORD,
        )
        connection.close()
        return True
    except Exception:
        return False


def _table_name(suffix: str) -> str:
    return "pycubrid_test_%s_%s" % (suffix, uuid.uuid4().hex[:8])


def _drop_table(cur: Cursor, table_name: str) -> None:
    cur.execute("DROP TABLE IF EXISTS %s" % table_name)


def _create_basic_table(cur: Cursor, table_name: str) -> None:
    cur.execute(
        "CREATE TABLE IF NOT EXISTS %s "
        "(id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(100), val INT)" % table_name
    )


pytestmark = pytest.mark.skipif(not _can_connect(), reason="CUBRID instance not available")


@pytest.fixture
def conn() -> Generator[Connection, None, None]:
    connection = pycubrid.connect(
        host=TEST_HOST,
        port=TEST_PORT,
        database=TEST_DB,
        user=TEST_USER,
        password=TEST_PASSWORD,
    )
    yield connection
    connection.close()


@pytest.fixture
def cursor(conn: Connection) -> Generator[Cursor, None, None]:
    cur = conn.cursor()
    yield cur
    cur.close()


@pytest.fixture
def test_table(cursor: Cursor) -> Generator[str, None, None]:
    table_name = _table_name("dml")
    _create_basic_table(cursor, table_name)
    yield table_name
    _drop_table(cursor, table_name)


class TestConnection:
    def test_connect_and_close(self) -> None:
        connection = pycubrid.connect(
            host=TEST_HOST,
            port=TEST_PORT,
            database=TEST_DB,
            user=TEST_USER,
            password=TEST_PASSWORD,
        )
        cur = connection.cursor()
        cur.execute("SELECT 1")
        assert cur.fetchone() == (1,)
        cur.close()
        connection.close()
        with pytest.raises(pycubrid.InterfaceError, match="closed"):
            connection.commit()

    def test_server_version(self, conn: Connection) -> None:
        version = conn.get_server_version()
        assert isinstance(version, str)
        assert version
        major = version.split(".", 1)[0]
        assert major.isdigit() and int(major) >= 10

    def test_commit(self, conn: Connection) -> None:
        conn.commit()

    def test_rollback(self, conn: Connection) -> None:
        conn.rollback()

    def test_autocommit_toggle(self, conn: Connection) -> None:
        conn.autocommit = True
        assert conn.autocommit is True
        conn.autocommit = False
        assert conn.autocommit is False

    def test_context_manager(self) -> None:
        closed_connection: Connection | None = None
        with pycubrid.connect(
            host=TEST_HOST,
            port=TEST_PORT,
            database=TEST_DB,
            user=TEST_USER,
            password=TEST_PASSWORD,
        ) as connection:
            closed_connection = connection
            cur = connection.cursor()
            cur.execute("SELECT 1")
            assert cur.fetchone() == (1,)
            cur.close()
        assert closed_connection is not None
        with pytest.raises(pycubrid.InterfaceError, match="closed"):
            closed_connection.commit()

    def test_multiple_cursors(self, conn: Connection) -> None:
        cur1 = conn.cursor()
        cur2 = conn.cursor()
        try:
            assert cur1 is not cur2
            cur1.execute("SELECT 1")
            cur2.execute("SELECT 2")
            assert cur1.fetchone() == (1,)
            assert cur2.fetchone() == (2,)
        finally:
            cur1.close()
            cur2.close()


class TestCursorDDL:
    def test_create_and_drop_table(self, cursor: Cursor) -> None:
        table_name = _table_name("ddl")
        cursor.execute("CREATE TABLE %s (id INT PRIMARY KEY)" % table_name)
        _drop_table(cursor, table_name)

    def test_create_table_if_not_exists(self, cursor: Cursor) -> None:
        table_name = _table_name("ddl_if_exists")
        try:
            cursor.execute("CREATE TABLE IF NOT EXISTS %s (id INT PRIMARY KEY)" % table_name)
            cursor.execute("CREATE TABLE IF NOT EXISTS %s (id INT PRIMARY KEY)" % table_name)
        finally:
            _drop_table(cursor, table_name)


class TestCursorDML:
    def test_insert_and_select(self, cursor: Cursor, test_table: str) -> None:
        cursor.execute("INSERT INTO %s (name, val) VALUES (?, ?)" % test_table, ("alpha", 10))
        cursor.execute("SELECT name, val FROM %s WHERE name = ?" % test_table, ("alpha",))
        assert cursor.fetchone() == ("alpha", 10)

    def test_insert_multiple_rows(self, cursor: Cursor, test_table: str) -> None:
        cursor.executemany(
            "INSERT INTO %s (name, val) VALUES (?, ?)" % test_table,
            [("a", 1), ("b", 2), ("c", 3)],
        )
        cursor.execute("SELECT COUNT(*) FROM %s" % test_table)
        assert cursor.fetchone() == (3,)

    def test_update(self, cursor: Cursor, test_table: str) -> None:
        cursor.execute("INSERT INTO %s (name, val) VALUES (?, ?)" % test_table, ("target", 1))
        cursor.execute("UPDATE %s SET val = ? WHERE name = ?" % test_table, (99, "target"))
        cursor.execute("SELECT val FROM %s WHERE name = ?" % test_table, ("target",))
        assert cursor.fetchone() == (99,)

    def test_delete(self, cursor: Cursor, test_table: str) -> None:
        cursor.execute("INSERT INTO %s (name, val) VALUES (?, ?)" % test_table, ("gone", 7))
        cursor.execute("DELETE FROM %s WHERE name = ?" % test_table, ("gone",))
        cursor.execute("SELECT name FROM %s WHERE name = ?" % test_table, ("gone",))
        assert cursor.fetchall() == []

    def test_select_with_where(self, cursor: Cursor, test_table: str) -> None:
        cursor.executemany(
            "INSERT INTO %s (name, val) VALUES (?, ?)" % test_table,
            [("a", 1), ("b", 2), ("c", 3)],
        )
        cursor.execute("SELECT name FROM %s WHERE val >= ? ORDER BY val" % test_table, (2,))
        assert cursor.fetchall() == [("b",), ("c",)]

    def test_fetchone(self, cursor: Cursor, test_table: str) -> None:
        cursor.execute("INSERT INTO %s (name, val) VALUES (?, ?)" % test_table, ("one", 1))
        cursor.execute("SELECT name, val FROM %s" % test_table)
        row = cursor.fetchone()
        assert isinstance(row, tuple)
        assert row == ("one", 1)

    def test_fetchmany(self, cursor: Cursor, test_table: str) -> None:
        cursor.executemany(
            "INSERT INTO %s (name, val) VALUES (?, ?)" % test_table,
            [("a", 1), ("b", 2), ("c", 3)],
        )
        cursor.execute("SELECT name FROM %s ORDER BY val" % test_table)
        assert len(cursor.fetchmany(2)) == 2

    def test_fetchall(self, cursor: Cursor, test_table: str) -> None:
        cursor.executemany(
            "INSERT INTO %s (name, val) VALUES (?, ?)" % test_table,
            [("a", 1), ("b", 2), ("c", 3)],
        )
        cursor.execute("SELECT name FROM %s ORDER BY val" % test_table)
        assert cursor.fetchall() == [("a",), ("b",), ("c",)]

    def test_rowcount_insert(self, cursor: Cursor, test_table: str) -> None:
        cursor.execute("INSERT INTO %s (name, val) VALUES (?, ?)" % test_table, ("rowcount", 5))
        assert cursor.rowcount == 1

    def test_rowcount_update(self, cursor: Cursor, test_table: str) -> None:
        cursor.execute("INSERT INTO %s (name, val) VALUES (?, ?)" % test_table, ("rowcount", 5))
        cursor.execute("UPDATE %s SET val = ? WHERE name = ?" % test_table, (8, "rowcount"))
        assert cursor.rowcount == 1

    def test_description(self, cursor: Cursor, test_table: str) -> None:
        cursor.execute("INSERT INTO %s (name, val) VALUES (?, ?)" % test_table, ("meta", 5))
        cursor.execute("SELECT id, name, val FROM %s" % test_table)
        assert cursor.description is not None
        assert len(cursor.description) == 3
        assert cursor.description[0][0] == "id"
        assert cursor.description[1][0] == "name"
        assert cursor.description[2][0] == "val"

    def test_lastrowid(self, cursor: Cursor, test_table: str) -> None:
        cursor.execute("INSERT INTO %s (name, val) VALUES (?, ?)" % test_table, ("new", 42))
        assert cursor.lastrowid is not None


class TestParameterBinding:
    def test_string_parameter(self, cursor: Cursor, test_table: str) -> None:
        cursor.execute("INSERT INTO %s (name, val) VALUES (?, ?)" % test_table, ("string", 1))
        cursor.execute("SELECT name FROM %s WHERE val = ?" % test_table, (1,))
        assert cursor.fetchone() == ("string",)

    def test_int_parameter(self, cursor: Cursor, test_table: str) -> None:
        cursor.execute("INSERT INTO %s (name, val) VALUES (?, ?)" % test_table, ("int", 123))
        cursor.execute("SELECT val FROM %s WHERE name = ?" % test_table, ("int",))
        assert cursor.fetchone() == (123,)

    def test_null_parameter(self, cursor: Cursor, test_table: str) -> None:
        cursor.execute("INSERT INTO %s (name, val) VALUES (?, ?)" % test_table, ("null", None))
        cursor.execute("SELECT val FROM %s WHERE name = ?" % test_table, ("null",))
        assert cursor.fetchone() == (None,)

    def test_multiple_parameters(self, cursor: Cursor, test_table: str) -> None:
        cursor.execute("INSERT INTO %s (name, val) VALUES (?, ?)" % test_table, ("multi", 77))
        cursor.execute(
            "SELECT name, val FROM %s WHERE name = ? AND val = ?" % test_table,
            ("multi", 77),
        )
        assert cursor.fetchone() == ("multi", 77)


class TestTransactions:
    def test_commit_persists_data(self, conn: Connection) -> None:
        table_name = _table_name("tx_commit")
        cur = conn.cursor()
        try:
            _create_basic_table(cur, table_name)
            cur.execute("INSERT INTO %s (name, val) VALUES (?, ?)" % table_name, ("commit", 1))
            conn.commit()

            verifier = conn.cursor()
            try:
                verifier.execute("SELECT COUNT(*) FROM %s WHERE name = ?" % table_name, ("commit",))
                assert verifier.fetchone() == (1,)
            finally:
                verifier.close()
        finally:
            _drop_table(cur, table_name)
            cur.close()

    def test_rollback_discards_data(self, conn: Connection) -> None:
        table_name = _table_name("tx_rollback")
        cur = conn.cursor()
        try:
            _create_basic_table(cur, table_name)
            conn.commit()  # persist DDL before testing rollback of DML
            cur.execute("INSERT INTO %s (name, val) VALUES (?, ?)" % table_name, ("rollback", 1))
            conn.rollback()
            cur.execute("SELECT COUNT(*) FROM %s WHERE name = ?" % table_name, ("rollback",))
            assert cur.fetchone() == (0,)
        finally:
            _drop_table(cur, table_name)
            cur.close()

    def test_autocommit_mode(self, conn: Connection) -> None:
        table_name = _table_name("tx_autocommit")
        cur = conn.cursor()
        try:
            _create_basic_table(cur, table_name)
            conn.autocommit = True
            cur.execute("INSERT INTO %s (name, val) VALUES (?, ?)" % table_name, ("auto", 1))

            verifier = conn.cursor()
            try:
                verifier.execute("SELECT COUNT(*) FROM %s WHERE name = ?" % table_name, ("auto",))
                assert verifier.fetchone() == (1,)
            finally:
                verifier.close()
        finally:
            conn.autocommit = False
            _drop_table(cur, table_name)
            cur.close()


class TestExecutemany:
    def test_executemany(self, cursor: Cursor, test_table: str) -> None:
        cursor.executemany(
            "INSERT INTO %s (name, val) VALUES (?, ?)" % test_table,
            [("a", 1), ("b", 2), ("c", 3)],
        )
        cursor.execute("SELECT COUNT(*) FROM %s" % test_table)
        assert cursor.fetchone() == (3,)


class TestDataTypes:
    @pytest.fixture
    def datatype_table(self, cursor: Cursor) -> Generator[str, None, None]:
        table_name = _table_name("types")
        cursor.execute(
            "CREATE TABLE IF NOT EXISTS %s "
            "(id INT AUTO_INCREMENT PRIMARY KEY, c_int INT, c_smallint SMALLINT, c_bigint BIGINT, "
            "c_float FLOAT, c_double DOUBLE, c_varchar VARCHAR(100), c_date DATE, c_time TIME, "
            "c_datetime DATETIME, c_nullable INT)" % table_name
        )
        yield table_name
        _drop_table(cursor, table_name)

    def test_integer_types(self, cursor: Cursor, datatype_table: str) -> None:
        cursor.execute(
            "INSERT INTO %s (c_int, c_smallint, c_bigint) VALUES (?, ?, ?)" % datatype_table,
            (123, 12, 1234567890123),
        )
        cursor.execute("SELECT c_int, c_smallint, c_bigint FROM %s" % datatype_table)
        assert cursor.fetchone() == (123, 12, 1234567890123)

    def test_float_double(self, cursor: Cursor, datatype_table: str) -> None:
        cursor.execute(
            "INSERT INTO %s (c_float, c_double) VALUES (?, ?)" % datatype_table, (1.25, 2.5)
        )
        cursor.execute("SELECT c_float, c_double FROM %s" % datatype_table)
        row = cursor.fetchone()
        assert row is not None
        assert abs(float(row[0]) - 1.25) < 1e-9
        assert abs(float(row[1]) - 2.5) < 1e-9

    def test_varchar(self, cursor: Cursor, datatype_table: str) -> None:
        cursor.execute("INSERT INTO %s (c_varchar) VALUES (?)" % datatype_table, ("hello",))
        cursor.execute("SELECT c_varchar FROM %s" % datatype_table)
        assert cursor.fetchone() == ("hello",)

    def test_date(self, cursor: Cursor, datatype_table: str) -> None:
        value = datetime.date(2026, 1, 1)
        cursor.execute("INSERT INTO %s (c_date) VALUES (?)" % datatype_table, (value,))
        cursor.execute("SELECT c_date FROM %s" % datatype_table)
        row = cursor.fetchone()
        assert row is not None
        assert str(row[0]) == str(value)

    def test_time(self, cursor: Cursor, datatype_table: str) -> None:
        value = datetime.time(12, 30, 5)
        cursor.execute("INSERT INTO %s (c_time) VALUES (?)" % datatype_table, (value,))
        cursor.execute("SELECT c_time FROM %s" % datatype_table)
        row = cursor.fetchone()
        assert row is not None
        assert str(row[0]) == str(value)

    def test_datetime(self, cursor: Cursor, datatype_table: str) -> None:
        value = datetime.datetime(2026, 1, 1, 12, 30, 5, 123000)
        cursor.execute("INSERT INTO %s (c_datetime) VALUES (?)" % datatype_table, (value,))
        cursor.execute("SELECT c_datetime FROM %s" % datatype_table)
        row = cursor.fetchone()
        assert row is not None
        assert str(row[0]) == str(value)

    def test_null_value(self, cursor: Cursor, datatype_table: str) -> None:
        cursor.execute("INSERT INTO %s (c_nullable) VALUES (?)" % datatype_table, (None,))
        cursor.execute("SELECT c_nullable FROM %s" % datatype_table)
        assert cursor.fetchone() == (None,)


class TestBatchExecution:
    def test_executemany_batch(self, cursor: Cursor) -> None:
        table_name = _table_name("batch")
        try:
            _create_basic_table(cursor, table_name)
            cursor.executemany_batch(
                [
                    "INSERT INTO %s (name, val) VALUES ('batch1', 1)" % table_name,
                    "INSERT INTO %s (name, val) VALUES ('batch2', 2)" % table_name,
                    "INSERT INTO %s (name, val) VALUES ('batch3', 3)" % table_name,
                ]
            )
            cursor.execute("SELECT COUNT(*) FROM %s" % table_name)
            assert cursor.fetchone() == (3,)
        finally:
            _drop_table(cursor, table_name)


class TestIterator:
    def test_cursor_iterator(self, cursor: Cursor, test_table: str) -> None:
        cursor.executemany(
            "INSERT INTO %s (name, val) VALUES (?, ?)" % test_table,
            [("a", 1), ("b", 2), ("c", 3)],
        )
        cursor.execute("SELECT name FROM %s ORDER BY val" % test_table)
        assert [row for row in cursor] == [("a",), ("b",), ("c",)]


class TestErrorHandling:
    def test_syntax_error(self, cursor: Cursor) -> None:
        with pytest.raises((pycubrid.DatabaseError, pycubrid.ProgrammingError)):
            cursor.execute("SELEC FROM nowhere")

    def test_table_not_exists(self, cursor: Cursor) -> None:
        with pytest.raises((pycubrid.DatabaseError, pycubrid.ProgrammingError)):
            cursor.execute("SELECT * FROM %s" % _table_name("missing"))

    def test_duplicate_key(self, cursor: Cursor) -> None:
        table_name = _table_name("dup")
        try:
            cursor.execute(
                "CREATE TABLE IF NOT EXISTS %s (id INT PRIMARY KEY, name VARCHAR(100))" % table_name
            )
            cursor.execute("INSERT INTO %s (id, name) VALUES (?, ?)" % table_name, (1, "first"))
            with pytest.raises(pycubrid.IntegrityError):
                cursor.execute(
                    "INSERT INTO %s (id, name) VALUES (?, ?)" % table_name, (1, "second")
                )
        finally:
            _drop_table(cursor, table_name)
