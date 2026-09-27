"""cursor.description per-type metadata contract against live CUBRID (#398).

Validates the 7-tuple ``(name, type_code, display_size, internal_size,
precision, scale, null_ok)`` for each column type.  The official
CUBRID/cubrid-python ``tests3/test_description.py`` checks 18 types;
this suite mirrors those plus BIGINT and ENUM as pycubrid extensions.

Known divergences from CUBRIDdb are documented inline and asserted as
the current pycubrid behaviour so any future alignment is a deliberate
change, not a silent regression.
"""

from __future__ import annotations

import os
import uuid
from collections.abc import Generator

import pycubrid
import pytest
from pycubrid.connection import Connection
from pycubrid.cursor import Cursor

from ._parity_helpers import AsyncParityAdapter, ParityAdapter, SyncParityAdapter

TEST_HOST = os.environ.get("CUBRID_TEST_HOST", "localhost")
TEST_PORT = int(os.environ.get("CUBRID_TEST_PORT", "33000"))
TEST_DB = os.environ.get("CUBRID_TEST_DB", "testdb")
TEST_USER = os.environ.get("CUBRID_TEST_USER", "dba")
TEST_PASSWORD = os.environ.get("CUBRID_TEST_PASSWORD", "")


def _table_name() -> str:
    return "pycubrid_desc_%s" % uuid.uuid4().hex[:8]


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


@pytest.fixture
def desc_table(cursor: Cursor) -> Generator[str, None, None]:
    table = _table_name()
    cursor.execute(
        "CREATE TABLE %s ("
        "c_int INT,"
        "c_short SHORT,"
        "c_bigint BIGINT,"
        "c_numeric NUMERIC(15,0),"
        "c_float FLOAT,"
        "c_double DOUBLE,"
        "c_monetary MONETARY,"
        "c_date DATE,"
        "c_time TIME,"
        "c_datetime DATETIME,"
        "c_timestamp TIMESTAMP,"
        "c_bit BIT(8),"
        "c_varbit BIT VARYING(8),"
        "c_char CHAR(4),"
        "c_varchar VARCHAR(4),"
        "c_string STRING,"
        "c_set SET(INT),"
        "c_multiset MULTISET(INT),"
        "c_sequence SEQUENCE(INT),"
        "c_enum ENUM('a','b','c'),"
        "c_notnull INT NOT NULL DEFAULT 0,"
        "c_primary INT PRIMARY KEY"
        ")" % table
    )
    yield table
    cursor.execute("DROP TABLE IF EXISTS %s" % table)


class TestDescriptionStructure:
    """Every column's description must be a 7-tuple (PEP 249)."""

    def test_seven_element_tuples(self, cursor: Cursor, desc_table: str) -> None:
        cursor.execute("SELECT * FROM %s" % desc_table)
        assert cursor.description is not None
        for col in cursor.description:
            assert len(col) == 7, "cursor.description tuples must have 7 elements"

    def test_ddl_clears_description(self, cursor: Cursor) -> None:
        table = _table_name()
        try:
            cursor.execute("SELECT 1")
            assert cursor.description is not None
            cursor.execute("CREATE TABLE %s (id INT)" % table)
            assert cursor.description is None
        finally:
            cursor.execute("DROP TABLE IF EXISTS %s" % table)

    def test_display_size_and_internal_size_are_none(self, cursor: Cursor, desc_table: str) -> None:
        """pycubrid returns None for display_size and internal_size.

        CUBRIDdb returns 0 for both. This is a known divergence — PEP 249
        permits None for columns where the value is not applicable.
        """
        cursor.execute("SELECT c_int FROM %s" % desc_table)
        assert cursor.description is not None
        display_size = cursor.description[0][2]
        internal_size = cursor.description[0][3]
        assert display_size is None, "pycubrid returns None for display_size (CUBRIDdb: 0)"
        assert internal_size is None, "pycubrid returns None for internal_size (CUBRIDdb: 0)"


# Measured on CUBRID 10.2/11.4 for these exact declarations. Preserve pycubrid's
# None/None size fields and normalized collection codes, not CUBRIDdb's 0/0 and
# 32/64/96. Full native-call compatibility is a separate design decision (#438).
_DESCRIPTION_CASES = [
    # name, type code, precision, scale, null_ok
    ("c_int", 8, 10, 0, True),
    ("c_short", 9, 5, 0, True),
    ("c_bigint", 21, 19, 0, True),
    ("c_numeric", 7, 15, 0, True),
    ("c_float", 11, 7, 0, True),
    ("c_double", 12, 15, 0, True),
    ("c_monetary", 10, 15, 0, True),
    ("c_date", 13, 10, 0, True),
    ("c_time", 14, 8, 0, True),
    ("c_datetime", 22, 23, 3, True),
    ("c_timestamp", 15, 19, 0, True),
    ("c_bit", 5, 8, 0, True),
    ("c_varbit", 6, 8, 0, True),
    ("c_char", 1, 4, 0, True),
    ("c_varchar", 2, 4, 0, True),
    ("c_string", 2, 1073741823, 0, True),
    ("c_set", 16, 0, 0, True),
    ("c_multiset", 17, 0, 0, True),
    ("c_sequence", 18, 0, 0, True),
    ("c_enum", 25, 0, 0, True),
    ("c_notnull", 8, 10, 0, False),
    ("c_primary", 8, 10, 0, False),
]


class TestDescriptionTypeCode:
    """Validate every description field for each declared type."""

    @pytest.mark.parametrize("col,type_code,precision,scale,null_ok", _DESCRIPTION_CASES)
    def test_full_description(
        self,
        cursor: Cursor,
        desc_table: str,
        col: str,
        type_code: int,
        precision: int,
        scale: int,
        null_ok: bool,
    ) -> None:
        cursor.execute("SELECT %s FROM %s" % (col, desc_table))
        assert cursor.description is not None
        assert cursor.description == ((col, type_code, None, None, precision, scale, null_ok),)
        assert cursor.description[0][6] is null_ok

    def test_column_name(self, cursor: Cursor, desc_table: str) -> None:
        cursor.execute("SELECT c_int, c_varchar FROM %s" % desc_table)
        assert cursor.description is not None
        assert cursor.description[0][0] == "c_int"
        assert cursor.description[1][0] == "c_varchar"


class TestDescriptionPrecisionScale:
    """Validate precision (index 4) and scale (index 5) for key types."""

    def test_numeric_precision(self, cursor: Cursor, desc_table: str) -> None:
        cursor.execute("SELECT c_numeric FROM %s" % desc_table)
        assert cursor.description is not None
        precision = cursor.description[0][4]
        assert precision == 15

    def test_datetime_scale(self, cursor: Cursor, desc_table: str) -> None:
        cursor.execute("SELECT c_datetime FROM %s" % desc_table)
        assert cursor.description is not None
        scale = cursor.description[0][5]
        assert scale == 3  # milliseconds

    def test_int_precision(self, cursor: Cursor, desc_table: str) -> None:
        cursor.execute("SELECT c_int FROM %s" % desc_table)
        assert cursor.description is not None
        precision = cursor.description[0][4]
        assert precision == 10

    def test_varchar_precision(self, cursor: Cursor, desc_table: str) -> None:
        cursor.execute("SELECT c_varchar FROM %s" % desc_table)
        assert cursor.description is not None
        precision = cursor.description[0][4]
        assert precision == 4


class TestDescriptionNullable:
    """Validate null_ok (index 6) for both nullable and NOT NULL columns."""

    def test_nullable_column(self, cursor: Cursor, desc_table: str) -> None:
        cursor.execute("SELECT c_int FROM %s" % desc_table)
        assert cursor.description is not None
        assert cursor.description[0][6] is True

    def test_not_null_column(self, cursor: Cursor, desc_table: str) -> None:
        cursor.execute("SELECT c_notnull FROM %s" % desc_table)
        assert cursor.description is not None
        assert cursor.description[0][6] is False


class TestDescriptionEnum:
    """ENUM column description (pycubrid extension beyond official 18 types)."""

    def test_enum_type_code(self, cursor: Cursor) -> None:
        table = _table_name()
        try:
            cursor.execute("CREATE TABLE %s (e ENUM('a','b','c'))" % table)
            cursor.execute("SELECT e FROM %s" % table)
            assert cursor.description is not None
            assert cursor.description[0][1] == 25  # CUBRIDDataType.ENUM
        finally:
            cursor.execute("DROP TABLE IF EXISTS %s" % table)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "adapter", [SyncParityAdapter(), AsyncParityAdapter()], ids=["sync", "async"]
)
async def test_nullability_parity(adapter: ParityAdapter) -> None:
    conn = await adapter.connect()
    cur = adapter.cursor(conn)
    table = _table_name()
    try:
        await adapter.execute(
            cur, "CREATE TABLE %s (optional INT, required INT NOT NULL, id INT PRIMARY KEY)" % table
        )
        await adapter.execute(cur, "SELECT optional, required, id FROM %s" % table)
        assert cur.description == (
            ("optional", 8, None, None, 10, 0, True),
            ("required", 8, None, None, 10, 0, False),
            ("id", 8, None, None, 10, 0, False),
        )
        assert [item[6] for item in cur.description] == [True, False, False]
    finally:
        try:
            await adapter.execute(cur, "DROP TABLE IF EXISTS %s" % table)
        finally:
            try:
                await adapter.close_cursor(cur)
            finally:
                await adapter.close_connection(conn)
