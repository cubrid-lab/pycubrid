"""Differential tests: pycubrid vs the official CUBRIDdb driver (issue #344).

Runs identical DB-API workloads through pycubrid and the official C-extension
driver (``CUBRIDdb``) against the same live CUBRID server, using CUBRIDdb as an
independent implementation oracle. Each type's result is compared; a divergence
is NOT automatically a pycubrid bug, so every known divergence is classified
explicitly and asserted as such, while all other types must agree.

Classifications observed on CUBRID 11.2 / CUBRIDdb 11.3:

* ``MONETARY`` — DIVERGENCE (documented): pycubrid decodes to ``float``
  (value-preserving, e.g. ``99.99``); CUBRIDdb decodes to ``int`` (``99``),
  dropping the fractional part. pycubrid is the more faithful mapping here.

All other covered types agree in both Python type and value.

Skipped when either CUBRIDdb is not importable or no CUBRID server is reachable,
so it runs opportunistically (e.g. nightly / environments with the C extension)
without breaking the pure-Python offline suite.
"""

from __future__ import annotations

import sys
import uuid

import pytest

import pycubrid
from pycubrid.exceptions import Error as DBAPIError

from ._parity_helpers import TEST_DB, TEST_HOST, TEST_PASSWORD, TEST_PORT, TEST_USER, can_connect

CUBRIDdb = pytest.importorskip("CUBRIDdb", reason="official CUBRIDdb C-extension not installed")

pytestmark = pytest.mark.skipif(not can_connect(), reason="CUBRID instance not available")


def _py_conn() -> pycubrid.Connection:
    c = pycubrid.connect(
        host=TEST_HOST,
        port=TEST_PORT,
        database=TEST_DB,
        user=TEST_USER,
        password=TEST_PASSWORD,
    )
    c.autocommit = True
    return c


def _cdb_conn() -> object:
    url = "CUBRID:%s:%d:%s:::" % (TEST_HOST, TEST_PORT, TEST_DB)
    return CUBRIDdb.connect(url, TEST_USER, TEST_PASSWORD)


def _tbl() -> str:
    return "df_%s" % uuid.uuid4().hex[:8]


# (name, ddl, insert literal). Types that agree in both driver mappings.
_AGREE_CASES = [
    ("integer", "INTEGER", "42"),
    ("bigint", "BIGINT", "9223372036854775807"),
    ("numeric", "NUMERIC(10,2)", "12.34"),
    ("double", "DOUBLE", "3.14159"),
    ("char", "CHAR(5)", "'ab'"),
    ("varchar", "VARCHAR(20)", "'hello'"),
    ("date", "DATE", "DATE'2024-01-15'"),
    ("time", "TIME", "TIME'13:30:45'"),
    ("datetime", "DATETIME", "DATETIME'2024-01-15 13:30:45'"),
    ("json", "JSON", "'{\"a\": 1}'"),
]


@pytest.fixture(scope="module")
def drivers() -> tuple[pycubrid.Connection, object]:
    py = _py_conn()
    cdb = _cdb_conn()
    yield py, cdb
    py.close()
    cdb.close()


def _read_both(
    py: pycubrid.Connection, cdb: object, ddl: str, literal: str
) -> tuple[object, object]:
    table = _tbl()
    pc = py.cursor()
    pc.execute("CREATE TABLE %s (v %s)" % (table, ddl))
    pc.execute("INSERT INTO %s VALUES (%s)" % (table, literal))
    cdb.commit()
    try:
        pc.execute("SELECT v FROM %s" % table)
        py_val = pc.fetchone()[0]
        cc = cdb.cursor()
        cc.execute("SELECT v FROM %s" % table)
        cdb_val = cc.fetchone()[0]
        cc.close()
        return py_val, cdb_val
    finally:
        try:
            pc.execute("DROP TABLE IF EXISTS %s" % table)
        except DBAPIError:
            pass  # best-effort teardown
        pc.close()


class TestDriverDifferential:
    @pytest.mark.parametrize("case", _AGREE_CASES, ids=[c[0] for c in _AGREE_CASES])
    def test_types_agree(
        self, drivers: tuple[pycubrid.Connection, object], case: tuple[str, str, str]
    ) -> None:
        py, cdb = drivers
        _name, ddl, literal = case
        py_val, cdb_val = _read_both(py, cdb, ddl, literal)
        assert type(py_val) is type(cdb_val), (
            f"{_name}: pycubrid={type(py_val).__name__} cdb={type(cdb_val).__name__}"
        )
        assert py_val == cdb_val, f"{_name}: {py_val!r} != {cdb_val!r}"

    def test_monetary_divergence_is_documented(
        self, drivers: tuple[pycubrid.Connection, object]
    ) -> None:
        # DOCUMENTED IMPLEMENTATION DIFFERENCE (not a pycubrid bug): CUBRIDdb
        # decodes MONETARY to int (dropping the fraction); pycubrid decodes to
        # float and preserves it. Pin the divergence so a change on either side
        # is caught and re-triaged.
        py, cdb = drivers
        py_val, cdb_val = _read_both(py, cdb, "MONETARY", "99.99")
        assert isinstance(py_val, float) and py_val == 99.99
        assert isinstance(cdb_val, int) and cdb_val == 99


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
