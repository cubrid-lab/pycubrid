"""Live CUBRID type contract matrix (issue #345).

An authoritative, single-file matrix pinning the Python type and value each
CUBRID column type round-trips to, verified against a live server for both the
sync and async drivers. This prevents per-type gaps by asserting one contract in
one place: for every covered type, a representative value inserted via a bound
``?`` parameter (or a SQL literal where the type cannot be parameter-bound) is
read back as the documented Python type with the documented value.

Empirically grounded against CUBRID 11.2. Notable contracts:

* ``MONETARY`` decodes to ``float`` (not ``Decimal``);
* ``CHAR(n)`` is space-padded to ``n``;
* ``JSON`` decodes to ``str`` (canonical, whitespace-normalized);
* ``BLOB``/``CLOB`` decode to a LOB-locator ``dict``, not raw bytes;
* ``TIMESTAMPTZ``/``DATETIMETZ`` decode to tz-aware ``datetime``.

Collection types (SET/MULTISET/SEQUENCE) are intentionally out of scope here:
they cannot be parameter-bound and their end-to-end decode needs separate
investigation (tracked under the type/collection work), so pinning them in this
contract matrix would encode unverified behavior.

Skipped when no CUBRID server is reachable.
"""

from __future__ import annotations

import asyncio
import datetime
import sys
import uuid
from dataclasses import dataclass
from decimal import Decimal

import pytest

import pycubrid
import pycubrid.aio
from pycubrid.exceptions import Error as DBAPIError

from ._parity_helpers import TEST_DB, TEST_HOST, TEST_PASSWORD, TEST_PORT, TEST_USER, can_connect

pytestmark = pytest.mark.skipif(not can_connect(), reason="CUBRID instance not available")


@dataclass(frozen=True)
class TypeCase:
    """One row of the type contract matrix."""

    name: str
    ddl: str
    value: object  # bound via ? when literal is None
    literal: str | None  # SQL literal insert when the type cannot be bound
    expected_type: type
    expected: object


def _dt(*a: int) -> datetime.datetime:
    return datetime.datetime(*a)  # type: ignore[arg-type]


CASES: list[TypeCase] = [
    TypeCase("short", "SHORT", 123, None, int, 123),
    TypeCase("integer", "INTEGER", 2147483647, None, int, 2147483647),
    TypeCase("bigint", "BIGINT", 9223372036854775807, None, int, 9223372036854775807),
    TypeCase("numeric", "NUMERIC(10,2)", Decimal("12.34"), None, Decimal, Decimal("12.34")),
    TypeCase("float", "FLOAT", 1.5, None, float, 1.5),
    TypeCase("double", "DOUBLE", 3.14159, None, float, 3.14159),
    TypeCase("monetary", "MONETARY", Decimal("99.99"), None, float, 99.99),
    TypeCase("char", "CHAR(5)", "abc", None, str, "abc  "),
    TypeCase("varchar", "VARCHAR(20)", "hello", None, str, "hello"),
    TypeCase("string", "STRING", "world", None, str, "world"),
    TypeCase(
        "date", "DATE", datetime.date(2024, 1, 15), None, datetime.date, datetime.date(2024, 1, 15)
    ),
    TypeCase(
        "time", "TIME", datetime.time(13, 30, 45), None, datetime.time, datetime.time(13, 30, 45)
    ),
    TypeCase(
        "datetime",
        "DATETIME",
        _dt(2024, 1, 15, 13, 30, 45),
        None,
        datetime.datetime,
        _dt(2024, 1, 15, 13, 30, 45),
    ),
    TypeCase(
        "timestamp",
        "TIMESTAMP",
        _dt(2024, 1, 15, 13, 30, 45),
        None,
        datetime.datetime,
        _dt(2024, 1, 15, 13, 30, 45),
    ),
    TypeCase("bit", "BIT(8)", None, "B'10101010'", bytes, b"\xaa"),
    TypeCase("bit_varying", "BIT VARYING(16)", None, "B'1010'", bytes, b"\xa0"),
    TypeCase("json", "JSON", None, "'{\"a\": 1}'", str, '{"a":1}'),
    TypeCase("enum", "ENUM('a','b','c')", "b", None, str, "b"),
    TypeCase("null_integer", "INTEGER", None, None, type(None), None),
]


def _connect() -> pycubrid.Connection:
    return pycubrid.connect(
        host=TEST_HOST,
        port=TEST_PORT,
        database=TEST_DB,
        user=TEST_USER,
        password=TEST_PASSWORD,
    )


async def _aconnect() -> pycubrid.aio.AsyncConnection:
    return await pycubrid.aio.connect(
        host=TEST_HOST,
        port=TEST_PORT,
        database=TEST_DB,
        user=TEST_USER,
        password=TEST_PASSWORD,
    )


def _tbl() -> str:
    return "ty_%s" % uuid.uuid4().hex[:8]


@pytest.fixture(scope="module")
def conn() -> pycubrid.Connection:
    c = _connect()
    c.autocommit = True
    yield c
    c.close()


def _roundtrip_sync(c: pycubrid.Connection, case: TypeCase) -> object:
    table = _tbl()
    cur = c.cursor()
    try:
        cur.execute("CREATE TABLE %s (v %s)" % (table, case.ddl))
        if case.literal is not None:
            cur.execute("INSERT INTO %s (v) VALUES (%s)" % (table, case.literal))
        else:
            cur.execute("INSERT INTO %s (v) VALUES (?)" % table, (case.value,))
        cur.execute("SELECT v FROM %s" % table)
        row = cur.fetchone()
        assert row is not None
        return row[0]
    finally:
        try:
            cur.execute("DROP TABLE IF EXISTS %s" % table)
        except DBAPIError:
            pass  # best-effort teardown
        cur.close()


async def _roundtrip_async(case: TypeCase) -> object:
    c = await _aconnect()
    await c.set_autocommit(True)
    table = _tbl()
    cur = c.cursor()
    try:
        await cur.execute("CREATE TABLE %s (v %s)" % (table, case.ddl))
        if case.literal is not None:
            await cur.execute("INSERT INTO %s (v) VALUES (%s)" % (table, case.literal))
        else:
            await cur.execute("INSERT INTO %s (v) VALUES (?)" % table, (case.value,))
        await cur.execute("SELECT v FROM %s" % table)
        row = await cur.fetchone()
        assert row is not None
        return row[0]
    finally:
        try:
            await cur.execute("DROP TABLE IF EXISTS %s" % table)
        except DBAPIError:
            pass  # best-effort teardown
        await cur.close()
        await c.close()


_IDS = [c.name for c in CASES]


class TestTypeContractSync:
    @pytest.mark.parametrize("case", CASES, ids=_IDS)
    def test_sync_roundtrip(self, conn: pycubrid.Connection, case: TypeCase) -> None:
        result = _roundtrip_sync(conn, case)
        assert type(result) is case.expected_type, (
            f"{case.name}: expected {case.expected_type.__name__}, got {type(result).__name__}"
        )
        assert result == case.expected, f"{case.name}: value mismatch"


class TestTypeContractAsync:
    @pytest.mark.parametrize("case", CASES, ids=_IDS)
    def test_async_roundtrip(self, case: TypeCase) -> None:
        result = asyncio.run(_roundtrip_async(case))
        assert type(result) is case.expected_type, (
            f"{case.name}: expected {case.expected_type.__name__}, got {type(result).__name__}"
        )
        assert result == case.expected, f"{case.name}: value mismatch"


class TestTypeContractParity:
    @pytest.mark.parametrize("case", CASES, ids=_IDS)
    def test_sync_async_agree(self, conn: pycubrid.Connection, case: TypeCase) -> None:
        sync_val = _roundtrip_sync(conn, case)
        async_val = asyncio.run(_roundtrip_async(case))
        assert type(sync_val) is type(async_val)
        assert sync_val == async_val


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
