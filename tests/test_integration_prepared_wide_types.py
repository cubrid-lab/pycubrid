"""Live round trips for the wider prepared types against the literal path (#840)."""

from __future__ import annotations

import datetime
from collections.abc import Generator
from decimal import Decimal
from typing import Any
from zoneinfo import ZoneInfo

import pytest

import pycubrid
from pycubrid.compat import native
from pycubrid.connection import Connection
from pycubrid.exceptions import DatabaseError, ProgrammingError

from ._cubrid_endpoint import TEST_DB, TEST_HOST, TEST_PASSWORD, TEST_PORT, TEST_USER
from ._parity_helpers import connect_kwargs, table_name

pytestmark = [pytest.mark.integration, pytest.mark.no_escape_pin]

_NEW_YORK = ZoneInfo("America/New_York")
_COLUMNS = (
    ("b", "INTEGER"),
    ("bi", "BIGINT"),
    ("big", "NUMERIC(38,0)"),
    ("d", "DOUBLE"),
    ("n", "NUMERIC(38,10)"),
    ("frac", "NUMERIC(38,38)"),
    ("dt", "DATE"),
    ("tm", "TIME"),
    ("dtm", "DATETIME"),
    ("dtz", "DATETIMETZ"),
    ("vb", "BIT VARYING(64)"),
)
# One row per case; every column is bound in each row.
_ROWS: tuple[tuple[Any, ...], ...] = (
    (
        True,
        2**63 - 1,
        10**38 - 1,
        0.1,
        Decimal("-123.4500000000"),
        Decimal("0." + "1" * 38),
        datetime.date(1, 1, 1),
        datetime.time(0, 0, 0),
        datetime.datetime(2024, 2, 29, 23, 59, 59, 999999),
        datetime.datetime(2024, 1, 2, 3, 4, 5, 6000, tzinfo=ZoneInfo("Asia/Seoul")),
        b"\x00\xff",
    ),
    (
        False,
        -(2**63),
        -(2**63) - 1,
        -1e300,
        Decimal("0.5"),
        Decimal("-0.00000000000000000000000000000000000001"),
        datetime.date(9999, 12, 31),
        datetime.time(23, 59, 59, 999999),
        datetime.datetime(1, 1, 1, 0, 0, 0),
        # The second 01:30 of the repeated hour (EST), not the first (EDT).
        datetime.datetime(2024, 11, 3, 1, 30, tzinfo=_NEW_YORK, fold=1),
        b"\x01",
    ),
    (
        True,
        2**31,
        2**64,
        -0.0,
        Decimal("1E+5"),
        Decimal("0E-38"),
        datetime.date(2024, 2, 29),
        datetime.time(12, 0, 1),
        datetime.datetime(2024, 1, 1, 0, 0, 0, 1000),
        datetime.datetime(
            2024, 6, 1, 12, 0, tzinfo=datetime.timezone(-datetime.timedelta(hours=5, minutes=30))
        ),
        bytearray(b"pycubrid"),
    ),
    (None,) * 11,
)


def _native() -> native.connection:
    return native.connect(f"CUBRID:{TEST_HOST}:{TEST_PORT}:{TEST_DB}:::", TEST_USER, TEST_PASSWORD)


@pytest.fixture
def ordinary() -> Generator[Connection, None, None]:
    connection = pycubrid.connect(**connect_kwargs(), autocommit=True)
    yield connection
    connection.close()


@pytest.fixture
def table(ordinary: Connection) -> Generator[str, None, None]:
    name = table_name("p840")
    columns = ", ".join(f"{column} {sql_type}" for column, sql_type in _COLUMNS)
    with ordinary.cursor() as cursor:
        cursor.execute(f"CREATE TABLE {name} (id INTEGER, path VARCHAR(8), {columns})")
    try:
        yield name
    finally:
        with ordinary.cursor() as cursor:
            cursor.execute(f"DROP TABLE IF EXISTS {name}")


def _stored(ordinary: Connection, table: str, path: str) -> list[tuple[Any, ...]]:
    names = ", ".join(column for column, _sql_type in _COLUMNS)
    with ordinary.cursor() as cursor:
        cursor.execute(f"SELECT id, {names} FROM {table} WHERE path = ? ORDER BY id", (path,))
        return [tuple(row) for row in cursor.fetchall()]


def test_prepared_binds_store_the_same_values_as_literals(ordinary: Connection, table: str) -> None:
    markers = ", ".join("?" * (len(_COLUMNS) + 2))
    sql = f"INSERT INTO {table} VALUES ({markers})"
    with ordinary.cursor() as cursor:
        for row_id, row in enumerate(_ROWS):
            cursor.execute(sql, (row_id, "literal", *row))

    conn = _native()
    try:
        cur = conn.cursor()
        try:
            cur.prepare(sql)
            for row_id, row in enumerate(_ROWS):
                for index, value in enumerate((row_id, "prepared", *row), 1):
                    cur.bind_param(index, value)
                assert cur.execute() == 1
        finally:
            cur.close()
    finally:
        conn.close()

    literal = _stored(ordinary, table, "literal")
    prepared = _stored(ordinary, table, "prepared")
    assert prepared == literal
    assert len(prepared) == len(_ROWS)
    # Spot-check the values themselves, not only the parity.
    first = prepared[0]
    assert first[1:4] == (1, 2**63 - 1, Decimal(10**38 - 1))
    assert first[6] == Decimal("0." + "1" * 38)
    assert first[9] == datetime.datetime(2024, 2, 29, 23, 59, 59, 999000)
    assert first[11] == b"\x00\xff"
    repeated = prepared[1][10]
    assert repeated is not None
    assert repeated.utcoffset() == datetime.timedelta(hours=-5)


@pytest.mark.parametrize(
    "value",
    [
        True,
        2**40,
        Decimal("-0.5"),
        Decimal("0.0"),
        Decimal("-0.00"),
        datetime.date(2024, 2, 29),
        datetime.time(1, 2, 3),
        datetime.datetime(2024, 2, 29, 1, 2, 3, 4000),
        datetime.datetime(2024, 2, 29, 1, 2, 3, tzinfo=ZoneInfo("Asia/Seoul")),
        b"\x01\x02",
    ],
    ids=repr,
)
def test_untyped_marker_returns_the_bound_type(value: Any) -> None:
    conn = _native()
    try:
        cur = conn.cursor()
        try:
            cur.prepare("SELECT ?")
            cur.bind_param(1, value)
            assert cur.execute() == 1
            (fetched,) = cur.fetch_row()
        finally:
            cur.close()
    finally:
        conn.close()
    expected = int(value) if isinstance(value, bool) else value
    if isinstance(value, datetime.datetime):
        expected = value.replace(microsecond=value.microsecond // 1000 * 1000)
    assert fetched == expected
    assert type(fetched) is type(expected)


@pytest.mark.parametrize(
    ("sql", "value"),
    [
        # Host variables are coerced to the target type, not parsed as SQL.
        ("SELECT CAST(? AS INTEGER)", 2**40),
        ("SELECT CAST(? AS DATETIME)", datetime.time(1, 2, 3)),
    ],
    ids=["bigint-into-int", "time-into-datetime"],
)
def test_host_variable_coercion_errors_are_server_errors(sql: str, value: Any) -> None:
    conn = _native()
    try:
        cur = conn.cursor()
        try:
            cur.prepare(sql)
            cur.bind_param(1, value)
            with pytest.raises(ProgrammingError) as caught:
                cur.execute()
            assert isinstance(caught.value, DatabaseError)
            assert caught.value.errno == -494
            # The cursor stays usable after the server error.
            cur.bind_param(1, None)
            assert cur.execute() == 1
        finally:
            cur.close()
    finally:
        conn.close()
