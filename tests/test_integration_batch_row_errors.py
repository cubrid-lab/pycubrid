"""Live per-row outcome of a failed ``executemany()`` batch (#841).

The #836 probes found that with row 3 of 5 failing, CUBRID 10.2-11.4 still
applies rows 1, 2, 4 and 5. These checks pin what the raised exception now
reports about each row, on the literal and the prepared batch, sync and async,
and that ``rowcount`` stays ``-1``.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Generator
from typing import Any

import pytest

import pycubrid
import pycubrid.aio
from pycubrid.connection import Connection
from pycubrid.exceptions import DatabaseError, IntegrityError, ProgrammingError

from ._parity_helpers import connect_kwargs, table_name

pytestmark = [pytest.mark.integration, pytest.mark.no_escape_pin]

AUTOCOMMIT = pytest.mark.parametrize("autocommit", [False, True], ids=["manual", "autocommit"])
PREPARED = pytest.mark.parametrize("prepared", [False, True], ids=["literal", "prepared"])


@pytest.fixture
def admin() -> Generator[Connection, None, None]:
    connection = pycubrid.connect(**connect_kwargs(), autocommit=True)
    yield connection
    connection.close()


@pytest.fixture
def table(admin: Connection) -> Generator[str, None, None]:
    name = table_name("b841")
    with admin.cursor() as cursor:
        cursor.execute(f"CREATE TABLE {name} (id INT PRIMARY KEY, k INT)")
        cursor.execute(f"INSERT INTO {name} (id) VALUES (3)")
        cursor.execute(f"INSERT INTO {name} (id) VALUES (5)")
    try:
        yield name
    finally:
        with admin.cursor() as cursor:
            cursor.execute(f"DROP TABLE IF EXISTS {name}")


def _ids(admin: Connection, table: str) -> list[int]:
    with admin.cursor() as cursor:
        cursor.execute(f"SELECT id FROM {table} ORDER BY id")
        return [row[0] for row in cursor.fetchall()]


def _check(exc: DatabaseError) -> None:
    # Rows at positions 2 (id 3) and 4 (id 5) collide with the seeded keys.
    assert type(exc) is IntegrityError
    assert exc.errno == -670
    assert exc.sqlstate == "23000"
    assert exc.batch_index == 2
    assert exc.batch_errors is not None
    assert [(i, type(e), e.errno) for i, e in exc.batch_errors] == [
        (2, IntegrityError, -670),
        (4, IntegrityError, -670),
    ]
    assert exc.batch_rowcounts == (1, 1, -1, 1, -1, 1)


@AUTOCOMMIT
@PREPARED
def test_failed_rows_are_reported_in_order(
    admin: Connection, table: str, autocommit: bool, prepared: bool
) -> None:
    rows = [(i,) for i in (1, 2, 3, 4, 5, 6)]
    with contextlib.closing(pycubrid.connect(**connect_kwargs(), autocommit=autocommit)) as conn:
        with conn.cursor() as cursor:
            with pytest.raises(IntegrityError) as raised:
                cursor.executemany(f"INSERT INTO {table} (id) VALUES (?)", rows, prepared=prepared)
            assert cursor.rowcount == -1
            _check(raised.value)
        conn.commit()
    assert _ids(admin, table) == [1, 2, 3, 4, 5, 6]


@PREPARED
def test_same_first_error_as_before(admin: Connection, table: str, prepared: bool) -> None:
    # The raised error is the one a single failing INSERT of that row gives.
    with admin.cursor() as cursor:
        with pytest.raises(IntegrityError) as single:
            cursor.execute(f"INSERT INTO {table} (id) VALUES (3)")
        with pytest.raises(IntegrityError) as batch:
            cursor.executemany(
                f"INSERT INTO {table} (id) VALUES (?)", [(7,), (3,)], prepared=prepared
            )
    assert batch.value.errno == single.value.errno
    assert batch.value.sqlstate == single.value.sqlstate
    assert batch.value.batch_index == 1
    assert batch.value.batch_rowcounts == (1, -1)


def test_executemany_batch_reports_statement_positions(admin: Connection, table: str) -> None:
    with admin.cursor() as cursor:
        with pytest.raises(DatabaseError) as raised:
            cursor.executemany_batch(
                [
                    f"UPDATE {table} SET k = 1 WHERE id = 3",
                    f"INSERT INTO {table} (id) VALUES (5)",
                    f"UPDATE {table} SET k = 2",
                    f"INSERT INTO {table} (id, k) VALUES (9, 'abc')",
                ]
            )
        assert cursor.rowcount == -1
    exc = raised.value
    assert type(exc) is IntegrityError
    assert exc.batch_index == 1
    assert exc.batch_errors is not None
    assert [i for i, _e in exc.batch_errors] == [1, 3]
    # 'abc' into INT is a semantic error (-494) on CUBRID 10.2-11.4.
    assert type(exc.batch_errors[1][1]) is ProgrammingError
    assert exc.batch_errors[1][1].errno == -494
    assert exc.batch_rowcounts == (1, -1, 2, -1)


@AUTOCOMMIT
@PREPARED
def test_async_cursor(admin: Connection, table: str, autocommit: bool, prepared: bool) -> None:
    async def run() -> Any:
        conn = await pycubrid.aio.connect(**connect_kwargs(), autocommit=autocommit)
        try:
            cursor = conn.cursor()
            with pytest.raises(IntegrityError) as raised:
                await cursor.executemany(
                    f"INSERT INTO {table} (id) VALUES (?)",
                    [(i,) for i in (1, 2, 3, 4, 5, 6)],
                    prepared=prepared,
                )
            assert cursor.rowcount == -1
            await conn.commit()
            await cursor.close()
            return raised.value
        finally:
            await conn.close()

    _check(asyncio.run(run()))
    assert _ids(admin, table) == [1, 2, 3, 4, 5, 6]
