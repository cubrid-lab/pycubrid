"""Live scalar binds through PREPARE + EXECUTE_ARRAY (#475, #478, #839)."""

from __future__ import annotations

import datetime
from collections.abc import Generator, Sequence
from typing import Any

import pytest

import pycubrid
from pycubrid.connection import Connection
from pycubrid.constants import CCIPrepareOption
from pycubrid.exceptions import DatabaseError, OperationalError, ProgrammingError
from pycubrid.protocol import (
    CloseQueryPacket,
    ExecuteArrayPacket,
    PreparePacket,
    _encode_prepared_scalar,
)

from ._parity_helpers import connect_kwargs, table_name

pytestmark = [pytest.mark.integration, pytest.mark.no_escape_pin]


@pytest.fixture
def admin() -> Generator[Connection, None, None]:
    connection = pycubrid.connect(**connect_kwargs(), autocommit=True)
    yield connection
    connection.close()


@pytest.fixture
def table(admin: Connection) -> Generator[str, None, None]:
    name = table_name("p475")
    try:
        yield name
    finally:
        with admin.cursor() as cursor:
            cursor.execute(f"DROP TABLE IF EXISTS {name}")


def _create(admin: Connection, table: str, columns: str) -> None:
    with admin.cursor() as cursor:
        cursor.execute(f"CREATE TABLE {table} ({columns})")


def _rows(admin: Connection, sql: str) -> list[tuple[Any, ...]]:
    with admin.cursor() as cursor:
        cursor.execute(sql)
        return [tuple(row) for row in cursor.fetchall()]


@pytest.mark.parametrize(
    ("column", "cast", "value"),
    [
        # Host variables are coerced to the target type, not parsed as SQL.
        ("INTEGER", "CAST(? AS INTEGER)", 2**40),
        ("DATETIME", "CAST(? AS DATETIME)", datetime.time(1, 2, 3)),
    ],
    ids=["bigint-into-int", "time-into-datetime"],
)
def test_host_variable_coercion_errors_keep_the_session(
    admin: Connection, table: str, column: str, cast: str, value: Any
) -> None:
    _create(admin, table, f"id INTEGER, v {column}")
    sql = f"INSERT INTO {table} VALUES (?, {cast})"
    with pycubrid.connect(**connect_kwargs(), autocommit=True) as conn:
        generation = conn._physical_generation
        with conn.cursor() as cursor:
            with pytest.raises(ProgrammingError) as caught:
                # The second row runs on the same handle after the server error.
                cursor.executemany(sql, [(1, value), (2, None)], prepared=True)
            assert isinstance(caught.value, DatabaseError)
            assert caught.value.errno == -494
            assert cursor._batch_row_results is not None
            assert cursor._batch_row_results[1] == (1, 0, "")
            # The session stays usable for a new statement too.
            cursor.executemany(sql, [(3, None)], prepared=True)
        assert conn._physical_generation == generation
    assert _rows(admin, f"SELECT id, v FROM {table} ORDER BY id") == [(2, None), (3, None)]


@pytest.mark.parametrize("autocommit", [False, True], ids=["manual", "auto"])
@pytest.mark.parametrize(
    ("column", "cast", "values"),
    [
        ("INTEGER", "CAST(? AS INTEGER)", (11, 12, None)),
        ("VARCHAR(30)", "CAST(? AS VARCHAR(30))", ("a'\\한", "", None)),
    ],
    ids=["int-null", "char-null"],
)
def test_scalar_binds_repeat_on_one_handle(
    admin: Connection,
    table: str,
    autocommit: bool,
    column: str,
    cast: str,
    values: Sequence[object],
) -> None:
    _create(admin, table, f"id INTEGER, v {column}")
    rows = list(enumerate(values))
    with pycubrid.connect(**connect_kwargs(), autocommit=autocommit) as conn:
        generation = conn._physical_generation
        with conn.cursor() as cursor:
            cursor.executemany(f"INSERT INTO {table} VALUES (?, {cast})", rows, prepared=True)
            assert cursor.rowcount == len(rows)
        assert conn._physical_generation == generation
        conn.commit()
    assert _rows(admin, f"SELECT id, v FROM {table} ORDER BY id") == rows


def test_dml_binds_repeat_without_implicit_manual_commit(admin: Connection, table: str) -> None:
    _create(admin, table, "id INTEGER")
    count = f"SELECT COUNT(*) FROM {table}"
    with pycubrid.connect(**connect_kwargs(), autocommit=False) as conn:
        cursor = conn.cursor()
        cursor.executemany(f"INSERT INTO {table} VALUES (?)", [(11,), (12,)], prepared=True)
        assert cursor.rowcount == 2
        assert _rows(admin, count) == [(0,)]
        # Releasing the handle does not commit either.
        cursor.close()
        assert _rows(admin, count) == [(0,)]
        conn.commit()
        assert _rows(admin, count) == [(2,)]


def test_prepared_generation_rejects_old_handle_after_live_reconnect(
    admin: Connection, table: str
) -> None:
    _create(admin, table, "id INTEGER")
    with pycubrid.connect(**connect_kwargs()) as conn:
        generation = conn._physical_generation
        prep = PreparePacket(
            f"INSERT INTO {table} VALUES (?)", prepare_flag=CCIPrepareOption.HOLDABLE
        )
        conn._send_and_receive(prep, expected_generation=generation)
        packet = ExecuteArrayPacket(
            prep.query_handle,
            ((_encode_prepared_scalar(42),),),
            auto_commit=False,
            bind_count=prep.bind_count,
            protocol_version=conn._protocol_version,
        )
        conn._send_and_receive(packet, expected_generation=generation)
        assert packet.row_results == ((1, 0, ""),)

        conn._drop_connection()
        healthy = conn.ping(reconnect=True)
        assert healthy is True
        assert conn._physical_generation == generation + 1
        with pytest.raises(OperationalError, match="earlier physical session"):
            conn._send_and_receive(
                CloseQueryPacket(prep.query_handle), expected_generation=generation
            )

        cursor = conn.cursor()
        try:
            cursor.execute("SELECT 1")
            row = cursor.fetchone()
            assert row == (1,)
        finally:
            cursor.close()
