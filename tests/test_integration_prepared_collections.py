"""Live typed collection binds through PREPARE + EXECUTE_ARRAY (#482, #839)."""

from __future__ import annotations

from collections.abc import Generator
from collections.abc import Sequence as AbcSequence
from typing import Any

import pytest

import pycubrid
from pycubrid.connection import Connection
from pycubrid.constants import CCIPrepareOption, CUBRIDDataType
from pycubrid.exceptions import DatabaseError
from pycubrid.protocol import (
    CloseQueryPacket,
    ExecuteArrayPacket,
    PreparePacket,
    _encode_prepared_collection,
    _encode_prepared_scalar,
)
from pycubrid.types import Multiset, Sequence, Set

from ._parity_helpers import connect_kwargs, table_name

pytestmark = pytest.mark.integration

_KINDS = {"set": Set, "multiset": Multiset, "sequence": Sequence}
_ELEMENTS = {"int": "INTEGER", "varchar": "VARCHAR(20)"}


@pytest.fixture
def conn() -> Generator[Connection, None, None]:
    connection = pycubrid.connect(**connect_kwargs(), autocommit=True)
    yield connection
    connection.close()


@pytest.fixture
def observer() -> Generator[Connection, None, None]:
    connection = pycubrid.connect(**connect_kwargs(), autocommit=True, decode_collections=True)
    yield connection
    connection.close()


def _sql(conn: Connection, sql: str) -> None:
    cursor = conn.cursor()
    try:
        cursor.execute(sql)
    finally:
        cursor.close()


@pytest.fixture
def table(conn: Connection) -> Generator[str, None, None]:
    name = table_name("p482")
    try:
        yield name
    finally:
        # The test body creates the table; drop it whether or not it exists.
        _sql(conn, f"DROP TABLE IF EXISTS {name}")


def _stored(observer: Connection, table: str) -> list[tuple[int, Any]]:
    cursor = observer.cursor()
    try:
        cursor.execute(f"SELECT id, c FROM {table} ORDER BY id")
        return [(int(row[0]), row[1]) for row in cursor.fetchall()]
    finally:
        cursor.close()


def _expected(kind: str, values: AbcSequence[Any]) -> Any:
    if kind == "sequence":
        return list(values)
    if kind == "set":
        return frozenset(values)
    # CUBRID returns MULTISET elements sorted, with NULL last.
    return sorted(values, key=lambda item: (item is None, item))


@pytest.mark.parametrize("element", list(_ELEMENTS))
@pytest.mark.parametrize("column", list(_KINDS))
def test_collection_binds_store_exact_values_on_one_handle(
    conn: Connection, observer: Connection, table: str, column: str, element: str
) -> None:
    _sql(conn, f"CREATE TABLE {table} (id INTEGER, c {column.upper()}({_ELEMENTS[element]}))")
    if element == "int":
        cases: list[tuple[Any, ...]] = [(3, 1, 3), (9, -2, 0), (), (None, 5), ("7", "-8")]
    else:
        cases = [("b", "a", "b"), ("한글", "x"), (), (None, "z"), ("",), ("NULL",)]
    rows: list[tuple[Any, Any]] = [
        (row_id, _KINDS[column](values)) for row_id, values in enumerate(cases)
    ]
    # Whole SQL NULL goes through the existing scalar NULL path.
    rows.append((len(cases), None))
    with conn.cursor() as cursor:
        cursor.executemany(f"INSERT INTO {table} VALUES (?, ?)", rows, prepared=True)
        assert cursor.rowcount == len(rows)

    expected: list[tuple[int, Any]] = []
    for row_id, values in enumerate(cases):
        if element == "int":
            values = tuple(None if item is None else int(item) for item in values)
        expected.append((row_id, _expected(column, values)))
    expected.append((len(cases), None))
    assert _stored(observer, table) == expected


def test_multiset_kind_is_rejected_by_the_broker(
    conn: Connection, observer: Connection, table: str
) -> None:
    # cas_execute.c builds the MULTISET with db_set_create_multi() but wraps it
    # with db_make_set(), which accepts only a SET: ER_QPROC_INVALID_DATATYPE
    # (-454) on 10.2 and 11.4. A SET kind into a MULTISET column drops
    # duplicates. The handle stays usable after the error. The public path
    # sends a Multiset as SEQUENCE for this reason, so these packets are direct.
    _sql(conn, f"CREATE TABLE {table} (id INTEGER, c MULTISET(INTEGER))")
    generation = conn._physical_generation
    prep = PreparePacket(
        f"INSERT INTO {table} VALUES (?, ?)",
        auto_commit=True,
        prepare_flag=CCIPrepareOption.HOLDABLE,
    )
    conn._send_and_receive(prep, expected_generation=generation)

    def execute(binding: Any) -> tuple[int, int, str]:
        packet = ExecuteArrayPacket(
            prep.query_handle,
            ((_encode_prepared_scalar(1), binding),),
            auto_commit=True,
            bind_count=prep.bind_count,
            protocol_version=conn._protocol_version,
        )
        conn._send_and_receive(packet, expected_generation=generation)
        assert conn._physical_generation == generation
        return packet.row_results[0]

    try:
        for values in ((3, 1, 3), ()):
            multiset = _encode_prepared_collection(
                values, CUBRIDDataType.MULTISET, CUBRIDDataType.INT
            )
            # FC21 fails the whole request on this bind, with no per-row result.
            with pytest.raises(DatabaseError) as caught:
                execute(multiset)
            assert caught.value.errno == -454
        as_set = _encode_prepared_collection((3, 1, 3), CUBRIDDataType.SET, CUBRIDDataType.INT)
        assert execute(as_set)[:2] == (1, 0)
    finally:
        conn._send_and_receive(CloseQueryPacket(prep.query_handle), expected_generation=generation)
    assert _stored(observer, table) == [(1, [1, 3])]


def test_server_error_keeps_handle_reusable(
    conn: Connection, observer: Connection, table: str
) -> None:
    _sql(conn, f"CREATE TABLE {table} (id INTEGER, c SEQUENCE(INTEGER))")
    with conn.cursor() as cursor:
        with pytest.raises(DatabaseError):
            cursor.executemany(
                f"INSERT INTO {table} VALUES (?, ?)",
                [(1, Sequence(("not a number",))), (2, Sequence((4, 4, 2)))],
                prepared=True,
            )
    assert _stored(observer, table) == [(2, [4, 4, 2])]


def test_collection_bind_in_a_predicate(conn: Connection, observer: Connection, table: str) -> None:
    _sql(conn, f"CREATE TABLE {table} (id INTEGER, c SET(INTEGER), hits INTEGER)")
    _sql(conn, f"INSERT INTO {table} VALUES (1, {{1, 2}}, 0), (2, {{3}}, 0)")
    with conn.cursor() as cursor:
        cursor.executemany(
            f"UPDATE {table} SET hits = hits + 1 WHERE c SUBSETEQ ?",
            [(Set((1, 2, 3)),), (Set((3,)),), (Set(()),)],
            prepared=True,
        )
        assert cursor._batch_row_results is not None
        assert [count for count, _code, _text in cursor._batch_row_results] == [2, 1, 0]
        cursor.execute(f"SELECT id, hits FROM {table} ORDER BY id")
        assert [tuple(row) for row in cursor.fetchall()] == [(1, 1), (2, 2)]
    assert len(_stored(observer, table)) == 2
