"""Internal typed FC3 collection binds against a live broker; no public API (#482)."""

from __future__ import annotations

from collections.abc import Generator, Sequence
from typing import Any

import pytest

import pycubrid
from pycubrid.connection import Connection
from pycubrid.constants import CCIPrepareOption, CUBRIDDataType
from pycubrid.exceptions import DatabaseError
from pycubrid.protocol import (
    CloseQueryPacket,
    ExecutePacket,
    PreparePacket,
    _encode_prepared_collection,
    _encode_prepared_scalar,
    _PreparedCollection,
    _PreparedScalar,
)

from ._parity_helpers import connect_kwargs, table_name

pytestmark = pytest.mark.integration

_KINDS = {
    "set": CUBRIDDataType.SET,
    "multiset": CUBRIDDataType.MULTISET,
    "sequence": CUBRIDDataType.SEQUENCE,
}
_ELEMENTS = {
    "int": ("INTEGER", CUBRIDDataType.INT),
    "varchar": ("VARCHAR(20)", CUBRIDDataType.STRING),
}


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


class _Prepared:
    """One FC2 handle on the connection's current physical session."""

    def __init__(self, conn: Connection, sql: str) -> None:
        self.conn = conn
        self.generation = conn._physical_generation
        self.prep = PreparePacket(sql, auto_commit=True, prepare_flag=CCIPrepareOption.HOLDABLE)
        conn._send_and_receive(self.prep, expected_generation=self.generation)

    def execute(self, *bindings: _PreparedScalar | _PreparedCollection) -> ExecutePacket:
        packet = ExecutePacket(
            self.prep.query_handle,
            self.prep.statement_type,
            auto_commit=True,
            protocol_version=self.conn._protocol_version,
            decode_collections=True,
            bindings=bindings,
            bind_count=self.prep.bind_count,
        )
        packet.columns = self.prep.columns
        self.conn._send_and_receive(packet, expected_generation=self.generation)
        assert self.conn._physical_generation == self.generation
        return packet

    def close(self) -> None:
        self.conn._send_and_receive(
            CloseQueryPacket(self.prep.query_handle), expected_generation=self.generation
        )


def _stored(observer: Connection, table: str) -> list[tuple[int, Any]]:
    cursor = observer.cursor()
    try:
        cursor.execute(f"SELECT id, c FROM {table} ORDER BY id")
        return [(int(row[0]), row[1]) for row in cursor.fetchall()]
    finally:
        cursor.close()


def _expected(kind: str, values: Sequence[Any]) -> Any:
    if kind == "sequence":
        return list(values)
    if kind == "set":
        return frozenset(values)
    # CUBRID returns MULTISET elements sorted, with NULL last.
    return sorted(values, key=lambda item: (item is None, item))


# The MULTISET column is bound with the SEQUENCE kind: the broker rejects the
# MULTISET kind (see test_multiset_kind_is_rejected_by_the_broker) and SET
# would drop duplicates.
@pytest.mark.parametrize("element", list(_ELEMENTS))
@pytest.mark.parametrize(
    ("column", "kind"),
    [("set", "set"), ("multiset", "sequence"), ("sequence", "sequence")],
    ids=["set", "multiset", "sequence"],
)
def test_collection_binds_store_exact_values_on_one_handle(
    conn: Connection, observer: Connection, table: str, column: str, kind: str, element: str
) -> None:
    sql_type, element_type = _ELEMENTS[element]
    _sql(conn, f"CREATE TABLE {table} (id INTEGER, c {column.upper()}({sql_type}))")
    if element == "int":
        cases: list[tuple[Any, ...]] = [(3, 1, 3), (9, -2, 0), (), (None, 5), ("7", "-8")]
    else:
        cases = [("b", "a", "b"), ("한글", "x"), (), (None, "z"), ("",), ("NULL",)]
    prepared = _Prepared(conn, f"INSERT INTO {table} VALUES (?, ?)")
    try:
        for row_id, values in enumerate(cases):
            binding = _encode_prepared_collection(values, _KINDS[kind], element_type)
            packet = prepared.execute(_encode_prepared_scalar(row_id), binding)
            assert packet.total_tuple_count == 1
        # Whole SQL NULL goes through the existing scalar NULL path.
        prepared.execute(_encode_prepared_scalar(len(cases)), _encode_prepared_scalar(None))
    finally:
        prepared.close()

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
    # duplicates. The handle stays usable after the error.
    _sql(conn, f"CREATE TABLE {table} (id INTEGER, c MULTISET(INTEGER))")
    prepared = _Prepared(conn, f"INSERT INTO {table} VALUES (?, ?)")
    try:
        for values in ((3, 1, 3), ()):
            multiset = _encode_prepared_collection(
                values, CUBRIDDataType.MULTISET, CUBRIDDataType.INT
            )
            with pytest.raises(DatabaseError) as caught:
                prepared.execute(_encode_prepared_scalar(1), multiset)
            assert caught.value.errno == -454
        as_set = _encode_prepared_collection((3, 1, 3), CUBRIDDataType.SET, CUBRIDDataType.INT)
        prepared.execute(_encode_prepared_scalar(2), as_set)
    finally:
        prepared.close()
    assert _stored(observer, table) == [(2, [1, 3])]


def test_server_error_keeps_handle_reusable(
    conn: Connection, observer: Connection, table: str
) -> None:
    _sql(conn, f"CREATE TABLE {table} (id INTEGER, c SEQUENCE(INTEGER))")
    prepared = _Prepared(conn, f"INSERT INTO {table} VALUES (?, ?)")
    try:
        bad = _encode_prepared_collection(
            ("not a number",), CUBRIDDataType.SEQUENCE, CUBRIDDataType.STRING
        )
        with pytest.raises(DatabaseError):
            prepared.execute(_encode_prepared_scalar(1), bad)
        good = _encode_prepared_collection((4, 4, 2), CUBRIDDataType.SEQUENCE, CUBRIDDataType.INT)
        prepared.execute(_encode_prepared_scalar(2), good)
    finally:
        prepared.close()
    assert _stored(observer, table) == [(2, [4, 4, 2])]


def test_collection_bind_in_select_predicate(
    conn: Connection, observer: Connection, table: str
) -> None:
    _sql(conn, f"CREATE TABLE {table} (id INTEGER, c SET(INTEGER))")
    _sql(conn, f"INSERT INTO {table} VALUES (1, {{1, 2}}), (2, {{3}})")
    prepared = _Prepared(conn, f"SELECT id FROM {table} WHERE c SUBSETEQ ? ORDER BY id")
    try:
        for values, rows in (((1, 2, 3), [(1,), (2,)]), ((3,), [(2,)]), ((), [])):
            binding = _encode_prepared_collection(values, CUBRIDDataType.SET, CUBRIDDataType.INT)
            assert prepared.execute(binding).rows == rows
    finally:
        prepared.close()
    assert len(_stored(observer, table)) == 2
