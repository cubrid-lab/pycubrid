"""Internal typed FC3 LOB-handle binds against a live broker; no public API (#441)."""

from __future__ import annotations

from collections.abc import Generator
from typing import Any

import pytest

import pycubrid
from pycubrid.connection import Connection
from pycubrid.constants import CCIPrepareOption, CUBRIDDataType
from pycubrid.lob import Lob
from pycubrid.protocol import (
    CloseQueryPacket,
    ExecutePacket,
    PreparePacket,
    _encode_prepared_scalar,
    _packed_lob_size,
    _PreparedLob,
    _PreparedScalar,
)

from ._parity_helpers import connect_kwargs, table_name

pytestmark = pytest.mark.integration

BLOB, CLOB = CUBRIDDataType.BLOB, CUBRIDDataType.CLOB
# Above the broker's single LOB_READ reply cap (~80 KB, #362).
LARGE = 100_000


@pytest.fixture
def conn() -> Generator[Connection, None, None]:
    connection = pycubrid.connect(**connect_kwargs(), autocommit=True)
    yield connection
    connection.close()


@pytest.fixture
def observer() -> Generator[Connection, None, None]:
    connection = pycubrid.connect(**connect_kwargs(), autocommit=True)
    yield connection
    connection.close()


def _sql(conn: Connection, sql: str, params: Any = None) -> None:
    cursor = conn.cursor()
    try:
        cursor.execute(sql, params)
    finally:
        cursor.close()


@pytest.fixture
def table(conn: Connection) -> Generator[str, None, None]:
    name = table_name("p441")
    _sql(conn, f"CREATE TABLE {name} (id INT, b BLOB, c CLOB)")
    try:
        yield name
    finally:
        _sql(conn, f"DROP TABLE IF EXISTS {name}")


class _Prepared:
    """One FC2 handle on the connection's current physical session."""

    def __init__(self, conn: Connection, sql: str) -> None:
        self.conn = conn
        self.generation = conn._physical_generation
        self.prep = PreparePacket(sql, auto_commit=True, prepare_flag=CCIPrepareOption.HOLDABLE)
        conn._send_and_receive(self.prep, expected_generation=self.generation)

    def execute(self, *bindings: _PreparedScalar | _PreparedLob) -> ExecutePacket:
        packet = ExecutePacket(
            self.prep.query_handle,
            self.prep.statement_type,
            auto_commit=True,
            protocol_version=self.conn._protocol_version,
            bindings=bindings,
            bind_count=self.prep.bind_count,
        )
        packet.columns = self.prep.columns
        self.conn._send_and_receive(packet, expected_generation=self.generation)
        return packet

    def close(self) -> None:
        self.conn._send_and_receive(
            CloseQueryPacket(self.prep.query_handle), expected_generation=self.generation
        )


def _stored(observer: Connection, table: str, column: str) -> dict[int, tuple[int, bytes] | None]:
    """Read each row's stored LOB length and content through an ordinary ``Lob``."""
    cursor = observer.cursor()
    try:
        cursor.execute(f"SELECT id, {column} FROM {table} ORDER BY id")
        rows = cursor.fetchall()
    finally:
        cursor.close()
    result: dict[int, tuple[int, bytes] | None] = {}
    for row_id, cell in rows:
        if cell is None:
            result[row_id] = None
            continue
        lob = Lob(observer, cell["lob_type"], cell["packed_lob_handle"])
        # Read past the stored length to show nothing is hidden beyond it.
        result[row_id] = (cell["lob_length"], lob.read(cell["lob_length"] + 16))
    return result


def _written(conn: Connection, lob_type: int, chunks: list[tuple[int, bytes]]) -> Lob:
    lob = Lob.create(conn, lob_type)
    for offset, data in chunks:
        written = lob.write(data, offset)
        assert written == len(data)
    return lob


@pytest.mark.parametrize("size", [1, 1024, LARGE, 1024 * 1024], ids=["1b", "1kb", "100kb", "1mb"])
def test_written_blob_handle_binds_with_its_written_length(
    conn: Connection, observer: Connection, table: str, size: int
) -> None:
    data = bytes(range(256)) * (size // 256) + bytes(range(size % 256))
    # Several writes, as an application streaming a value would issue.
    step = max(1, size // 3)
    chunks = [(offset, data[offset : offset + step]) for offset in range(0, size, step)]
    lob = _written(conn, BLOB, chunks)
    assert _packed_lob_size(lob.lob_handle) == size
    insert = _Prepared(conn, f"INSERT INTO {table} (id, b) VALUES (?, ?)")
    try:
        packet = insert.execute(
            _encode_prepared_scalar(1),
            _PreparedLob(BLOB, lob.lob_handle, conn, insert.generation),
        )
        assert packet.total_tuple_count == 1
    finally:
        insert.close()
    assert _stored(observer, table, "b") == {1: (size, data)}


def test_written_clob_handle_keeps_utf8_cjk_text(
    conn: Connection, observer: Connection, table: str
) -> None:
    text = ("한글 CLOB ✓ " * 9000).encode("utf-8")
    assert len(text) > LARGE
    lob = _written(conn, CLOB, [(0, text[:50_000]), (50_000, text[50_000:])])
    insert = _Prepared(conn, f"INSERT INTO {table} (id, c) VALUES (?, ?)")
    try:
        insert.execute(
            _encode_prepared_scalar(1), _PreparedLob(CLOB, lob.lob_handle, conn, insert.generation)
        )
    finally:
        insert.close()
    assert _stored(observer, table, "c") == {1: (len(text), text)}


def test_rejected_write_leaves_the_handle_unchanged(conn: Connection) -> None:
    # The server only appends: a write at any offset other than the current
    # size (inside the value or past its end) fails, and the size field must
    # not move.
    lob = _written(conn, BLOB, [(0, b"0123456789")])
    before = lob.lob_handle
    for offset in (2, 11, 100):
        with pytest.raises(pycubrid.DatabaseError) as info:
            lob.write(b"ab", offset)
        assert info.value.errno == -1016
        assert lob.lob_handle == before


def test_stale_size_field_would_store_the_wrong_length(
    conn: Connection, observer: Connection, table: str
) -> None:
    """Why the size fix is mandatory: the broker trusts the handle's size field."""
    lob = _written(conn, BLOB, [(0, b"0123456789")])
    stale = lob.lob_handle[:4] + (0).to_bytes(8, "big") + lob.lob_handle[12:]
    insert = _Prepared(conn, f"INSERT INTO {table} (id, b) VALUES (?, ?)")
    try:
        insert.execute(
            _encode_prepared_scalar(1), _PreparedLob(BLOB, stale, conn, insert.generation)
        )
    finally:
        insert.close()
    stored = _stored(observer, table, "b")[1]
    assert stored is not None
    assert stored[0] == 0


def test_fetched_handles_copy_with_nulls_and_repeated_execution(
    conn: Connection, observer: Connection, table: str
) -> None:
    blob = bytes(range(256)) * 400  # > LARGE
    clob = "가나다 abc " * 10
    _sql(conn, f"INSERT INTO {table} VALUES (?, ?, ?)", (1, blob, clob))
    cursor = conn.cursor()
    try:
        cursor.execute(f"SELECT b, c FROM {table} WHERE id = 1")
        b_cell, c_cell = cursor.fetchone()
    finally:
        cursor.close()
    insert = _Prepared(conn, f"INSERT INTO {table} VALUES (?, ?, ?)")
    try:
        gen = insert.generation
        b = _PreparedLob(BLOB, b_cell["packed_lob_handle"], conn, gen)
        c = _PreparedLob(CLOB, c_cell["packed_lob_handle"], conn, gen)
        null = _encode_prepared_scalar(None)
        # One FC2 handle executed repeatedly with mixed LOB, NULL and INT binds.
        insert.execute(_encode_prepared_scalar(2), b, c)
        insert.execute(_encode_prepared_scalar(3), null, c)
        insert.execute(_encode_prepared_scalar(4), b, null)
    finally:
        insert.close()
    clob_bytes = clob.encode("utf-8")
    assert _stored(observer, table, "b") == {
        1: (len(blob), blob),
        2: (len(blob), blob),
        3: None,
        4: (len(blob), blob),
    }
    assert _stored(observer, table, "c") == {
        1: (len(clob_bytes), clob_bytes),
        2: (len(clob_bytes), clob_bytes),
        3: (len(clob_bytes), clob_bytes),
        4: None,
    }
    # Copies are independent of the source row.
    _sql(conn, f"DELETE FROM {table} WHERE id = 1")
    assert _stored(observer, table, "b")[2] == (len(blob), blob)


def test_server_error_after_a_fetched_handle_bind_leaves_it_reusable(
    conn: Connection, observer: Connection, table: str
) -> None:
    _sql(conn, f"INSERT INTO {table} (id, b) VALUES (?, ?)", (1, b"payload"))
    cursor = conn.cursor()
    try:
        cursor.execute(f"SELECT b FROM {table}")
        (cell,) = cursor.fetchone()
    finally:
        cursor.close()
    insert = _Prepared(conn, f"INSERT INTO {table} (id, b) VALUES (?, ?)")
    try:
        bound = _PreparedLob(BLOB, cell["packed_lob_handle"], conn, insert.generation)
        # A BLOB bound where an INTEGER is expected fails on the server (-494).
        with pytest.raises(pycubrid.ProgrammingError):
            insert.execute(bound, bound)
        insert.execute(_encode_prepared_scalar(5), bound)
    finally:
        insert.close()
    assert _stored(observer, table, "b") == {1: (7, b"payload"), 5: (7, b"payload")}


def test_created_handle_is_consumed_by_its_first_autocommit_statement(
    conn: Connection, observer: Connection, table: str
) -> None:
    """Measured server behavior: the statement takes over a new LOB's file.

    A fetched handle is copied (the tests above reuse one); a handle from
    LOB_NEW names a temporary file that its first autocommit statement takes
    over whether it succeeds or fails, so a second bind finds no file.
    """
    insert = _Prepared(conn, f"INSERT INTO {table} (id, b) VALUES (?, ?)")
    try:
        for fail_first in (False, True):
            lob = _written(conn, BLOB, [(0, b"temp")])
            bound = _PreparedLob(BLOB, lob.lob_handle, conn, insert.generation)
            if fail_first:
                with pytest.raises(pycubrid.ProgrammingError):
                    insert.execute(bound, bound)
            else:
                insert.execute(_encode_prepared_scalar(1), bound)
            with pytest.raises(pycubrid.DatabaseError) as info:
                insert.execute(_encode_prepared_scalar(2), bound)
            assert info.value.errno == -1016
            # The text after the code is the server's locale-dependent strerror.
            text = str(info.value)
            assert "No such file" in text or "external storage" in text
    finally:
        insert.close()
    assert _stored(observer, table, "b") == {1: (4, b"temp")}
