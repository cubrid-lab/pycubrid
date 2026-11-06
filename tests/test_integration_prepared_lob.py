"""Live typed LOB-handle binds through PREPARE + EXECUTE_ARRAY (#441, #839)."""

from __future__ import annotations

import contextlib
from collections.abc import Generator
from typing import Any

import pytest

import pycubrid
from pycubrid.connection import Connection
from pycubrid.constants import CCIPrepareOption, CUBRIDDataType
from pycubrid.lob import Lob
from pycubrid.protocol import (
    CloseQueryPacket,
    ExecuteArrayPacket,
    PreparePacket,
    _encode_prepared_scalar,
    _packed_lob_size,
    _PreparedLob,
)

from ._parity_helpers import connect_kwargs, table_name

pytestmark = pytest.mark.integration

BLOB, CLOB = CUBRIDDataType.BLOB, CUBRIDDataType.CLOB
# Above the broker's single LOB_READ reply cap (~80 KB, #362).
LARGE = 100_000


@pytest.fixture
def conn() -> Generator[Connection, None, None]:
    # executemany(prepared=True) binds a Lob only with autocommit off.
    connection = pycubrid.connect(**connect_kwargs(), autocommit=False)
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
def table(observer: Connection) -> Generator[str, None, None]:
    name = table_name("p441")
    _sql(observer, f"CREATE TABLE {name} (id INT, b BLOB, c CLOB)")
    try:
        yield name
    finally:
        _sql(observer, f"DROP TABLE IF EXISTS {name}")


def _insert(conn: Connection, sql: str, rows: list[tuple[Any, ...]]) -> None:
    with conn.cursor() as cursor:
        cursor.executemany(sql, rows, prepared=True)


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
    with conn.cursor() as cursor:
        cursor.executemany(f"INSERT INTO {table} (id, b) VALUES (?, ?)", [(1, lob)], prepared=True)
        assert cursor.rowcount == 1
    conn.commit()
    assert _stored(observer, table, "b") == {1: (size, data)}


def test_written_clob_handle_keeps_utf8_cjk_text(
    conn: Connection, observer: Connection, table: str
) -> None:
    text = ("한글 CLOB ✓ " * 9000).encode("utf-8")
    assert len(text) > LARGE
    lob = _written(conn, CLOB, [(0, text[:50_000]), (50_000, text[50_000:])])
    _insert(conn, f"INSERT INTO {table} (id, c) VALUES (?, ?)", [(1, lob)])
    conn.commit()
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
    lob._lob_handle = lob.lob_handle[:4] + (0).to_bytes(8, "big") + lob.lob_handle[12:]
    _insert(conn, f"INSERT INTO {table} (id, b) VALUES (?, ?)", [(1, lob)])
    conn.commit()
    stored = _stored(observer, table, "b")[1]
    assert stored is not None
    assert stored[0] == 0


def _fetched(conn: Connection, sql: str) -> tuple[Lob, ...]:
    with conn.cursor() as cursor:
        cursor.execute(sql)
        cells = cursor.fetchone()
    return tuple(Lob(conn, cell["lob_type"], cell["packed_lob_handle"]) for cell in cells)


def test_fetched_handles_copy_with_nulls_and_repeated_execution(
    conn: Connection, observer: Connection, table: str
) -> None:
    blob = bytes(range(256)) * 400  # > LARGE
    clob = "가나다 abc " * 10
    _sql(conn, f"INSERT INTO {table} VALUES (?, ?, ?)", (1, blob, clob))
    conn.commit()
    b, c = _fetched(conn, f"SELECT b, c FROM {table} WHERE id = 1")
    # One FC2 handle executed repeatedly with mixed LOB, NULL and INT binds.
    _insert(
        conn,
        f"INSERT INTO {table} VALUES (?, ?, ?)",
        [(2, b, c), (3, None, c), (4, b, None)],
    )
    conn.commit()
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
    conn.commit()
    assert _stored(observer, table, "b")[2] == (len(blob), blob)


def test_server_error_after_a_fetched_handle_bind_leaves_it_reusable(
    conn: Connection, observer: Connection, table: str
) -> None:
    _sql(conn, f"INSERT INTO {table} (id, b) VALUES (?, ?)", (1, b"payload"))
    conn.commit()
    (bound,) = _fetched(conn, f"SELECT b FROM {table}")
    # A BLOB bound where an INTEGER is expected fails on the server (-494);
    # the next row reuses both the statement handle and the LOB handle.
    with pytest.raises(pycubrid.ProgrammingError):
        _insert(conn, f"INSERT INTO {table} (id, b) VALUES (?, ?)", [(bound, bound), (5, bound)])
    conn.commit()
    assert _stored(observer, table, "b") == {1: (7, b"payload"), 5: (7, b"payload")}


def test_created_handle_is_consumed_by_its_first_autocommit_statement(
    observer: Connection, table: str
) -> None:
    """Measured server behavior: the statement takes over a new LOB's file.

    A fetched handle is copied (the tests above reuse one); a handle from
    LOB_NEW names a temporary file that its first autocommit statement takes
    over whether it succeeds or fails, so a second bind finds no file. This
    is why ``executemany(prepared=True)`` refuses a ``Lob`` in autocommit, so
    the packets are sent directly here.
    """
    with contextlib.closing(pycubrid.connect(**connect_kwargs(), autocommit=True)) as conn:
        generation = conn._physical_generation
        prep = PreparePacket(
            f"INSERT INTO {table} (id, b) VALUES (?, ?)",
            auto_commit=True,
            prepare_flag=CCIPrepareOption.HOLDABLE,
        )
        conn._send_and_receive(prep, expected_generation=generation)

        def execute(*bindings: Any) -> tuple[int, int, str]:
            packet = ExecuteArrayPacket(
                prep.query_handle,
                (bindings,),
                auto_commit=True,
                bind_count=prep.bind_count,
                protocol_version=conn._protocol_version,
            )
            conn._send_and_receive(packet, expected_generation=generation)
            return packet.row_results[0]

        try:
            for fail_first in (False, True):
                lob = _written(conn, BLOB, [(0, b"temp")])
                bound = _PreparedLob(BLOB, lob.lob_handle, conn, generation)
                if fail_first:
                    assert execute(bound, bound)[0] == -1
                else:
                    assert execute(_encode_prepared_scalar(1), bound)[:2] == (1, 0)
                count, code, message = execute(_encode_prepared_scalar(2), bound)
                assert (count, code) == (-1, -1016)
                # The text after the code is the server's locale-dependent strerror.
                assert "No such file" in message or "external storage" in message
        finally:
            conn._send_and_receive(
                CloseQueryPacket(prep.query_handle), expected_generation=generation
            )
    assert _stored(observer, table, "b") == {1: (4, b"temp")}
