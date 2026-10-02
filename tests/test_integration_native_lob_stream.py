"""Live stateful native LOB stream and safe official-driver comparison (#442)."""

from __future__ import annotations

from collections.abc import Generator
from contextlib import closing
from typing import Any

import pytest

import pycubrid
from pycubrid.compat import native
from pycubrid.connection import Connection
from pycubrid.exceptions import DatabaseError
from pycubrid.lob import Lob

from ._cubrid_endpoint import TEST_DB, TEST_HOST, TEST_PASSWORD, TEST_PORT, TEST_USER
from ._parity_helpers import connect_kwargs, table_name

pytestmark = pytest.mark.integration


def _native() -> native.connection:
    return native.connect(f"CUBRID:{TEST_HOST}:{TEST_PORT}:{TEST_DB}:::", TEST_USER, TEST_PASSWORD)


@pytest.fixture
def observer() -> Generator[Connection, None, None]:
    with closing(pycubrid.connect(**connect_kwargs(), autocommit=True)) as conn:
        yield conn


def _sql(conn: Connection, sql: str, params: Any = None) -> None:
    cur = conn.cursor()
    try:
        cur.execute(sql, params)
    finally:
        cur.close()


@pytest.fixture
def table(observer: Connection) -> Generator[str, None, None]:
    name = table_name("p442")
    _sql(observer, f"CREATE TABLE {name} (id INT, b BLOB, c CLOB)")
    try:
        yield name
    finally:
        _sql(observer, f"DROP TABLE IF EXISTS {name}")


def _stored(observer: Connection, table: str) -> dict[int, tuple[bytes | None, bytes | None]]:
    cur = observer.cursor()
    try:
        cur.execute(f"SELECT id, b, c FROM {table} ORDER BY id")
        rows = cur.fetchall()
    finally:
        cur.close()

    def value(cell: Any) -> bytes | None:
        if cell is None:
            return None
        lob = Lob(observer, cell["lob_type"], cell["packed_lob_handle"])
        # The old row's declared size remains unchanged after appending through
        # a fetched handle; raw reads past that size can see the shared file.
        return lob.read(cell["lob_length"])

    return {row_id: (value(blob), value(clob)) for row_id, blob, clob in rows}


def test_created_blob_and_clob_stream_bind_once(observer: Connection, table: str) -> None:
    conn = _native()
    try:
        blob, clob = conn.lob(), conn.lob()
        written = blob.write(b"abc")
        assert written is None
        position = blob.seek(0, native.SEEK_END)
        assert position == 3
        written = blob.write("d")
        assert written is None
        position = blob.seek(0, native.SEEK_SET)
        assert position == 0
        value = blob.read(2)
        assert value == "ab"
        value = blob.read()
        assert value == "cd"

        written = clob.write("A한é", "C")
        assert written is None
        position = clob.seek(0)
        assert position == 6
        written = clob.write("B")
        assert written is None
        position = clob.seek(0, native.SEEK_SET)
        assert position == 0
        value = clob.read(1)
        assert value == "A"
        value = clob.read()
        assert value == "한éB"

        cur = conn.cursor()
        try:
            cur.prepare(f"INSERT INTO {table} VALUES (1, ?, ?)")
            cur.bind_lob(1, blob)
            cur.bind_lob(2, clob)
            count = cur.execute()
            assert count == 1
        finally:
            cur.close()
        assert _stored(observer, table) == {1: (b"abcd", "A한éB".encode("utf-8"))}

        blob.seek(0, native.SEEK_SET)
        with pytest.raises(DatabaseError) as info:
            blob.read(1)
        assert info.value.errno == -1020  # LOB_NEW temp file was consumed on bind
    finally:
        conn.close()


def test_fetched_clob_append_keeps_original_declared_size_and_binds_edited_handle(
    observer: Connection, table: str
) -> None:
    _sql(observer, f"INSERT INTO {table} VALUES (?, ?, ?)", (1, None, "abc"))
    conn = _native()
    try:
        source = conn.cursor()
        source.prepare(f"SELECT c FROM {table} WHERE id = 1")
        source.execute()
        clob = conn.lob()
        source.fetch_lob(1, clob)
        position = clob.seek(0, native.SEEK_END)
        assert position == 3
        written = clob.write("d")
        assert written is None
        position = clob.seek(0, native.SEEK_SET)
        assert position == 0
        value = clob.read()
        assert value == "abcd"

        dest = conn.cursor()
        dest.prepare(f"INSERT INTO {table} (id, c) VALUES (2, ?)")
        dest.bind_lob(1, clob)
        count = dest.execute()
        assert count == 1
        source.close()
        dest.close()
    finally:
        conn.close()
    assert _stored(observer, table) == {1: (None, b"abc"), 2: (None, b"abcd")}


def test_large_created_stream_crosses_broker_read_and_write_chunks(
    observer: Connection, table: str
) -> None:
    text = "A한éB" * 25_000  # 175,000 UTF-8 bytes
    conn = _native()
    try:
        clob = conn.lob()
        written = clob.write(text, "C")
        assert written is None
        position = clob.seek(0)
        assert position == len(text.encode("utf-8"))
        position = clob.seek(0, native.SEEK_SET)
        assert position == 0
        value = clob.read()
        assert value == text
        cur = conn.cursor()
        try:
            cur.prepare(f"INSERT INTO {table} (id, c) VALUES (3, ?)")
            cur.bind_lob(1, clob)
            count = cur.execute()
            assert count == 1
        finally:
            cur.close()
    finally:
        conn.close()
    assert _stored(observer, table) == {3: (None, text.encode("utf-8"))}


def test_empty_created_stream_is_readable_and_bindable(observer: Connection, table: str) -> None:
    conn = _native()
    try:
        blob = conn.lob()
        written = blob.write(b"")
        assert written is None
        position = blob.seek(0, native.SEEK_END)
        assert position == 0
        value = blob.read()
        assert value == ""
        cur = conn.cursor()
        try:
            cur.prepare(f"INSERT INTO {table} (id, b) VALUES (4, ?)")
            cur.bind_lob(1, blob)
            count = cur.execute()
            assert count == 1
        finally:
            cur.close()
    finally:
        conn.close()
    assert _stored(observer, table) == {4: (b"", None)}


def test_fetching_another_row_keeps_the_official_byte_position(
    observer: Connection, table: str
) -> None:
    for row_id, value in ((1, "abcd"), (2, "wxyz"), (3, None), (4, "1234")):
        _sql(observer, f"INSERT INTO {table} (id, c) VALUES (?, ?)", (row_id, value))
    conn = _native()
    try:
        cur = conn.cursor()
        try:
            cur.prepare(f"SELECT c FROM {table} ORDER BY id")
            cur.execute()
            lob = conn.lob()
            cur.fetch_lob(1, lob)
            position = lob.seek(2, native.SEEK_SET)
            assert position == 2
            cur.fetch_lob(1, lob)
            position = lob.seek(0)
            assert position == 2
            value = lob.read()
            assert value == "yz"
            cur.fetch_lob(1, lob)  # NULL clears the value, not the position
            position = lob.seek(0)
            assert position == 4
            cur.fetch_lob(1, lob)
            position = lob.seek(0)
            assert position == 4
            position = lob.seek(0, native.SEEK_SET)
            assert position == 0
            value = lob.read()
            assert value == "1234"
        finally:
            cur.close()
    finally:
        conn.close()
