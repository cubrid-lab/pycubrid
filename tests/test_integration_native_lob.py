"""Live ``compat.native`` LOB handles (#441): ``lob()``/``fetch_lob()``/``bind_lob()``."""

from __future__ import annotations

from collections.abc import Generator
from typing import Any

import pytest

import pycubrid
from pycubrid.compat import native
from pycubrid.connection import Connection
from pycubrid.exceptions import InterfaceError, ProgrammingError
from pycubrid.lob import Lob

from ._cubrid_endpoint import TEST_DB, TEST_HOST, TEST_PASSWORD, TEST_PORT, TEST_USER
from ._parity_helpers import connect_kwargs, table_name

pytestmark = [pytest.mark.integration, pytest.mark.no_escape_pin]

# Above the broker's single LOB_READ reply cap (~80 KB, #362).
LARGE = 100_000
MB = 1024 * 1024


def _connect() -> native.connection:
    return native.connect(f"CUBRID:{TEST_HOST}:{TEST_PORT}:{TEST_DB}:::", TEST_USER, TEST_PASSWORD)


@pytest.fixture
def observer() -> Generator[Connection, None, None]:
    conn = pycubrid.connect(**connect_kwargs(), autocommit=True)
    yield conn
    conn.close()


def _sql(observer: Connection, sql: str, params: Any = None) -> None:
    cursor = observer.cursor()
    try:
        cursor.execute(sql, params)
    finally:
        cursor.close()


@pytest.fixture
def tables(observer: Connection) -> Generator[tuple[str, str], None, None]:
    src, dst = table_name("p441s"), table_name("p441d")
    for name in (src, dst):
        _sql(observer, f"CREATE TABLE {name} (id INT, b BLOB, c CLOB)")
    try:
        yield src, dst
    finally:
        for name in (src, dst):
            _sql(observer, f"DROP TABLE IF EXISTS {name}")


def _stored(observer: Connection, table: str) -> dict[int, tuple[Any, Any]]:
    """Read every row's BLOB/CLOB through an ordinary ``Lob``: (length, bytes) or None."""
    cursor = observer.cursor()
    try:
        cursor.execute(f"SELECT id, b, c FROM {table} ORDER BY id")
        rows = cursor.fetchall()
    finally:
        cursor.close()

    def read(cell: Any) -> Any:
        if cell is None:
            return None
        lob = Lob(observer, cell["lob_type"], cell["packed_lob_handle"])
        return cell["lob_length"], lob.read(cell["lob_length"] + 16)

    return {row_id: (read(b), read(c)) for row_id, b, c in rows}


def _payloads(size: int) -> tuple[bytes, str]:
    blob = bytes(range(256)) * (size // 256) + bytes(range(size % 256))
    unit = "한글 CLOB ✓ "
    text = (unit * (size // len(unit.encode("utf-8")) + 1)).encode("utf-8")[:size]
    # Trim to a character boundary so the CLOB is valid UTF-8 text.
    return blob, text.decode("utf-8", errors="ignore")


@pytest.mark.parametrize("size", [0, 1024, LARGE, MB], ids=["empty", "1kb", "100kb", "1mb"])
def test_fetch_then_bind_copies_blob_and_clob(
    observer: Connection, tables: tuple[str, str], size: int
) -> None:
    src, dst = tables
    blob, text = _payloads(size)
    _sql(observer, f"INSERT INTO {src} VALUES (?, ?, ?)", (1, blob, text))
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.prepare(f"SELECT id, b, c FROM {src}")
        result = cur.execute()
        assert result == 1
        b, c = conn.lob(), conn.lob()
        result = cur.fetch_lob(2, b)
        assert result is None
        ins = conn.cursor()
        ins.prepare(f"INSERT INTO {dst} VALUES (?, ?, ?)")
        # The row is consumed: read the CLOB from a second execution.
        cur.execute()
        cur.fetch_lob(3, c)
        ins.bind_param(1, 1)
        ins.bind_lob(2, b)
        ins.bind_lob(3, c)
        result = ins.execute()
        assert result == 1
        cur.close()
        ins.close()
    finally:
        conn.close()
    text_bytes = text.encode("utf-8")
    assert _stored(observer, dst) == {1: ((len(blob), blob), (len(text_bytes), text_bytes))}


def test_null_cells_mixed_columns_and_repeated_cursor_use(
    observer: Connection, tables: tuple[str, str]
) -> None:
    src, dst = tables
    blob, text = _payloads(LARGE)
    rows = [(1, blob, text), (2, None, "only clob"), (3, b"\x00only blob", None)]
    for row in rows:
        _sql(observer, f"INSERT INTO {src} VALUES (?, ?, ?)", row)
    conn = _connect()
    try:
        cur = conn.cursor()
        ins = conn.cursor()
        cur.prepare(f"SELECT id, b, c FROM {src} ORDER BY id")
        ins.prepare(f"INSERT INTO {dst} VALUES (?, ?, ?)")
        for round_ in range(2):  # the same two handles, re-executed
            result = cur.execute()
            assert result == 3
            b = conn.lob()
            for row_id in (1, 2, 3):
                cur.fetch_lob(2, b)
                ins.bind_param(1, round_ * 10 + row_id)
                if row_id == 2:
                    # NULL cell: the row is consumed and the lob has no value.
                    with pytest.raises(InterfaceError, match="no value"):
                        ins.bind_lob(2, b)
                    ins.bind_param(2, None)
                else:
                    ins.bind_lob(2, b)
                ins.bind_param(3, None)
                result = ins.execute()
                assert result == 1
            # End of result: None, and the lob keeps the last handle.
            result = cur.fetch_lob(2, b)
            assert result is None
            ins.bind_param(1, round_ * 10 + 9)
            ins.bind_lob(2, b)
            ins.execute()
        cur.close()
        ins.close()
    finally:
        conn.close()
    stored = _stored(observer, dst)
    only = (len(b"\x00only blob"), b"\x00only blob")
    for base in (0, 10):
        assert stored[base + 1] == ((len(blob), blob), None)
        assert stored[base + 2] == (None, None)
        assert stored[base + 3] == (only, None)
        assert stored[base + 9] == (only, None)


def test_requested_column_type_and_non_lob_column(
    observer: Connection, tables: tuple[str, str]
) -> None:
    src, dst = tables
    _sql(observer, f"INSERT INTO {src} VALUES (?, ?, ?)", (1, b"\x01blob", "clob 한"))
    conn = _connect()
    try:
        cur = conn.cursor()
        # CLOB first, BLOB second: the official driver would type column 2 as CLOB.
        cur.prepare(f"SELECT c, b, id FROM {src}")
        cur.execute()
        lob = conn.lob()
        with pytest.raises(ProgrammingError, match="not a BLOB or CLOB"):
            cur.fetch_lob(3, lob)
        cur.fetch_lob(2, lob)  # the rejected call did not consume the row
        ins = conn.cursor()
        ins.prepare(f"INSERT INTO {dst} (id, b) VALUES (1, ?)")
        ins.bind_lob(1, lob)
        ins.execute()
        cur.close()
        ins.close()
    finally:
        conn.close()
    assert _stored(observer, dst) == {1: ((5, b"\x01blob"), None)}


def test_server_rejection_keeps_the_handle_and_the_statement_usable(
    observer: Connection, tables: tuple[str, str]
) -> None:
    src, dst = tables
    _sql(observer, f"INSERT INTO {src} VALUES (?, ?, ?)", (1, b"payload", "text"))
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.prepare(f"SELECT b, c FROM {src}")
        cur.execute()
        b = conn.lob()
        cur.fetch_lob(1, b)
        ins = conn.cursor()
        ins.prepare(f"INSERT INTO {dst} (id, b) VALUES (?, ?)")
        ins.bind_lob(1, b)  # a BLOB where an INTEGER is expected
        ins.bind_lob(2, b)
        with pytest.raises(ProgrammingError):  # server -494
            ins.execute()
        ins.bind_param(1, 7)
        ins.execute()
        cur.close()
        ins.close()
    finally:
        conn.close()
    assert _stored(observer, dst) == {7: ((7, b"payload"), None)}


def test_cross_connection_closed_and_reconnected_handles(
    observer: Connection, tables: tuple[str, str]
) -> None:
    src, dst = tables
    _sql(observer, f"INSERT INTO {src} VALUES (?, ?, ?)", (1, b"payload", "text"))
    conn, other = _connect(), _connect()
    try:
        cur = conn.cursor()
        cur.prepare(f"SELECT b FROM {src}")
        cur.execute()
        lob = conn.lob()
        cur.fetch_lob(1, lob)
        # A fetched handle of a committed row binds on another connection:
        # the server stores an independent copy.
        foreign = other.cursor()
        foreign.prepare(f"INSERT INTO {dst} (id, b) VALUES (1, ?)")
        foreign.bind_lob(1, lob)
        result = foreign.execute()
        assert result == 1
        with pytest.raises(TypeError):
            foreign.bind_lob(1, b"payload")
        # Filling a lob through another connection's cursor is refused.
        other_select = other.cursor()
        other_select.prepare(f"SELECT b FROM {src}")
        other_select.execute()
        with pytest.raises(InterfaceError, match="another connection"):
            other_select.fetch_lob(1, lob)
        other_select.close()

        # A rollback (a no-op on this autocommit connection) leaves an already
        # fetched handle usable.
        ins = conn.cursor()
        ins.prepare(f"INSERT INTO {dst} (id, b) VALUES (2, ?)")
        conn.rollback()
        ins.bind_lob(1, lob)
        ins.execute()

        # A fetched handle names a stored file the server copies, so it
        # stays bindable after its physical session is replaced; the binding
        # belongs to the new session.
        driver = conn._driver
        generation = driver._physical_generation
        driver._drop_connection()
        result = driver.ping(reconnect=True)
        assert result
        assert driver._physical_generation != generation
        ins.prepare(f"INSERT INTO {dst} (id, b) VALUES (3, ?)")
        ins.bind_lob(1, lob)
        ins.execute()

        # ... and after its source connection is closed, on another one.
        cur.close()
        ins.close()
        conn.close()
        foreign.prepare(f"INSERT INTO {dst} (id, b) VALUES (4, ?)")
        foreign.bind_lob(1, lob)
        foreign.execute()

        lob.close()
        foreign.prepare(f"INSERT INTO {dst} (id, b) VALUES (5, ?)")
        with pytest.raises(InterfaceError, match="closed"):
            foreign.bind_lob(1, lob)
        foreign.close()
    finally:
        other.close()
        conn.close()
    stored = (7, b"payload")
    assert _stored(observer, dst) == {n: (stored, None) for n in (1, 2, 3, 4)}
