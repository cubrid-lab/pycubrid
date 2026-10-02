"""Live qualified wrapper rows and per-connection converters (#466)."""

from __future__ import annotations

from collections.abc import Generator
from contextlib import closing
from typing import Any

import pytest

import pycubrid
from pycubrid.compat import cubriddb
from pycubrid.compat.cursors import Cursor, DictCursor
from pycubrid.connection import Connection
from pycubrid.protocol import FetchPacket

from ._cubrid_endpoint import TEST_DB, TEST_HOST, TEST_PASSWORD, TEST_PORT, TEST_USER
from ._parity_helpers import connect_kwargs, table_name

pytestmark = pytest.mark.integration


@pytest.fixture
def observer() -> Generator[Connection, None, None]:
    with closing(pycubrid.connect(**connect_kwargs(), autocommit=True)) as conn:
        yield conn


def _sql(conn: Connection, sql: str, args: Any = None) -> Any:
    cursor = conn.cursor()
    try:
        cursor.execute(sql, args)
        return cursor.fetchone() if sql.startswith("SELECT") else None
    finally:
        cursor.close()


@pytest.fixture
def table(observer: Connection) -> Generator[str, None, None]:
    name = table_name("p466")
    _sql(observer, f"CREATE TABLE {name} (id INT NOT NULL, txt VARCHAR(40), opt VARCHAR(40))")
    try:
        for row in ((1, "한", None), (2, "", ""), (3, "third", None)):
            _sql(observer, f"INSERT INTO {name} VALUES (?, ?, ?)", row)
        yield name
    finally:
        _sql(observer, f"DROP TABLE IF EXISTS {name}")


def _wrapper() -> cubriddb.Connection:
    return cubriddb.Connection(
        f"CUBRID:{TEST_HOST}:{TEST_PORT}:{TEST_DB}:::", TEST_USER, TEST_PASSWORD
    )


def _select(table: str) -> str:
    return (
        f'SELECT id AS "MiXeD", txt AS dup, opt AS dup, txt AS "CaseKeep" FROM {table} ORDER BY id'
    )


def test_exact_tuple_dict_metadata(table: str) -> None:
    wrapper = _wrapper()
    try:
        tuple_cursor = Cursor(wrapper)
        dict_cursor = DictCursor(wrapper)
        assert (tuple_cursor.arraysize, tuple_cursor.rowcount, tuple_cursor.description) == (
            1,
            -1,
            None,
        )
        count = tuple_cursor.execute(_select(table))
        assert count == 3 and tuple_cursor.rowcount == 3
        expected = (
            ("MiXeD", 8, 0, 0, 10, 0, 0),
            ("dup", 2, 0, 0, 40, 0, 1),
            ("dup", 2, 0, 0, 40, 0, 1),
            ("CaseKeep", 2, 0, 0, 40, 0, 1),
        )
        assert tuple_cursor.description == expected
        first = tuple_cursor.fetchone()
        assert first == (1, "한", None, "한")
        second = tuple_cursor.fetchmany(1)
        assert second == [(2, "", "", "")]
        rest = tuple_cursor.fetchall()
        assert rest == [(3, "third", None, "third")]

        count = dict_cursor.execute(_select(table))
        assert count == 3
        row = dict_cursor.fetchone()
        assert row == {"MiXeD": 1, "dup": None, "CaseKeep": "한"}
        assert list(row) == ["MiXeD", "dup", "CaseKeep"]
        dict_cursor.close()
        tuple_cursor.close()
    finally:
        wrapper.close()


def test_wrapper_fetchall_continues_beyond_inline_broker_page(observer: Connection) -> None:
    name = table_name("p466_page")
    _sql(observer, f"CREATE TABLE {name} (id INT, payload VARCHAR(8192))")
    payload = "한" * 2_000  # many rows exceed the broker's inline reply buffer
    try:
        for start in range(0, 130, 10):
            values = ", ".join(f"({row_id}, ?)" for row_id in range(start, start + 10))
            _sql(observer, f"INSERT INTO {name} VALUES {values}", (payload,) * 10)
        wrapper = _wrapper()
        try:
            driver = wrapper.connection._driver
            driver._fetch_size = 5
            packets: list[object] = []
            original = driver._send_and_receive

            def capture(packet: Any, **kwargs: Any) -> Any:
                packets.append(packet)
                return original(packet, **kwargs)

            driver._send_and_receive = capture
            cursor = wrapper.cursor()
            count = cursor.execute(f"SELECT id, payload FROM {name} ORDER BY id")
            assert count == 130
            rows = cursor.fetchall()
            assert len(rows) == 130
            assert rows[0] == (0, payload)
            assert rows[-1] == (129, payload)
            assert any(isinstance(packet, FetchPacket) for packet in packets)
            cursor.close()
        finally:
            wrapper.close()
    finally:
        _sql(observer, f"DROP TABLE IF EXISTS {name}")


def test_converter_replacement_is_connection_scoped_and_falsey_paths_resume(table: str) -> None:
    left, right = _wrapper(), _wrapper()
    try:
        left_cursor = left.cursor()
        right_cursor = right.cursor(True)
        left_cursor.execute(_select(table))
        right_cursor.execute(_select(table))
        left.set_fetch_value_converter(lambda row, metadata: ("left", row))
        right.set_fetch_value_converter(lambda row, metadata: ("right", row))
        left_first = left_cursor.fetchone()
        right_first = right_cursor.fetchone()
        assert left_first == ("left", (1, "한", None, "한"))
        assert right_first == ("right", {"MiXeD": 1, "dup": None, "CaseKeep": "한"})

        left.set_fetch_value_converter(lambda row, metadata: 0)
        stopped = left_cursor.fetchmany(3)
        assert stopped == []  # row 2 consumed
        left.set_fetch_value_converter(None)
        resumed = left_cursor.fetchone()
        assert resumed == (3, "third", None, "third")
        right_second = right_cursor.fetchone()
        assert right_second == ("right", {"MiXeD": 2, "dup": "", "CaseKeep": ""})

        left_cursor.execute(_select(table))
        left.set_fetch_value_converter(lambda row, metadata: None)
        iterator = iter(left_cursor)
        with pytest.raises(StopIteration):
            next(iterator)  # consumes row 1 but does not permanently end it
        left.set_fetch_value_converter(None)
        after_stop = next(iterator)
        assert after_stop == (2, "", "", "")
        left_cursor.close()
        right_cursor.close()
    finally:
        right.close()
        left.close()


def test_scalar_bridge_insert_has_stable_nonselect_metadata(
    observer: Connection, table: str
) -> None:
    wrapper = _wrapper()
    try:
        cursor = wrapper.cursor()
        cursor.execute(_select(table))
        assert cursor.description is not None
        count = cursor.execute(f"INSERT INTO {table} VALUES (?, ?, ?)", (4, "added", None))
        assert count == 1 and cursor.rowcount == 1 and cursor.description is None
        observed = _sql(observer, f"SELECT id, txt, opt FROM {table} WHERE id = 4")
        assert observed == (4, "added", None)
        cursor.close()
    finally:
        wrapper.close()
