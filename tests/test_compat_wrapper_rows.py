"""Qualified CUBRIDdb-style tuple/dict cursors over existing native scalars (#466)."""

from __future__ import annotations

import importlib
import subprocess
import sys
from typing import Any

import pytest

from pycubrid.compat import cubriddb, native
from pycubrid.constants import CUBRIDDataType, CUBRIDStatementType
from pycubrid.exceptions import InterfaceError, NotSupportedError, ProgrammingError
from pycubrid.protocol import (
    ColumnMetaData,
    ExecutePacket,
    FetchPacket,
    GetDbParameterPacket,
    PreparePacket,
)

from .test_compat_prepared import DSN, FakeDriver


@pytest.mark.parametrize(
    "imports",
    (
        "from pycubrid.compat import cubriddb; import pycubrid.compat.cursors as cursors",
        "import pycubrid.compat.cursors as cursors; from pycubrid.compat import cubriddb",
    ),
)
def test_wrapper_module_import_order_in_fresh_process(imports: str) -> None:
    code = (
        imports
        + "; assert cursors.Cursor.__module__ == 'pycubrid.compat.cursors'"
        + "; assert cubriddb.Connection.cursor.__name__ == 'cursor'"
    )
    subprocess.run([sys.executable, "-c", code], check=True, capture_output=True, text=True)


class RowsDriver(FakeDriver):
    """Model one metadata shape and a multi-page result without a broker."""

    columns: list[ColumnMetaData] = []
    result: list[tuple[Any, ...]] = []

    def _send_and_receive(
        self,
        packet: Any,
        *,
        allow_reconnect: bool = True,
        expected_generation: int | None = None,
    ) -> Any:
        if isinstance(packet, GetDbParameterPacket):
            return super()._send_and_receive(
                packet, allow_reconnect=allow_reconnect, expected_generation=expected_generation
            )
        assert expected_generation == self._physical_generation
        self.requests.append((packet, expected_generation))
        if isinstance(packet, PreparePacket):
            packet.query_handle = 41
            packet.bind_count = packet.sql.count("?")
            selected = packet.sql.lstrip().upper().startswith("SELECT")
            packet.statement_type = (
                CUBRIDStatementType.SELECT if selected else CUBRIDStatementType.INSERT
            )
            packet.columns = list(self.columns) if selected else []
        elif isinstance(packet, ExecutePacket):
            if packet.statement_type == CUBRIDStatementType.SELECT:
                packet.total_tuple_count = len(self.result)
                packet.rows = list(self.result[: self._fetch_size])
            else:
                packet.total_tuple_count = 1
                packet.rows = []
        elif isinstance(packet, FetchPacket):
            start = packet.current_tuple_count
            packet.rows = list(self.result[start : start + self._fetch_size])
        return packet


@pytest.fixture
def wrapped(monkeypatch: pytest.MonkeyPatch) -> cubriddb.Connection:
    RowsDriver.created.clear()
    monkeypatch.setattr(native, "_DriverConnection", RowsDriver)
    conn = cubriddb.Connection(DSN)
    driver = conn.connection._driver
    driver.columns = [
        ColumnMetaData(name="MiXeD", column_type=CUBRIDDataType.INT, precision=10, scale=0),
        ColumnMetaData(
            name="dup", column_type=CUBRIDDataType.STRING, precision=40, scale=0, is_nullable=True
        ),
        ColumnMetaData(
            name="dup", column_type=CUBRIDDataType.STRING, precision=40, scale=0, is_nullable=True
        ),
        ColumnMetaData(
            name="CaseKeep",
            column_type=CUBRIDDataType.STRING,
            precision=40,
            scale=0,
            is_nullable=True,
        ),
    ]
    driver.result = [(1, "한", None, "한"), (2, "", "", ""), (3, "third", None, "third")]
    try:
        yield conn
    finally:
        conn.close()


def _module() -> Any:
    return importlib.import_module("pycubrid.compat.cursors")


def _selected(cursor: Any) -> int:
    return cursor.execute('SELECT id AS "MiXeD", txt AS dup, opt AS dup, txt AS "CaseKeep" FROM t')


def test_cursor_choice_direct_constructors_and_initial_state(wrapped: cubriddb.Connection) -> None:
    module = _module()
    for choice in (None, False, 0, []):
        cur = wrapped.cursor(choice)
        assert type(cur) is module.Cursor
        assert (cur.arraysize, cur.rowcount, cur.description) == (1, -1, None)
        cur.close()
    for choice in (True, 1, [1], "yes"):
        cur = wrapped.cursor(choice)
        assert type(cur) is module.DictCursor
        cur.close()
    for cls in (module.Cursor, module.DictCursor):
        cur = cls(wrapped)
        assert (cur.arraysize, cur.rowcount, cur.description) == (1, -1, None)
        cur.close()
    assert cubriddb.__all__ == ["Connection", "Connect", "connect", "connection"]
    assert not hasattr(cubriddb, "DictCursor")  # no guessed broken package export


def test_exact_description_tuple_and_duplicate_dict_names(wrapped: cubriddb.Connection) -> None:
    cur = wrapped.cursor()
    result = _selected(cur)
    assert result == 3 and cur.rowcount == 3
    expected = (
        ("MiXeD", 8, 0, 0, 10, 0, 0),
        ("dup", 2, 0, 0, 40, 0, 1),
        ("dup", 2, 0, 0, 40, 0, 1),
        ("CaseKeep", 2, 0, 0, 40, 0, 1),
    )
    assert cur.description == expected
    first = cur.fetchone()
    assert first == (1, "한", None, "한")
    cur.arraysize = 1
    second = cur.fetchmany()
    assert second == [(2, "", "", "")]
    rest = cur.fetchall()
    assert rest == [(3, "third", None, "third")]
    end = cur.fetchone()
    assert end is None

    dic = wrapped.cursor(True)
    _selected(dic)
    row = dic.fetchone()
    assert row == {"MiXeD": 1, "dup": None, "CaseKeep": "한"}
    assert list(row) == ["MiXeD", "dup", "CaseKeep"]


def test_public_description_tamper_does_not_change_native_callback_or_dict_keys(
    wrapped: cubriddb.Connection,
) -> None:
    cur = wrapped.cursor()
    _selected(cur)
    original = cur.description
    cur.description = ("caller-overwrite",)
    wrapped.set_fetch_value_converter(lambda row, metadata: metadata)
    returned = cur.fetchone()
    assert returned is original
    wrapped.set_fetch_value_converter(None)

    dic = wrapped.cursor(True)
    _selected(dic)
    dic.description = ("caller-overwrite",)
    row = dic.fetchone()
    assert row == {"MiXeD": 1, "dup": None, "CaseKeep": "한"}


def test_converter_is_current_per_connection_and_sees_already_shaped_row(
    wrapped: cubriddb.Connection,
) -> None:
    other = cubriddb.Connection(DSN)
    try:
        other._connection._driver.columns = list(wrapped.connection._driver.columns)
        other._connection._driver.result = list(wrapped.connection._driver.result)
        left, right = wrapped.cursor(True), other.cursor()
        _selected(left)
        _selected(right)
        seen: list[tuple[Any, Any]] = []

        def left_converter(row: Any, metadata: Any) -> Any:
            seen.append((row, metadata))
            return ("left", row)

        wrapped.set_fetch_value_converter(left_converter)
        other.set_fetch_value_converter(lambda row, metadata: ("right", row))
        left_first = left.fetchone()
        right_first = right.fetchone()
        assert left_first == ("left", {"MiXeD": 1, "dup": None, "CaseKeep": "한"})
        assert right_first == ("right", (1, "한", None, "한"))
        assert seen[0][0] == {"MiXeD": 1, "dup": None, "CaseKeep": "한"}
        assert seen[0][1] is left._native_description
        wrapped.set_fetch_value_converter(lambda row, metadata: ("replacement", row))
        left_second = left.fetchone()
        assert left_second == ("replacement", {"MiXeD": 2, "dup": "", "CaseKeep": ""})
        right_second = right.fetchone()
        assert right_second == ("right", (2, "", "", ""))
    finally:
        other.close()


def test_falsey_callback_results_stop_bulk_but_not_iteration(
    wrapped: cubriddb.Connection,
) -> None:
    cur = wrapped.cursor()
    _selected(cur)
    wrapped.set_fetch_value_converter(lambda row, metadata: 0)
    many = cur.fetchmany(3)
    assert many == []  # row 1 was consumed despite falsey callback result
    wrapped.set_fetch_value_converter(None)
    next_row = cur.fetchone()
    assert next_row == (2, "", "", "")

    _selected(cur)
    wrapped.set_fetch_value_converter(lambda row, metadata: None)
    all_rows = cur.fetchall()
    assert all_rows == []
    wrapped.set_fetch_value_converter(None)
    next_row = cur.fetchone()
    assert next_row == (2, "", "", "")

    _selected(cur)
    wrapped.set_fetch_value_converter(lambda row, metadata: 0 if row[0] == 1 else None)
    stream = iter(cur)
    first = next(stream)
    assert first == 0
    with pytest.raises(StopIteration):
        next(stream)  # callback None consumes row 2, but no terminal flag
    wrapped.set_fetch_value_converter(None)
    resumed = next(stream)
    assert resumed == (3, "third", None, "third")


def test_converter_errors_consume_one_row_and_falsey_noncallable_disables(
    wrapped: cubriddb.Connection,
) -> None:
    cur = wrapped.cursor()
    _selected(cur)
    wrapped.set_fetch_value_converter(1)
    with pytest.raises(TypeError):
        cur.fetchone()
    wrapped.set_fetch_value_converter(None)
    after_type_error = cur.fetchone()
    assert after_type_error == (2, "", "", "")

    _selected(cur)

    def failing(row: Any, metadata: Any) -> Any:
        raise ValueError("converter failed")

    wrapped.set_fetch_value_converter(failing)
    with pytest.raises(ValueError, match="converter failed"):
        cur.fetchone()
    wrapped.set_fetch_value_converter(0)  # falsey noncallable turns conversion off
    after_error = cur.fetchone()
    assert after_error == (2, "", "", "")


def test_execution_bridge_stays_with_existing_scalar_values_and_no_set_type(
    wrapped: cubriddb.Connection,
) -> None:
    cur = wrapped.cursor()
    driver = wrapped.connection._driver
    before = len(driver.requests)
    with pytest.raises(NotSupportedError):
        cur.execute("SELECT ?", (1,), set_type=8)
    assert len(driver.requests) == before
    with pytest.raises(ProgrammingError):
        cur.execute("SELECT ?", {"named": 1})
    assert len(driver.requests) == before
    result = cur.execute("SELECT ?", (1,))
    assert result == 3
    executed = [packet for packet, _ in driver.requests if isinstance(packet, ExecutePacket)]
    assert len(executed) == 1
    assert executed[0].bind_count == 1


def test_nonselect_description_none_and_closed_operations_fail(
    wrapped: cubriddb.Connection,
) -> None:
    cur = wrapped.cursor()
    _selected(cur)
    assert cur.description is not None
    result = cur.execute("INSERT INTO t VALUES (1)")
    assert result == 1 and cur.rowcount == 1 and cur.description is None
    cur.close()
    for operation in (cur.close, cur.fetchone, cur.fetchall, lambda: cur.fetchmany(0)):
        with pytest.raises(InterfaceError):
            operation()
    with pytest.raises(InterfaceError):
        iter(cur)
