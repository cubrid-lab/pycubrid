"""Observed native positioning contracts over a finite, absolute FETCH page (#444)."""

from __future__ import annotations

import struct
from collections.abc import Iterator
from typing import Any, TypeAlias

import pytest

from pycubrid.compat import native
from pycubrid.constants import CUBRIDStatementType
from pycubrid.exceptions import InterfaceError, OperationalError
from pycubrid.protocol import ColumnMetaData, ExecutePacket, FetchPacket, PreparePacket

from .test_compat_prepared import DSN, FakeDriver


class PagedDriver(FakeDriver):
    """Reuse the owner fake, returning each requested absolute server page."""

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._fetch_size = 3
        self.data: list[tuple[object, ...]] = [(i,) for i in range(1, 258)]
        column = ColumnMetaData(column_type=8, name="id", precision=10, scale=0)
        column._cci_type = 8
        self.columns = [column]
        self.fetch_starts: list[int] = []
        self.fetch_reconnect: list[bool] = []

    def _send_and_receive(
        self,
        packet: Any,
        *,
        allow_reconnect: bool = True,
        expected_generation: int | None = None,
    ) -> Any:
        if isinstance(packet, FetchPacket):
            self.fetch_starts.append(packet.current_tuple_count + 1)
            self.fetch_reconnect.append(allow_reconnect)
        result = super()._send_and_receive(
            packet, allow_reconnect=allow_reconnect, expected_generation=expected_generation
        )
        if isinstance(packet, PreparePacket):
            packet.columns = (
                list(self.columns) if packet.statement_type == CUBRIDStatementType.SELECT else []
            )
        elif isinstance(packet, ExecutePacket):
            if packet.statement_type == CUBRIDStatementType.SELECT:
                packet.rows = self.data[: self._fetch_size]
                packet.total_tuple_count = len(self.data)
                packet.tuple_count = len(packet.rows)
            else:
                packet.columns = []
        elif isinstance(packet, FetchPacket):
            start = packet.current_tuple_count
            packet.rows = self.data[start : start + packet.fetch_size]
            packet.tuple_count = len(packet.rows)
        return result


_Owned: TypeAlias = tuple[native.connection, PagedDriver]


@pytest.fixture
def owned(monkeypatch: pytest.MonkeyPatch) -> Iterator[_Owned]:
    monkeypatch.setattr(native, "_DriverConnection", PagedDriver)
    conn = native.connect(DSN)
    yield conn, conn._driver
    conn.close()


def _selected(conn: native.connection) -> native.cursor:
    cur = conn.cursor()
    cur.prepare("SELECT id FROM position_fixture ORDER BY id")
    cur.execute()
    return cur


def test_position_aware_fixture_preserves_existing_forward_fetch(owned: _Owned) -> None:
    """Control: transport setup and existing forward fetching work before new APIs."""
    conn, driver = owned
    cur = _selected(conn)
    for value in range(1, 258):
        assert cur.fetch_row() == (value,)
        assert len(cur._rows) <= 3
    assert cur.fetch_row() is None
    assert driver.fetch_starts == list(range(4, 258, 3))


def test_observed_dual_counters_and_same_statement_reexecute(owned: _Owned) -> None:
    conn, _ = owned
    cur = _selected(conn)
    assert cur.row_tell() == 0
    assert cur.fetch_row() == (1,)
    assert cur.row_tell() == 1
    assert cur.data_seek(3) is None
    assert cur.row_tell() == 3
    assert cur.row_seek(-2) is None
    assert cur.row_tell() == 1
    assert cur.row_seek(4) is None
    assert cur.row_tell() == 5
    assert cur.fetch_row() == (5,)
    assert cur.row_tell() == 6
    assert cur.execute() == 257
    assert cur.row_tell() == 6
    assert cur.fetch_row() == (1,)
    assert cur.row_tell() == 7
    cur.prepare("SELECT id FROM position_fixture ORDER BY id")
    cur.execute()
    assert cur.row_tell() == 0


def test_backward_evicted_page_and_forward_relative_fetch_are_absolute(owned: _Owned) -> None:
    conn, driver = owned
    cur = _selected(conn)
    metadata = cur.result_info()
    for value in range(1, 206):
        assert cur.fetch_row() == (value,)
    assert cur.row_tell() == 205
    before = len(driver.requests)
    cur.data_seek(3)
    assert len(driver.requests) == before
    assert cur.fetch_row() == (3,)
    assert driver.fetch_starts[-1] == 3
    assert cur.row_tell() == 4
    cur.data_seek(220)
    assert cur.fetch_row() == (220,)
    assert cur.row_tell() == 221
    cur.row_seek(-115)
    assert cur.fetch_row() == (106,)
    assert cur.row_tell() == 107
    assert driver.fetch_starts[-2:] == [220, 106]
    packets = [p for p, _ in driver.requests if isinstance(p, FetchPacket)]
    assert [struct.unpack(">i", p.write(driver._cas_info)[8:][13:17])[0] for p in packets[-3:]] == [
        3,
        220,
        106,
    ]
    assert all(flag is False for flag in driver.fetch_reconnect)
    assert len([p for p, _ in driver.requests if isinstance(p, PreparePacket)]) == 1
    assert len([p for p, _ in driver.requests if isinstance(p, ExecutePacket)]) == 1
    assert len(cur._rows) <= 3
    assert cur.result_info() == metadata


def test_in_page_seeks_and_tell_send_no_requests(owned: _Owned) -> None:
    conn, driver = owned
    cur = _selected(conn)
    before = list(driver.requests)
    cur.data_seek(1)
    cur.row_seek(1)
    assert cur.row_tell() == 2
    assert cur.fetch_row() == (2,)
    assert cur.row_tell() == 3
    cur.data_seek(1)
    assert cur.fetch_row() == (1,)
    assert driver.requests == before


@pytest.mark.parametrize("seek_first", [False, True])
def test_observed_eof_tell_depends_on_absolute_seek(owned: _Owned, seek_first: bool) -> None:
    conn, _ = owned
    cur = _selected(conn)
    assert cur.row_tell() == 0
    if seek_first:
        cur.data_seek(1)
    assert [cur.fetch_row() for _ in range(257)] == [(i,) for i in range(1, 258)]
    assert cur.fetch_row() is None
    assert cur.fetch_row() is None
    if seek_first:
        with pytest.raises(InterfaceError) as raised:
            cur.row_tell()
        assert raised.value.code == -30012
    else:
        assert cur.row_tell() == 257


@pytest.mark.parametrize(("absolute", "offset", "recovery"), [(3, -3, 3), (256, 2, 257)])
def test_failed_relative_clamps_physical_only_and_absolute_seek_recovers(
    owned: _Owned, absolute: int, offset: int, recovery: int
) -> None:
    conn, driver = owned
    cur = _selected(conn)
    cur.data_seek(absolute)
    before = list(driver.requests)
    with pytest.raises(InterfaceError) as raised:
        cur.row_seek(offset)
    assert raised.value.code == -20005
    assert cur.row_tell() == absolute
    assert cur.fetch_row() is None
    assert driver.requests == before
    cur.data_seek(recovery)
    assert cur.fetch_row() == (recovery,)


@pytest.mark.parametrize("method", ["data_seek", "row_seek", "row_tell"])
@pytest.mark.parametrize("state", ["fresh", "prepare", "dml", "rollback", "closed", "stale"])
def test_positioning_rejects_unusable_result_without_new_requests(
    owned: _Owned, method: str, state: str
) -> None:
    conn, driver = owned
    cur = conn.cursor()
    if state == "prepare":
        cur.prepare("SELECT id FROM position_fixture")
    elif state == "dml":
        cur.prepare("INSERT INTO position_fixture VALUES (1)")
        cur.execute()
    elif state != "fresh":
        cur = _selected(conn)
        if state == "rollback":
            conn.rollback()
        elif state == "closed":
            cur.close()
        else:
            driver._physical_generation += 1
    before = list(driver.requests)
    args = () if method == "row_tell" else (1,)
    with pytest.raises(InterfaceError):
        getattr(cur, method)(*args)
    assert driver.requests == before
    driver._physical_generation = 1


def test_failed_fetch_invalidates_rows_but_preserves_metadata(owned: _Owned) -> None:
    conn, driver = owned
    cur = _selected(conn)
    metadata = cur.result_info()
    for _ in range(3):
        cur.fetch_row()
    driver.fail_packet_type = FetchPacket
    driver.fail_packet_error = OperationalError("owned fetch failed")
    with pytest.raises(OperationalError, match="owned fetch failed"):
        cur.fetch_row()
    assert cur._result_invalidated is True
    assert cur._rows == []
    assert cur.result_info() == metadata
    assert driver.fetch_reconnect[-1] is False
