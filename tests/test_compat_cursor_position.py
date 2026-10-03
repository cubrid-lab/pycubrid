"""Observed native positioning contracts over a finite, absolute FETCH page (#444)."""

from __future__ import annotations

import struct
from collections.abc import Iterator
from typing import Any, TypeAlias
from unittest.mock import patch

import pytest

from pycubrid.compat import cubriddb, native
from pycubrid.constants import CASFunctionCode, CUBRIDStatementType
from pycubrid.exceptions import DataError, DatabaseError, InterfaceError, OperationalError
from pycubrid.protocol import ColumnMetaData, ExecutePacket, FetchPacket, PreparePacket

from .test_compat_prepared import DSN, FakeDriver
from .test_compat_lob import BLOB_CELL, CLOB_CELL, LobDriver
from .test_network_edge_cases import make_connected_connection
from .test_prepared_packet_contract import _arguments


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
        self.invalid_page: str | None = None

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
            if self.invalid_page == "empty":
                packet.rows = []
            elif self.invalid_page == "overrun":
                packet.rows = self.data[start:] + [("extra",)]
            packet.tuple_count = len(packet.rows)
        return result


class LifetimeDriver(PagedDriver):
    """CAS releases an autocommit/forward-only result after its final page."""

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.remote_result_closed = False
        self.release_on_final_page = False

    def _send_and_receive(
        self,
        packet: Any,
        *,
        allow_reconnect: bool = True,
        expected_generation: int | None = None,
    ) -> Any:
        if isinstance(packet, FetchPacket) and self.remote_result_closed:
            error = DatabaseError("server cursor is no longer available", code=-1012, errno=-1012)
            setattr(error, "_cas_server_error", True)
            self.fail_packet_type, self.fail_packet_error = FetchPacket, error
        result = super()._send_and_receive(
            packet, allow_reconnect=allow_reconnect, expected_generation=expected_generation
        )
        if isinstance(packet, ExecutePacket):
            self.remote_result_closed = False
            self.release_on_final_page = packet.auto_commit and packet.forward_only
            end = len(packet.rows)
        elif isinstance(packet, FetchPacket):
            end = packet.current_tuple_count + len(packet.rows)
        else:
            return result
        if self.release_on_final_page and end >= len(self.data):
            self.remote_result_closed = True
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


@pytest.mark.parametrize("manual", [False, True], ids=["native-default", "explicit-manual"])
def test_actual_packet_mode_controls_final_page_lifetime(
    monkeypatch: pytest.MonkeyPatch, manual: bool
) -> None:
    monkeypatch.setattr(native, "_DriverConnection", LifetimeDriver)
    conn = native.connect(DSN)
    try:
        assert conn.autocommit is True and conn._driver.autocommit is True
        if manual:
            conn.set_autocommit(False)  # explicit caller choice, never a seek side effect
        driver = conn._driver
        driver.data = [(i,) for i in range(1, 8)]
        cur = _selected(conn)
        metadata = cur.result_info()
        execute = next(p for p, _ in driver.requests if isinstance(p, ExecutePacket))
        assert execute.auto_commit is (not manual)
        assert execute.forward_only is (not manual)
        arguments = _arguments(execute.write(b"\x01\x00\x00\x00"), CASFunctionCode.EXECUTE)
        assert arguments[6] == arguments[7] == (b"\x00" if manual else b"\x01")
        assert [cur.fetch_row() for _ in range(7)] == [(i,) for i in range(1, 8)]
        assert cur.fetch_row() is None
        assert driver.remote_result_closed is (not manual)
        assert len(cur._rows) == 1  # retain the final response page, not all rows
        before = len(driver.requests)
        created = len(FakeDriver.created)
        generation = driver._physical_generation
        cur.data_seek(1)
        assert len(driver.requests) == before
        if manual:
            assert cur.fetch_row() == (1,)
            assert cur.row_tell() == 2
        else:
            with pytest.raises(DatabaseError) as raised:
                cur.fetch_row()
            assert raised.value.code == raised.value.errno == -1012
            assert getattr(raised.value, "_cas_server_error") is True
            assert cur._result_invalidated is True and cur._rows == []
            with pytest.raises(InterfaceError) as invalid:
                cur.row_tell()
            assert invalid.value.code == 0
            assert driver._connected is True
        assert driver._physical_generation == generation
        assert len(FakeDriver.created) == created
        assert len(driver.requests) == before + 1
        assert isinstance(driver.requests[-1][0], FetchPacket)
        assert driver.fetch_starts[-1] == 1 and driver.fetch_reconnect[-1] is False
        assert len([p for p, _ in driver.requests if isinstance(p, PreparePacket)]) == 1
        assert len([p for p, _ in driver.requests if isinstance(p, ExecutePacket)]) == 1
        assert cur.result_info() == metadata
        assert conn.autocommit is (not manual) and driver.autocommit is (not manual)
    finally:
        conn.close()


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
@pytest.mark.parametrize(
    "state", ["fresh", "prepare", "dml", "rollback", "closed", "stale", "foreign", "disconnected"]
)
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
        elif state == "stale":
            driver._physical_generation += 1
        elif state == "foreign":
            conn._driver = PagedDriver(**driver.options)
        else:
            driver._connected = False
    before = list(driver.requests)
    args = () if method == "row_tell" else (1,)
    try:
        with pytest.raises(InterfaceError) as raised:
            getattr(cur, method)(*args)
        assert raised.value.code == (-30019 if state == "closed" else 0)
        assert driver.requests == before
    finally:
        conn._driver = driver
        driver._physical_generation = 1
        driver._connected = True


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


def test_actual_out_tran_transport_failure_never_probes_connects_or_replays(owned: _Owned) -> None:
    conn, _ = owned
    cur = _selected(conn)
    # Existing handshake/socket helper constructs the real Connection machinery.
    driver, sock = make_connected_connection()
    conn._driver = cur._prepared_driver = driver
    cur._generation = driver._physical_generation
    driver._record_reply_cas_info(b"\x00\x00\x00\x00")
    assert driver._needs_cas_probe(True) is True
    assert driver._needs_cas_probe(False) is False
    cur.data_seek(4)
    sock.sendall.reset_mock()
    sock.sendall.side_effect = ConnectionResetError("owned socket lost")
    generation = driver._physical_generation
    with (
        patch.object(driver, "_reconnect_after_failed_probe") as reconnect,
        patch.object(driver, "_connect_locked") as connect,
        patch("socket.create_connection") as create,
    ):
        with pytest.raises(OperationalError, match="socket communication failed"):
            cur.fetch_row()
        reconnect.assert_not_called()
        connect.assert_not_called()
        create.assert_not_called()
    assert sock.sendall.call_count == 1
    assert sock.sendall.call_args.args[0][8] == CASFunctionCode.FETCH
    assert driver._physical_generation == generation
    assert driver._connected is False and driver._socket is None
    assert cur._rows == [] and cur._result_invalidated is True
    with pytest.raises(InterfaceError):
        cur.row_tell()


@pytest.mark.parametrize("mode", ["empty", "overrun"])
def test_invalid_absolute_page_fails_closed(owned: _Owned, mode: str) -> None:
    conn, driver = owned
    cur = _selected(conn)
    metadata = cur.result_info()
    cur.data_seek(256)
    driver.invalid_page = mode
    with pytest.raises(OperationalError, match="page"):
        cur.fetch_row()
    for method, args in [("data_seek", (1,)), ("row_seek", (0,)), ("row_tell", ())]:
        with pytest.raises(InterfaceError) as raised:
            getattr(cur, method)(*args)
        assert raised.value.code == 0
    assert cur._rows == [] and cur.result_info() == metadata


@pytest.mark.parametrize("method", ["data_seek", "row_seek"])
@pytest.mark.parametrize("value", [None, "1", 1.0, -(2**31) - 1, 2**31])
def test_position_integer_parser_errors_do_not_move_rows(
    owned: _Owned, method: str, value: object
) -> None:
    conn, driver = owned
    cur = _selected(conn)
    before = list(driver.requests)
    error = OverflowError if isinstance(value, int) else TypeError
    with pytest.raises(error):
        getattr(cur, method)(value)
    assert cur.row_tell() == 0 and cur.fetch_row() == (1,)
    assert driver.requests == before


@pytest.mark.parametrize("method", ["data_seek", "row_seek", "row_tell"])
def test_closed_positional_precedence_and_keyword_rejection(owned: _Owned, method: str) -> None:
    conn, _ = owned
    cur = _selected(conn)
    operation = getattr(cur, method)
    bad_args = [(1,)] if method == "row_tell" else [(), (0, 1), (None,)]
    for args in bad_args:
        with pytest.raises(TypeError):
            operation(*args)
    cur.close()
    for args in bad_args:
        with pytest.raises(InterfaceError) as raised:
            operation(*args)
        assert raised.value.code == -30019
    with pytest.raises(TypeError):
        operation(n=1)


def test_bool_and_index_are_valid_but_int_only_is_not(owned: _Owned) -> None:
    conn, _ = owned
    cur = _selected(conn)

    class Index:
        def __index__(self) -> int:
            return 2

    class IntOnly:
        def __int__(self) -> int:
            return 2

    cur.data_seek(True)
    assert cur.row_tell() == 1
    cur.row_seek(False)
    assert cur.row_tell() == 1
    cur.data_seek(Index())
    assert cur.fetch_row() == (2,)
    with pytest.raises(TypeError):
        cur.data_seek(IntOnly())
    with pytest.raises(InterfaceError) as raised:
        cur.data_seek(False)
    assert raised.value.code == -30006


@pytest.mark.parametrize(
    "effect", ["seek", "execute", "fetch", "close", "rollback", "foreign", "generation"]
)
def test_index_callback_state_changes_are_rejected(owned: _Owned, effect: str) -> None:
    conn, driver = owned
    cur = _selected(conn)

    class MutatingIndex:
        def __index__(self) -> int:
            if effect == "seek":
                cur.data_seek(2)
            elif effect == "execute":
                cur.execute()
            elif effect == "fetch":
                assert cur.fetch_row() == (1,)
            elif effect == "close":
                cur.close()
            elif effect == "rollback":
                conn.rollback()
            elif effect == "foreign":
                conn._driver = PagedDriver(**driver.options)
            else:
                driver._physical_generation += 1
            return 4

    try:
        with pytest.raises(InterfaceError):
            cur.data_seek(MutatingIndex())
        if effect == "seek":
            assert cur.row_tell() == 2 and cur.fetch_row() == (2,)
        elif effect == "fetch":
            assert cur.row_tell() == 1 and cur.fetch_row() == (2,)
        elif effect == "execute":
            assert cur.row_tell() == 0 and cur.fetch_row() == (1,)
    finally:
        conn._driver = driver
        driver._physical_generation = 1


def test_conversion_exception_and_shadow_overflow_are_atomic(owned: _Owned) -> None:
    conn, driver = owned
    cur = _selected(conn)

    class BrokenIndex:
        def __index__(self) -> int:
            raise RuntimeError("caller conversion")

    with pytest.raises(RuntimeError, match="caller conversion"):
        cur.row_seek(BrokenIndex())
    cur._tell_position = 2**31 - 1  # local safe-boundary test, never a C oracle probe
    before = (cur._next_position, cur._row_index, list(driver.requests))
    with pytest.raises(OverflowError):
        cur.row_seek(1)
    with pytest.raises(OverflowError):
        cur.fetch_row()
    assert (cur._next_position, cur._row_index, driver.requests) == before


def test_empty_result_and_transaction_boundaries(owned: _Owned) -> None:
    conn, driver = owned
    driver.data = []
    empty = _selected(conn)
    assert empty.row_tell() == 0 and empty.fetch_row() is None
    with pytest.raises(InterfaceError) as raised:
        empty.data_seek(1)
    assert raised.value.code == -30006
    driver.data = [(1,), (2,), (3,)]
    cur = _selected(conn)
    assert cur.fetch_row() == (1,)
    conn.commit()
    assert cur.row_tell() == 1 and cur.fetch_row() == (2,)
    driver.fail_boundary = True
    with pytest.raises(RuntimeError):
        conn.rollback()
    assert cur.row_tell() == 2 and cur.fetch_row() == (3,)


def test_error_refresh_is_a_new_statement_and_resets_shadow(owned: _Owned) -> None:
    conn, driver = owned
    cur = _selected(conn)
    cur.data_seek(3)
    error = DatabaseError("failed execution", code=-493)
    setattr(error, "_cas_server_error", True)
    driver.fail_packet_type, driver.fail_packet_error = ExecutePacket, error
    with pytest.raises(DatabaseError):
        cur.execute()
    driver.fail_packet_type = driver.fail_packet_error = None
    cur.execute()
    assert cur.row_tell() == 0 and cur.fetch_row() == (1,)


@pytest.mark.parametrize("prefix", [0, 2])
def test_lob_mismatch_rewinds_physical_shadow_and_page_index(
    monkeypatch: pytest.MonkeyPatch, prefix: int
) -> None:
    monkeypatch.setattr(native, "_DriverConnection", LobDriver)
    monkeypatch.setattr(LobDriver, "columns", [ColumnMetaData(column_type=23, name="b")])
    monkeypatch.setattr(LobDriver, "result", [(BLOB_CELL,)] * prefix + [(CLOB_CELL,), (None,)])
    conn = native.connect(DSN)
    try:
        cur = _selected(conn)
        holder = conn.lob()
        for _ in range(prefix):
            cur.fetch_lob(1, holder)
        before = cur.row_tell()
        old_handle = holder._handle
        with pytest.raises(DataError, match="does not match"):
            cur.fetch_lob(1, holder)
        assert cur.row_tell() == before
        assert holder._handle == old_handle
        assert cur.fetch_row() == (CLOB_CELL,)
        assert cur.row_tell() == before + 1
        cur.fetch_lob(1, holder)
        assert cur.row_tell() == before + 2 and holder._handle is None
    finally:
        conn.close()


def test_clamped_lob_eof_precedes_column_and_holder_state(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(native, "_DriverConnection", LobDriver)
    monkeypatch.setattr(LobDriver, "columns", [ColumnMetaData(column_type=23, name="b")])
    monkeypatch.setattr(LobDriver, "result", [(BLOB_CELL,)])
    conn = native.connect(DSN)
    try:
        cur = _selected(conn)
        cur.data_seek(1)
        with pytest.raises(InterfaceError):
            cur.row_seek(-1)
        holder = conn.lob()
        holder.close()
        before = list(conn._driver.requests)
        assert cur.fetch_lob(999, holder) is None
        assert cur.row_tell() == 1 and conn._driver.requests == before
    finally:
        conn.close()


def test_damaged_lob_handle_still_retires_positioned_result(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(native, "_DriverConnection", LobDriver)
    monkeypatch.setattr(LobDriver, "columns", [ColumnMetaData(column_type=23, name="b")])
    monkeypatch.setattr(LobDriver, "result", [({**BLOB_CELL, "packed_lob_handle": b""},)])
    conn = native.connect(DSN)
    try:
        cur = _selected(conn)
        with pytest.raises(OperationalError, match="malformed"):
            cur.fetch_lob(1, conn.lob())
        assert conn._driver.discarded == 1
        for method, args in [("row_tell", ()), ("data_seek", (1,)), ("row_seek", (0,))]:
            with pytest.raises(InterfaceError):
                getattr(cur, method)(*args)
    finally:
        conn.close()


def test_existing_wrapper_fetches_follow_native_position_without_new_forwards(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(native, "_DriverConnection", PagedDriver)
    conn = cubriddb.Connection(DSN)
    try:
        tuples = conn.cursor()
        tuples.execute("SELECT id FROM position_fixture ORDER BY id")
        description = tuples.description
        tuples._cs.data_seek(254)
        assert tuples.fetchmany(2) == [(254,), (255,)]
        assert tuples.fetchall() == [(256,), (257,)]
        assert tuples.description is description
        assert not hasattr(tuples, "data_seek")
        dictionaries = conn.cursor(True)
        dictionaries.execute("SELECT id FROM position_fixture ORDER BY id")
        dictionaries._cs.data_seek(7)
        assert dictionaries.fetchone() == {"id": 7}
        assert dictionaries.fetchmany(2) == [{"id": 8}, {"id": 9}]
    finally:
        conn.close()
