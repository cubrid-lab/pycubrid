"""Public sync ``compat.native`` LOB handles: ``lob()``/``fetch_lob()``/``bind_lob()`` (#441)."""

from __future__ import annotations

import inspect
import struct
from typing import Any

import pytest

from pycubrid.compat import native
from pycubrid.constants import CUBRIDDataType, CUBRIDStatementType
from pycubrid.exceptions import DataError, InterfaceError, OperationalError, ProgrammingError
from pycubrid.protocol import ColumnMetaData, ExecutePacket, FetchPacket, PreparePacket

from .test_compat_prepared import DSN, FakeDriver, _packets
from .test_prepared_lob_contract import FETCHED_BLOB, FETCHED_CLOB, OFFICIAL_BIND_PAIRS, _bind_pair

BLOB, CLOB, INT = CUBRIDDataType.BLOB, CUBRIDDataType.CLOB, CUBRIDDataType.INT


def _cell(lob_type: int, handle: bytes) -> dict[str, object]:
    return {
        "lob_type": lob_type,
        "lob_length": struct.unpack_from(">q", handle, 4)[0],
        "file_locator": "",
        "packed_lob_handle": handle,
    }


BLOB_CELL = _cell(BLOB, FETCHED_BLOB)
CLOB_CELL = _cell(CLOB, FETCHED_CLOB)


class LobDriver(FakeDriver):
    """Serve one configured SELECT result in pages of ``_fetch_size`` rows."""

    columns: list[ColumnMetaData] = []
    result: list[tuple[Any, ...]] = []
    discarded = 0

    def _discard_uncertain_prepared_session(self) -> None:
        self.discarded += 1
        self._connected = False

    def _send_and_receive(self, packet: Any, *, expected_generation: int | None = None) -> Any:
        if expected_generation != self._physical_generation:
            raise InterfaceError("stale prepared owner")
        self.requests.append((packet, expected_generation))
        if isinstance(packet, PreparePacket):
            packet.query_handle = 37
            packet.bind_count = packet.sql.count("?")
            select = packet.sql.lstrip().upper().startswith("SELECT")
            packet.statement_type = (
                CUBRIDStatementType.SELECT if select else CUBRIDStatementType.INSERT
            )
            packet.columns = list(self.columns) if select else []
        elif isinstance(packet, ExecutePacket):
            packet.total_tuple_count = len(self.result)
            if packet.statement_type == CUBRIDStatementType.SELECT:
                packet.rows = list(self.result[: self._fetch_size])
            else:
                packet.total_tuple_count = 1
        elif isinstance(packet, FetchPacket):
            start = packet.current_tuple_count
            packet.rows = list(self.result[start : start + self._fetch_size])
        return packet


def _columns(*types: int) -> list[ColumnMetaData]:
    return [ColumnMetaData(column_type=t, name=f"c{i}") for i, t in enumerate(types, 1)]


@pytest.fixture
def driver(monkeypatch: pytest.MonkeyPatch) -> LobDriver:
    FakeDriver.created.clear()
    monkeypatch.setattr(native, "_DriverConnection", LobDriver)
    monkeypatch.setattr(LobDriver, "columns", _columns(INT, BLOB, CLOB))
    monkeypatch.setattr(
        LobDriver,
        "result",
        [(1, BLOB_CELL, CLOB_CELL), (2, None, None), (3, BLOB_CELL, CLOB_CELL)],
    )
    conn = native.connect(DSN)
    conn._driver._native_connection = conn
    return conn._driver


def _conn(driver: LobDriver) -> native.connection:
    conn: native.connection = driver._native_connection
    return conn


def _selected(conn: native.connection) -> Any:
    cur = conn.cursor()
    cur.prepare("SELECT id, b, c FROM t ORDER BY id")
    cur.execute()
    return cur


def _insert(conn: native.connection) -> Any:
    cur = conn.cursor()
    cur.prepare("INSERT INTO t2 VALUES (?)")
    return cur


# --- fetch -> bind ----------------------------------------------------------


def test_fetched_handles_bind_with_the_official_bytes(driver: LobDriver) -> None:
    conn = _conn(driver)
    try:
        cur = _selected(conn)
        blob, clob = conn.lob(), native.lob(conn)
        result = cur.fetch_lob(2, blob)
        assert result is None
        result = cur.fetch_row()
        assert result == (2, None, None)
        result = cur.fetch_lob(3, clob)
        assert result is None
        ins = _insert(conn)
        for lob, pair in ((blob, OFFICIAL_BIND_PAIRS[0][2]), (clob, OFFICIAL_BIND_PAIRS[1][2])):
            result = ins.bind_lob(1, lob)
            assert result is None
            result = ins.execute()
            assert result == 1
            sent = _packets(driver, ExecutePacket)[-1]
            assert _bind_pair(sent.bindings[0]) == bytes.fromhex(pair)
        # The same handle binds again (the server copies the value).
        ins.bind_lob(1, blob)
        ins.execute()
        assert _bind_pair(_packets(driver, ExecutePacket)[-1].bindings[0]) == bytes.fromhex(
            OFFICIAL_BIND_PAIRS[0][2]
        )
    finally:
        conn.close()


def test_the_requested_column_decides_blob_or_clob(driver: LobDriver) -> None:
    # The official driver reads the type of column 1 instead (a bug).
    conn = _conn(driver)
    try:
        cur = _selected(conn)
        lob = conn.lob()
        cur.fetch_lob(3, lob)
        assert (lob._lob_type, lob._handle) == (CLOB, FETCHED_CLOB)
        cur.fetch_row()
        cur.fetch_lob(2, lob)
        assert (lob._lob_type, lob._handle) == (BLOB, FETCHED_BLOB)
    finally:
        conn.close()


def test_fetch_lob_pages_like_fetch_row(driver: LobDriver) -> None:
    conn = _conn(driver)
    try:
        cur = _selected(conn)
        lob = conn.lob()
        for _ in range(3):
            cur.fetch_lob(2, lob)
        # fetch_size 2: the third row needed one FETCH continuation.
        assert len(_packets(driver, FetchPacket)) == 1
        assert lob._handle == FETCHED_BLOB
    finally:
        conn.close()


def test_null_cell_consumes_the_row_and_empties_the_lob(driver: LobDriver) -> None:
    conn = _conn(driver)
    try:
        cur = _selected(conn)
        lob = conn.lob()
        cur.fetch_lob(2, lob)
        result = cur.fetch_lob(3, lob)
        assert result is None
        assert lob._handle is None and lob._lob_type == CLOB
        result = cur.fetch_row()
        assert result == (3, BLOB_CELL, CLOB_CELL)
        ins = _insert(conn)
        with pytest.raises(InterfaceError, match="no value"):
            ins.bind_lob(1, lob)
    finally:
        conn.close()


def test_end_of_result_returns_none_and_keeps_the_lob(driver: LobDriver) -> None:
    conn = _conn(driver)
    try:
        cur = _selected(conn)
        lob = conn.lob()
        cur.fetch_row()
        cur.fetch_row()
        cur.fetch_lob(2, lob)
        result = cur.fetch_lob(3, lob)
        assert result is None
        assert (lob._lob_type, lob._handle) == (BLOB, FETCHED_BLOB)
        result = cur.fetch_row()
        assert result is None
    finally:
        conn.close()


# --- errors before consumption or I/O ----------------------------------------


@pytest.mark.parametrize(
    ("col", "error"),
    [
        (1, "not a BLOB or CLOB"),
        (0, "out of range"),
        (4, "out of range"),
    ],
)
def test_bad_fetch_column_keeps_the_row(driver: LobDriver, col: Any, error: str) -> None:
    conn = _conn(driver)
    try:
        cur = _selected(conn)
        lob = conn.lob()
        with pytest.raises(ProgrammingError, match=error):
            cur.fetch_lob(col, lob)
        assert lob._handle is None
        result = cur.fetch_row()
        assert result == (1, BLOB_CELL, CLOB_CELL)
    finally:
        conn.close()


@pytest.mark.parametrize("col", [True, "2", 2.0, None])
@pytest.mark.parametrize("at_end", [False, True])
def test_non_int_column_raises_type_error_even_at_the_end(
    driver: LobDriver, col: Any, at_end: bool
) -> None:
    # The official PyArg_ParseTuple("iO!") rejects a non-int before any work.
    conn = _conn(driver)
    try:
        cur = _selected(conn)
        if at_end:
            for _ in range(3):
                cur.fetch_row()
        with pytest.raises(TypeError):
            cur.fetch_lob(col, conn.lob())
        if not at_end:
            result = cur.fetch_row()
            assert result == (1, BLOB_CELL, CLOB_CELL)
    finally:
        conn.close()


def test_fetch_lob_state_errors_keep_the_row(driver: LobDriver) -> None:
    conn = _conn(driver)
    other = native.connect(DSN)
    try:
        cur = _selected(conn)
        with pytest.raises(TypeError):
            cur.fetch_lob(2, object())
        closed = conn.lob()
        closed.close()
        closed.close()  # idempotent
        with pytest.raises(InterfaceError, match="closed"):
            cur.fetch_lob(2, closed)
        with pytest.raises(InterfaceError, match="another connection"):
            cur.fetch_lob(2, other.lob())
        result = cur.fetch_row()
        assert result == (1, BLOB_CELL, CLOB_CELL)

        fresh = conn.cursor()
        fresh.prepare("SELECT id, b, c FROM t")
        # Same as fetch_row() before execute().
        with pytest.raises(InterfaceError, match="invalidated"):
            fresh.fetch_lob(2, conn.lob())
        ins = _insert(conn)
        ins.bind_param(1, 1)
        ins.execute()
        with pytest.raises(InterfaceError, match="no SELECT result"):
            ins.fetch_lob(1, conn.lob())
    finally:
        other.close()
        conn.close()


def test_fetch_lob_after_rollback_is_invalidated(driver: LobDriver) -> None:
    conn = _conn(driver)
    try:
        cur = _selected(conn)
        conn.rollback()
        with pytest.raises(InterfaceError, match="invalidated"):
            cur.fetch_lob(2, conn.lob())
    finally:
        conn.close()


def _bad_handle(handle: bytes) -> bytes:
    # db_type 34 (CLOB) inside a BLOB cell.
    return struct.pack(">i", 34) + handle[4:]


@pytest.mark.parametrize(
    "cell",
    [CLOB_CELL, "text", 7, {"lob_type": INT}],
    ids=["other-lob-type", "text", "int", "non-lob-dict"],
)
def test_mismatched_complete_cell_is_a_data_error(
    driver: LobDriver, monkeypatch: pytest.MonkeyPatch, cell: Any
) -> None:
    # A complete reply holding a value of another type (#492/#512): the
    # session and the row survive, as for the other fetch_lob rejections.
    monkeypatch.setattr(LobDriver, "result", [(1, cell, CLOB_CELL), (2, None, None)])
    conn = _conn(driver)
    try:
        cur = _selected(conn)
        lob = conn.lob()
        with pytest.raises(DataError, match="does not match"):
            cur.fetch_lob(2, lob)
        assert lob._handle is None
        assert driver.discarded == 0
        row = cur.fetch_row()
        assert row == (1, cell, CLOB_CELL)
        cur.fetch_lob(3, lob)  # the session and result stay usable
        assert lob._handle is None  # row 2 has a NULL cell
    finally:
        conn.close()


@pytest.mark.parametrize(
    "handle",
    [_bad_handle(FETCHED_BLOB), FETCHED_BLOB[:-1], b"", None],
    ids=["db-type", "framing", "empty", "missing"],
)
def test_damaged_handle_framing_retires_the_session(
    driver: LobDriver, monkeypatch: pytest.MonkeyPatch, handle: Any
) -> None:
    monkeypatch.setattr(
        LobDriver, "result", [(1, {**BLOB_CELL, "packed_lob_handle": handle}, CLOB_CELL)]
    )
    conn = _conn(driver)
    try:
        cur = _selected(conn)
        lob = conn.lob()
        with pytest.raises(OperationalError, match="malformed response from broker"):
            cur.fetch_lob(2, lob)
        assert lob._handle is None
        assert driver.discarded == 1
        with pytest.raises(InterfaceError):
            cur.fetch_row()
    finally:
        conn.close()


def test_arguments_are_parsed_before_cursor_state(driver: LobDriver) -> None:
    # PyArg_ParseTuple("iO!") runs before the official cursor looks at its
    # result, so argument TypeErrors win over result-state errors.
    conn = _conn(driver)
    try:
        cur = conn.cursor()
        with pytest.raises(TypeError):
            cur.fetch_lob("2", conn.lob())  # not even prepared
        with pytest.raises(TypeError):
            cur.fetch_lob(2, object())
        with pytest.raises(TypeError):
            cur.bind_lob(1, object())
        # The official "iO!" parses the index before the lob.
        for index in ("1", 1.5, None):
            with pytest.raises(TypeError, match="index"):
                cur.bind_lob(index, object())
        cur.prepare("SELECT id, b, c FROM t")
        with pytest.raises(TypeError):
            cur.fetch_lob("2", conn.lob())  # result not executed yet
        cur.close()
        with pytest.raises(InterfaceError):
            cur.fetch_lob("2", conn.lob())  # a closed cursor is checked first
    finally:
        conn.close()


def test_bind_lob_rejections_leave_the_slot_unchanged(driver: LobDriver) -> None:
    conn = _conn(driver)
    other = native.connect(DSN)
    try:
        cur = _selected(conn)
        good = conn.lob()
        cur.fetch_lob(2, good)
        ins = _insert(conn)
        ins.bind_param(1, 7)
        before = list(ins._bindings)
        created = other.lob()
        other_cur = other.cursor()
        other_cur.prepare("SELECT id, b, c FROM t")
        other_cur.execute()
        other_cur.fetch_lob(2, created)
        # What #442 lob.write() will record for a LOB_NEW handle.
        created._set(created._lob_type, created._handle, native._CREATED, created._state[3])
        closed = conn.lob()
        cur.fetch_lob(2, closed)
        closed.close()
        cases: list[tuple[Any, Any, type[Exception], str]] = [
            (1, b"handle", TypeError, "connection.lob"),
            (1, FETCHED_BLOB, TypeError, "connection.lob"),
            (1, None, TypeError, "connection.lob"),
            (0, good, ProgrammingError, "out of range"),
            (2, good, ProgrammingError, "out of range"),
            (True, good, ProgrammingError, "out of range"),
            (1, conn.lob(), InterfaceError, "no value"),
            (1, closed, InterfaceError, "closed"),
            (1, created, InterfaceError, "another connection"),
        ]
        for index, value, error, message in cases:
            with pytest.raises(error, match=message):
                ins.bind_lob(index, value)
            assert ins._bindings == before
        sent = len(driver.requests)
        result = ins.execute()
        assert result == 1
        assert len(driver.requests) == sent + 1
    finally:
        other.close()
        conn.close()


def test_fetched_handle_binds_on_another_open_connection(driver: LobDriver) -> None:
    conn = _conn(driver)
    other = native.connect(DSN)
    try:
        cur = _selected(conn)
        lob = conn.lob()
        cur.fetch_lob(2, lob)
        assert lob._origin == native._FETCHED
        ins = other.cursor()
        ins.prepare("INSERT INTO t2 VALUES (?)")
        ins.bind_lob(1, lob)
        binding = ins._bindings[0]
        # The binding belongs to the session it is sent on.
        assert binding.owner is other._driver and binding.generation == 1
        assert _bind_pair(binding) == bytes.fromhex(OFFICIAL_BIND_PAIRS[0][2])
        result = ins.execute()
        assert result == 1
        # The server copies the committed file, so the handle stays bindable
        # after its source connection closes, as in the official driver.
        conn.close()
        ins.bind_lob(1, lob)
        result = ins.execute()
        assert result == 1
    finally:
        other.close()
        conn.close()


def test_end_of_result_returns_none_before_checking_col_or_lob(driver: LobDriver) -> None:
    # The official driver returns None at the end before looking at col.
    conn = _conn(driver)
    other = native.connect(DSN)
    try:
        cur = _selected(conn)
        for _ in range(3):
            cur.fetch_row()
        closed = conn.lob()
        closed.close()
        for col, lob in (
            (1, conn.lob()),
            (0, conn.lob()),
            (9, conn.lob()),
            (2, closed),
            (2, other.lob()),
        ):
            result = cur.fetch_lob(col, lob)
            assert result is None
        with pytest.raises(TypeError):
            cur.fetch_lob(2, object())
    finally:
        other.close()
        conn.close()


def test_a_fetched_handle_survives_a_reconnect_but_a_created_one_does_not(
    driver: LobDriver,
) -> None:
    conn = _conn(driver)
    try:
        cur = _selected(conn)
        fetched, created = conn.lob(), conn.lob()
        cur.fetch_lob(2, fetched)
        cur.fetch_row()  # row 2 has NULL cells
        cur.fetch_lob(2, created)
        created._set(created._lob_type, created._handle, native._CREATED, created._state[3])
        driver._physical_generation += 1  # the physical session was replaced
        ins = _insert(conn)
        with pytest.raises(InterfaceError, match="earlier physical session"):
            ins.bind_lob(1, created)
        # A fetched (stored) handle is copied by the server on any session;
        # the binding belongs to the current one.
        ins.bind_lob(1, fetched)
        assert ins._bindings[0].generation == 2
        result = ins.execute()
        assert result == 1
    finally:
        conn.close()


def test_a_created_handle_never_crosses_connections(driver: LobDriver) -> None:
    conn = _conn(driver)
    other = native.connect(DSN)
    try:
        cur = _selected(conn)
        lob = conn.lob()
        cur.fetch_lob(2, lob)
        lob._set(lob._lob_type, lob._handle, native._CREATED, lob._state[3])
        ins = other.cursor()
        ins.prepare("INSERT INTO t2 VALUES (?)")
        with pytest.raises(InterfaceError, match="another connection"):
            ins.bind_lob(1, lob)
        own = _insert(conn)
        own.bind_lob(1, lob)  # its own session is still current
    finally:
        other.close()
        conn.close()


def test_an_uncommitted_fetched_handle_never_crosses_connections(driver: LobDriver) -> None:
    # A non-autocommit source could hand over an uncommitted row, which the
    # server would copy permanently.
    conn = _conn(driver)
    other = native.connect(DSN)
    try:
        cur = _selected(conn)
        lob = conn.lob()
        cur.fetch_lob(2, lob)
        lob._set(lob._lob_type, lob._handle, native._FETCHED, lob._state[3], committed=False)
        ins = other.cursor()
        ins.prepare("INSERT INTO t2 VALUES (?)")
        with pytest.raises(InterfaceError, match="committed row"):
            ins.bind_lob(1, lob)
        own = _insert(conn)
        own.bind_lob(1, lob)
    finally:
        other.close()
        conn.close()


def test_fetch_records_the_autocommit_source(
    driver: LobDriver, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = _conn(driver)
    try:
        cur = _selected(conn)
        lob = conn.lob()
        cur.fetch_lob(2, lob)
        assert lob._state[4] is True
        monkeypatch.setattr(driver, "autocommit", False)
        cur.fetch_lob(2, lob)
        assert lob._state[4] is False
    finally:
        conn.close()


def test_native_connection_stays_autocommit_only() -> None:
    # Cross-connection binds of fetched handles rely on the source being
    # autocommit (its rows committed). Adding an autocommit setter must
    # revisit lob._bindable(), so make that change fail here first.
    for name in ("autocommit", "set_autocommit"):
        assert not hasattr(native.connection, name)
    for factory in (native.connection.__init__, native.connect):
        assert "autocommit" not in inspect.signature(factory).parameters


def test_closed_cursor_and_connection_raise_interface_error(driver: LobDriver) -> None:
    conn = _conn(driver)
    cur = _selected(conn)
    lob = conn.lob()
    cur.fetch_lob(2, lob)
    ins = _insert(conn)
    ins.close()
    with pytest.raises(InterfaceError):
        ins.bind_lob(1, lob)
    conn.close()
    with pytest.raises(InterfaceError):
        cur.fetch_lob(2, lob)
    with pytest.raises(InterfaceError, match="closed"):
        conn.lob()
    with pytest.raises(InterfaceError, match="closed"):
        native.lob(conn)


def test_lob_constructor_requires_a_compatibility_connection() -> None:
    not_a_connection: Any = object()
    with pytest.raises(TypeError):
        native.lob(not_a_connection)


def test_lob_performs_no_io(driver: LobDriver) -> None:
    conn = _conn(driver)
    try:
        lob = conn.lob()
        assert isinstance(lob, native.lob)
        assert lob._lob_type == BLOB and lob._handle is None
        lob.close()
        assert driver.requests == []
    finally:
        conn.close()


def test_a_binding_made_before_close_keeps_its_handle(driver: LobDriver) -> None:
    conn = _conn(driver)
    try:
        cur = _selected(conn)
        lob = conn.lob()
        cur.fetch_lob(2, lob)
        ins = _insert(conn)
        ins.bind_lob(1, lob)
        lob.close()
        result = ins.execute()
        assert result == 1
        sent = _packets(driver, ExecutePacket)[-1]
        assert _bind_pair(sent.bindings[0]) == bytes.fromhex(OFFICIAL_BIND_PAIRS[0][2])
    finally:
        conn.close()
