"""Public sync prepared-owner contract for the first scalar compatibility slice."""

from __future__ import annotations

import struct
from threading import RLock
from typing import Any

import pytest
from hypothesis import HealthCheck, given, settings, strategies as st

from pycubrid.compat import native
from pycubrid.constants import CUBRIDStatementType
from pycubrid.exceptions import (
    DataError,
    InterfaceError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
)
from pycubrid.protocol import (
    CloseQueryPacket,
    ExecutePacket,
    FetchPacket,
    PrepareAndExecutePacket,
    PreparePacket,
)


DSN = "CUBRID:127.0.0.1:33000:testdb:::"


class FakeDriver:
    """Record public-owner requests without emulating CAS framing or a broker."""

    created: list[FakeDriver] = []

    def __init__(self, **kwargs: Any) -> None:
        self.options = kwargs
        self._session_lock = RLock()
        self._physical_generation = 1
        self._statement_pooling: int | None = 1
        self._protocol_version = 8
        self._fetch_size = 2
        self._decode_collections = False
        self._json_deserializer = None
        self._encoding = "utf-8"
        self._connected = True
        self._socket = object()
        self.autocommit = kwargs["autocommit"]
        self.requests: list[tuple[object, int | None]] = []
        self.rows: list[tuple[object, ...]] = []
        self.close_calls = 0
        self.commit_calls = 0
        self.rollback_calls = 0
        self.fail_boundary = False
        self.fail_packet_type: type | None = None
        self.fail_packet_error: Exception | None = None
        self.drop_on_packet_failure = False
        self.created.append(self)

    def _send_and_receive(self, packet: Any, *, expected_generation: int | None = None) -> Any:
        if expected_generation != self._physical_generation:
            raise InterfaceError("stale prepared owner")
        self.requests.append((packet, expected_generation))
        if self.fail_packet_type is not None and isinstance(packet, self.fail_packet_type):
            if self.drop_on_packet_failure:
                self._connected = False
                self._socket = None
            assert self.fail_packet_error is not None
            raise self.fail_packet_error
        if isinstance(packet, PreparePacket):
            packet.query_handle = 37
            packet.bind_count = packet.sql.count("?")
            packet.statement_type = (
                CUBRIDStatementType.SELECT
                if packet.sql.lstrip().upper().startswith("SELECT")
                else CUBRIDStatementType.INSERT
            )
            packet.columns = []
        elif isinstance(packet, ExecutePacket):
            packet.total_tuple_count = 1
            if packet.statement_type == CUBRIDStatementType.SELECT:
                value = packet.bindings[0] if packet.bindings else None
                if value is None or value.type_code == 0:
                    decoded: object = None
                elif value.type_code == 8:
                    decoded = struct.unpack(">i", value.payload)[0]
                else:
                    decoded = value.payload[:-1].decode("utf-8")
                self.rows = [(decoded,), ("later",)]
                packet.rows = self.rows[:1]
                packet.tuple_count = 1
                packet.total_tuple_count = 2
        elif isinstance(packet, FetchPacket):
            packet.rows = self.rows[packet.current_tuple_count :]
            packet.tuple_count = len(packet.rows)
        return packet

    def commit(self) -> None:
        self.commit_calls += 1
        if self.fail_boundary:
            raise RuntimeError("commit failed")

    def rollback(self) -> None:
        self.rollback_calls += 1
        if self.fail_boundary:
            raise RuntimeError("rollback failed")

    def close(self) -> None:
        self.close_calls += 1
        self._connected = False


@pytest.fixture
def fake_driver(monkeypatch: pytest.MonkeyPatch) -> FakeDriver:
    FakeDriver.created.clear()
    monkeypatch.setattr(native, "_DriverConnection", FakeDriver)
    conn = native.connect(DSN)
    conn._driver._native_connection = conn
    return conn._driver


def _owner(fake_driver: FakeDriver) -> tuple[native.connection, Any]:
    native_conn = fake_driver._native_connection
    return native_conn, native_conn.cursor()


def _packets(driver: FakeDriver, kind: type) -> list[Any]:
    return [packet for packet, _generation in driver.requests if isinstance(packet, kind)]


def test_public_cursor_reuses_one_handle_and_scalar_bindings(fake_driver: FakeDriver) -> None:
    conn, cur = _owner(fake_driver)
    try:
        assert cur.prepare("SELECT CAST(? AS INTEGER)") is None
        cur.bind_param(1, 11)
        assert cur.execute() == 2
        assert cur.fetch_row() == (11,)
        assert cur.fetch_row() == ("later",)
        assert cur.fetch_row() is None
        cur.bind_param(1, 12)
        assert cur.execute() == 2
        assert cur.fetch_row() == (12,)
        assert len(_packets(fake_driver, PreparePacket)) == 1
        executions = _packets(fake_driver, ExecutePacket)
        assert len(executions) == 2
        assert [p.query_handle for p in executions] == [37, 37]
        assert [p.bindings[0].payload for p in executions] == [
            struct.pack(">i", 11),
            struct.pack(">i", 12),
        ]
        assert all(generation == 1 for _, generation in fake_driver.requests)
    finally:
        conn.close()
    assert [p.query_handle for p in _packets(fake_driver, CloseQueryPacket)] == [37]
    assert fake_driver.close_calls == 1


@pytest.mark.parametrize("value", [None, "", "a'\\한"])
def test_null_and_utf8_values_are_typed_not_substituted(
    fake_driver: FakeDriver, value: str | None
) -> None:
    conn, cur = _owner(fake_driver)
    try:
        cur.prepare("SELECT CAST(? AS VARCHAR(40))")
        cur.bind_param(1, value)
        cur.execute()
        assert cur.fetch_row() == (value,)
        assert len(_packets(fake_driver, PreparePacket)) == 1
        assert _packets(fake_driver, PreparePacket)[0].sql == "SELECT CAST(? AS VARCHAR(40))"
        bound = _packets(fake_driver, ExecutePacket)[0].bindings[0]
        assert (bound.type_code, bound.payload) == (
            (0, b"") if value is None else (1, value.encode("utf-8") + b"\x00")
        )
    finally:
        conn.close()


@pytest.mark.parametrize(
    ("value", "error"),
    [
        (True, ProgrammingError),
        (1.5, ProgrammingError),
        (b"bytes", ProgrammingError),
        (2**31, DataError),
        (-(2**31) - 1, DataError),
        ("bad\x00value", ProgrammingError),
        ("\ud800", DataError),
    ],
)
def test_invalid_bind_does_not_write_or_replace_usable_result(
    fake_driver: FakeDriver, value: object, error: type[Exception]
) -> None:
    conn, cur = _owner(fake_driver)
    try:
        cur.prepare("SELECT ?")
        cur.bind_param(1, 11)
        cur.execute()
        before = len(fake_driver.requests)
        with pytest.raises(error):
            cur.bind_param(1, value)
        assert len(fake_driver.requests) == before
        assert cur.fetch_row() == (11,)
        cur.execute()
        assert cur.fetch_row() == (11,)
    finally:
        conn.close()


@pytest.mark.parametrize(
    ("operation", "error"),
    [
        (lambda c: c.bind_param(0, 1), ProgrammingError),
        (lambda c: c.bind_param(2, 1), ProgrammingError),
        (lambda c: c.bind_param(1, 1, 8), ProgrammingError),
        (lambda c: c.execute(1), ProgrammingError),
        (lambda c: c.execute(0, 1), ProgrammingError),
        (lambda c: c.fetch_row(1), ProgrammingError),
    ],
)
def test_unsupported_modes_and_indexes_reject_without_io(
    fake_driver: FakeDriver, operation: Any, error: type[Exception]
) -> None:
    conn, cur = _owner(fake_driver)
    try:
        cur.prepare("SELECT ?")
        before = len(fake_driver.requests)
        with pytest.raises(error):
            operation(cur)
        assert len(fake_driver.requests) == before
    finally:
        conn.close()


def test_unbound_slot_rejects_before_execute(fake_driver: FakeDriver) -> None:
    conn, cur = _owner(fake_driver)
    try:
        cur.prepare("SELECT ?, ?")
        cur.bind_param(1, None)
        with pytest.raises(ProgrammingError):
            cur.execute()
        assert not _packets(fake_driver, ExecutePacket)
    finally:
        conn.close()


@pytest.mark.parametrize(
    ("sql", "error"), [("SELECT '\x00'", ProgrammingError), ("\ud800", DataError)]
)
def test_bad_sql_fails_before_prepare(
    fake_driver: FakeDriver, sql: str, error: type[Exception]
) -> None:
    conn, cur = _owner(fake_driver)
    try:
        with pytest.raises(error):
            cur.prepare(sql)
        assert not fake_driver.requests
    finally:
        conn.close()


def test_sql_subclass_cannot_change_bytes_between_validation_and_wire(
    fake_driver: FakeDriver,
) -> None:
    class ChangingSQL(str):
        calls = 0

        def encode(self, encoding: str = "utf-8", errors: str = "strict") -> bytes:
            self.calls += 1
            return b"SELECT ?" if self.calls == 1 else b"SELECT secret"

    conn, cur = _owner(fake_driver)
    sql = ChangingSQL("SELECT ?")
    try:
        with pytest.raises(ProgrammingError):
            cur.prepare(sql)
        assert not fake_driver.requests
        assert sql.calls == 0
    finally:
        conn.close()


@pytest.mark.parametrize("pooling", [None, 0, 2])
def test_unmeasured_pooling_rejects_before_prepared_io(
    fake_driver: FakeDriver, pooling: int | None
) -> None:
    fake_driver._statement_pooling = pooling
    conn, cur = _owner(fake_driver)
    try:
        with pytest.raises(NotSupportedError):
            cur.prepare("SELECT ?")
        assert not fake_driver.requests
    finally:
        conn.close()


def test_commit_preserves_result_and_rollback_invalidates_it(fake_driver: FakeDriver) -> None:
    fake_driver.autocommit = False  # Private manual-mode fixture; no public setter in #439.
    conn, cur = _owner(fake_driver)
    try:
        cur.prepare("SELECT ?")
        cur.bind_param(1, 11)
        cur.execute()
        assert cur.fetch_row() == (11,)
        conn.commit()
        assert cur.fetch_row() == ("later",)
        cur.execute()
        conn.rollback()
        with pytest.raises(InterfaceError):
            cur.fetch_row()
        cur.execute()
        assert cur.fetch_row() == (11,)
        assert len(_packets(fake_driver, PreparePacket)) == 1
        assert fake_driver.commit_calls == fake_driver.rollback_calls == 1
    finally:
        conn.close()


@pytest.mark.parametrize("boundary", ["commit", "rollback"])
def test_failed_boundary_does_not_notify_owner(fake_driver: FakeDriver, boundary: str) -> None:
    fake_driver.autocommit = False
    conn, cur = _owner(fake_driver)
    try:
        cur.prepare("SELECT ?")
        cur.bind_param(1, 11)
        cur.execute()
        fake_driver.fail_boundary = True
        with pytest.raises(RuntimeError, match=f"{boundary} failed"):
            getattr(conn, boundary)()
        assert cur.fetch_row() == (11,)
    finally:
        conn.close()


def test_replaced_physical_session_never_sends_old_execute_or_close(
    fake_driver: FakeDriver,
) -> None:
    conn, cur = _owner(fake_driver)
    cur.prepare("SELECT ?")
    cur.bind_param(1, 11)
    fake_driver._physical_generation += 1
    fake_driver._socket = object()
    before = len(fake_driver.requests)
    with pytest.raises(InterfaceError):
        cur.execute()
    cur.close()
    conn.close()
    assert len(fake_driver.requests) == before


def test_close_is_idempotent_and_never_commits(fake_driver: FakeDriver) -> None:
    fake_driver.autocommit = False
    conn, cur = _owner(fake_driver)
    cur.prepare("INSERT INTO t VALUES (?)")
    cur.bind_param(1, 1)
    cur.execute()
    cur.close()
    cur.close()
    conn.close()
    assert len(_packets(fake_driver, CloseQueryPacket)) == 1
    assert fake_driver.commit_calls == 0
    assert fake_driver.close_calls == 1


def test_reprepare_closes_old_handle_before_replacing_it(fake_driver: FakeDriver) -> None:
    conn, cur = _owner(fake_driver)
    try:
        cur.prepare("SELECT ?")
        cur.bind_param(1, 11)
        cur.prepare("SELECT CAST(? AS INTEGER)")
        assert [type(packet) for packet, _ in fake_driver.requests] == [
            PreparePacket,
            CloseQueryPacket,
            PreparePacket,
        ]
        with pytest.raises(ProgrammingError):
            cur.execute()  # A new statement has no inherited bound slot.
        cur.bind_param(1, 12)
        assert cur.execute() == 2
        assert cur.fetch_row() == (12,)
    finally:
        conn.close()


def test_failed_close_during_reprepare_retires_old_result_and_owner(
    fake_driver: FakeDriver,
) -> None:
    conn, cur = _owner(fake_driver)
    cur.prepare("SELECT ?")
    cur.bind_param(1, 11)
    cur.execute()
    assert cur.fetch_row() == (11,)
    fake_driver.fail_packet_type = CloseQueryPacket
    fake_driver.fail_packet_error = OperationalError("close outcome unknown")
    before = len(_packets(fake_driver, PreparePacket))
    with pytest.raises(OperationalError, match="close outcome unknown"):
        cur.prepare("SELECT 2")
    assert len(_packets(fake_driver, PreparePacket)) == before
    assert cur._handle is None
    with pytest.raises(InterfaceError):
        cur.fetch_row()
    fake_driver.fail_packet_type = None
    conn.close()


def test_failed_initial_prepare_does_not_invent_a_close_handle(fake_driver: FakeDriver) -> None:
    conn, cur = _owner(fake_driver)
    fake_driver.fail_packet_type = PreparePacket
    fake_driver.fail_packet_error = ProgrammingError("server rejected SQL")
    with pytest.raises(ProgrammingError, match="server rejected SQL"):
        cur.prepare("SELECT broken")
    assert cur._handle is None
    fake_driver.fail_packet_type = None
    conn.close()
    assert not _packets(fake_driver, CloseQueryPacket)


def test_server_execute_error_cannot_expose_previous_result(fake_driver: FakeDriver) -> None:
    conn, cur = _owner(fake_driver)
    try:
        cur.prepare("SELECT ?")
        cur.bind_param(1, 11)
        cur.execute()
        assert cur.fetch_row() == (11,)
        cur.bind_param(1, 12)
        fake_driver.fail_packet_type = ExecutePacket
        fake_driver.fail_packet_error = ProgrammingError("server rejected execution")
        with pytest.raises(ProgrammingError, match="server rejected execution"):
            cur.execute()
        with pytest.raises(InterfaceError, match="invalidated"):
            cur.fetch_row()  # The old buffered second row is no longer this execution's result.
        fake_driver.fail_packet_type = None
        assert cur.execute() == 2  # A completed SQL error does not retire the handle.
        assert cur.fetch_row() == (12,)
    finally:
        conn.close()


@pytest.mark.parametrize("stage", ["execute", "fetch", "close"])
def test_uncertain_transport_or_cleanup_failure_never_replays_or_commits(
    fake_driver: FakeDriver, stage: str
) -> None:
    conn, cur = _owner(fake_driver)
    cur.prepare("SELECT ?")
    cur.bind_param(1, 11)
    if stage == "execute":
        kind = ExecutePacket
    elif stage == "fetch":
        kind = FetchPacket
        cur.execute()
        assert cur.fetch_row() == (11,)
    else:
        kind = CloseQueryPacket
    fake_driver.fail_packet_type = kind
    fake_driver.fail_packet_error = OperationalError("uncertain transport")
    fake_driver.drop_on_packet_failure = stage != "close"
    if stage == "close":
        conn.close()  # Cleanup errors are secondary; transport close still happens.
    else:
        with pytest.raises(OperationalError, match="uncertain transport"):
            cur.execute() if stage == "execute" else cur.fetch_row()
        conn.close()
    assert len(_packets(fake_driver, kind)) == 1
    assert not _packets(fake_driver, PrepareAndExecutePacket)
    assert fake_driver.commit_calls == 0
    assert fake_driver.close_calls == 1


@given(st.text(alphabet="abc012'\\한", max_size=30))
@settings(max_examples=30, suppress_health_check=[HealthCheck.function_scoped_fixture])
def test_public_owner_keeps_generated_values_out_of_sql(
    fake_driver: FakeDriver, value: str
) -> None:
    conn = native.connect(DSN)
    driver = conn._driver
    try:
        cur = conn.cursor()
        cur.prepare("SELECT CAST(? AS VARCHAR(40))")
        cur.bind_param(1, value)
        cur.execute()
        assert cur.fetch_row() == (value,)
        assert _packets(driver, PreparePacket)[0].sql == "SELECT CAST(? AS VARCHAR(40))"
        assert _packets(driver, ExecutePacket)[0].bindings[0].payload == (
            value.encode("utf-8") + b"\x00"
        )
        assert not _packets(driver, PrepareAndExecutePacket)
    finally:
        conn.close()


@pytest.mark.parametrize("invalid", ["\x00", "\ud800"])
def test_invalid_sql_and_value_do_not_leak_or_send(
    fake_driver: FakeDriver, invalid: str, caplog: pytest.LogCaptureFixture
) -> None:
    conn, cur = _owner(fake_driver)
    secret = "private-marker-quoted'\\한"
    caplog.set_level("DEBUG")
    try:
        error = ProgrammingError if invalid == "\x00" else DataError
        with pytest.raises(error) as sql_error:
            cur.prepare(f"SELECT '{secret}{invalid}'")
        assert not fake_driver.requests
        cur.prepare("SELECT ?")
        before = len(fake_driver.requests)
        with pytest.raises(error) as value_error:
            cur.bind_param(1, secret + invalid)
        assert len(fake_driver.requests) == before
        assert secret not in str(sql_error.value)
        assert secret not in str(value_error.value)
        assert secret not in caplog.text
    finally:
        conn.close()


def test_broker_error_preserves_class_and_code_without_echoing_sql(
    fake_driver: FakeDriver, caplog: pytest.LogCaptureFixture
) -> None:
    conn, cur = _owner(fake_driver)
    secret = "private-marker-quoted'\\한"
    broker_error = ProgrammingError(f"syntax error near {secret}", code=-1, errno=-1)
    broker_error._cas_server_error = True
    fake_driver.fail_packet_type = PreparePacket
    fake_driver.fail_packet_error = broker_error
    caplog.set_level("DEBUG")
    try:
        with pytest.raises(ProgrammingError) as caught:
            cur.prepare(f"SELECT '{secret}'")
        assert caught.value.code == -1
        assert caught.value.errno == -1
        assert secret not in str(caught.value)
        assert secret not in repr(caught.value)
        assert caught.value.__context__ is None
        assert secret not in caplog.text
        assert cur._handle is None
    finally:
        conn.close()
