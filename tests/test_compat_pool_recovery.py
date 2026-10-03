"""Deferred same-session refresh of an errored native prepared handle (#611)."""

from __future__ import annotations

from collections.abc import Generator
from typing import Any

import pytest

from pycubrid.compat import native
from pycubrid.constants import CUBRIDDataType, CUBRIDStatementType
from pycubrid.exceptions import DatabaseError, InterfaceError, OperationalError, ProgrammingError
from pycubrid.protocol import (
    CloseQueryPacket,
    ColumnMetaData,
    ExecutePacket,
    GetDbParameterPacket,
    PreparePacket,
    _PreparedLob,
)

from .test_compat_prepared import DSN, FakeDriver
from .test_prepared_lob_contract import FETCHED_BLOB


class PoolDriver(FakeDriver):
    """Model an errored pooled handle whose later FC3 reports legacy -1024."""

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.next_handle = 40
        self.errored_handles: set[int] = set()
        self.server_errors_remaining = 2
        self.effects = 0
        self.close_allow_reconnect: list[bool] = []
        self.reconnect_flags: list[tuple[object, bool]] = []
        self.refresh_bind_count: int | None = None
        self.incomplete_error_once = False
        self.effect_before_error = False
        self.nonserver_error_once = False
        self.after_close: Any = None
        self.after_prepare: Any = None
        self.initial_columns: list[ColumnMetaData] = []
        self.refreshed_columns: list[ColumnMetaData] = []

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
        self.reconnect_flags.append((packet, allow_reconnect))
        if isinstance(packet, PreparePacket):
            result = super()._send_and_receive(
                packet, allow_reconnect=allow_reconnect, expected_generation=expected_generation
            )
            self.next_handle += 1
            packet.query_handle = self.next_handle
            if packet.statement_type == CUBRIDStatementType.SELECT:
                packet.columns = list(
                    self.initial_columns if self.next_handle == 41 else self.refreshed_columns
                )
            if self.next_handle > 41 and self.refresh_bind_count is not None:
                packet.bind_count = self.refresh_bind_count
            if self.after_prepare is not None:
                self.after_prepare()
            return result
        if isinstance(packet, CloseQueryPacket):
            self.close_allow_reconnect.append(allow_reconnect)
            result = super()._send_and_receive(
                packet, allow_reconnect=allow_reconnect, expected_generation=expected_generation
            )
            if self.after_close is not None:
                self.after_close()
            return result
        if isinstance(packet, ExecutePacket):
            assert expected_generation == self._physical_generation
            self.requests.append((packet, expected_generation))
            if self.incomplete_error_once:
                self.incomplete_error_once = False
                raise OperationalError("transport lost during execute")
            if self.nonserver_error_once:
                self.nonserver_error_once = False
                raise ProgrammingError("local callback failed", code=-494, errno=-494)
            if self.server_errors_remaining:
                self.server_errors_remaining -= 1
                code = -1024 if packet.query_handle in self.errored_handles else -494
                self.errored_handles.add(packet.query_handle)
                if self.effect_before_error:
                    self.effects += 1
                error = ProgrammingError("secret SQL and bound value", code=code, errno=code)
                setattr(error, "_cas_server_error", True)
                raise error
            self.effects += 1
            packet.total_tuple_count = 1
            if packet.statement_type == CUBRIDStatementType.SELECT:
                packet.rows = [("refreshed",)]
            return packet
        return super()._send_and_receive(
            packet, allow_reconnect=allow_reconnect, expected_generation=expected_generation
        )


@pytest.fixture
def owned(
    monkeypatch: pytest.MonkeyPatch,
) -> Generator[tuple[native.connection, PoolDriver], None, None]:
    monkeypatch.setattr(native, "_DriverConnection", PoolDriver)
    conn = native.connect(DSN)
    try:
        yield conn, conn._driver
    finally:
        conn.close()


def _sent(driver: PoolDriver, kind: type) -> list[Any]:
    return [packet for packet, _generation in driver.requests if isinstance(packet, kind)]


def test_repeated_complete_errors_refresh_before_next_explicit_call_only(
    owned: tuple[native.connection, PoolDriver],
) -> None:
    conn, driver = owned
    cur = conn.cursor()
    sql = "INSERT INTO confidential_table VALUES (?)"
    cur.prepare(sql)
    for index, value in enumerate(("first", "second"), 1):
        cur.bind_param(1, value)
        before = len(_sent(driver, ExecutePacket))
        with pytest.raises(DatabaseError) as caught:
            cur.execute()
        assert caught.value.errno == -494
        assert "confidential_table" not in str(caught.value)
        assert "secret SQL" not in str(caught.value)
        assert caught.value.__context__ is None
        assert getattr(caught.value, "_cas_server_error", False) is True
        assert len(_sent(driver, ExecutePacket)) == before + 1
        assert len(_sent(driver, PreparePacket)) == index
    cur.bind_param(1, "good")
    result = cur.execute()
    assert result == 1
    assert driver.effects == 1
    assert len(_sent(driver, ExecutePacket)) == 3
    assert len(_sent(driver, PreparePacket)) == 3
    assert [packet.sql for packet in _sent(driver, PreparePacket)] == [sql] * 3
    assert len(_sent(driver, CloseQueryPacket)) == 2
    assert driver.close_allow_reconnect == [False, False]
    assert [
        flag for packet, flag in driver.reconnect_flags if isinstance(packet, PreparePacket)
    ] == [
        True,
        False,
        False,
    ]
    assert [
        flag for packet, flag in driver.reconnect_flags if isinstance(packet, ExecutePacket)
    ] == [
        True,
        False,
        False,
    ]
    assert "confidential_table" not in repr(cur)
    cur.close()


def test_ambiguous_post_effect_pool_error_is_not_replayed_in_the_same_call(
    owned: tuple[native.connection, PoolDriver],
) -> None:
    conn, driver = owned
    driver.server_errors_remaining = 1
    driver.effect_before_error = True
    cur = conn.cursor()
    cur.prepare("INSERT INTO t VALUES (?)")
    driver.errored_handles.add(cur._handle)
    cur.bind_param(1, 1)
    with pytest.raises(DatabaseError) as caught:
        cur.execute()
    assert caught.value.errno == -1024
    assert driver.effects == 1
    assert len(_sent(driver, ExecutePacket)) == 1
    cur.bind_param(1, 2)
    result = cur.execute()  # A second explicit user intent, not a replay.
    assert result == 1
    assert driver.effects == 2
    assert len(_sent(driver, ExecutePacket)) == 2
    cur.close()


@pytest.mark.parametrize("failure", ("transport", "nonserver"))
def test_incomplete_or_local_error_does_not_schedule_refresh(
    owned: tuple[native.connection, PoolDriver], failure: str
) -> None:
    conn, driver = owned
    driver.server_errors_remaining = 0
    driver.incomplete_error_once = failure == "transport"
    driver.nonserver_error_once = failure == "nonserver"
    cur = conn.cursor()
    cur.prepare("INSERT INTO t VALUES (?)")
    cur.bind_param(1, 1)
    with pytest.raises(DatabaseError):
        cur.execute()
    assert cur._needs_reprepare is False
    cur.bind_param(1, 2)
    result = cur.execute()
    assert result == 1
    assert len(_sent(driver, PreparePacket)) == 1
    assert len(_sent(driver, ExecutePacket)) == 2
    cur.close()


def test_refreshed_bind_count_drift_stops_before_execute_and_owns_new_handle(
    owned: tuple[native.connection, PoolDriver],
) -> None:
    conn, driver = owned
    driver.server_errors_remaining = 1
    cur = conn.cursor()
    cur.prepare("INSERT INTO t VALUES (?)")
    cur.bind_param(1, 1)
    with pytest.raises(DatabaseError):
        cur.execute()
    driver.refresh_bind_count = 2
    with pytest.raises(ProgrammingError, match="parameter count changed"):
        cur.execute()
    assert len(_sent(driver, ExecutePacket)) == 1
    assert len(_sent(driver, PreparePacket)) == 2
    assert cur._handle == 42
    assert cur._bind_count == 2
    assert cur._bindings == [None, None]
    cur.bind_param(1, 2)
    cur.bind_param(2, 3)
    result = cur.execute()
    assert result == 1
    assert driver.effects == 1
    cur.close()


def test_select_refresh_uses_new_column_metadata_and_row_shape(
    owned: tuple[native.connection, PoolDriver],
) -> None:
    conn, driver = owned
    driver.server_errors_remaining = 1
    driver.initial_columns = [ColumnMetaData(name="old", column_type=CUBRIDDataType.INT)]
    driver.refreshed_columns = [
        ColumnMetaData(name="new", column_type=CUBRIDDataType.STRING, precision=40)
    ]
    cur = conn.cursor()
    cur.prepare("SELECT ? AS value")
    cur.bind_param(1, "first")
    with pytest.raises(DatabaseError):
        cur.execute()
    count = cur.execute()
    assert count == 1
    assert cur._statement_type == CUBRIDStatementType.SELECT
    assert [column.name for column in cur._columns] == ["new"]
    assert cur._columns[0].precision == 40
    executions = _sent(driver, ExecutePacket)
    assert [column.name for column in executions[-1].columns] == ["new"]
    row = cur.fetch_row()
    assert row == ("refreshed",)
    cur.close()


def test_close_failure_aborts_refresh_without_new_prepare_or_execute(
    owned: tuple[native.connection, PoolDriver],
) -> None:
    conn, driver = owned
    driver.server_errors_remaining = 1
    cur = conn.cursor()
    cur.prepare("INSERT INTO t VALUES (?)")
    cur.bind_param(1, 1)
    with pytest.raises(DatabaseError):
        cur.execute()
    driver.fail_packet_type = CloseQueryPacket
    driver.fail_packet_error = OperationalError("close outcome unknown")
    with pytest.raises(OperationalError, match="close outcome unknown"):
        cur.execute()
    assert cur._handle is None
    assert cur._prepared_sql is None
    assert len(_sent(driver, PreparePacket)) == 1
    assert len(_sent(driver, ExecutePacket)) == 1
    cur.close()


def test_prepare_failure_aborts_refresh_and_does_not_expose_sql(
    owned: tuple[native.connection, PoolDriver],
) -> None:
    conn, driver = owned
    driver.server_errors_remaining = 1
    cur = conn.cursor()
    cur.prepare("INSERT INTO secret_table VALUES (?)")
    cur.bind_param(1, "secret-bound-value")
    with pytest.raises(DatabaseError):
        cur.execute()
    failure = ProgrammingError("secret_table and secret-bound-value", code=-494, errno=-494)
    setattr(failure, "_cas_server_error", True)
    driver.fail_packet_type = PreparePacket
    driver.fail_packet_error = failure
    with pytest.raises(DatabaseError) as caught:
        cur.execute()
    assert "secret_table" not in str(caught.value)
    assert "secret-bound-value" not in repr(caught.value)
    assert caught.value.__context__ is None
    assert cur._handle is None
    assert cur._prepared_sql is None
    assert len(_sent(driver, ExecutePacket)) == 1
    cur.close()


def test_generation_or_driver_replacement_cannot_replay_old_statement(
    owned: tuple[native.connection, PoolDriver],
) -> None:
    conn, driver = owned
    driver.server_errors_remaining = 1
    cur = conn.cursor()
    cur.prepare("INSERT INTO t VALUES (?)")
    cur.bind_param(1, 1)
    with pytest.raises(DatabaseError):
        cur.execute()
    replacement = PoolDriver(**driver.options)
    conn._driver = replacement
    try:
        with pytest.raises(InterfaceError, match="another connection"):
            cur.execute()
        assert replacement.requests == []
    finally:
        conn._driver = driver
        replacement.close()
    driver._physical_generation += 1
    with pytest.raises(InterfaceError, match="earlier physical session"):
        cur.execute()
    assert len(_sent(driver, ExecutePacket)) == 1
    cur.close()


def test_driver_switch_during_close_fails_before_reprepare(
    owned: tuple[native.connection, PoolDriver],
) -> None:
    conn, driver = owned
    driver.server_errors_remaining = 1
    cur = conn.cursor()
    cur.prepare("INSERT INTO t VALUES (?)")
    cur.bind_param(1, 1)
    with pytest.raises(DatabaseError):
        cur.execute()
    replacement = PoolDriver(**driver.options)
    driver.after_close = lambda: setattr(conn, "_driver", replacement)
    try:
        with pytest.raises(InterfaceError, match="session changed"):
            cur.execute()
        assert len(_sent(driver, PreparePacket)) == 1
        assert len(_sent(driver, ExecutePacket)) == 1
        assert replacement.requests == []
    finally:
        conn._driver = driver
        driver.after_close = None
        replacement.close()
    cur.close()


def test_driver_switch_after_fresh_prepare_fails_before_execute(
    owned: tuple[native.connection, PoolDriver],
) -> None:
    conn, driver = owned
    driver.server_errors_remaining = 1
    cur = conn.cursor()
    cur.prepare("INSERT INTO t VALUES (?)")
    cur.bind_param(1, 1)
    with pytest.raises(DatabaseError):
        cur.execute()
    replacement = PoolDriver(**driver.options)
    driver.after_prepare = lambda: setattr(conn, "_driver", replacement)
    try:
        with pytest.raises(InterfaceError, match="session changed"):
            cur.execute()
        assert len(_sent(driver, PreparePacket)) == 2
        assert len(_sent(driver, ExecutePacket)) == 1
        assert replacement.requests == []
    finally:
        conn._driver = driver
        driver.after_prepare = None
        replacement.close()
    cur.close()  # The new handle still belongs to the original driver.
    assert len(_sent(driver, CloseQueryPacket)) == 2


def test_autocommit_change_during_close_fails_before_reprepare(
    owned: tuple[native.connection, PoolDriver],
) -> None:
    conn, driver = owned
    driver.server_errors_remaining = 1
    cur = conn.cursor()
    cur.prepare("INSERT INTO t VALUES (?)")
    cur.bind_param(1, 1)
    with pytest.raises(DatabaseError):
        cur.execute()
    driver.after_close = lambda: setattr(driver, "_autocommit", False)
    with pytest.raises(InterfaceError, match="session changed"):
        cur.execute()
    assert len(_sent(driver, PreparePacket)) == 1
    assert len(_sent(driver, ExecutePacket)) == 1
    driver.after_close = None
    cur.close()


def test_explicit_new_prepare_clears_deferred_refresh(
    owned: tuple[native.connection, PoolDriver],
) -> None:
    conn, driver = owned
    driver.server_errors_remaining = 1
    cur = conn.cursor()
    cur.prepare("INSERT INTO t VALUES (?)")
    cur.bind_param(1, 1)
    with pytest.raises(DatabaseError):
        cur.execute()
    cur.prepare("INSERT INTO other_t VALUES (?)")
    cur.bind_param(1, 2)
    result = cur.execute()
    assert result == 1
    assert len(_sent(driver, PreparePacket)) == 2
    assert len(_sent(driver, ExecutePacket)) == 2
    assert cur._needs_reprepare is False
    cur.close()


def test_lob_snapshot_is_not_automatically_rebound_after_server_error(
    owned: tuple[native.connection, PoolDriver],
) -> None:
    conn, driver = owned
    cur = conn.cursor()
    cur.prepare("INSERT INTO t VALUES (?)")
    cur._bindings[0] = _PreparedLob(
        CUBRIDDataType.BLOB, FETCHED_BLOB, driver, driver._physical_generation
    )
    with pytest.raises(DatabaseError):
        cur.execute()
    assert cur._needs_reprepare is False
    with pytest.raises(DatabaseError) as caught:
        cur.execute()
    assert caught.value.errno == -1024
    assert len(_sent(driver, PreparePacket)) == 1
    cur.close()
