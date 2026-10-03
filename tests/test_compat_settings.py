"""Cached native members are distinct from effective CCI-style setters (#467)."""

from __future__ import annotations

from threading import RLock
from typing import Any

import pytest

from pycubrid.compat import cubriddb, native
from pycubrid.constants import CCIDbParam, CUBRIDIsolationLevel
from pycubrid.exceptions import DatabaseError, InterfaceError, OperationalError
from pycubrid.protocol import CommitPacket, GetDbParameterPacket, SetDbParameterPacket

DSN = "CUBRID:127.0.0.1:33000:testdb:::"
_NAMES = (
    "CUBRID_REP_CLASS_COMMIT_INSTANCE",
    "CUBRID_REP_CLASS_REP_INSTANCE",
    "CUBRID_SERIALIZABLE",
)


class SettingsDriver:
    """Record the three constructor GETs and each later effective operation."""

    created: list[SettingsDriver] = []
    next_get_error: tuple[int, Exception] | None = None
    next_get_in_tran = False
    next_boundary_replaces_session = False
    next_initial_isolation = 4

    def __init__(self, **kwargs: Any) -> None:
        self.options = kwargs
        self._session_lock = RLock()
        self._connected = True
        self._socket = object()
        self._physical_generation = 1
        self._cas_info = b"\x00\x00\x00\x00"
        self._autocommit = kwargs["autocommit"]
        self._autocommit_explicitly_set = True
        self.actual_isolation = type(self).next_initial_isolation
        self.requests: list[object] = []
        self.get_error = type(self).next_get_error
        self.set_error: Exception | None = None
        self.commit_error: Exception | None = None
        self.drop_calls = 0
        self.close_calls = 0
        self.created.append(self)

    @property
    def autocommit(self) -> bool:
        return self._autocommit

    def _check_reconnect(self) -> bool:
        return False

    def _send_and_receive(
        self,
        packet: Any,
        *,
        allow_reconnect: bool = True,
        expected_generation: int | None = None,
    ) -> Any:
        self.requests.append(packet)
        if isinstance(packet, GetDbParameterPacket):
            if self.get_error is not None and packet.parameter == self.get_error[0]:
                raise self.get_error[1]
            packet.value = {
                CCIDbParam.LOCK_TIMEOUT: -1,
                CCIDbParam.MAX_STRING_LENGTH: 1_073_741_823,
                CCIDbParam.ISOLATION_LEVEL: self.actual_isolation,
                CCIDbParam.AUTO_COMMIT: int(self._autocommit),
            }[packet.parameter]
            if packet.parameter == CCIDbParam.ISOLATION_LEVEL and self.next_get_in_tran:
                self._cas_info = b"\x01\x00\x00\x00"
        elif isinstance(packet, SetDbParameterPacket):
            if self.set_error is not None:
                raise self.set_error
            if packet.parameter == CCIDbParam.ISOLATION_LEVEL:
                self.actual_isolation = packet.value
        elif isinstance(packet, CommitPacket):
            if self.commit_error is not None:
                raise self.commit_error
            self._cas_info = b"\x00\x00\x00\x00"
            if self.next_boundary_replaces_session:
                self._physical_generation += 1
                self.actual_isolation = 4
        return packet

    def commit(self) -> None:
        self._send_and_receive(CommitPacket())

    def rollback(self) -> None:
        self._cas_info = b"\x00\x00\x00\x00"

    def _drop_connection(self) -> None:
        self.drop_calls += 1
        self._connected = False

    def close(self) -> None:
        self.close_calls += 1
        self._connected = False


@pytest.fixture
def owner(monkeypatch: pytest.MonkeyPatch) -> native.connection:
    SettingsDriver.created.clear()
    monkeypatch.setattr(native, "_DriverConnection", SettingsDriver)
    conn = native.connect(DSN)
    try:
        yield conn
    finally:
        conn.close()


def _parameters(driver: SettingsDriver, kind: type) -> list[Any]:
    return [packet for packet in driver.requests if isinstance(packet, kind)]


def test_constructor_records_real_snapshots_and_three_gets(owner: native.connection) -> None:
    driver = owner._driver
    gets = _parameters(driver, GetDbParameterPacket)
    assert [packet.parameter for packet in gets] == [
        CCIDbParam.LOCK_TIMEOUT,
        CCIDbParam.MAX_STRING_LENGTH,
        CCIDbParam.ISOLATION_LEVEL,
    ]
    assert len(driver.requests) == 3
    assert owner.autocommit is True
    assert owner.lock_timeout == -1
    assert owner.max_string_len == 1_073_741_823
    assert owner.isolation_level == "CUBRID_TRAN_UNKNOWN_ISOLATION"


def test_all_raw_member_assignments_preserve_identity_without_packets(
    owner: native.connection,
) -> None:
    driver = owner._driver
    markers = [object() for _ in range(4)]
    before = len(driver.requests)
    for name, marker in zip(
        ("autocommit", "isolation_level", "lock_timeout", "max_string_len"), markers
    ):
        setattr(owner, name, marker)
        assert getattr(owner, name) is marker
    assert len(driver.requests) == before
    assert driver._autocommit is True
    assert driver.actual_isolation == 4


def test_autocommit_setter_uses_effective_mode_not_the_raw_cache(
    owner: native.connection,
) -> None:
    driver = owner._driver
    driver.requests.clear()
    driver._cas_info = b"\x01\x00\x00\x00"  # active transaction
    owner.autocommit = object()  # cache mutation is not an effective change
    result = owner.set_autocommit(False)
    assert result is None
    assert owner.autocommit is False and driver._autocommit is False
    assert [type(packet) for packet in driver.requests] == [CommitPacket]

    driver.requests.clear()
    driver._cas_info = b"\x01\x00\x00\x00"
    owner.autocommit = True
    result = owner.set_autocommit(False)
    assert result is None
    assert owner.autocommit is False and driver._autocommit is False
    assert driver.requests == []  # same effective mode never commits

    owner.autocommit = False
    result = owner.set_autocommit(True)
    assert result is None
    assert owner.autocommit is True and driver._autocommit is True
    assert [type(packet) for packet in driver.requests] == [CommitPacket]


def test_autocommit_out_of_transaction_changes_locally_and_rejects_nonbool(
    owner: native.connection,
) -> None:
    driver = owner._driver
    driver.requests.clear()
    with pytest.raises(InterfaceError):
        owner.set_autocommit(1)
    with pytest.raises(TypeError):
        owner.set_autocommit(mode=False)
    assert driver.requests == []
    result = owner.set_autocommit(False)
    assert result is None
    assert driver.requests == []
    assert driver._autocommit is False and owner.autocommit is False
    result = owner.set_autocommit(True)
    assert result is None
    assert driver.requests == []
    assert driver._autocommit is True and owner.autocommit is True


def test_failed_conditional_commit_keeps_mode_and_cache(owner: native.connection) -> None:
    driver = owner._driver
    driver.requests.clear()
    driver._cas_info = b"\x01\x00\x00\x00"
    cache = object()
    owner.autocommit = cache
    driver.commit_error = DatabaseError("COMMIT rejected")
    with pytest.raises(DatabaseError, match="COMMIT rejected"):
        owner.set_autocommit(False)
    assert owner.autocommit is cache
    assert driver._autocommit is True
    assert [type(packet) for packet in driver.requests] == [CommitPacket]
    assert driver.drop_calls == 1 and driver._connected is False


def test_isolation_setter_uses_actual_level_and_generation_not_raw_text(
    owner: native.connection,
) -> None:
    driver = owner._driver
    driver.requests.clear()
    owner.isolation_level = object()
    result = owner.set_isolation_level(4)
    assert result is None
    assert owner.isolation_level == _NAMES[0]
    assert _parameters(driver, SetDbParameterPacket) == []

    owner.isolation_level = object()
    result = owner.set_isolation_level(CUBRIDIsolationLevel.REP_CLASS_REP_INSTANCE)
    assert result is None
    assert owner.isolation_level == _NAMES[1]
    sets = _parameters(driver, SetDbParameterPacket)
    assert [(packet.parameter, packet.value) for packet in sets] == [
        (CCIDbParam.ISOLATION_LEVEL, 5)
    ]
    assert _parameters(driver, CommitPacket) == []

    driver.requests.clear()
    driver._physical_generation += 1  # same integer on a replacement CAS
    driver.actual_isolation = 4
    result = owner.set_isolation_level(5)
    assert result is None
    assert driver.actual_isolation == 5
    assert _parameters(driver, SetDbParameterPacket) or _parameters(driver, GetDbParameterPacket)


def test_failed_isolation_setting_keeps_old_cached_text(owner: native.connection) -> None:
    driver = owner._driver
    driver.requests.clear()
    cached = object()
    owner.isolation_level = cached
    driver.set_error = DatabaseError("SET rejected")
    with pytest.raises(DatabaseError, match="SET rejected"):
        owner.set_isolation_level(5)
    assert owner.isolation_level is cached
    assert driver.actual_isolation == 4


@pytest.mark.parametrize("level", [True, 1, 3, 7, "4", None])
def test_invalid_or_legacy_isolation_levels_fail_before_wire(
    owner: native.connection, level: object
) -> None:
    driver = owner._driver
    driver.requests.clear()
    cached = owner.isolation_level
    with pytest.raises(InterfaceError):
        owner.set_isolation_level(level)
    assert driver.requests == []
    assert owner.isolation_level == cached
    assert driver.actual_isolation == 4


def test_native_isolation_keyword_is_rejected_before_wire(owner: native.connection) -> None:
    driver = owner._driver
    driver.requests.clear()
    with pytest.raises(TypeError):
        owner.set_isolation_level(level=4)
    assert driver.requests == []


def test_wrapper_cache_getter_and_effective_bool_setter(monkeypatch: pytest.MonkeyPatch) -> None:
    SettingsDriver.created.clear()
    monkeypatch.setattr(native, "_DriverConnection", SettingsDriver)
    wrapper = cubriddb.Connection(DSN)
    try:
        marker = object()
        wrapper.connection.autocommit = marker
        assert wrapper.get_autocommit() is marker
        assert wrapper.autocommit is marker
        before = len(wrapper.connection._driver.requests)
        with pytest.raises(ValueError):
            wrapper.set_autocommit(1)
        assert len(wrapper.connection._driver.requests) == before
        wrapper.set_autocommit(value=False)  # wrapper remains keyword-capable
        assert wrapper.autocommit is False
        assert wrapper.connection._driver._autocommit is False
    finally:
        wrapper.close()


def test_complete_max_string_error_maps_only_that_snapshot_to_zero(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    SettingsDriver.created.clear()
    monkeypatch.setattr(native, "_DriverConnection", SettingsDriver)
    error = DatabaseError("max unavailable")
    setattr(error, "_cas_server_error", True)
    monkeypatch.setattr(SettingsDriver, "next_get_error", (CCIDbParam.MAX_STRING_LENGTH, error))
    conn = native.connect(DSN)
    try:
        assert conn.max_string_len == 0
        assert conn.lock_timeout == -1
        assert conn.isolation_level == "CUBRID_TRAN_UNKNOWN_ISOLATION"
    finally:
        conn.close()


@pytest.mark.parametrize(
    ("parameter", "error"),
    [
        (CCIDbParam.MAX_STRING_LENGTH, OperationalError("transport lost")),
        (CCIDbParam.MAX_STRING_LENGTH, ValueError("malformed reply")),
        (CCIDbParam.LOCK_TIMEOUT, DatabaseError("lock unavailable")),
        (CCIDbParam.ISOLATION_LEVEL, DatabaseError("isolation unavailable")),
    ],
)
def test_other_constructor_read_failures_retire_incomplete_connection(
    monkeypatch: pytest.MonkeyPatch, parameter: int, error: Exception
) -> None:
    SettingsDriver.created.clear()
    monkeypatch.setattr(native, "_DriverConnection", SettingsDriver)
    monkeypatch.setattr(SettingsDriver, "next_get_error", (parameter, error))
    with pytest.raises(type(error), match=str(error)):
        native.connect(DSN)
    driver = SettingsDriver.created[-1]
    assert driver.drop_calls == 1
    assert driver._connected is False


def test_constructor_rejects_a_replacement_during_its_final_boundary(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    SettingsDriver.created.clear()
    monkeypatch.setattr(native, "_DriverConnection", SettingsDriver)
    monkeypatch.setattr(SettingsDriver, "next_get_in_tran", True)
    monkeypatch.setattr(SettingsDriver, "next_boundary_replaces_session", True)
    monkeypatch.setattr(SettingsDriver, "next_initial_isolation", 5)
    with pytest.raises(OperationalError, match="settings session changed"):
        native.connect(DSN)
    driver = SettingsDriver.created[-1]
    assert driver.drop_calls == 1
    assert driver._connected is False


def test_constructor_finishes_an_unexpected_active_get_transaction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    SettingsDriver.created.clear()
    monkeypatch.setattr(native, "_DriverConnection", SettingsDriver)
    monkeypatch.setattr(SettingsDriver, "next_get_in_tran", True)
    monkeypatch.setattr(SettingsDriver, "next_initial_isolation", 5)
    conn = native.connect(DSN)
    try:
        driver = conn._driver
        assert [type(packet) for packet in driver.requests] == [
            GetDbParameterPacket,
            GetDbParameterPacket,
            GetDbParameterPacket,
            CommitPacket,
        ]
        assert driver._cas_info[0] == 0
        assert conn.isolation_level == _NAMES[1]
    finally:
        conn.close()
