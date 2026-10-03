"""Bounded utility contracts over existing real CAS parsers and socket fakes."""

from __future__ import annotations

import struct
from collections.abc import Iterator
from typing import Any
from unittest.mock import MagicMock

import pytest

import pycubrid
from pycubrid.compat import cubriddb, native
from pycubrid.connection import Connection
from pycubrid.constants import CUBRIDDataType as T
from pycubrid.exceptions import DataError, DatabaseError, InterfaceError, OperationalError
from pycubrid.protocol import (
    FetchPacket,
    GetEngineVersionPacket,
    GetSchemaPacket,
    PrepareAndExecutePacket,
)

from .helpers.cas_reply import Column, ResultSet, fetch_reply, int_, prepare_and_execute_reply
from .helpers.replay_broker import (
    IN_TRAN,
    OUT_TRAN,
    cas_info,
    error_body,
    ok_body,
    split_args,
    with_status,
)
from .test_connection import build_server_version_response
from .test_network_edge_cases import make_connected_connection, make_socket_from_chunks


def _result(*values: int) -> ResultSet:
    return ResultSet("utility", (Column("1+1", T.INT),), tuple((int_(value),) for value in values))


def _query(*values: int, total: int | None = None, status: int = IN_TRAN) -> bytes:
    return with_status(prepare_and_execute_reply(_result(*values), total=total).data, status)


def _fetch(*values: int, status: int = IN_TRAN) -> bytes:
    return with_status(fetch_reply(_result(*values)).data, status)


def _load(sock: MagicMock, *bodies: bytes) -> None:
    frames = [struct.pack(">i", len(body) - 4) + body for body in bodies]
    sock.recv_into.side_effect = make_socket_from_chunks(frames).recv_into.side_effect
    sock.sendall.reset_mock()


def _codes(sock: MagicMock) -> list[int]:
    return [call.args[0][8] for call in sock.sendall.call_args_list]


@pytest.fixture
def owned(
    monkeypatch: pytest.MonkeyPatch,
) -> Iterator[tuple[native.connection, Connection, MagicMock]]:
    driver, sock = make_connected_connection()
    driver._protocol_version = 8
    driver._statement_pooling = 1
    driver._broker_db_type = 1
    driver._record_reply_cas_info(cas_info(OUT_TRAN))
    owner = native.connection.__new__(native.connection)
    owner._driver = driver
    owner._closed = False
    owner._session_lock = driver._session_lock
    owner._prepared_owners = set()
    owner.autocommit = False
    owner.lock_timeout = -1
    owner.max_string_len = 1_073_741_823
    owner.isolation_level = "snapshot"
    driver.utility_requests = []
    driver.utility_errors = []
    original_send = driver._send_and_receive

    def send(
        packet: Any,
        *,
        allow_reconnect: bool = True,
        expected_generation: int | None = None,
        **kwargs: Any,
    ) -> Any:
        driver.utility_requests.append((packet, allow_reconnect, expected_generation))
        try:
            return original_send(
                packet,
                allow_reconnect=allow_reconnect,
                expected_generation=expected_generation,
                **kwargs,
            )
        except BaseException as exc:
            driver.utility_errors.append(exc)
            raise

    monkeypatch.setattr(driver, "_send_and_receive", send)
    unexpected_connect = MagicMock(
        side_effect=AssertionError("utility must not open another socket")
    )
    monkeypatch.setattr("socket.create_connection", unexpected_connect)
    driver.utility_connect = unexpected_connect
    sock.sendall.reset_mock()
    try:
        yield owner, driver, sock
    finally:
        driver._drop_connection()
        if owner._driver is not driver:
            owner._driver._drop_connection()


def _guarded(driver: Connection) -> None:
    assert driver.utility_requests
    assert all(not retry and generation == 1 for _, retry, generation in driver.utility_requests)
    driver.utility_connect.assert_not_called()


def test_existing_reply_builders_parse_real_version_and_paged_scalar_results() -> None:
    query = PrepareAndExecutePacket("select 1+1 from db_root", protocol_version=8)
    query.parse(_query(0, total=2))
    fetched = FetchPacket(7, 1, columns=query.columns)
    fetched.parse(_fetch(2))
    version = GetEngineVersionPacket()
    version.parse(build_server_version_response("11.4.6.1963")[4:])
    assert query.rows == [(0,)] and query.total_tuple_count == 2
    assert fetched.rows == [(2,)]
    assert version.engine_version == "11.4.6.1963"
    request_args = split_args(query.write(cas_info(IN_TRAN))[9:])
    assert request_args[:2] == (struct.pack(">i", 3), b"select 1+1 from db_root\x00")


@pytest.mark.parametrize("method", ["server_version", "client_version", "ping"])
@pytest.mark.parametrize("closed", [False, True])
def test_native_argument_errors_precede_closed_and_network_work(
    owned, method: str, closed: bool
) -> None:
    owner, driver, sock = owned
    if closed:
        owner.close()
    before = len(driver.utility_requests)
    for args, kwargs in (((1,), {}), ((), {"unexpected": 1})):
        with pytest.raises(TypeError):
            getattr(owner, method)(*args, **kwargs)
    assert len(driver.utility_requests) == before
    driver.utility_connect.assert_not_called()


def test_client_version_is_frozen_own_text_available_after_close_without_io(
    owned, monkeypatch: pytest.MonkeyPatch
) -> None:
    owner, driver, sock = owned
    expected = pycubrid.__version__
    monkeypatch.setattr(pycubrid, "__version__", "changed-after-native-import")
    first = owner.client_version()
    owner.close()
    before = len(driver.utility_requests)
    closed = owner.client_version()
    assert type(first) is type(closed) is str
    assert first == closed == expected
    assert len(driver.utility_requests) == before
    driver.utility_connect.assert_not_called()


@pytest.mark.parametrize("method", ["server_version", "ping"])
def test_closed_live_utility_fails_without_request_or_reconnect(owned, method: str) -> None:
    owner, driver, sock = owned
    owner.close()
    before = len(driver.utility_requests)
    with pytest.raises(InterfaceError):
        getattr(owner, method)()
    assert len(driver.utility_requests) == before
    driver.utility_connect.assert_not_called()


def test_invalid_utf8_version_retires_only_the_captured_session(owned) -> None:
    owner, driver, sock = owned
    _load(sock, cas_info(IN_TRAN) + struct.pack(">i", 0) + b"\xff\x00")
    with pytest.raises(OperationalError) as caught:
        owner.server_version()
    assert isinstance(caught.value.__cause__, UnicodeDecodeError)
    assert _codes(sock) == [15]
    assert driver._connected is False
    assert driver._socket is None
    sock.close.assert_called()
    _guarded(driver)


def test_complete_version_error_preserves_error_identity_and_numeric_fields(owned) -> None:
    owner, driver, sock = owned
    _load(sock, error_body(IN_TRAN, -493, "version rejected"))
    with pytest.raises(DatabaseError) as caught:
        owner.server_version()
    assert caught.value is driver.utility_errors[0]
    assert caught.value.errno == caught.value.code == -493
    assert caught.value.sqlstate == "42000"
    assert _codes(sock) == [15]
    assert driver._connected
    _guarded(driver)


class BoolTrap:
    def __bool__(self) -> bool:
        raise AssertionError("raw native cache must not choose a utility flag")


@pytest.mark.parametrize("mode", [False, True])
def test_server_version_is_fresh_full_text_and_uses_effective_mode(owned, mode: bool) -> None:
    owner, driver, sock = owned
    owner.set_autocommit(mode)
    marker = BoolTrap()
    owner.autocommit = marker
    _load(
        sock,
        build_server_version_response("11.4.6.1963+long-build", cas_info(OUT_TRAN))[4:],
        build_server_version_response("11.4.6.1963+next", cas_info(OUT_TRAN))[4:],
    )
    first = owner.server_version()
    second = owner.server_version()
    assert type(first) is type(second) is str
    assert first == "11.4.6.1963+long-build"
    assert second == "11.4.6.1963+next"
    assert _codes(sock) == [15, 15]
    for call in sock.sendall.call_args_list:
        assert split_args(call.args[0][9:]) == (bytes((int(mode),)),)
    assert owner.autocommit is marker
    assert driver._autocommit is mode
    _guarded(driver)


@pytest.mark.parametrize(("values", "expected"), [((), 0), ((0, 1), 0), ((0, 2), 1)])
def test_ping_returns_exact_int_after_consumption_and_immediate_owned_close(
    owned, values: tuple[int, ...], expected: int
) -> None:
    owner, driver, sock = owned
    _load(sock, _query(*values), ok_body(IN_TRAN))
    result = owner.ping()
    assert type(result) is int and result == expected
    assert _codes(sock) == [41, 6]
    assert (
        split_args(sock.sendall.call_args_list[0].args[0][9:])[1] == b"select 1+1 from db_root\x00"
    )
    assert split_args(sock.sendall.call_args_list[-1].args[0][9:])[0] == struct.pack(">i", 7)
    assert driver._deferred_closes == []
    _guarded(driver)


@pytest.mark.parametrize("mode", [False, True])
def test_ping_validates_paged_results_effective_flags_and_no_callbacks(owned, mode: bool) -> None:
    owner, driver, sock = owned
    owner.set_autocommit(mode)
    owner.autocommit = BoolTrap()

    def forbidden(value: object) -> object:
        raise AssertionError("utility must not dispatch a configured converter")

    driver._json_deserializer = forbidden
    driver._decode_collections = True
    _load(sock, _query(0, total=3), _fetch(2), _fetch(1), ok_body(IN_TRAN))
    result = owner.ping()
    assert type(result) is int and result == 1
    assert _codes(sock) == [41, 8, 8, 6]
    packets = [packet for packet, _, _ in driver.utility_requests]
    assert packets[0].auto_commit is mode
    assert all(
        packet.json_deserializer is None and packet.decode_collections is False
        for packet in packets[:3]
    )
    assert split_args(sock.sendall.call_args_list[0].args[0][9:])[3] == bytes((int(mode),))
    assert driver._autocommit is mode
    _guarded(driver)


@pytest.mark.parametrize("last_fetch", [False, True])
def test_pooling_off_retired_handle_is_never_closed_or_reused(owned, last_fetch: bool) -> None:
    owner, driver, sock = owned
    owner.set_autocommit(True)
    driver._statement_pooling = 0
    bodies = (
        (_query(total=1), _fetch(2, status=OUT_TRAN))
        if last_fetch
        else (_query(2, status=OUT_TRAN),)
    )
    _load(sock, *bodies)
    result = owner.ping()
    assert result == 1 and type(result) is int
    assert _codes(sock) == ([41, 8] if last_fetch else [41])
    assert driver._deferred_closes == []
    _guarded(driver)


@pytest.mark.parametrize("values", [(), (1, 2)])
def test_invalid_fetch_progress_cannot_become_false_or_success(
    owned, values: tuple[int, ...]
) -> None:
    owner, driver, sock = owned
    _load(sock, _query(2, total=2), _fetch(*values), ok_body(IN_TRAN))
    with pytest.raises((OperationalError, DataError)):
        owner.ping()
    assert _codes(sock) == [41, 8, 6]
    _guarded(driver)


def test_invalid_final_fetch_never_revives_a_pooling_off_retired_handle(owned) -> None:
    owner, driver, sock = owned
    owner.set_autocommit(True)
    driver._statement_pooling = 0
    _load(sock, _query(2, total=2), _fetch(1, 2, status=OUT_TRAN), ok_body(IN_TRAN))
    with pytest.raises(OperationalError):
        owner.ping()
    assert _codes(sock) == [41, 8]
    assert driver._connected
    _guarded(driver)


def test_complete_final_fetch_error_preserves_error_without_closing_retired_id(owned) -> None:
    owner, driver, sock = owned
    owner.set_autocommit(True)
    driver._statement_pooling = 0
    _load(sock, _query(2, total=2), error_body(OUT_TRAN, -670, "primary"), ok_body(IN_TRAN))
    with pytest.raises(DatabaseError) as caught:
        owner.ping()
    assert caught.value is driver.utility_errors[0]
    assert caught.value.errno == -670
    assert _codes(sock) == [41, 8]
    assert driver._connected
    _guarded(driver)


def test_later_fetch_and_cleanup_errors_preserve_the_primary_object_and_metadata(owned) -> None:
    owner, driver, sock = owned
    _load(
        sock,
        _query(2, total=2),
        error_body(IN_TRAN, -670, "primary"),
        error_body(IN_TRAN, -1011, "cleanup"),
    )
    with pytest.raises(DatabaseError) as caught:
        owner.ping()
    assert caught.value is driver.utility_errors[0]
    assert caught.value.code == caught.value.errno == -670
    assert caught.value.sqlstate == "23000"
    assert len(driver.utility_errors) == 2
    assert driver.utility_errors[1].code == -1011
    assert _codes(sock) == [41, 8, 6]
    assert driver._connected is False
    _guarded(driver)


def test_cleanup_only_error_raises_instead_of_returning_a_ping_result(owned) -> None:
    owner, driver, sock = owned
    _load(sock, _query(2), error_body(IN_TRAN, -1011, "cleanup"))
    with pytest.raises(DatabaseError) as caught:
        owner.ping()
    assert caught.value is driver.utility_errors[0]
    assert caught.value.errno == -1011
    assert _codes(sock) == [41, 6]
    assert driver._connected is False
    _guarded(driver)


@pytest.mark.parametrize("method", ["server_version", "ping"])
def test_active_schema_is_rejected_before_an_effective_auto_request(owned, method: str) -> None:
    owner, driver, sock = owned
    owner.set_autocommit(True)
    schema = GetSchemaPacket(1, "t")
    schema.query_handle = 99
    driver._register_schema_result(schema)
    with pytest.raises(InterfaceError):
        getattr(owner, method)()
    assert _codes(sock) == []
    assert schema in driver._schema_results
    assert driver._connected


@pytest.mark.parametrize("method", ["server_version", "ping"])
@pytest.mark.parametrize("phase", ["write", "receive", "parse"])
def test_real_request_fences_cannot_adopt_or_clean_up_a_replacement(
    owned, monkeypatch: pytest.MonkeyPatch, method: str, phase: str
) -> None:
    owner, driver, sock = owned
    body = (
        build_server_version_response("11.4.6.1963")[4:]
        if method == "server_version"
        else _query(2)
    )
    _load(sock, body)
    replacement = make_socket_from_chunks([])
    new_info = b"\x01\x09\x08\x07"

    def replace() -> None:
        driver._socket = replacement
        driver._physical_generation += 1
        driver._record_reply_cas_info(new_info)

    packet_type = GetEngineVersionPacket if method == "server_version" else PrepareAndExecutePacket
    if phase == "receive":
        original_receive = sock.recv_into.side_effect

        def receive(buffer: memoryview, size: int = 0) -> int:
            count = original_receive(buffer, size)
            replace()
            return count

        sock.recv_into.side_effect = receive
    else:
        original = getattr(packet_type, phase)

        def callback(packet: Any, *args: Any) -> Any:
            result = original(packet, *args)
            replace()
            return result

        monkeypatch.setattr(packet_type, phase, callback)
    with pytest.raises((OperationalError, InterfaceError)):
        getattr(owner, method)()
    assert driver._socket is replacement
    assert driver._cas_info == new_info
    assert driver._connected
    replacement.sendall.assert_not_called()
    replacement.close.assert_not_called()
    if phase == "write":
        sock.sendall.assert_not_called()
    _guarded(driver)


def test_native_owner_driver_rebinding_after_parse_does_not_retire_the_new_owner(
    owned, monkeypatch: pytest.MonkeyPatch
) -> None:
    owner, driver, sock = owned
    _load(sock, build_server_version_response("11.4.6.1963")[4:])
    replacement_driver, replacement_socket = make_connected_connection()
    original = GetEngineVersionPacket.parse

    def parse(packet: GetEngineVersionPacket, body: bytes) -> None:
        original(packet, body)
        owner._driver = replacement_driver

    monkeypatch.setattr(GetEngineVersionPacket, "parse", parse)
    with pytest.raises((InterfaceError, OperationalError)):
        owner.server_version()
    assert replacement_driver._connected
    replacement_socket.close.assert_not_called()
    _guarded(driver)


@pytest.mark.parametrize("method", ["server_version", "ping"])
def test_wrapper_delegates_to_exact_native_result_without_converting_or_requerying(
    owned, method: str
) -> None:
    owner, driver, sock = owned
    wrapper = cubriddb.Connection.__new__(cubriddb.Connection)
    wrapper._connection = owner
    wrapper.fetch_value_converter = BoolTrap()
    bodies = (
        (build_server_version_response("11.4.6.1963+full")[4:],)
        if method == "server_version"
        else (_query(2), ok_body(IN_TRAN))
    )
    _load(sock, *bodies)
    result = getattr(wrapper, method)()
    assert wrapper.connection is owner
    if method == "server_version":
        assert type(result) is str and result == "11.4.6.1963+full"
        assert _codes(sock) == [15]
    else:
        assert type(result) is int and result == 1
        assert _codes(sock) == [41, 6]
    for args, kwargs in (((1,), {}), ((), {"unexpected": 1})):
        before = len(driver.utility_requests)
        with pytest.raises(TypeError):
            getattr(wrapper, method)(*args, **kwargs)
        assert len(driver.utility_requests) == before
    _guarded(driver)
