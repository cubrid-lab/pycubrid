"""Sync physical-session send fence for future prepared owners (#478)."""

from __future__ import annotations

import struct
from collections.abc import Callable
from threading import Event, Thread
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from pycubrid.constants import CUBRIDStatementType
from pycubrid.exceptions import OperationalError, ProgrammingError
from pycubrid.protocol import CloseQueryPacket, ExecutePacket

from .test_network_edge_cases import (
    build_simple_ok_response,
    make_connected_connection,
    make_socket_from_chunks,
)


def _response_socket() -> MagicMock:
    frame = build_simple_ok_response()
    return make_socket_from_chunks([frame])


def _bounded_thread_call(action: Callable[[], Any]) -> Any:
    result: dict[str, Any] = {}

    def run() -> None:
        try:
            result["value"] = action()
        except (Exception, KeyboardInterrupt, SystemExit, GeneratorExit) as exc:
            result["error"] = exc

    thread = Thread(target=run, daemon=True)
    thread.start()
    thread.join(timeout=2)
    finished = not thread.is_alive()
    assert finished, "session-lock operation deadlocked"
    if "error" in result:
        raise result["error"]
    return result.get("value")


def test_stale_generation_rejects_before_packet_write() -> None:
    conn, sock = make_connected_connection()
    packet = MagicMock()
    conn._physical_generation = 2
    sock.sendall.reset_mock()

    with pytest.raises(OperationalError, match="prepared|generation|session"):
        conn._send_and_receive(packet, expected_generation=1)

    packet.write.assert_not_called()
    sock.sendall.assert_not_called()
    assert conn._connected is True


@pytest.mark.parametrize(
    "packet",
    [ExecutePacket(7, CUBRIDStatementType.SELECT), CloseQueryPacket(7)],
    ids=["FC3", "FC6"],
)
def test_physical_replacement_during_serialization_rejects_before_send(
    packet: ExecutePacket | CloseQueryPacket,
) -> None:
    conn, old_sock = make_connected_connection()
    replacement = _response_socket()
    old_generation = conn._physical_generation
    old_sock.sendall.reset_mock()
    original_write = packet.write

    def replace_during_write(_cas_info: bytes) -> bytes:
        frame = original_write(_cas_info)
        conn._drop_connection()
        conn._socket = replacement
        conn._connected = True
        conn._physical_generation += 1
        return frame

    packet.write = replace_during_write

    with pytest.raises(OperationalError, match="prepared|generation|session"):
        conn._send_and_receive(packet, expected_generation=old_generation)

    old_sock.sendall.assert_not_called()
    replacement.sendall.assert_not_called()
    assert conn._physical_generation == old_generation + 1


def test_replacement_during_sendall_cannot_report_stale_handle_success() -> None:
    conn, old_sock = make_connected_connection()
    replacement = _response_socket()
    old_generation = conn._physical_generation
    packet = CloseQueryPacket(7)

    def replace_during_send(_frame: bytes) -> None:
        conn._drop_connection()
        conn._socket = replacement
        conn._connected = True
        conn._physical_generation += 1

    old_sock.sendall.side_effect = replace_during_send

    with pytest.raises(OperationalError, match="prepared|physical session"):
        conn._send_and_receive(packet, expected_generation=old_generation)

    assert conn._physical_generation == old_generation + 1
    assert conn._connected is True
    assert conn._socket is replacement
    replacement.sendall.assert_not_called()
    replacement.close.assert_not_called()


def test_reentry_after_generation_check_never_sends_to_replacement() -> None:
    conn, old_sock = make_connected_connection()
    replacement = _response_socket()
    old_generation = conn._physical_generation
    original_check = conn._validate_prepared_generation
    checks = 0

    def replace_after_check(*args: object, **kwargs: object) -> None:
        nonlocal checks
        original_check(*args, **kwargs)
        checks += 1
        if checks == 2:
            conn._drop_connection()
            conn._socket = replacement
            conn._connected = True
            conn._physical_generation += 1

    conn._validate_prepared_generation = replace_after_check
    old_sock.sendall.reset_mock()

    with pytest.raises(OperationalError, match="prepared|physical session"):
        conn._send_and_receive(CloseQueryPacket(7), expected_generation=old_generation)

    replacement.sendall.assert_not_called()
    replacement.close.assert_not_called()
    assert conn._socket is replacement
    assert conn._connected is True


def test_replacement_after_response_check_keeps_new_cas_info() -> None:
    conn, old_sock = make_connected_connection()
    old_sock.recv_into.side_effect = _response_socket().recv_into.side_effect
    replacement = _response_socket()
    replacement_info = b"\x09\x08\x07\x06"
    old_generation = conn._physical_generation
    original_check = conn._validate_prepared_session
    checks = 0

    def replace_after_response_check(expected: int | None, request_socket: object) -> None:
        nonlocal checks
        original_check(expected, request_socket)
        checks += 1
        if checks == 3:
            conn._drop_connection()
            conn._socket = replacement
            conn._connected = True
            conn._physical_generation += 1
            conn._record_reply_cas_info(replacement_info)

    conn._validate_prepared_session = replace_after_response_check
    packet = MagicMock()
    packet.write.return_value = b"prepared request"

    with pytest.raises(OperationalError, match="prepared|physical session"):
        conn._send_and_receive(packet, expected_generation=old_generation)

    assert conn._connected is True
    assert conn._socket is replacement
    assert conn._cas_info == replacement_info


def test_prebyte_prepared_validation_failure_preserves_session() -> None:
    conn, sock = make_connected_connection()
    sock.sendall.reset_mock()
    packet = MagicMock()
    packet.write.side_effect = ProgrammingError("invalid local bind")

    with pytest.raises(ProgrammingError, match="invalid local bind"):
        conn._send_and_receive(packet, expected_generation=conn._physical_generation)

    assert conn._connected is True
    assert conn._socket is sock
    sock.sendall.assert_not_called()


def test_prepared_interrupt_after_send_attempt_retires_session() -> None:
    conn, sock = make_connected_connection()
    sock.sendall.side_effect = KeyboardInterrupt("interrupted after write began")
    packet = MagicMock()
    packet.write.return_value = b"prepared request"

    with pytest.raises(KeyboardInterrupt, match="interrupted after write began"):
        conn._send_and_receive(packet, expected_generation=conn._physical_generation)

    assert conn._connected is False
    assert conn._socket is None
    sock.close.assert_called()


def test_custom_base_exception_after_send_attempt_retires_session() -> None:
    class _Stop(BaseException):
        pass

    conn, sock = make_connected_connection()
    sock.sendall.side_effect = _Stop("uncertain send")
    packet = MagicMock()
    packet.write.return_value = b"prepared request"

    with pytest.raises(_Stop, match="uncertain send"):
        conn._send_and_receive(packet, expected_generation=conn._physical_generation)

    assert conn._connected is False
    assert conn._socket is None


def test_prepared_partial_send_error_retires_session() -> None:
    conn, sock = make_connected_connection()
    sock.sendall.side_effect = BrokenPipeError("partial send")
    packet = MagicMock()
    packet.write.return_value = b"prepared request"

    with pytest.raises(OperationalError, match="socket communication failed"):
        conn._send_and_receive(packet, expected_generation=conn._physical_generation)

    assert conn._connected is False
    assert conn._socket is None


def test_prepared_interrupt_during_parse_retires_session() -> None:
    conn, sock = make_connected_connection()
    sock.recv_into.side_effect = _response_socket().recv_into.side_effect
    packet = MagicMock()
    packet.write.return_value = b"prepared request"
    packet.parse.side_effect = KeyboardInterrupt("parse interrupted")

    with pytest.raises(KeyboardInterrupt, match="parse interrupted"):
        conn._send_and_receive(packet, expected_generation=conn._physical_generation)

    assert conn._connected is False
    assert conn._socket is None


def test_complete_broker_error_preserves_prepared_session() -> None:
    conn, sock = make_connected_connection()
    response_cas_info = b"\x00\x01\x02\x03"
    body = response_cas_info + struct.pack(">ii", -1, -493) + b"syntax\x00"
    frame = struct.pack(">i", len(body) - 4) + body
    sock.recv_into.side_effect = make_socket_from_chunks([frame]).recv_into.side_effect
    packet = CloseQueryPacket(7)

    with pytest.raises(ProgrammingError, match="syntax") as caught:
        conn._send_and_receive(packet, expected_generation=conn._physical_generation)

    assert caught.value.code == -493
    assert conn._connected is True
    assert conn._socket is sock
    assert conn._cas_info == response_cas_info


@pytest.mark.parametrize("code", [0, -493])
def test_local_parser_database_error_retires_prepared_session(code: int) -> None:
    conn, sock = make_connected_connection()
    sock.recv_into.side_effect = _response_socket().recv_into.side_effect
    packet = MagicMock()
    packet.write.return_value = b"prepared request"
    packet.parse.side_effect = OperationalError("local parser callback failure", code=code)

    with pytest.raises(OperationalError, match="malformed response") as caught:
        conn._send_and_receive(packet, expected_generation=conn._physical_generation)

    assert isinstance(caught.value.__cause__, OperationalError)
    assert conn._connected is False
    assert conn._socket is None


def test_current_generation_prepared_request_sends_once() -> None:
    conn, sock = make_connected_connection()
    sock.recv_into.side_effect = _response_socket().recv_into.side_effect
    sock.sendall.reset_mock()
    packet = MagicMock()
    packet.write.return_value = b"prepared request"

    result = conn._send_and_receive(packet, expected_generation=conn._physical_generation)

    assert result is packet
    packet.parse.assert_called_once()
    sock.sendall.assert_called_once_with(b"prepared request")


def test_ping_cannot_interleave_with_prepared_send() -> None:
    conn, sock = make_connected_connection()
    frame = build_simple_ok_response()
    sock.recv_into.side_effect = make_socket_from_chunks([frame, frame]).recv_into.side_effect
    entered_send = Event()
    release_send = Event()
    entered_ping = Event()

    def blocked_send(_data: bytes) -> None:
        entered_send.set()
        released = release_send.wait(timeout=2)
        assert released

    def ping() -> bool:
        entered_ping.set()
        return conn.ping(reconnect=False)

    sock.sendall.side_effect = blocked_send
    packet = MagicMock()
    packet.write.return_value = b"prepared request"

    results: dict[str, object] = {}
    errors: list[BaseException] = []

    def record(name: str, action: Callable[[], object]) -> None:
        try:
            results[name] = action()
        except (Exception, KeyboardInterrupt, SystemExit, GeneratorExit) as exc:
            errors.append(exc)

    request = Thread(
        target=record,
        args=(
            "request",
            lambda: conn._send_and_receive(packet, expected_generation=conn._physical_generation),
        ),
        daemon=True,
    )
    health = Thread(target=record, args=("health", ping), daemon=True)
    request.start()
    try:
        send_started = entered_send.wait(timeout=2)
        assert send_started
        health.start()
        ping_started = entered_ping.wait(timeout=2)
        assert ping_started
        assert health.is_alive()
    finally:
        release_send.set()
    request.join(timeout=2)
    health.join(timeout=2)
    assert not request.is_alive()
    assert not health.is_alive()
    assert not errors
    assert results == {"request": packet, "health": True}


def test_reentrant_ping_reconnect_and_close_do_not_deadlock() -> None:
    conn, _ = make_connected_connection()
    conn._drop_connection()
    old_generation = conn._physical_generation
    open_db = b"\x01\x01\x02\x03" + (0).to_bytes(4, "big") + b"\x00" * 8 + (2).to_bytes(4, "big")
    new_sock = make_socket_from_chunks(
        [(0).to_bytes(4, "big"), len(open_db[4:]).to_bytes(4, "big"), open_db]
    )
    with patch("socket.create_connection", return_value=new_sock):
        recovered = _bounded_thread_call(lambda: conn.ping(True))
    assert recovered is True
    assert conn._physical_generation == old_generation + 1

    new_sock.recv_into.side_effect = _response_socket().recv_into.side_effect
    _bounded_thread_call(conn.close)
    assert conn._connected is False


def test_prepared_partial_read_and_malformed_parse_retire_session() -> None:
    for failure in ("truncated", "malformed"):
        conn, sock = make_connected_connection()
        packet = MagicMock()
        packet.write.return_value = b"prepared request"
        if failure == "truncated":
            sock.recv_into.side_effect = lambda _buf, _size=0: 0
        else:
            sock.recv_into.side_effect = _response_socket().recv_into.side_effect
            packet.parse.side_effect = ValueError("malformed body")

        with pytest.raises(OperationalError):
            conn._send_and_receive(packet, expected_generation=conn._physical_generation)

        assert conn._connected is False
        assert conn._socket is None


def test_prepared_invalid_length_retires_session() -> None:
    conn, sock = make_connected_connection()
    sock.recv_into.side_effect = make_socket_from_chunks(
        [struct.pack(">i", -1)]
    ).recv_into.side_effect
    packet = MagicMock()
    packet.write.return_value = b"prepared request"

    with pytest.raises(OperationalError, match="DATA_LENGTH"):
        conn._send_and_receive(packet, expected_generation=conn._physical_generation)

    assert conn._connected is False
    assert conn._socket is None


def test_unexpected_prepared_parser_failure_retires_session() -> None:
    conn, sock = make_connected_connection()
    sock.recv_into.side_effect = _response_socket().recv_into.side_effect
    packet = MagicMock()
    packet.write.return_value = b"prepared request"
    packet.parse.side_effect = RuntimeError("unexpected parser failure")

    with pytest.raises(OperationalError, match="malformed response") as caught:
        conn._send_and_receive(packet, expected_generation=conn._physical_generation)

    assert isinstance(caught.value.__cause__, RuntimeError)
    assert conn._connected is False
    assert conn._socket is None


def test_prepared_cleanup_failure_does_not_mask_primary_interrupt() -> None:
    conn, sock = make_connected_connection()
    conn._drop_connection = MagicMock(side_effect=RuntimeError("secondary cleanup error"))
    sock.sendall.side_effect = KeyboardInterrupt("primary request interruption")
    packet = MagicMock()
    packet.write.return_value = b"prepared request"

    with pytest.raises(KeyboardInterrupt, match="primary request interruption"):
        conn._send_and_receive(packet, expected_generation=conn._physical_generation)

    assert conn._connected is False
    assert conn._socket is None


def test_custom_cleanup_base_exception_does_not_mask_primary() -> None:
    class _CleanupStop(BaseException):
        pass

    conn, sock = make_connected_connection()
    conn._drop_connection = MagicMock(side_effect=_CleanupStop("secondary cleanup failure"))
    sock.sendall.side_effect = KeyboardInterrupt("primary interruption")
    packet = MagicMock()
    packet.write.return_value = b"prepared request"

    with pytest.raises(KeyboardInterrupt, match="primary interruption"):
        conn._send_and_receive(packet, expected_generation=conn._physical_generation)

    assert conn._connected is False
    assert conn._socket is None
