"""Empty bytes writes keep existing validation but avoid broker I/O (#394)."""

from __future__ import annotations

import struct
from unittest.mock import MagicMock

import pytest

from pycubrid.constants import CUBRIDDataType
from pycubrid.exceptions import DataError, InterfaceError, OperationalError
from pycubrid.lob import Lob
from pycubrid.protocol import LOBWritePacket
from tests.test_network_edge_cases import make_connected_connection


@pytest.fixture
def connection() -> MagicMock:
    conn = MagicMock()

    def send(packet: object) -> None:
        assert isinstance(packet, LOBWritePacket)
        packet.write(b"\x00" * 4)  # Exercise real argument serialization, not only a mock ACK.
        packet.bytes_written = len(packet.data)

    conn._send_and_receive = MagicMock(side_effect=send)
    return conn


@pytest.mark.parametrize("lob_type", [CUBRIDDataType.BLOB, CUBRIDDataType.CLOB])
@pytest.mark.parametrize("offset", [0, 2, 2**63 - 1])
def test_empty_bytes_write_validates_without_sending(
    connection: MagicMock, lob_type: int, offset: int
) -> None:
    lob = Lob(connection, lob_type, b"opaque-handle")
    written = lob.write(b"", offset=offset)
    assert written == 0
    connection._ensure_connected.assert_called_once_with()
    connection._send_and_receive.assert_not_called()
    assert lob.lob_handle == b"opaque-handle"
    assert lob.lob_type == lob_type


def test_empty_bytes_rejects_bool_offset(connection: MagicMock) -> None:
    lob = Lob(connection, CUBRIDDataType.BLOB, b"handle")
    with pytest.raises(InterfaceError, match="offset must be an int"):
        lob.write(b"", offset=True)
    connection._ensure_connected.assert_not_called()
    connection._send_and_receive.assert_not_called()


def test_closed_lob_precedes_offset_and_connection_checks(connection: MagicMock) -> None:
    lob = Lob(connection, CUBRIDDataType.BLOB, b"handle")
    lob.close()
    with pytest.raises(InterfaceError, match="LOB is closed"):
        lob.write(b"", offset=-1)
    connection._ensure_connected.assert_not_called()
    connection._send_and_receive.assert_not_called()


def test_empty_write_keeps_negative_offset_check(connection: MagicMock) -> None:
    lob = Lob(connection, CUBRIDDataType.BLOB, b"handle")
    with pytest.raises(InterfaceError, match="offset must be non-negative"):
        lob.write(b"", offset=-1)
    connection._ensure_connected.assert_not_called()
    connection._send_and_receive.assert_not_called()


def test_empty_write_keeps_closed_connection_check(connection: MagicMock) -> None:
    lob = Lob(connection, CUBRIDDataType.BLOB, b"handle")
    connection._ensure_connected.side_effect = InterfaceError("connection is closed")
    with pytest.raises(InterfaceError, match="connection is closed"):
        lob.write(b"")
    connection._ensure_connected.assert_called_once_with()
    connection._send_and_receive.assert_not_called()


def test_empty_write_rejects_float_offset_before_serialization(
    connection: MagicMock,
) -> None:
    lob = Lob(connection, CUBRIDDataType.BLOB, b"handle")
    with pytest.raises(InterfaceError, match="offset must be an int"):
        lob.write(b"", offset=0.5)
    connection._ensure_connected.assert_not_called()
    connection._send_and_receive.assert_not_called()


def test_empty_write_keeps_signed64_serialization_rejection(connection: MagicMock) -> None:
    lob = Lob(connection, CUBRIDDataType.BLOB, b"handle")
    with pytest.raises(DataError):
        lob.write(b"", offset=2**63)
    connection._ensure_connected.assert_called_once_with()


@pytest.mark.parametrize("data", [b"", b"x"], ids=["empty", "nonempty"])
def test_real_connection_preserves_serialization_error_translation(data: bytes) -> None:
    conn, sock = make_connected_connection()
    conn._record_reply_cas_info(
        b"\x01\x00\x00\x00"
    )  # Active session: no reconnect before validation.
    sock.sendall.reset_mock()
    try:
        lob = Lob(conn, CUBRIDDataType.BLOB, b"handle")
        with pytest.raises(DataError, match="serialize into CAS request") as raised:
            lob.write(data, offset=2**63)
        assert isinstance(raised.value.__cause__, struct.error)
        sock.sendall.assert_not_called()
    finally:
        conn._drop_connection()


@pytest.mark.parametrize("data", [b"", b"x"], ids=["empty", "nonempty"])
def test_real_connection_rejects_float_offset_without_socket_io(data: bytes) -> None:
    conn, sock = make_connected_connection()
    conn._cas_info = b"\x01\x00\x00\x00"
    sock.sendall.reset_mock()
    try:
        lob = Lob(conn, CUBRIDDataType.BLOB, b"handle")
        with pytest.raises(InterfaceError, match="offset must be an int"):
            lob.write(data, offset=0.5)
        sock.sendall.assert_not_called()
    finally:
        conn._drop_connection()


def test_empty_write_keeps_handle_serialization_rejection(connection: MagicMock) -> None:
    lob = Lob(connection, CUBRIDDataType.BLOB, None)
    with pytest.raises(TypeError):
        lob.write(b"")
    connection._ensure_connected.assert_called_once_with()


@pytest.mark.parametrize("data", [bytearray(), memoryview(b""), "", []])
def test_other_falsey_data_keeps_existing_send_path(connection: MagicMock, data: object) -> None:
    lob = Lob(connection, CUBRIDDataType.BLOB, b"handle")
    written = lob.write(data)  # Simulated broker ACK: the send path must not be shortcut.
    assert written == 0
    connection._send_and_receive.assert_called_once()


def test_none_data_does_not_become_success(connection: MagicMock) -> None:
    lob = Lob(connection, CUBRIDDataType.BLOB, b"handle")
    with pytest.raises(TypeError):
        lob.write(None)


@pytest.mark.parametrize("ack", [0, 2, 6])
def test_nonempty_acknowledgement_checks_remain(connection: MagicMock, ack: int) -> None:
    lob = Lob(connection, CUBRIDDataType.BLOB, b"handle")

    def send(packet: object) -> None:
        assert isinstance(packet, LOBWritePacket)
        packet.bytes_written = ack

    connection._send_and_receive.side_effect = send
    with pytest.raises(OperationalError, match="LOB write truncated"):
        lob.write(b"hello")
    connection._send_and_receive.assert_called_once()


def test_nonempty_after_empty_still_sends(connection: MagicMock) -> None:
    lob = Lob(connection, CUBRIDDataType.BLOB, b"handle")
    empty_written = lob.write(b"")
    assert empty_written == 0
    connection._send_and_receive.reset_mock()
    written = lob.write(b"abc", offset=2)
    assert written == 3
    connection._send_and_receive.assert_called_once()


def test_nonempty_bytes_subclass_does_not_use_truthiness_shortcut(connection: MagicMock) -> None:
    class FalseyBytes(bytes):
        def __bool__(self) -> bool:
            return False

    lob = Lob(connection, CUBRIDDataType.BLOB, b"handle")
    written = lob.write(FalseyBytes(b"abc"))
    assert written == 3
    connection._send_and_receive.assert_called_once()
