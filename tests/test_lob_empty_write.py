"""Empty bytes writes keep existing validation but avoid broker I/O (#394)."""

from __future__ import annotations

import struct
from unittest.mock import MagicMock

import pytest

from pycubrid.constants import CUBRIDDataType
from pycubrid.exceptions import InterfaceError, OperationalError
from pycubrid.lob import Lob
from pycubrid.protocol import LOBWritePacket


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
    assert lob.write(b"", offset=offset) == 0
    connection._ensure_connected.assert_called_once_with()
    connection._send_and_receive.assert_not_called()
    assert lob.lob_handle == b"opaque-handle"
    assert lob.lob_type == lob_type


def test_empty_bytes_preserves_bool_offset(connection: MagicMock) -> None:
    lob = Lob(connection, CUBRIDDataType.BLOB, b"handle")
    assert lob.write(b"", offset=True) == 0
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


@pytest.mark.parametrize("offset", [0.5, 2**63])
def test_empty_write_keeps_signed64_serialization_rejection(
    connection: MagicMock, offset: object
) -> None:
    lob = Lob(connection, CUBRIDDataType.BLOB, b"handle")
    with pytest.raises(struct.error):
        lob.write(b"", offset=offset)
    connection._ensure_connected.assert_called_once_with()


def test_empty_write_keeps_handle_serialization_rejection(connection: MagicMock) -> None:
    lob = Lob(connection, CUBRIDDataType.BLOB, None)
    with pytest.raises(TypeError):
        lob.write(b"")
    connection._ensure_connected.assert_called_once_with()


@pytest.mark.parametrize("data", [bytearray(), memoryview(b""), "", []])
def test_other_falsey_data_keeps_existing_send_path(connection: MagicMock, data: object) -> None:
    lob = Lob(connection, CUBRIDDataType.BLOB, b"handle")
    assert lob.write(data) == 0  # The simulated broker ACKs; the send path must not be shortcut.
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
    assert lob.write(b"") == 0
    connection._send_and_receive.reset_mock()
    assert lob.write(b"abc", offset=2) == 3
    connection._send_and_receive.assert_called_once()


def test_nonempty_bytes_subclass_does_not_use_truthiness_shortcut(connection: MagicMock) -> None:
    class FalseyBytes(bytes):
        def __bool__(self) -> bool:
            return False

    lob = Lob(connection, CUBRIDDataType.BLOB, b"handle")
    assert lob.write(FalseyBytes(b"abc")) == 3
    connection._send_and_receive.assert_called_once()
