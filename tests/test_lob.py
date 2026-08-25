from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from pycubrid.constants import CUBRIDDataType
from pycubrid.exceptions import InterfaceError, OperationalError
from pycubrid.lob import Lob
from pycubrid.protocol import LOBNewPacket, LOBReadPacket, LOBWritePacket


@pytest.fixture
def mock_connection() -> MagicMock:
    connection = MagicMock()
    connection._ensure_connected = MagicMock()

    def send_and_receive(packet: object) -> object:
        if isinstance(packet, LOBWritePacket):
            packet.bytes_written = len(packet.data)
        return packet

    connection._send_and_receive = MagicMock(side_effect=send_and_receive)
    return connection


def test_init_with_explicit_handle_and_properties(mock_connection: MagicMock) -> None:
    lob = Lob(mock_connection, CUBRIDDataType.BLOB, b"\x01\x02")
    assert lob.lob_type == CUBRIDDataType.BLOB
    assert lob.lob_handle == b"\x01\x02"


def test_init_raises_for_invalid_lob_type(mock_connection: MagicMock) -> None:
    with pytest.raises(ValueError, match="lob_type"):
        Lob(mock_connection, 999)


def test_create_sends_lob_new_and_returns_lob(mock_connection: MagicMock) -> None:
    def send_and_receive(packet: object) -> object:
        if isinstance(packet, LOBNewPacket):
            packet.lob_handle = b"server-handle"
        return packet

    mock_connection._send_and_receive.side_effect = send_and_receive

    lob = Lob.create(mock_connection, CUBRIDDataType.CLOB)

    sent_packet = mock_connection._send_and_receive.call_args.args[0]
    assert isinstance(sent_packet, LOBNewPacket)
    assert sent_packet.lob_type == CUBRIDDataType.CLOB
    assert lob.lob_type == CUBRIDDataType.CLOB
    assert lob.lob_handle == b"server-handle"
    mock_connection._ensure_connected.assert_called_once()


def test_create_propagates_connection_closed_error(mock_connection: MagicMock) -> None:
    mock_connection._ensure_connected.side_effect = InterfaceError("connection is closed")

    with pytest.raises(InterfaceError, match="connection is closed"):
        Lob.create(mock_connection, CUBRIDDataType.BLOB)


def test_create_propagates_server_error(mock_connection: MagicMock) -> None:
    mock_connection._send_and_receive.side_effect = OperationalError("server error")

    with pytest.raises(OperationalError, match="server error"):
        Lob.create(mock_connection, CUBRIDDataType.BLOB)


def test_write_sends_lob_write_packet_and_returns_written_length(
    mock_connection: MagicMock,
) -> None:
    lob = Lob(mock_connection, CUBRIDDataType.BLOB, b"lob-handle")

    written = lob.write(b"hello", offset=12)

    sent_packet = mock_connection._send_and_receive.call_args.args[0]
    assert isinstance(sent_packet, LOBWritePacket)
    assert sent_packet.packed_lob_handle == b"lob-handle"
    assert sent_packet.offset == 12
    assert sent_packet.data == b"hello"
    assert written == 5
    mock_connection._ensure_connected.assert_called_once()


def test_write_propagates_connection_closed_error(mock_connection: MagicMock) -> None:
    lob = Lob(mock_connection, CUBRIDDataType.BLOB, b"lob-handle")
    mock_connection._ensure_connected.side_effect = InterfaceError("connection is closed")

    with pytest.raises(InterfaceError, match="connection is closed"):
        lob.write(b"x")


def test_write_raises_on_partial_write(mock_connection: MagicMock) -> None:
    """Server writing fewer bytes than requested raises OperationalError."""
    lob = Lob(mock_connection, CUBRIDDataType.BLOB, b"lob-handle")

    def partial_write(packet: object) -> object:
        if isinstance(packet, LOBWritePacket):
            packet.bytes_written = 2  # Only 2 of 5 bytes written
        return packet

    mock_connection._send_and_receive.side_effect = partial_write

    with pytest.raises(OperationalError, match="LOB write truncated"):
        lob.write(b"hello")


def test_write_raises_on_zero_bytes_written(mock_connection: MagicMock) -> None:
    """Server writing zero bytes raises OperationalError with count."""
    lob = Lob(mock_connection, CUBRIDDataType.BLOB, b"lob-handle")

    # Override fixture: don't set bytes_written (stays 0 from __init__)
    mock_connection._send_and_receive.side_effect = lambda packet: packet

    with pytest.raises(OperationalError, match="wrote 0 of 5 bytes"):
        lob.write(b"hello")


def test_read_sends_lob_read_packet_and_returns_data(mock_connection: MagicMock) -> None:
    lob = Lob(mock_connection, CUBRIDDataType.CLOB, b"lob-handle")

    def send_and_receive(packet: object) -> object:
        if isinstance(packet, LOBReadPacket):
            packet.bytes_read = 4
            packet.lob_data = b"data"
        return packet

    mock_connection._send_and_receive.side_effect = send_and_receive

    data = lob.read(10, offset=7)

    sent_packet = mock_connection._send_and_receive.call_args.args[0]
    assert isinstance(sent_packet, LOBReadPacket)
    assert sent_packet.packed_lob_handle == b"lob-handle"
    assert sent_packet.offset == 7
    assert sent_packet.length == 10
    assert data == b"data"
    mock_connection._ensure_connected.assert_called_once()


def test_read_returns_empty_bytes_when_server_returns_no_data(mock_connection: MagicMock) -> None:
    lob = Lob(mock_connection, CUBRIDDataType.BLOB, b"lob-handle")

    data = lob.read(128)

    assert data == b""


def test_read_propagates_server_error(mock_connection: MagicMock) -> None:
    lob = Lob(mock_connection, CUBRIDDataType.CLOB, b"lob-handle")
    mock_connection._send_and_receive.side_effect = OperationalError("read failed")

    with pytest.raises(OperationalError, match="read failed"):
        lob.read(3)


def test_read_raises_when_server_returns_more_than_requested(
    mock_connection: MagicMock,
) -> None:
    """Overlong server response is a protocol violation -> OperationalError."""
    lob = Lob(mock_connection, CUBRIDDataType.BLOB, b"lob-handle")

    def send_and_receive(packet: object) -> object:
        if isinstance(packet, LOBReadPacket):
            packet.bytes_read = 6
            packet.lob_data = b"toolong"[:6] + b"X"  # 7 bytes for a 5-byte request
        return packet

    mock_connection._send_and_receive.side_effect = send_and_receive

    with pytest.raises(OperationalError, match="exceeding requested 5"):
        lob.read(5)


@pytest.mark.parametrize("offset", [-1, -100])
def test_read_rejects_negative_offset(mock_connection: MagicMock, offset: int) -> None:
    lob = Lob(mock_connection, CUBRIDDataType.BLOB, b"lob-handle")
    with pytest.raises(InterfaceError, match="offset must be non-negative"):
        lob.read(10, offset=offset)
    mock_connection._send_and_receive.assert_not_called()


def test_read_rejects_negative_length(mock_connection: MagicMock) -> None:
    lob = Lob(mock_connection, CUBRIDDataType.BLOB, b"lob-handle")
    with pytest.raises(InterfaceError, match="length must be non-negative"):
        lob.read(-1)
    mock_connection._send_and_receive.assert_not_called()


def test_write_rejects_negative_offset(mock_connection: MagicMock) -> None:
    lob = Lob(mock_connection, CUBRIDDataType.BLOB, b"lob-handle")
    with pytest.raises(InterfaceError, match="offset must be non-negative"):
        lob.write(b"x", offset=-1)
    mock_connection._send_and_receive.assert_not_called()


def test_close_is_idempotent_and_network_free(mock_connection: MagicMock) -> None:
    lob = Lob(mock_connection, CUBRIDDataType.BLOB, b"lob-handle")
    lob.close()
    lob.close()  # idempotent, no error
    mock_connection._send_and_receive.assert_not_called()


def test_read_after_close_raises(mock_connection: MagicMock) -> None:
    lob = Lob(mock_connection, CUBRIDDataType.BLOB, b"lob-handle")
    lob.close()
    with pytest.raises(InterfaceError, match="LOB is closed"):
        lob.read(10)
    mock_connection._send_and_receive.assert_not_called()


def test_write_after_close_raises(mock_connection: MagicMock) -> None:
    lob = Lob(mock_connection, CUBRIDDataType.BLOB, b"lob-handle")
    lob.close()
    with pytest.raises(InterfaceError, match="LOB is closed"):
        lob.write(b"data")
    mock_connection._send_and_receive.assert_not_called()


def test_properties_remain_readable_after_close(mock_connection: MagicMock) -> None:
    lob = Lob(mock_connection, CUBRIDDataType.CLOB, b"lob-handle")
    lob.close()
    assert lob.lob_type == CUBRIDDataType.CLOB
    assert lob.lob_handle == b"lob-handle"


def test_context_manager_closes_on_exit(mock_connection: MagicMock) -> None:
    with Lob(mock_connection, CUBRIDDataType.BLOB, b"lob-handle") as lob:
        assert lob.lob_handle == b"lob-handle"
    with pytest.raises(InterfaceError, match="LOB is closed"):
        lob.read(10)


def test_context_manager_does_not_suppress_exceptions(mock_connection: MagicMock) -> None:
    with pytest.raises(ValueError, match="boom"):
        with Lob(mock_connection, CUBRIDDataType.BLOB, b"lob-handle") as lob:
            assert lob is not None
            raise ValueError("boom")
