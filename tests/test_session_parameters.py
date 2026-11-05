"""Reading the effective isolation level and lock timeout (#1129)."""

from __future__ import annotations

import struct
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from pycubrid.aio.connection import AsyncConnection
from pycubrid.constants import CASFunctionCode, CCIDbParam
from pycubrid.exceptions import DatabaseError, InterfaceError, OperationalError
from pycubrid.protocol import GetDbParameterPacket

from .test_cas_reconnect_probe import (
    IN_TRAN,
    OUT_TRAN,
    _frames,
    _function_codes,
    _replacement_socket,
    _script,
    _sync_out_tran,
)
from .test_network_edge_cases import build_simple_ok_response, make_connected_connection

READS = pytest.mark.parametrize(
    ("method", "parameter"),
    [
        ("get_isolation_level", CCIDbParam.ISOLATION_LEVEL),
        ("get_lock_timeout", CCIDbParam.LOCK_TIMEOUT),
    ],
)


def _reply(value: int, cas_info: bytes = IN_TRAN) -> bytes:
    body = cas_info + struct.pack(">ii", 0, value)
    return struct.pack(">i", len(body) - 4) + body


def _sent_parameter(sock: MagicMock, index: int = -1) -> tuple[int, int]:
    frame = sock.sendall.call_args_list[index].args[0]
    # [length][cas_info][function code][int32 size][int32 parameter]
    return frame[8], struct.unpack(">i", frame[13:17])[0]


def _async_connection() -> AsyncConnection:
    conn = AsyncConnection("localhost", 33000, "testdb", "dba", "")
    conn._connected = True
    return conn


@READS
def test_sync_read_sends_one_request_and_returns_the_value(
    method: str, parameter: CCIDbParam
) -> None:
    conn, sock = make_connected_connection()
    conn._record_reply_cas_info(bytearray(IN_TRAN))
    _script(sock, _frames(_reply(-1)))
    start = sock.sendall.call_count

    assert getattr(conn, method)() == -1

    assert sock.sendall.call_count == start + 1
    assert _sent_parameter(sock) == (CASFunctionCode.GET_DB_PARAMETER, parameter)


@READS
def test_sync_read_is_not_cached(method: str, parameter: CCIDbParam) -> None:
    conn, sock = make_connected_connection()
    conn._record_reply_cas_info(bytearray(IN_TRAN))
    _script(sock, _frames(_reply(4), _reply(5)))

    assert [getattr(conn, method)(), getattr(conn, method)()] == [4, 5]


@READS
def test_sync_read_out_of_transaction_probes_first(method: str, parameter: CCIDbParam) -> None:
    conn, sock = _sync_out_tran()
    _script(sock, _frames(build_simple_ok_response(OUT_TRAN), _reply(5, OUT_TRAN)))
    start = sock.sendall.call_count
    generation = conn._physical_generation

    assert getattr(conn, method)() == 5

    assert _function_codes(sock, start) == [
        CASFunctionCode.CHECK_CAS,
        CASFunctionCode.GET_DB_PARAMETER,
    ]
    assert conn._physical_generation == generation


@READS
def test_sync_read_reports_the_replacement_session(method: str, parameter: CCIDbParam) -> None:
    conn, sock = _sync_out_tran()
    _script(sock, [])
    replacement = _replacement_socket(_reply(4, OUT_TRAN))
    generation = conn._physical_generation

    with patch("socket.create_connection", return_value=replacement):
        assert getattr(conn, method)() == 4

    assert conn._socket is replacement
    assert conn._physical_generation == generation + 1
    assert _sent_parameter(replacement) == (CASFunctionCode.GET_DB_PARAMETER, parameter)


@READS
def test_sync_server_error_keeps_the_session(method: str, parameter: CCIDbParam) -> None:
    conn, sock = make_connected_connection()
    conn._record_reply_cas_info(bytearray(IN_TRAN))
    body = IN_TRAN + struct.pack(">ii", -2, -1011) + b"text\x00"
    _script(sock, _frames(struct.pack(">i", len(body) - 4) + body))

    with pytest.raises(DatabaseError) as raised:
        getattr(conn, method)()

    assert raised.value.errno == -1011
    assert conn._connected is True


@READS
def test_sync_truncated_reply_retires_the_session(method: str, parameter: CCIDbParam) -> None:
    conn, sock = make_connected_connection()
    conn._record_reply_cas_info(bytearray(IN_TRAN))
    body = IN_TRAN + struct.pack(">i", 0)  # no value follows the response code
    _script(sock, _frames(struct.pack(">i", len(body) - 4) + body))

    with pytest.raises(OperationalError, match="malformed response from broker"):
        getattr(conn, method)()

    assert conn._connected is False


@READS
def test_sync_closed_connection_sends_nothing(method: str, parameter: CCIDbParam) -> None:
    conn, sock = make_connected_connection()
    conn._connected = False
    conn._closed = True
    start = sock.sendall.call_count

    with pytest.raises(InterfaceError):
        getattr(conn, method)()

    assert sock.sendall.call_count == start


@READS
@pytest.mark.asyncio
async def test_async_read_sends_one_request_and_returns_the_value(
    method: str, parameter: CCIDbParam
) -> None:
    conn = _async_connection()
    sent: list[GetDbParameterPacket] = []

    async def fake(packet: GetDbParameterPacket) -> GetDbParameterPacket:
        sent.append(packet)
        packet.value = 5000
        return packet

    with patch.object(conn, "_send_and_receive", AsyncMock(side_effect=fake)):
        assert await getattr(conn, method)() == 5000

    assert [type(packet) for packet in sent] == [GetDbParameterPacket]
    assert sent[0].parameter == parameter


@READS
@pytest.mark.asyncio
async def test_async_closed_connection_sends_nothing(method: str, parameter: CCIDbParam) -> None:
    conn = _async_connection()
    conn._connected = False
    conn._closed = True

    with patch.object(conn, "_send_and_receive", AsyncMock()) as send:
        with pytest.raises(InterfaceError):
            await getattr(conn, method)()

    send.assert_not_called()
