"""Zero and out-of-range temporal values keep the session usable (#512).

CUBRID accepts zero dates such as ``DATE'0000-00-00'`` and sends them to the
client, but Python ``datetime`` cannot represent year 0. The reply is complete
when the value is decoded, so this is a data problem, not a framing problem:
the driver must raise ``DataError`` and keep the connection, exactly as for
invalid UTF-8 (#492) and unresolvable time zones (#413).
"""

from __future__ import annotations

import struct

import pytest

from pycubrid.aio.connection import AsyncConnection
from pycubrid.connection import Connection
from pycubrid.constants import CUBRIDDataType, CUBRIDStatementType
from pycubrid.exceptions import DataError
from pycubrid.packet import PacketReader
from pycubrid.protocol import FetchPacket, PrepareAndExecutePacket, _read_value
from tests.test_connection import make_connected_connection, socket_queue  # noqa: F401
from tests.test_invalid_utf8_response import (
    CAS_INFO,
    _async_connection_with_reply,
    _connection_with_reply,
)
from tests.test_json_decode import _build_select_response

_ZERO_TZ = b"+09:00\x00"

# (column type, wire payload of a zero value of that type)
ZERO_VALUES = [
    pytest.param(CUBRIDDataType.DATE, struct.pack(">3h", 0, 0, 0), id="date"),
    pytest.param(CUBRIDDataType.DATETIME, struct.pack(">7h", 0, 0, 0, 0, 0, 0, 0), id="datetime"),
    pytest.param(CUBRIDDataType.TIMESTAMP, struct.pack(">6h", 0, 0, 0, 0, 0, 0), id="timestamp"),
    pytest.param(
        CUBRIDDataType.TIMESTAMPTZ,
        struct.pack(">6h", 0, 0, 0, 0, 0, 0) + _ZERO_TZ,
        id="timestamptz",
    ),
    pytest.param(
        CUBRIDDataType.TIMESTAMPLTZ,
        struct.pack(">6h", 0, 0, 0, 0, 0, 0) + _ZERO_TZ,
        id="timestampltz",
    ),
    pytest.param(
        CUBRIDDataType.DATETIMETZ,
        struct.pack(">7h", 0, 0, 0, 0, 0, 0, 0) + _ZERO_TZ,
        id="datetimetz",
    ),
    pytest.param(
        CUBRIDDataType.DATETIMELTZ,
        struct.pack(">7h", 0, 0, 0, 0, 0, 0, 0) + _ZERO_TZ,
        id="datetimeltz",
    ),
]


@pytest.mark.parametrize(("column_type", "payload"), ZERO_VALUES)
def test_zero_temporal_value_raises_data_error(column_type: int, payload: bytes) -> None:
    with pytest.raises(DataError, match="cannot be represented") as raised:
        _read_value(PacketReader(payload), column_type, len(payload))
    assert isinstance(raised.value.__cause__, ValueError)


def test_out_of_range_time_raises_data_error() -> None:
    payload = struct.pack(">3h", 25, 0, 0)
    with pytest.raises(DataError, match="cannot be represented"):
        _read_value(PacketReader(payload), CUBRIDDataType.TIME, len(payload))


def test_zero_date_collection_element_raises_data_error() -> None:
    element = struct.pack(">3h", 0, 0, 0)
    payload = struct.pack(">Bi", CUBRIDDataType.DATE, 1) + struct.pack(">i", len(element))
    payload += element
    reader = PacketReader(payload, decode_collections=True)
    with pytest.raises(DataError, match="cannot be represented"):
        _read_value(reader, CUBRIDDataType.SET, len(payload))


def test_valid_temporal_value_is_unchanged() -> None:
    payload = struct.pack(">3h", 2024, 1, 15)
    value = _read_value(PacketReader(payload), CUBRIDDataType.DATE, len(payload))
    assert str(value) == "2024-01-15"


def _zero_date_select_body() -> bytes:
    return _build_select_response([(CUBRIDDataType.DATE, "d")], [struct.pack(">3h", 0, 0, 0)])


def test_zero_date_on_fetch_page_raises_data_error() -> None:
    columns = PrepareAndExecutePacket("SELECT d FROM t")
    columns.parse(
        _build_select_response([(CUBRIDDataType.DATE, "d")], [struct.pack(">3h", 2024, 1, 1)])
    )
    row_value = struct.pack(">3h", 0, 0, 0)
    body = CAS_INFO + struct.pack(">ii", 0, 1) + struct.pack(">i", 1) + b"\x00" * 8
    body += struct.pack(">i", len(row_value)) + row_value
    packet = FetchPacket(1, 0, statement_type=CUBRIDStatementType.SELECT)
    with pytest.raises(DataError, match="cannot be represented"):
        packet.parse(body, columns=columns.columns)


def test_sync_zero_date_keeps_connection(
    socket_queue: list,  # noqa: F811
) -> None:
    conn, _ = _connection_with_reply(socket_queue, _zero_date_select_body())
    assert isinstance(conn, Connection)
    with pytest.raises(DataError, match="cannot be represented"):
        conn._send_and_receive(PrepareAndExecutePacket("SELECT d FROM t"))
    assert conn._connected is True
    assert conn._socket is not None


@pytest.mark.asyncio
async def test_async_zero_date_keeps_connection() -> None:
    conn = _async_connection_with_reply(_zero_date_select_body())
    assert isinstance(conn, AsyncConnection)
    with pytest.raises(DataError, match="cannot be represented"):
        await conn._send_and_receive(PrepareAndExecutePacket("SELECT d FROM t"))
    assert conn._connected is True
    assert conn._writer is not None
