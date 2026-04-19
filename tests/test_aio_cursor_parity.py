from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from pycubrid.aio import connect
from pycubrid.aio.connection import AsyncConnection
from pycubrid.aio.cursor import AsyncCursor
from pycubrid.constants import CUBRIDDataType, CUBRIDStatementType
from pycubrid.exceptions import ProgrammingError
from pycubrid.protocol import ColumnMetaData, FetchPacket, PrepareAndExecutePacket


def make_connection() -> MagicMock:
    connection = MagicMock()
    connection._timing = None
    connection._cursors = set()
    connection.autocommit = False
    connection._protocol_version = 1
    connection._decode_collections = False
    connection._json_deserializer = None
    connection._no_backslash_escapes = False
    connection._connected = True
    connection._ensure_connected = MagicMock()
    connection._send_and_receive = AsyncMock()
    return connection


def test_async_connection_stores_parity_kwargs() -> None:
    connection = AsyncConnection(
        "localhost",
        33000,
        "testdb",
        "dba",
        "",
        decode_collections=True,
        json_deserializer=json.loads,
        no_backslash_escapes=True,
    )

    assert connection._decode_collections is True
    assert connection._json_deserializer is json.loads
    assert connection._no_backslash_escapes is True


@pytest.mark.asyncio
async def test_async_connect_threads_decode_collection_and_json_kwargs() -> None:
    mock_connection = MagicMock()
    mock_connection.connect = AsyncMock()
    mock_connection.set_autocommit = AsyncMock()

    with patch("pycubrid.aio.AsyncConnection", return_value=mock_connection) as connection_class:
        result = await connect(
            database="testdb",
            decode_collections=True,
            json_deserializer=json.loads,
            autocommit=True,
        )

    assert result is mock_connection
    connection_class.assert_called_once_with(
        host="localhost",
        port=33000,
        database="testdb",
        user="dba",
        password="",
        decode_collections=True,
        json_deserializer=json.loads,
    )
    mock_connection.connect.assert_awaited_once_with()
    mock_connection.set_autocommit.assert_awaited_once_with(True)


@pytest.mark.asyncio
async def test_execute_rejects_null_byte_before_sending() -> None:
    connection = make_connection()
    cursor = AsyncCursor(connection)

    with pytest.raises(ProgrammingError, match="null byte"):
        await cursor.execute("SELECT ?", ["bad\x00value"])

    connection._send_and_receive.assert_not_awaited()


def test_escape_string_backslash_and_quote() -> None:
    cursor = AsyncCursor(make_connection())

    assert cursor._format_parameter("O'Reilly\\path") == "'O''Reilly\\\\path'"


def test_escape_string_control_characters() -> None:
    cursor = AsyncCursor(make_connection())

    assert cursor._format_parameter("line1\rline2\nend\x1a") == "'line1\\\rline2\\\nend\\\x1a'"


def test_escape_string_no_backslash_escapes_mode() -> None:
    connection = make_connection()
    connection._no_backslash_escapes = True
    cursor = AsyncCursor(connection)

    assert cursor._format_parameter("O'Reilly\\path\r\n\x1a") == "'O''Reilly\\path\r\n\x1a'"


@pytest.mark.asyncio
async def test_execute_threads_protocol_and_decode_options_to_packet() -> None:
    connection = make_connection()
    connection._protocol_version = 8
    connection._decode_collections = True
    connection._json_deserializer = json.loads
    cursor = AsyncCursor(connection)

    async def fake_send(packet: PrepareAndExecutePacket) -> None:
        assert packet.protocol_version == 8
        assert packet.decode_collections is True
        assert packet.json_deserializer is json.loads
        packet.query_handle = 1
        packet.statement_type = CUBRIDStatementType.SELECT
        packet.columns = [ColumnMetaData(name="payload", column_type=CUBRIDDataType.JSON)]
        packet.total_tuple_count = 1
        packet.rows = [({"ok": True},)]
        packet.result_infos = []

    connection._send_and_receive = AsyncMock(side_effect=fake_send)

    await cursor.execute("SELECT payload")

    assert cursor.description == (("payload", CUBRIDDataType.JSON, None, None, -1, -1, False),)
    assert await cursor.fetchone() == ({"ok": True},)


@pytest.mark.asyncio
async def test_fetch_threads_decode_and_json_options_to_packet() -> None:
    connection = make_connection()
    connection._decode_collections = True
    connection._json_deserializer = json.loads
    cursor = AsyncCursor(connection)
    cursor._description = (("items", CUBRIDDataType.SEQUENCE, None, None, 0, 0, False),)
    cursor._columns = [ColumnMetaData(name="items", column_type=CUBRIDDataType.SEQUENCE)]
    cursor._statement_type = CUBRIDStatementType.SELECT
    cursor._query_handle = 1
    cursor._row_index = 0
    cursor._total_tuple_count = 1

    async def fake_send(packet: FetchPacket) -> None:
        assert packet.decode_collections is True
        assert packet.json_deserializer is json.loads
        packet.rows = [([1, 2],)]

    connection._send_and_receive = AsyncMock(side_effect=fake_send)

    assert await cursor._fetch_more_rows() is True
    assert await cursor.fetchone() == ([1, 2],)
