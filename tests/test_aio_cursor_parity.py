from __future__ import annotations

import json
from typing import cast
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from pycubrid.aio import connect
from pycubrid.aio.connection import AsyncConnection
from pycubrid.aio.cursor import AsyncCursor
from pycubrid.constants import CUBRIDDataType, CUBRIDStatementType
from pycubrid.cursor import Cursor
from pycubrid.exceptions import InterfaceError, ProgrammingError
from pycubrid.protocol import ColumnMetaData, FetchPacket, PrepareAndExecutePacket
from tests.test_cursor import _set_prepare_packet


def make_connection() -> MagicMock:
    connection = MagicMock()
    connection._timing = None
    connection._cursors = set()
    connection.autocommit = False
    connection._protocol_version = 1
    connection._decode_collections = False
    connection._json_deserializer = None
    connection._no_backslash_escapes = False
    connection._physical_generation = 1
    connection._generation_for_binding = AsyncMock(return_value=1)
    connection._connected = True
    connection._ensure_connected = MagicMock()
    connection._wait_for_setup_if_needed = AsyncMock()
    connection._send_and_receive = AsyncMock()
    # A pooling-off broker: CLOSE_REQ is sent, never deferred (#488).
    connection._defer_close = MagicMock(return_value=False)
    return connection


@pytest.mark.parametrize("cursor_class", [Cursor, AsyncCursor], ids=["sync", "async"])
@pytest.mark.parametrize("literal_mode", [True, False], ids=["literal", "escaped"])
def test_format_and_bind_adapters_use_the_connection_escape_mode(
    cursor_class: type[Cursor | AsyncCursor],
    literal_mode: bool,
) -> None:
    connection = make_connection()
    connection._no_backslash_escapes = literal_mode
    cursor = cursor_class(connection)
    expected = "'O''Reilly\\path\r\n'" if literal_mode else "'O''Reilly\\\\path\\\r\\\n'"
    value = "O'Reilly\\path\r\n"

    assert cursor._format_parameter(value) == expected
    assert cursor._bind_parameters("SELECT ?", (value,)) == "SELECT " + expected
    connection._send_and_receive.assert_not_called()


@pytest.mark.parametrize("cursor_class", [Cursor, AsyncCursor], ids=["sync", "async"])
def test_format_and_bind_adapters_require_a_negotiated_escape_mode(
    cursor_class: type[Cursor | AsyncCursor],
) -> None:
    connection = make_connection()
    connection._no_backslash_escapes = None
    cursor = cursor_class(connection)

    with pytest.raises(InterfaceError, match="escape mode not negotiated"):
        cursor._format_parameter("value")
    with pytest.raises(InterfaceError, match="escape mode not negotiated"):
        cursor._bind_parameters("SELECT ?", ("value",))
    connection._send_and_receive.assert_not_called()


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
    # autocommit now flows straight through to AsyncConnection's own
    # constructor (applied inside connect()) instead of the factory calling
    # set_autocommit() separately — see AsyncConnection.__init__/connect().
    connection_class.assert_called_once_with(
        host="localhost",
        port=33000,
        database="testdb",
        user="dba",
        password="",
        decode_collections=True,
        json_deserializer=json.loads,
        charset="utf-8",
        autocommit=True,
    )
    mock_connection.connect.assert_awaited_once_with()


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

    assert cursor._format_parameter("line1\rline2\nend") == "'line1\\\rline2\\\nend'"


def test_escape_string_ctrl_z_rejected() -> None:
    # 0x1A has no safe CUBRID literal escape and is rejected in both modes.
    cursor = AsyncCursor(make_connection())
    with pytest.raises(ProgrammingError, match="Ctrl-Z"):
        cursor._format_parameter("data\x1amore")

    connection = make_connection()
    connection._no_backslash_escapes = True
    cursor = AsyncCursor(connection)
    with pytest.raises(ProgrammingError, match="Ctrl-Z"):
        cursor._format_parameter("data\x1amore")


def test_escape_string_no_backslash_escapes_mode() -> None:
    connection = make_connection()
    connection._no_backslash_escapes = True
    cursor = AsyncCursor(connection)

    assert cursor._format_parameter("O'Reilly\\path\r\n") == "'O''Reilly\\path\r\n'"


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

    async def fake_send(packet: FetchPacket, **_: object) -> None:
        assert packet.decode_collections is True
        assert packet.json_deserializer is json.loads
        packet.rows = [([1, 2],)]

    connection._send_and_receive = AsyncMock(side_effect=fake_send)

    assert await cursor._fetch_more_rows() is True
    assert await cursor.fetchone() == ([1, 2],)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "size",
    [
        1.5,
        2.0,
        -1.0,
        0.0,
        True,
        False,
        "2",
        pytest.param(CUBRIDStatementType.SELECT, id="int-subclass"),
    ],
)
@pytest.mark.parametrize("paged", [False, True], ids=["buffered", "paged"])
async def test_fetchmany_rejects_invalid_size_without_consuming_or_fetching(
    size: object, paged: bool
) -> None:
    connection = make_connection()
    cursor = AsyncCursor(connection)

    async def send(packet: object, **_: object) -> None:
        if isinstance(packet, PrepareAndExecutePacket):
            _set_prepare_packet(
                packet, stmt_type=CUBRIDStatementType.SELECT, rows=[(1,)], total_count=3
            )
        elif isinstance(packet, FetchPacket):
            packet.rows = [(2,), (3,)]

    connection._send_and_receive.side_effect = send
    await cursor.execute("SELECT id FROM t")
    if paged:
        assert await cursor.fetchone() == (1,)
    original_rows = cursor._rows.copy()
    original_index = cursor._row_index
    connection._send_and_receive.reset_mock()

    with pytest.raises(ProgrammingError, match="size must be an integer"):
        await cursor.fetchmany(cast(int, size))

    assert cursor._rows == original_rows
    assert cursor._row_index == original_index
    connection._send_and_receive.assert_not_awaited()
    assert await cursor.fetchmany(2) == ([(2,), (3,)] if paged else [(1,), (2,)])
    connection._send_and_receive.assert_awaited_once()


@pytest.mark.asyncio
async def test_fetchmany_with_size_and_default_arraysize() -> None:
    connection = make_connection()
    cursor = AsyncCursor(connection)

    async def send(packet: object, **_: object) -> None:
        if isinstance(packet, PrepareAndExecutePacket):
            _set_prepare_packet(
                packet,
                stmt_type=CUBRIDStatementType.SELECT,
                rows=[(1,), (2,), (3,), (4,), (5,)],
                total_count=5,
            )

    connection._send_and_receive.side_effect = send
    await cursor.execute("SELECT id FROM t")
    assert await cursor.fetchmany(2) == [(1,), (2,)]
    cursor.arraysize = 2
    assert await cursor.fetchmany(None) == [(3,), (4,)]
    assert await cursor.fetchmany() == [(5,)]
    assert await cursor.fetchmany(2) == []


@pytest.mark.asyncio
@pytest.mark.parametrize("cursor_class", [Cursor, AsyncCursor], ids=["sync", "async"])
@pytest.mark.parametrize("closed", [False, True], ids=["no-result", "closed"])
@pytest.mark.parametrize("size", [2.5, True])
async def test_fetchmany_checks_cursor_state_before_size(
    cursor_class: type[Cursor | AsyncCursor], closed: bool, size: object
) -> None:
    connection = make_connection()
    cursor = cursor_class(connection)
    cursor._closed = closed

    with pytest.raises(InterfaceError, match="closed" if closed else "No result set available"):
        if isinstance(cursor, AsyncCursor):
            await cursor.fetchmany(cast(int, size))
        else:
            cursor.fetchmany(cast(int, size))

    connection._send_and_receive.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("cursor_class", [Cursor, AsyncCursor], ids=["sync", "async"])
@pytest.mark.parametrize("size", [0, -1])
@pytest.mark.parametrize("row_index", [0, 1], ids=["buffered", "paged"])
async def test_fetchmany_nonpositive_integer_preserves_result_without_fetching(
    cursor_class: type[Cursor | AsyncCursor], size: int, row_index: int
) -> None:
    connection = make_connection()
    cursor = cursor_class(connection)
    cursor._description = (("id", CUBRIDDataType.INT, None, None, 10, 0, False),)
    cursor._rows = [(1,)]
    cursor._row_index = row_index
    cursor._query_handle = 1
    cursor._fetched_count = 1
    cursor._total_tuple_count = 2

    if isinstance(cursor, AsyncCursor):
        assert await cursor.fetchmany(size) == []
    else:
        assert cursor.fetchmany(size) == []

    assert cursor._rows == [(1,)]
    assert cursor._row_index == row_index
    assert cursor._query_handle == 1
    connection._send_and_receive.assert_not_called()
