"""Failed execute calls must not expose a previous result set (#373)."""

from __future__ import annotations

import asyncio
import inspect
from typing import Any
from unittest.mock import MagicMock

import pytest

from pycubrid.aio.cursor import AsyncCursor
from pycubrid.constants import CUBRIDStatementType
from pycubrid.cursor import Cursor
from pycubrid.exceptions import InterfaceError, OperationalError, ProgrammingError
from pycubrid.protocol import CloseQueryPacket, GetLastInsertIdPacket, PrepareAndExecutePacket
from tests.test_aio_cursor_parity import make_connection
from tests.test_cursor import _set_prepare_packet


@pytest.fixture(params=[False, True], ids=["sync", "async"])
def cursor_case(request: pytest.FixtureRequest) -> tuple[Cursor | AsyncCursor, MagicMock]:
    connection = make_connection()
    if request.param:
        cursor: Cursor | AsyncCursor = AsyncCursor(connection)
    else:
        connection._send_and_receive = MagicMock()
        connection._generation_for_binding = MagicMock(return_value=1)
        cursor = Cursor(connection)
    return cursor, connection


async def _call(cursor: Cursor | AsyncCursor, method: str, *args: Any) -> Any:
    result = getattr(cursor, method)(*args)
    return await result if inspect.isawaitable(result) else result


def _select_reply(packet: object, **kwargs: object) -> object:
    if isinstance(packet, PrepareAndExecutePacket):
        _set_prepare_packet(
            packet,
            stmt_type=CUBRIDStatementType.SELECT,
            rows=[(1,), (2,), (3,)],
            total_count=3,
        )
    return packet


async def _assert_no_result(cursor: Cursor | AsyncCursor) -> None:
    assert cursor._page_error is None
    assert cursor._columns == []
    assert cursor._rows == []
    assert cursor._row_index == 0
    assert cursor._fetched_count == 0
    assert cursor._total_tuple_count == 0
    assert cursor.description is None
    assert cursor.rowcount == -1
    assert cursor.lastrowid is None
    for method in ("fetchone", "fetchmany", "fetchall"):
        with pytest.raises(InterfaceError, match="No result set available"):
            await _call(cursor, method)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["sql", "transport", "binding", "generation"])
async def test_failed_execute_clears_previous_result(
    cursor_case: tuple[Cursor | AsyncCursor, MagicMock], failure: str
) -> None:
    cursor, connection = cursor_case
    connection._send_and_receive.side_effect = _select_reply
    await _call(cursor, "execute", "SELECT id FROM t")
    assert await _call(cursor, "fetchone") == (1,)
    connection._last_insert_id = "88"
    error = (
        ProgrammingError("invalid SQL") if failure == "sql" else OperationalError("lost session")
    )

    def send(packet: object, **kwargs: object) -> object:
        if isinstance(packet, PrepareAndExecutePacket):
            raise error
        return packet

    connection._send_and_receive.reset_mock()
    connection._send_and_receive.side_effect = send
    if failure == "generation":
        connection._generation_for_binding.side_effect = error
    operation = "SELECT ?" if failure in {"binding", "generation"} else "INVALID SQL"
    parameters = (1, 2) if failure == "binding" else (1,) if failure == "generation" else None
    with pytest.raises((ProgrammingError, OperationalError)) as raised:
        await _call(cursor, "execute", operation, parameters)
    if failure != "binding":
        assert raised.value is error

    await _assert_no_result(cursor)
    assert cursor._query_handle is None
    assert connection._last_insert_id == "88"
    packets = [call.args[0] for call in connection._send_and_receive.call_args_list]
    assert isinstance(packets[0], CloseQueryPacket)
    assert packets[0].query_handle == 1
    assert len(packets) == (1 if failure in {"binding", "generation"} else 2)

    connection._send_and_receive.side_effect = _select_reply
    connection._generation_for_binding.side_effect = None
    await _call(cursor, "execute", "SELECT id FROM t")
    assert await _call(cursor, "fetchall") == [(1,), (2,), (3,)]


@pytest.mark.asyncio
async def test_failed_execute_clears_previous_insert_metadata(
    cursor_case: tuple[Cursor | AsyncCursor, MagicMock],
) -> None:
    cursor, connection = cursor_case

    def insert_reply(packet: object, **kwargs: object) -> object:
        if isinstance(packet, PrepareAndExecutePacket):
            _set_prepare_packet(
                packet, stmt_type=CUBRIDStatementType.INSERT, result_count=3, with_columns=False
            )
        elif isinstance(packet, GetLastInsertIdPacket):
            packet.last_insert_id = "88"
        return packet

    connection._send_and_receive.side_effect = insert_reply
    await _call(cursor, "execute", "INSERT INTO t VALUES (1), (2), (3)")
    assert cursor.rowcount == 3
    assert cursor.lastrowid == 88

    error = ProgrammingError("invalid SQL")

    def fail(packet: object, **kwargs: object) -> object:
        if isinstance(packet, PrepareAndExecutePacket):
            raise error
        return packet

    connection._send_and_receive.side_effect = fail
    with pytest.raises(ProgrammingError) as raised:
        await _call(cursor, "execute", "INVALID SQL")
    assert raised.value is error
    await _assert_no_result(cursor)
    assert connection._last_insert_id == "88"


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["SELECT 99", "INSERT INTO t VALUES (99)"])
async def test_execute_close_failure_preserves_previous_result_and_handle(
    cursor_case: tuple[Cursor | AsyncCursor, MagicMock],
    operation: str,
) -> None:
    cursor, connection = cursor_case
    connection._send_and_receive.side_effect = _select_reply
    await _call(cursor, "execute", "SELECT id FROM t")
    assert await _call(cursor, "fetchone") == (1,)
    previous_description = cursor.description
    connection._last_insert_id = "88"
    error = ProgrammingError("close failed")
    connection._send_and_receive.reset_mock()
    connection._send_and_receive.side_effect = error

    with pytest.raises(ProgrammingError) as raised:
        await _call(cursor, "execute", operation)
    assert raised.value is error
    assert cursor.description == previous_description
    assert cursor._query_handle == 1
    assert await _call(cursor, "fetchone") == (2,)
    assert connection._last_insert_id == (None if operation.startswith("INSERT") else "88")
    connection._send_and_receive.assert_called_once()

    connection._send_and_receive.side_effect = _select_reply
    await _call(cursor, "execute", "SELECT id FROM t")
    packets = [call.args[0] for call in connection._send_and_receive.call_args_list]
    assert [packet.query_handle for packet in packets if isinstance(packet, CloseQueryPacket)] == [
        1,
        1,
    ]
    assert await _call(cursor, "fetchall") == [(1,), (2,), (3,)]


@pytest.mark.asyncio
async def test_async_execute_cancellation_clears_previous_result() -> None:
    connection = make_connection()
    cursor = AsyncCursor(connection)
    connection._send_and_receive.side_effect = _select_reply
    await cursor.execute("SELECT id FROM t")
    assert await cursor.fetchone() == (1,)
    started = asyncio.Event()

    async def wait_for_reply(packet: object, **kwargs: object) -> object:
        if isinstance(packet, PrepareAndExecutePacket):
            started.set()
            await asyncio.Future()
        return packet

    connection._send_and_receive.side_effect = wait_for_reply
    task = asyncio.create_task(cursor.execute("SELECT 99"))
    await asyncio.wait_for(started.wait(), timeout=5)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, timeout=5)
    await _assert_no_result(cursor)
    assert cursor._query_handle is None
