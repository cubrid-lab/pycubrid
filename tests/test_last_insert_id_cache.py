"""Exercise identity cache boundaries with real sync/async connections and cursors."""

from __future__ import annotations

from types import SimpleNamespace

import inspect
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from pycubrid.aio.connection import AsyncConnection
from pycubrid.connection import Connection
from pycubrid.constants import CUBRIDStatementType
from pycubrid.exceptions import InterfaceError, OperationalError, ProgrammingError
from pycubrid.protocol import (
    BatchExecutePacket,
    CloseQueryPacket,
    GetLastInsertIdPacket,
    PrepareAndExecutePacket,
)

pytestmark = pytest.mark.asyncio


async def _call(value: Any) -> Any:
    return await value if inspect.isawaitable(value) else value


@pytest.fixture(params=["sync", "async"])
def connection(request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch) -> Any:
    if request.param == "sync":
        with monkeypatch.context() as patch:
            patch.setattr(Connection, "connect", lambda self: None)
            conn = Connection("localhost", 33000, "testdb", "dba", "")
    else:
        conn = AsyncConnection("localhost", 33000, "testdb", "dba", "")
    conn._connected = True
    conn._no_backslash_escapes = False
    return conn


def _sender(connection: Any, monkeypatch: pytest.MonkeyPatch, send: Any) -> None:
    if isinstance(connection, AsyncConnection):

        async def async_send(packet: Any, **kwargs: Any) -> Any:
            return send(packet, **kwargs)

        monkeypatch.setattr(connection, "_send_and_receive", async_send)
        monkeypatch.setattr(connection, "_send_and_receive_locked", async_send)
    else:
        monkeypatch.setattr(connection, "_send_and_receive", send)


async def test_identity_survives_transactions_and_other_cursor_select(
    connection: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    ids = iter(["41", "42"])
    requests: list[Any] = []

    def send(packet: Any, **_: object) -> Any:
        requests.append(packet)
        if isinstance(packet, PrepareAndExecutePacket):
            packet.statement_type = (
                CUBRIDStatementType.INSERT if "INSERT" in packet.sql else CUBRIDStatementType.SELECT
            )
        elif isinstance(packet, GetLastInsertIdPacket):
            packet.last_insert_id = next(ids)
        return packet

    _sender(connection, monkeypatch, send)
    first = connection.cursor()
    second = connection.cursor()
    assert await _call(connection.get_last_insert_id()) is None
    await _call(first.execute("INSERT INTO t VALUES (1)"))
    assert first.lastrowid == 41
    assert await _call(connection.get_last_insert_id()) == "41"
    for boundary in (connection.commit, connection.rollback):
        await _call(boundary())
        assert first.lastrowid == 41
        assert await _call(connection.get_last_insert_id()) == "41"
    await _call(second.execute("SELECT 1"))
    assert first.lastrowid == 41
    assert second.lastrowid is None
    assert await _call(connection.get_last_insert_id()) == "41"
    await _call(second.execute("INSERT INTO t VALUES (2)"))
    assert first.lastrowid == 41
    assert second.lastrowid == 42
    count = len(requests)
    assert await _call(connection.get_last_insert_id()) == "42"
    assert len(requests) == count  # Reading the connection cache does no I/O.


@pytest.mark.parametrize(
    "outcome",
    [
        "",
        "not-an-id",
        None,
        InterfaceError("id failed"),
        OperationalError("id failed"),
        OSError("id failed"),
        TypeError("id failed"),
        ValueError("id failed"),
    ],
)
async def test_unavailable_identity_clears_previous_value(
    connection: Any, monkeypatch: pytest.MonkeyPatch, outcome: Any
) -> None:
    connection._last_insert_id = "99"
    cur = connection.cursor()
    cur._lastrowid = 99

    def send(packet: Any) -> Any:
        if isinstance(packet, PrepareAndExecutePacket):
            packet.statement_type = CUBRIDStatementType.INSERT
        elif isinstance(packet, GetLastInsertIdPacket):
            if isinstance(outcome, Exception):
                raise outcome
            packet.last_insert_id = outcome
        return packet

    _sender(connection, monkeypatch, send)
    await _call(cur.execute("INSERT INTO t VALUES (1)"))
    assert cur.lastrowid is None
    assert await _call(connection.get_last_insert_id()) is None


@pytest.mark.parametrize(
    "sql",
    [
        "INSERT INTO t VALUES (1)",
        "INSERT/* adjacent comment */INTO t VALUES (1)",
        "INSERT-- adjacent comment\nINTO t VALUES (1)",
        "INSERT// adjacent comment\nINTO t VALUES (1)",
        "/* comment */\n-- comment\nINSERT INTO t VALUES (1)",
        "// comment\nINSERT INTO t VALUES (1)",
    ],
)
async def test_failed_insert_request_does_not_expose_previous_identity(
    connection: Any, monkeypatch: pytest.MonkeyPatch, sql: str
) -> None:
    connection._last_insert_id = "99"
    cur = connection.cursor()
    cur._lastrowid = 99

    def send(packet: Any) -> Any:
        raise ProgrammingError("insert failed")

    _sender(connection, monkeypatch, send)
    with pytest.raises(ProgrammingError, match="insert failed"):
        await _call(cur.execute(sql))
    assert cur.lastrowid is None
    assert await _call(connection.get_last_insert_id()) is None


async def test_insert_binding_failure_clears_previous_identity(connection: Any) -> None:
    connection._last_insert_id = "99"
    cur = connection.cursor()
    with pytest.raises(ProgrammingError):
        await _call(cur.execute("INSERT INTO t VALUES (?)", []))
    assert await _call(connection.get_last_insert_id()) is None


@pytest.mark.parametrize("failed", [False, True])
async def test_nonempty_batch_clears_previous_identity(
    connection: Any, monkeypatch: pytest.MonkeyPatch, failed: bool
) -> None:
    connection._last_insert_id = "99"
    cur = connection.cursor()
    cur._lastrowid = 99

    def send(packet: Any) -> Any:
        assert isinstance(packet, BatchExecutePacket)
        if failed:
            raise ProgrammingError("batch failed")
        return packet

    _sender(connection, monkeypatch, send)
    if failed:
        with pytest.raises(ProgrammingError, match="batch failed"):
            await _call(cur.executemany_batch(["INSERT INTO t VALUES (1)"]))
    else:
        await _call(cur.executemany_batch(["INSERT INTO t VALUES (1)"]))
    assert cur.lastrowid is None
    assert await _call(connection.get_last_insert_id()) is None


async def test_empty_batch_clears_cursor_snapshot_but_preserves_connection_identity(
    connection: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    connection._last_insert_id = "99"
    cur = connection.cursor()
    cur._lastrowid = 99
    _sender(connection, monkeypatch, lambda packet: packet)
    await _call(cur.executemany_batch([]))
    assert cur.lastrowid is None
    assert await _call(connection.get_last_insert_id()) == "99"


@pytest.mark.parametrize("operation", ["INSERT INTO t VALUES (?)", "SELECT ?"])
async def test_empty_executemany_preserves_connection_identity(
    connection: Any, monkeypatch: pytest.MonkeyPatch, operation: str
) -> None:
    connection._last_insert_id = "99"
    cur = connection.cursor()
    cur._lastrowid = 99

    def send(packet: Any) -> Any:
        raise AssertionError("empty executemany must send no SQL")

    _sender(connection, monkeypatch, send)
    await _call(cur.executemany(operation, []))
    assert cur.lastrowid is None
    assert cur.rowcount == 0
    assert await _call(connection.get_last_insert_id()) == "99"


async def test_failed_query_close_preserves_both_identities_before_batch(
    connection: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    connection._last_insert_id = "99"
    cur = connection.cursor()
    cur._lastrowid = 99
    cur._query_handle = 123
    sent: list[Any] = []

    def send(packet: Any, **_: object) -> Any:
        sent.append(packet)
        assert isinstance(packet, CloseQueryPacket)
        raise ProgrammingError("close failed")

    _sender(connection, monkeypatch, send)
    with pytest.raises(ProgrammingError, match="close failed"):
        await _call(cur.executemany_batch(["INSERT INTO t VALUES (1)"]))
    assert len(sent) == 1
    assert cur._query_handle == 123
    assert cur.lastrowid == 99
    assert await _call(connection.get_last_insert_id()) == "99"


@pytest.mark.parametrize("cached", [None, "99"])
async def test_call_does_not_refresh_connection_snapshot(
    connection: Any, monkeypatch: pytest.MonkeyPatch, cached: str | None
) -> None:
    connection._last_insert_id = cached
    sent: list[Any] = []

    def send(packet: Any) -> Any:
        sent.append(packet)
        assert isinstance(packet, PrepareAndExecutePacket)
        packet.statement_type = CUBRIDStatementType.CALL
        return packet

    _sender(connection, monkeypatch, send)
    await _call(connection.cursor().execute("CALL insert_procedure()"))
    assert len(sent) == 1  # No eager identity RPC for an untracked CALL.
    assert await _call(connection.get_last_insert_id()) == cached


async def test_transport_discard_clears_connection_identity_only(connection: Any) -> None:
    connection._last_insert_id = "99"
    cur = connection.cursor()
    cur._lastrowid = 99
    connection._safe_close_socket()
    assert connection._last_insert_id is None
    assert cur.lastrowid == 99
    connection._connected = False
    with pytest.raises(InterfaceError):
        await _call(connection.get_last_insert_id())


async def test_async_transport_shutdown_clears_identity_before_wait(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = AsyncConnection("localhost", 33000, "testdb", "dba", "")
    conn._last_insert_id = "99"
    writer = MagicMock()

    async def wait_closed() -> None:
        assert conn._last_insert_id is None

    writer.wait_closed = wait_closed
    monkeypatch.setattr(conn, "_writer", writer)
    await conn._close_streams()
    assert conn._last_insert_id is None
    assert conn._writer is None


async def test_physical_connect_clears_previous_identity_even_on_failure(
    connection: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    connection._connected = False
    connection._last_insert_id = "99"
    if isinstance(connection, AsyncConnection):
        monkeypatch.setattr(
            connection, "_open_connection", AsyncMock(side_effect=OSError("offline"))
        )
    else:
        monkeypatch.setattr(connection, "_create_socket", MagicMock(side_effect=OSError("offline")))
    with pytest.raises(OperationalError):
        await _call(connection.connect())
    assert connection._last_insert_id is None


async def test_out_tran_keeps_identity_until_physical_reconnect(
    connection: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    connection._last_insert_id = "99"
    connection._record_reply_cas_info(b"\x00\x00\x00\x00")
    if isinstance(connection, AsyncConnection):
        connection._writer = MagicMock()
        connection._writer.wait_closed = AsyncMock()
        connection._physical_generation = 1
        reader, writer = MagicMock(), MagicMock()
        writer.wait_closed = AsyncMock()
        open_connection = AsyncMock(return_value=(reader, writer))

        async def handshake(_reader: Any, _writer: Any) -> None:
            connection._reader = _reader
            connection._writer = _writer

        restore = AsyncMock()
        monkeypatch.setattr(connection, "_open_connection", open_connection)
        monkeypatch.setattr(connection, "_do_connect_handshake", handshake)
        monkeypatch.setattr(connection, "_restore_session_state_locked", restore)
        # A live OUT_TRAN CAS answers the CHECK_CAS probe (#485).
        probe = AsyncMock(return_value=SimpleNamespace(response_code=0))
        monkeypatch.setattr(connection, "_send_and_receive_locked", probe)
        assert await connection._check_reconnect_locked() is False
        monkeypatch.delattr(connection, "_send_and_receive_locked")
        probe.assert_awaited_once()
        assert connection._last_insert_id == "99"
        open_connection.assert_not_awaited()
        restore.assert_not_awaited()
        await connection._close_streams()
        connection._connected = False
    else:
        connection._socket = MagicMock()
        connection._physical_generation = 1

        def physical_open() -> None:
            connection._connected = True
            connection._physical_generation += 1

        # Fake only the physical open: connect() itself restores state (#520).
        connect = MagicMock(side_effect=physical_open)
        restore = MagicMock()
        monkeypatch.setattr(connection, "_connect_locked", connect)
        monkeypatch.setattr(connection, "_restore_session_state", restore)
        # A live OUT_TRAN CAS answers the CHECK_CAS probe (#485).
        probe = MagicMock(return_value=SimpleNamespace(response_code=0))
        monkeypatch.setattr(connection, "_send_and_receive_locked", probe)
        assert connection._check_reconnect() is False
        monkeypatch.delattr(connection, "_send_and_receive_locked")
        probe.assert_called_once()
        assert connection._last_insert_id == "99"
        connect.assert_not_called()
        restore.assert_not_called()
        connection._drop_connection()

    assert await _call(connection.ping(reconnect=True)) is True
    assert connection._last_insert_id is None
    if isinstance(connection, AsyncConnection):
        open_connection.assert_awaited_once()
        assert connection._physical_generation == 2
        restore.assert_awaited_once()
    else:
        connect.assert_called_once()
        restore.assert_called_once()
