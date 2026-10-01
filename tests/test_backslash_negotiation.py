"""Tests for automatic backslash-escape mode negotiation (issue #255).

CUBRID's ``no_backslash_escapes`` system parameter defaults to ``yes``
(a backslash is an ordinary literal character).  When the caller does not
pin the mode explicitly, the driver probes the live server with
``SELECT CHAR_LENGTH('\\\\')`` and derives the correct client-side escaping
behaviour, avoiding the silent doubling of backslashes.
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from pycubrid.aio.connection import AsyncConnection
from pycubrid.connection import Connection
from pycubrid.exceptions import OperationalError
from pycubrid.protocol import PrepareAndExecutePacket

from .test_async import make_streams_for_connect
from .test_connection import build_handshake_response, build_open_db_response, make_socket
from .test_aio_ping import make_async_connection

pytestmark = pytest.mark.no_escape_pin


def _make_sync_conn(
    fetchone_result: object,
    *,
    preset: bool | None = None,
    raise_on_execute: bool = False,
    raise_on_rollback: bool = False,
) -> tuple[Connection, MagicMock]:
    conn = Connection.__new__(Connection)
    conn._no_backslash_escapes = preset
    mock_cursor = MagicMock()
    if raise_on_execute:
        mock_cursor.execute.side_effect = RuntimeError("boom")
    mock_cursor.fetchone.return_value = fetchone_result
    conn.cursor = MagicMock(return_value=mock_cursor)  # type: ignore[method-assign]
    if raise_on_rollback:
        conn.rollback = MagicMock(side_effect=RuntimeError("rollback boom"))  # type: ignore[method-assign]
    else:
        conn.rollback = MagicMock()  # type: ignore[method-assign]
    conn.close = MagicMock()  # type: ignore[method-assign]
    return conn, mock_cursor


class TestSyncNegotiation:
    def test_probe_two_means_literal_mode(self) -> None:
        conn, cur = _make_sync_conn((2,))
        conn._negotiate_backslash_escapes()
        assert conn._no_backslash_escapes is True
        cur.execute.assert_called_once_with("SELECT CHAR_LENGTH('\\\\')")
        conn.rollback.assert_called_once()  # probe transaction discarded

    def test_probe_one_means_escape_mode(self) -> None:
        conn, _ = _make_sync_conn((1,))
        conn._negotiate_backslash_escapes()
        assert conn._no_backslash_escapes is False

    def test_probe_unexpected_raises(self) -> None:
        conn, _ = _make_sync_conn((7,))
        with pytest.raises(OperationalError, match="backslash-escape"):
            conn._negotiate_backslash_escapes()
        assert conn._no_backslash_escapes is None
        conn.rollback.assert_called_once()  # probe tx discarded even on failure

    def test_probe_none_row_raises(self) -> None:
        conn, _ = _make_sync_conn(None)
        with pytest.raises(OperationalError, match="backslash-escape"):
            conn._negotiate_backslash_escapes()
        assert conn._no_backslash_escapes is None
        conn.rollback.assert_called_once()  # probe tx discarded even on failure

    def test_execute_error_raises(self) -> None:
        conn, _ = _make_sync_conn(None, raise_on_execute=True)
        with pytest.raises(OperationalError, match="backslash-escape"):
            conn._negotiate_backslash_escapes()
        assert conn._no_backslash_escapes is None
        conn.rollback.assert_called_once()  # probe tx discarded even on error

    def test_explicit_true_is_not_overridden(self) -> None:
        conn, cur = _make_sync_conn((1,), preset=True)
        conn._negotiate_backslash_escapes()
        assert conn._no_backslash_escapes is True
        cur.execute.assert_not_called()
        conn.rollback.assert_not_called()  # no probe, no transaction to discard

    def test_explicit_false_is_not_overridden(self) -> None:
        conn, cur = _make_sync_conn((2,), preset=False)
        conn._negotiate_backslash_escapes()
        assert conn._no_backslash_escapes is False
        cur.execute.assert_not_called()

    def test_rollback_failure_closes_connection_and_raises(self) -> None:
        # On the success path, a failing probe-transaction rollback must not be
        # swallowed: the connection may be in an unknown tx state, so it is
        # closed and an OperationalError is raised (issue #300).
        conn, _ = _make_sync_conn((2,), raise_on_rollback=True)
        with pytest.raises(OperationalError, match="unknown"):
            conn._negotiate_backslash_escapes()
        conn.rollback.assert_called_once()
        conn.close.assert_called_once()

    def test_rollback_failure_preserves_probe_error(self) -> None:
        # When the probe itself fails AND rollback fails, the original probe
        # error propagates (not the rollback error) but the connection is still
        # closed so it is never reused (issue #300).
        conn, _ = _make_sync_conn(None, raise_on_execute=True, raise_on_rollback=True)
        with pytest.raises(OperationalError, match="detect"):
            conn._negotiate_backslash_escapes()
        conn.close.assert_called_once()


def _make_async_conn(
    fetchone_result: object,
    *,
    preset: bool | None = None,
    raise_on_execute: bool = False,
    raise_on_rollback: bool = False,
) -> tuple[AsyncConnection, AsyncMock]:
    conn = AsyncConnection.__new__(AsyncConnection)
    conn._no_backslash_escapes = preset
    mock_cursor = MagicMock()
    if raise_on_execute:
        mock_cursor.execute = AsyncMock(side_effect=RuntimeError("boom"))
    else:
        mock_cursor.execute = AsyncMock()
    mock_cursor.fetchone = AsyncMock(return_value=fetchone_result)
    mock_cursor.close = AsyncMock()
    conn.cursor = MagicMock(return_value=mock_cursor)  # type: ignore[method-assign]
    conn.close = AsyncMock()  # type: ignore[method-assign]
    if raise_on_rollback:
        conn.rollback = AsyncMock(side_effect=RuntimeError("rollback boom"))  # type: ignore[method-assign]
    else:
        conn.rollback = AsyncMock()  # type: ignore[method-assign]
    return conn, mock_cursor


class TestAsyncNegotiation:
    @pytest.mark.asyncio
    async def test_probe_two_means_literal_mode(self) -> None:
        conn, cur = _make_async_conn((2,))
        await conn._negotiate_backslash_escapes()
        assert conn._no_backslash_escapes is True
        cur.execute.assert_awaited_once_with("SELECT CHAR_LENGTH('\\\\')")
        conn.rollback.assert_awaited_once()  # probe transaction discarded

    @pytest.mark.asyncio
    async def test_probe_one_means_escape_mode(self) -> None:
        conn, _ = _make_async_conn((1,))
        await conn._negotiate_backslash_escapes()
        assert conn._no_backslash_escapes is False

    @pytest.mark.asyncio
    async def test_execute_error_raises(self) -> None:
        conn, _ = _make_async_conn(None, raise_on_execute=True)
        with pytest.raises(OperationalError, match="backslash-escape"):
            await conn._negotiate_backslash_escapes()
        assert conn._no_backslash_escapes is None
        conn.rollback.assert_awaited_once()  # probe tx discarded even on error

    @pytest.mark.asyncio
    async def test_probe_unexpected_raises(self) -> None:
        conn, _ = _make_async_conn((7,))
        with pytest.raises(OperationalError, match="backslash-escape"):
            await conn._negotiate_backslash_escapes()
        assert conn._no_backslash_escapes is None
        conn.rollback.assert_awaited_once()  # probe tx discarded even on failure

    @pytest.mark.asyncio
    async def test_explicit_setting_is_not_overridden(self) -> None:
        conn, cur = _make_async_conn((1,), preset=True)
        await conn._negotiate_backslash_escapes()
        assert conn._no_backslash_escapes is True
        cur.execute.assert_not_awaited()
        conn.rollback.assert_not_awaited()  # no probe, no transaction to discard

    @pytest.mark.asyncio
    async def test_rollback_failure_closes_connection_and_raises(self) -> None:
        # On the success path, a failing probe-transaction rollback must not be
        # swallowed: the connection may be in an unknown tx state, so it is
        # closed and an OperationalError is raised (issue #300).
        conn, _ = _make_async_conn((2,), raise_on_rollback=True)
        with pytest.raises(OperationalError, match="unknown"):
            await conn._negotiate_backslash_escapes()
        conn.rollback.assert_awaited_once()
        conn.close.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_rollback_failure_preserves_probe_error(self) -> None:
        # When the probe itself fails AND rollback fails, the original probe
        # error propagates (not the rollback error) but the connection is still
        # closed so it is never reused (issue #300).
        conn, _ = _make_async_conn(None, raise_on_execute=True, raise_on_rollback=True)
        with pytest.raises(OperationalError, match="detect"):
            await conn._negotiate_backslash_escapes()
        conn.close.assert_awaited_once()


@pytest.mark.parametrize("first_mode,next_mode", [(True, False), (False, True)])
def test_sync_recovery_reprobes_automatic_mode(
    monkeypatch: pytest.MonkeyPatch, first_mode: bool, next_mode: bool
) -> None:
    open_db = build_open_db_response()
    sockets = [
        make_socket([build_handshake_response(), open_db[:4], open_db[4:]]) for _ in range(2)
    ]
    observed: list[bool] = []

    def fake_probe(conn: Connection) -> None:
        mode = (first_mode, next_mode)[len(observed)]
        conn._no_backslash_escapes = mode
        observed.append(mode)

    monkeypatch.setattr(Connection, "_negotiate_backslash_escapes", fake_probe)
    with patch("socket.create_connection", side_effect=sockets):
        conn = Connection("localhost", 33000, "testdb", "dba", "")
        assert conn._no_backslash_escapes == first_mode
        conn._drop_connection()
        assert conn.ping(reconnect=True) is True

    assert observed == [first_mode, next_mode]
    assert conn._no_backslash_escapes == next_mode
    assert conn._physical_generation == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("first_mode,next_mode", [(True, False), (False, True)])
async def test_async_recovery_reprobes_automatic_mode(
    monkeypatch: pytest.MonkeyPatch, first_mode: bool, next_mode: bool
) -> None:
    conn = AsyncConnection("localhost", 33000, "testdb", "dba", "")
    first_reader, first_writer, _ = make_streams_for_connect()
    next_reader, next_writer, _ = make_streams_for_connect()
    conn._open_connection = AsyncMock(
        side_effect=[(first_reader, first_writer), (next_reader, next_writer)]
    )
    observed: list[bool] = []

    async def fake_probe() -> None:
        mode = (first_mode, next_mode)[len(observed)]
        conn._no_backslash_escapes = mode
        observed.append(mode)

    monkeypatch.setattr(conn, "_negotiate_backslash_escapes", fake_probe)
    await conn.connect()
    assert conn._no_backslash_escapes == first_mode
    conn._drop_connection()
    assert await conn.ping(reconnect=True) is True

    assert observed == [first_mode, next_mode]
    assert conn._no_backslash_escapes == next_mode
    assert conn._physical_generation == 2


@pytest.mark.parametrize("mode", [False, True])
def test_sync_explicit_mode_survives_recovery_without_probe(mode: bool) -> None:
    open_db = build_open_db_response()
    sockets = [
        make_socket([build_handshake_response(), open_db[:4], open_db[4:]]) for _ in range(2)
    ]
    with patch("socket.create_connection", side_effect=sockets):
        conn = Connection("localhost", 33000, "testdb", "dba", "", no_backslash_escapes=mode)
        conn._drop_connection()
        assert conn.ping(reconnect=True) is True

    assert conn._no_backslash_escapes is mode
    assert conn._no_backslash_escapes_explicit is True
    assert conn._physical_generation == 2
    assert [sock.sendall.call_count for sock in sockets] == [2, 2]


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", [False, True])
async def test_async_explicit_mode_survives_recovery_without_probe(mode: bool) -> None:
    conn = AsyncConnection("localhost", 33000, "testdb", "dba", "", no_backslash_escapes=mode)
    first_reader, first_writer, _ = make_streams_for_connect()
    next_reader, next_writer, _ = make_streams_for_connect()
    conn._open_connection = AsyncMock(
        side_effect=[(first_reader, first_writer), (next_reader, next_writer)]
    )

    await conn.connect()
    conn._drop_connection()
    assert await conn.ping(reconnect=True) is True

    assert conn._no_backslash_escapes is mode
    assert conn._no_backslash_escapes_explicit is True
    assert conn._physical_generation == 2
    assert [writer.write.call_count for writer in (first_writer, next_writer)] == [2, 2]


def test_sync_healthy_ping_does_not_reprobe(monkeypatch: pytest.MonkeyPatch) -> None:
    open_db = build_open_db_response()
    sock = make_socket([build_handshake_response(), open_db[:4], open_db[4:]])
    probes = 0

    def fake_probe(conn: Connection) -> None:
        nonlocal probes
        probes += 1
        conn._no_backslash_escapes = True

    monkeypatch.setattr(Connection, "_negotiate_backslash_escapes", fake_probe)
    with patch("socket.create_connection", return_value=sock):
        conn = Connection("localhost", 33000, "testdb", "dba", "")
    conn._send_and_receive = MagicMock(return_value=MagicMock(response_code=0))

    assert conn.ping(reconnect=True) is True
    assert probes == 1
    assert conn._physical_generation == 1
    assert conn._socket is sock


@pytest.mark.asyncio
async def test_async_healthy_ping_does_not_reprobe(monkeypatch: pytest.MonkeyPatch) -> None:
    conn = AsyncConnection("localhost", 33000, "testdb", "dba", "")
    reader, writer, _ = make_streams_for_connect()
    conn._open_connection = AsyncMock(return_value=(reader, writer))
    probes = 0

    async def fake_probe() -> None:
        nonlocal probes
        probes += 1
        conn._no_backslash_escapes = True

    monkeypatch.setattr(conn, "_negotiate_backslash_escapes", fake_probe)
    await conn.connect()
    conn._send_and_receive_locked = AsyncMock(return_value=MagicMock(response_code=0))

    assert await conn.ping(reconnect=True) is True
    assert probes == 1
    assert conn._physical_generation == 1
    assert conn._writer is writer


@pytest.mark.parametrize("via_ping", [False, True], ids=["connect", "ping"])
def test_sync_recovery_probe_failure_retires_new_session(
    monkeypatch: pytest.MonkeyPatch, via_ping: bool
) -> None:
    open_db = build_open_db_response()
    sockets = [
        make_socket([build_handshake_response(), open_db[:4], open_db[4:]]) for _ in range(2)
    ]
    probes = 0

    def fake_probe(conn: Connection) -> None:
        nonlocal probes
        probes += 1
        if probes == 2:
            raise OperationalError("new-session probe failed")
        conn._no_backslash_escapes = True

    monkeypatch.setattr(Connection, "_negotiate_backslash_escapes", fake_probe)
    with patch("socket.create_connection", side_effect=sockets):
        conn = Connection("localhost", 33000, "testdb", "dba", "")
        conn._drop_connection()
        if via_ping:
            assert conn.ping(reconnect=True) is False
        else:
            with pytest.raises(OperationalError, match="new-session probe failed"):
                conn.connect()

    assert probes == 2
    assert conn._connected is False
    assert conn._socket is None
    sockets[1].close.assert_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("via_ping", [False, True], ids=["connect", "ping"])
async def test_async_recovery_probe_failure_retires_new_session(
    monkeypatch: pytest.MonkeyPatch, via_ping: bool
) -> None:
    conn = AsyncConnection("localhost", 33000, "testdb", "dba", "")
    first_reader, first_writer, _ = make_streams_for_connect()
    next_reader, next_writer, _ = make_streams_for_connect()
    conn._open_connection = AsyncMock(
        side_effect=[(first_reader, first_writer), (next_reader, next_writer)]
    )
    probes = 0

    async def fake_probe() -> None:
        nonlocal probes
        probes += 1
        if probes == 2:
            raise OperationalError("new-session probe failed")
        conn._no_backslash_escapes = True

    monkeypatch.setattr(conn, "_negotiate_backslash_escapes", fake_probe)
    await conn.connect()
    conn._drop_connection()
    if via_ping:
        assert await conn.ping(reconnect=True) is False
    else:
        with pytest.raises(OperationalError, match="new-session probe failed"):
            await conn.connect()

    assert probes == 2
    assert conn._connected is False
    assert conn._writer is None
    assert conn._setup_done.is_set()
    next_writer.close.assert_called()


@pytest.mark.asyncio
async def test_async_recovery_probe_cancellation_releases_gate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = AsyncConnection("localhost", 33000, "testdb", "dba", "")
    first_reader, first_writer, _ = make_streams_for_connect()
    next_reader, next_writer, _ = make_streams_for_connect()
    conn._open_connection = AsyncMock(
        side_effect=[(first_reader, first_writer), (next_reader, next_writer)]
    )
    probes = 0

    async def fake_probe() -> None:
        nonlocal probes
        probes += 1
        if probes == 2:
            raise asyncio.CancelledError()
        conn._no_backslash_escapes = True

    monkeypatch.setattr(conn, "_negotiate_backslash_escapes", fake_probe)
    await conn.connect()
    conn._drop_connection()
    with pytest.raises(asyncio.CancelledError):
        await conn.ping(reconnect=True)

    assert conn._connected is False
    assert conn._writer is None
    assert conn._setup_done.is_set()


@pytest.mark.asyncio
async def test_async_probe_failure_releases_waiter_without_sending_sql(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = AsyncConnection("localhost", 33000, "testdb", "dba", "")
    first_reader, first_writer, _ = make_streams_for_connect()
    next_reader, next_writer, _ = make_streams_for_connect()
    conn._open_connection = AsyncMock(
        side_effect=[(first_reader, first_writer), (next_reader, next_writer)]
    )
    probe_started = asyncio.Event()
    finish_probe = asyncio.Event()
    probes = 0

    async def fake_probe() -> None:
        nonlocal probes
        probes += 1
        if probes == 1:
            conn._no_backslash_escapes = True
            return
        probe_started.set()
        await finish_probe.wait()
        raise OperationalError("new-session probe failed")

    monkeypatch.setattr(conn, "_negotiate_backslash_escapes", fake_probe)
    conn._do_send_and_receive = AsyncMock()
    await conn.connect()
    conn._drop_connection()

    recovery = asyncio.create_task(conn.ping(reconnect=True))
    await probe_started.wait()
    query = asyncio.create_task(conn._send_and_receive(PrepareAndExecutePacket("SELECT 1")))
    await asyncio.sleep(0)
    assert not query.done()
    finish_probe.set()

    assert await recovery is False
    with pytest.raises(OperationalError, match="new-session probe failed"):
        await asyncio.wait_for(query, timeout=2)
    assert conn._connected is False
    assert conn._setup_done.is_set()
    conn._do_send_and_receive.assert_not_awaited()


@pytest.mark.asyncio
async def test_ping_that_passed_gate_does_not_close_a_new_session_during_probe() -> None:
    conn = AsyncConnection("localhost", 33000, "testdb", "dba", "", no_backslash_escapes=True)
    conn._connected = True
    conn._physical_generation = 1
    conn._reader = MagicMock()
    conn._writer = MagicMock()
    conn._do_send_and_receive = AsyncMock(return_value=MagicMock(response_code=0))
    conn.connect = AsyncMock()

    await conn._lock.acquire()
    ping = asyncio.create_task(conn.ping(reconnect=True))
    await asyncio.sleep(0)  # ping passed the outer gate and queued for _lock.
    new_writer = MagicMock()
    new_writer.wait_closed = AsyncMock()
    conn._writer = new_writer
    conn._reader = MagicMock()
    conn._physical_generation = 2
    conn._setup_owner = asyncio.current_task()
    conn._setup_done.clear()  # Another task is probing a just-opened session.
    conn._lock.release()
    await asyncio.sleep(0)
    ping_waited = not ping.done()
    new_writer_closed = new_writer.close.called
    conn._setup_owner = None
    conn._setup_done.set()

    assert ping_waited
    assert not new_writer_closed
    assert await asyncio.wait_for(ping, timeout=2) is True
    assert conn._writer is new_writer
    assert conn._physical_generation == 2
    conn.connect.assert_not_awaited()


def test_sync_probe_error_survives_secondary_close_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    open_db = build_open_db_response()
    sockets = [
        make_socket([build_handshake_response(), open_db[:4], open_db[4:]]) for _ in range(2)
    ]
    sockets[1].close.side_effect = RuntimeError("secondary close failure")
    probes = 0

    def fake_probe(conn: Connection) -> None:
        nonlocal probes
        probes += 1
        if probes == 2:
            raise OperationalError("primary probe failure")
        conn._no_backslash_escapes = True

    monkeypatch.setattr(Connection, "_negotiate_backslash_escapes", fake_probe)
    with patch("socket.create_connection", side_effect=sockets):
        conn = Connection("localhost", 33000, "testdb", "dba", "")
        conn._drop_connection()
        with pytest.raises(OperationalError, match="primary probe failure"):
            conn.connect()

    assert conn._connected is False
    assert conn._socket is None


@pytest.mark.asyncio
async def test_async_probe_error_survives_secondary_close_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = AsyncConnection("localhost", 33000, "testdb", "dba", "")
    first_reader, first_writer, _ = make_streams_for_connect()
    next_reader, next_writer, _ = make_streams_for_connect()
    next_writer.close.side_effect = RuntimeError("secondary close failure")
    conn._open_connection = AsyncMock(
        side_effect=[(first_reader, first_writer), (next_reader, next_writer)]
    )
    probes = 0

    async def fake_probe() -> None:
        nonlocal probes
        probes += 1
        if probes == 2:
            raise OperationalError("primary probe failure")
        conn._no_backslash_escapes = True

    monkeypatch.setattr(conn, "_negotiate_backslash_escapes", fake_probe)
    await conn.connect()
    conn._drop_connection()
    with pytest.raises(OperationalError, match="primary probe failure"):
        await conn.connect()

    assert conn._connected is False
    assert conn._writer is None
    assert conn._setup_done.is_set()


@pytest.mark.asyncio
@pytest.mark.parametrize("batch", [False, True], ids=["execute", "executemany"])
async def test_async_prebound_sql_cannot_cross_mode_generation(
    monkeypatch: pytest.MonkeyPatch, batch: bool
) -> None:
    conn, _, _ = make_async_connection()
    conn._physical_generation = 1
    conn._no_backslash_escapes = True
    conn._do_send_and_receive = AsyncMock()
    cursor = conn.cursor()
    bound = asyncio.Event()
    original_bind = cursor._bind_parameters

    def record_bind(operation: str, parameters: object) -> str:
        sql = original_bind(operation, parameters)
        bound.set()
        return sql

    monkeypatch.setattr(cursor, "_bind_parameters", record_bind)
    original_generation = conn._generation_for_binding

    async def generation_then_hold_lock() -> int:
        # Bind against generation 1, then keep the send waiting on _lock.
        generation = await original_generation()
        await conn._lock.acquire()
        return generation

    monkeypatch.setattr(conn, "_generation_for_binding", generation_then_hold_lock)
    if batch:
        task = asyncio.create_task(cursor.executemany("INSERT INTO t VALUES (?)", [(r"a\b",)]))
    else:
        task = asyncio.create_task(cursor.execute("SELECT ?", (r"a\b",)))
    await asyncio.wait_for(bound.wait(), timeout=2)
    conn._physical_generation = 2
    conn._no_backslash_escapes = False
    conn._lock.release()

    with pytest.raises(OperationalError, match="parameter binding; retry operation"):
        await asyncio.wait_for(task, timeout=2)
    conn._do_send_and_receive.assert_not_awaited()
