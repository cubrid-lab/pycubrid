from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from pycubrid.aio.connection import AsyncConnection
from pycubrid.constants import CUBRIDStatementType
from pycubrid.exceptions import InterfaceError, OperationalError
from pycubrid.protocol import CloseDatabasePacket


def make_connected_async_connection() -> AsyncConnection:
    conn = AsyncConnection("localhost", 33000, "testdb", "dba", "")
    conn._connected = True
    conn._cas_info = b"\x01\x01\x02\x03"
    conn._reader = MagicMock()
    conn._writer = MagicMock()
    conn._writer.close = MagicMock()
    conn._writer.wait_closed = AsyncMock()
    return conn


@pytest.mark.asyncio
async def test_concurrent_execute_calls_are_serialized_per_connection() -> None:
    conn = make_connected_async_connection()
    cur1 = conn.cursor()
    cur2 = conn.cursor()

    first_started = asyncio.Event()
    release_first = asyncio.Event()
    in_flight = 0
    max_in_flight = 0
    query_handle = 0

    async def fake_do_send_and_receive(packet: Any) -> Any:
        nonlocal in_flight, max_in_flight, query_handle

        in_flight += 1
        max_in_flight = max(max_in_flight, in_flight)
        try:
            if max_in_flight > 1:
                raise AssertionError("request/response sections overlapped")

            if not first_started.is_set():
                first_started.set()
                await release_first.wait()

            await asyncio.sleep(0)

            query_handle += 1
            packet.query_handle = query_handle
            packet.statement_type = CUBRIDStatementType.SELECT
            packet.column_count = 0
            packet.columns = []
            packet.total_tuple_count = 0
            packet.rows = []
            packet.result_infos = []
            return packet
        finally:
            in_flight -= 1

    conn._do_send_and_receive = AsyncMock(side_effect=fake_do_send_and_receive)

    task1 = asyncio.create_task(cur1.execute("SELECT 1"))
    await first_started.wait()

    task2 = asyncio.create_task(cur2.execute("SELECT 2"))
    await asyncio.sleep(0)
    assert not task2.done()

    release_first.set()
    await asyncio.gather(task1, task2)

    assert max_in_flight == 1
    assert cur1._query_handle == 1
    assert cur2._query_handle == 2


@pytest.mark.asyncio
async def test_close_waits_for_in_flight_send_and_receive() -> None:
    conn = make_connected_async_connection()
    started = asyncio.Event()
    release_request = asyncio.Event()
    events: list[str] = []

    async def fake_do_send_and_receive(packet: Any) -> Any:
        if isinstance(packet, CloseDatabasePacket):
            events.append("close")
            return packet

        events.append("request-start")
        started.set()
        await release_request.wait()
        events.append("request-finish")
        return packet

    conn._do_send_and_receive = AsyncMock(side_effect=fake_do_send_and_receive)
    request = MagicMock()

    request_task = asyncio.create_task(conn._send_and_receive(request))
    await started.wait()

    close_task = asyncio.create_task(conn.close())
    await asyncio.sleep(0)
    assert not close_task.done()

    release_request.set()

    try:
        result = await request_task
        assert result is request
    except (InterfaceError, OperationalError):
        pass

    await close_task

    assert events == ["request-start", "request-finish", "close"]
    assert conn._connected is False
    assert conn._writer is None


@pytest.mark.asyncio
async def test_concurrent_connect_performs_single_handshake() -> None:
    conn = AsyncConnection("localhost", 33000, "testdb", "dba", "")

    handshake_calls = 0
    started = asyncio.Event()
    release = asyncio.Event()

    async def fake_open_connection(host: str, port: int) -> tuple[MagicMock, MagicMock]:
        reader = MagicMock()
        writer = MagicMock()
        writer.close = MagicMock()
        writer.wait_closed = AsyncMock()
        return reader, writer

    async def fake_do_connect_handshake(hs_reader: Any, hs_writer: Any) -> None:
        nonlocal handshake_calls
        handshake_calls += 1
        # Block the first handshake until we've queued all concurrent callers,
        # forcing them to contend on the connect path.
        started.set()
        await release.wait()
        conn._reader = hs_reader
        conn._writer = hs_writer
        conn._cas_info = b"\x01\x00\x00\x00"
        conn._session_id = 1

    conn._open_connection = AsyncMock(side_effect=fake_open_connection)
    conn._do_connect_handshake = AsyncMock(side_effect=fake_do_connect_handshake)

    n = 5
    first = asyncio.create_task(conn.connect())
    await started.wait()
    others = [asyncio.create_task(conn.connect()) for _ in range(n - 1)]
    await asyncio.sleep(0)
    for task in others:
        assert not task.done(), "concurrent connect() should wait on the lock"

    release.set()
    await asyncio.gather(first, *others)

    assert handshake_calls == 1, (
        f"expected single handshake under concurrency, got {handshake_calls}"
    )
    assert conn._connected is True


@pytest.mark.asyncio
async def test_concurrent_ping_reconnect_performs_single_reconnect() -> None:
    conn = make_connected_async_connection()
    conn._cas_info = bytes([AsyncConnection._CAS_INFO_STATUS_INACTIVE, 0x00, 0x00, 0x00])

    handshake_calls = 0

    async def fake_open_connection(host: str, port: int) -> tuple[Any, Any]:
        reader = MagicMock()
        writer = MagicMock()
        writer.close = MagicMock()
        writer.wait_closed = AsyncMock()
        return reader, writer

    async def fake_do_connect_handshake(hs_reader: Any, hs_writer: Any) -> None:
        nonlocal handshake_calls
        handshake_calls += 1
        conn._reader = hs_reader
        conn._writer = hs_writer
        conn._cas_info = bytes([AsyncConnection._CAS_INFO_STATUS_ACTIVE, 0x00, 0x00, 0x00])
        conn._session_id = 1

    async def fake_do_send_and_receive(packet: Any) -> Any:
        packet.response_code = 0
        return packet

    conn._open_connection = AsyncMock(side_effect=fake_open_connection)
    conn._do_connect_handshake = AsyncMock(side_effect=fake_do_connect_handshake)
    conn._do_send_and_receive = AsyncMock(side_effect=fake_do_send_and_receive)

    n = 5
    results = await asyncio.gather(*[conn.ping(reconnect=True) for _ in range(n)])

    assert all(results)
    assert handshake_calls == 1
    assert conn._connected is True


@pytest.mark.asyncio
async def test_concurrent_ping_reconnect_with_subclass_connect_no_deadlock() -> None:
    class _TracingAsyncConnection(AsyncConnection):
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            super().__init__(*args, **kwargs)
            self.subclass_connect_calls = 0

        async def connect(self) -> None:
            self.subclass_connect_calls += 1
            if self._connected:
                return
            await self._connect_locked()

    conn = _TracingAsyncConnection("localhost", 33000, "testdb", "dba", "")
    conn._connected = True
    conn._cas_info = bytes([AsyncConnection._CAS_INFO_STATUS_INACTIVE, 0x00, 0x00, 0x00])
    conn._reader = MagicMock()
    conn._writer = MagicMock()
    conn._writer.close = MagicMock()
    conn._writer.wait_closed = AsyncMock()

    async def fake_open_connection(host: str, port: int) -> tuple[Any, Any]:
        reader = MagicMock()
        writer = MagicMock()
        writer.close = MagicMock()
        writer.wait_closed = AsyncMock()
        return reader, writer

    async def fake_do_connect_handshake(hs_reader: Any, hs_writer: Any) -> None:
        conn._reader = hs_reader
        conn._writer = hs_writer
        conn._cas_info = bytes([AsyncConnection._CAS_INFO_STATUS_ACTIVE, 0x00, 0x00, 0x00])
        conn._session_id = 1

    async def fake_do_send_and_receive(packet: Any) -> Any:
        packet.response_code = 0
        return packet

    conn._open_connection = AsyncMock(side_effect=fake_open_connection)
    conn._do_connect_handshake = AsyncMock(side_effect=fake_do_connect_handshake)
    conn._do_send_and_receive = AsyncMock(side_effect=fake_do_send_and_receive)

    n = 3
    try:
        results = await asyncio.wait_for(
            asyncio.gather(*[conn.ping(reconnect=True) for _ in range(n)]),
            timeout=2.0,
        )
    except asyncio.TimeoutError:
        pytest.fail("ping(reconnect=True) with subclass connect() override deadlocked")

    assert all(results)
    assert conn.subclass_connect_calls >= 1


@pytest.mark.asyncio
async def test_setup_gate_orders_negotiate_before_autocommit_and_fences_queries() -> None:
    """Regression for #264: async setup runs connect -> negotiate -> autocommit
    (matching the sync driver), the negotiation probe (same task) bypasses the
    setup gate without deadlocking, and a concurrent public query is fenced
    until the backslash-escape mode is pinned.
    """
    conn = AsyncConnection("localhost", 33000, "testdb", "dba", "")
    conn._pending_autocommit = True

    events: list[str] = []
    negotiate_started = asyncio.Event()
    release_negotiate = asyncio.Event()

    async def fake_connect_locked() -> None:
        events.append("connect")
        conn._connected = True

    async def fake_negotiate() -> None:
        if conn._no_backslash_escapes is not None:
            return
        events.append("negotiate:start")
        negotiate_started.set()
        # The owning task's own probe must bypass the setup gate (no deadlock).
        await conn._send_and_receive("probe")
        await release_negotiate.wait()
        conn._no_backslash_escapes = False
        events.append("negotiate:end")

    async def fake_apply_autocommit() -> None:
        # Ordering guarantee: escape mode is pinned before autocommit is applied.
        assert conn._no_backslash_escapes is not None
        events.append("autocommit")
        conn._pending_autocommit = False

    async def fake_send_locked(packet: Any, *, allow_reconnect: bool = True) -> Any:
        events.append(f"{packet}-send")
        return packet

    conn._connect_locked = fake_connect_locked  # type: ignore[method-assign]
    conn._negotiate_backslash_escapes = fake_negotiate  # type: ignore[method-assign]
    conn._apply_pending_autocommit_locked = fake_apply_autocommit  # type: ignore[method-assign]
    conn._send_and_receive_locked = fake_send_locked  # type: ignore[method-assign]

    connect_task = asyncio.create_task(conn.connect())
    await negotiate_started.wait()

    # A concurrent public query must not slip past the setup gate while the
    # escape mode is still unpinned.
    query_task = asyncio.create_task(conn._send_and_receive("query"))
    await asyncio.sleep(0)
    assert not query_task.done(), "query must be fenced until setup completes"
    assert "query-send" not in events

    release_negotiate.set()
    await connect_task
    await query_task

    assert events == [
        "connect",
        "negotiate:start",
        "probe-send",
        "negotiate:end",
        "autocommit",
        "query-send",
    ]
    assert conn._no_backslash_escapes is not None


@pytest.mark.asyncio
async def test_setup_gate_propagates_setup_failure_to_waiters() -> None:
    """Regression for #264: if setup fails, a task waiting on the setup gate
    wakes and receives the setup error instead of hanging or running against a
    half-initialized connection.
    """
    conn = AsyncConnection("localhost", 33000, "testdb", "dba", "")

    negotiate_started = asyncio.Event()
    release_negotiate = asyncio.Event()

    async def fake_connect_locked() -> None:
        conn._connected = True

    async def fake_negotiate() -> None:
        negotiate_started.set()
        await release_negotiate.wait()
        raise OperationalError("probe failed")

    conn._connect_locked = fake_connect_locked  # type: ignore[method-assign]
    conn._negotiate_backslash_escapes = fake_negotiate  # type: ignore[method-assign]

    connect_task = asyncio.create_task(conn.connect())
    await negotiate_started.wait()

    query_task = asyncio.create_task(conn._send_and_receive("query"))
    await asyncio.sleep(0)
    assert not query_task.done()

    release_negotiate.set()

    with pytest.raises(OperationalError, match="probe failed"):
        await connect_task
    with pytest.raises(OperationalError, match="probe failed"):
        await query_task
