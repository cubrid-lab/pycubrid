"""Setup-gate failure isolation for ``AsyncConnection.connect()`` (#554).

The task that runs setup and the tasks waiting on the setup gate must not share
one exception instance: cancelling the setup owner is that task's cancellation
only, and each waiter receives its own DB-API exception.
"""

from __future__ import annotations

import asyncio
import traceback
from typing import Any

import pytest

from pycubrid.aio.connection import AsyncConnection
from pycubrid.exceptions import InterfaceError, OperationalError


class _GatedSetup:
    """Offline setup stand-in whose escape probe blocks until released."""

    def __init__(self, conn: AsyncConnection, error: BaseException | None = None) -> None:
        self.conn = conn
        self.error = error
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.sent: list[Any] = []
        conn.__dict__["_connect_locked"] = self._connect_locked
        conn.__dict__["_negotiate_backslash_escapes"] = self._negotiate
        conn.__dict__["_send_and_receive_locked"] = self._send_locked

    async def _connect_locked(self) -> None:
        self.conn._connected = True

    async def _negotiate(self) -> None:
        self.started.set()
        await self.release.wait()
        if self.error is not None:
            raise self.error
        self.conn._no_backslash_escapes = True

    async def _send_locked(self, packet: Any, **_: Any) -> Any:
        self.sent.append(packet)
        return packet


async def _owner_and_waiters(
    conn: AsyncConnection, setup: _GatedSetup
) -> tuple[asyncio.Task[None], list[asyncio.Task[Any]]]:
    owner = asyncio.create_task(conn.connect())
    await setup.started.wait()
    waiters = [asyncio.create_task(conn._send_and_receive(f"query-{i}")) for i in range(2)]
    await asyncio.sleep(0)  # Both waiters are now parked on the setup gate.
    assert not any(w.done() for w in waiters)
    return owner, waiters


@pytest.mark.asyncio
async def test_cancelled_setup_owner_does_not_cancel_waiters() -> None:
    conn = AsyncConnection("localhost", 33000, "testdb", "dba", "")
    setup = _GatedSetup(conn)
    owner, waiters = await _owner_and_waiters(conn, setup)

    owner.cancel()
    with pytest.raises(asyncio.CancelledError):
        _ = await owner
    assert owner.cancelled()

    results = await asyncio.gather(*waiters, return_exceptions=True)
    for waiter, result in zip(waiters, results):
        assert not waiter.cancelled()
        assert isinstance(result, OperationalError)
        assert "cancelled" in str(result)
        assert result.__cause__ is None
    assert results[0] is not results[1]

    # The owner's CancelledError was never re-raised in a waiter, so its
    # traceback names neither waiter's frames.
    stored = conn._setup_error
    assert isinstance(stored, asyncio.CancelledError)
    frames = "".join(traceback.format_tb(stored.__traceback__))
    assert "_wait_for_setup_if_needed" not in frames

    # The partially configured session is retired and the gate released.
    assert not conn._connected
    assert conn._setup_done.is_set()
    assert conn._setup_owner is None
    assert setup.sent == []
    del conn.__dict__["_send_and_receive_locked"]  # Real send path from here on.
    with pytest.raises(InterfaceError):
        await conn._send_and_receive("late-query")


@pytest.mark.asyncio
async def test_ordinary_setup_failure_gives_each_waiter_a_fresh_exception() -> None:
    conn = AsyncConnection("localhost", 33000, "testdb", "dba", "")
    original = OperationalError("probe failed", -1, errno=-21003, sqlstate="08S01")
    setup = _GatedSetup(conn, error=original)
    owner, waiters = await _owner_and_waiters(conn, setup)

    setup.release.set()
    with pytest.raises(OperationalError) as owner_info:
        _ = await owner
    assert owner_info.value is original

    results = await asyncio.gather(*waiters, return_exceptions=True)
    for result in results:
        assert type(result) is OperationalError
        assert result is not original
        assert "probe failed" in str(result)
        assert result.code == -1
        assert result.errno == -21003
        assert result.sqlstate == "08S01"
        assert result.__cause__ is original
    assert results[0] is not results[1]
    assert "_wait_for_setup_if_needed" not in "".join(traceback.format_tb(original.__traceback__))
    assert setup.sent == []


@pytest.mark.asyncio
async def test_non_dbapi_setup_failure_is_wrapped_for_waiters() -> None:
    conn = AsyncConnection("localhost", 33000, "testdb", "dba", "")
    original = RuntimeError("boom")
    setup = _GatedSetup(conn, error=original)
    owner, waiters = await _owner_and_waiters(conn, setup)

    setup.release.set()
    with pytest.raises(RuntimeError):
        _ = await owner
    results = await asyncio.gather(*waiters, return_exceptions=True)
    for result in results:
        assert type(result) is OperationalError
        assert "RuntimeError" in str(result)
        assert result.__cause__ is original
    assert results[0] is not results[1]


@pytest.mark.asyncio
async def test_waiter_cancellation_remains_cancellation() -> None:
    conn = AsyncConnection("localhost", 33000, "testdb", "dba", "")
    setup = _GatedSetup(conn)
    owner, waiters = await _owner_and_waiters(conn, setup)

    waiters[0].cancel()
    with pytest.raises(asyncio.CancelledError):
        _ = await waiters[0]

    setup.release.set()
    _ = await owner
    assert await waiters[1] == "query-1"
    assert setup.sent == ["query-1"]


@pytest.mark.asyncio
async def test_reconnect_after_cancelled_setup_succeeds() -> None:
    conn = AsyncConnection("localhost", 33000, "testdb", "dba", "")
    setup = _GatedSetup(conn)
    owner, waiters = await _owner_and_waiters(conn, setup)
    owner.cancel()
    with pytest.raises(asyncio.CancelledError):
        _ = await owner
    await asyncio.gather(*waiters, return_exceptions=True)

    setup.started.clear()
    setup.release.set()
    await conn.connect()
    assert conn._connected
    assert conn._setup_error is None
    assert await conn._send_and_receive("after") == "after"
    assert setup.sent == ["after"]


@pytest.mark.asyncio
async def test_interface_error_setup_failure_keeps_class_for_waiters() -> None:
    conn = AsyncConnection("localhost", 33000, "testdb", "dba", "")
    original = InterfaceError("handshake rejected", 7)
    setup = _GatedSetup(conn, error=original)
    owner, waiters = await _owner_and_waiters(conn, setup)

    setup.release.set()
    with pytest.raises(InterfaceError):
        _ = await owner
    results = await asyncio.gather(*waiters, return_exceptions=True)
    for result in results:
        assert type(result) is InterfaceError
        assert result is not original
        assert result.msg == "handshake rejected"
        assert result.code == 7
        assert result.__cause__ is original
    assert results[0] is not results[1]


class _AppOperationalError(OperationalError):
    """An application subclass outside ``pycubrid.exceptions``."""


class _DetailedOperationalError(_AppOperationalError):
    """A subclass whose constructor differs from ``DatabaseError``."""

    def __init__(self, msg: str, *, detail: str) -> None:
        super().__init__(msg, -5, errno=-21003, sqlstate="08S01")
        self.detail = detail


@pytest.mark.asyncio
async def test_subclass_with_other_constructor_falls_back_to_pycubrid_class() -> None:
    conn = AsyncConnection("localhost", 33000, "testdb", "dba", "")
    original = _DetailedOperationalError("probe failed", detail="x")
    setup = _GatedSetup(conn, error=original)
    owner, waiters = await _owner_and_waiters(conn, setup)

    setup.release.set()
    with pytest.raises(_DetailedOperationalError):
        _ = await owner
    results = await asyncio.gather(*waiters, return_exceptions=True)
    for result in results:
        assert type(result) is OperationalError
        assert result.msg == "probe failed"
        assert result.code == -5
        assert result.errno == -21003
        assert result.sqlstate == "08S01"
        assert result.__cause__ is original
    assert results[0] is not results[1]


@pytest.mark.asyncio
async def test_wrapped_setup_error_with_empty_text_names_its_type() -> None:
    conn = AsyncConnection("localhost", 33000, "testdb", "dba", "")
    original = TimeoutError()
    setup = _GatedSetup(conn, error=original)
    owner, waiters = await _owner_and_waiters(conn, setup)

    setup.release.set()
    with pytest.raises(TimeoutError):
        _ = await owner
    results = await asyncio.gather(*waiters, return_exceptions=True)
    for result in results:
        assert type(result) is OperationalError
        assert str(result) == "connection setup failed in another task: TimeoutError()"
        assert result.__cause__ is original
