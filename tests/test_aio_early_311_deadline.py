"""Async driver deadlines on CPython 3.11.0-3.11.2 (#744).

On CPython 3.11.0-3.11.2, an ``asyncio.timeout()`` block entered by a task that
was already cancelled (for example cleanup code that caught ``CancelledError``)
re-raises ``CancelledError`` instead of ``TimeoutError`` when its own deadline
expires: its exit check is ``task.uncancel() == 0`` (python/cpython#102780,
fixed in 3.11.3 by comparing against the count recorded on entry). A pycubrid
read deadline would then take the cancellation branch instead of raising
``OperationalError``. The driver therefore keeps ``asyncio.wait_for()``.

Latest-patch 3.11 CI cannot observe the defect, so every test here runs twice:

* ``interpreter`` -- the running interpreter's own asyncio. On an actual
  3.11.0-3.11.2 interpreter this is the affected behavior itself.
* ``simulated-cpython-3.11.2`` -- SEMANTIC SIMULATION, not a CPython 3.11.2
  run: ``asyncio.timeout`` and ``asyncio.wait_for`` are replaced by minimal
  transcriptions of the v3.11.2 ``Lib/asyncio/timeouts.py`` exit condition and
  ``Lib/asyncio/tasks.py`` ``wait_for`` control flow, so the regression is
  reproducible on any supported Python.
"""

from __future__ import annotations

import asyncio
import contextlib
import struct
import sys
from collections.abc import Awaitable, Callable, Iterator
from types import TracebackType
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from pycubrid.aio.connection import AsyncConnection
from pycubrid.exceptions import OperationalError
from pycubrid.protocol import CommitPacket

pytestmark = pytest.mark.asyncio

CAS_INFO = b"\x01\x01\x02\x03"
DEADLINE = 0.05
GENEROUS = 5.0
SITES = ["tcp-connect", "connect-handshake", "round-trip"]
AFFECTED_INTERPRETER = (3, 11, 0) <= sys.version_info[:3] <= (3, 11, 2)


# -- simulated CPython 3.11.2 asyncio (semantic simulation, see module doc) ---


class _Py3112Timeout:
    """``asyncio.timeout()`` with the v3.11.2 exit condition."""

    def __init__(self, delay: float | None) -> None:
        self._delay = delay
        self._handle: asyncio.TimerHandle | None = None
        self._expired = False
        self._task: asyncio.Task[Any] | None = None

    async def __aenter__(self) -> _Py3112Timeout:
        self._task = asyncio.current_task()
        if self._delay is not None:
            self._handle = asyncio.get_running_loop().call_later(self._delay, self._on_timeout)
        return self

    def _on_timeout(self) -> None:
        assert self._task is not None
        self._task.cancel()
        self._expired = True

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        if self._handle is not None:
            self._handle.cancel()
        if self._expired:
            assert self._task is not None
            # v3.11.2: converts only when no other cancel request is pending.
            if self._task.uncancel() == 0 and exc_type is asyncio.CancelledError:
                raise TimeoutError from exc


async def _cancel_and_wait(fut: asyncio.Future[Any]) -> None:
    waiter = asyncio.get_running_loop().create_future()

    def release(_: object) -> None:
        if not waiter.done():
            waiter.set_result(None)

    fut.add_done_callback(release)
    try:
        fut.cancel()
        await waiter
    finally:
        fut.remove_done_callback(release)


async def _py3112_wait_for(aw: Awaitable[Any], timeout: float | None) -> Any:
    """``asyncio.wait_for()`` control flow of v3.11.2 (no ``timeout()`` use)."""
    loop = asyncio.get_running_loop()
    if timeout is None:
        return await aw
    fut = asyncio.ensure_future(aw)
    if timeout <= 0:
        if fut.done():
            return fut.result()
        await _cancel_and_wait(fut)
        try:
            return fut.result()
        except asyncio.CancelledError as exc:
            raise TimeoutError from exc

    waiter = loop.create_future()

    def release(*_: object) -> None:
        if not waiter.done():
            waiter.set_result(None)

    handle = loop.call_later(timeout, release)
    fut.add_done_callback(release)
    try:
        try:
            await waiter
        except asyncio.CancelledError:
            if fut.done():
                return fut.result()
            fut.remove_done_callback(release)
            await _cancel_and_wait(fut)
            raise
        if fut.done():
            return fut.result()
        fut.remove_done_callback(release)
        await _cancel_and_wait(fut)
        try:
            return fut.result()
        except asyncio.CancelledError as exc:
            raise TimeoutError from exc
    finally:
        handle.cancel()


@pytest.fixture(params=["interpreter", "simulated-cpython-3.11.2"])
def affected(request: pytest.FixtureRequest) -> Iterator[bool]:
    """Yield whether ``asyncio.timeout()`` has the 3.11.0-3.11.2 defect here."""
    if request.param == "interpreter":
        yield AFFECTED_INTERPRETER
        return
    with (
        patch.object(asyncio, "timeout", _Py3112Timeout),
        patch.object(asyncio, "wait_for", _py3112_wait_for),
    ):
        yield True


# -- driver fixtures ----------------------------------------------------------


def _ok_frame() -> bytes:
    body = CAS_INFO + struct.pack(">i", 0)
    return struct.pack(">i", len(body) - 4) + body


def _open_db_frame() -> bytes:
    body = CAS_INFO + struct.pack(">i", 0) + b"\x00" * 8 + struct.pack(">i", 1234)
    return struct.pack(">i", len(body) - 4) + body


def _streams(chunks: list[bytes] | None = None) -> tuple[MagicMock, MagicMock]:
    reader = MagicMock(spec=asyncio.StreamReader)
    reader.readexactly = AsyncMock(side_effect=list(chunks or []))
    writer = MagicMock(spec=asyncio.StreamWriter)
    writer.drain = AsyncMock()
    writer.wait_closed = AsyncMock()
    writer.transport = MagicMock()
    writer.transport.get_extra_info.return_value = None
    return reader, writer


def _hang(started: asyncio.Event | None = None) -> Callable[..., Awaitable[Any]]:
    """A peer that never answers; only a deadline or a cancel ends the wait."""

    async def hang(*_args: object) -> Any:
        if started is not None:
            started.set()
        await asyncio.Event().wait()
        raise AssertionError("unreachable")

    return hang


class _Driver:
    """One driver deadline site wired to mock streams."""

    def __init__(
        self,
        site: str,
        timeout: float | None,
        reply: Callable[..., Awaitable[Any]] | None = None,
    ) -> None:
        self.site = site
        if site == "tcp-connect":
            self.conn = AsyncConnection(
                "localhost", 33000, "db", "dba", "", connect_timeout=timeout
            )
            self.reader, self.writer = _streams()
            self.open_connection = AsyncMock(
                side_effect=reply, return_value=(self.reader, self.writer)
            )
        else:
            self.conn = AsyncConnection("localhost", 33000, "db", "dba", "", read_timeout=timeout)
            chunks = [struct.pack(">i", 0), *_split(_open_db_frame())]
            if site == "round-trip":
                chunks = _split(_ok_frame())
                self.conn._connected = True
                self.conn._record_reply_cas_info(CAS_INFO)
            self.reader, self.writer = _streams(chunks)
            if reply is not None:
                self.reader.readexactly = AsyncMock(side_effect=reply)
            if site == "round-trip":
                self.conn._reader, self.conn._writer = self.reader, self.writer
            else:
                self.conn._open_connection = AsyncMock(  # type: ignore[method-assign]
                    return_value=(self.reader, self.writer)
                )

    async def run(self) -> Any:
        if self.site == "tcp-connect":
            with patch("pycubrid.aio.connection.asyncio.open_connection", new=self.open_connection):
                return await self.conn._open_connection("localhost", 33000)
        if self.site == "connect-handshake":
            return await self.conn._connect_locked()
        return await self.conn._send_and_receive(CommitPacket())

    def operation_started(self) -> bool:
        if self.site == "tcp-connect":
            return self.open_connection.await_count > 0
        return bool(self.writer.write.called)

    def assert_retired(self) -> None:
        if self.site == "tcp-connect":
            return  # nothing was opened, nothing to retire
        assert self.conn._connected is False
        assert self.conn._writer is None
        self.writer.close.assert_called()


def _split(frame: bytes) -> list[bytes]:
    return [frame[:4], frame[4:]]


TIMEOUT_MESSAGE = {
    "tcp-connect": "could not connect",
    "connect-handshake": "read timeout during connect handshake",
    "round-trip": "read timeout: no complete round trip",
}


async def _absorb_prior_cancellation() -> None:
    """Receive one cancellation and keep running, like cleanup code would."""
    task = asyncio.current_task()
    assert task is not None
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await asyncio.sleep(0)
    assert task.cancelling() == 1


async def _assert_no_stray_tasks() -> None:
    await asyncio.sleep(0)
    current = asyncio.current_task()
    stray = [t for t in asyncio.all_tasks() if t is not current and not t.done()]
    assert stray == []


# -- tests --------------------------------------------------------------------


async def test_simulation_reproduces_the_upstream_defect(affected: bool) -> None:
    """Guard: plain ``asyncio.timeout()`` really misclassifies under the
    affected semantics, so the driver tests below would catch a revert."""

    async def scenario() -> type[BaseException]:
        await _absorb_prior_cancellation()
        try:
            async with asyncio.timeout(DEADLINE):
                await asyncio.Event().wait()
        except BaseException as exc:  # noqa: BLE001 - classified below
            return type(exc)
        raise AssertionError("unreachable")

    raised = await asyncio.create_task(scenario())
    assert raised is (asyncio.CancelledError if affected else TimeoutError)


@pytest.mark.parametrize("site", SITES)
async def test_deadline_after_prior_cancellation_is_operational_error(
    affected: bool, site: str
) -> None:
    driver = _Driver(site, DEADLINE, reply=_hang())

    async def cleanup_after_cancel() -> int:
        await _absorb_prior_cancellation()
        with pytest.raises(OperationalError, match=TIMEOUT_MESSAGE[site]) as raised:
            await driver.run()
        assert isinstance(raised.value.__cause__, TimeoutError)
        task = asyncio.current_task()
        assert task is not None
        return task.cancelling()

    # The application's pending cancel request is left exactly as it was.
    assert await asyncio.create_task(cleanup_after_cancel()) == 1
    driver.assert_retired()
    await _assert_no_stray_tasks()


@pytest.mark.parametrize("prior_cancel", [False, True], ids=["fresh", "after-prior-cancel"])
@pytest.mark.parametrize("site", SITES)
async def test_new_caller_cancellation_still_propagates(
    affected: bool, site: str, prior_cancel: bool
) -> None:
    started = asyncio.Event()
    driver = _Driver(site, GENEROUS, reply=_hang(started))

    async def operation() -> None:
        if prior_cancel:
            await _absorb_prior_cancellation()
        await driver.run()

    task = asyncio.create_task(operation())
    await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    driver.assert_retired()
    await _assert_no_stray_tasks()


@pytest.mark.parametrize("timeout", [None, GENEROUS], ids=["none", "positive"])
@pytest.mark.parametrize("site", SITES)
async def test_none_and_positive_deadlines_complete(
    affected: bool, site: str, timeout: float | None
) -> None:
    driver = _Driver(site, timeout)

    result = await driver.run()

    if site == "tcp-connect":
        assert result == (driver.reader, driver.writer)
    else:
        assert driver.conn._connected is True
    await _assert_no_stray_tasks()


@pytest.mark.parametrize("site", SITES)
async def test_zero_deadline_cancels_the_operation_before_it_starts(
    affected: bool, site: str
) -> None:
    driver = _Driver(site, 0)

    with pytest.raises(OperationalError, match=TIMEOUT_MESSAGE[site]) as raised:
        await driver.run()

    assert isinstance(raised.value.__cause__, TimeoutError)
    assert driver.operation_started() is False
    await _assert_no_stray_tasks()


@pytest.mark.parametrize("timeout", [None, GENEROUS], ids=["none", "positive"])
async def test_transport_timeout_is_a_socket_failure(affected: bool, timeout: float | None) -> None:
    transport_error = TimeoutError("ETIMEDOUT")
    driver = _Driver("round-trip", timeout, reply=AsyncMock(side_effect=transport_error))

    with pytest.raises(OperationalError, match="socket communication timed out") as raised:
        await driver.run()

    assert raised.value.__cause__ is transport_error
    driver.assert_retired()


@pytest.mark.parametrize("timeout", [None, GENEROUS], ids=["none", "positive"])
async def test_complete_reply_callback_timeout_keeps_session(
    affected: bool, timeout: float | None
) -> None:
    callback_error = TimeoutError("callback")
    driver = _Driver("round-trip", timeout)

    with patch.object(CommitPacket, "parse", side_effect=callback_error):
        with pytest.raises(TimeoutError) as raised:
            await driver.run()

    assert raised.value is callback_error
    assert driver.conn._connected is True
    assert driver.conn._writer is driver.writer
