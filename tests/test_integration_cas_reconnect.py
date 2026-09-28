"""Live regressions for CAS recycling at transaction boundaries (#485).

The CAS may close the client socket right after answering END_TRAN (memory
restart, ``cubrid broker reset``, CHANGE CLIENT). The driver must verify an
OUT_TRAN session with CHECK_CAS and reconnect only when the probe fails, while
a live session keeps its state across commit/rollback (#468). Query handles
left open by unclosed cursors are released at END_TRAN so they do not
accumulate in the CAS.

Broker-control scenarios run ``broker_changer``/``cubrid broker reset`` inside
the server container named by ``CUBRID_TEST_DOCKER_CONTAINER``; CI passes its
service container ID, and ``make integration`` its compose container.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import re
import select
import shlex
import socket
import subprocess  # nosec B404 - fixed docker CLI argv, no shell on the host
import time
import uuid
from collections.abc import Iterator

import pytest

import pycubrid
import pycubrid.aio
from pycubrid.aio.connection import AsyncConnection
from pycubrid.connection import Connection

from ._parity_helpers import ADAPTERS, ParityAdapter, connect_kwargs

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

CONTAINER = os.environ.get("CUBRID_TEST_DOCKER_CONTAINER", "")
BROKER = os.environ.get("CUBRID_TEST_BROKER", "broker1")
needs_broker_control = pytest.mark.skipif(
    not CONTAINER,
    reason="broker control requires CUBRID_TEST_DOCKER_CONTAINER (the server container)",
)
PARAMS = pytest.mark.parametrize("adapter", ADAPTERS, ids=[adapter.kind for adapter in ADAPTERS])


def _broker_cli(command: list[str]) -> str:
    result = subprocess.run(  # nosec B603 B607 - fixed argv; container from the test env
        ["docker", "exec", "-u", "cubrid", CONTAINER, "bash", "-lc", shlex.join(command)],
        check=True,
        capture_output=True,
        text=True,
        timeout=120,
    )
    return result.stdout


def _broker_param(name: str) -> str:
    """Return a live broker1 parameter from ``cubrid broker info``."""
    section = ""
    for line in _broker_cli(["cubrid", "broker", "info"]).splitlines():
        header = re.match(r"\[%(\S+)\]", line.strip())
        if header:
            section = header.group(1).lower()
        elif section == BROKER.lower() and line.strip().startswith(name):
            return line.split("=", 1)[1].strip()
    raise AssertionError(f"{name} not reported for {BROKER}")


@contextlib.contextmanager
def _small_appl_server_max_size() -> Iterator[None]:
    """Make every END_TRAN restart the CAS (``restart_is_needed``), then restore."""
    original = _broker_param("APPL_SERVER_MAX_SIZE")
    _broker_cli(["broker_changer", BROKER, "APPL_SERVER_MAX_SIZE", "1"])
    try:
        yield
    finally:
        _broker_cli(["broker_changer", BROKER, "APPL_SERVER_MAX_SIZE", original])


async def _connect(adapter: ParityAdapter, **kwargs: object) -> Connection | AsyncConnection:
    params = dict(connect_kwargs(), **kwargs)
    if adapter.kind == "sync":
        return pycubrid.connect(**params)  # type: ignore[arg-type]
    return await pycubrid.aio.connect(**params)  # type: ignore[arg-type]


async def _query(
    adapter: ParityAdapter, conn: Connection | AsyncConnection, sql: str, *, close: bool = True
) -> tuple[object, ...] | None:
    cur = adapter.cursor(conn)
    try:
        await adapter.execute(cur, sql)
        return await adapter.fetchone(cur) if cur.description else None
    finally:
        if close:
            await adapter.close_cursor(cur)


async def _wait_until_cas_closed(conn: Connection | AsyncConnection) -> None:
    """Wait until the CAS has closed this client socket (FIN received)."""
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        if isinstance(conn, AsyncConnection):
            assert conn._reader is not None
            await asyncio.sleep(0.1)
            if conn._reader.at_eof():
                return
        else:
            sock = conn._socket
            assert sock is not None
            readable, _, _ = select.select([sock], [], [], 0.1)
            if readable and sock.recv(1, socket.MSG_PEEK) == b"":
                return
    raise AssertionError("CAS did not close the idle client socket")


@needs_broker_control
@PARAMS
async def test_cas_restart_after_end_tran_reconnects(adapter: ParityAdapter) -> None:
    # An explicit escape mode keeps the replacement session's setup free of its
    # own END_TRAN, which this deliberately tiny memory limit would also recycle.
    conn = await _connect(adapter, no_backslash_escapes=True)
    try:
        await adapter.set_autocommit(conn, False)
        start = conn._physical_generation
        with _small_appl_server_max_size():
            for boundary in range(4):
                assert await _query(adapter, conn, "SELECT 1 FROM db_root") == (1,)
                # The CAS answers END_TRAN, then restarts and closes this socket.
                await (adapter.commit if boundary % 2 == 0 else adapter.rollback)(conn)
            assert await _query(adapter, conn, "SELECT 1 FROM db_root") == (1,)
        # At most one reconnect per recycled boundary. Not every boundary is
        # guaranteed to recycle a fresh CAS (observed on 10.2), so assert bounds.
        assert start < conn._physical_generation <= start + 4
        assert adapter.autocommit_state(conn) is False
    finally:
        await adapter.close_connection(conn)


@needs_broker_control
@PARAMS
async def test_broker_reset_reconnects_and_restores_autocommit(adapter: ParityAdapter) -> None:
    table = f"r485_reset_{uuid.uuid4().hex[:8]}"
    observer = pycubrid.connect(**connect_kwargs())
    observer.autocommit = True
    conn = await _connect(adapter)
    try:
        await _query(adapter, conn, f"CREATE TABLE {table} (id INT)")
        await adapter.set_autocommit(conn, True)
        generation = conn._physical_generation
        restores: list[int] = []
        if isinstance(conn, AsyncConnection):
            restore_locked = conn._restore_session_state_locked

            async def spy_locked() -> None:
                restores.append(conn._physical_generation)
                await restore_locked()

            conn._restore_session_state_locked = spy_locked  # type: ignore[method-assign]
        else:
            restore = conn._restore_session_state

            def spy() -> None:
                restores.append(conn._physical_generation)
                restore()

            conn._restore_session_state = spy  # type: ignore[method-assign]

        _broker_cli(["cubrid", "broker", "reset", BROKER])
        await _wait_until_cas_closed(conn)

        # The replacement session re-emits the explicit autocommit setting, and
        # the observer sees the row without any commit from this connection.
        await _query(adapter, conn, f"INSERT INTO {table} VALUES (1)")
        assert conn._physical_generation == generation + 1
        assert restores == [generation + 1]
        assert adapter.autocommit_state(conn) is True
        cur = observer.cursor()
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        assert cur.fetchone() == (1,)
        cur.close()
    finally:
        await adapter.close_connection(conn)
        drop = observer.cursor()
        drop.execute(f"DROP TABLE IF EXISTS {table}")
        drop.close()
        observer.close()


@needs_broker_control
@PARAMS
async def test_change_client_with_more_clients_than_cas(adapter: ParityAdapter) -> None:
    """KEEP_CONNECTION=AUTO hands an idle OUT_TRAN CAS to a waiting client."""
    max_cas = int(_broker_param("MAX_NUM_APPL_SERVER"))
    conns: list[Connection | AsyncConnection] = []
    try:
        for _ in range(max_cas + 1):
            conn = await _connect(adapter)
            await adapter.set_autocommit(conn, False)
            assert await _query(adapter, conn, "SELECT 1 FROM db_root") == (1,)
            await adapter.commit(conn)
            conns.append(conn)
        generations = [conn._physical_generation for conn in conns]

        # More clients than CAS processes: at least one of these sessions was
        # handed to another client, and every client must still work.
        for _ in range(2):
            for conn in conns:
                assert await _query(adapter, conn, "SELECT 2 FROM db_root") == (2,)
                await adapter.commit(conn)
        reconnects = sum(
            conn._physical_generation - generation
            for conn, generation in zip(conns, generations, strict=True)
        )
        assert reconnects >= 1
    finally:
        for conn in conns:
            await adapter.close_connection(conn)


@PARAMS
async def test_unclosed_cursor_handles_do_not_accumulate(adapter: ParityAdapter) -> None:
    """Unclosed cursors across 2000 commits: bounded handle ids, same session."""
    conn = await _connect(adapter)
    try:
        await adapter.set_autocommit(conn, False)
        generation = conn._physical_generation
        highest = 0
        for _ in range(2000):
            cur = adapter.cursor(conn)
            await adapter.execute(cur, "SELECT 1 FROM db_root")
            assert cur._query_handle is not None
            highest = max(highest, cur._query_handle)
            await adapter.commit(conn)
            assert cur._query_handle is None  # released at END_TRAN, not leaked
        # Server handle ids are reused once released; a leak grows them per commit.
        assert highest < 50
        assert conn._physical_generation == generation
    finally:
        await adapter.close_connection(conn)


@PARAMS
@pytest.mark.parametrize("boundary", ["commit", "rollback"])
async def test_session_state_survives_boundary_with_open_cursor(
    adapter: ParityAdapter, boundary: str
) -> None:
    conn = await _connect(adapter)
    marker = f"@session_485_{uuid.uuid4().hex[:10]}"
    try:
        await adapter.set_autocommit(conn, False)
        await _query(adapter, conn, "SET TRANSACTION ISOLATION LEVEL 6")
        await _query(adapter, conn, f"SET {marker} = 42")
        marker_value = await _query(adapter, conn, f"SELECT {marker}")
        assert marker_value is not None
        open_cursor = adapter.cursor(conn)
        await adapter.execute(open_cursor, "SELECT 1 FROM db_root")
        generation = conn._physical_generation

        await getattr(adapter, boundary)(conn)

        assert open_cursor._query_handle is None
        assert await _query(adapter, conn, f"SELECT {marker}") == marker_value
        await _query(adapter, conn, "GET TRANSACTION ISOLATION LEVEL TO X")
        row = await _query(adapter, conn, "SELECT X")
        assert row is not None and str(row[0]) == "6"
        assert conn._physical_generation == generation
    finally:
        await adapter.close_connection(conn)
