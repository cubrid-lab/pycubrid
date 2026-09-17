"""Broker/transport chaos scenarios (issue #354).

Tests what happens when the connection to CUBRID dies while the driver is
actively working. Rather than kill a shared broker process, this drops the
live socket out from under the driver mid-session (an abrupt transport loss
indistinguishable, from the driver's side, from a broker restart or a killed
CAS), and drives the CAS-inactive transparent-reconnect path. It asserts the
durable contract:

* the current request fails clearly with a PEP 249 error (never a raw
  exception, never a hang);
* no corrupted / partial result is returned as if it succeeded;
* after the failure the connection state is deterministic (a fresh op either
  works via transparent reconnect or raises a DB-API error);
* an active transaction is never silently replayed after a reconnect.

Marked ``integration`` and skip-gated. Runs against the normal broker; the full
process-kill / server-restart matrix belongs in the nightly chaos job.
"""

from __future__ import annotations

import asyncio
import socket
import sys
import uuid
from contextlib import closing

import pytest

import pycubrid
import pycubrid.aio
from pycubrid.exceptions import Error as DBAPIError

from ._parity_helpers import TEST_DB, TEST_HOST, TEST_PASSWORD, TEST_PORT, TEST_USER, can_connect

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(not can_connect(), reason="CUBRID instance not available"),
]


def _connect() -> pycubrid.Connection:
    return pycubrid.connect(
        host=TEST_HOST,
        port=TEST_PORT,
        database=TEST_DB,
        user=TEST_USER,
        password=TEST_PASSWORD,
    )


async def _aconnect() -> pycubrid.aio.AsyncConnection:
    return await pycubrid.aio.connect(
        host=TEST_HOST,
        port=TEST_PORT,
        database=TEST_DB,
        user=TEST_USER,
        password=TEST_PASSWORD,
    )


def _tbl() -> str:
    return "chaos_%s" % uuid.uuid4().hex[:8]


def _drop_socket(conn: pycubrid.Connection) -> None:
    """Abruptly kill the live socket, simulating a broker/CAS disappearance."""
    sock = conn._socket
    if sock is None:
        return
    try:
        sock.shutdown(socket.SHUT_RDWR)
    except OSError:
        pass  # socket may already be half-closed; the close() below still runs
    sock.close()
    conn._socket = None


class TestSyncTransportChaos:
    def test_request_after_transport_drop_fails_clearly(self) -> None:
        with closing(_connect()) as conn:
            cur = conn.cursor()
            cur.execute("SELECT 1")
            assert cur.fetchone() == (1,)
            # Broker "disappears" mid-session.
            _drop_socket(conn)
            # The next request must fail with a DB-API error, not a raw one/hang.
            with pytest.raises(DBAPIError):
                cur.execute("SELECT 1")
            cur.close()

    def test_buffered_result_intact_after_drop(self) -> None:
        # pycubrid materializes a result set it can fetch in the initial exchange
        # into a client-side buffer on execute(). Dropping the socket afterwards
        # must not corrupt the already-buffered rows: they read back correctly,
        # and only an operation that needs the (now-dead) server raises.
        table = _tbl()
        with closing(_connect()) as setup:
            setup.autocommit = True
            cur = setup.cursor()
            cur.execute("CREATE TABLE %s (id INT PRIMARY KEY)" % table)
            cur.executemany("INSERT INTO %s VALUES (?)" % table, [(i,) for i in range(50)])
            cur.close()
            try:
                with closing(_connect()) as conn:
                    c = conn.cursor()
                    c.execute("SELECT id FROM %s ORDER BY id" % table)
                    _drop_socket(conn)
                    # Already-buffered rows read back intact (no corruption/garble).
                    rows = c.fetchall()
                    assert [r[0] for r in rows] == list(range(50))
                    # A new statement needs the dead server and must raise cleanly.
                    with pytest.raises(DBAPIError):
                        c.execute("SELECT 1")
                    c.close()
            finally:
                cur = setup.cursor()
                cur.execute("DROP TABLE IF EXISTS %s" % table)
                cur.close()

    def test_connection_state_deterministic_after_drop(self) -> None:
        with closing(_connect()) as conn:
            cur = conn.cursor()
            cur.execute("SELECT 1")
            cur.fetchall()
            _drop_socket(conn)
            with pytest.raises(DBAPIError):
                cur.execute("SELECT 1")
            # A fresh connection is unaffected and works normally.
            with closing(_connect()) as fresh:
                fcur = fresh.cursor()
                fcur.execute("SELECT 1")
                assert fcur.fetchone() == (1,)
                fcur.close()

    def test_transaction_not_silently_replayed_after_drop(self) -> None:
        table = _tbl()
        with closing(_connect()) as setup:
            setup.autocommit = True
            scur = setup.cursor()
            scur.execute("CREATE TABLE %s (id INT PRIMARY KEY)" % table)
            scur.close()
            try:
                with closing(_connect()) as conn:
                    conn.autocommit = False
                    cur = conn.cursor()
                    cur.execute("INSERT INTO %s VALUES (1)" % table)  # uncommitted
                    _drop_socket(conn)  # broker vanishes with the txn open
                    # The uncommitted INSERT must NOT be silently replayed/committed
                    # on any transparent reconnect.
                    with pytest.raises(DBAPIError):
                        cur.execute("SELECT 1")
                    cur.close()
                # Verify from an independent connection: the row never landed.
                with closing(_connect()) as check:
                    check.autocommit = True
                    ccur = check.cursor()
                    ccur.execute("SELECT COUNT(*) FROM %s" % table)
                    row = ccur.fetchone()
                    assert row is not None and row[0] == 0
                    ccur.close()
            finally:
                scur = setup.cursor()
                scur.execute("DROP TABLE IF EXISTS %s" % table)
                scur.close()


class TestAsyncTransportChaos:
    def test_async_request_after_drop_fails_clearly(self) -> None:
        async def run() -> None:
            conn = await _aconnect()
            try:
                cur = conn.cursor()
                await cur.execute("SELECT 1")
                assert await cur.fetchone() == (1,)
                # Drop the async transport's underlying socket.
                writer = conn._writer
                if writer is not None:
                    writer.transport.abort()
                with pytest.raises(DBAPIError):
                    await asyncio.wait_for(cur.execute("SELECT 1"), timeout=10.0)
                await cur.close()
            finally:
                await conn.close()

        asyncio.run(run())


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
