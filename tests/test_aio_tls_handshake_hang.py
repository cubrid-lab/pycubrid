"""Regression tests for #513: async TLS connect must not hang on a failed handshake.

``AsyncConnection._upgrade_to_tls`` hands the plaintext transport to
``loop.start_tls()``, which installs an ``SSLProtocol`` in front of the stream
protocol. When the TLS handshake fails while it is still in progress (the
peer stalls until ``read_timeout`` cancels it, or resets the connection
before the ServerHello), ``SSLProtocol`` does not forward ``connection_lost``
to the stream protocol, so ``StreamWriter.wait_closed()`` in the connect
cleanup never returned and ``aio.connect()`` hung forever on Python 3.11+.

Each case runs a local asyncio server that answers the plaintext CUBRS
handshake with status ``0`` and then misbehaves during the TLS handshake.
No CUBRID broker is needed. The sync driver never had the hang (a blocking
``wrap_socket`` honors the socket timeout); a parity case keeps it that way.
"""

from __future__ import annotations

import asyncio
import gc
import socket
import ssl
import struct
import threading
import time
import warnings

import pytest

import pycubrid
import pycubrid.aio
from pycubrid.exceptions import OperationalError

READ_TIMEOUT = 0.5
# connect() must fail close to READ_TIMEOUT; the slack covers slow CI hosts
# and the Python 3.10 preflight probe, which also waits up to READ_TIMEOUT.
MAX_ELAPSED = READ_TIMEOUT + 2.0
# Upper bound on the whole test, so a regression fails instead of hanging.
GUARD_TIMEOUT = 10.0


def _client_context() -> ssl.SSLContext:
    context = ssl.create_default_context()
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    return context


def _reset(writer: asyncio.StreamWriter) -> None:
    """Close the server side with an RST (SO_LINGER 0) instead of a FIN."""
    sock = writer.get_extra_info("socket")
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
    writer.transport.abort()


async def _serve(behavior: str, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    try:
        if behavior == "silent":
            # Accepts TCP but never answers the plaintext handshake.
            await reader.read()
            return
        await reader.readexactly(10)  # CUBRS client-info packet
        writer.write(struct.pack(">i", 0))
        await writer.drain()
        if behavior == "stall":
            # Accepts the TLS request but never speaks TLS: swallow the
            # ClientHello and anything after it until the client goes away.
            while await reader.read(4096):
                pass
        elif behavior == "reset_before_hello":
            _reset(writer)
        elif behavior == "reset_mid_handshake":
            await reader.read(1)  # first byte of the ClientHello record
            _reset(writer)
    except (ConnectionError, asyncio.IncompleteReadError):
        # The client went away first, which is what these cases expect.
        pass
    finally:
        writer.transport.abort()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "behavior", ["stall", "reset_before_hello", "reset_mid_handshake", "silent"]
)
async def test_aio_tls_connect_fails_within_read_timeout(behavior: str) -> None:
    server_tasks: list[asyncio.Task[None]] = []

    def on_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        server_tasks.append(asyncio.ensure_future(_serve(behavior, reader, writer)))

    server = await asyncio.start_server(on_client, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    tasks_before = asyncio.all_tasks()

    error: type[Exception] | None = None
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always", ResourceWarning)
        try:
            started = time.monotonic()
            try:
                await asyncio.wait_for(
                    pycubrid.aio.connect(
                        host="127.0.0.1",
                        port=port,
                        database="testdb",
                        user="dba",
                        ssl=_client_context(),
                        connect_timeout=READ_TIMEOUT,
                        read_timeout=READ_TIMEOUT,
                    ),
                    timeout=GUARD_TIMEOUT,
                )
            except Exception as exc:  # keep no traceback alive for the GC check
                error = type(exc)
            elapsed = time.monotonic() - started
            if behavior in ("stall", "silent"):
                # The server only returns once the client closed its socket.
                _, pending = await asyncio.wait(server_tasks, timeout=GUARD_TIMEOUT)
                assert not pending, "client socket was left open after connect() failed"
        finally:
            # Abort the server side first: Server.wait_closed() waits for
            # every accepted connection on Python 3.12+.
            for task in server_tasks:
                task.cancel()
            await asyncio.gather(*server_tasks, return_exceptions=True)
            server.close()
            await server.wait_closed()
        # Let aborted transports finish their call_soon close callbacks.
        for _ in range(5):
            await asyncio.sleep(0)
        gc.collect()

    assert error is OperationalError
    assert elapsed < MAX_ELAPSED, f"connect() took {elapsed:.2f}s (read_timeout={READ_TIMEOUT})"
    if behavior in ("stall", "silent"):
        # A stalled peer is only detected by the timeout itself.
        assert elapsed >= READ_TIMEOUT * 0.9
    assert asyncio.all_tasks() <= tasks_before
    # The Python 3.10 preflight probe closes its own socket on a reset too (#535).
    leaked = [w for w in caught if issubclass(w.category, ResourceWarning)]
    assert not leaked, [str(w.message) for w in leaked]


@pytest.mark.parametrize("behavior", ["stall", "reset_mid_handshake"])
def test_sync_tls_connect_fails_within_read_timeout(behavior: str) -> None:
    listener = socket.create_server(("127.0.0.1", 0))
    port = listener.getsockname()[1]
    done = threading.Event()

    def serve() -> None:
        conn, _ = listener.accept()
        with conn:
            conn.recv(10)
            conn.sendall(struct.pack(">i", 0))
            conn.recv(1)
            if behavior == "reset_mid_handshake":
                conn.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
            else:
                done.wait(GUARD_TIMEOUT)

    thread = threading.Thread(target=serve, daemon=True)
    thread.start()
    try:
        started = time.monotonic()
        with pytest.raises(OperationalError):
            pycubrid.connect(
                host="127.0.0.1",
                port=port,
                database="testdb",
                user="dba",
                ssl=_client_context(),
                connect_timeout=READ_TIMEOUT,
                read_timeout=READ_TIMEOUT,
            )
        elapsed = time.monotonic() - started
    finally:
        done.set()
        thread.join(GUARD_TIMEOUT)
        listener.close()

    assert elapsed < MAX_ELAPSED, f"connect() took {elapsed:.2f}s (read_timeout={READ_TIMEOUT})"
