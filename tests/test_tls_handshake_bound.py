"""TLS handshake bound and socket ownership on the sync paths (#535).

* The sync driver bounds the TLS handshake with ``read_timeout``. Without a
  ``read_timeout`` a peer that stalls during the handshake used to block
  ``connect()`` forever; it now gives up after the same 10-second default the
  async driver passes as ``ssl_handshake_timeout``. Once the session is up the
  socket goes back to blocking, so the default never bounds later requests.
* The Python 3.10 preflight probe of the async driver used a blocking
  ``wrap_socket()``. On 3.10, ``SSLSocket._create()`` takes over the fd and can
  raise on a peer reset just before the ClientHello without closing it, so the
  socket was left for the garbage collector (``ResourceWarning``). The probe
  now runs the handshake over memory BIOs on a socket it owns and closes.

Everything runs against the in-process TLS broker; no CUBRID server is needed.
"""

from __future__ import annotations

import gc
import socket
import ssl
import struct
import threading
import time
import warnings
from typing import Any

import pytest

import pycubrid
import pycubrid.connection as sync_connection
from pycubrid.aio.connection import AsyncConnection
from pycubrid.exceptions import OperationalError

from .helpers.tls_broker import (
    STALL_HANDSHAKE,
    TLS_OK,
    WRONG_HOST_CERT,
    client_context,
    run_tls_broker,
    server_context,
)

HOST = "127.0.0.1"
# Stand-in for the 10-second default so the stalled case runs quickly.
SHORT_DEFAULT = 0.5


def _connect(port: int, *, read_timeout: float | None) -> Any:
    return pycubrid.connect(
        host=HOST,
        port=port,
        database="testdb",
        user="dba",
        password="",
        ssl=client_context(),
        connect_timeout=2.0,
        read_timeout=read_timeout,
        # The fake broker answers every request with a bare OK.
        no_backslash_escapes=True,
    )


def test_default_tls_handshake_timeout_matches_async() -> None:
    assert sync_connection._DEFAULT_TLS_HANDSHAKE_TIMEOUT == 10.0


def test_sync_stalled_tls_handshake_without_read_timeout_is_bounded(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        sync_connection, "_DEFAULT_TLS_HANDSHAKE_TIMEOUT", SHORT_DEFAULT, raising=False
    )
    with run_tls_broker(STALL_HANDSHAKE) as broker:
        started = time.monotonic()
        with pytest.raises(OperationalError) as excinfo:
            _connect(broker.port, read_timeout=None)
        elapsed = time.monotonic() - started

    assert isinstance(excinfo.value.__cause__, TimeoutError), repr(excinfo.value.__cause__)
    assert SHORT_DEFAULT * 0.9 <= elapsed < SHORT_DEFAULT + 2.0, elapsed


@pytest.mark.parametrize("read_timeout", [None, 3.0])
def test_sync_tls_session_keeps_read_timeout_after_handshake(
    monkeypatch: pytest.MonkeyPatch, read_timeout: float | None
) -> None:
    """The handshake default must not leak into requests after the handshake."""
    monkeypatch.setattr(
        sync_connection, "_DEFAULT_TLS_HANDSHAKE_TIMEOUT", SHORT_DEFAULT, raising=False
    )
    with run_tls_broker(TLS_OK) as broker:
        with _connect(broker.port, read_timeout=read_timeout) as conn:
            assert conn._socket.version() is not None
            assert conn._socket.gettimeout() == read_timeout


@pytest.fixture
def reset_before_client_hello(monkeypatch: pytest.MonkeyPatch) -> Any:
    """A peer that resets the TCP connection before the client's ClientHello.

    The probe's socket is handed back only after the reset reached it, so the
    reset deterministically lands between the TCP connect and the TLS start.
    """
    listener = socket.create_server((HOST, 0))
    reset_done = threading.Event()

    def serve() -> None:
        conn, _ = listener.accept()
        conn.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
        conn.close()
        reset_done.set()

    thread = threading.Thread(target=serve, daemon=True)
    thread.start()
    opened: list[socket.socket] = []
    real_create_connection = socket.create_connection

    def create_connection(*args: Any, **kwargs: Any) -> socket.socket:
        sock = real_create_connection(*args, **kwargs)
        opened.append(sock)
        reset_done.wait(5.0)
        time.sleep(0.05)  # let the RST arrive
        return sock

    monkeypatch.setattr("pycubrid.aio.connection.socket.create_connection", create_connection)
    try:
        yield listener.getsockname()[1], opened
    finally:
        thread.join(5.0)
        listener.close()


def test_probe_closes_socket_on_reset_before_client_hello(
    reset_before_client_hello: Any,
) -> None:
    port, opened = reset_before_client_hello
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always", ResourceWarning)
        raised: type[BaseException] | None = None
        try:
            AsyncConnection._probe_tls_verification_sync(
                HOST, port, client_context(), False, 2.0, 2.0
            )
        except OSError as exc:  # keep no traceback alive for the GC check
            raised = type(exc)
        gc.collect()

    assert raised is not None and issubclass(raised, OSError)
    assert [sock.fileno() for sock in opened] == [-1]
    leaked = [str(w.message) for w in caught if issubclass(w.category, ResourceWarning)]
    assert not leaked, leaked


def test_probe_handshake_timeout_is_a_total_deadline() -> None:
    """A peer that trickles handshake bytes must not reset the probe's timeout.

    ``wrap_socket()`` bounds the whole handshake by the socket timeout; the
    memory-BIO handshake must keep that total deadline, not turn it into a
    per-``recv()`` inactivity timeout.
    """
    listener = socket.create_server((HOST, 0))
    stop = threading.Event()

    def serve() -> None:
        conn, _ = listener.accept()
        with conn:
            conn.recv(10)  # CUBRS
            conn.sendall(struct.pack(">i", 0))
            # A TLS handshake record header announcing 16 KiB, then one byte of
            # it every 0.1 s: never a complete record, never a quiet period.
            conn.sendall(bytes([0x16, 0x03, 0x03, 0x40, 0x00]))
            give_up = time.monotonic() + 3.0  # a regression fails instead of hanging
            while not stop.wait(0.1) and time.monotonic() < give_up:
                try:
                    conn.sendall(b"\x00")
                except OSError:
                    return

    thread = threading.Thread(target=serve, daemon=True)
    thread.start()
    try:
        started = time.monotonic()
        with pytest.raises(TimeoutError):
            AsyncConnection._probe_tls_verification_sync(
                HOST, listener.getsockname()[1], client_context(), True, 2.0, SHORT_DEFAULT
            )
        elapsed = time.monotonic() - started
    finally:
        stop.set()
        thread.join(5.0)
        listener.close()

    assert elapsed < SHORT_DEFAULT + 1.0, elapsed


def test_probe_completes_real_tls_handshake() -> None:
    with run_tls_broker(TLS_OK) as broker:
        AsyncConnection._probe_tls_verification_sync(
            HOST, broker.port, client_context(), True, 2.0, 2.0
        )
        deadline = time.monotonic() + 2.0
        while not broker.records or not broker.records[-1].tls_completed:
            assert time.monotonic() < deadline, "broker never completed the TLS handshake"
            time.sleep(0.01)


def test_probe_reports_certificate_verification_failure() -> None:
    with run_tls_broker(TLS_OK, tls_context=server_context(WRONG_HOST_CERT)) as broker:
        with pytest.raises(ssl.SSLCertVerificationError):
            AsyncConnection._probe_tls_verification_sync(
                HOST, broker.port, client_context(), True, 2.0, 2.0
            )
