"""TLS handshake bound on the sync paths (#535).

The sync driver bounds the TLS handshake with ``read_timeout``. Without a
``read_timeout`` a peer that stalls during the handshake used to block
``connect()`` forever; it now gives up after the same 10-second default the
async driver passes as ``ssl_handshake_timeout``. Once the session is up the
socket goes back to blocking, so the default never bounds later requests.

Everything runs against the in-process TLS broker; no CUBRID server is needed.
"""

from __future__ import annotations

import socket
import struct
import threading
import time
from typing import Any

import pytest

import pycubrid
import pycubrid.connection as sync_connection
from pycubrid.exceptions import OperationalError

from .helpers.tls_broker import (
    STALL_HANDSHAKE,
    TLS_OK,
    client_context,
    run_tls_broker,
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


@pytest.mark.parametrize("path", ["sync-default", "sync-explicit"])
def test_sync_handshake_timeout_is_a_total_deadline(
    monkeypatch: pytest.MonkeyPatch, path: str
) -> None:
    """A peer that trickles bytes must not reset the sync TLS timeout.

    ``wrap_socket()`` bounds the whole handshake by the socket timeout, not
    each ``recv()``.
    """
    monkeypatch.setattr(sync_connection, "_DEFAULT_TLS_HANDSHAKE_TIMEOUT", SHORT_DEFAULT)
    listener = socket.create_server((HOST, 0))
    listener.settimeout(2.0)
    port = listener.getsockname()[1]
    stop = threading.Event()
    accepted = threading.Event()
    peer_errors: list[Exception] = []

    def serve() -> None:
        try:
            conn, _ = listener.accept()
            accepted.set()
            with conn:
                conn.settimeout(2.0)
                conn.recv(10)  # CUBRS
                conn.sendall(struct.pack(">i", 0))
                # An incomplete 16 KiB record with a byte every 0.1 s.
                conn.sendall(bytes([0x16, 0x03, 0x03, 0x40, 0x00]))
                give_up = time.monotonic() + 3.0  # fail instead of hanging
                while not stop.wait(0.1) and time.monotonic() < give_up:
                    try:
                        conn.sendall(b"\x00")
                    except (BrokenPipeError, ConnectionResetError):
                        return  # expected when the client times out and closes
        except Exception as exc:
            peer_errors.append(exc)

    thread = threading.Thread(target=serve, daemon=True)
    thread.start()
    try:
        started = time.monotonic()
        with pytest.raises(OperationalError) as caught:
            _connect(port, read_timeout=SHORT_DEFAULT if path == "sync-explicit" else None)
        elapsed = time.monotonic() - started
        assert isinstance(caught.value.__cause__, TimeoutError)
    finally:
        stop.set()
        listener.close()
        thread.join(5.0)
        assert not thread.is_alive(), "trickle peer did not finish bounded cleanup"
        assert not peer_errors, peer_errors

    assert accepted.is_set()
    assert SHORT_DEFAULT * 0.9 <= elapsed < SHORT_DEFAULT + 1.0, elapsed
