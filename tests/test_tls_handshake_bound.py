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
import traceback
import warnings
from types import SimpleNamespace
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
    listener.settimeout(5.0)
    client_owned = threading.Event()
    reset_done = threading.Event()

    def serve() -> None:
        try:
            conn, _ = listener.accept()
            with conn:
                # Darwin can surface an immediate reset inside TCP connect,
                # before the client wrapper has recorded an owned socket.
                if not client_owned.wait(5.0):
                    return  # the client's bounded reset_done assertion fails
                conn.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
        except OSError:
            return  # listener teardown, or a reset_done assertion on failure
        reset_done.set()

    thread = threading.Thread(target=serve, daemon=True)
    thread.start()
    opened: list[socket.socket] = []
    real_create_connection = socket.create_connection

    def create_connection(*args: Any, **kwargs: Any) -> socket.socket:
        sock = real_create_connection(*args, **kwargs)
        opened.append(sock)
        client_owned.set()
        assert reset_done.wait(5.0), "peer did not reset the recorded probe socket"
        time.sleep(0.05)  # let the RST arrive
        return sock

    monkeypatch.setattr("pycubrid.aio.connection.socket.create_connection", create_connection)
    try:
        yield listener.getsockname()[1], opened
    finally:
        client_owned.set()  # release the peer if connection setup failed
        listener.close()
        thread.join(5.0)
        for sock in opened:
            sock.close()  # after the test's strict fd/leak assertions
        assert not thread.is_alive(), "reset peer did not finish its bounded cleanup"


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


@pytest.mark.parametrize("path", ["probe", "sync-default", "sync-explicit"])
def test_probe_handshake_timeout_is_a_total_deadline(
    monkeypatch: pytest.MonkeyPatch, path: str
) -> None:
    """A peer that trickles bytes must not reset a probe or sync TLS timeout.

    ``wrap_socket()`` bounds the whole handshake by the socket timeout; the
    memory-BIO handshake must keep that total deadline, not turn it into a
    per-``recv()`` inactivity timeout.
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
        with pytest.raises(TimeoutError if path == "probe" else OperationalError) as caught:
            if path == "probe":
                AsyncConnection._probe_tls_verification_sync(
                    HOST, port, client_context(), True, 2.0, SHORT_DEFAULT
                )
            else:
                _connect(port, read_timeout=SHORT_DEFAULT if path == "sync-explicit" else None)
        elapsed = time.monotonic() - started
        if path != "probe":
            assert isinstance(caught.value.__cause__, TimeoutError)
    finally:
        stop.set()
        listener.close()
        thread.join(5.0)
        assert not thread.is_alive(), "trickle peer did not finish bounded cleanup"
        assert not peer_errors, peer_errors

    assert accepted.is_set()
    assert SHORT_DEFAULT * 0.9 <= elapsed < SHORT_DEFAULT + 1.0, elapsed


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


def _timed_probe(
    monkeypatch: pytest.MonkeyPatch,
    *,
    hello_delay: float,
    receive_delay: float,
    completion_delay: float = 0.0,
    shutdown_delay: float = 0.0,
    fail_finished: bool = False,
    fatal_error: ssl.SSLError | None = None,
    receive_error: TimeoutError | None = None,
    fail_alert: bool = False,
) -> Any:
    """Exercise the actual probe with independent per-I/O socket deadlines."""
    clock = [0.0]

    class ProbeSocket:
        closed = False
        timeout = 0.0
        sent: list[bytes] = []

        def settimeout(self, timeout: float) -> None:
            self.timeout = timeout

        def wait(self, duration: float) -> None:
            if duration > self.timeout:
                clock[0] += self.timeout
                raise TimeoutError("socket operation timed out")
            clock[0] += duration

        def sendall(self, data: bytes) -> None:
            self.sent.append(data)
            if data == b"fatal-alert" and fail_alert:
                raise ConnectionResetError("peer reset during fatal alert")
            if b"client-finished" in data and fail_finished:
                raise ConnectionResetError("peer reset during final flight")
            self.wait(
                shutdown_delay
                if b"close-notify" in data
                else hello_delay
                if data == b"hello"
                else 0.0
            )

        def recv(self, _size: int) -> bytes:
            self.wait(receive_delay)
            if receive_error is not None:
                raise receive_error
            return b"server-flight"

        def close(self) -> None:
            self.closed = True

    class ProbeTLS:
        started = False

        def __init__(self, outgoing: Any) -> None:
            self.outgoing = outgoing

        def do_handshake(self) -> None:
            if not self.started:
                self.started = True
                self.outgoing.write(b"hello")
                raise ssl.SSLWantReadError()
            clock[0] += completion_delay
            if fatal_error is not None:
                self.outgoing.write(b"fatal-alert")
                raise fatal_error
            self.outgoing.write(b"client-finished")

        def unwrap(self) -> None:
            self.outgoing.write(b"close-notify")
            raise ssl.SSLWantReadError()

    class ProbeContext:
        def wrap_bio(self, _incoming: Any, outgoing: Any, **_kwargs: Any) -> ProbeTLS:
            return ProbeTLS(outgoing)

    sock = ProbeSocket()
    monkeypatch.setattr("pycubrid.aio.connection.time", SimpleNamespace(monotonic=lambda: clock[0]))
    monkeypatch.setattr("pycubrid.aio.connection.socket.create_connection", lambda *a, **k: sock)
    return SimpleNamespace(socket=sock, context=ProbeContext(), now=lambda: clock[0])


def test_probe_slow_send_does_not_extend_receive_deadline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    probe = _timed_probe(monkeypatch, hello_delay=0.08, receive_delay=0.05)
    with pytest.raises(TimeoutError):
        AsyncConnection._probe_tls_verification_sync(HOST, 1, probe.context, False, 1.0, 0.1)
    assert probe.socket.closed
    assert probe.now() == pytest.approx(0.1)


def test_probe_rejects_completion_after_total_deadline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    probe = _timed_probe(monkeypatch, hello_delay=0.0, receive_delay=0.08, completion_delay=0.04)
    with pytest.raises(TimeoutError):
        AsyncConnection._probe_tls_verification_sync(HOST, 1, probe.context, False, 1.0, 0.1)
    assert probe.socket.closed


def test_probe_shutdown_does_not_extend_total_deadline(monkeypatch: pytest.MonkeyPatch) -> None:
    probe = _timed_probe(monkeypatch, hello_delay=0.02, receive_delay=0.06, shutdown_delay=0.05)
    AsyncConnection._probe_tls_verification_sync(HOST, 1, probe.context, False, 1.0, 0.1)
    assert probe.socket.closed
    assert probe.now() <= 0.100001


def test_probe_reports_final_handshake_send_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    probe = _timed_probe(monkeypatch, hello_delay=0.0, receive_delay=0.0, fail_finished=True)
    with pytest.raises(ConnectionResetError):
        AsyncConnection._probe_tls_verification_sync(HOST, 1, probe.context, False, 1.0, 0.1)
    assert probe.socket.closed


@pytest.mark.parametrize(
    "completion_delay, fail_alert, attempts_alert",
    [(0.0, False, True), (0.12, False, False), (0.0, True, True)],
    ids=["sent", "deadline-expired", "send-failed"],
)
def test_probe_fatal_alert_preserves_original_error_and_socket_cleanup(
    monkeypatch: pytest.MonkeyPatch,
    completion_delay: float,
    fail_alert: bool,
    attempts_alert: bool,
) -> None:
    original = ssl.SSLError(1, "fatal TLS failure")
    probe = _timed_probe(
        monkeypatch,
        hello_delay=0.0,
        receive_delay=0.0,
        completion_delay=completion_delay,
        fatal_error=original,
        fail_alert=fail_alert,
    )
    with pytest.raises(ssl.SSLError) as caught:
        AsyncConnection._probe_tls_verification_sync(HOST, 1, probe.context, False, 1.0, 0.1)
    assert caught.value is original
    assert caught.value.args == (1, "fatal TLS failure")
    assert probe.socket.closed
    assert (b"fatal-alert" in probe.socket.sent) is attempts_alert
    assert probe.now() == pytest.approx(completion_delay)


def test_probe_receive_timeout_suppresses_want_read_displayed_context(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = TimeoutError(110, "custom receive timeout")
    probe = _timed_probe(monkeypatch, hello_delay=0.0, receive_delay=0.0, receive_error=original)
    with pytest.raises(TimeoutError) as caught:
        AsyncConnection._probe_tls_verification_sync(HOST, 1, probe.context, False, 1.0, 0.1)
    assert caught.value is original
    assert caught.value.args == (110, "custom receive timeout")
    assert caught.value.errno == 110
    assert caught.value.__suppress_context__ is True
    assert isinstance(caught.value.__context__, ssl.SSLWantReadError)
    displayed = "".join(
        traceback.format_exception(type(original), original, original.__traceback__)
    )
    assert "SSLWantReadError" not in displayed
    assert probe.socket.closed
