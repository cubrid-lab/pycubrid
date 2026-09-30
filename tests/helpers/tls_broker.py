"""In-process fake TLS CAS broker for the offline TLS negative matrix (issue #350).

A small threaded TCP server that speaks CUBRID's STARTTLS-style connect flow:

1. read the 10-byte ``ClientInfoExchange`` handshake (``CUBRS``/``CUBRK``);
2. answer with a 4-byte broker status;
3. then run one *behavior* — a real server-side TLS handshake with a chosen
   certificate/protocol range, or a transport fault injected at a precise
   point of the TLS upgrade.

Unlike mocking ``SSLContext.wrap_socket``/``loop.start_tls``, this drives the
driver's real sync and asyncio TLS code paths against a real OpenSSL peer, so
certificate verification, hostname checking, protocol-version negotiation,
and handshake interruption are all exercised end to end without a CUBRID
server.

Every accepted connection is recorded (:class:`ConnectionRecord`) so tests
can assert *security posture*, not just the exception type: which magic the
client sent, whether the first byte after the handshake was a TLS record, and
every byte the client sent in the clear after asking for TLS (to prove there
is no plaintext fallback and no credential leak).

Test PKI lives in ``tests/fixtures/tls`` (see ``generate.sh`` there).
"""

from __future__ import annotations

import socket
import ssl
import struct
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path

from .fault_broker import build_open_db_body, framed

FIXTURES = Path(__file__).resolve().parent.parent / "fixtures" / "tls"
CA_FILE = FIXTURES / "ca.pem"
SERVER_KEY = FIXTURES / "server.key"
SERVER_CERT = FIXTURES / "server.pem"
WRONG_HOST_CERT = FIXTURES / "wrong_host.pem"
EXPIRED_CERT = FIXTURES / "expired.pem"
SELF_SIGNED_CERT = FIXTURES / "self_signed.pem"

_HANDSHAKE_LEN = 10
_OPEN_DB_LEN = 628
# TLS record content type for a handshake message (ClientHello).
TLS_HANDSHAKE_RECORD = 0x16
# How long the broker waits on any single client read before giving up. Tests
# use much shorter client timeouts, so this only bounds broker-thread lifetime.
_BROKER_IO_TIMEOUT = 5.0

# Behaviors run after the broker status was sent.
TLS_OK = "tls_ok"  # full TLS handshake, OPEN_DB, then answer every request OK
CLOSE_BEFORE_TLS = "close_before_tls"  # close right after the status reply
CLOSE_MID_HANDSHAKE = "close_mid_handshake"  # read the ClientHello, then close
STALL_HANDSHAKE = "stall_handshake"  # read the ClientHello, never answer
PLAINTEXT_REPLY = "plaintext_reply"  # answer the ClientHello like a non-TLS CAS
MALFORMED_RECORD = "malformed_record"  # answer with a corrupt TLS handshake record
CLOSE_AFTER_TLS = "close_after_tls"  # finish TLS, read OPEN_DB, close unanswered


@dataclass
class ConnectionRecord:
    """What one accepted client connection did on the wire."""

    magic: bytes = b""
    first_post_handshake_byte: int | None = None
    plaintext_after_handshake: bytearray = field(default_factory=bytearray)
    tls_completed: bool = False
    tls_version: str | None = None


def server_context(
    cert: Path = SERVER_CERT,
    *,
    minimum_version: ssl.TLSVersion | None = None,
    maximum_version: ssl.TLSVersion | None = None,
) -> ssl.SSLContext:
    """Build a server-side context presenting *cert* (always ``server.key``)."""
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.load_cert_chain(certfile=str(cert), keyfile=str(SERVER_KEY))
    if minimum_version is not None:
        ctx.minimum_version = minimum_version
    if maximum_version is not None:
        ctx.maximum_version = maximum_version
    return ctx


def client_context(
    *,
    cafile: Path | None = CA_FILE,
    minimum_version: ssl.TLSVersion = ssl.TLSVersion.TLSv1_2,
    maximum_version: ssl.TLSVersion | None = None,
) -> ssl.SSLContext:
    """A verifying client context that trusts only *cafile* (no system CAs).

    TLS 1.2 is the floor, as for ``ssl=True``; pass *minimum_version* to raise it.
    """
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.minimum_version = minimum_version
    if cafile is not None:
        ctx.load_verify_locations(cafile=str(cafile))
    if maximum_version is not None:
        ctx.maximum_version = maximum_version
    return ctx


class TlsBroker:
    """Threaded fake CAS broker; serves every connection with ``behavior``.

    ``behavior``, ``handshake_status`` and ``tls_context`` are read per connection, so a test
    may switch them between phases (e.g. fail the first connect, then let a
    reconnect succeed). :meth:`drop_active` closes every live client socket to
    simulate the broker/CAS going away under an established TLS session.
    """

    def __init__(
        self,
        behavior: str = TLS_OK,
        *,
        tls_context: ssl.SSLContext | None = None,
        handshake_status: int = 0,
    ) -> None:
        self.behavior = behavior
        self.handshake_status = handshake_status
        self.tls_context = tls_context if tls_context is not None else server_context()
        self.records: list[ConnectionRecord] = []
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._active: list[socket.socket] = []
        self._threads: list[threading.Thread] = []
        self._server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server.bind(("127.0.0.1", 0))
        self._server.listen(8)
        self._server.settimeout(0.05)
        self._acceptor = threading.Thread(target=self._accept_loop, daemon=True)

    @property
    def port(self) -> int:
        return int(self._server.getsockname()[1])

    def start(self) -> None:
        self._acceptor.start()

    def stop(self) -> None:
        """Stop accepting, shut down every client socket, and join all threads."""
        self._stop.set()
        self._acceptor.join(timeout=_BROKER_IO_TIMEOUT)
        self.drop_active()
        for thread in list(self._threads):
            thread.join(timeout=_BROKER_IO_TIMEOUT)
        try:
            self._server.close()
        except OSError:
            pass  # listener already closed

    def drop_active(self) -> None:
        """Shut down every live client connection, as a dying broker would.

        Only ``shutdown()`` is called here: the owning handler thread wakes up
        on EOF and closes its own socket. Closing from this thread instead
        would free the descriptor while OpenSSL in the handler thread may still
        read from that fd number, which the next ``accept()`` can reuse.
        """
        with self._lock:
            active = list(self._active)
        for sock in active:
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass  # peer may already be gone

    # -- internals -------------------------------------------------------

    def _accept_loop(self) -> None:
        while not self._stop.is_set():
            try:
                client, _ = self._server.accept()
            except socket.timeout:
                continue
            except OSError:
                return
            client.settimeout(_BROKER_IO_TIMEOUT)
            record = ConnectionRecord()
            with self._lock:
                self.records.append(record)
                self._active.append(client)
            thread = threading.Thread(target=self._handle, args=(client, record), daemon=True)
            self._threads.append(thread)
            thread.start()

    def _handle(self, client: socket.socket, record: ConnectionRecord) -> None:
        conn: socket.socket = client
        try:
            record.magic = self._recv_exact(client, _HANDSHAKE_LEN)[:5]
            status = self.handshake_status
            behavior = self.behavior
            client.sendall(struct.pack(">i", status))
            if status != 0:
                # A rejecting broker keeps listening: any byte the client sends
                # now would be a plaintext retry on a refused TLS session.
                self._capture_plaintext(client, record)
                return
            if behavior == CLOSE_BEFORE_TLS:
                # Close right after the status reply, before reading (or even
                # peeking at) the ClientHello; waiting for it would turn this
                # into the mid-handshake case.
                return
            peek = client.recv(1, socket.MSG_PEEK)
            if peek:
                record.first_post_handshake_byte = peek[0]
            if behavior == TLS_OK or behavior == CLOSE_AFTER_TLS:
                conn = self._tls_accept(client, record)
                self._recv_exact(conn, _OPEN_DB_LEN)
                if behavior == CLOSE_AFTER_TLS:
                    return
                conn.sendall(framed(build_open_db_body()))
                self._serve_requests(conn)
            elif behavior == CLOSE_MID_HANDSHAKE:
                self._capture_once(client, record)
            elif behavior == STALL_HANDSHAKE:
                self._capture_once(client, record)
                self._stop.wait(_BROKER_IO_TIMEOUT)
            elif behavior == PLAINTEXT_REPLY:
                self._capture_once(client, record)
                client.sendall(framed(build_open_db_body()))
                self._capture_plaintext(client, record)
            elif behavior == MALFORMED_RECORD:
                self._capture_once(client, record)
                # A TLS handshake record header followed by a nonsense body.
                client.sendall(bytes([TLS_HANDSHAKE_RECORD, 0x03, 0x03, 0x00, 0x08]) + b"\xff" * 8)
                self._capture_plaintext(client, record)
            else:  # pragma: no cover - programming error in a test
                raise AssertionError(f"unknown behavior {behavior!r}")
        except (OSError, ValueError):
            pass  # the client aborting mid-exchange is the point of most cases
        finally:
            for sock in {id(client): client, id(conn): conn}.values():
                try:
                    sock.close()
                except OSError:
                    pass  # already closed
            with self._lock:
                self._active = [s for s in self._active if s is not client and s is not conn]

    def _tls_accept(self, client: socket.socket, record: ConnectionRecord) -> ssl.SSLSocket:
        tls = self.tls_context.wrap_socket(client, server_side=True)
        # wrap_socket detaches *client*; track the TLS socket for drop_active().
        with self._lock:
            if client in self._active:
                self._active.remove(client)
            self._active.append(tls)
        record.tls_completed = True
        record.tls_version = tls.version()
        return tls

    def _serve_requests(self, conn: socket.socket) -> None:
        """Answer every framed CAS request with a bare success response."""
        ok = framed(b"\x01\x00\x00\x00" + struct.pack(">i", 0))
        while not self._stop.is_set():
            header = self._recv_exact(conn, 4)
            (length,) = struct.unpack(">i", header)
            self._recv_exact(conn, length + 4)
            conn.sendall(ok)

    @staticmethod
    def _capture_once(client: socket.socket, record: ConnectionRecord) -> None:
        chunk = client.recv(65536)
        record.plaintext_after_handshake.extend(chunk)

    def _capture_plaintext(self, client: socket.socket, record: ConnectionRecord) -> None:
        client.settimeout(0.05)
        while not self._stop.is_set():
            try:
                chunk = client.recv(65536)
            except socket.timeout:
                continue
            if not chunk:
                return
            record.plaintext_after_handshake.extend(chunk)

    @staticmethod
    def _recv_exact(sock: socket.socket, size: int) -> bytes:
        buf = bytearray()
        while len(buf) < size:
            chunk = sock.recv(size - len(buf))
            if not chunk:
                raise ValueError("peer closed")
            buf.extend(chunk)
        return bytes(buf)


@contextmanager
def run_tls_broker(
    behavior: str = TLS_OK,
    *,
    tls_context: ssl.SSLContext | None = None,
    handshake_status: int = 0,
) -> Iterator[TlsBroker]:
    """Start a :class:`TlsBroker`, yield it, and fully tear it down."""
    broker = TlsBroker(behavior, tls_context=tls_context, handshake_status=handshake_status)
    broker.start()
    try:
        yield broker
    finally:
        broker.stop()
