"""Deterministic in-process fault-injection broker for pycubrid (issue #342).

A tiny threaded TCP server that speaks just enough of the CUBRID CAS handshake
to let a real :class:`pycubrid.connection.Connection` reach the ``OPEN_DATABASE``
exchange, then injects a chosen transport-level fault. Unlike monkeypatching a
socket, this drives the driver's *actual* socket code path (``recv_into``,
framing, length validation), so it catches hangs, unbounded allocation, and
raw-exception leaks that mock-based tests cannot.

The broker is intentionally minimal and offline: no CUBRID server is required.
Each fault is a small callable that receives the accepted client socket after
the handshake has been answered and decides what (if anything) to send.
"""

from __future__ import annotations

import socket
import struct
import threading
from collections.abc import Callable
from contextlib import contextmanager
from collections.abc import Iterator

from pycubrid.constants import DataSize

# A fault handler is given the post-handshake client socket. It may send bytes,
# close the socket, sleep, etc. It must not raise; the broker swallows errors.
FaultHandler = Callable[[socket.socket], None]

_HANDSHAKE_LEN = 10


def build_open_db_body(
    *,
    cas_info: bytes = b"\x01\x00\x00\x00",
    response_code: int = 0,
    protocol_version: int = 8,
    session_id: int = 1,
) -> bytes:
    """Build a well-formed ``OPEN_DATABASE`` response *body*.

    The body is what follows the 4-byte DATA_LENGTH prefix: it begins with
    CAS_INFO(4) and matches :meth:`OpenDatabasePacket.parse`'s expectations.
    """
    broker_info = bytearray(DataSize.BROKER_INFO)
    # broker_info[4] low 6 bits carry the protocol version (see parse()).
    broker_info[4] = protocol_version & 0x3F
    return (
        cas_info
        + struct.pack(">i", response_code)
        + bytes(broker_info)
        + struct.pack(">i", session_id)
    )


def framed(body: bytes) -> bytes:
    """Prefix *body* (which already contains CAS_INFO) with its DATA_LENGTH.

    ``_send_and_receive``/``connect`` read DATA_LENGTH first, then
    ``data_length + CAS_INFO`` more bytes. So the on-wire DATA_LENGTH must be
    ``len(body) - CAS_INFO``.
    """
    data_length = len(body) - DataSize.CAS_INFO
    return struct.pack(">i", data_length) + body


# ---------------------------------------------------------------------------
# Fault handlers
# ---------------------------------------------------------------------------


def fault_close_before_response(sock: socket.socket) -> None:
    sock.close()


def fault_close_after_length_only(sock: socket.socket) -> None:
    # Send a DATA_LENGTH claiming N bytes follow, then close without them.
    sock.sendall(struct.pack(">i", 64))
    sock.close()


def fault_truncated_body(sock: socket.socket) -> None:
    body = build_open_db_body()
    wire = framed(body)
    sock.sendall(wire[: len(wire) // 2])
    sock.close()


def fault_negative_data_length(sock: socket.socket) -> None:
    sock.sendall(struct.pack(">i", -1))
    sock.close()


def fault_oversized_data_length(sock: socket.socket) -> None:
    # Absurd length; the driver's _validate_data_length must reject it BEFORE
    # attempting to allocate/read that many bytes (no unbounded allocation).
    sock.sendall(struct.pack(">i", 2**31 - 1))
    sock.close()


def fault_extra_trailing_bytes(sock: socket.socket) -> None:
    body = build_open_db_body()
    sock.sendall(framed(body) + b"\xde\xad\xbe\xef" * 16)
    # Keep socket open briefly so the client reads its exact slice.


def fault_reset(sock: socket.socket) -> None:
    # Force an RST rather than an orderly FIN.
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
    sock.close()


def fault_garbage_body(sock: socket.socket) -> None:
    # Correct DATA_LENGTH framing but random payload of the promised size.
    payload = b"\x00\xff" * 64
    sock.sendall(struct.pack(">i", len(payload) - DataSize.CAS_INFO) + payload)


# ---------------------------------------------------------------------------
# Broker
# ---------------------------------------------------------------------------


class FaultBroker:
    """A one-shot threaded TCP broker that answers the handshake then faults."""

    def __init__(
        self,
        fault: FaultHandler,
        *,
        handshake_status: int = 0,
        answer_handshake: bool = True,
    ) -> None:
        self._fault = fault
        self._handshake_status = handshake_status
        self._answer_handshake = answer_handshake
        self._server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server.bind(("127.0.0.1", 0))
        self._server.listen(1)
        self._thread = threading.Thread(target=self._serve, daemon=True)

    @property
    def port(self) -> int:
        return self._server.getsockname()[1]

    def start(self) -> None:
        self._thread.start()

    def _serve(self) -> None:
        try:
            self._server.settimeout(5.0)
            client, _ = self._server.accept()
        except OSError:
            return
        try:
            with client:
                client.settimeout(5.0)
                self._drain_handshake(client)
                if self._answer_handshake:
                    client.sendall(struct.pack(">i", self._handshake_status))
                    self._drain_open_db(client)
                try:
                    self._fault(client)
                except OSError:
                    pass
        except OSError:
            pass

    def _drain_handshake(self, client: socket.socket) -> None:
        remaining = _HANDSHAKE_LEN
        while remaining > 0:
            chunk = client.recv(remaining)
            if not chunk:
                return
            remaining -= len(chunk)

    def _drain_open_db(self, client: socket.socket) -> None:
        # The client sends a 628-byte OPEN_DB request (no header). Read what is
        # available without blocking forever; we only need to unblock its send.
        try:
            client.recv(628)
        except OSError:
            pass

    def stop(self) -> None:
        try:
            self._server.close()
        except OSError:
            pass


@contextmanager
def run_fault_broker(
    fault: FaultHandler,
    *,
    handshake_status: int = 0,
    answer_handshake: bool = True,
) -> Iterator[int]:
    """Start a :class:`FaultBroker`, yield its port, and tear it down."""
    broker = FaultBroker(
        fault,
        handshake_status=handshake_status,
        answer_handshake=answer_handshake,
    )
    broker.start()
    try:
        yield broker.port
    finally:
        broker.stop()


ALL_FAULTS: dict[str, FaultHandler] = {
    "close_before_response": fault_close_before_response,
    "close_after_length_only": fault_close_after_length_only,
    "truncated_body": fault_truncated_body,
    "negative_data_length": fault_negative_data_length,
    "oversized_data_length": fault_oversized_data_length,
    "extra_trailing_bytes": fault_extra_trailing_bytes,
    "reset": fault_reset,
    "garbage_body": fault_garbage_body,
}
