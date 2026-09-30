"""One API over the sync and asyncio drivers for the TLS matrix (issue #350).

The offline (fake broker) and live (``SSL=ON`` broker) TLS suites run the very
same connect / query / ping / close sequence against ``pycubrid.connect`` and
``pycubrid.aio.connect`` and compare the outcomes, so sync/async divergence is
visible as a parametrized test failure instead of two hand-written copies
drifting apart.
"""

from __future__ import annotations

import asyncio
import ssl
from collections.abc import Coroutine
from typing import Any

import pytest

import pycubrid
import pycubrid.aio
from pycubrid.exceptions import Error as DBAPIError
from pycubrid.exceptions import OperationalError

MODES = ("sync", "aio")

# Upper bound on any single async operation. A driver hang turns into a test
# failure (TimeoutError, which is not a PEP 249 error) instead of wedging the
# whole suite.
HANG_GUARD = 10.0


class DriverClient:
    """Drive a sync ``Connection`` or an ``AsyncConnection`` through one API.

    The async flavour owns a private event loop, so a plain (non-async) test
    can run an identical scenario against both drivers.
    """

    def __init__(self, mode: str, **connect_kwargs: Any) -> None:
        assert mode in MODES, mode
        self.mode = mode
        self._kwargs = connect_kwargs
        self._loop = asyncio.new_event_loop() if mode == "aio" else None
        self.conn: Any = None

    def _run(self, coro: Coroutine[Any, Any, Any]) -> Any:
        assert self._loop is not None
        return self._loop.run_until_complete(asyncio.wait_for(coro, HANG_GUARD))

    def connect(self) -> None:
        if self.mode == "sync":
            self.conn = pycubrid.connect(**self._kwargs)
        else:
            self.conn = self._run(pycubrid.aio.connect(**self._kwargs))

    def scalar(self, sql: str) -> Any:
        """Execute *sql* and return the first column of the first row."""
        if self.mode == "sync":
            cur = self.conn.cursor()
            try:
                cur.execute(sql)
                row = cur.fetchone()
            finally:
                cur.close()
            return row[0] if row else None

        async def run() -> Any:
            cur = self.conn.cursor()
            try:
                await cur.execute(sql)
                row = await cur.fetchone()
            finally:
                await cur.close()
            return row[0] if row else None

        return self._run(run())

    def ping(self, reconnect: bool = True) -> bool:
        if self.mode == "sync":
            return bool(self.conn.ping(reconnect=reconnect))
        return bool(self._run(self.conn.ping(reconnect=reconnect)))

    def close(self) -> None:
        if self.conn is None:
            return
        if self.mode == "sync":
            self.conn.close()
        else:
            self._run(self.conn.close())

    def transport(self) -> Any:
        """The live transport object (socket or asyncio transport), or None."""
        if self.mode == "sync":
            return self.conn._socket
        writer = self.conn._writer
        return writer.transport if writer is not None else None

    def tls_version(self) -> str | None:
        """Negotiated TLS version of the live transport, or None if plaintext."""
        if self.mode == "sync":
            sock = self.conn._socket
            return sock.version() if isinstance(sock, ssl.SSLSocket) else None
        writer = self.conn._writer
        ssl_object = writer.get_extra_info("ssl_object") if writer is not None else None
        return ssl_object.version() if ssl_object is not None else None

    def transport_released(self) -> bool:
        if self.mode == "sync":
            return self.conn._socket is None
        return self.conn._reader is None and self.conn._writer is None

    def kill_transport(self) -> None:
        """Abruptly drop the live transport, as a network failure would."""
        if self.mode == "sync":
            self.conn._socket.shutdown(2)  # socket.SHUT_RDWR
        else:
            self.conn._writer.transport.abort()

    def shutdown(self) -> None:
        try:
            self.close()
        finally:
            if self._loop is not None:
                # Let call_soon'd transport close callbacks run and stop the
                # default executor (Python 3.10 verification preflight), so
                # sockets are closed here rather than by the GC.
                self._loop.run_until_complete(asyncio.sleep(0))
                self._loop.run_until_complete(self._loop.shutdown_default_executor())
                self._loop.close()


def connect_error(mode: str, **connect_kwargs: Any) -> OperationalError:
    """Connect, expecting failure; return the raised PEP 249 error.

    Also asserts the failure is an :class:`OperationalError` (never a raw
    ``ssl``/``OSError``/timeout leak) and that no connection object escaped.
    """
    client = DriverClient(mode, **connect_kwargs)
    try:
        with pytest.raises(DBAPIError) as excinfo:
            client.connect()
    finally:
        client.shutdown()
    assert client.conn is None, "a failed connect must not hand out a connection"
    assert isinstance(excinfo.value, OperationalError), repr(excinfo.value)
    return excinfo.value


def verify_code(exc: BaseException) -> int:
    """The OpenSSL X509_V_ERR_* code behind a certificate-verification failure."""
    cause = exc.__cause__
    assert isinstance(cause, ssl.SSLCertVerificationError), repr(cause)
    return int(cause.verify_code)


# OpenSSL X509_V_ERR_* codes surfaced on ssl.SSLCertVerificationError.verify_code.
X509_V_ERR_CERT_HAS_EXPIRED = 10
X509_V_ERR_DEPTH_ZERO_SELF_SIGNED_CERT = 18
X509_V_ERR_UNABLE_TO_GET_ISSUER_CERT_LOCALLY = 20
X509_V_ERR_HOSTNAME_MISMATCH = 62
X509_V_ERR_IP_ADDRESS_MISMATCH = 64
HOSTNAME_MISMATCH_CODES = frozenset({X509_V_ERR_HOSTNAME_MISMATCH, X509_V_ERR_IP_ADDRESS_MISMATCH})
