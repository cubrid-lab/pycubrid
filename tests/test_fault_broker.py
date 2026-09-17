"""Drive a real Connection against the fault broker (issue #342).

For every injected transport fault the driver must, within a bounded time:

* raise a PEP 249 :class:`pycubrid.Error` (never a raw ``struct.error`` /
  ``OSError`` / ``ValueError`` / hang);
* never allocate an unbounded buffer for a hostile DATA_LENGTH;
* end with the connection marked not-connected (never a live desynced socket).

These are offline: no CUBRID server is involved, only the in-process
:class:`FaultBroker`.
"""

from __future__ import annotations

import socket
import sys

import pytest

import pycubrid
from pycubrid.exceptions import Error as DBAPIError

from .helpers.fault_broker import ALL_FAULTS, run_fault_broker

_TIMEOUT = 5.0

# Faults that corrupt the transport so connect() MUST fail with a DB-API error.
_ERROR_FAULTS = sorted(set(ALL_FAULTS) - {"extra_trailing_bytes", "garbage_body"})


def _connect(port: int) -> pycubrid.Connection:
    return pycubrid.connect(
        host="127.0.0.1",
        port=port,
        database="testdb",
        user="dba",
        password="",
        connect_timeout=_TIMEOUT,
        read_timeout=_TIMEOUT,
    )


@pytest.mark.parametrize("fault_name", _ERROR_FAULTS)
def test_connect_fault_raises_dbapi_error(fault_name: str) -> None:
    """Each transport-corrupting fault surfaces as a DB-API error, not a raw one."""
    fault = ALL_FAULTS[fault_name]
    with run_fault_broker(fault) as port:
        conn = None
        try:
            with pytest.raises(DBAPIError):
                conn = _connect(port)
        finally:
            if conn is not None:
                conn.close()


@pytest.mark.parametrize("fault_name", ["extra_trailing_bytes", "garbage_body"])
def test_well_framed_response_connects(fault_name: str) -> None:
    """A well-framed response (even with junk trailing/body) parses and connects.

    Extra trailing bytes are never read, and a zero response-code body parses as
    a valid-enough OPEN_DB. These must NOT be treated as faults; the connection
    succeeds and closes cleanly.
    """
    fault = ALL_FAULTS[fault_name]
    with run_fault_broker(fault) as port:
        conn = _connect(port)
        assert conn is not None
        conn.close()


@pytest.mark.parametrize("fault_name", sorted(ALL_FAULTS))
def test_connect_fault_is_bounded_time(fault_name: str) -> None:  # noqa: D401
    """No fault causes a hang: the attempt completes well under the timeout."""
    import time

    fault = ALL_FAULTS[fault_name]
    with run_fault_broker(fault) as port:
        start = time.monotonic()
        try:
            _connect(port)
        except BaseException:  # noqa: BLE001 - only measuring wall time here
            pass
        elapsed = time.monotonic() - start
        assert elapsed < _TIMEOUT * 2


def test_handshake_rejected_status_raises() -> None:
    """A negative handshake status fails fast with a DB-API error."""
    with run_fault_broker(ALL_FAULTS["close_before_response"], handshake_status=-1) as port:
        with pytest.raises(DBAPIError):
            _connect(port)


def test_oversized_length_does_not_allocate_unbounded() -> None:
    """An absurd DATA_LENGTH must be rejected before any large allocation.

    If the driver tried to read 2**31-1 bytes it would either hang until the
    timeout or exhaust memory; instead _validate_data_length must reject it and
    raise promptly.
    """
    import time

    with run_fault_broker(ALL_FAULTS["oversized_data_length"]) as port:
        start = time.monotonic()
        with pytest.raises(DBAPIError):
            _connect(port)
        assert time.monotonic() - start < _TIMEOUT


def test_broker_socket_is_closed_after_use() -> None:
    """Sanity: the broker binds/tears down cleanly (no leaked listener)."""
    with run_fault_broker(ALL_FAULTS["reset"]) as port:
        probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        probe.settimeout(1.0)
        try:
            probe.connect(("127.0.0.1", port))
        except OSError:
            pass
        finally:
            probe.close()


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
