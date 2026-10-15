"""A ``Lob`` is never sent on a physical session that replaced its own (#932).

LOB handles are session scoped. After ``ping(reconnect=True)`` or a failed
OUT_TRAN CHECK_CAS probe replaces the physical session, an older ``Lob``
raises before any LOB byte reaches the new session and never reconnects; a
``Lob`` created or wrapped on the new session works there.
"""

from __future__ import annotations

import struct
from unittest.mock import MagicMock, patch

import pytest

from pycubrid.connection import Connection
from pycubrid.constants import CASFunctionCode, CUBRIDDataType
from pycubrid.exceptions import InterfaceError, OperationalError
from pycubrid.lob import Lob

from .test_cas_reconnect_probe import (
    IN_TRAN,
    _frames,
    _function_codes,
    _replacement_socket,
    _script,
    _sync_out_tran,
)
from .test_network_edge_cases import make_connected_connection

HANDLE = struct.pack(">iqi", 33, 0, 4) + b"a/b\x00"
LOB_CODES = {CASFunctionCode.LOB_NEW, CASFunctionCode.LOB_WRITE, CASFunctionCode.LOB_READ}


def _reply(code: int, payload: bytes = b"", cas_info: bytes = IN_TRAN) -> bytes:
    body = cas_info + struct.pack(">i", code) + payload
    return struct.pack(">i", len(body) - 4) + body


def _stale_after_ping() -> tuple[Connection, Lob, MagicMock]:
    conn, old = make_connected_connection()
    lob = Lob(conn, CUBRIDDataType.BLOB, HANDLE)
    _script(old, [])  # CHECK_CAS gets no answer: the CAS is gone
    new = _replacement_socket()
    with patch("socket.create_connection", return_value=new):
        recovered = conn.ping(reconnect=True)
    assert recovered is True
    assert conn._physical_generation == lob._generation + 1
    return conn, lob, new


@pytest.mark.parametrize("operation", ["read", "write"])
def test_lob_from_before_ping_reconnect_sends_nothing_to_the_new_session(
    operation: str,
) -> None:
    conn, lob, new = _stale_after_ping()
    start = new.sendall.call_count

    with patch("socket.create_connection") as create:
        with pytest.raises(InterfaceError, match="earlier physical session"):
            if operation == "read":
                lob.read(4)
            else:
                lob.write(b"data")

    create.assert_not_called()  # the guard never opens a connection
    assert new.sendall.call_count == start
    assert conn._connected is True
    assert conn._socket is new


def test_lob_wrapped_on_the_new_session_reads_and_writes_there() -> None:
    conn, _stale, new = _stale_after_ping()
    _script(new, _frames(_reply(4), _reply(4, b"data")))
    start = new.sendall.call_count

    lob = Lob(conn, CUBRIDDataType.BLOB, HANDLE)
    assert lob._generation == conn._physical_generation
    written = lob.write(b"data")
    data = lob.read(4)
    assert (written, data) == (4, b"data")
    assert _function_codes(new, start) == [CASFunctionCode.LOB_WRITE, CASFunctionCode.LOB_READ]


def test_failed_probe_before_a_lob_request_fails_it_and_later_ones() -> None:
    conn, old = _sync_out_tran()
    lob = Lob(conn, CUBRIDDataType.BLOB, HANDLE)
    _script(old, [])
    new = _replacement_socket()

    with patch("socket.create_connection", return_value=new) as create:
        with pytest.raises(OperationalError, match="LOB handle lost due to broker reconnect"):
            lob.read(4)
        assert create.call_count == 1
        with pytest.raises(InterfaceError, match="earlier physical session"):
            lob.write(b"data")
        assert create.call_count == 1

    assert not LOB_CODES & set(_function_codes(new))


def test_lob_created_after_a_failed_probe_belongs_to_the_new_session() -> None:
    conn, old = _sync_out_tran()
    _script(old, [])
    generation = conn._physical_generation
    # LOB_NEW, then LOB_WRITE on the new session.
    new = _replacement_socket(_reply(0, HANDLE), _reply(4))

    with patch("socket.create_connection", return_value=new):
        lob = conn.create_lob(CUBRIDDataType.BLOB)
    assert lob._generation == generation + 1 == conn._physical_generation

    with patch("socket.create_connection") as create:
        written = lob.write(b"data")
    assert written == 4
    create.assert_not_called()
    assert [code for code in _function_codes(new) if code in LOB_CODES] == [
        CASFunctionCode.LOB_NEW,
        CASFunctionCode.LOB_WRITE,
    ]
