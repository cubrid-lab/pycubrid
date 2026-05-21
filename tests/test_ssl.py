from __future__ import annotations

import ssl as ssl_module
from typing import cast
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from pycubrid._connection_common import resolve_ssl_context
from pycubrid.connection import Connection
from pycubrid.protocol import ClientInfoExchangePacket


def test_resolve_ssl_context_true_creates_default_context() -> None:
    context = ssl_module.create_default_context()

    with patch(
        "pycubrid.connection.ssl_module.create_default_context", return_value=context
    ) as create_ctx:
        result = resolve_ssl_context(True)

    assert result is context
    assert result is not None
    assert result.minimum_version >= ssl_module.TLSVersion.TLSv1_2
    create_ctx.assert_called_once_with()


def test_resolve_ssl_context_false_and_none_disable_tls() -> None:
    assert resolve_ssl_context(False) is None
    assert resolve_ssl_context(None) is None


def test_resolve_ssl_context_custom_context_is_reused() -> None:
    context = ssl_module.create_default_context()
    context.minimum_version = ssl_module.TLSVersion.TLSv1

    assert resolve_ssl_context(context) is context
    assert context.minimum_version == ssl_module.TLSVersion.TLSv1


def test_resolve_ssl_context_invalid_value_raises_value_error() -> None:
    bad_value = cast(bool | ssl_module.SSLContext | None, cast(object, "invalid"))

    with pytest.raises(ValueError, match="ssl must be bool, ssl.SSLContext, or None"):
        _ = resolve_ssl_context(bad_value)


def test_connection_create_socket_never_wraps_ssl() -> None:
    conn = Connection.__new__(Connection)
    conn._connect_timeout = None
    conn._read_timeout = None
    conn._ssl_context = MagicMock()

    raw_sock = MagicMock()

    with patch("pycubrid.connection.socket.create_connection", return_value=raw_sock):
        result = Connection._create_socket(conn, "db.example.com", 33000)

    assert result is raw_sock
    conn._ssl_context.wrap_socket.assert_not_called()


def test_connection_wrap_ssl_wraps_with_server_hostname() -> None:
    conn = Connection.__new__(Connection)
    conn._ssl_context = MagicMock()
    raw_sock = MagicMock()
    wrapped_sock = MagicMock()
    conn._ssl_context.wrap_socket.return_value = wrapped_sock

    result = Connection._wrap_ssl(conn, raw_sock, "db.example.com")

    assert result is wrapped_sock
    conn._ssl_context.wrap_socket.assert_called_once_with(
        raw_sock, server_hostname="db.example.com"
    )


def test_connection_wrap_ssl_noop_when_disabled() -> None:
    conn = Connection.__new__(Connection)
    conn._ssl_context = None
    raw_sock = MagicMock()

    result = Connection._wrap_ssl(conn, raw_sock, "db.example.com")

    assert result is raw_sock


def test_connection_does_not_wrap_socket_when_ssl_disabled() -> None:
    conn = Connection.__new__(Connection)
    conn._connect_timeout = None
    conn._read_timeout = None
    conn._ssl_context = None

    raw_sock = MagicMock()

    with patch("pycubrid.connection.socket.create_connection", return_value=raw_sock):
        result = Connection._create_socket(conn, "db.example.com", 33000)

    assert result is raw_sock


def test_async_connection_resolves_ssl_true_to_context() -> None:
    from pycubrid.aio.connection import AsyncConnection

    conn = AsyncConnection("localhost", 33000, "testdb", "dba", "", ssl=True)
    assert isinstance(conn._ssl_context, ssl_module.SSLContext)


def test_async_connection_accepts_ssl_context_parameter() -> None:
    from pycubrid.aio.connection import AsyncConnection

    ctx = ssl_module.create_default_context()
    conn = AsyncConnection("localhost", 33000, "testdb", "dba", "", ssl=ctx)
    assert conn._ssl_context is ctx


def test_async_connection_accepts_ssl_false() -> None:
    from pycubrid.aio.connection import AsyncConnection

    conn = AsyncConnection("localhost", 33000, "testdb", "dba", "", ssl=False)
    assert conn._ssl_context is None


def test_async_connection_accepts_ssl_none() -> None:
    from pycubrid.aio.connection import AsyncConnection

    conn = AsyncConnection("localhost", 33000, "testdb", "dba", "", ssl=None)
    assert conn._ssl_context is None


@pytest.mark.asyncio
async def test_async_connection_close_streams_clears_reader_writer() -> None:
    from pycubrid.aio.connection import AsyncConnection

    conn = AsyncConnection("localhost", 33000, "testdb", "dba", "")
    mock_writer = MagicMock()
    mock_writer.close = MagicMock()
    mock_writer.wait_closed = AsyncMock()
    mock_reader = MagicMock()
    conn._writer = mock_writer
    conn._reader = mock_reader

    await conn._close_streams()

    assert conn._writer is None
    assert conn._reader is None
    mock_writer.close.assert_called_once()
    mock_writer.wait_closed.assert_awaited_once()


def test_async_connection_safe_close_socket_delegates_to_close_streams_sync() -> None:
    from pycubrid.aio.connection import AsyncConnection

    conn = AsyncConnection("localhost", 33000, "testdb", "dba", "")
    mock_writer = MagicMock()
    conn._writer = mock_writer
    conn._reader = MagicMock()

    conn._safe_close_socket()

    assert conn._writer is None
    assert conn._reader is None


def test_client_info_packet_plain_magic_is_cubrk() -> None:
    pkt = ClientInfoExchangePacket()
    assert pkt.write()[:5] == b"CUBRK"


def test_client_info_packet_ssl_magic_is_cubrs() -> None:
    pkt = ClientInfoExchangePacket(use_ssl=True)
    assert pkt.write()[:5] == b"CUBRS"


def test_client_info_packet_explicit_false_is_cubrk() -> None:
    pkt = ClientInfoExchangePacket(use_ssl=False)
    assert pkt.write()[:5] == b"CUBRK"


def test_connect_rejects_negative_handshake_status() -> None:
    import struct

    from pycubrid.exceptions import OperationalError
    from tests.test_connection import make_socket

    bad_sock = make_socket([struct.pack(">i", -22)])

    with (
        patch("pycubrid.connection.socket.create_connection", return_value=bad_sock),
        pytest.raises(OperationalError, match="broker rejected handshake with status -22"),
    ):
        Connection("localhost", 33000, "testdb", "dba", "", ssl=False)


def test_connect_redirect_sends_no_second_handshake() -> None:
    """Regression guard: per CUBRID JDBC the redirected CAS worker does not
    re-handshake.  Re-introducing a second handshake here would break
    plaintext redirected deployments."""
    import struct

    from tests.test_connection import build_open_db_response, make_socket

    first_sock = make_socket([struct.pack(">i", 33100)])
    open_db = build_open_db_response()
    second_sock = make_socket([open_db[:4], open_db[4:]])

    with patch(
        "pycubrid.connection.socket.create_connection",
        side_effect=[first_sock, second_sock],
    ):
        Connection("localhost", 33000, "testdb", "dba", "", ssl=False)

    sent_bytes = b"".join(call.args[0] for call in second_sock.sendall.call_args_list)
    assert b"CUBRK" not in sent_bytes
    assert b"CUBRS" not in sent_bytes
