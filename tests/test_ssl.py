from __future__ import annotations

import ssl as ssl_module
from typing import cast
from unittest.mock import MagicMock, patch

import pytest

from pycubrid.connection import Connection, resolve_ssl_context
from pycubrid.exceptions import NotSupportedError


def test_resolve_ssl_context_true_creates_default_context() -> None:
    context = ssl_module.create_default_context()

    with patch(
        "pycubrid.connection.ssl_module.create_default_context", return_value=context
    ) as create_ctx:
        result = resolve_ssl_context(True)

    assert result is context
    create_ctx.assert_called_once_with()


def test_resolve_ssl_context_false_and_none_disable_tls() -> None:
    assert resolve_ssl_context(False) is None
    assert resolve_ssl_context(None) is None


def test_resolve_ssl_context_custom_context_is_reused() -> None:
    context = ssl_module.create_default_context()

    assert resolve_ssl_context(context) is context


def test_resolve_ssl_context_invalid_value_raises_value_error() -> None:
    bad_value = cast(bool | ssl_module.SSLContext | None, cast(object, "invalid"))

    with pytest.raises(ValueError, match="ssl must be bool, ssl.SSLContext, or None"):
        _ = resolve_ssl_context(bad_value)


def test_connection_wraps_socket_when_ssl_enabled() -> None:
    conn = Connection.__new__(Connection)
    conn._connect_timeout = None
    conn._read_timeout = None
    conn._ssl_context = MagicMock()

    raw_sock = MagicMock()
    wrapped_sock = MagicMock()
    conn._ssl_context.wrap_socket.return_value = wrapped_sock

    with patch("pycubrid.connection.socket.create_connection", return_value=raw_sock):
        result = Connection._create_socket(conn, "db.example.com", 33000)

    assert result is wrapped_sock
    conn._ssl_context.wrap_socket.assert_called_once_with(
        raw_sock, server_hostname="db.example.com"
    )


def test_connection_does_not_wrap_socket_when_ssl_disabled() -> None:
    conn = Connection.__new__(Connection)
    conn._connect_timeout = None
    conn._read_timeout = None
    conn._ssl_context = None

    raw_sock = MagicMock()

    with patch("pycubrid.connection.socket.create_connection", return_value=raw_sock):
        result = Connection._create_socket(conn, "db.example.com", 33000)

    assert result is raw_sock


def test_async_connection_rejects_ssl_parameter() -> None:
    from pycubrid.aio.connection import AsyncConnection

    with pytest.raises(NotSupportedError, match="SSL/TLS is not yet supported for async"):
        AsyncConnection("localhost", 33000, "testdb", "dba", "", ssl=True)


def test_async_connection_rejects_ssl_context_parameter() -> None:
    from pycubrid.aio.connection import AsyncConnection

    ctx = ssl_module.create_default_context()
    with pytest.raises(NotSupportedError, match="SSL/TLS is not yet supported for async"):
        AsyncConnection("localhost", 33000, "testdb", "dba", "", ssl=ctx)


def test_async_connection_accepts_ssl_false() -> None:
    from pycubrid.aio.connection import AsyncConnection

    conn = AsyncConnection("localhost", 33000, "testdb", "dba", "", ssl=False)
    assert conn._ssl_context is None


def test_async_connection_accepts_ssl_none() -> None:
    from pycubrid.aio.connection import AsyncConnection

    conn = AsyncConnection("localhost", 33000, "testdb", "dba", "", ssl=None)
    assert conn._ssl_context is None
