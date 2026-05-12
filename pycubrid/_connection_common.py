"""Shared mixin for sync and async connection implementations.

This module centralises common state initialisation, pure-logic helpers,
and constants so that ``connection.py`` and ``aio/connection.py`` import
from one place instead of duplicating code.

All methods are either pure (no I/O) or operate solely on instance state.
I/O-dependent methods (connect, send_and_receive, etc.) remain in the
concrete sync/async classes.
"""

from __future__ import annotations

import json
import logging
import os
import socket
import ssl as ssl_module
from typing import TYPE_CHECKING, Any

from .exceptions import InterfaceError

if TYPE_CHECKING:
    from .timing import TimingStats

_LOGGER = logging.getLogger(__name__)


def resolve_ssl_context(
    ssl_param: bool | ssl_module.SSLContext | None,
) -> ssl_module.SSLContext | None:
    """Resolve an ``ssl`` parameter into an SSLContext or None."""
    if ssl_param is None:
        return None
    if isinstance(ssl_param, bool):
        if ssl_param:
            return ssl_module.create_default_context()
        return None
    if isinstance(ssl_param, ssl_module.SSLContext):
        return ssl_param
    raise ValueError(f"ssl must be bool, ssl.SSLContext, or None, got {type(ssl_param)}")


# Alias kept for backwards compatibility with existing imports.
_resolve_ssl_context = resolve_ssl_context


class ConnectionCommonMixin:
    """Mixin providing shared state and pure helpers for Connection classes."""

    # -- CAS_INFO status constants (matches JDBC UConnection) ----------------
    _CAS_INFO_STATUS_INACTIVE: int = 0
    _CAS_INFO_STATUS_ACTIVE: int = 1

    def _init_common_state(
        self,
        *,
        host: str,
        port: int,
        database: str,
        user: str,
        password: str,
        fetch_size: int = 100,
        connect_timeout: float | None = None,
        read_timeout: float | None = None,
        decode_collections: bool = False,
        json_deserializer: Any = None,
        no_backslash_escapes: bool = False,
        enable_timing: bool | None = None,
    ) -> None:
        """Initialise attributes common to sync and async connections."""
        self._host = host
        self._port = port
        self._database = database
        self._user = user
        self._password = password
        self._connect_timeout = connect_timeout
        self._read_timeout = read_timeout
        self._decode_collections = decode_collections
        self._json_deserializer = json_deserializer
        self._no_backslash_escapes = no_backslash_escapes

        if type(fetch_size) is not int or fetch_size < 1:
            raise ValueError("fetch_size must be an integer >= 1")
        self._fetch_size = fetch_size

        if self._json_deserializer is not None and not callable(self._json_deserializer):
            raise TypeError("json_deserializer must be callable or None")
        if self._json_deserializer is json.loads:
            self._json_deserializer = json.loads

        # Timing support
        self._timing: TimingStats | None = None
        if enable_timing is None:
            enable_timing = os.environ.get("PYCUBRID_ENABLE_TIMING", "").lower() in (
                "1",
                "true",
                "yes",
            )
        if enable_timing:
            from .timing import TimingStats as _TimingStats

            self._timing = _TimingStats()

        # Connection state
        self._socket: socket.socket | None = None
        self._connected = False
        self._cas_info: bytes | bytearray = b"\x00\x00\x00\x00"
        self._session_id = 0
        self._autocommit = False
        self._cursors: set[Any] = set()
        self._protocol_version: int = 1

    # -- Pure helpers (no I/O) -----------------------------------------------

    def _invalidate_query_handles(self) -> None:
        """Invalidate all cursor query handles.

        After commit/rollback the CUBRID broker may reset the CAS
        connection, making previous query handles stale.
        """
        for cursor in self._cursors:
            cursor._query_handle = None

    def _ensure_connected(self) -> None:
        """Raise ``InterfaceError`` when called on a closed connection."""
        if not self._connected:
            raise InterfaceError("connection is closed")

    def _check_closed(self) -> None:
        """Alias for ``_ensure_connected`` used by DB-API call sites."""
        self._ensure_connected()

    def _safe_close_socket(self) -> None:
        """Close the socket safely, ignoring any OS errors."""
        if self._socket is not None:
            try:
                self._socket.close()
            except OSError:
                pass
            finally:
                self._socket = None

    def _drop_connection(self) -> None:
        """Close the socket, mark disconnected, and invalidate cursors."""
        self._safe_close_socket()
        self._connected = False
        self._invalidate_query_handles()

    @property
    def timing_stats(self) -> TimingStats | None:
        """Return the timing statistics object, or ``None`` if timing is disabled."""
        return self._timing
