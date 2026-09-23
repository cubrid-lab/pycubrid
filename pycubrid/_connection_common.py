"""Shared mixin for sync and async connection implementations.

This module centralises common state initialisation, pure-logic helpers,
and constants so that ``connection.py`` and ``aio/connection.py`` import
from one place instead of duplicating code.

All methods are either pure (no I/O) or operate solely on instance state.
I/O-dependent methods (connect, send_and_receive, etc.) remain in the
concrete sync/async classes.
"""

from __future__ import annotations


import difflib
import logging
import os
import socket
import ssl as ssl_module
import sys
import warnings
from typing import TYPE_CHECKING, Any

from .constants import DataSize
from .exceptions import (
    DatabaseError,
    DataError,
    Error,
    IntegrityError,
    InterfaceError,
    InternalError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
    UnknownConnectionOptionWarning,
    Warning,
)

if TYPE_CHECKING:
    from .timing import TimingStats

_LOGGER = logging.getLogger(__name__)


# Every public connection option accepted by ``pycubrid.connect``,
# ``pycubrid.aio.connect``, ``Connection.__init__`` and
# ``AsyncConnection.__init__``. The two constructors name a different subset
# explicitly and read the rest out of ``**kwargs``, so the union is kept here
# once and checked against the live signatures by
# ``tests/test_unknown_options.py``. Anything reaching a constructor's
# ``**kwargs`` that is not in this set is a typo (or an option this driver does
# not implement) and is surfaced rather than silently dropped — issue #377.
KNOWN_CONNECTION_OPTIONS: frozenset[str] = frozenset(
    {
        "host",
        "port",
        "database",
        "user",
        "password",
        "ssl",
        "autocommit",
        "fetch_size",
        "decode_collections",
        "json_deserializer",
        "connect_timeout",
        "read_timeout",
        "no_backslash_escapes",
        "enable_timing",
    }
)

_PACKAGE_ROOT = os.path.dirname(os.path.abspath(__file__))


def _caller_stacklevel() -> int:
    """Return the ``warnings.warn`` stacklevel of the nearest non-pycubrid frame.

    No fixed stacklevel can point at user code here: a connection is built
    either directly (``Connection(...)``) or through one frame of
    ``pycubrid.connect()`` / ``pycubrid.aio.connect()``. Walking outwards until
    a frame's file leaves the package directory makes the warning attach to the
    caller's own line in both cases, which is the whole point of reporting it.

    Level 1 is the caller of this helper (the frame that calls
    :func:`warnings.warn`), so counting starts there.
    """
    frame = sys._getframe(1) if hasattr(sys, "_getframe") else None
    level = 1
    while frame is not None and frame.f_code.co_filename.startswith(_PACKAGE_ROOT + os.sep):
        frame = frame.f_back
        level += 1
    return level


def warn_unknown_connection_options(kwargs: dict[str, Any]) -> None:
    """Warn about connection keywords pycubrid does not recognise.

    Called from the sync and async connection constructors with whatever landed
    in their ``**kwargs``. Known options are left untouched; unknown ones are
    reported once, with a spelling suggestion when one is close enough, through
    :class:`~pycubrid.exceptions.UnknownConnectionOptionWarning`.
    """
    unknown = sorted(key for key in kwargs if key not in KNOWN_CONNECTION_OPTIONS)
    if not unknown:
        return

    known = sorted(KNOWN_CONNECTION_OPTIONS)
    fragments: list[str] = []
    for name in unknown:
        matches = difflib.get_close_matches(name, known, n=1, cutoff=0.6)
        if not matches:
            # Retry case-insensitively so camelCase spellings such as
            # ``connectTimeout`` still map onto ``connect_timeout``.
            matches = difflib.get_close_matches(name.lower(), known, n=1, cutoff=0.6)
        if matches:
            fragments.append(f"{name!r} (did you mean {matches[0]!r}?)")
        else:
            fragments.append(repr(name))

    plural = "s" if len(unknown) > 1 else ""
    warnings.warn(
        f"Unknown connection option{plural} ignored by pycubrid: "
        f"{', '.join(fragments)}. Supported options: {', '.join(known)}.",
        UnknownConnectionOptionWarning,
        stacklevel=_caller_stacklevel(),
    )


def resolve_ssl_context(
    ssl_param: bool | ssl_module.SSLContext | None,
) -> ssl_module.SSLContext | None:
    """Resolve an ``ssl`` parameter into an SSLContext or None."""
    if ssl_param is None:
        return None
    if isinstance(ssl_param, bool):
        if ssl_param:
            ctx = ssl_module.create_default_context()
            ctx.minimum_version = ssl_module.TLSVersion.TLSv1_2
            return ctx
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

    # -- DB-API 2.0 exception classes exposed on Connection instances -------
    # PEP 249 optional extension: exception classes are accessible as
    # attributes on Connection objects (and instances), so callers can handle
    # errors without importing the module. Identity is preserved, e.g.
    # ``conn.IntegrityError is pycubrid.IntegrityError``.
    Warning = Warning
    Error = Error
    InterfaceError = InterfaceError
    DatabaseError = DatabaseError
    DataError = DataError
    OperationalError = OperationalError
    IntegrityError = IntegrityError
    InternalError = InternalError
    ProgrammingError = ProgrammingError
    NotSupportedError = NotSupportedError

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
        no_backslash_escapes: bool | None = None,
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
        self._no_backslash_escapes: bool | None = no_backslash_escapes

        if type(fetch_size) is not int or fetch_size < 1:
            raise ValueError("fetch_size must be an integer >= 1")
        self._fetch_size = fetch_size

        if self._json_deserializer is not None and not callable(self._json_deserializer):
            raise TypeError("json_deserializer must be callable or None")
        # Note: json_deserializer is used as-is (issue #220 — removed no-op assignment).

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
        self._autocommit_explicitly_set = False
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

    def _invalidate_query_handles_for_reconnect(self) -> None:
        """Invalidate query handles and mark cursors as reconnect-invalidated.

        Distinct from :meth:`_invalidate_query_handles` so that mid-fetch
        callers can detect a transparent reconnect (and raise
        :class:`OperationalError`) without altering the commit/rollback
        invalidation semantics that PEP 249 callers already depend on.
        """
        for cursor in self._cursors:
            cursor._query_handle = None
            cursor._invalidated_by_reconnect = True

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

    @staticmethod
    def _validate_data_length(data_length: int) -> None:
        """Validate the DATA_LENGTH header from the broker.

        Raises OperationalError for negative or oversized values to prevent
        ValueError on allocation or unbounded memory usage (issue #188).
        """
        if data_length < 0:
            raise OperationalError(f"invalid DATA_LENGTH from broker: {data_length} (negative)")
        if data_length > DataSize.MAX_PACKET_SIZE:
            raise OperationalError(
                f"invalid DATA_LENGTH from broker: {data_length} "
                f"(exceeds max {DataSize.MAX_PACKET_SIZE})"
            )
