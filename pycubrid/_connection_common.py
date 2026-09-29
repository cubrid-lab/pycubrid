"""Shared mixin for sync and async connection implementations.

This module centralises common state initialisation, pure-logic helpers,
and constants so that ``connection.py`` and ``aio/connection.py`` import
from one place instead of duplicating code.

All methods are either pure (no I/O) or operate solely on instance state.
I/O-dependent methods (connect, send_and_receive, etc.) remain in the
concrete sync/async classes.
"""

from __future__ import annotations


import codecs
import difflib
import functools
import logging
import os
import re
import socket
import ssl as ssl_module
import sys
import warnings
from dataclasses import dataclass
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
from .packet import _codec_label, _encode_text, _unencodable_message
from .protocol import (
    CloseQueryPacket,
    FetchPacket,
    GetLastInsertIdPacket,
    GetSchemaPacket,
    LOBReadPacket,
    LOBWritePacket,
    _SchemaColumn,
)

if TYPE_CHECKING:
    from .timing import TimingStats

_LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class _SchemaResult:
    """Immutable original-session resource, independent of public packet fields."""

    handle: int
    count: int
    columns: tuple[_SchemaColumn, ...]


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
        "charset",
    }
)

# CUBRID charset names (``CHARSET utf8``, ``createdb ... ko_KR.euckr``) that
# are not all Python codec aliases. ``ksc5601`` is already a Python alias of
# EUC-KR. CUBRID's ``binary`` charset has no text codec and is rejected.
_CUBRID_CHARSET_ALIASES: dict[str, str] = {
    "utf8": "utf-8",
    "euckr": "euc-kr",
    "iso88591": "latin-1",
}
_ASCII_PROBE = bytes(range(128)).decode("ascii")
_LOCALE_PREFIX = re.compile(r"[A-Za-z]{2,3}_[A-Za-z]{2}\.(?=.)")
# Codecs of the CUBRID server charsets: every non-ASCII character encodes to
# bytes >= 0x80 only (asserted by the unit tests), so connecting with them
# skips the one-time full scan below.
_KNOWN_ASCII_SAFE_CODECS = frozenset({"utf-8", "euc_kr", "iso8859-1"})


def _encodes(text: str, name: str) -> bool:
    """Return whether codec ``name`` can encode ``text``."""
    try:
        text.encode(name)
    except UnicodeEncodeError:
        return False
    return True


@functools.lru_cache(maxsize=None)
def _codec_is_ascii_safe(name: str) -> bool:
    """Return whether ``name`` keeps ASCII bytes for ASCII characters only.

    SQL quoting and escaping run on ``str`` before encoding, so the codec must
    encode ASCII as itself and never emit a byte below 0x80 inside a non-ASCII
    character: a trail byte equal to ``'`` or ``\\`` (Shift_JIS, Big5, GBK,
    GB18030, stateful ISO-2022 and UTF-7/16/32) could end a literal early.
    """
    try:
        if _ASCII_PROBE.encode(name) != _ASCII_PROBE.encode("ascii"):
            return False
        if _ASCII_PROBE.encode("ascii").decode(name) != _ASCII_PROBE:
            return False
    except (UnicodeError, LookupError, TypeError, ValueError):
        return False
    if name in _KNOWN_ASCII_SAFE_CODECS:
        return True
    # Supplementary planes are scanned only when the codec can encode them
    # (no accepted stdlib codec does), keeping the one-time check fast.
    supplementary = any(_encodes(chr(probe), name) for probe in (0x10000, 0x1F600, 0x20000))
    for code_point in range(0x80, 0x110000 if supplementary else 0x10000):
        if 0xD800 <= code_point <= 0xDFFF:
            continue
        try:
            encoded = chr(code_point).encode(name)
        except UnicodeEncodeError:
            continue
        if not encoded or min(encoded) < 0x80:
            return False
    return True


def resolve_charset(charset: Any) -> str:
    """Validate a ``charset`` connection option and return its codec name.

    Accepts Python codec names, the CUBRID spellings ``utf8``, ``euckr``
    and ``iso88591``, and a CUBRID locale such as ``ko_KR.euckr`` (the part
    after the dot is used). ``None`` means the default ``"utf-8"``. Raises ``TypeError`` for a non-string and ``ValueError``
    for an unknown codec, CUBRID ``binary`` or a codec that is not
    ASCII-compatible, like the other connection options, before any socket
    work (#86).
    """
    if charset is None:
        return "utf-8"
    if not isinstance(charset, str):
        raise TypeError(f"charset must be a string, got {type(charset).__name__}")
    # Accept a CUBRID locale spelling such as "ko_KR.euckr" (createdb syntax).
    locale_match = _LOCALE_PREFIX.match(charset)
    if locale_match is not None:
        charset = charset[locale_match.end() :]
    key = charset.lower()
    if key == "binary":
        raise ValueError(
            "charset 'binary' has no text codec; connect with the charset of "
            "the character columns (for example 'utf8' or 'euckr')"
        )
    try:
        info = codecs.lookup(_CUBRID_CHARSET_ALIASES.get(key, charset))
    except LookupError:
        raise ValueError(f"unknown charset {charset!r}") from None
    if not getattr(info, "_is_text_encoding", True) or not _codec_is_ascii_safe(info.name):
        raise ValueError(
            f"charset {charset!r} is not ASCII-compatible; pycubrid supports "
            "codecs such as utf-8, euc-kr and latin-1"
        )
    return info.name


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

    # -- CAS_INFO transaction status (0 = OUT_TRAN, not socket release) -------
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
        charset: Any = "utf-8",
    ) -> None:
        """Initialise attributes common to sync and async connections."""
        # Validated here, before any socket work; reconnects reuse the codec.
        self._encoding = resolve_charset(charset)
        self._check_encodable("database", database)
        self._check_encodable("user", user)
        self._check_encodable("password", password)
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
        self._no_backslash_escapes_explicit = no_backslash_escapes is not None
        self._physical_generation = 0

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
        # CAS_INFO object whose OUT_TRAN status is already known to be live:
        # the OPEN_DATABASE reply or a successful CHECK_CAS. Any later reply
        # replaces ``_cas_info`` and requires a fresh probe (#485).
        self._verified_cas_info: bytes | bytearray | None = None
        # Depth of scopes (close, implicit-reconnect setup) whose requests must
        # never probe or reconnect; a counter so overlapping scopes nest safely.
        self._implicit_reconnect_suspended = 0
        self._session_id = 0
        self._autocommit = False
        self._autocommit_explicitly_set = False
        self._cursors: set[Any] = set()
        self._schema_owner = object()
        self._schema_results: dict[GetSchemaPacket, _SchemaResult] = {}
        self._protocol_version: int = 1
        # Broker identity captured after an INSERT by any cursor, as a string
        # (the cursor keeps its own integer lastrowid snapshot).
        # `commit()`/`rollback()` clear the broker's own session-scoped
        # last-insert-id state, so this is cached here instead of queried
        # live — see get_last_insert_id().
        self._last_insert_id: str | None = None

    # -- Pure helpers (no I/O) -----------------------------------------------

    def _check_encodable(self, name: str, value: Any) -> None:
        """Raise ``DataError`` before any I/O if ``value`` needs a missing character.

        Raised outside the handler so no chained exception keeps the text
        (it may be a password or a bound value).
        """
        if not isinstance(value, str):
            return
        encoded, position = _encode_text(value, self._encoding)
        if encoded is None:
            if name == "password":
                # Even a character position narrows down a secret.
                raise DataError(f"password cannot be encoded as {_codec_label(self._encoding)}")
            raise DataError(_unencodable_message(name, self._encoding, position))

    def _register_schema_result(self, packet: GetSchemaPacket) -> None:
        packet._owner = self._schema_owner
        self._schema_results[packet] = _SchemaResult(
            packet.query_handle, packet.tuple_count, tuple(packet.columns)
        )

    def _owned_schema_result(self, packet: GetSchemaPacket) -> _SchemaResult | None:
        if not isinstance(packet, GetSchemaPacket) or packet._owner is not self._schema_owner:
            raise InterfaceError("schema packet is not owned by this connection")
        return self._schema_results.get(packet)

    def _active_schema_result(self, packet: GetSchemaPacket) -> _SchemaResult:
        result = self._owned_schema_result(packet)
        if result is None:
            raise InterfaceError("schema result has been retired")
        self._ensure_connected()
        return result

    def _invalidate_query_handles(self) -> None:
        """Invalidate all cursor query handles.

        After commit/rollback the CUBRID broker may reset the CAS
        connection, making previous query handles stale. Retain buffers and
        delivered/advertised counts so cursors can consume cached rows and
        distinguish a missing unfinished FETCH from normal EOF.
        """
        for cursor in self._cursors:
            cursor._query_handle = None
        self._schema_results.clear()

    def _invalidate_query_handles_for_reconnect(self) -> None:
        """Invalidate query handles and mark cursors as reconnect-invalidated.

        Distinct from :meth:`_invalidate_query_handles` so that mid-fetch
        callers can detect explicit ping recovery (and raise
        :class:`OperationalError`) rather than the generic invalidated-result
        :class:`InterfaceError` at the next required server FETCH.
        """
        for cursor in self._cursors:
            cursor._query_handle = None
            cursor._invalidated_by_reconnect = True
        self._schema_results.clear()

    def _needs_cas_probe(self, allow_reconnect: bool) -> bool:
        """Return whether the next request must first be preceded by CHECK_CAS.

        JDBC ``UClientSideConnection.checkReconnect`` parity (#485): OUT_TRAN
        (``CAS_INFO[0] == 0``) does not mean the socket was released, but the
        CAS *may* close it after replying at a transaction boundary (memory
        restart, ``cubrid broker reset``, CHANGE CLIENT). Probe only then, and
        not again when this exact status was already verified live.
        """
        return (
            allow_reconnect
            and not self._implicit_reconnect_suspended
            and self._cas_status_unverified()
        )

    def _cas_status_unverified(self) -> bool:
        """Return whether the last reply was OUT_TRAN and not yet verified live."""
        return (
            self._cas_info[0] == self._CAS_INFO_STATUS_INACTIVE
            and self._cas_info is not self._verified_cas_info
        )

    @staticmethod
    def _skip_request_after_reconnect(packet: Any) -> bool:
        """Decide what happens to a request whose CAS session was just replaced.

        A CLOSE_REQ names a handle that died with the old CAS session, so there
        is nothing left to close. FETCH, last-insert-id and LOB read/write
        requests use state of the old session and must fail explicitly instead
        of reaching the new one. Every other request is session independent and
        is sent as-is; it has not been sent before, so this is not a replay.
        """
        if isinstance(packet, CloseQueryPacket):
            return True
        if isinstance(packet, GetLastInsertIdPacket):
            _LOGGER.warning(
                "CAS session was replaced before the last-insert-id lookup; "
                "lastrowid is unavailable for the preceding INSERT"
            )
            raise OperationalError("last insert id lost due to broker reconnect")
        if isinstance(packet, (LOBReadPacket, LOBWritePacket)):
            raise OperationalError("LOB handle lost due to broker reconnect; fetch the LOB again")
        if isinstance(packet, FetchPacket):
            raise OperationalError(
                "result set lost due to broker reconnect; re-execute the query to continue"
            )
        return False

    def _ensure_connected(self) -> None:
        """Raise ``InterfaceError`` when called on a closed connection."""
        if not self._connected:
            raise InterfaceError("connection is closed")

    def _check_closed(self) -> None:
        """Alias for ``_ensure_connected`` used by DB-API call sites."""
        self._ensure_connected()

    def _safe_close_socket(self) -> None:
        """Close the socket safely, ignoring any OS errors."""
        self._last_insert_id = None
        self._schema_results.clear()
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
