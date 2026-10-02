"""Explicit native-style compatibility subset over the pure Python driver.

Only the sync prepared INT, string and NULL cursor and SET/MULTISET/SEQUENCE
collection binding (``connection.set()``, ``set.imports()``,
``cursor.bind_set()``) are supported here. Strings use the connection charset
(UTF-8 unless ``charset`` says otherwise). Ordinary DB-API cursors continue to
use their existing FC41 path.
"""

from __future__ import annotations

import builtins
import logging
import re
from threading import RLock
from typing import Any

from pycubrid.connection import Connection as _DriverConnection
from pycubrid.constants import CCIPrepareOption, CUBRIDDataType, CUBRIDStatementType
from pycubrid.exceptions import (
    DataError,
    DatabaseError,
    InterfaceError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
)
from pycubrid.packet import _codec_label, _encode_text
from pycubrid.protocol import (
    CloseQueryPacket,
    ExecutePacket,
    FetchPacket,
    PreparePacket,
    _PreparedCollection,
    _PreparedScalar,
    _encode_prepared_collection,
    _encode_prepared_scalar,
)

_LOGGER = logging.getLogger(__name__)
_HOST = re.compile(r"[A-Za-z0-9_.-]+\Z")
_PORT = re.compile(r"[0-9]+\Z")


def _parse_url(url: str) -> tuple[str, int, str]:
    if not isinstance(url, str):
        raise TypeError("url must be a string")
    if "\x00" in url:
        raise ValueError("url must not contain NUL")
    parts = url.split(":", 6)
    if len(parts) != 7:
        raise InterfaceError("invalid CUBRID DSN")
    backend, host, port_text, database, _dsn_user, _dsn_password, options = parts
    if backend.casefold() in {"cubrid-mysql", "cubrid-oracle"}:
        raise NotSupportedError("alternate CUBRID backends are not supported")
    if backend.casefold() != "cubrid":
        raise InterfaceError("invalid CUBRID DSN")
    if options:
        raise NotSupportedError("CCI DSN options are not supported")
    if not _HOST.fullmatch(host) or not _PORT.fullmatch(port_text) or not database:
        raise InterfaceError("invalid CUBRID DSN")
    port = int(port_text)
    if not 1 <= port <= 65535:
        raise InterfaceError("invalid CUBRID broker port")
    return host, port, database


class connection:
    """Own one sync transport and its separately tracked prepared cursors."""

    def __init__(
        self,
        url: str,
        user: str = "public",
        passwd: str = "",  # nosec B107
        *,
        charset: str = "utf-8",
    ) -> None:
        host, port, database = _parse_url(url)
        for name, value in (("user", user), ("passwd", passwd)):
            if not isinstance(value, str):
                raise TypeError(f"{name} must be a string")
            if "\x00" in value:
                raise ValueError(f"{name} must not contain NUL")

        # Retain the object if its initializer fails after acquiring a socket.
        # Calling the ordinary factory would lose the reference on that path.
        driver = _DriverConnection.__new__(_DriverConnection)
        try:
            _DriverConnection.__init__(
                driver,
                host=host,
                port=port,
                database=database,
                user=user,
                password=passwd,
                autocommit=True,
                charset=charset,
            )
        except BaseException:
            if hasattr(driver, "_socket"):
                try:
                    # Fail closed: a broker reply may be unread after setup fails.
                    driver._drop_connection()
                except BaseException:
                    # A secondary cleanup failure must not replace the original.
                    _LOGGER.warning("Failed to discard an incomplete compatibility session")
            raise
        self._driver = driver
        self._closed = False
        self._session_lock = getattr(driver, "_session_lock", RLock())
        self._prepared_owners: builtins.set[cursor] = builtins.set()

    def cursor(self) -> cursor:
        """Create an explicit sync prepared cursor; no SQL is sent yet."""
        with self._session_lock:
            if self._closed or not getattr(self._driver, "_connected", True):
                raise InterfaceError("compatibility connection is closed")
            owner = cursor(self)
            self._prepared_owners.add(owner)
            return owner

    def set(self) -> _NativeSet:
        """Create an empty collection value for ``cursor.bind_set()``; no I/O."""
        return _NativeSet(self)

    def commit(self) -> None:
        """Commit, then notify prepared results only after a successful boundary."""
        with self._session_lock:
            if self._closed:
                raise InterfaceError("compatibility connection is closed")
            self._driver.commit()
            for owner in tuple(self._prepared_owners):
                owner._on_commit()

    def rollback(self) -> None:
        """Roll back and invalidate active prepared results after success."""
        with self._session_lock:
            if self._closed:
                raise InterfaceError("compatibility connection is closed")
            self._driver.rollback()
            for owner in tuple(self._prepared_owners):
                owner._on_rollback()

    def close(self) -> None:
        """Close the owned transport once."""
        with self._session_lock:
            if self._closed:
                return
            for owner in tuple(self._prepared_owners):
                try:
                    owner._close_locked()
                except Exception:  # noqa: BLE001 — best-effort owner cleanup before transport close
                    _LOGGER.warning("Failed to close a compatibility prepared cursor")
            self._driver.close()
            self._closed = True


class cursor:
    """One same-session FC2 handle with typed scalar FC3 bindings and row fetch."""

    def __init__(self, owner: connection) -> None:
        self._connection = owner
        self._closed = False
        self._handle: int | None = None
        self._generation: int | None = None
        self._statement_type = 0
        self._bind_count = 0
        self._columns: list[Any] = []
        self._bindings: list[_PreparedScalar | _PreparedCollection | None] = []
        self._rows: list[tuple[Any, ...]] = []
        self._row_index = 0
        self._fetched_count = 0
        self._total_tuple_count = 0
        self._has_result = False
        self._result_invalidated = False

    def _check_open(self) -> None:
        driver = self._connection._driver
        if self._closed or self._connection._closed or not getattr(driver, "_connected", True):
            raise InterfaceError("prepared cursor or connection is closed")

    def _check_handle(self) -> tuple[_DriverConnection, int, int]:
        self._check_open()
        driver = self._connection._driver
        handle = self._handle
        generation = self._generation
        if handle is None or generation is None:
            raise InterfaceError("prepared cursor has no current statement")
        if generation != driver._physical_generation:
            self._invalidate_result()
            raise InterfaceError("prepared statement belongs to an earlier physical session")
        return driver, handle, generation

    def _invalidate_result(self) -> None:
        self._rows = []
        self._row_index = 0
        self._fetched_count = 0
        self._total_tuple_count = 0
        self._has_result = False
        self._result_invalidated = True

    def _clear_statement(self) -> None:
        self._handle = None
        self._generation = None
        self._statement_type = 0
        self._bind_count = 0
        self._columns = []
        self._bindings = []
        self._invalidate_result()

    def _request(self, packet: Any, generation: int) -> None:
        """Keep broker-controlled text out of the explicit prepared API."""
        redacted_error: DatabaseError | None = None
        try:
            self._connection._driver._send_and_receive(packet, expected_generation=generation)
        except DatabaseError as exc:
            if not getattr(exc, "_cas_server_error", False):
                raise
            # The broker can echo SQL/value text in an error. Preserve its
            # DB-API class and machine-readable fields, not that raw text.
            redacted_error = type(exc)(
                "prepared statement failed on server",
                code=exc.code,
                errno=exc.errno,
                sqlstate=exc.sqlstate,
            )
        if redacted_error is not None:
            # Raise outside the except suite: `from None` still retains the
            # original broker message in __context__ for error collectors.
            raise redacted_error

    def _release_handle(self) -> None:
        driver = self._connection._driver
        handle = self._handle
        generation = self._generation
        try:
            if (
                handle is not None
                and generation is not None
                and getattr(driver, "_connected", False)
                and generation == driver._physical_generation
            ):
                self._request(CloseQueryPacket(handle), generation)
        finally:
            # FC6 may have succeeded remotely before its reply failed. Never
            # expose buffered rows or reuse an uncertain owner after that.
            self._clear_statement()

    def prepare(self, sql: str) -> None:
        """Prepare SQL once on a measured pooling-on physical CAS session."""
        with self._connection._session_lock:
            self._check_open()
            # Exact str avoids a subclass changing encode() between this
            # preflight and PreparePacket.write() under the session lock.
            if type(sql) is not str or "\x00" in sql:
                raise ProgrammingError("prepared SQL must be a string without NUL")
            driver = self._connection._driver
            encoded, _position = _encode_text(sql, driver._encoding)
            if encoded is None:
                # Raised outside the handler so no chained exception keeps the SQL.
                raise DataError(
                    f"prepared SQL cannot be encoded as {_codec_label(driver._encoding)}"
                )
            if driver._statement_pooling != 1:
                raise NotSupportedError("prepared statements require broker statement pooling")
            if self._handle is not None:
                self._release_handle()
            generation = driver._physical_generation
            packet = PreparePacket(
                sql,
                auto_commit=driver.autocommit,
                prepare_flag=CCIPrepareOption.HOLDABLE,
            )
            self._request(packet, generation)
            self._handle = packet.query_handle
            self._generation = generation
            self._statement_type = packet.statement_type
            self._bind_count = packet.bind_count
            self._columns = list(packet.columns)
            self._bindings = [None] * packet.bind_count
            self._invalidate_result()

    def bind_param(self, index: int, value: Any, bind_type: int = 0, /) -> None:
        """Bind a one-based INT, string or SQL NULL without wire I/O."""
        with self._connection._session_lock:
            driver, _handle, _generation = self._check_handle()
            if type(bind_type) is not int or bind_type != 0:
                raise ProgrammingError("unsupported prepared bind type")
            if type(index) is not int or not 1 <= index <= self._bind_count:
                raise ProgrammingError("prepared parameter index is out of range")
            binding = _encode_prepared_scalar(value, driver._encoding)
            self._bindings[index - 1] = binding

    def bind_set(self, index: int, s: _NativeSet, /) -> None:
        """Bind a one-based collection snapshot from ``connection.set()``; no I/O.

        A set that was never imported binds SQL NULL, as the official driver does.
        """
        with self._connection._session_lock:
            driver, _handle, _generation = self._check_handle()
            if type(index) is not int or not 1 <= index <= self._bind_count:
                raise ProgrammingError("prepared parameter index is out of range")
            if not isinstance(s, _NativeSet):
                raise InterfaceError("bind_set() requires a set from connection.set()")
            binding = s._binding
            if binding is None:
                self._bindings[index - 1] = _encode_prepared_scalar(None)
                return
            if binding.encoding != driver._encoding:
                raise ProgrammingError("collection was imported for a different charset")
            self._bindings[index - 1] = binding

    def execute(self, option: int = 0, max_col_size: int = 0, /) -> int:
        """Execute the current handle once with a complete typed binding snapshot."""
        with self._connection._session_lock:
            driver, handle, generation = self._check_handle()
            if type(option) is not int or option != 0:
                raise ProgrammingError("unsupported prepared execute option")
            if type(max_col_size) is not int or max_col_size != 0:
                raise ProgrammingError("unsupported prepared max_col_size")
            if any(binding is None for binding in self._bindings):
                raise ProgrammingError("prepared parameter is unbound")
            bindings = tuple(binding for binding in self._bindings if binding is not None)
            packet = ExecutePacket(
                handle,
                self._statement_type,
                auto_commit=driver.autocommit,
                protocol_version=driver._protocol_version,
                decode_collections=driver._decode_collections,
                json_deserializer=driver._json_deserializer,
                bindings=bindings,
                bind_count=self._bind_count,
            )
            packet.columns = list(self._columns)
            # A broker-side execution failure has replaced the previous
            # result, even if the physical prepared handle remains reusable.
            # Local option/binding failures above preserve the old result.
            self._invalidate_result()
            self._request(packet, generation)
            self._statement_type = packet.statement_type
            self._columns = list(packet.columns)
            self._rows = list(packet.rows)
            self._row_index = 0
            self._fetched_count = len(packet.rows)
            self._total_tuple_count = packet.total_tuple_count
            self._has_result = packet.statement_type == CUBRIDStatementType.SELECT
            self._result_invalidated = False
            if self._has_result and self._fetched_count > self._total_tuple_count:
                self._invalidate_result()
                raise OperationalError("prepared result contains more rows than advertised")
            return packet.total_tuple_count

    def fetch_row(self, how: int = 0, /) -> tuple[Any, ...] | None:
        """Fetch the next tuple; unsupported row shapes fail before I/O."""
        with self._connection._session_lock:
            driver, handle, generation = self._check_handle()
            if type(how) is not int or how != 0:
                raise ProgrammingError("unsupported prepared fetch mode")
            if self._result_invalidated:
                raise InterfaceError("prepared result was invalidated")
            if not self._has_result:
                raise InterfaceError("prepared cursor has no SELECT result")
            if self._row_index >= len(self._rows):
                if self._fetched_count >= self._total_tuple_count:
                    return None
                packet = FetchPacket(
                    handle,
                    self._fetched_count,
                    fetch_size=driver._fetch_size,
                    columns=self._columns,
                    statement_type=self._statement_type,
                    decode_collections=driver._decode_collections,
                    json_deserializer=driver._json_deserializer,
                )
                self._request(packet, generation)
                if (
                    not packet.rows
                    or len(packet.rows) > self._total_tuple_count - self._fetched_count
                ):
                    self._invalidate_result()
                    raise OperationalError("prepared result page does not match row count")
                self._rows = list(packet.rows)
                self._row_index = 0
                self._fetched_count += len(packet.rows)
            row = self._rows[self._row_index]
            self._row_index += 1
            return row

    def _on_commit(self) -> None:
        # HOLDABLE SELECT rows and the same-generation statement survive.
        return

    def _on_rollback(self) -> None:
        if self._has_result:
            self._invalidate_result()

    def _close_locked(self) -> None:
        if self._closed:
            return
        try:
            self._release_handle()
        finally:
            self._closed = True
            self._connection._prepared_owners.discard(self)

    def close(self) -> None:
        """Release only the handle owned by this physical CAS generation."""
        with self._connection._session_lock:
            self._close_locked()


# The official imports() converts BIT/VARBIT element text to bit strings;
# every other element type code is sent as STRING elements.
_BIT_TYPES = frozenset({CUBRIDDataType.BIT, CUBRIDDataType.VARBIT})
# The 10.2/11.4 brokers reject the MULTISET bind kind with -454, and a SET
# bind drops duplicates, so MULTISET is sent as SEQUENCE: the server stores
# it into a MULTISET column with its duplicates.
_WIRE_KINDS: dict[int, int] = {
    CUBRIDDataType.SET: CUBRIDDataType.SET,
    CUBRIDDataType.MULTISET: CUBRIDDataType.SEQUENCE,
    CUBRIDDataType.SEQUENCE: CUBRIDDataType.SEQUENCE,
}


class set:  # the official native type name shadows the builtin here
    """A collection value for ``cursor.bind_set()``, created by ``connection.set()``.

    It holds no server resource. ``imports()`` replaces the value; a bind
    keeps the value it was given.
    """

    def __init__(self, conn: connection, /) -> None:
        if not isinstance(conn, connection):
            raise InterfaceError("set() requires a compatibility connection")
        with conn._session_lock:
            if conn._closed or not getattr(conn._driver, "_connected", True):
                raise InterfaceError("compatibility connection is closed")
            # Elements are encoded at import time with the connection charset.
            self._encoding: str = conn._driver._encoding
        self._binding: _PreparedCollection | None = None

    def imports(
        self,
        data: tuple[Any, ...],
        type: int,  # the official positional name; builtins.type is used below
        /,
        *,
        kind: int = CUBRIDDataType.SET,
    ) -> None:
        """Set the value from a tuple of elements; no I/O.

        Like the official driver, every element is sent as a STRING (type 2)
        element whatever ``type`` is, and the server converts it to the
        column's element type. ``type`` is any CCI type code except ``BIT``
        (5) and ``VARBIT`` (6), which are not supported. Elements are ``str``,
        ``None`` (a NULL element) or, for ``INT`` (8), ``int`` in signed 64-bit
        range (sent as its decimal text; it may be mixed with digit strings).
        ``kind`` is ``SET`` (16, the official bytes), ``MULTISET`` (17, sent as
        ``SEQUENCE``) or ``SEQUENCE`` (18). Invalid input raises before
        anything changes.
        """
        if builtins.type(data) is not tuple:
            raise InterfaceError("imports() data must be a tuple")
        # int or an int enum such as CUBRIDDataType, never bool.
        if isinstance(type, bool) or not isinstance(type, int):
            raise InterfaceError("collection element type must be an int type code")
        if type in _BIT_TYPES:
            raise NotSupportedError("BIT/VARBIT collection elements are not supported")
        if isinstance(kind, bool) or not isinstance(kind, int) or kind not in _WIRE_KINDS:
            raise ProgrammingError("unsupported collection kind")
        elements: list[str | None] = []
        for value in data:
            if value is None:
                elements.append(None)
            elif isinstance(value, str):
                # A plain copy: a str subclass cannot choose the encoded bytes.
                elements.append(str.__str__(value))
            elif type == CUBRIDDataType.INT and builtins.type(value) is int:
                # No integer column holds more than BIGINT; this bound also
                # keeps str() below Python's integer-string digit limit.
                if not -(2**63) <= value < 2**63:
                    raise DataError("collection INT element is outside signed 64-bit range")
                elements.append(str(value))
            else:
                raise ProgrammingError("unsupported collection element type")
        self._binding = _encode_prepared_collection(
            tuple(elements), _WIRE_KINDS[kind], CUBRIDDataType.STRING, self._encoding
        )


_NativeSet = set


def connect(
    url: str,
    user: str = "public",
    passwd: str = "",  # nosec B107
    *,
    charset: str = "utf-8",
) -> connection:
    """Create a native-style sync compatibility connection."""
    return connection(url, user, passwd, charset=charset)


__all__ = ["connection", "connect", "cursor", "set"]
