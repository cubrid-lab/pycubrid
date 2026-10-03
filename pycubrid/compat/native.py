"""Explicit native-style compatibility subset over the pure Python driver.

The sync prepared INT, string and NULL cursor, SET/MULTISET/SEQUENCE
collection binding (``connection.set()``, ``set.imports()``,
``cursor.bind_set()``), and BLOB/CLOB handle fetch, bind and byte-position
stream operations (``connection.lob()``, ``cursor.fetch_lob()``,
``cursor.bind_lob()``, ``lob.write/read/seek``), raw file transfers
(``lob.imports/export``), and cached column metadata
(``cursor.result_info()``) and current-result positioning
(``cursor.data_seek/row_seek/row_tell``) are supported here. Connection version
text and query-based integer ping use the captured physical session without retry.
Prepared
scalar strings use the connection charset (UTF-8 unless ``charset`` says
otherwise); native LOB stream text uses UTF-8 like the official Python 3
extension. Writable cached connection settings are distinct from effective
autocommit/isolation setters. Ordinary DB-API cursors continue to use their
existing FC41 path.
"""

from __future__ import annotations

import builtins
import logging
import operator
import os
import re
import struct
import tempfile
from socket import socket as _Socket
from threading import RLock
from typing import Any, BinaryIO, SupportsIndex

from pycubrid import __version__ as _CLIENT_VERSION
from pycubrid._connection_common import _CAS_DBMS_CUBRID
from pycubrid.connection import Connection as _DriverConnection
from pycubrid.constants import (
    CCIDbParam,
    CCIPrepareOption,
    CUBRIDDataType,
    CUBRIDIsolationLevel,
    CUBRIDStatementType,
)
from pycubrid.exceptions import (
    DataError,
    DatabaseError,
    InterfaceError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
)
from pycubrid.lob import Lob as _OrdinaryLob
from pycubrid.packet import _codec_label, _encode_text
from pycubrid.protocol import (
    CloseQueryPacket,
    ExecutePacket,
    FetchPacket,
    GetDbParameterPacket,
    GetEngineVersionPacket,
    LOBReadPacket,
    PreparePacket,
    PrepareAndExecutePacket,
    SetDbParameterPacket,
    _PreparedCollection,
    _PreparedLob,
    _PreparedScalar,
    _encode_prepared_collection,
    _encode_prepared_scalar,
)

_LOGGER = logging.getLogger(__name__)
_HOST = re.compile(r"[A-Za-z0-9_.-]+\Z")
_PORT = re.compile(r"[0-9]+\Z")
SEEK_SET = os.SEEK_SET
SEEK_CUR = os.SEEK_CUR
SEEK_END = os.SEEK_END
_LOB_IO_CHUNK = 64 * 1024
_MAX_LOB_POSITION = (1 << 63) - 1
_UNKNOWN_ISOLATION = "CUBRID_TRAN_UNKNOWN_ISOLATION"
_UtilityState = tuple[_DriverConnection, int, _Socket, bool]
_ISOLATION_NAMES = {
    4: "CUBRID_REP_CLASS_COMMIT_INSTANCE",
    5: "CUBRID_REP_CLASS_REP_INSTANCE",
    6: "CUBRID_SERIALIZABLE",
}


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
            generation = driver._physical_generation

            def read_parameter(parameter: CCIDbParam) -> int:
                packet = GetDbParameterPacket(parameter)
                driver._send_and_receive(
                    packet, allow_reconnect=False, expected_generation=generation
                )
                return packet.value

            lock_timeout = read_parameter(CCIDbParam.LOCK_TIMEOUT)
            try:
                max_string_len = read_parameter(CCIDbParam.MAX_STRING_LENGTH)
            except DatabaseError as exc:
                # The official C extension falls back only for a complete
                # server rejection, never for a lost or malformed reply.
                if not getattr(exc, "_cas_server_error", False):
                    raise
                max_string_len = 0
            isolation = read_parameter(CCIDbParam.ISOLATION_LEVEL)
            # On supported brokers these GETs leave CAS OUT_TRAN. If another
            # broker reports IN_TRAN, finish the constructor on a clean session.
            if driver._cas_info[0] == 1:
                driver.commit()
            if driver._physical_generation != generation:
                raise OperationalError("native settings session changed during initialization")
            if driver._cas_info[0] == 1:
                raise OperationalError("native settings left an active transaction")
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
        self.autocommit: Any = driver._autocommit
        self.lock_timeout: Any = lock_timeout
        self.max_string_len: Any = max_string_len
        # Match the pinned extension's initial READ COMMITTED text bug. The
        # effective numeric value stays separate and set_isolation_level(4)
        # repairs the visible name without sending an unnecessary SET.
        self.isolation_level: Any = (
            _UNKNOWN_ISOLATION
            if isolation == 4
            else _ISOLATION_NAMES.get(isolation, _UNKNOWN_ISOLATION)
        )
        self._effective_isolation = (driver, driver._physical_generation, isolation)

    def set_autocommit(self, mode: bool, /) -> None:
        """Change the effective CCI-style mode; raw cache assignment does not."""
        if type(mode) is not bool:
            raise InterfaceError("autocommit mode must be a bool")
        with self._session_lock:
            driver = self._driver
            if self._closed or not getattr(driver, "_connected", True):
                raise InterfaceError("compatibility connection is closed")
            if driver._autocommit != mode and driver._cas_info[0] == 1:
                try:
                    self.commit()
                except BaseException:
                    try:
                        driver._drop_connection()
                    except BaseException:
                        _LOGGER.warning("Failed to discard session after autocommit boundary")
                    raise
            driver._autocommit = mode
            driver._autocommit_explicitly_set = True
            self.autocommit = mode

    def set_isolation_level(self, level: int, /) -> None:
        """Set supported MVCC isolation without committing the transaction."""
        if type(level) not in (int, CUBRIDIsolationLevel) or int(level) not in _ISOLATION_NAMES:
            raise InterfaceError("isolation level must be a supported MVCC int (4, 5 or 6)")
        selected = int(level)
        with self._session_lock:
            driver = self._driver
            if self._closed or not getattr(driver, "_connected", True):
                raise InterfaceError("compatibility connection is closed")
            driver._check_reconnect()
            generation = driver._physical_generation
            owner, known_generation, known_level = self._effective_isolation
            if not (owner is driver and known_generation == generation and known_level == selected):
                driver._send_and_receive(
                    SetDbParameterPacket(CCIDbParam.ISOLATION_LEVEL, selected),
                    allow_reconnect=False,
                    expected_generation=generation,
                )
                self._effective_isolation = (driver, generation, selected)
            self.isolation_level = _ISOLATION_NAMES[selected]

    def cursor(self) -> cursor:
        """Create an explicit sync prepared cursor; no SQL is sent yet."""
        with self._session_lock:
            if self._closed or not getattr(self._driver, "_connected", True):
                raise InterfaceError("compatibility connection is closed")
            owner = cursor(self)
            self._prepared_owners.add(owner)
            return owner

    def _utility_state(self) -> _UtilityState:
        driver = self._driver
        if self._closed or not driver._connected or driver._socket is None:
            raise InterfaceError("compatibility connection is closed")
        state = (driver, driver._physical_generation, driver._socket, driver._autocommit)
        self._check_utility_state(state)
        return state

    def _check_utility_state(self, state: _UtilityState) -> None:
        driver, generation, sock, mode = state
        if (
            self._closed
            or self._driver is not driver
            or not driver._prepared_session_is_current(generation, sock)
            or driver._autocommit is not mode
        ):
            raise InterfaceError("utility connection or physical session changed")
        if mode and driver._schema_results:
            raise InterfaceError("utility cannot auto-commit an active schema result")

    def _utility_request(
        self,
        packet: GetEngineVersionPacket | PrepareAndExecutePacket | FetchPacket | CloseQueryPacket,
        state: _UtilityState,
    ) -> None:
        self._check_utility_state(state)
        driver, generation, _sock, _mode = state
        driver._send_and_receive(packet, allow_reconnect=False, expected_generation=generation)
        self._check_utility_state(state)

    def _utility_fetch_retired(self, state: _UtilityState) -> bool:
        driver, generation, sock, mode = state
        return (
            mode
            and driver._prepared_session_is_current(generation, sock)
            and driver._statement_pooling == 0
            and driver._broker_db_type == _CAS_DBMS_CUBRID
            and driver._cas_info[0] == 0
        )

    def server_version(self, /) -> str:
        """Read full server text on the current physical session, without retry."""
        with self._session_lock:
            state = self._utility_state()
            packet = GetEngineVersionPacket(auto_commit=state[3])
            self._utility_request(packet, state)
            return packet.engine_version

    def client_version(self, /) -> str:
        """Return this driver's frozen version text, including after close."""
        return _CLIENT_VERSION

    def ping(self, /) -> int:
        """Verify the constant scalar query and close its owned result before success."""
        with self._session_lock:
            state = self._utility_state()
            driver, generation, sock, mode = state
            packet = PrepareAndExecutePacket(
                "select 1+1 from db_root",
                auto_commit=mode,
                protocol_version=driver._protocol_version,
            )
            primary: BaseException | None = None
            handle: int | None = None
            retired = False
            try:
                self._utility_request(packet, state)
                retired = packet._query_handle_retired
                handle = None if retired else packet.query_handle or None
                if packet.statement_type != CUBRIDStatementType.SELECT or len(packet.columns) != 1:
                    raise DataError("utility query did not return one scalar column")
                total = packet.total_tuple_count
                rows = packet.rows
                consumed = 0
                found = False
                while True:
                    if len(rows) > total - consumed or any(len(row) != 1 for row in rows):
                        raise OperationalError("utility result page does not match row count")
                    found = found or any(row[0] == 2 for row in rows)
                    consumed += len(rows)
                    if consumed == total:
                        return int(found)
                    if handle is None:
                        raise OperationalError("utility result lost its required query handle")
                    fetched = FetchPacket(
                        handle,
                        consumed,
                        fetch_size=driver._fetch_size,
                        columns=packet.columns,
                    )
                    try:
                        self._utility_request(fetched, state)
                    except BaseException as fetch_error:
                        # Only a complete guarded broker error adopts its reply
                        # status before propagating. Prior/local status is not proof.
                        if getattr(
                            fetch_error, "_cas_server_error", False
                        ) and self._utility_fetch_retired(state):
                            retired = True
                            handle = None
                        raise
                    if self._utility_fetch_retired(state):
                        retired = True
                        handle = None
                    rows = fetched.rows
                    if not rows:
                        raise OperationalError("utility result ended before its declared row count")
            except BaseException as exc:
                primary = exc
                # A complete parse failure may still have disclosed an owned ID.
                if handle is None and not retired and not packet._query_handle_retired:
                    handle = packet.query_handle or None
                raise
            finally:
                if handle is not None and driver._prepared_session_is_current(generation, sock):
                    try:
                        self._utility_request(CloseQueryPacket(handle), state)
                    except BaseException:
                        if driver._prepared_session_is_current(generation, sock):
                            driver._discard_uncertain_prepared_session()
                        if primary is None:
                            raise
                        _LOGGER.warning("Failed to close utility query after primary failure")

    def set(self) -> _NativeSet:
        """Create an empty collection value for ``cursor.bind_set()``; no I/O."""
        return _NativeSet(self)

    def lob(self) -> _NativeLob:
        """Create an empty LOB handle holder for ``cursor.fetch_lob()``; no I/O."""
        return _NativeLob(self)

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
        self._prepared_driver: _DriverConnection | None = None
        self._prepared_sql: str | None = None
        self._needs_reprepare = False
        self._statement_type = 0
        self._bind_count = 0
        self._columns: list[Any] = []
        self._bindings: list[_PreparedScalar | _PreparedCollection | _PreparedLob | None] = []
        self._rows: list[tuple[Any, ...]] = []
        self._row_index = 0
        self._fetched_count = 0
        self._total_tuple_count = 0
        self._has_result = False
        self._result_invalidated = False
        self._metadata_ready = False
        self._next_position = 0
        self._tell_position = 0

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
        if self._prepared_driver is not driver:
            self._invalidate_result()
            raise InterfaceError("prepared statement belongs to another connection")
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
        self._next_position = 0

    def _clear_statement(self) -> None:
        self._handle = None
        self._generation = None
        self._prepared_driver = None
        self._prepared_sql = None
        self._needs_reprepare = False
        self._statement_type = 0
        self._bind_count = 0
        self._columns = []
        self._metadata_ready = False
        self._tell_position = 0
        self._bindings = []
        self._invalidate_result()

    def _request(self, packet: Any, generation: int, *, allow_reconnect: bool = True) -> None:
        """Keep broker-controlled text out of the explicit prepared API."""
        redacted_error: DatabaseError | None = None
        try:
            self._connection._driver._send_and_receive(
                packet, allow_reconnect=allow_reconnect, expected_generation=generation
            )
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
            setattr(redacted_error, "_cas_server_error", True)
        if redacted_error is not None:
            # Raise outside the except suite: `from None` still retains the
            # original broker message in __context__ for error collectors.
            raise redacted_error

    def _release_handle(self, *, allow_reconnect: bool = True) -> None:
        driver = self._connection._driver
        handle = self._handle
        generation = self._generation
        try:
            if (
                handle is not None
                and generation is not None
                and getattr(driver, "_connected", False)
                and self._prepared_driver is driver
                and generation == driver._physical_generation
            ):
                self._request(CloseQueryPacket(handle), generation, allow_reconnect=allow_reconnect)
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
            self._prepared_driver = driver
            self._prepared_sql = sql
            self._needs_reprepare = False
            self._statement_type = packet.statement_type
            self._bind_count = packet.bind_count
            self._columns = list(packet.columns)
            self._metadata_ready = False
            self._tell_position = 0
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

    def bind_lob(self, index: int, lob: _NativeLob, /) -> None:
        """Bind a one-based BLOB/CLOB handle held by ``lob``; no I/O.

        A handle fetched in effective autocommit mode names a committed stored
        value that the server copies into the new row, so it may be bound again,
        on another open connection, and after its own connection closed or
        reconnected. A manual-mode fetch stays on its own connection until a
        fresh autocommit fetch confirms a committed row. A newly written
        handle is temporary and may be bound only on its original live session;
        its first autocommit statement consumes that temporary file. The
        binding keeps the handle snapshot it was given and belongs to this
        cursor's current physical session.
        """
        with self._connection._session_lock:
            self._check_open()
            # The official PyArg_ParseTuple("iO!") order: index, then lob.
            if not isinstance(index, int):
                raise TypeError("bind_lob() index must be an int")
            if not isinstance(lob, _NativeLob):
                raise TypeError("bind_lob() requires a lob from connection.lob()")
            driver, _handle, generation = self._check_handle()
            if type(index) is not int or not 1 <= index <= self._bind_count:
                raise ProgrammingError("prepared parameter index is out of range")
            lob_type, handle = lob._bindable(self._connection)
            self._bindings[index - 1] = _PreparedLob(lob_type, handle, driver, generation)

    def fetch_lob(self, col: int, lob: _NativeLob, /) -> None:
        """Fetch the next row and put its BLOB/CLOB handle at ``col`` into ``lob``.

        ``col`` is one-based and its own column type decides BLOB or CLOB. A
        non-int ``col`` or a non-lob ``lob`` raises ``TypeError`` first, in
        the official argument-parsing order. At the end of the result it
        returns ``None`` and changes nothing, before the column range or type
        or the lob's state is checked, as the official driver does. Otherwise
        a column that is not BLOB/CLOB raises ``ProgrammingError`` without
        consuming the row, and ``lob`` must be open and belong to this
        connection. A NULL cell consumes the row and leaves ``lob`` without
        a value. Returns ``None``, like the official driver.

        A complete reply whose cell at ``col`` is not a LOB handle of the
        column's type raises ``DataError``; the row is not consumed, ``lob``
        is unchanged and the session stays usable (``fetch_row()`` still
        returns that row). A handle with damaged framing raises
        ``OperationalError`` and retires the uncertain physical session.
        """
        with self._connection._session_lock:
            self._check_open()
            # The official PyArg_ParseTuple("iO!") order: col, then lob,
            # before any cursor or result state is looked at.
            if type(col) is not int:
                raise TypeError("fetch_lob() column must be an int")
            if not isinstance(lob, _NativeLob):
                raise TypeError("fetch_lob() requires a lob from connection.lob()")
            driver, _handle, generation = self._check_handle()
            if self._result_invalidated:
                raise InterfaceError("prepared result was invalidated")
            if not self._has_result:
                raise InterfaceError("prepared cursor has no SELECT result")
            if not 1 <= self._next_position <= self._total_tuple_count:
                return None
            if not 1 <= col <= len(self._columns):
                raise ProgrammingError("prepared column index is out of range")
            lob_type = self._columns[col - 1].column_type
            if lob_type not in _LOB_TYPES:
                raise ProgrammingError("prepared column is not a BLOB or CLOB")
            lob._check_fillable(self._connection)
            row = self.fetch_row()
            if row is None:
                return None
            cell = row[col - 1]
            if cell is None:
                lob._set(lob_type, None, None, None)
                return None
            if not isinstance(cell, dict) or cell.get("lob_type") != lob_type:
                # A complete reply holding a value of another type is a data
                # problem (#492/#512), not framing damage: keep the session,
                # and leave the row unread like the other rejections here.
                self._row_index -= 1
                self._next_position -= 1
                self._tell_position -= 1
                raise DataError("prepared LOB cell does not match its column type")
            # The handle bytes come from the broker; damaged framing means an
            # uncertain reply, so the session is retired.
            try:
                packed: Any = cell.get("packed_lob_handle")  # validated by _PreparedLob
                binding = _PreparedLob(lob_type, packed, driver, generation)
            except ProgrammingError:
                self._invalidate_result()
                driver._discard_uncertain_prepared_session()
                raise OperationalError("malformed response from broker") from None
            lob._set(
                lob_type,
                binding.packed_handle,
                _FETCHED,
                (driver, generation),
                committed=driver.autocommit is True,
            )
            return None

    def _assert_refresh_session(
        self, driver: _DriverConnection, generation: int, autocommit: bool
    ) -> None:
        if (
            self._connection._driver is not driver
            or driver._physical_generation != generation
            or driver.autocommit is not autocommit
        ):
            raise InterfaceError("prepared session changed during statement refresh")

    def _refresh_for_explicit_execute(
        self,
        driver: _DriverConnection,
        generation: int,
        autocommit: bool,
        bindings: tuple[_PreparedScalar | _PreparedCollection | _PreparedLob, ...],
    ) -> int:
        """Replace an errored pooled handle before this new user execution."""
        sql = self._prepared_sql
        if sql is None:
            raise InterfaceError("prepared cursor has no retained statement")
        if driver._statement_pooling != 1:
            raise NotSupportedError("prepared statements require broker statement pooling")
        self._assert_refresh_session(driver, generation, autocommit)
        self._release_handle(allow_reconnect=False)
        self._assert_refresh_session(driver, generation, autocommit)
        packet = PreparePacket(
            sql,
            auto_commit=autocommit,
            prepare_flag=CCIPrepareOption.HOLDABLE,
        )
        self._request(packet, generation, allow_reconnect=False)
        self._handle = packet.query_handle
        self._generation = generation
        self._prepared_driver = driver
        self._prepared_sql = sql
        self._statement_type = packet.statement_type
        self._bind_count = packet.bind_count
        self._columns = list(packet.columns)
        self._metadata_ready = False
        self._tell_position = 0
        self._bindings = [None] * packet.bind_count
        self._invalidate_result()
        self._assert_refresh_session(driver, generation, autocommit)
        if packet.bind_count != len(bindings):
            raise ProgrammingError("refreshed prepared parameter count changed; rebind")
        self._bindings = list(bindings)
        return packet.query_handle

    def execute(self, option: int = 0, max_col_size: int = 0, /) -> int:
        """Execute once; an earlier complete error refreshes only this new call."""
        with self._connection._session_lock:
            driver, handle, generation = self._check_handle()
            if type(option) is not int or option != 0:
                raise ProgrammingError("unsupported prepared execute option")
            if type(max_col_size) is not int or max_col_size != 0:
                raise ProgrammingError("unsupported prepared max_col_size")
            if any(binding is None for binding in self._bindings):
                raise ProgrammingError("prepared parameter is unbound")
            bindings = tuple(binding for binding in self._bindings if binding is not None)
            for binding in bindings:
                # A LOB handle is valid only on the physical session it came
                # from; never send it to a replacement session.
                if isinstance(binding, _PreparedLob) and (
                    binding.owner is not driver or binding.generation != generation
                ):
                    raise InterfaceError("LOB binding belongs to another physical session")
            autocommit = driver.autocommit
            refreshed = False
            if self._needs_reprepare:
                if any(isinstance(binding, _PreparedLob) for binding in bindings):
                    raise InterfaceError(
                        "LOB binding needs explicit prepare and rebind after error"
                    )
                handle = self._refresh_for_explicit_execute(
                    driver, generation, autocommit, bindings
                )
                refreshed = True
            packet = ExecutePacket(
                handle,
                self._statement_type,
                auto_commit=autocommit,
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
            self._metadata_ready = False
            try:
                self._request(packet, generation, allow_reconnect=not refreshed)
            except DatabaseError as exc:
                if getattr(exc, "_cas_server_error", False) and not any(
                    isinstance(binding, _PreparedLob) for binding in bindings
                ):
                    self._needs_reprepare = True
                raise
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
            self._metadata_ready = True
            self._next_position = 1 if self._has_result else 0
            return packet.total_tuple_count

    def _check_positioning_result(self) -> None:
        """Check the current SELECT owner without clearing rows or probing CAS."""
        if self._closed:
            raise InterfaceError("prepared cursor is closed", code=-30019)
        self._check_open()
        driver = self._connection._driver
        if self._handle is None or self._generation is None:
            raise InterfaceError("prepared cursor has no current statement")
        if self._prepared_driver is not driver:
            raise InterfaceError("prepared statement belongs to another connection")
        if self._generation != driver._physical_generation:
            raise InterfaceError("prepared statement belongs to an earlier physical session")
        if self._result_invalidated:
            raise InterfaceError("prepared result was invalidated")
        if not self._has_result:
            raise InterfaceError("prepared cursor has no SELECT result")

    def _position_selector(self, args: tuple[SupportsIndex, ...]) -> int:
        """Parse one C-int selector and reject reentrant result/position changes."""
        if self._closed:
            raise InterfaceError("prepared cursor is closed", code=-30019)
        if len(args) != 1:
            raise TypeError("cursor positioning requires one positional argument")
        rows, columns = self._rows, self._columns
        state = (
            self._handle,
            self._generation,
            self._next_position,
            self._tell_position,
            self._row_index,
            self._fetched_count,
            self._total_tuple_count,
            self._metadata_ready,
        )
        n = operator.index(args[0])
        if not -(2**31) <= n < 2**31:
            raise OverflowError("cursor position is outside the C int range")
        self._check_positioning_result()
        current = (
            self._handle,
            self._generation,
            self._next_position,
            self._tell_position,
            self._row_index,
            self._fetched_count,
            self._total_tuple_count,
            self._metadata_ready,
        )
        if self._rows is not rows or self._columns is not columns or current != state:
            raise InterfaceError("prepared result changed during position argument conversion")
        return n

    def data_seek(self, *args: SupportsIndex) -> None:
        """Move to an absolute one-based row in the current SELECT result."""
        with self._connection._session_lock:
            n = self._position_selector(args)
            if not 1 <= n <= self._total_tuple_count:
                raise InterfaceError("prepared row index is out of range", code=-30006)
            self._next_position = self._tell_position = n

    def row_seek(self, *args: SupportsIndex) -> None:
        """Move relative to the physical cursor, retaining the separate tell counter."""
        with self._connection._session_lock:
            offset = self._position_selector(args)
            target = self._next_position + offset
            if target <= 0:
                self._next_position = 0
                raise InterfaceError("prepared cursor is before the first row", code=-20005)
            if target > self._total_tuple_count:
                self._next_position = self._total_tuple_count + 1
                raise InterfaceError("prepared cursor is after the last row", code=-20005)
            shadow = self._tell_position + offset
            if not -(2**31) <= shadow < 2**31:
                raise OverflowError("cursor tell counter is outside the C int range")
            self._next_position = target
            self._tell_position = shadow

    def row_tell(self, *args: object) -> int:
        """Return the native extension's shadow counter without requesting rows."""
        with self._connection._session_lock:
            if self._closed:
                raise InterfaceError("prepared cursor is closed", code=-30019)
            if args:
                raise TypeError("row_tell() accepts no positional arguments")
            self._check_positioning_result()
            if self._tell_position > self._total_tuple_count:
                raise InterfaceError("prepared cursor tell position is invalid", code=-30012)
            return self._tell_position

    def result_info(self, *args: SupportsIndex) -> tuple[tuple[int | str, ...], ...] | None:
        """Return cached 15-field column tuples for all columns or one-based n."""
        with self._connection._session_lock:
            if self._closed:
                raise InterfaceError("prepared cursor is closed", code=-30019)
            if len(args) > 1:
                raise TypeError("result_info() accepts at most one positional argument")
            n = operator.index(args[0]) if args else 0
            if not -(2**31) <= n < 2**31:
                raise OverflowError("result_info() selector is outside the C int range")
            # __index__ can reenter the RLock and change cursor/session ownership.
            if self._closed:
                raise InterfaceError("prepared cursor is closed", code=-30019)
            self._check_open()
            driver = self._connection._driver
            if self._handle is None or self._generation is None:
                raise InterfaceError("prepared cursor has no executed metadata", code=-30006)
            if self._prepared_driver is not driver:
                raise InterfaceError("prepared statement belongs to another connection")
            if self._generation != driver._physical_generation:
                raise InterfaceError("prepared statement belongs to an earlier physical session")
            if not self._metadata_ready:
                raise InterfaceError("prepared cursor has no executed metadata", code=-30006)
            if not self._columns:
                return None
            if n < 0 or n > len(self._columns):
                raise InterfaceError("prepared column index is out of range", code=-30006)
            columns = self._columns if n == 0 else self._columns[n - 1 : n]
            result: list[tuple[int | str, ...]] = []
            for column in columns:
                if column._cci_type is None:
                    raise InterfaceError("prepared column has no measured native type")
                result.append(
                    (
                        column._cci_type,
                        int(not column.is_nullable),
                        column.scale,
                        column.precision,
                        column.name,
                        column.real_name,
                        column.table_name,
                        column.default_value,
                        int(column.is_auto_increment),
                        int(column.is_unique_key),
                        int(column.is_primary_key),
                        int(column.is_foreign_key),
                        int(column.is_reverse_index),
                        int(column.is_reverse_unique),
                        int(column.is_shared),
                    )
                )
            return tuple(result)

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
            position = self._next_position
            if not 1 <= position <= self._total_tuple_count:
                return None
            shadow = self._tell_position + 1
            if not -(2**31) <= shadow < 2**31:
                raise OverflowError("cursor tell counter is outside the C int range")
            page_start = self._fetched_count - len(self._rows) + 1
            if not self._rows or not page_start <= position <= self._fetched_count:
                packet = FetchPacket(
                    handle,
                    position - 1,
                    fetch_size=driver._fetch_size,
                    columns=self._columns,
                    statement_type=self._statement_type,
                    decode_collections=driver._decode_collections,
                    json_deserializer=driver._json_deserializer,
                )
                try:
                    self._request(packet, generation, allow_reconnect=False)
                except BaseException:
                    self._invalidate_result()
                    raise
                if not packet.rows or len(packet.rows) > self._total_tuple_count - position + 1:
                    self._invalidate_result()
                    raise OperationalError("prepared result page does not match row count")
                self._rows = list(packet.rows)
                page_start = position
                self._fetched_count = position - 1 + len(packet.rows)
            index = position - page_start
            row = self._rows[index]
            self._row_index = index + 1
            self._next_position = position + 1
            self._tell_position = shadow
            return row

    def _on_commit(self) -> None:
        # HOLDABLE SELECT rows and the same-generation statement survive.
        return

    def _on_rollback(self) -> None:
        if self._has_result:
            self._invalidate_result()

    def _close_locked(self, *, allow_reconnect: bool = True) -> None:
        if self._closed:
            return
        try:
            self._release_handle(allow_reconnect=allow_reconnect)
        finally:
            self._closed = True
            self._connection._prepared_owners.discard(self)

    def close(self) -> None:
        """Release only the handle owned by this physical CAS generation."""
        with self._connection._session_lock:
            self._close_locked()

    def _close_collected_wrapper(self) -> None:
        """Best-effort same-session close for an abandoned qualified wrapper."""
        with self._connection._session_lock:
            self._close_locked(allow_reconnect=False)


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

_LOB_TYPES = frozenset({CUBRIDDataType.BLOB, CUBRIDDataType.CLOB})


# Where a lob's handle came from: fetched from a stored row, or created by
# LOB_NEW and written. A created handle names a
# temporary file of its own session, so it must stay on that session; a
# fetched handle of a committed row may be bound anywhere, because the server
# copies the stored file.
_FETCHED = "fetched"
_CREATED = "created"  # #442: lob.write() sets this origin


class _SessionLobTransport:
    """Give ordinary LOB I/O a fixed native physical-session boundary."""

    def __init__(self, owner: connection, driver: _DriverConnection, generation: int) -> None:
        self._owner = owner
        self._driver = driver
        self._generation = generation

    def _ensure_connected(self) -> None:
        if (
            self._owner._closed
            or self._owner._driver is not self._driver
            or not getattr(self._driver, "_connected", True)
            or self._driver._physical_generation != self._generation
        ):
            raise InterfaceError("lob handle belongs to an earlier physical session")

    def _send_and_receive(self, packet: Any) -> Any:
        self._ensure_connected()
        return self._driver._send_and_receive(
            packet, allow_reconnect=False, expected_generation=self._generation
        )


class lob:  # the official native type name
    """A BLOB/CLOB handle holder, created by ``connection.lob()``.

    ``cursor.fetch_lob()`` fills it with a fetched handle and
    ``cursor.bind_lob()`` binds that handle. The lob records the connection
    that created it, where its handle came from and the physical session it
    was fetched on. ``close()`` is local only: the CAS protocol has no LOB
    free request.
    """

    def __init__(self, conn: connection, /) -> None:
        if not isinstance(conn, connection):
            raise TypeError("lob() requires a compatibility connection")
        with conn._session_lock:
            if conn._closed or not getattr(conn._driver, "_connected", True):
                raise InterfaceError("compatibility connection is closed")
        self._connection = conn
        # (lob type, handle, origin, session, committed), replaced as one
        # tuple so a bind on another connection always reads a consistent
        # snapshot without taking this connection's lock. ``committed`` says
        # the handle was fetched in effective autocommit mode; a manual-mode
        # fetch stays conservative even after a later commit. The
        # official lob starts empty in BLOB mode.
        self._state: tuple[int, bytes | None, str | None, tuple[object, int] | None, bool] = (
            CUBRIDDataType.BLOB,
            None,
            None,
            None,
            False,
        )
        self._closed = False
        self._position = 0

    @property
    def _lob_type(self) -> int:
        return self._state[0]

    @property
    def _handle(self) -> bytes | None:
        return self._state[1]

    @property
    def _origin(self) -> str | None:
        return self._state[2]

    def _set(
        self,
        lob_type: int,
        handle: bytes | None,
        origin: str | None,
        session: tuple[object, int] | None,
        *,
        committed: bool = False,
    ) -> None:
        self._state = (lob_type, handle, origin, session, committed)

    def _check_fillable(self, conn: connection) -> None:
        if self._closed:
            raise InterfaceError("lob is closed")
        if self._connection is not conn:
            raise InterfaceError("lob belongs to another connection")

    def _bindable(self, conn: connection) -> tuple[int, bytes]:
        """Return the (lob type, handle) to bind on ``conn``, or raise."""
        source = self._connection
        lob_type, handle, origin, session, committed = self._state
        if self._closed:
            raise InterfaceError("lob is closed")
        if handle is None or session is None:
            raise InterfaceError("lob has no value")
        if origin not in (_FETCHED, _CREATED):
            raise InterfaceError("lob has no value")
        if origin == _FETCHED:
            # The server copies the stored file into the new row, whatever
            # happened to the source session since the fetch; that is safe
            # only for a committed row (an uncommitted one would be copied
            # permanently).
            if source is not conn and not committed:
                raise InterfaceError("lob was not fetched from a committed row")
            return lob_type, handle
        # A created (temporary) handle stays on its own physical session.
        driver, generation = session
        if source is not conn:
            raise InterfaceError("lob belongs to another connection")
        if (
            source._closed
            or not getattr(source._driver, "_connected", True)
            or source._driver is not driver
            or source._driver._physical_generation != generation
        ):
            raise InterfaceError("lob handle belongs to an earlier physical session")
        return lob_type, handle

    def _io(self) -> tuple[_SessionLobTransport, int, bytes, str, bool]:
        """Require the handle's original live physical session for read/write."""
        if self._closed:
            raise InterfaceError("lob is closed")
        source = self._connection
        lob_type, handle, origin, session, committed = self._state
        if handle is None or session is None or origin not in (_FETCHED, _CREATED):
            raise InterfaceError("lob has no value")
        driver, generation = session
        if source._driver is not driver:
            raise InterfaceError("lob handle belongs to an earlier physical session")
        transport = _SessionLobTransport(source, driver, generation)
        transport._ensure_connected()
        return transport, lob_type, handle, origin, committed

    def _adopt_written(
        self,
        ordinary: _OrdinaryLob,
        origin: str,
        transport: _SessionLobTransport,
        committed: bool,
    ) -> None:
        """Keep each confirmed chunk, even if a later broker request fails."""
        handle = ordinary.lob_handle
        self._set(
            ordinary.lob_type,
            handle,
            origin,
            (transport._driver, transport._generation),
            committed=committed,
        )
        self._position = int(struct.unpack_from(">q", handle, 4)[0])

    def write(self, data: str | bytes, type: str = "B", /) -> None:
        """Append UTF-8 text or bytes and advance the byte position (#442).

        The first write creates a BLOB by default or a CLOB with ``type='C'``.
        An existing handle keeps its type. CUBRID's external storage accepts
        writes only at the current end; seeking elsewhere cannot overwrite it.
        """
        # Avoid user-defined subclass hooks changing session/position while a
        # value is being encoded or sized for several broker requests.
        if builtins.type(data) not in (str, bytes):
            raise TypeError("lob.write() data must be str or bytes")
        if builtins.type(type) is not str:
            raise TypeError("lob.write() type must be a string")
        payload = data.encode("utf-8") if isinstance(data, str) else data
        with self._connection._session_lock:
            if self._closed:
                raise InterfaceError("lob is closed")
            if self._handle is None:
                if type.upper() not in {"B", "C"} or len(type) != 1:
                    raise ProgrammingError("lob type must be B or C")
                if self._position != 0:
                    raise NotSupportedError("LOB writes are append-only; seek to the end first")
                source = self._connection
                if source._closed or not getattr(source._driver, "_connected", True):
                    raise InterfaceError("compatibility connection is closed")
                driver = source._driver
                driver._check_reconnect()
                generation = driver._physical_generation
                transport = _SessionLobTransport(source, driver, generation)
                lob_type: int = CUBRIDDataType.BLOB if type.upper() == "B" else CUBRIDDataType.CLOB
                ordinary = _OrdinaryLob.create(transport, lob_type)
                try:
                    _PreparedLob(lob_type, ordinary.lob_handle, driver, generation)
                except ProgrammingError:
                    driver._discard_uncertain_prepared_session()
                    raise OperationalError("malformed response from broker") from None
                self._set(lob_type, ordinary.lob_handle, _CREATED, (driver, generation))
                origin, committed = _CREATED, False
            else:
                transport, lob_type, handle, origin, committed = self._io()
                ordinary = _OrdinaryLob(transport, lob_type, handle)

            size = struct.unpack_from(">q", ordinary.lob_handle, 4)[0]
            if self._position != size:
                raise NotSupportedError("LOB writes are append-only; seek to the end first")
            if size + len(payload) > _MAX_LOB_POSITION:
                raise InterfaceError("lob write would exceed the maximum byte position")
            if not payload:
                ordinary.write(b"", self._position)
                return
            for start in range(0, len(payload), _LOB_IO_CHUNK):
                try:
                    ordinary.write(payload[start : start + _LOB_IO_CHUNK], size + start)
                except OperationalError:
                    # Ordinary Lob.write keeps a confirmed short write's size.
                    self._adopt_written(ordinary, origin, transport, committed)
                    raise
                self._adopt_written(ordinary, origin, transport, committed)

    def read(self, length: int = 0, /) -> str:
        """Read UTF-8 text from the current byte position; zero means remaining."""
        if type(length) is not int or length < 0:
            raise InterfaceError("lob read length must be a non-negative int")
        with self._connection._session_lock:
            transport, _lob_type, handle, _origin, _committed = self._io()
            size = struct.unpack_from(">q", handle, 4)[0]
            remaining = max(0, size - self._position)
            requested = remaining if length == 0 else min(length, remaining)
            if requested == 0:
                return ""
            chunks: list[bytes] = []
            while requested > 0:
                packet = LOBReadPacket(handle, self._position, min(requested, _LOB_IO_CHUNK))
                transport._send_and_receive(packet)
                got, data = packet.bytes_read, packet.lob_data
                if type(got) is not int or got < 0 or type(data) is not bytes:
                    raise OperationalError("LOB read returned an invalid byte count or payload")
                if got > packet.length:
                    raise OperationalError(
                        f"LOB read returned {got} bytes exceeding requested {packet.length}"
                    )
                if len(data) != got:
                    raise OperationalError("LOB read byte count does not match payload")
                if got == 0:
                    break
                chunks.append(data)
                self._position += got
                requested -= got
            return b"".join(chunks).decode("utf-8")

    def seek(self, offset: int, whence: int = SEEK_CUR, /) -> int:
        """Move a byte position; SEEK_END subtracts its offset as in _cubrid."""
        if type(offset) is not int or type(whence) is not int:
            raise TypeError("lob seek offset and whence must be ints")
        if whence not in (SEEK_SET, SEEK_CUR, SEEK_END):
            raise ProgrammingError("unsupported lob seek whence")
        with self._connection._session_lock:
            if self._closed:
                raise InterfaceError("lob is closed")
            if self._connection._closed or not getattr(
                self._connection._driver, "_connected", True
            ):
                raise InterfaceError("compatibility connection is closed")
            if self._handle is not None:
                self._io()
            if whence == SEEK_END:
                _transport, _lob_type, handle, _origin, _committed = self._io()
                base = int(struct.unpack_from(">q", handle, 4)[0])
                position = base - offset
            else:
                position = offset if whence == SEEK_SET else self._position + offset
            if not 0 <= position <= _MAX_LOB_POSITION:
                raise InterfaceError("lob seek position is out of range")
            self._position = position
            return position

    def _file_boundary(self) -> tuple[_SessionLobTransport, object, int]:
        """Capture a live fixed session and the receiver state before file I/O."""
        if self._closed:
            raise InterfaceError("lob is closed")
        source = self._connection
        if source._closed or not getattr(source._driver, "_connected", True):
            raise InterfaceError("compatibility connection is closed")
        if self._handle is not None:
            transport = self._io()[0]
        else:
            transport = _SessionLobTransport(
                source, source._driver, source._driver._physical_generation
            )
            transport._ensure_connected()
        return transport, self._state, self._position

    def _check_file_boundary(
        self, transport: _SessionLobTransport, state: object, position: int
    ) -> None:
        transport._ensure_connected()
        if self._closed or self._state is not state or self._position != position:
            raise InterfaceError("lob changed during file transfer")

    @staticmethod
    def _validate_file_path(file: str) -> None:
        if builtins.type(file) is not str:
            raise TypeError("lob file must be an exact string")
        if "\x00" in file:
            raise ValueError("lob file path must not contain NUL")

    def imports(self, file: str, type: str = "B", /) -> None:
        """Replace the value with raw file bytes after a complete fixed-session transfer."""
        self._validate_file_path(file)
        if builtins.type(type) is not str:
            raise TypeError("lob type must be an exact string")
        if type not in ("B", "b", "C", "c"):
            raise InterfaceError("lob type must be B or C", code=-30006)
        with self._connection._session_lock:
            transport, state, position = self._file_boundary()
            try:
                path = os.path.abspath(file)
            except OSError as exc:
                raise InterfaceError("lob file open failed", code=-30009) from exc
            self._check_file_boundary(transport, state, position)
            reader: BinaryIO | None = None
            staged: lob | None = None
            try:
                try:
                    reader = open(path, "rb")
                except OSError as exc:
                    raise InterfaceError("lob file open failed", code=-30009) from exc
                self._check_file_boundary(transport, state, position)
                staged = _NativeLob(self._connection)
                lob_type = CUBRIDDataType.BLOB if type in ("B", "b") else CUBRIDDataType.CLOB
                ordinary = _OrdinaryLob.create(transport, lob_type)
                try:
                    _PreparedLob(
                        lob_type, ordinary.lob_handle, transport._driver, transport._generation
                    )
                except ProgrammingError:
                    transport._driver._discard_uncertain_prepared_session()
                    raise OperationalError("malformed response from broker") from None
                staged._set(
                    lob_type,
                    ordinary.lob_handle,
                    _CREATED,
                    (transport._driver, transport._generation),
                )
                self._check_file_boundary(transport, state, position)
                while True:
                    try:
                        chunk = reader.read(_LOB_IO_CHUNK)
                    except OSError as exc:
                        raise InterfaceError("lob file read failed", code=-30016) from exc
                    self._check_file_boundary(transport, state, position)
                    if builtins.type(chunk) is not bytes:
                        raise TypeError("lob file read must return bytes")
                    if not chunk:
                        break
                    _NativeLob.write(staged, chunk)
                    self._check_file_boundary(transport, state, position)
                try:
                    reader.close()
                except OSError as exc:
                    raise InterfaceError("lob file read failed", code=-30016) from exc
                reader = None
                self._check_file_boundary(transport, state, position)
                self._state = staged._state
            finally:
                if reader is not None:
                    try:
                        reader.close()
                    except OSError:
                        _LOGGER.warning("Failed to close LOB input during cleanup")
                if staged is not None:
                    _NativeLob.close(staged)

    def export(self, file: str, /) -> None:
        """Publish raw bytes through one sibling temporary file, preserving position."""
        self._validate_file_path(file)
        with self._connection._session_lock:
            transport, state, position = self._file_boundary()
            if self._handle is None:
                raise InterfaceError("lob has no value", code=-30018)
            lob_type, handle = self._lob_type, self._handle
            try:
                _PreparedLob(lob_type, handle, transport._driver, transport._generation)
            except ProgrammingError:
                transport._driver._discard_uncertain_prepared_session()
                raise OperationalError("malformed response from broker") from None
            size = struct.unpack_from(">q", handle, 4)[0]
            try:
                path = os.path.abspath(file)
            except OSError as exc:
                raise InterfaceError("lob file open failed", code=-30009) from exc
            self._check_file_boundary(transport, state, position)
            fd: int | None = None
            temporary: str | None = None
            writer: BinaryIO | None = None
            try:
                try:
                    fd, temporary = tempfile.mkstemp(
                        prefix=".pycubrid-lob-", dir=os.path.dirname(path)
                    )
                    self._check_file_boundary(transport, state, position)
                    writer = os.fdopen(fd, "wb")
                    fd = None
                except OSError as exc:
                    raise InterfaceError("lob file open failed", code=-30009) from exc
                self._check_file_boundary(transport, state, position)
                offset = 0
                while offset < size:
                    packet = LOBReadPacket(handle, offset, min(size - offset, _LOB_IO_CHUNK))
                    transport._send_and_receive(packet)
                    self._check_file_boundary(transport, state, position)
                    got, chunk = packet.bytes_read, packet.lob_data
                    if (
                        builtins.type(got) is not int
                        or builtins.type(chunk) is not bytes
                        or not 0 < got <= packet.length
                        or len(chunk) != got
                    ):
                        raise OperationalError("LOB file read returned invalid progress or payload")
                    try:
                        written = writer.write(chunk)
                    except OSError as exc:
                        raise InterfaceError("lob file write failed", code=-30017) from exc
                    self._check_file_boundary(transport, state, position)
                    if builtins.type(written) is not int or written != got:
                        raise InterfaceError("lob file write failed", code=-30017)
                    offset += got
                try:
                    writer.flush()
                    self._check_file_boundary(transport, state, position)
                    writer.close()
                except OSError as exc:
                    raise InterfaceError("lob file write failed", code=-30017) from exc
                writer = None
                self._check_file_boundary(transport, state, position)
                try:
                    os.replace(temporary, path)
                except OSError as exc:
                    raise InterfaceError("lob file write failed", code=-30017) from exc
                temporary = None
            finally:
                if writer is not None:
                    try:
                        writer.close()
                    except OSError:
                        _LOGGER.warning("Failed to close LOB export during cleanup %s", temporary)
                if fd is not None:
                    try:
                        os.close(fd)
                    except OSError:
                        _LOGGER.warning(
                            "Failed to close LOB export descriptor during cleanup %s", temporary
                        )
                if temporary is not None:
                    try:
                        os.unlink(temporary)
                    except OSError:
                        _LOGGER.warning("Failed to remove LOB export temporary file %s", temporary)

    def close(self) -> None:
        """Drop the handle locally; no I/O. Later use raises ``InterfaceError``.

        A binding already made from this lob keeps its handle.
        """
        with self._connection._session_lock:
            self._closed = True
            self._set(self._lob_type, None, None, None)


_NativeLob = lob


def connect(
    url: str,
    user: str = "public",
    passwd: str = "",  # nosec B107
    *,
    charset: str = "utf-8",
) -> connection:
    """Create a native-style sync compatibility connection."""
    return connection(url, user, passwd, charset=charset)


__all__ = ["connection", "connect", "cursor", "lob", "set", "SEEK_SET", "SEEK_CUR", "SEEK_END"]
