from __future__ import annotations

import logging
import socket
import ssl as ssl_module
import struct
import time
from importlib import import_module
from threading import RLock
from typing import TYPE_CHECKING, Any

from ._connection_common import (
    ConnectionCommonMixin,
    resolve_ssl_context,
    warn_unknown_connection_options,
)
from .constants import CCIDbParam, DataSize
from .exceptions import DataError, Error, InterfaceError, OperationalError
from .protocol import (
    BatchExecutePacket,
    CheckCasPacket,
    ClientInfoExchangePacket,
    CloseDatabasePacket,
    CloseQueryPacket,
    CommitPacket,
    FetchPacket,
    GetEngineVersionPacket,
    GetSchemaPacket,
    OpenDatabasePacket,
    PrepareAndExecutePacket,
    RollbackPacket,
    SetDbParameterPacket,
)

if TYPE_CHECKING:
    from typing import Any as Cursor

_CursorClass: type | None = None

_LOGGER = logging.getLogger(__name__)

# Re-export for backwards compatibility.
_resolve_ssl_context = resolve_ssl_context


class Connection(ConnectionCommonMixin):
    """PEP 249 DB-API connection for the CUBRID CAS protocol."""

    def __init__(
        self,
        host: str,
        port: int,
        database: str,
        user: str,
        password: str,
        autocommit: bool = False,
        decode_collections: bool = False,
        json_deserializer: Any = None,
        ssl: bool | ssl_module.SSLContext | None = None,
        fetch_size: int = 100,
        **kwargs: Any,
    ) -> None:
        # The first connect() runs during construction, and failure cleanup in
        # compat.native may call _drop_connection() on this partial object.
        self._session_lock = RLock()
        # Report typo'd/unsupported options before any socket work, so a
        # mis-spelled option is surfaced even when the connection then fails.
        warn_unknown_connection_options(kwargs)
        self._ssl_context = resolve_ssl_context(ssl)
        self._init_common_state(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password,
            fetch_size=fetch_size,
            connect_timeout=kwargs.get("connect_timeout"),
            read_timeout=kwargs.get("read_timeout"),
            decode_collections=decode_collections,
            json_deserializer=json_deserializer,
            no_backslash_escapes=kwargs.get("no_backslash_escapes", None),
            enable_timing=kwargs.get("enable_timing"),
        )
        # OPEN_DATABASE advertises this per physical broker session.  A
        # prepared handle may be reused only on a measured pooling-on lane.
        self._statement_pooling: int | None = None

        self.connect()
        if autocommit:
            self.autocommit = True

    def _negotiate_backslash_escapes(self) -> None:
        """Detect the server's backslash-escape mode when not pinned.

        CUBRID's ``no_backslash_escapes`` system parameter defaults to
        ``yes``: a backslash is an ordinary literal character, not an
        escape marker.  When the caller has not set ``no_backslash_escapes``
        explicitly, probe the live server with ``SELECT CHAR_LENGTH('\\')``
        (the SQL literal ``'\\'`` — two backslash characters):

        * result ``2`` — literal mode (the server default); the driver must
          NOT double backslashes, so ``self._no_backslash_escapes`` is
          ``True``.
        * result ``1`` — the server unescaped the pair, i.e. backslash-escape
          processing is on, so ``self._no_backslash_escapes`` is ``False``.
        * any other value or any error — raise :class:`OperationalError`.
          Guessing the escape mode is unsafe: a wrong value silently
          corrupts string escaping (and can enable SQL injection), so a
          detection failure fails loud rather than falling back. Pass
          ``no_backslash_escapes`` explicitly to skip the probe entirely.
        """
        if self._no_backslash_escapes is not None:
            return
        # From here the probe SELECT runs in the default manual-commit mode,
        # which opens a driver-owned transaction before the constructor's
        # autocommit setting or recovered session state is applied. Roll it
        # back on *every* exit path
        # (success, unexpected result, or probe error) so a freshly opened
        # connection is handed back with clean transaction state (e.g.
        # SQLAlchemy setting isolation_level on a new pooled connection can be
        # rejected mid-transaction). Passing no_backslash_escapes explicitly
        # skips the probe entirely (early return above) and opens no tx.
        try:
            try:
                cursor = self.cursor()
                try:
                    cursor.execute("SELECT CHAR_LENGTH('\\\\')")
                    row = cursor.fetchone()
                finally:
                    cursor.close()
            except Exception as exc:  # noqa: BLE001 — re-raised as OperationalError
                raise OperationalError(
                    "Failed to detect CUBRID backslash-escape mode; refusing to "
                    "guess because a wrong mode silently corrupts string escaping. "
                    "Pass no_backslash_escapes explicitly to skip detection."
                ) from exc
            length = row[0] if row else None
            if length == 2:
                self._no_backslash_escapes = True
            elif length == 1:
                self._no_backslash_escapes = False
            else:
                raise OperationalError(
                    "Could not detect CUBRID backslash-escape mode "
                    f"(CHAR_LENGTH probe returned {length!r}); refusing to guess "
                    "because a wrong mode silently corrupts string escaping. Pass "
                    "no_backslash_escapes explicitly to skip detection."
                )
        except BaseException:
            probe_failed = True
            raise
        else:
            probe_failed = False
        finally:
            try:
                self.rollback()
            except Exception as rollback_exc:  # noqa: BLE001
                # Rolling back the probe transaction failed, so it may still be
                # open and the connection is in an unknown transaction state.
                # Close it so it can never be handed back to a pool, then surface
                # the failure — unless a probe error is already propagating, in
                # which case that original error is preserved.
                try:
                    self.close()
                except Exception:  # nosec B110 — best-effort close after rollback failure
                    pass
                if not probe_failed:
                    raise OperationalError(
                        "Failed to roll back the CUBRID backslash-escape probe "
                        "transaction; the connection may be in an unknown "
                        "transaction state and has been closed. Pass "
                        "no_backslash_escapes explicitly to skip detection."
                    ) from rollback_exc

    def connect(self) -> None:
        """Establish a TCP CAS session with broker handshake and open database.

        Performs CUBRID's STARTTLS-style upgrade when ``ssl`` was supplied:

        1. Open a plaintext TCP socket to ``(host, port)``.
        2. Send the 10-byte ``ClientInfoExchange`` handshake. The magic string
           is ``"CUBRS"`` when TLS is requested (``ssl=True`` / custom
           ``SSLContext``) and ``"CUBRK"`` when not.
        3. Read the broker status int32: ``0`` proceeds, ``> 0`` redirects to
           a new CAS worker port (reconnect without re-handshaking), ``< 0``
           raises :class:`OperationalError` immediately.
        4. If TLS was requested, upgrade the live socket via
           :func:`ssl.SSLContext.wrap_socket` *before* sending ``OPEN_DB``.
        5. Send ``OPEN_DATABASE`` and parse the session response.

        The ``ssl`` constructor argument accepts:

        - ``None`` / ``False`` — plaintext (default)
        - ``True`` — :func:`ssl.create_default_context` with
          ``minimum_version = TLSv1_2`` (see :mod:`pycubrid._connection_common`)
        - :class:`ssl.SSLContext` — caller-owned context used as-is

        Sync TLS uses blocking :meth:`ssl.SSLContext.wrap_socket`, which
        raises :class:`ssl.SSLCertVerificationError` synchronously on
        verification failure (unlike the async path on Python 3.10 — see
        `#156 <https://github.com/cubrid-lab/pycubrid/issues/156>`_).
        """
        with self._session_lock:
            self._connect_locked()

    def _connect_locked(self) -> None:
        if self._connected:
            return

        self._last_insert_id = None
        _timing = self._timing
        _start = 0
        if _timing is not None:
            _start = time.perf_counter_ns()
        handshake_socket: socket.socket | None = None
        try:
            use_ssl = self._ssl_context is not None
            handshake_socket = self._create_socket(self._host, self._port)
            client_info_packet = ClientInfoExchangePacket(use_ssl=use_ssl)
            handshake_socket.sendall(client_info_packet.write())
            handshake_response = self._recv_exact(handshake_socket, DataSize.INT)
            client_info_packet.parse(handshake_response)

            # Negative status from the broker indicates rejection (e.g. SSL
            # disabled on broker but CUBRS sent, or vice versa).  Fail fast
            # before any TLS upgrade or OPEN_DB so callers see a clear error.
            if client_info_packet.new_connection_port < 0:
                raise OperationalError(
                    f"CUBRID broker rejected handshake with status "
                    f"{client_info_packet.new_connection_port} (ssl={use_ssl})"
                )

            if client_info_packet.new_connection_port > 0:
                handshake_socket.close()
                handshake_socket = None
                # Per CUBRID JDBC BrokerHandler.connectBroker, the redirected
                # CAS worker does NOT expect a second CUBRK/CUBRS handshake —
                # the client reconnects to the new port and proceeds directly
                # to the TLS upgrade (if SSL) followed by OPEN_DATABASE.
                self._socket = self._create_socket(
                    self._host, client_info_packet.new_connection_port
                )
            else:
                self._socket = handshake_socket
                handshake_socket = None  # ownership transferred

            if use_ssl:
                self._socket = self._wrap_ssl(self._socket, self._host)

            open_db_packet = OpenDatabasePacket(
                database=self._database,
                user=self._user,
                password=self._password,
            )
            self._socket.sendall(open_db_packet.write())
            data_length_bytes = self._recv_exact(self._socket, DataSize.DATA_LENGTH)
            data_length = struct.unpack(">i", data_length_bytes)[0]
            self._validate_data_length(data_length)
            response_body = self._recv_exact(self._socket, data_length + DataSize.CAS_INFO)
            open_db_packet.parse(response_body)

            self._cas_info = open_db_packet.cas_info
            self._session_id = open_db_packet.session_id
            self._protocol_version = open_db_packet.broker_info.get("protocol_version", 1)
            self._statement_pooling = open_db_packet.broker_info.get("statement_pooling")
            self._connected = True
            self._verified_cas_info = self._cas_info
            self._physical_generation += 1
            if not self._no_backslash_escapes_explicit:
                self._no_backslash_escapes = None
            _LOGGER.debug(
                "Connected to %s:%d/%s (protocol_version=%d, tls=%s)",
                self._host,
                self._port,
                self._database,
                self._protocol_version,
                use_ssl,
            )
        except (OSError, ValueError, struct.error, IndexError, UnicodeDecodeError) as exc:
            _LOGGER.debug(
                "Connection failed to %s:%d/%s",
                self._host,
                self._port,
                self._database,
            )
            raise OperationalError("failed to connect to CUBRID broker") from exc
        finally:
            if handshake_socket is not None:
                try:
                    handshake_socket.close()
                except OSError:
                    pass
            if not self._connected:
                self._safe_close_socket()
            if _timing is not None:
                _timing.record_connect(time.perf_counter_ns() - _start)

        # Every newly opened physical session may have a different server
        # escape mode. Do not expose it to callers (or restore autocommit)
        # until this session has been probed. An explicit option remains pinned.
        try:
            self._negotiate_backslash_escapes()
        except BaseException:
            try:
                self._drop_connection()
            except BaseException:
                _LOGGER.warning(
                    "Failed to discard connection after escape-mode probe", exc_info=True
                )
            finally:
                self._connected = False
                self._invalidate_query_handles()
            raise

    def close(self) -> None:
        """Close the connection and all tracked cursors."""
        with self._session_lock:
            self._close_locked()

    def _close_locked(self) -> None:
        if not self._connected:
            return

        _LOGGER.debug("Closing connection to %s:%d/%s", self._host, self._port, self._database)

        _timing = self._timing
        _start = 0
        if _timing is not None:
            _start = time.perf_counter_ns()

        # Closing never reconnects: a CAS that already went away needs no
        # CLOSE_REQ or CLOSE_DATABASE, and failures here are best effort.
        self._implicit_reconnect_suspended += 1
        try:
            for cursor in list(self._cursors):
                try:
                    cursor.close()
                except Exception:  # nosec B110 — best-effort cursor cleanup
                    pass
                finally:
                    self._cursors.discard(cursor)

            self._close_schema_results()
            self._send_and_receive(CloseDatabasePacket())
        except Exception:  # nosec B110 — best-effort socket cleanup on close
            pass
        finally:
            self._implicit_reconnect_suspended -= 1
            self._safe_close_socket()
            self._connected = False
            self._statement_pooling = None
            if _timing is not None:
                _timing.record_close(time.perf_counter_ns() - _start)

    def _drop_connection(self) -> None:
        """Retire a physical session under the same lock as prepared sends."""
        with self._session_lock:
            super()._drop_connection()
            self._statement_pooling = None

    def commit(self) -> None:
        """Commit the current transaction."""
        self._ensure_connected()
        _LOGGER.debug("commit")
        with self._session_lock:
            # Verify an OUT_TRAN CAS first: a replaced session has no schema or
            # query handles left to close on the dead transport.
            self._check_reconnect()
            self._close_schema_results()
            self._close_open_query_handles()
            self._send_and_receive(CommitPacket())
            self._invalidate_query_handles()

    def rollback(self) -> None:
        """Roll back the current transaction."""
        self._ensure_connected()
        _LOGGER.debug("rollback")
        with self._session_lock:
            # Verify an OUT_TRAN CAS first: a replaced session has no schema or
            # query handles left to close on the dead transport.
            self._check_reconnect()
            self._close_schema_results()
            self._close_open_query_handles()
            self._send_and_receive(RollbackPacket())
            self._invalidate_query_handles()

    def _close_open_query_handles(self) -> None:
        """Send CLOSE_REQ for query handles still held by tracked cursors (#485).

        The CAS session survives END_TRAN, so a handle that is only forgotten
        locally stays allocated in the CAS until disconnect. Buffered rows and
        delivered/advertised counts are kept, so an unfinished result still
        raises ``InterfaceError`` at its next required FETCH (#395). A native
        CLOSE_REQ error is ignored; a transport failure is raised, since the
        transaction boundary itself can no longer be delivered.
        """
        # Read each handle only when its turn comes: a probe-verified reconnect
        # during an earlier CLOSE_REQ invalidates every remaining handle.
        for cursor in list(self._cursors):
            handle = cursor._query_handle
            if handle is None:
                continue
            cursor._query_handle = None
            try:
                self._send_and_receive(CloseQueryPacket(handle))
            except Error:
                if not self._connected:
                    raise
                _LOGGER.debug("CLOSE_REQ for handle %d failed", handle, exc_info=True)

    def _check_reconnect(self, *, allow_reconnect: bool = True) -> bool:
        """Probe an OUT_TRAN CAS with CHECK_CAS and reconnect only if it is gone.

        CAS_INFO status 0 means out of transaction; normally the CAS keeps this
        physical session, and its settings, across END_TRAN (#468). The CAS may
        still close the socket after replying (memory restart, broker reset,
        CHANGE CLIENT), so before the next request a CHECK_CAS probe verifies it
        (JDBC ``UClientSideConnection.checkReconnect`` parity, #485). Only a
        failed probe replaces the session: once per request, restoring
        driver-owned state and never replaying SQL. Returns ``True`` when the
        session was replaced.
        """
        self._ensure_connected()
        if self._socket is None or not self._needs_cas_probe(allow_reconnect):
            return False
        try:
            probe = self._send_and_receive_locked(
                CheckCasPacket(), allow_reconnect=False, expected_generation=None
            )
            if probe.response_code >= 0:
                self._verified_cas_info = self._cas_info
                return False
            _LOGGER.debug("CHECK_CAS returned %d", probe.response_code)
        except (Error, OSError, struct.error) as exc:
            # No answer: the CAS closed or reset this socket after OUT_TRAN.
            _LOGGER.debug("CHECK_CAS got no answer: %r", exc)
        self._reconnect_after_failed_probe()
        return True

    def _reconnect_after_failed_probe(self) -> None:
        """Replace a CAS session that failed its OUT_TRAN probe, exactly once."""
        _LOGGER.debug(
            "CAS did not answer CHECK_CAS out of transaction; reconnecting to %s:%d",
            self._host,
            self._port,
        )
        escape_mode = self._no_backslash_escapes
        self._drop_connection()
        self._invalidate_query_handles_for_reconnect()
        self._implicit_reconnect_suspended += 1
        try:
            self.connect()
            self._restore_session_state()
            # Setup may itself end OUT_TRAN (the escape probe's rollback), and
            # that CAS may be recycled too: verify it before the pending request.
            if self._cas_status_unverified():
                probe = self._send_and_receive_locked(
                    CheckCasPacket(), allow_reconnect=False, expected_generation=None
                )
                if probe.response_code < 0:
                    raise OperationalError("replacement CAS session failed CHECK_CAS")
                self._verified_cas_info = self._cas_info
        except BaseException as exc:
            self._drop_connection()
            if isinstance(exc, Exception):
                raise OperationalError(
                    "CAS did not answer CHECK_CAS out of transaction and reconnecting failed"
                ) from exc
            raise
        finally:
            self._implicit_reconnect_suspended -= 1
        self._check_replacement_escape_mode(escape_mode)

    def _restore_session_state(self) -> None:
        """Re-emit session-level settings after explicit ping recovery.

        Re-applies any session state that the caller has explicitly set
        on this connection (currently only ``autocommit``).  Settings the
        caller has never touched are left at the broker default to
        avoid spurious round-trips on reconnect.

        On failure the connection is torn down and the original cause is
        chained via ``raise ... from exc`` so the caller can diagnose
        the restore failure. **Any** exception raised by
        ``_send_and_receive`` — including parse-layer errors such as
        ``ValueError``, ``struct.error``, ``IndexError``, or
        ``UnicodeDecodeError`` — is treated as a restore failure so the
        connection is never left in a half-restored state.
        """
        if not self._autocommit_explicitly_set:
            return
        try:
            self._send_and_receive(
                SetDbParameterPacket(
                    parameter=CCIDbParam.AUTO_COMMIT,
                    value=1 if self._autocommit else 0,
                ),
                allow_reconnect=False,
            )
        except Exception as exc:
            self._drop_connection()
            raise OperationalError("failed to restore session state after reconnect") from exc

    def cursor(self) -> Cursor:
        """Create and return a new cursor bound to this connection."""
        self._ensure_connected()
        global _CursorClass  # noqa: PLW0603
        if _CursorClass is None:
            _CursorClass = getattr(import_module("pycubrid.cursor"), "Cursor")
        cls = _CursorClass
        assert cls is not None
        cursor = cls(self)
        self._cursors.add(cursor)
        return cursor

    @property
    def autocommit(self) -> bool:
        """Return the current auto-commit mode."""
        self._ensure_connected()
        return self._autocommit

    @autocommit.setter
    def autocommit(self, value: bool) -> None:
        """Set auto-commit mode and flush transaction state on the server."""
        self._ensure_connected()
        enabled = bool(value)
        self._close_schema_results()
        self._send_and_receive(
            SetDbParameterPacket(
                parameter=CCIDbParam.AUTO_COMMIT,
                value=1 if enabled else 0,
            )
        )
        self._send_and_receive(CommitPacket())
        self._autocommit = enabled
        self._autocommit_explicitly_set = True
        _LOGGER.debug("autocommit=%s", enabled)

    def get_server_version(self) -> str:
        """Return the server engine version string."""
        self._ensure_connected()
        packet = self._send_and_receive(GetEngineVersionPacket(auto_commit=self._autocommit))
        version: str = packet.engine_version
        return version

    def get_last_insert_id(self) -> str | None:
        """Return the cached broker-reported auto-increment id as a string.

        Captured after INSERT, corresponding to the cursor's integer
        ``lastrowid`` snapshot, without querying the broker here. Commit,
        rollback, and non-INSERT statements preserve this observation; it
        does not prove that a row still exists or that the latest INSERT
        generated an identity. The broker may retain an earlier identity
        after an INSERT into a table without an auto-increment column.
        Only cursor operations with an INSERT server response refresh this
        snapshot; CALL, stored-procedure INSERTs, and out-of-band SQL do not.

        Returns:
            The captured id as a string, or ``None`` when unavailable.
            A new INSERT attempt or nonempty batch clears the previous id,
            as does discarding or replacing the physical connection.
        """
        self._ensure_connected()
        return self._last_insert_id

    def ping(self, reconnect: bool = True) -> bool:
        """Check if the CAS broker connection is alive.

        Uses the native ``CHECK_CAS`` function code (FC=32) which
        performs a lightweight network-level ping without executing SQL.

        Args:
            reconnect: If ``True`` and the connection is dead, attempt
                to reconnect before returning ``False``.

        Returns:
            ``True`` if the connection is alive, ``False`` otherwise.

        Contract: reconnect+session-restore is attempted **at most once**
        per ``ping()`` call. A restore failure tears the connection down
        and returns ``False`` rather than retrying.
        """
        with self._session_lock:
            return self._ping_locked(reconnect)

    def _ping_locked(self, reconnect: bool) -> bool:
        if not self._connected:
            if not reconnect:
                return False
            try:
                self._invalidate_query_handles_for_reconnect()
                _LOGGER.debug("ping: reconnecting")
                self.connect()
                self._restore_session_state()
                return True
            except (OSError, OperationalError, InterfaceError):
                return False
        # OUT_TRAN is not a release signal. Probe this socket before deciding
        # whether an explicit recovery attempt is needed.
        try:
            packet = self._send_and_receive(CheckCasPacket(), allow_reconnect=False)
            healthy = packet.response_code >= 0
        except (InterfaceError, OperationalError, OSError, struct.error):
            healthy = False
        if healthy:
            self._verified_cas_info = self._cas_info
            return True
        if not reconnect:
            return False
        try:
            self._drop_connection()
            self._invalidate_query_handles_for_reconnect()
            _LOGGER.debug("ping: reconnecting after CHECK_CAS failure")
            self.connect()
            self._restore_session_state()
            return True
        except (OSError, OperationalError, InterfaceError):
            return False

    def create_lob(self, lob_type: int) -> Any:
        """Create a new LOB object on the server."""
        self._ensure_connected()
        from .lob import Lob

        return Lob.create(self, lob_type)

    def get_schema_info(
        self,
        schema_type: int,
        table_name: str = "",
        pattern_match_flag: int = 1,
        *,
        arg2: str | None = None,
    ) -> GetSchemaPacket:
        """Create an owned schema result; consume or explicitly close its packet."""
        self._ensure_connected()
        packet = GetSchemaPacket(
            schema_type=schema_type,
            table_name=table_name,
            pattern_match_flag=pattern_match_flag,
            arg2=arg2,
            protocol_version=self._protocol_version,
        )
        try:
            self._send_and_receive(packet)
        except BaseException:
            # A failed FC9 may already have allocated a handle whose metadata
            # could not be parsed. Never reuse that uncertain CAS session.
            self._drop_connection()
            raise
        self._register_schema_result(packet)
        return packet

    def fetch_schema_info(self, packet: GetSchemaPacket) -> list[tuple[Any, ...]]:
        """Eagerly read all schema rows and release their original CAS handle."""
        result = self._active_schema_result(packet)
        rows: list[tuple[Any, ...]] = []
        try:
            while len(rows) < result.count:
                fetched = FetchPacket(
                    result.handle,
                    len(rows),
                    self._fetch_size,
                    columns=result.columns,
                    decode_collections=self._decode_collections,
                    json_deserializer=self._json_deserializer,
                )
                try:
                    self._send_and_receive(fetched, allow_reconnect=False)
                except OperationalError as exc:
                    if exc.code == 0:
                        # Driver-local framing/transport failures may leave an
                        # unread reply. Native errors have a negative CAS code.
                        self._drop_connection()
                    raise
                if (
                    not fetched.rows
                    or fetched.tuple_count != len(fetched.rows)
                    or len(rows) + len(fetched.rows) > result.count
                ):
                    raise OperationalError("inconsistent schema FETCH row count")
                rows.extend(fetched.rows)
        except BaseException as exc:
            if not isinstance(exc, Exception):
                self._drop_connection()
                raise
            try:
                self.close_schema_info(packet)
            except Exception:
                _LOGGER.warning("Failed to close schema result after FETCH failure", exc_info=True)
            raise
        self.close_schema_info(packet)
        return rows

    def close_schema_info(self, packet: GetSchemaPacket) -> None:
        """Abandon an owned schema result; closing a retired result is a no-op."""
        result = self._owned_schema_result(packet)
        if result is None:
            return
        try:
            self._send_and_receive(CloseQueryPacket(result.handle), allow_reconnect=False)
        except BaseException:
            self._drop_connection()
            raise
        self._schema_results.pop(packet, None)

    def _close_schema_results(self) -> None:
        for packet in list(self._schema_results):
            self.close_schema_info(packet)

    def __enter__(self) -> Connection:
        """Enter context manager scope and return this connection."""
        self._ensure_connected()
        return self

    def __exit__(self, *args: Any) -> None:
        """Commit on success, rollback on exception, then close the connection."""
        exc_type = args[0]
        try:
            if exc_type is None:
                self.commit()
            else:
                self.rollback()
        finally:
            self.close()

    def _create_socket(self, host: str, port: int) -> socket.socket:
        sock = socket.create_connection(
            (host, port),
            timeout=self._connect_timeout,
        )
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        if self._read_timeout is not None:
            sock.settimeout(self._read_timeout)
        elif self._connect_timeout is not None:
            sock.settimeout(None)
        return sock

    def _wrap_ssl(self, sock: socket.socket, host: str) -> socket.socket:
        """Upgrade a plaintext CAS socket to TLS after the CUBRS handshake.

        CUBRID uses STARTTLS-style negotiation: the 10-byte handshake with
        the ``CUBRS`` magic is sent in plaintext, and only on a ``0`` reply
        is the same socket wrapped in TLS.  Wrapping earlier (TLS from
        byte 0) is rejected by the broker.
        """
        ssl_context = self._ssl_context
        if ssl_context is None:
            return sock
        try:
            return ssl_context.wrap_socket(sock, server_hostname=host)
        except (OSError, ssl_module.SSLError):
            try:
                sock.close()
            except OSError:
                pass
            raise

    def _send_and_receive(
        self,
        packet: Any,
        *,
        allow_reconnect: bool = True,
        expected_generation: int | None = None,
    ) -> Any:
        """Send a framed CAS request and parse the framed response into ``packet``.

        CAS_INFO status 0 is OUT_TRAN, not a release signal. Keep the physical
        session after normal transaction boundaries; do not replay arbitrary
        requests on a different session after an uncertain transport failure.
        """
        with self._session_lock:
            return self._send_and_receive_locked(
                packet,
                allow_reconnect=allow_reconnect,
                expected_generation=expected_generation,
            )

    def _send_and_receive_locked(
        self,
        packet: Any,
        *,
        allow_reconnect: bool,
        expected_generation: int | None,
    ) -> Any:
        self._validate_prepared_generation(expected_generation)
        if self._check_reconnect(
            allow_reconnect=allow_reconnect
        ) and self._skip_request_after_reconnect(packet):
            return packet
        if self._socket is None:
            raise InterfaceError("connection is closed")
        if (
            self._schema_results
            and isinstance(
                packet, (PrepareAndExecutePacket, BatchExecutePacket, GetEngineVersionPacket)
            )
            and packet.auto_commit
        ):
            self._close_schema_results()
            # FC6 replies OUT_TRAN; verify the CAS again before auto-committing.
            if self._check_reconnect(
                allow_reconnect=allow_reconnect
            ) and self._skip_request_after_reconnect(packet):
                return packet
            if self._socket is None:
                raise InterfaceError("connection is closed")

        request_socket = self._socket
        attempted_send = False
        response_complete = False
        try:
            try:
                request_data = packet.write(self._cas_info)
            except struct.error as exc:
                raise DataError("parameter value too large to serialize into CAS request") from exc
            # RLock permits a same-thread packet.write() callback to reconnect.
            # Re-check immediately before bytes can leave this socket.
            self._validate_prepared_session(expected_generation, request_socket)
            attempted_send = True
            request_socket.sendall(request_data)
            self._validate_prepared_session(expected_generation, request_socket)
            if _LOGGER.isEnabledFor(logging.DEBUG):
                _LOGGER.debug("send: %d bytes", len(request_data))

            try:
                data_length_bytes = self._recv_exact(request_socket, DataSize.DATA_LENGTH)
                data_length = struct.unpack(">i", data_length_bytes)[0]
                self._validate_data_length(data_length)
                response_body = self._recv_exact(request_socket, data_length + DataSize.CAS_INFO)
            except OperationalError:
                # Incomplete framing leaves the next response boundary unknown.
                # Server-reported packet.parse errors do not retire a valid CAS.
                if expected_generation is None:
                    self._drop_connection()
                raise

            self._validate_prepared_session(expected_generation, request_socket)
            response_complete = True
            response_cas_info = response_body[: DataSize.CAS_INFO]
            if expected_generation is None:
                self._cas_info = response_cas_info

            try:
                packet.parse(response_body)
            except (ValueError, struct.error, IndexError, UnicodeDecodeError) as exc:
                if expected_generation is None:
                    self._safe_close_socket()
                    self._connected = False
                elif self._prepared_session_is_current(expected_generation, request_socket):
                    self._discard_uncertain_prepared_session()
                raise OperationalError("malformed response from broker") from exc
            except Error as exc:
                if expected_generation is None:
                    raise
                if getattr(exc, "_cas_server_error", False):
                    self._validate_prepared_session(expected_generation, request_socket)
                    self._cas_info = response_cas_info
                    raise
                if self._prepared_session_is_current(expected_generation, request_socket):
                    self._discard_uncertain_prepared_session()
                raise OperationalError("malformed response from broker") from exc
            except Exception as exc:
                if expected_generation is None:
                    raise  # Preserve the ordinary parser's existing contract.
                if self._prepared_session_is_current(expected_generation, request_socket):
                    self._discard_uncertain_prepared_session()
                raise OperationalError("malformed response from broker") from exc
            self._validate_prepared_session(expected_generation, request_socket)
            if expected_generation is not None:
                self._cas_info = response_cas_info
            if _LOGGER.isEnabledFor(logging.DEBUG):
                _LOGGER.debug("recv: %d bytes", data_length + DataSize.CAS_INFO)
            return packet
        except OSError as exc:
            if expected_generation is not None and not attempted_send:
                raise  # Local pre-byte failure cannot corrupt the broker reply.
            if expected_generation is None:
                self._safe_close_socket()
                self._connected = False
            elif self._prepared_session_is_current(expected_generation, request_socket):
                self._discard_uncertain_prepared_session()
            raise OperationalError("socket communication failed") from exc
        # Fail closed for any BaseException subclass after an attempted send.
        # codeql[py/catch-base-exception]
        except BaseException as exc:
            if (
                expected_generation is not None
                and attempted_send
                and (not response_complete or not isinstance(exc, Exception))
                and self._prepared_session_is_current(expected_generation, request_socket)
            ):
                self._discard_uncertain_prepared_session()
            raise

    def _prepared_session_is_current(self, expected: int, request_socket: socket.socket) -> bool:
        return (
            self._connected
            and expected == self._physical_generation
            and self._socket is request_socket
        )

    def _validate_prepared_session(
        self, expected: int | None, request_socket: socket.socket
    ) -> None:
        self._validate_prepared_generation(expected)
        if expected is not None and not self._prepared_session_is_current(expected, request_socket):
            raise OperationalError("prepared physical session changed during request")

    def _discard_uncertain_prepared_session(self) -> None:
        """Best-effort retirement without replacing the request's primary error."""
        try:
            self._drop_connection()
        # Preserve the primary request failure even for nonstandard BaseException.
        # codeql[py/catch-base-exception]
        except BaseException:
            _LOGGER.warning("Failed to discard uncertain prepared session", exc_info=True)
            self._connected = False
            try:
                self._safe_close_socket()
            # codeql[py/catch-base-exception]
            except BaseException:
                _LOGGER.warning("Failed to close uncertain prepared socket", exc_info=True)
            try:
                self._invalidate_query_handles()
            # codeql[py/catch-base-exception]
            except BaseException:
                _LOGGER.warning(
                    "Failed to invalidate cursors after prepared failure", exc_info=True
                )

    def _validate_prepared_generation(self, expected: int | None) -> None:
        if expected is None:
            return
        if type(expected) is not int:
            raise InterfaceError("invalid prepared session generation")
        if expected != self._physical_generation:
            raise OperationalError("prepared handle belongs to an earlier physical session")

    def _recv_exact(self, sock: socket.socket, size: int) -> bytearray:
        """Receive exactly ``size`` bytes from the socket."""
        buf = bytearray(size)
        view = memoryview(buf)
        pos = 0
        while pos < size:
            n = sock.recv_into(view[pos:], size - pos)
            if n == 0:
                raise OperationalError("connection lost during receive")
            pos += n
        return buf
