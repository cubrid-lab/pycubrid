"""Async connection implementation for pycubrid."""

from __future__ import annotations

import asyncio
import logging
import socket
import ssl as ssl_module
import struct
import sys
import time
from typing import Any

from pycubrid._connection_common import ConnectionCommonMixin, resolve_ssl_context
from pycubrid.constants import CCIDbParam, DataSize
from pycubrid.exceptions import DataError, InterfaceError, OperationalError
from pycubrid.protocol import (
    CheckCasPacket,
    ClientInfoExchangePacket,
    CloseDatabasePacket,
    CommitPacket,
    GetEngineVersionPacket,
    GetLastInsertIdPacket,
    GetSchemaPacket,
    OpenDatabasePacket,
    RollbackPacket,
    SetDbParameterPacket,
)

_LOGGER = logging.getLogger(__name__)


class AsyncConnection(ConnectionCommonMixin):
    """Async connection to a CUBRID broker via the CAS protocol.

    Mirrors the sync :class:`pycubrid.Connection` surface using
    :mod:`asyncio` streams. Supports the same ``ssl`` parameter
    (``None`` / ``False`` / ``True`` / :class:`ssl.SSLContext`) and the
    same STARTTLS-style upgrade flow as the sync driver — plaintext
    ``CUBRS`` handshake, optional port redirect, then
    :meth:`asyncio.AbstractEventLoop.start_tls` before ``OPEN_DATABASE``.

    Differences vs sync :class:`pycubrid.Connection`:

    - ``create_lob()`` is **not** available on async connections.
    - Autocommit changes go through :meth:`set_autocommit` (coroutine)
      rather than a property setter.
    - :meth:`ping` (added in 1.3.2) performs native ``CHECK_CAS`` and
      accepts ``reconnect=True/False`` to suppress broker-handoff
      reconnect.

    .. note::
       On Python 3.10, :meth:`asyncio.AbstractEventLoop.start_tls` has a
       known CPython bug (fixed in 3.13/3.14) that causes it to hang on
       certificate-verify failures instead of raising. As of #156, an
       automatic preflight :meth:`ssl.SSLContext.wrap_socket` probe runs
       on Python 3.10 immediately before :meth:`_upgrade_to_tls` to
       surface verification failures as :class:`OperationalError`,
       matching the 3.11+ behavior. The probe is a no-op on Python
       3.11+. See :meth:`_maybe_probe_tls_verification` for the
       contract.
    """

    def __init__(
        self,
        host: str,
        port: int,
        database: str,
        user: str,
        password: str,
        ssl: bool | ssl_module.SSLContext | None = None,
        fetch_size: int = 100,
        *,
        autocommit: bool = False,
        **kwargs: Any,
    ) -> None:
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
            decode_collections=kwargs.get("decode_collections", False),
            json_deserializer=kwargs.get("json_deserializer"),
            no_backslash_escapes=kwargs.get("no_backslash_escapes", None),
            enable_timing=kwargs.get("enable_timing"),
        )
        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None
        self._lock = asyncio.Lock()
        # Applied in connect() (can't await a live SET_DB_PARAMETER round-trip
        # here — __init__ isn't a coroutine). See connect() below.
        self._pending_autocommit = autocommit

    async def connect(self) -> None:
        """Establish a TCP CAS session with broker handshake and open database.

        Runs :meth:`_do_connect_handshake` under the connection's
        :class:`asyncio.Lock` so concurrent ``await conn.connect()`` calls
        do not race. The handshake itself is bounded by ``read_timeout``
        (passed to :func:`asyncio.wait_for`); a timeout surfaces as
        :class:`OperationalError`.

        See :class:`AsyncConnection` for the ``ssl`` parameter semantics
        and the Python 3.10 caveat (`#156`_).

        .. _#156: https://github.com/cubrid-lab/pycubrid/issues/156
        """
        async with self._lock:
            # Read the connecting edge *inside* the lock so two concurrent
            # connect() calls can't both see "not connected" and both apply
            # autocommit (PR #226 review). Apply it in the same lock hold via
            # _send_and_receive_locked so no query on another task can slip
            # between the handshake and the SET_DB_PARAMETER round-trip.
            did_connect = not self._connected
            await self._connect_locked()
            if did_connect and self._pending_autocommit:
                await self._apply_pending_autocommit_locked()
        # Negotiate the backslash-escape mode outside the lock: it issues a
        # regular query round-trip via a cursor, and the guard keeps it to a
        # single probe across transparent reconnects.
        if self._no_backslash_escapes is None:
            await self._negotiate_backslash_escapes()

    async def _negotiate_backslash_escapes(self) -> None:
        """Async counterpart of
        :meth:`pycubrid.connection.Connection._negotiate_backslash_escapes`.

        Probes the live server with ``SELECT CHAR_LENGTH('\\')`` and pins
        ``self._no_backslash_escapes`` to ``True`` (literal mode, the CUBRID
        default), ``False`` (backslash-escape processing on), or the legacy
        ``False`` with a warning if detection fails.
        """
        if self._no_backslash_escapes is not None:
            return
        try:
            cursor = self.cursor()
            try:
                await cursor.execute("SELECT CHAR_LENGTH('\\\\')")
                row = await cursor.fetchone()
            finally:
                await cursor.close()
        except Exception as exc:  # noqa: BLE001 — detection must never break connect
            self._no_backslash_escapes = False
            _LOGGER.warning(
                "Failed to detect CUBRID backslash-escape mode (%s); "
                "falling back to no_backslash_escapes=False (legacy "
                "behaviour). Pass no_backslash_escapes explicitly to "
                "silence this warning.",
                exc,
            )
            return
        length = row[0] if row else None
        if length == 2:
            self._no_backslash_escapes = True
        elif length == 1:
            self._no_backslash_escapes = False
        else:
            self._no_backslash_escapes = False
            _LOGGER.warning(
                "Could not detect CUBRID backslash-escape mode "
                "(CHAR_LENGTH probe returned %r); falling back to "
                "no_backslash_escapes=False (legacy behaviour). Pass "
                "no_backslash_escapes explicitly to silence this warning.",
                length,
            )

    async def _connect_locked(self) -> None:
        if self._connected:
            return

        _timing = self._timing
        _start = 0
        if _timing is not None:
            _start = time.perf_counter_ns()
        hs_writer: asyncio.StreamWriter | None = None
        try:
            hs_reader, hs_writer = await self._open_connection(self._host, self._port)

            coro = self._do_connect_handshake(hs_reader, hs_writer)
            if self._read_timeout is not None:
                await asyncio.wait_for(coro, timeout=self._read_timeout)
            else:
                await coro
            hs_writer = None  # ownership transferred to self or closed

            self._connected = True
        except asyncio.TimeoutError as exc:
            raise OperationalError("read timeout during connect handshake") from exc
        except (OSError, ValueError, struct.error, IndexError, UnicodeDecodeError) as exc:
            raise OperationalError("failed to connect to CUBRID broker") from exc
        finally:
            if hs_writer is not None and hs_writer is not self._writer:
                try:
                    hs_writer.close()
                    await self._writer_wait_closed(hs_writer)
                except OSError:
                    pass
            if not self._connected:
                await self._close_streams()
            if _timing is not None:
                _timing.record_connect(time.perf_counter_ns() - _start)

    async def _apply_pending_autocommit_locked(self) -> None:
        """Apply the constructor's ``autocommit=True`` the first time
        ``connect()`` succeeds.  Must be called while ``self._lock`` is held,
        in the same hold that performed the connect.  Uses
        ``_send_and_receive_locked`` directly (like
        ``_restore_session_state_locked``) rather than the public
        ``set_autocommit`` — the latter would re-acquire ``self._lock`` and
        deadlock, and releasing the lock first would let a query on another
        task interleave before the SET_DB_PARAMETER round-trip completes
        (torn autocommit state).

        This bootstraps the explicit-autocommit state (``_autocommit`` +
        ``_autocommit_explicitly_set``), after which the normal reconnect
        restore machinery (``_restore_session_state_locked``) takes over — so
        it is intentionally driven only from the public ``connect()`` path,
        not the internal ``ping``/reconnect path, to avoid re-emitting on top
        of restore.

        ``self._pending_autocommit`` is cleared only on success, so a
        transient failure leaves it set for the next ``connect()`` attempt to
        retry, and — once cleared — a later reconnect will never re-apply the
        constructor's original intent over an explicit ``set_autocommit(False)``
        the caller made in between.
        """
        try:
            await self._send_and_receive_locked(
                SetDbParameterPacket(parameter=CCIDbParam.AUTO_COMMIT, value=1),
                allow_reconnect=False,
            )
            await self._send_and_receive_locked(CommitPacket(), allow_reconnect=False)
        except Exception as exc:
            await self._close_streams()
            self._connected = False
            raise OperationalError("failed to apply autocommit after connect") from exc
        self._autocommit = True
        self._autocommit_explicitly_set = True
        self._pending_autocommit = False

    async def _do_connect_handshake(
        self,
        hs_reader: asyncio.StreamReader,
        hs_writer: asyncio.StreamWriter,
    ) -> None:
        """Run broker handshake, optional redirect, TLS upgrade, and OPEN_DB exchange.

        Three-step STARTTLS-style flow, matching CUBRID JDBC's
        ``BrokerHandler.connectBroker``:

        1. **Plaintext handshake** — write the 10-byte
           :class:`ClientInfoExchangePacket` with magic ``"CUBRS"`` (TLS
           requested) or ``"CUBRK"`` (plaintext), then read the 4-byte
           broker status. ``< 0`` raises :class:`OperationalError`
           immediately; ``> 0`` closes the handshake stream and reopens
           on the redirected CAS worker port **without** repeating the
           handshake.
        2. **Optional TLS upgrade** — when ``ssl`` was supplied, hand off
           to :meth:`_upgrade_to_tls`, which calls
           :meth:`asyncio.AbstractEventLoop.start_tls` with
           ``ssl_handshake_timeout = read_timeout or 10.0`` so a stalled
           handshake cannot block the event loop indefinitely.
        3. **OPEN_DATABASE** — send :class:`OpenDatabasePacket` over the
           (now possibly TLS-wrapped) stream and parse the session
           response (``cas_info``, ``response_code``, ``broker_info``,
           ``session_id``).
        """
        use_ssl = self._ssl_context is not None
        client_info_packet = ClientInfoExchangePacket(use_ssl=use_ssl)
        hs_writer.write(client_info_packet.write())
        await hs_writer.drain()
        handshake_response = await self._recv_exact(hs_reader, DataSize.INT)
        client_info_packet.parse(handshake_response)

        if client_info_packet.new_connection_port < 0:
            raise OperationalError(
                f"CUBRID broker rejected handshake with status "
                f"{client_info_packet.new_connection_port} (ssl={use_ssl})"
            )

        if client_info_packet.new_connection_port > 0:
            hs_writer.close()
            await self._writer_wait_closed(hs_writer)
            # See sync connect(): CUBRID redirects the client to a fresh
            # CAS worker port without expecting a second handshake.
            self._reader, self._writer = await self._open_connection(
                self._host, client_info_packet.new_connection_port
            )
        else:
            self._reader = hs_reader
            self._writer = hs_writer

        if use_ssl:
            await self._maybe_probe_tls_verification(
                effective_port=(
                    client_info_packet.new_connection_port
                    if client_info_packet.new_connection_port > 0
                    else self._port
                ),
                followed_redirect=client_info_packet.new_connection_port > 0,
            )
            await self._upgrade_to_tls()

        open_db_packet = OpenDatabasePacket(
            database=self._database,
            user=self._user,
            password=self._password,
        )
        if self._writer is None:
            raise InterfaceError("Connection not established: writer is None")
        self._writer.write(open_db_packet.write())
        await self._writer.drain()
        data_length_bytes = await self._recv_exact(self._reader, DataSize.DATA_LENGTH)
        data_length = struct.unpack(">i", data_length_bytes)[0]
        self._validate_data_length(data_length)
        response_body = await self._recv_exact(self._reader, data_length + DataSize.CAS_INFO)
        open_db_packet.parse(response_body)

        self._cas_info = open_db_packet.cas_info
        self._session_id = open_db_packet.session_id
        self._protocol_version = open_db_packet.broker_info.get("protocol_version", 1)

    async def _upgrade_to_tls(self) -> None:
        """Upgrade the active stream from plaintext to TLS via ``loop.start_tls``.

        Uses ``loop.start_tls`` rather than ``StreamWriter.start_tls`` for
        Python 3.10 compatibility (the latter is 3.11+).  The new SSL
        transport is rebound onto the existing ``StreamReader``/
        ``StreamWriter`` via their private ``_transport`` attribute because
        the public alternatives (``StreamReader.set_transport`` + rebuilt
        ``StreamWriter``) desynchronise the shared ``StreamReaderProtocol``
        state and break subsequent reads after the upgrade.

        ``ssl_handshake_timeout`` is passed explicitly so a stalled
        handshake cannot block the event loop indefinitely, and the
        pre-TLS transport is ``abort()``ed on any failure so the next
        reconnect starts cleanly instead of leaking a half-upgraded SSL
        transport.  Note: this bounds peer-unresponsive hangs only.
        Python 3.10 has separate known issues with hangs during
        TLS-handshake-internal failures (the known CPython 3.10 async-TLS handshake bug, fixed
        in 3.13/3.14) that this kwarg does not address.
        """
        ssl_context = self._ssl_context
        if ssl_context is None:
            raise InterfaceError("SSL context not configured")
        if self._writer is None:
            raise InterfaceError("Connection not established: writer is None")
        if self._reader is None:
            raise InterfaceError("Connection not established: reader is None")

        loop = asyncio.get_running_loop()
        old_transport = self._writer.transport
        protocol = old_transport.get_protocol()
        handshake_timeout = float(self._read_timeout) if self._read_timeout is not None else 10.0

        try:
            new_transport = await loop.start_tls(
                old_transport,
                protocol,
                ssl_context,
                server_hostname=self._host,
                ssl_handshake_timeout=handshake_timeout,
            )
        except BaseException:
            old_transport.abort()
            raise

        if new_transport is None:
            old_transport.abort()
            raise OperationalError("TLS upgrade returned no transport")
        self._writer._transport = new_transport  # type: ignore[attr-defined]
        self._reader._transport = new_transport  # type: ignore[attr-defined]

    async def _maybe_probe_tls_verification(
        self, *, effective_port: int, followed_redirect: bool
    ) -> None:
        """Py3.10-only preflight TLS verify probe via :func:`run_in_executor`.

        Works around the known CPython 3.10 ``asyncio`` bug (gh-142352 family,
        fixed in 3.13/3.14) where :meth:`asyncio.AbstractEventLoop.start_tls`
        hangs indefinitely on TLS-handshake-internal verification failures
        (wrong CN, missing SAN, untrusted CA) instead of raising
        :class:`ssl.SSLCertVerificationError`. ``ssl_handshake_timeout``
        does **not** bound this hang because the peer responds normally and
        the failure happens inside CPython's TLS state machine.

        The probe opens a **separate** TCP socket to the same effective
        endpoint, replays the CUBRS broker handshake when needed
        (no-redirect path only — redirected CAS workers go straight to TLS
        on any incoming connection), then performs a synchronous
        :meth:`ssl.SSLContext.wrap_socket` using the **same** ``SSLContext``
        object and ``server_hostname=self._host`` as the real upgrade. Any
        :class:`ssl.SSLError` raised propagates as :class:`OSError`
        (``SSLError`` is an ``OSError`` subclass) into
        :meth:`_connect_locked`'s ``except`` clause, which wraps it as
        :class:`OperationalError` — matching the 3.11+ failure surface.

        The probe is a no-op on Python 3.11+, where ``start_tls`` already
        surfaces verification failures promptly. On the no-redirect path,
        the broker is contacted twice (probe + real handshake); on the
        redirect path, the redirected worker is contacted twice. This is
        accepted as the cost of working around the upstream 3.10 bug and
        is best-effort against cert rotation between probe and real
        upgrade.
        """
        if sys.version_info[:2] != (3, 10):
            return
        ssl_context = self._ssl_context
        if ssl_context is None:
            raise InterfaceError("SSL context not configured")

        connect_timeout = (
            float(self._connect_timeout) if self._connect_timeout is not None else None
        )
        handshake_timeout = float(self._read_timeout) if self._read_timeout is not None else 10.0
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(
            None,
            self._probe_tls_verification_sync,
            self._host,
            effective_port,
            ssl_context,
            not followed_redirect,
            connect_timeout,
            handshake_timeout,
        )

    @staticmethod
    def _probe_tls_verification_sync(
        host: str,
        port: int,
        ssl_context: ssl_module.SSLContext,
        needs_handshake_replay: bool,
        connect_timeout: float | None,
        handshake_timeout: float,
    ) -> None:
        """Synchronous TLS verification probe — runs in the default executor.

        Raises :class:`ssl.SSLError` (an :class:`OSError` subclass) on
        verification failure, :class:`OSError` on connect/IO errors.
        Both surface as :class:`OperationalError` via the caller chain.
        """
        sock: socket.socket | None = socket.create_connection((host, port), timeout=connect_timeout)
        try:
            if sock is None:
                raise OperationalError("Failed to create socket connection")
            sock.settimeout(handshake_timeout)
            if needs_handshake_replay:
                # Replay the plaintext CUBRS handshake so the broker
                # routes us identically to the real connection. If the
                # broker redirects, follow the redirect on a fresh socket
                # — the worker port is in TLS-expect mode and skips the
                # second handshake (matches the redirect logic above in
                # `_do_connect_handshake`).
                client_info = ClientInfoExchangePacket(use_ssl=True)
                sock.sendall(client_info.write())
                response = AsyncConnection._recv_exact_sync(sock, DataSize.INT)
                client_info.parse(response)
                if client_info.new_connection_port < 0:
                    # Broker rejected at handshake — not a TLS verification
                    # issue; let the real handshake surface the rejection.
                    return
                if client_info.new_connection_port > 0:
                    sock.close()
                    sock = socket.create_connection(
                        (host, client_info.new_connection_port), timeout=connect_timeout
                    )
                    sock.settimeout(handshake_timeout)
            ssock = ssl_context.wrap_socket(sock, server_hostname=host)
            sock = None  # ownership transferred to ssock
            try:
                # Handshake happens implicitly in wrap_socket on a blocking
                # socket; closing immediately suffices to validate the cert.
                try:
                    ssock.unwrap()
                except OSError:
                    # Best-effort TLS shutdown; verification already passed.
                    pass
            finally:
                ssock.close()
        finally:
            if sock is not None:
                try:
                    sock.close()
                except OSError:
                    pass

    @staticmethod
    def _recv_exact_sync(sock: socket.socket, size: int) -> bytes:
        """Receive exactly *size* bytes from a blocking socket or raise OSError."""
        buf = bytearray()
        while len(buf) < size:
            chunk = sock.recv(size - len(buf))
            if not chunk:
                raise OSError("connection closed during TLS preflight probe")
            buf.extend(chunk)
        return bytes(buf)

    async def close(self) -> None:
        """Close the connection and all tracked cursors."""
        if not self._connected:
            return

        _timing = self._timing
        _start = 0
        if _timing is not None:
            _start = time.perf_counter_ns()

        for cursor in list(self._cursors):
            try:
                await cursor.close()
            except Exception:  # noqa: BLE001 - best-effort cleanup
                _LOGGER.debug(
                    "Suppressed error while closing cursor during shutdown", exc_info=True
                )
            finally:
                self._cursors.discard(cursor)

        async with self._lock:
            try:
                if self._connected:
                    await self._send_and_receive_locked(
                        CloseDatabasePacket(), allow_reconnect=False
                    )
            except Exception:  # noqa: BLE001 - best-effort cleanup
                _LOGGER.debug(
                    "Suppressed error sending CloseDatabasePacket during shutdown", exc_info=True
                )
            finally:
                await self._close_streams()
                self._connected = False
                if _timing is not None:
                    _timing.record_close(time.perf_counter_ns() - _start)

    async def commit(self) -> None:
        """Commit the current transaction."""
        self._ensure_connected()
        await self._send_and_receive(CommitPacket())
        self._invalidate_query_handles()

    async def rollback(self) -> None:
        """Roll back the current transaction."""
        self._ensure_connected()
        await self._send_and_receive(RollbackPacket())
        self._invalidate_query_handles()

    def cursor(self) -> Any:
        """Create and return a new async cursor bound to this connection."""
        self._ensure_connected()
        from pycubrid.aio.cursor import AsyncCursor

        cur = AsyncCursor(self)
        self._cursors.add(cur)
        return cur

    @property
    def autocommit(self) -> bool:
        self._ensure_connected()
        return self._autocommit

    async def set_autocommit(self, value: bool) -> None:
        """Set auto-commit mode on the server."""
        self._ensure_connected()
        enabled = bool(value)
        await self._send_and_receive(
            SetDbParameterPacket(
                parameter=CCIDbParam.AUTO_COMMIT,
                value=1 if enabled else 0,
            )
        )
        await self._send_and_receive(CommitPacket())
        self._autocommit = enabled
        self._autocommit_explicitly_set = True

    async def get_server_version(self) -> str:
        self._ensure_connected()
        packet = await self._send_and_receive(GetEngineVersionPacket(auto_commit=self._autocommit))
        version: str = packet.engine_version
        return version

    async def get_last_insert_id(self) -> str:
        self._ensure_connected()
        packet = await self._send_and_receive(GetLastInsertIdPacket())
        last_id: str = packet.last_insert_id
        return last_id

    async def ping(self, reconnect: bool = True) -> bool:
        """Contract: reconnect+session-restore is attempted at most once per
        call.  A restore failure tears the connection down and returns
        ``False`` rather than retrying.  Reconnect and restore run under a
        single hold of ``self._lock`` so a concurrent task cannot observe
        an un-restored session between the two operations."""
        if not self._connected:
            if not reconnect:
                return False
            try:
                self._invalidate_query_handles_for_reconnect()
                _LOGGER.debug("ping: reconnecting")
                async with self._lock:
                    await self._invoke_connect_locked()
                    await self._restore_session_state_locked()
                return True
            except (OSError, OperationalError, InterfaceError):
                return False
        if reconnect:
            try:
                await self._check_reconnect(allow_reconnect=True)
            except (OSError, OperationalError, InterfaceError):
                return False
        try:
            packet = await self._send_and_receive(CheckCasPacket(), allow_reconnect=False)
            return bool(packet.response_code >= 0)
        except (OSError, InterfaceError, OperationalError, struct.error):
            if not reconnect:
                return False
            try:
                _LOGGER.debug("ping: reconnecting after CHECK_CAS failure")
                async with self._lock:
                    await self._close_streams()
                    self._connected = False
                    self._invalidate_query_handles_for_reconnect()
                    await self._invoke_connect_locked()
                    await self._restore_session_state_locked()
                return True
            except (OSError, OperationalError, InterfaceError):
                return False

    async def get_schema_info(
        self,
        schema_type: int,
        table_name: str = "",
        pattern_match_flag: int = 1,
    ) -> Any:
        self._ensure_connected()
        packet = GetSchemaPacket(
            schema_type=schema_type,
            table_name=table_name,
            pattern_match_flag=pattern_match_flag,
        )
        await self._send_and_receive(packet)
        return packet

    async def __aenter__(self) -> AsyncConnection:
        self._ensure_connected()
        return self

    async def __aexit__(self, *args: Any) -> None:
        exc_type = args[0]
        try:
            if exc_type is None:
                await self.commit()
            else:
                await self.rollback()
        finally:
            await self.close()

    # -- internal I/O --------------------------------------------------------

    async def _open_connection(
        self, host: str, port: int
    ) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        try:
            coro = asyncio.open_connection(host, port)
            if self._connect_timeout is not None:
                reader, writer = await asyncio.wait_for(coro, timeout=self._connect_timeout)
            else:
                reader, writer = await coro

            sock = writer.transport.get_extra_info("socket")
            if sock is not None:
                import socket as _socket_mod

                sock.setsockopt(_socket_mod.IPPROTO_TCP, _socket_mod.TCP_NODELAY, 1)
                sock.setsockopt(_socket_mod.SOL_SOCKET, _socket_mod.SO_KEEPALIVE, 1)

            return reader, writer
        except (OSError, asyncio.TimeoutError) as exc:
            raise OperationalError(f"could not connect to {host}:{port}") from exc

    async def _send_and_receive(self, packet: Any, *, allow_reconnect: bool = True) -> Any:
        async with self._lock:
            return await self._send_and_receive_locked(packet, allow_reconnect=allow_reconnect)

    async def _send_and_receive_locked(self, packet: Any, *, allow_reconnect: bool = True) -> Any:
        await self._check_reconnect_locked(allow_reconnect=allow_reconnect)
        if self._writer is None or self._reader is None:
            raise InterfaceError("connection is closed")

        try:
            coro = self._do_send_and_receive(packet)
            if self._read_timeout is not None:
                return await asyncio.wait_for(coro, timeout=self._read_timeout)
            return await coro
        except asyncio.TimeoutError as exc:
            await self._close_streams()
            self._connected = False
            raise OperationalError("read timeout") from exc
        except OSError as exc:
            await self._close_streams()
            self._connected = False
            raise OperationalError("socket communication failed") from exc

    async def _do_send_and_receive(self, packet: Any) -> Any:
        writer = self._writer
        reader = self._reader
        if writer is None or reader is None:
            raise InterfaceError("connection is closed")
        try:
            request_data = packet.write(self._cas_info)
        except struct.error as exc:
            raise DataError("parameter value too large to serialize into CAS request") from exc
        writer.write(request_data)
        await writer.drain()

        data_length_bytes = await self._recv_exact(reader, DataSize.DATA_LENGTH)
        data_length = struct.unpack(">i", data_length_bytes)[0]
        self._validate_data_length(data_length)
        response_body = await self._recv_exact(reader, data_length + DataSize.CAS_INFO)

        self._cas_info = response_body[: DataSize.CAS_INFO]
        try:
            packet.parse(response_body)
        except (ValueError, struct.error, IndexError, UnicodeDecodeError) as exc:
            await self._close_streams()
            self._connected = False
            raise OperationalError("malformed response from broker") from exc
        return packet

    async def _recv_exact(
        self,
        reader: asyncio.StreamReader,
        size: int,
    ) -> bytes:
        """Receive exactly *size* bytes from the stream."""
        try:
            return await reader.readexactly(size)
        except asyncio.IncompleteReadError as exc:
            raise OperationalError("connection lost during receive") from exc

    async def _check_reconnect(self, *, allow_reconnect: bool = True) -> None:
        async with self._lock:
            await self._check_reconnect_locked(allow_reconnect=allow_reconnect)

    async def _check_reconnect_locked(self, *, allow_reconnect: bool = True) -> None:
        self._ensure_connected()
        if not allow_reconnect:
            return
        if self._cas_info[0] == self._CAS_INFO_STATUS_INACTIVE and self._writer is not None:
            await self._close_streams()
            self._connected = False
            self._invalidate_query_handles_for_reconnect()
            await self._invoke_connect_locked()
            await self._restore_session_state_locked()

    async def _restore_session_state_locked(self) -> None:
        """Re-emit explicit session settings after a transparent reconnect.

        Must be called while ``self._lock`` is held.  Uses
        ``_send_and_receive_locked`` with ``allow_reconnect=False`` to
        avoid both the public lock (deadlock) and a recursive reconnect.

        **Any** exception raised by ``_send_and_receive_locked`` — including
        parse-layer errors such as ``ValueError``, ``struct.error``,
        ``IndexError``, or ``UnicodeDecodeError`` — is treated as a restore
        failure: the streams are closed, the connection is marked
        disconnected, and ``OperationalError`` is raised with the original
        cause chained via ``from exc``. This guarantees the connection is
        never left in a half-restored state, matching the sync
        ``Connection._restore_session_state`` contract.
        """
        if not self._autocommit_explicitly_set:
            return
        try:
            await self._send_and_receive_locked(
                SetDbParameterPacket(
                    parameter=CCIDbParam.AUTO_COMMIT,
                    value=1 if self._autocommit else 0,
                ),
                allow_reconnect=False,
            )
        except Exception as exc:
            await self._close_streams()
            self._connected = False
            raise OperationalError("failed to restore session state after reconnect") from exc

    async def _invoke_connect_locked(self) -> None:
        connect_method = self.connect
        if getattr(connect_method, "__func__", None) is AsyncConnection.connect:
            await self._connect_locked()
            return
        await connect_method()

    async def _close_streams(self) -> None:
        """Close the stream writer, await TLS shutdown, and clear references."""
        if self._writer is not None:
            try:
                self._writer.close()
                await self._writer.wait_closed()
            except OSError:
                pass
            finally:
                self._writer = None
                self._reader = None

    def _close_streams_sync(self) -> None:
        """Sync fallback for _close_streams (used by mixin's _safe_close_socket)."""
        if self._writer is not None:
            try:
                self._writer.close()
            except OSError:
                pass
            finally:
                self._writer = None
                self._reader = None

    def _safe_close_socket(self) -> None:
        """Override mixin to close streams instead of raw socket."""
        self._close_streams_sync()

    @staticmethod
    async def _writer_wait_closed(writer: asyncio.StreamWriter) -> None:
        try:
            await writer.wait_closed()
        except OSError:
            pass
