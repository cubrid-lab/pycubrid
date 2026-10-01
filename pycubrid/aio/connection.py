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

from pycubrid._connection_common import (
    ConnectionCommonMixin,
    resolve_ssl_context,
    warn_unknown_connection_options,
)
from pycubrid.constants import CCIDbParam, DataSize
from pycubrid.exceptions import (
    DatabaseError,
    DataError,
    Error,
    InterfaceError,
    NotSupportedError,
    OperationalError,
)
from pycubrid.protocol import (
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

    - ``create_lob()`` raises :class:`~pycubrid.exceptions.NotSupportedError`;
      async LOB support is not implemented.
    - Autocommit changes go through :meth:`set_autocommit` (coroutine)
      rather than a property setter.
    - :meth:`ping` (added in 1.3.2) performs native ``CHECK_CAS`` and
      accepts ``reconnect=True/False`` to recover from a confirmed
      CAS/transport failure. OUT_TRAN alone never reconnects.

    .. note::
       On Python 3.10, :meth:`asyncio.AbstractEventLoop.start_tls` has a
       known CPython bug (fixed in 3.13/3.14) that causes it to hang on
       certificate-verify failures instead of raising. As of #156, an
       automatic preflight blocking TLS handshake probe runs
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
            decode_collections=kwargs.get("decode_collections", False),
            json_deserializer=kwargs.get("json_deserializer"),
            no_backslash_escapes=kwargs.get("no_backslash_escapes", None),
            enable_timing=kwargs.get("enable_timing"),
            charset=kwargs.get("charset", "utf-8"),
        )
        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None
        # Whether the current request's reply was read in full (#556): an
        # exception after that comes from parsing, not from the transport.
        self._reply_complete = False
        self._lock = asyncio.Lock()
        # Applied in connect() (can't await a live SET_DB_PARAMETER round-trip
        # here — __init__ isn't a coroutine). See connect() below.
        self._pending_autocommit = autocommit
        # Setup readiness gate (issue #264): serialize first-time setup
        # (connect -> negotiate escapes -> apply autocommit) against public
        # operations without holding the non-reentrant _lock across the
        # cursor-based negotiation probe. Public _send_and_receive waits on
        # _setup_done; the task performing setup bypasses the wait so its own
        # probe query does not deadlock.
        self._setup_lock = asyncio.Lock()
        self._setup_done = asyncio.Event()
        self._setup_done.set()
        self._setup_owner: asyncio.Task[Any] | None = None
        self._setup_error: BaseException | None = None

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
        async with self._setup_lock:
            did_connect = False
            previous_generation = self._physical_generation
            try:
                # Read the connecting edge and run the handshake under _lock so
                # two concurrent connect() calls can't both see "not connected"
                # and both drive setup (PR #226 review).
                async with self._lock:
                    if self._connected:
                        return  # Healthy no-op must not close the setup gate.
                    did_connect = True
                    self._setup_owner = asyncio.current_task()
                    self._setup_error = None
                    self._setup_done.clear()
                    await self._connect_locked()

                # Match the sync driver's setup order exactly: negotiate the
                # backslash-escape mode BEFORE applying pending autocommit.
                # The probe is a read-only SELECT issued via a cursor; it runs
                # without holding _lock (the cursor acquires it) but is fenced
                # from other tasks by the setup gate. Auto-detection is
                # repeated for every new physical session (#471).
                if did_connect and self._no_backslash_escapes is None:
                    await self._negotiate_backslash_escapes()

                # Finish session settings before releasing the setup gate.
                # The escape probe ends with a ROLLBACK, so this session may be
                # OUT_TRAN and its CAS already recycled: verify it once before
                # sending a setting. A replacement is configured by
                # _reconnect_after_failed_probe_locked itself.
                async with self._lock:
                    sends_setting = self._pending_autocommit or (
                        bool(previous_generation) and self._autocommit_explicitly_set
                    )
                    configured_by_recovery = (
                        self._physical_generation != previous_generation
                        and self._configured_generation == self._physical_generation
                    )
                    if configured_by_recovery:
                        pass  # a recovery nested in the escape probe configured it
                    elif sends_setting and await self._check_reconnect_locked():
                        pass
                    elif self._pending_autocommit:
                        await self._apply_pending_autocommit_locked()
                    elif previous_generation:
                        await self._restore_session_state_locked()
                    self._configured_generation = self._physical_generation
            except BaseException as exc:
                if did_connect:
                    self._setup_error = exc
                    # A failed/cancelled probe leaves the new session unsafe.
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
            finally:
                if did_connect:
                    self._setup_owner = None
                    self._setup_done.set()

    async def _negotiate_backslash_escapes(self) -> None:
        """Async counterpart of
        :meth:`pycubrid.connection.Connection._negotiate_backslash_escapes`.

        Probes the live server with ``SELECT CHAR_LENGTH('\\')`` and pins
        ``self._no_backslash_escapes`` to ``True`` (literal mode, the CUBRID
        default) or ``False`` (backslash-escape processing on). A detection
        failure raises :class:`OperationalError` rather than guessing, since
        a wrong mode silently corrupts string escaping.
        """
        if self._no_backslash_escapes is not None:
            return
        # The probe SELECT runs in the default manual-commit mode, which opens
        # a driver-owned transaction before the constructor's autocommit
        # setting is applied. Roll it back on *every* exit path (success,
        # unexpected result, or probe error) so a freshly opened connection is
        # handed back with clean transaction state. Safe during setup: this
        # task owns the setup gate, so rollback() bypasses
        # _wait_for_setup_if_needed(). Passing no_backslash_escapes explicitly
        # skips the probe entirely (early return above) and opens no tx.
        try:
            try:
                cursor = self.cursor()
                try:
                    await cursor.execute("SELECT CHAR_LENGTH('\\\\')")
                    row = await cursor.fetchone()
                finally:
                    await cursor.close()
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
                await self.rollback()
            except Exception as rollback_exc:  # noqa: BLE001
                # Rolling back the probe transaction failed, so it may still be
                # open and the connection is in an unknown transaction state.
                # Close it so it can never be handed back to a pool, then surface
                # the failure — unless a probe error is already propagating, in
                # which case that original error is preserved.
                try:
                    await self.close()
                except Exception:  # nosec B110 — best-effort close after rollback failure
                    pass
                if not probe_failed:
                    raise OperationalError(
                        "Failed to roll back the CUBRID backslash-escape probe "
                        "transaction; the connection may be in an unknown "
                        "transaction state and has been closed. Pass "
                        "no_backslash_escapes explicitly to skip detection."
                    ) from rollback_exc

    async def _connect_locked(self) -> None:
        if self._connected:
            return

        if not self._no_backslash_escapes_explicit:
            self._no_backslash_escapes = None

        self._last_insert_id = None
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
            self._verified_cas_info = self._cas_info
            self._physical_generation += 1
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
        ``_autocommit_explicitly_set``). The public ``connect()`` setup path
        also runs during ping recovery, and uses this pending value before
        normal reconnect restore when an initial application failed.

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
            await self._retire_session_locked()
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
            encoding=self._encoding,
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
        self._statement_pooling = open_db_packet.broker_info.get("statement_pooling")
        self._broker_db_type = open_db_packet.broker_info.get("db_type")

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
            # start_tls() moved the transport onto an SSLProtocol, which does
            # not forward connection_lost to the stream protocol while still in
            # DO_HANDSHAKE (peer reset, ssl_handshake_timeout, or cancellation
            # by read_timeout). Its close future would then never resolve and
            # the cleanup's StreamWriter.wait_closed() would hang forever
            # (#513). abort() already released the socket; deliver the missing
            # notification (a no-op if asyncio delivers it too).
            protocol.connection_lost(None)
            raise

        if new_transport is None:
            old_transport.abort()
            protocol.connection_lost(None)  # see the except branch above (#513)
            raise OperationalError("TLS upgrade returned no transport")
        self._writer._transport = new_transport  # type: ignore[attr-defined]
        self._reader._transport = new_transport  # type: ignore[attr-defined]
        # The stream protocol was built for the plaintext transport and still
        # has _over_ssl = False, so its eof_received() returns True and
        # SSLProtocol logs "returning true from eof_received() has no effect
        # when using ssl" on every TLS peer close (#514). Record the upgrade as
        # StreamReaderProtocol._replace_transport() (3.11+) would.
        setattr(protocol, "_over_ssl", True)

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
        on any incoming connection), then performs a blocking TLS handshake
        over :meth:`ssl.SSLContext.wrap_bio` memory BIOs using the **same**
        ``SSLContext`` object and ``server_hostname=self._host`` as the real
        upgrade. The probe owns its socket throughout and always closes it. Any
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
            # Run the handshake over memory BIOs instead of wrap_socket(): on
            # Python 3.10, SSLSocket._create() takes over the fd and can raise
            # on a peer reset before the ClientHello without closing it, which
            # left the socket to the garbage collector (#535). This way the
            # probe keeps owning the socket and the finally below closes it.
            # handshake_timeout bounds the whole handshake, as it does for
            # wrap_socket(), not each socket operation.
            deadline = time.monotonic() + handshake_timeout
            incoming = ssl_module.MemoryBIO()
            outgoing = ssl_module.MemoryBIO()
            tls = ssl_context.wrap_bio(incoming, outgoing, server_hostname=host)

            def remaining_timeout() -> float:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError("TLS preflight probe handshake timed out") from None
                return remaining

            while True:
                try:
                    tls.do_handshake()
                except ssl_module.SSLWantReadError:
                    pending = outgoing.read()
                    if pending:
                        sock.settimeout(remaining_timeout())
                        sock.sendall(pending)
                    sock.settimeout(remaining_timeout())
                    data = sock.recv(16384)
                    if not data:
                        raise OSError("connection closed during TLS preflight probe")
                    incoming.write(data)
                else:
                    # The BIO may still hold the final handshake flight.
                    # Its send is required, unlike optional close_notify.
                    pending = outgoing.read()
                    sock.settimeout(remaining_timeout())
                    if pending:
                        sock.sendall(pending)
                    remaining_timeout()  # reject a late successful completion
                    break
            # Verification passed; close_notify is best effort but may not
            # extend the same total deadline.
            try:
                tls.unwrap()
            except ssl_module.SSLError:
                # Expected: with memory BIOs unwrap() wants the peer's reply.
                pass
            try:
                pending = outgoing.read()
                if pending:
                    sock.settimeout(remaining_timeout())
                    sock.sendall(pending)
            except OSError:
                # The peer may already be gone; verification has passed.
                pass
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

        # Closing never reconnects: a CAS that already went away needs no
        # CLOSE_REQ or CLOSE_DATABASE, and failures here are best effort.
        self._implicit_reconnect_suspended += 1
        try:
            for cursor in list(self._cursors):
                try:
                    await cursor.close()
                except Exception:  # noqa: BLE001 - best-effort cleanup
                    _LOGGER.debug(
                        "Suppressed error while closing cursor during shutdown", exc_info=True
                    )
                finally:
                    self._cursors.discard(cursor)

            await self._wait_for_setup_if_needed()
            async with self._lock:
                try:
                    if self._connected:
                        await self._close_schema_results_locked()
                        await self._send_and_receive_locked(
                            CloseDatabasePacket(), allow_reconnect=False
                        )
                except Exception:  # noqa: BLE001 - best-effort cleanup
                    _LOGGER.debug(
                        "Suppressed error sending CloseDatabasePacket during shutdown",
                        exc_info=True,
                    )
                finally:
                    await self._close_streams()
                    self._connected = False
                    if _timing is not None:
                        _timing.record_close(time.perf_counter_ns() - _start)
        finally:
            self._implicit_reconnect_suspended -= 1

    async def commit(self) -> None:
        """Commit the current transaction."""
        await self._wait_for_setup_if_needed()
        async with self._lock:
            self._ensure_connected()
            # Verify an OUT_TRAN CAS first: a replaced session has no schema or
            # query handles left to close on the dead transport.
            await self._check_reconnect_locked()
            await self._close_schema_results_locked()
            await self._close_open_query_handles_locked()
            await self._send_and_receive_locked(CommitPacket())
            self._invalidate_query_handles()

    async def rollback(self) -> None:
        """Roll back the current transaction."""
        await self._wait_for_setup_if_needed()
        async with self._lock:
            self._ensure_connected()
            # Verify an OUT_TRAN CAS first: a replaced session has no schema or
            # query handles left to close on the dead transport.
            await self._check_reconnect_locked()
            await self._close_schema_results_locked()
            await self._close_open_query_handles_locked()
            await self._send_and_receive_locked(RollbackPacket())
            self._invalidate_query_handles()

    async def _close_open_query_handles_locked(self) -> None:
        """Async counterpart of ``Connection._close_open_query_handles`` (#485)."""
        # Read each handle only when its turn comes: a probe-verified reconnect
        # during an earlier CLOSE_REQ invalidates every remaining handle.
        for cursor in list(self._cursors):
            handle = cursor._query_handle
            if handle is None:
                continue
            cursor._query_handle = None
            await self._close_handle_at_boundary_locked(handle)
        # Handles of cursors collected without close() and queued (#488).
        generation = self._physical_generation
        for handle in self._take_deferred_closes():
            if self._physical_generation != generation:
                break  # replaced during an earlier CLOSE_REQ: nothing left to close
            await self._close_handle_at_boundary_locked(handle)

    async def _close_handle_at_boundary_locked(self, handle: int) -> None:
        try:
            await self._send_and_receive_locked(CloseQueryPacket(handle))
        except Error:
            if not self._connected:
                raise
            _LOGGER.debug("CLOSE_REQ for handle %d failed", handle, exc_info=True)

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
        """Set auto-commit mode on the server.

        Same contract as the sync ``Connection.autocommit`` setter (#551):
        ``SET_DB_PARAMETER`` and its ``COMMIT`` take effect on one CAS session,
        a CAS recycled between them is replaced at most once with the new value
        restored before the ``COMMIT``, and a failed ``COMMIT`` retires the
        session, keeps the previous value and raises :class:`OperationalError`.
        """
        await self._wait_for_setup_if_needed()
        async with self._lock:
            self._ensure_connected()
            enabled = bool(value)
            generation = self._physical_generation
            await self._close_schema_results_locked()
            await self._send_and_receive_locked(
                SetDbParameterPacket(
                    parameter=CCIDbParam.AUTO_COMMIT,
                    value=1 if enabled else 0,
                ),
                allow_reconnect=self._physical_generation == generation,
            )
            previous = (self._autocommit, self._autocommit_explicitly_set)
            self._autocommit = enabled
            self._autocommit_explicitly_set = True
            try:
                await self._send_and_receive_locked(
                    CommitPacket(), allow_reconnect=self._physical_generation == generation
                )
            except BaseException as exc:
                self._autocommit, self._autocommit_explicitly_set = previous
                self._drop_connection()
                if isinstance(exc, Exception):
                    raise OperationalError("failed to commit the autocommit change") from exc
                raise

    async def get_server_version(self) -> str:
        self._ensure_connected()
        packet = await self._send_and_receive(GetEngineVersionPacket(auto_commit=self._autocommit))
        version: str = packet.engine_version
        return version

    async def get_last_insert_id(self) -> str | None:
        """Return the cached broker-reported auto-increment id as a string.

        Corresponds to the cursor's integer ``lastrowid`` snapshot, without
        querying the broker here. Commit/rollback and non-INSERT statements
        preserve the observation. A non-auto-increment INSERT may still
        report an earlier broker identity; this is not proof the latest
        INSERT generated an identity or that a row exists after rollback.
        Only cursor operations with an INSERT server response refresh this
        snapshot; CALL, stored-procedure INSERTs, and out-of-band SQL do not.

        Returns:
            The captured id as a string, or ``None`` when unavailable.
            INSERT attempts, nonempty batches, and physical connection
            changes clear the previous id; commit and rollback preserve it.
        """
        self._ensure_connected()
        return self._last_insert_id

    async def ping(self, reconnect: bool = True) -> bool:
        """Contract: reconnect+session-restore is attempted at most once per
        call.  A restore failure tears the connection down and returns
        ``False`` rather than retrying. Recovery uses the public connect()
        setup gate, which probes escape mode and restores settings before any
        other task may send SQL on the new physical session. Subclass overrides
        of connect() cannot bypass that recovery invariant."""
        while True:
            try:
                await self._wait_for_setup_if_needed()
            except (OSError, Error, struct.error):
                return False
            async with self._lock:
                if (
                    not self._setup_done.is_set()
                    and self._setup_owner is not asyncio.current_task()
                ):
                    # Setup began after the outer wait. Release _lock and wait;
                    # this is not evidence that the new session is broken.
                    continue
                if self._connected:
                    try:
                        packet = await self._send_and_receive_locked(
                            CheckCasPacket(), allow_reconnect=False
                        )
                        healthy = packet.response_code >= 0
                    except (OSError, Error, struct.error):
                        healthy = False
                    if healthy:
                        self._verified_cas_info = self._cas_info
                        return True
                elif not reconnect:
                    return False
                await self._retire_session_locked(for_reconnect=True)
                if not reconnect:
                    return False
            break

        try:
            _LOGGER.debug("ping: recovering through a fully configured connection")
            # An override of connect() can establish transport without the
            # escape-mode probe or session restore. Recovery must run the base
            # setup protocol; its _connect_locked hook remains overridable.
            await AsyncConnection.connect(self)
            return True
        except (OSError, Error):
            return False

    def create_lob(self, lob_type: int) -> Any:
        """Reject LOB creation on async connections.

        Async LOB support is not implemented: :class:`~pycubrid.Lob` drives its
        connection synchronously, so it cannot operate over an
        :class:`AsyncConnection`. This method exists to make the unsupported
        behavior fail loudly and discoverably instead of raising
        ``AttributeError``.
        """
        raise NotSupportedError(
            "LOB operations are not supported on async connections; "
            "async LOB support is not implemented"
        )

    async def get_schema_info(
        self,
        schema_type: int,
        table_name: str = "",
        pattern_match_flag: int = 1,
        *,
        arg2: str | None = None,
    ) -> GetSchemaPacket:
        """Create an owned schema result; consume or explicitly close its packet."""
        await self._wait_for_setup_if_needed()
        async with self._lock:
            self._ensure_connected()
            # An unencodable argument fails here, before the request can drop
            # the session below (#86).
            self._check_encodable("schema argument", table_name)
            self._check_encodable("schema argument", arg2)
            packet = GetSchemaPacket(
                schema_type=schema_type,
                table_name=table_name,
                pattern_match_flag=pattern_match_flag,
                arg2=arg2,
                protocol_version=self._protocol_version,
            )
            try:
                await self._send_and_receive_locked(packet)
            except BaseException:
                self._drop_connection()
                raise
            self._register_schema_result(packet)
            return packet

    async def fetch_schema_info(self, packet: GetSchemaPacket) -> list[tuple[Any, ...]]:
        """Eagerly read schema rows and close within one connection-lock scope."""
        await self._wait_for_setup_if_needed()
        async with self._lock:
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
                        await self._send_and_receive_locked(fetched, allow_reconnect=False)
                    except OperationalError as exc:
                        if exc.code == 0:
                            # An unread/malformed frame is not a native error:
                            # discard it instead of writing FC6 on this stream.
                            self._drop_connection()
                        raise
                    if (
                        not fetched.rows
                        or fetched.tuple_count != len(fetched.rows)
                        or len(rows) + len(fetched.rows) > result.count
                    ):
                        raise OperationalError("inconsistent schema FETCH row count")
                    rows.extend(fetched.rows)
            except asyncio.CancelledError:
                # The response may still be unread: FC6 here would desynchronize
                # the stream. Retire synchronously before propagating cancellation.
                self._drop_connection()
                raise
            except BaseException as exc:
                if not isinstance(exc, Exception):
                    self._drop_connection()
                    raise
                try:
                    await self._close_schema_info_locked(packet)
                except Exception:
                    _LOGGER.warning(
                        "Failed to close schema result after FETCH failure", exc_info=True
                    )
                raise
            await self._close_schema_info_locked(packet)
            return rows

    async def close_schema_info(self, packet: GetSchemaPacket) -> None:
        """Abandon an owned result; a same-owner retired result is a no-op."""
        await self._wait_for_setup_if_needed()
        async with self._lock:
            await self._close_schema_info_locked(packet)

    async def _close_schema_info_locked(self, packet: GetSchemaPacket) -> None:
        result = self._owned_schema_result(packet)
        if result is None:
            return
        try:
            await self._send_and_receive_locked(
                CloseQueryPacket(result.handle), allow_reconnect=False
            )
        except BaseException:
            self._drop_connection()
            raise
        self._schema_results.pop(packet, None)

    async def _close_schema_results_locked(self) -> None:
        for packet in list(self._schema_results):
            await self._close_schema_info_locked(packet)

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

    async def _wait_for_setup_if_needed(self) -> None:
        """Block public operations until first-time setup finishes (#264).

        While ``connect()`` runs its ``connect -> negotiate -> autocommit``
        sequence, ``_setup_done`` is cleared so a query on another task cannot
        execute before the backslash-escape mode is pinned. The task that owns
        setup bypasses the wait so its own cursor-based probe does not deadlock
        against itself. If setup failed, the recorded error is re-raised here
        so waiters do not proceed against a half-initialized connection.

        Each waiter raises its own exception (#554): the recorded error belongs
        to the setup owner, so re-raising that one instance would cancel every
        waiter when the owner is cancelled and append their frames to a shared
        traceback. A cancelled or interrupted setup becomes
        :class:`OperationalError`; a pycubrid error is copied with its class
        (or the nearest :mod:`pycubrid.exceptions` class when a subclass
        constructor differs), code, errno and sqlstate; any other error is wrapped in
        :class:`OperationalError`. Non-cancellation originals are chained as
        ``__cause__``. A waiter's own cancellation still propagates unchanged.
        """
        if self._setup_owner is asyncio.current_task():
            return
        if not self._setup_done.is_set():
            await self._setup_done.wait()
            error = self._setup_error
            if error is None:
                return
            if not isinstance(error, Exception):
                raise OperationalError(
                    "connection setup was cancelled or interrupted in another task; retry operation"
                ) from None
            if isinstance(error, Error):
                raise self._copy_setup_error(error) from error
            raise OperationalError(f"connection setup failed in another task: {error!r}") from error

    @staticmethod
    def _copy_setup_error(error: Error) -> Error:
        """Build a fresh instance of a setup owner's pycubrid error (#554).

        A subclass whose constructor differs from ``Error``/``DatabaseError``
        is rebuilt as the nearest class defined in :mod:`pycubrid.exceptions`.
        """
        msg = getattr(error, "msg", str(error))
        code = getattr(error, "code", 0)
        errno = getattr(error, "errno", None)
        sqlstate = getattr(error, "sqlstate", None)
        for cls in type(error).__mro__:
            if not issubclass(cls, Error):
                break
            if cls is not type(error) and cls.__module__ != Error.__module__:
                continue
            try:
                if issubclass(cls, DatabaseError):
                    return cls(msg, code, errno, sqlstate)
                return cls(msg, code)
            except Exception:  # noqa: BLE001 - fall back to a pycubrid base class
                _LOGGER.debug("Cannot rebuild setup error as %s", cls.__name__, exc_info=True)
        return Error(msg, code)  # pragma: no cover - the loop always reaches Error

    async def _send_and_receive(
        self,
        packet: Any,
        *,
        allow_reconnect: bool = True,
        expected_escape_generation: int | None = None,
        handle_owner: Any = None,
    ) -> Any:
        await self._wait_for_setup_if_needed()
        async with self._lock:
            if handle_owner is not None and handle_owner._query_handle != packet.query_handle:
                # Another task's commit/rollback or reconnect released this
                # handle while the request waited; its id may name a new result.
                return self._stale_handle_request(packet, handle_owner)
            if expected_escape_generation is None:
                return await self._send_and_receive_locked(packet, allow_reconnect=allow_reconnect)
            return await self._send_and_receive_locked(
                packet,
                allow_reconnect=allow_reconnect,
                expected_escape_generation=expected_escape_generation,
            )

    @staticmethod
    def _stale_handle_request(packet: Any, owner: Any) -> Any:
        """Resolve a cursor's FETCH/CLOSE_REQ for a handle it no longer owns (#485)."""
        if isinstance(packet, CloseQueryPacket):
            return packet  # Already released by the boundary or the reconnect.
        if owner._invalidated_by_reconnect:
            raise OperationalError(
                "result set lost due to broker reconnect mid-fetch; "
                "re-execute the query to continue"
            )
        raise InterfaceError(
            "result set invalidated before all rows were fetched; re-execute the query to continue"
        )

    def _validate_escape_generation(self, expected: int | None) -> None:
        if expected is not None and (
            expected != self._physical_generation or self._no_backslash_escapes is None
        ):
            raise OperationalError("CAS session replaced after parameter binding; retry operation")

    async def _send_and_receive_locked(
        self,
        packet: Any,
        *,
        allow_reconnect: bool = True,
        expected_escape_generation: int | None = None,
    ) -> Any:
        if not self._setup_done.is_set() and self._setup_owner is not asyncio.current_task():
            # A caller may have passed the outer gate before recovery began.
            # Never send its prebuilt SQL while the new mode is unverified.
            raise OperationalError("connection setup in progress; retry operation")
        self._validate_escape_generation(expected_escape_generation)
        if await self._preflight_locked(packet, allow_reconnect):
            return packet
        if self._writer is None or self._reader is None:
            raise InterfaceError("connection is closed")
        self._validate_escape_generation(expected_escape_generation)
        if (
            self._schema_results
            and isinstance(
                packet, (PrepareAndExecutePacket, BatchExecutePacket, GetEngineVersionPacket)
            )
            and packet.auto_commit
        ):
            await self._close_schema_results_locked()
            # FC6 replies OUT_TRAN; verify the CAS again before auto-committing.
            if await self._preflight_locked(packet, allow_reconnect):
                return packet
            if self._writer is None or self._reader is None:
                raise InterfaceError("connection is closed")
            self._validate_escape_generation(expected_escape_generation)

        # On Python 3.11+ asyncio.TimeoutError is the built-in TimeoutError, an
        # OSError subclass a transport or a parse callback can raise too
        # (ETIMEDOUT): record whether one came from inside the round trip
        # instead of inferring the read_timeout deadline from its type.
        transport_timeout = False
        self._reply_complete = False

        async def round_trip() -> Any:
            nonlocal transport_timeout
            try:
                return await self._do_send_and_receive(packet)
            except (TimeoutError, asyncio.TimeoutError):
                transport_timeout = True
                raise

        try:
            if self._read_timeout is not None:
                return await asyncio.wait_for(round_trip(), timeout=self._read_timeout)
            return await round_trip()
        except (asyncio.TimeoutError, OSError) as exc:
            deadline = (
                self._read_timeout is not None
                and isinstance(exc, asyncio.TimeoutError)
                and not transport_timeout
            )
            if self._reply_complete and not deadline:
                # Raised by a parse callback (json_deserializer) after the whole
                # reply was read: not a transport failure, the session is intact.
                raise
            await self._retire_session_locked()
            if deadline:
                raise OperationalError(
                    "read timeout: no complete round trip within "
                    f"read_timeout={self._read_timeout}s"
                ) from exc
            if isinstance(exc, (TimeoutError, asyncio.TimeoutError)):
                raise OperationalError("socket communication timed out") from exc
            raise OperationalError("socket communication failed") from exc
        except asyncio.CancelledError:
            # The reply may arrive after cancellation and poison the next read.
            self._drop_connection()
            raise

    async def _do_send_and_receive(self, packet: Any) -> Any:
        writer = self._writer
        reader = self._reader
        if writer is None or reader is None:
            raise InterfaceError("connection is closed")
        # Every request on this connection uses its charset (#86); encoding
        # happens in write(), so an unencodable value sends nothing.
        packet.encoding = self._encoding
        deferred_count = 0
        if isinstance(packet, PrepareAndExecutePacket):
            # Release queued handles of this session with this request (#488).
            deferred_count, packet.deferred_close_handles = self._peek_deferred_closes()
        try:
            request_data = packet.write(self._cas_info)
        except struct.error as exc:
            raise DataError("parameter value too large to serialize into CAS request") from exc
        # Sent (or uncertain, which retires the session): never send them again.
        self._consume_deferred_closes(deferred_count)
        writer.write(request_data)
        await writer.drain()

        try:
            data_length_bytes = await self._recv_exact(reader, DataSize.DATA_LENGTH)
            data_length = struct.unpack(">i", data_length_bytes)[0]
            self._validate_data_length(data_length)
            response_body = await self._recv_exact(reader, data_length + DataSize.CAS_INFO)
        except OperationalError:
            # A partial frame is not a usable stream. Parse-layer server
            # errors below remain separate and do not close a healthy session.
            self._drop_connection()
            raise

        self._cas_info = response_body[: DataSize.CAS_INFO]
        self._reply_complete = True
        try:
            packet.parse(response_body)
        except (ValueError, struct.error, IndexError, UnicodeDecodeError) as exc:
            # The session is uncertain again: a deadline or transport error
            # while it shuts down is not a parse callback's exception.
            self._reply_complete = False
            await self._retire_session_locked()
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

    async def _preflight_locked(self, packet: Any, allow_reconnect: bool) -> bool:
        """Run the OUT_TRAN reconnect check; return whether to skip the request.

        SQL bound under the replaced generation keeps that generation, so the
        caller's ``_validate_escape_generation`` rejects it before send (#471).
        """
        if not await self._check_reconnect_locked(allow_reconnect=allow_reconnect):
            return False
        return self._skip_request_after_reconnect(packet)

    async def _generation_for_binding(self) -> int:
        """Verify an OUT_TRAN CAS before SQL is bound, then return its generation.

        Probing (and, if needed, reconnecting) before binding lets the literals
        be rendered for the session they will be sent on; the send-time
        generation fence then only rejects a replacement that raced the bind.
        """
        await self._wait_for_setup_if_needed()
        async with self._lock:
            await self._check_reconnect_locked()
            return self._physical_generation

    async def _check_reconnect(self, *, allow_reconnect: bool = True) -> bool:
        async with self._lock:
            return await self._check_reconnect_locked(allow_reconnect=allow_reconnect)

    async def _check_reconnect_locked(self, *, allow_reconnect: bool = True) -> bool:
        """Async counterpart of ``Connection._check_reconnect`` (#485).

        Probes an OUT_TRAN CAS with CHECK_CAS; only a failed probe replaces the
        session, once per request, under this same lock hold so no other task
        can observe the new session before its setup is complete.
        """
        self._ensure_connected()
        if self._writer is None or not self._needs_cas_probe(allow_reconnect):
            return False
        try:
            probe = await self._send_and_receive_locked(CheckCasPacket(), allow_reconnect=False)
            if probe.response_code >= 0:
                self._verified_cas_info = self._cas_info
                return False
            _LOGGER.debug("CHECK_CAS returned %d", probe.response_code)
        except (Error, OSError, struct.error) as exc:
            # No answer: the CAS closed or reset this socket after OUT_TRAN.
            _LOGGER.debug("CHECK_CAS got no answer: %r", exc)
        await self._reconnect_after_failed_probe_locked()
        return True

    async def _reconnect_after_failed_probe_locked(self) -> None:
        """Replace a CAS session that failed its OUT_TRAN probe, exactly once.

        Runs the same setup as ``connect()`` (handshake, escape-mode probe,
        autocommit restore) while ``self._lock`` stays held; the public
        ``connect()`` cannot be used here because it takes that lock.
        """
        _LOGGER.debug(
            "CAS did not answer CHECK_CAS out of transaction; reconnecting to %s:%d",
            self._host,
            self._port,
        )
        await self._retire_session_locked(for_reconnect=True)
        self._implicit_reconnect_suspended += 1
        try:
            await self._connect_locked()
            await self._negotiate_backslash_escapes_locked()
            if self._pending_autocommit:
                await self._apply_pending_autocommit_locked()
            else:
                await self._restore_session_state_locked()
            # Setup may itself end OUT_TRAN (the escape probe's rollback), and
            # that CAS may be recycled too: verify it before the pending request.
            if self._cas_status_unverified():
                probe = await self._send_and_receive_locked(CheckCasPacket(), allow_reconnect=False)
                if probe.response_code < 0:
                    raise OperationalError("replacement CAS session failed CHECK_CAS")
                self._verified_cas_info = self._cas_info
            self._configured_generation = self._physical_generation
        except BaseException as exc:
            self._drop_connection()
            if isinstance(exc, Exception):
                raise OperationalError(
                    "CAS did not answer CHECK_CAS out of transaction and reconnecting failed"
                ) from exc
            raise
        finally:
            self._implicit_reconnect_suspended -= 1

    async def _negotiate_backslash_escapes_locked(self) -> None:
        """Lock-held twin of :meth:`_negotiate_backslash_escapes`.

        Sends the same ``CHAR_LENGTH`` probe through ``_send_and_receive_locked``
        instead of a cursor, then closes its handle and rolls back.
        """
        if self._no_backslash_escapes is not None:
            return
        try:
            # Same request as the cursor-based probe of connect(), in both
            # drivers: it carries the connection's autocommit flag.
            probe = PrepareAndExecutePacket(
                sql="SELECT CHAR_LENGTH('\\\\')",
                auto_commit=self._autocommit,
                protocol_version=self._protocol_version,
            )
            await self._send_and_receive_locked(probe, allow_reconnect=False)
            await self._send_and_receive_locked(
                CloseQueryPacket(probe.query_handle), allow_reconnect=False
            )
            await self._send_and_receive_locked(RollbackPacket(), allow_reconnect=False)
        except Exception as exc:  # noqa: BLE001 — re-raised as OperationalError
            raise OperationalError(
                "Failed to detect CUBRID backslash-escape mode; refusing to "
                "guess because a wrong mode silently corrupts string escaping. "
                "Pass no_backslash_escapes explicitly to skip detection."
            ) from exc
        length = probe.rows[0][0] if probe.rows else None
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

    async def _restore_session_state_locked(self) -> None:
        """Re-emit explicit session settings after explicit ping recovery.

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
            await self._retire_session_locked()
            raise OperationalError("failed to restore session state after reconnect") from exc

    async def _invoke_connect_locked(self) -> None:
        connect_method = self.connect
        if getattr(connect_method, "__func__", None) is AsyncConnection.connect:
            await self._connect_locked()
            return
        await connect_method()

    async def _retire_session_locked(self, *, for_reconnect: bool = False) -> None:
        """Retire the physical session after an uncertain I/O failure (#556).

        Connection state and every cursor/schema handle are retired *before*
        the stream shutdown is awaited, so a ``wait_closed()`` that fails or is
        cancelled cannot leave live-looking handles on a dead session. A
        shutdown failure is logged rather than replacing the caller's error;
        cancellation still propagates as :class:`asyncio.CancelledError`.
        """
        self._connected = False
        if for_reconnect:
            self._invalidate_query_handles_for_reconnect()
        else:
            self._invalidate_query_handles()
        try:
            await self._close_streams()
        except Exception:  # noqa: BLE001 - the session is already retired
            _LOGGER.debug("Stream shutdown failed while retiring the session", exc_info=True)

    async def _close_streams(self) -> None:
        """Close the stream writer, await TLS shutdown, and clear references."""
        self._last_insert_id = None
        self._schema_results.clear()
        self._deferred_closes.clear()
        self._statement_pooling = None
        self._broker_db_type = None
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
        self._last_insert_id = None
        self._schema_results.clear()
        self._deferred_closes.clear()
        self._statement_pooling = None
        self._broker_db_type = None
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
