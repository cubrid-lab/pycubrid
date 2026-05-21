"""Async connection implementation for pycubrid."""

from __future__ import annotations

import asyncio
import logging
import ssl as ssl_module
import struct
import time
from typing import Any

from pycubrid._connection_common import ConnectionCommonMixin, resolve_ssl_context
from pycubrid.constants import CCIDbParam, DataSize
from pycubrid.exceptions import InterfaceError, OperationalError
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
    """Async connection to a CUBRID broker via the CAS protocol."""

    def __init__(
        self,
        host: str,
        port: int,
        database: str,
        user: str,
        password: str,
        ssl: bool | ssl_module.SSLContext | None = None,
        fetch_size: int = 100,
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
            no_backslash_escapes=kwargs.get("no_backslash_escapes", False),
            enable_timing=kwargs.get("enable_timing"),
        )
        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None
        self._lock = asyncio.Lock()

    async def connect(self) -> None:
        """Establish a TCP CAS session with broker handshake and open database."""
        async with self._lock:
            await self._connect_locked()

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
        except asyncio.TimeoutError:
            raise OperationalError("read timeout during connect handshake") from None
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

    async def _do_connect_handshake(
        self,
        hs_reader: asyncio.StreamReader,
        hs_writer: asyncio.StreamWriter,
    ) -> None:
        """Run broker handshake, optional redirect, TLS upgrade, and OPEN_DB exchange."""
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
            await self._upgrade_to_tls()

        open_db_packet = OpenDatabasePacket(
            database=self._database,
            user=self._user,
            password=self._password,
        )
        assert self._writer is not None
        self._writer.write(open_db_packet.write())
        await self._writer.drain()
        data_length_bytes = await self._recv_exact(self._reader, DataSize.DATA_LENGTH)
        data_length = struct.unpack(">i", data_length_bytes)[0]
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
        TLS-handshake-internal failures (CPython gh-142352 family, fixed
        in 3.13/3.14) that this kwarg does not address.
        """
        ssl_context = self._ssl_context
        assert ssl_context is not None
        assert self._writer is not None
        assert self._reader is not None

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
        if not self._connected:
            if not reconnect:
                return False
            try:
                self._invalidate_query_handles()
                _LOGGER.debug("ping: reconnecting")
                await self.connect()
                return True
            except (OSError, OperationalError, InterfaceError):
                return False
        try:
            packet = await self._send_and_receive(CheckCasPacket(), allow_reconnect=reconnect)
            return bool(packet.response_code >= 0)
        except (InterfaceError, OperationalError, struct.error):
            if not reconnect:
                return False
            try:
                _LOGGER.debug("ping: reconnecting")
                async with self._lock:
                    await self._close_streams()
                    self._connected = False
                    self._invalidate_query_handles()
                await self.connect()
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
        except asyncio.TimeoutError:
            await self._close_streams()
            self._connected = False
            raise OperationalError("read timeout") from None
        except OSError as exc:
            await self._close_streams()
            self._connected = False
            raise OperationalError("socket communication failed") from exc

    async def _do_send_and_receive(self, packet: Any) -> Any:
        writer = self._writer
        reader = self._reader
        if writer is None or reader is None:
            raise InterfaceError("connection is closed")
        request_data = packet.write(self._cas_info)
        writer.write(request_data)
        await writer.drain()

        data_length_bytes = await self._recv_exact(reader, DataSize.DATA_LENGTH)
        data_length = struct.unpack(">i", data_length_bytes)[0]
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
            self._invalidate_query_handles()
            await self._invoke_connect_locked()

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
