"""Async connection implementation for pycubrid."""

from __future__ import annotations

import asyncio
import logging
import socket
import ssl as ssl_module
import struct
import time
from typing import Any

from pycubrid._connection_common import ConnectionCommonMixin
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
        if ssl is not None and ssl is not False:
            from pycubrid.exceptions import NotSupportedError

            raise NotSupportedError(
                "SSL/TLS is not yet supported for async connections. "
                "Use the sync pycubrid.connect(ssl=...) interface for TLS, "
                "or use async without encryption."
            )
        self._ssl_context: ssl_module.SSLContext | None = None
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

    async def connect(self) -> None:
        """Establish a TCP CAS session with broker handshake and open database."""
        if self._connected:
            return

        _timing = self._timing
        _start = 0
        if _timing is not None:
            _start = time.perf_counter_ns()
        handshake_socket: socket.socket | None = None
        try:
            loop = asyncio.get_running_loop()

            handshake_socket = await self._create_socket_nonblocking(self._host, self._port)

            client_info_packet = ClientInfoExchangePacket()
            await loop.sock_sendall(handshake_socket, client_info_packet.write())
            handshake_response = await self._recv_exact_async(loop, handshake_socket, DataSize.INT)
            client_info_packet.parse(handshake_response)

            if client_info_packet.new_connection_port > 0:
                handshake_socket.close()
                handshake_socket = None
                self._socket = await self._create_socket_nonblocking(
                    self._host, client_info_packet.new_connection_port
                )
            else:
                self._socket = handshake_socket
                handshake_socket = None  # ownership transferred

            open_db_packet = OpenDatabasePacket(
                database=self._database,
                user=self._user,
                password=self._password,
            )
            await loop.sock_sendall(self._socket, open_db_packet.write())
            data_length_bytes = await self._recv_exact_async(
                loop, self._socket, DataSize.DATA_LENGTH
            )
            data_length = struct.unpack(">i", data_length_bytes)[0]
            response_body = await self._recv_exact_async(
                loop, self._socket, data_length + DataSize.CAS_INFO
            )
            open_db_packet.parse(response_body)

            self._cas_info = open_db_packet.cas_info
            self._session_id = open_db_packet.session_id
            self._protocol_version = open_db_packet.broker_info.get("protocol_version", 1)
            self._connected = True
        except (OSError, ValueError, struct.error, IndexError, UnicodeDecodeError) as exc:
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

        try:
            await self._send_and_receive(CloseDatabasePacket())
        except Exception:  # noqa: BLE001 - best-effort cleanup
            _LOGGER.debug(
                "Suppressed error sending CloseDatabasePacket during shutdown", exc_info=True
            )
        finally:
            self._safe_close_socket()
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
                self._drop_connection()
                _LOGGER.debug("ping: reconnecting")
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

    async def _create_socket_nonblocking(self, host: str, port: int) -> socket.socket:
        infos = socket.getaddrinfo(host, port, socket.AF_UNSPEC, socket.SOCK_STREAM)
        if not infos:
            raise OperationalError(f"could not resolve address: {host}:{port}")

        last_exc: Exception | None = None
        for af, socktype, proto, _canonname, sa in infos:
            sock = socket.socket(af, socktype, proto)
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            sock.setblocking(False)
            try:
                loop = asyncio.get_running_loop()
                coro = loop.sock_connect(sock, sa)
                if self._connect_timeout is not None:
                    await asyncio.wait_for(coro, timeout=self._connect_timeout)
                else:
                    await coro
                return sock
            except (OSError, asyncio.TimeoutError) as exc:
                last_exc = exc
                sock.close()

        raise OperationalError(f"could not connect to {host}:{port}") from last_exc

    async def _send_and_receive(self, packet: Any, *, allow_reconnect: bool = True) -> Any:
        await self._check_reconnect(allow_reconnect=allow_reconnect)
        if self._socket is None:
            raise InterfaceError("connection is closed")

        loop = asyncio.get_running_loop()
        try:
            coro = self._do_send_and_receive(loop, packet)
            if self._read_timeout is not None:
                return await asyncio.wait_for(coro, timeout=self._read_timeout)
            return await coro
        except asyncio.TimeoutError:
            self._safe_close_socket()
            self._connected = False
            raise OperationalError("read timeout") from None
        except OSError as exc:
            self._safe_close_socket()
            self._connected = False
            raise OperationalError("socket communication failed") from exc

    async def _do_send_and_receive(self, loop: asyncio.AbstractEventLoop, packet: Any) -> Any:
        sock = self._socket
        if sock is None:
            raise InterfaceError("connection is closed")
        request_data = packet.write(self._cas_info)
        await loop.sock_sendall(sock, request_data)

        data_length_bytes = await self._recv_exact_async(loop, sock, DataSize.DATA_LENGTH)
        data_length = struct.unpack(">i", data_length_bytes)[0]
        response_body = await self._recv_exact_async(loop, sock, data_length + DataSize.CAS_INFO)

        self._cas_info = response_body[: DataSize.CAS_INFO]
        packet.parse(response_body)
        return packet

    async def _recv_exact_async(
        self,
        loop: asyncio.AbstractEventLoop,
        sock: socket.socket,
        size: int,
    ) -> bytearray:
        """Receive exactly *size* bytes from a non-blocking socket."""
        buf = bytearray(size)
        view = memoryview(buf)
        pos = 0
        while pos < size:
            n = await loop.sock_recv_into(sock, view[pos:])
            if n == 0:
                raise OperationalError("connection lost during receive")
            pos += n
        return buf

    async def _check_reconnect(self, *, allow_reconnect: bool = True) -> None:
        self._ensure_connected()
        if not allow_reconnect:
            return
        if self._cas_info[0] == self._CAS_INFO_STATUS_INACTIVE and self._socket is not None:
            self._drop_connection()
            await self.connect()
