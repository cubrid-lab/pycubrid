"""Async connection implementation for pycubrid."""

from __future__ import annotations

import asyncio
import logging
import socket
import struct
import time
from typing import TYPE_CHECKING, Any

from pycubrid.constants import CCIDbParam, DataSize
from pycubrid.exceptions import InterfaceError, OperationalError
from pycubrid.protocol import (
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

if TYPE_CHECKING:
    from pycubrid.aio.cursor import AsyncCursor
    from pycubrid.timing import TimingStats

_LOGGER = logging.getLogger(__name__)


class AsyncConnection:
    """Async connection to a CUBRID broker via the CAS protocol."""

    _CAS_INFO_STATUS_INACTIVE: int = 0
    _CAS_INFO_STATUS_ACTIVE: int = 1

    def __init__(
        self,
        host: str,
        port: int,
        database: str,
        user: str,
        password: str,
        **kwargs: Any,
    ) -> None:
        self._host = host
        self._port = port
        self._database = database
        self._user = user
        self._password = password
        self._connect_timeout = kwargs.get("connect_timeout")

        self._timing: TimingStats | None = None
        _enable_timing = kwargs.get("enable_timing")
        if _enable_timing is None:
            import os

            _enable_timing = os.environ.get("PYCUBRID_ENABLE_TIMING", "").lower() in (
                "1",
                "true",
                "yes",
            )
        if _enable_timing:
            from pycubrid.timing import TimingStats as _TimingStats

            self._timing = _TimingStats()

        self._socket: socket.socket | None = None
        self._connected = False
        self._cas_info: bytes | bytearray = b"\x00\x00\x00\x00"
        self._session_id = 0
        self._autocommit = False
        self._cursors: set[AsyncCursor] = set()
        self._protocol_version: int = 1

    async def connect(self) -> None:
        """Establish a TCP CAS session with broker handshake and open database."""
        if self._connected:
            return

        _timing = self._timing
        _start = 0
        if _timing is not None:
            _start = time.perf_counter_ns()
        try:
            loop = asyncio.get_running_loop()

            handshake_socket = self._create_socket_nonblocking(self._host, self._port)
            await loop.sock_connect(handshake_socket, (self._host, self._port))

            client_info_packet = ClientInfoExchangePacket()
            await loop.sock_sendall(handshake_socket, client_info_packet.write())
            handshake_response = await self._recv_exact_async(loop, handshake_socket, DataSize.INT)
            client_info_packet.parse(handshake_response)

            if client_info_packet.new_connection_port > 0:
                handshake_socket.close()
                self._socket = self._create_socket_nonblocking(
                    self._host, client_info_packet.new_connection_port
                )
                await loop.sock_connect(
                    self._socket,
                    (self._host, client_info_packet.new_connection_port),
                )
            else:
                self._socket = handshake_socket

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
        except OSError as exc:
            self._safe_close_socket()
            raise OperationalError("failed to connect to CUBRID broker") from exc
        finally:
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

    def cursor(self) -> AsyncCursor:
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

    @property
    def timing_stats(self) -> TimingStats | None:
        return self._timing

    async def get_server_version(self) -> str:
        self._ensure_connected()
        packet = await self._send_and_receive(GetEngineVersionPacket(auto_commit=self._autocommit))
        return packet.engine_version

    async def get_last_insert_id(self) -> str:
        self._ensure_connected()
        packet = await self._send_and_receive(GetLastInsertIdPacket())
        return packet.last_insert_id

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

    def _create_socket_nonblocking(self, host: str, port: int) -> socket.socket:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        sock.setblocking(False)
        return sock

    async def _send_and_receive(self, packet: Any) -> Any:
        await self._check_reconnect()
        if self._socket is None:
            raise InterfaceError("connection is closed")

        loop = asyncio.get_running_loop()
        try:
            request_data = packet.write(self._cas_info)
            await loop.sock_sendall(self._socket, request_data)

            data_length_bytes = await self._recv_exact_async(
                loop, self._socket, DataSize.DATA_LENGTH
            )
            data_length = struct.unpack(">i", data_length_bytes)[0]
            response_body = await self._recv_exact_async(
                loop, self._socket, data_length + DataSize.CAS_INFO
            )

            self._cas_info = response_body[: DataSize.CAS_INFO]
            packet.parse(response_body)
            return packet
        except OSError as exc:
            self._safe_close_socket()
            self._connected = False
            raise OperationalError("socket communication failed") from exc

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

    async def _check_reconnect(self) -> None:
        self._ensure_connected()
        if self._cas_info[0] == self._CAS_INFO_STATUS_INACTIVE and self._socket is not None:
            self._safe_close_socket()
            self._connected = False
            self._invalidate_query_handles()
            await self.connect()

    def _invalidate_query_handles(self) -> None:
        for cursor in self._cursors:
            cursor._query_handle = None

    def _ensure_connected(self) -> None:
        if not self._connected:
            raise InterfaceError("connection is closed")

    def _safe_close_socket(self) -> None:
        if self._socket is not None:
            try:
                self._socket.close()
            finally:
                self._socket = None
