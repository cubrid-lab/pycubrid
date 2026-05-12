from __future__ import annotations

import logging
import socket
import ssl as ssl_module
import struct
import time
from importlib import import_module
from typing import TYPE_CHECKING, Any

from ._connection_common import ConnectionCommonMixin, resolve_ssl_context
from .constants import CCIDbParam, DataSize
from .exceptions import InterfaceError, OperationalError
from .protocol import (
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
            no_backslash_escapes=kwargs.get("no_backslash_escapes", False),
            enable_timing=kwargs.get("enable_timing"),
        )

        self.connect()
        if autocommit:
            self.autocommit = True

    def connect(self) -> None:
        """Establish a TCP CAS session with broker handshake and open database."""
        if self._connected:
            return

        _timing = self._timing
        _start = 0
        if _timing is not None:
            _start = time.perf_counter_ns()
        handshake_socket: socket.socket | None = None
        try:
            handshake_socket = self._create_socket(self._host, self._port)
            client_info_packet = ClientInfoExchangePacket()
            handshake_socket.sendall(client_info_packet.write())
            handshake_response = self._recv_exact(handshake_socket, DataSize.INT)
            client_info_packet.parse(handshake_response)

            if client_info_packet.new_connection_port > 0:
                handshake_socket.close()
                handshake_socket = None
                self._socket = self._create_socket(
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
            self._socket.sendall(open_db_packet.write())
            data_length_bytes = self._recv_exact(self._socket, DataSize.DATA_LENGTH)
            data_length = struct.unpack(">i", data_length_bytes)[0]
            response_body = self._recv_exact(self._socket, data_length + DataSize.CAS_INFO)
            open_db_packet.parse(response_body)

            self._cas_info = open_db_packet.cas_info
            self._session_id = open_db_packet.session_id
            self._protocol_version = open_db_packet.broker_info.get("protocol_version", 1)
            self._connected = True
            _LOGGER.debug(
                "Connected to %s:%d/%s (protocol_version=%d)",
                self._host,
                self._port,
                self._database,
                self._protocol_version,
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

    def close(self) -> None:
        """Close the connection and all tracked cursors."""
        if not self._connected:
            return

        _LOGGER.debug("Closing connection to %s:%d/%s", self._host, self._port, self._database)

        _timing = self._timing
        _start = 0
        if _timing is not None:
            _start = time.perf_counter_ns()

        for cursor in list(self._cursors):
            try:
                cursor.close()
            except Exception:  # nosec B110 — best-effort cursor cleanup
                pass
            finally:
                self._cursors.discard(cursor)

        try:
            self._send_and_receive(CloseDatabasePacket())
        except Exception:  # nosec B110 — best-effort socket cleanup on close
            pass
        finally:
            self._safe_close_socket()
            self._connected = False
            if _timing is not None:
                _timing.record_close(time.perf_counter_ns() - _start)

    def commit(self) -> None:
        """Commit the current transaction."""
        self._ensure_connected()
        _LOGGER.debug("commit")
        self._send_and_receive(CommitPacket())
        self._invalidate_query_handles()

    def rollback(self) -> None:
        """Roll back the current transaction."""
        self._ensure_connected()
        _LOGGER.debug("rollback")
        self._send_and_receive(RollbackPacket())
        self._invalidate_query_handles()

    def _check_reconnect(self, *, allow_reconnect: bool = True) -> None:
        """Reconnect to the broker when the CAS has been released.

        The CUBRID broker sets the first byte of CAS_INFO to ``INACTIVE``
        (0) when the CAS process is no longer reserved for this client
        (``KEEP_CONNECTION=AUTO``).  The official JDBC driver checks this
        before every request and transparently reconnects.  This method
        replicates that behaviour so that ``commit()`` followed by a new
        query works without the caller having to manage reconnection.
        """
        self._ensure_connected()
        if not allow_reconnect:
            return
        if self._cas_info[0] == self._CAS_INFO_STATUS_INACTIVE and self._socket is not None:
            self._drop_connection()
            _LOGGER.debug("CAS inactive, reconnecting to %s:%d", self._host, self._port)
            self.connect()

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
        self._send_and_receive(
            SetDbParameterPacket(
                parameter=CCIDbParam.AUTO_COMMIT,
                value=1 if enabled else 0,
            )
        )
        self._send_and_receive(CommitPacket())
        self._autocommit = enabled
        _LOGGER.debug("autocommit=%s", enabled)

    def get_server_version(self) -> str:
        """Return the server engine version string."""
        self._ensure_connected()
        packet = self._send_and_receive(GetEngineVersionPacket(auto_commit=self._autocommit))
        version: str = packet.engine_version
        return version

    def get_last_insert_id(self) -> str:
        """Return last inserted auto-increment value as string."""
        self._ensure_connected()
        packet = self._send_and_receive(GetLastInsertIdPacket())
        result: str = packet.last_insert_id
        return result

    def ping(self, reconnect: bool = True) -> bool:
        """Check if the CAS broker connection is alive.

        Uses the native ``CHECK_CAS`` function code (FC=32) which
        performs a lightweight network-level ping without executing SQL.

        Args:
            reconnect: If ``True`` and the connection is dead, attempt
                to reconnect before returning ``False``.

        Returns:
            ``True`` if the connection is alive, ``False`` otherwise.
        """
        if not self._connected:
            if not reconnect:
                return False
            try:
                self._invalidate_query_handles()
                _LOGGER.debug("ping: reconnecting")
                self.connect()
                return True
            except (OSError, OperationalError, InterfaceError):
                return False
        try:
            packet = self._send_and_receive(CheckCasPacket(), allow_reconnect=reconnect)
            return bool(packet.response_code >= 0)
        except (InterfaceError, OperationalError, OSError, struct.error):
            if not reconnect:
                return False
            try:
                self._drop_connection()
                _LOGGER.debug("ping: reconnecting")
                self.connect()
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
    ) -> Any:
        """Query schema information from the server."""
        self._ensure_connected()
        packet = GetSchemaPacket(
            schema_type=schema_type,
            table_name=table_name,
            pattern_match_flag=pattern_match_flag,
        )
        self._send_and_receive(packet)
        return packet

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
        ssl_context = getattr(self, "_ssl_context", None)
        if ssl_context is not None:
            try:
                sock = ssl_context.wrap_socket(sock, server_hostname=host)
            except (OSError, ssl_module.SSLError):
                sock.close()
                raise
        return sock

    def _send_and_receive(self, packet: Any, *, allow_reconnect: bool = True) -> Any:
        """Send a framed CAS request and parse the framed response into ``packet``.

        After each response the CAS_INFO status byte is checked.  When the
        broker signals ``INACTIVE`` (the CAS process has been released), the
        driver closes the current socket and reconnects transparently before
        the *next* request — matching the behaviour of the official CUBRID
        JDBC driver (``UClientSideConnection.checkReconnect``).
        """
        self._check_reconnect(allow_reconnect=allow_reconnect)
        if self._socket is None:
            raise InterfaceError("connection is closed")

        try:
            request_data = packet.write(self._cas_info)
            self._socket.sendall(request_data)
            if _LOGGER.isEnabledFor(logging.DEBUG):
                _LOGGER.debug("send: %d bytes", len(request_data))

            data_length_bytes = self._recv_exact(self._socket, DataSize.DATA_LENGTH)
            data_length = struct.unpack(">i", data_length_bytes)[0]
            response_body = self._recv_exact(self._socket, data_length + DataSize.CAS_INFO)

            # Update CAS_INFO from the response (first 4 bytes).
            self._cas_info = response_body[: DataSize.CAS_INFO]

            packet.parse(response_body)
            if _LOGGER.isEnabledFor(logging.DEBUG):
                _LOGGER.debug("recv: %d bytes", data_length + DataSize.CAS_INFO)
            return packet
        except OSError as exc:
            self._safe_close_socket()
            self._connected = False
            raise OperationalError("socket communication failed") from exc

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
