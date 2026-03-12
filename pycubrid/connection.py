from __future__ import annotations

import socket
import struct
from importlib import import_module
from typing import TYPE_CHECKING, Any

from .constants import CCIDbParam, DataSize
from .exceptions import InterfaceError, OperationalError
from .protocol import (
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


class Connection:
    """PEP 249 DB-API connection for the CUBRID CAS protocol."""

    def __init__(
        self,
        host: str,
        port: int,
        database: str,
        user: str,
        password: str,
        autocommit: bool = False,
        **kwargs: Any,
    ) -> None:
        """Initialize and connect to a CUBRID broker.

        Args:
            host: CUBRID broker host.
            port: CUBRID broker port.
            database: Database name.
            user: Database user name.
            password: Database password.
            autocommit: Initial auto-commit mode.
            **kwargs: Optional connection parameters.
        """
        self._host = host
        self._port = port
        self._database = database
        self._user = user
        self._password = password
        self._connect_timeout = kwargs.get("connect_timeout")

        self._socket: socket.socket | None = None
        self._connected = False
        self._cas_info: bytes = b"\x00\x00\x00\x00"
        self._session_id = 0
        self._autocommit = False
        self._cursors: set[Cursor] = set()

        self.connect()
        if autocommit:
            self.autocommit = True

    def connect(self) -> None:
        """Establish a TCP CAS session with broker handshake and open database."""
        if self._connected:
            return

        try:
            handshake_socket = self._create_socket(self._host, self._port)
            client_info_packet = ClientInfoExchangePacket()
            handshake_socket.sendall(client_info_packet.write())
            handshake_response = self._recv_exact(handshake_socket, DataSize.INT)
            client_info_packet.parse(handshake_response)

            if client_info_packet.new_connection_port > 0:
                handshake_socket.close()
                self._socket = self._create_socket(
                    self._host, client_info_packet.new_connection_port
                )
            else:
                self._socket = handshake_socket

            open_db_packet = OpenDatabasePacket(
                database=self._database,
                user=self._user,
                password=self._password,
            )
            self._socket.sendall(open_db_packet.write())
            data_length_bytes = self._recv_exact(self._socket, DataSize.DATA_LENGTH)
            data_length = struct.unpack(">i", data_length_bytes)[0]
            response_body = self._recv_exact(self._socket, data_length)
            open_db_packet.parse(response_body)

            self._cas_info = open_db_packet.cas_info
            self._session_id = open_db_packet.session_id
            self._connected = True
        except OSError as exc:
            self._safe_close_socket()
            raise OperationalError("failed to connect to CUBRID broker") from exc

    def close(self) -> None:
        """Close the connection and all tracked cursors."""
        if not self._connected:
            return

        for cursor in list(self._cursors):
            try:
                cursor.close()
            except Exception:
                pass
            finally:
                self._cursors.discard(cursor)

        try:
            self._send_and_receive(CloseDatabasePacket())
        except Exception:
            pass
        finally:
            self._safe_close_socket()
            self._connected = False

    def commit(self) -> None:
        """Commit the current transaction."""
        self._ensure_connected()
        self._send_and_receive(CommitPacket())

    def rollback(self) -> None:
        """Roll back the current transaction."""
        self._ensure_connected()
        self._send_and_receive(RollbackPacket())

    def cursor(self) -> Cursor:
        """Create and return a new cursor bound to this connection."""
        self._ensure_connected()
        cursor_module = import_module("pycubrid.cursor")
        cursor_class = getattr(cursor_module, "Cursor")
        cursor = cursor_class(self)
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

    def get_server_version(self) -> str:
        """Return the server engine version string."""
        self._ensure_connected()
        packet = self._send_and_receive(GetEngineVersionPacket(auto_commit=self._autocommit))
        return packet.engine_version

    def get_last_insert_id(self) -> str:
        """Return last inserted auto-increment value as string."""
        self._ensure_connected()
        packet = self._send_and_receive(GetLastInsertIdPacket())
        return packet.last_insert_id

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
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        if self._connect_timeout is not None:
            sock.settimeout(self._connect_timeout)
        sock.connect((host, port))
        return sock

    def _send_and_receive(self, packet: Any) -> Any:
        """Send a framed CAS request and parse the framed response into ``packet``."""
        self._ensure_connected()
        if self._socket is None:
            raise InterfaceError("connection is closed")

        try:
            request_data = packet.write(self._cas_info)
            self._socket.sendall(request_data)

            data_length_bytes = self._recv_exact(self._socket, DataSize.DATA_LENGTH)
            data_length = struct.unpack(">i", data_length_bytes)[0]
            response_body = self._recv_exact(self._socket, data_length)

            packet.parse(response_body)
            return packet
        except OSError as exc:
            raise OperationalError("socket communication failed") from exc

    def _recv_exact(self, sock: socket.socket, size: int) -> bytes:
        """Receive exactly ``size`` bytes from the socket."""
        chunks: list[bytes] = []
        received = 0
        while received < size:
            chunk = sock.recv(size - received)
            if not chunk:
                raise OperationalError("connection lost during receive")
            chunks.append(chunk)
            received += len(chunk)
        return b"".join(chunks)

    def _ensure_connected(self) -> None:
        """Raise ``InterfaceError`` when called on a closed connection."""
        if not self._connected:
            raise InterfaceError("connection is closed")

    def _check_closed(self) -> None:
        """Alias for ``_ensure_connected`` used by DB-API call sites."""
        self._ensure_connected()

    def _safe_close_socket(self) -> None:
        if self._socket is not None:
            try:
                self._socket.close()
            finally:
                self._socket = None
