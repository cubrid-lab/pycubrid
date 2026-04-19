from __future__ import annotations

import json
import socket
import struct
import time
from importlib import import_module
from typing import TYPE_CHECKING, Any

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

    from .timing import TimingStats

_CursorClass: type | None = None


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
        decode_collections: bool = False,
        json_deserializer: Any = None,
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
        self._decode_collections = decode_collections
        self._json_deserializer = json_deserializer
        self._no_backslash_escapes = kwargs.get("no_backslash_escapes", False)
        if self._json_deserializer is not None and not callable(self._json_deserializer):
            raise TypeError("json_deserializer must be callable or None")
        if self._json_deserializer is json.loads:
            self._json_deserializer = json.loads

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
            from .timing import TimingStats as _TimingStats

            self._timing = _TimingStats()

        self._socket: socket.socket | None = None
        self._connected = False
        self._cas_info: bytes | bytearray = b"\x00\x00\x00\x00"
        self._session_id = 0
        self._autocommit = False
        self._cursors: set[Cursor] = set()
        self._protocol_version: int = 1

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
            response_body = self._recv_exact(self._socket, data_length + DataSize.CAS_INFO)
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

    def close(self) -> None:
        """Close the connection and all tracked cursors."""
        if not self._connected:
            return

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
        self._send_and_receive(CommitPacket())
        self._invalidate_query_handles()

    def rollback(self) -> None:
        """Roll back the current transaction."""
        self._ensure_connected()
        self._send_and_receive(RollbackPacket())
        self._invalidate_query_handles()

    def _invalidate_query_handles(self) -> None:
        """Invalidate all cursor query handles.

        After commit/rollback the CUBRID broker may reset the CAS
        connection, making previous query handles stale.  Clearing them
        prevents cursors from sending CloseQueryPacket on a dead socket.
        """
        for cursor in self._cursors:
            cursor._query_handle = None

    # -- CAS_INFO status constants (matches JDBC UConnection) ----------------
    _CAS_INFO_STATUS_INACTIVE: int = 0
    _CAS_INFO_STATUS_ACTIVE: int = 1

    def _check_reconnect(self) -> None:
        """Reconnect to the broker when the CAS has been released.

        The CUBRID broker sets the first byte of CAS_INFO to ``INACTIVE``
        (0) when the CAS process is no longer reserved for this client
        (``KEEP_CONNECTION=AUTO``).  The official JDBC driver checks this
        before every request and transparently reconnects.  This method
        replicates that behaviour so that ``commit()`` followed by a new
        query works without the caller having to manage reconnection.
        """
        self._ensure_connected()
        if self._cas_info[0] == self._CAS_INFO_STATUS_INACTIVE and self._socket is not None:
            self._safe_close_socket()
            self._connected = False
            self._invalidate_query_handles()
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

    @property
    def timing_stats(self) -> TimingStats | None:
        """Return the timing statistics object, or ``None`` if timing is disabled."""
        return self._timing

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
            if reconnect:
                try:
                    self._invalidate_query_handles()
                    self.connect()
                    return True
                except Exception:
                    return False
            return False
        try:
            packet = self._send_and_receive(CheckCasPacket())
            return packet.response_code >= 0
        except Exception:
            if reconnect:
                try:
                    self._safe_close_socket()
                    self._connected = False
                    self._invalidate_query_handles()
                    self.connect()
                    return True
                except Exception:
                    return False
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
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        if self._connect_timeout is not None:
            sock.settimeout(self._connect_timeout)
        sock.connect((host, port))
        return sock

    def _send_and_receive(self, packet: Any) -> Any:
        """Send a framed CAS request and parse the framed response into ``packet``.

        After each response the CAS_INFO status byte is checked.  When the
        broker signals ``INACTIVE`` (the CAS process has been released), the
        driver closes the current socket and reconnects transparently before
        the *next* request — matching the behaviour of the official CUBRID
        JDBC driver (``UClientSideConnection.checkReconnect``).
        """
        self._check_reconnect()
        if self._socket is None:
            raise InterfaceError("connection is closed")

        try:
            request_data = packet.write(self._cas_info)
            self._socket.sendall(request_data)

            data_length_bytes = self._recv_exact(self._socket, DataSize.DATA_LENGTH)
            data_length = struct.unpack(">i", data_length_bytes)[0]
            response_body = self._recv_exact(self._socket, data_length + DataSize.CAS_INFO)

            # Update CAS_INFO from the response (first 4 bytes).
            self._cas_info = response_body[: DataSize.CAS_INFO]

            packet.parse(response_body)
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
