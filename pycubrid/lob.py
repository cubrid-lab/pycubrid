from __future__ import annotations

import logging
from types import TracebackType
from typing import Any, Literal, Protocol

from .constants import CUBRIDDataType as CCI_U_TYPE
from .exceptions import InterfaceError, OperationalError
from .protocol import LOBNewPacket, LOBReadPacket, LOBWritePacket


class _ConnectionLike(Protocol):
    def _ensure_connected(self) -> None: ...

    def _send_and_receive(self, packet: Any) -> Any: ...


_LOGGER = logging.getLogger(__name__)


class Lob:
    """Represents a CUBRID Large Object (BLOB or CLOB).

    Lifecycle: LOB handles are connection/session-scoped. The CUBRID CAS wire
    protocol exposes no release/free opcode, so server-side resources are
    reclaimed only when the owning connection/session closes. :meth:`close`
    (and context-manager use) performs a purely client-side invalidation of
    this Python object; it issues no network request. Operating on a closed
    LOB raises :class:`InterfaceError`.
    """

    def __init__(self, connection: _ConnectionLike, lob_type: int, lob_handle: bytes = b"") -> None:
        """Initialize a LOB object bound to a connection."""
        if lob_type not in (CCI_U_TYPE.BLOB, CCI_U_TYPE.CLOB):
            raise ValueError("lob_type must be CCI_U_TYPE.BLOB or CCI_U_TYPE.CLOB")
        self._connection = connection
        self._lob_type = lob_type
        self._lob_handle = lob_handle
        self._closed = False

    @classmethod
    def create(cls, connection: _ConnectionLike, lob_type: int) -> Lob:
        """Create a new LOB object on the server."""
        connection._ensure_connected()
        packet = LOBNewPacket(lob_type)
        connection._send_and_receive(packet)
        _LOGGER.debug("LOB created: type=%d handle=%d bytes", lob_type, len(packet.lob_handle))
        return cls(connection, lob_type, packet.lob_handle)

    def write(self, data: bytes, offset: int = 0) -> int:
        """Write bytes to the LOB starting from ``offset``.

        Raises ``OperationalError`` if the server writes fewer bytes than
        requested (e.g. disk full, quota exceeded).
        """
        self._check_open()
        if offset < 0:
            raise InterfaceError(f"offset must be non-negative, got {offset}")
        self._connection._ensure_connected()
        packet = LOBWritePacket(self._lob_handle, offset, data)
        self._connection._send_and_receive(packet)
        if packet.bytes_written != len(data):
            raise OperationalError(
                f"LOB write truncated: wrote {packet.bytes_written} of {len(data)} bytes"
            )
        _LOGGER.debug("LOB write: offset=%d size=%d", offset, len(data))
        return len(data)

    def read(self, length: int, offset: int = 0) -> bytes:
        """Read up to ``length`` bytes from the LOB starting from ``offset``."""
        self._check_open()
        if offset < 0:
            raise InterfaceError(f"offset must be non-negative, got {offset}")
        if length < 0:
            raise InterfaceError(f"length must be non-negative, got {length}")
        self._connection._ensure_connected()
        packet = LOBReadPacket(self._lob_handle, offset, length)
        self._connection._send_and_receive(packet)
        if len(packet.lob_data) > length:
            got = len(packet.lob_data)
            raise OperationalError(f"LOB read returned {got} bytes exceeding requested {length}")
        _LOGGER.debug(
            "LOB read: offset=%d requested=%d got=%d", offset, length, len(packet.lob_data)
        )
        return packet.lob_data

    @property
    def lob_handle(self) -> bytes:
        """Return the raw LOB handle bytes."""
        return self._lob_handle

    @property
    def lob_type(self) -> int:
        """Return the LOB type code."""
        return self._lob_type

    def _check_open(self) -> None:
        """Raise ``InterfaceError`` if this LOB has been closed."""
        if self._closed:
            raise InterfaceError("LOB is closed")

    def close(self) -> None:
        """Invalidate this LOB object (client-side only).

        This performs no network I/O: the CUBRID CAS protocol has no LOB-release
        opcode. Server-side LOB resources are connection/session-scoped and are
        reclaimed when the owning connection/session closes. After ``close``,
        :meth:`read`/:meth:`write` raise :class:`InterfaceError`. Idempotent.
        """
        self._closed = True

    def __enter__(self) -> Lob:
        """Enter the runtime context and return this LOB."""
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> Literal[False]:
        """Close the LOB on context-manager exit without suppressing exceptions."""
        self.close()
        return False
