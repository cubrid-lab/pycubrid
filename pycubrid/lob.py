from __future__ import annotations

import logging
from typing import Any, Protocol

from .constants import CUBRIDDataType as CCI_U_TYPE
from .protocol import LOBNewPacket, LOBReadPacket, LOBWritePacket


class _ConnectionLike(Protocol):
    def _ensure_connected(self) -> None: ...

    def _send_and_receive(self, packet: Any) -> Any: ...


_LOGGER = logging.getLogger(__name__)


class Lob:
    """Represents a CUBRID Large Object (BLOB or CLOB)."""

    def __init__(self, connection: _ConnectionLike, lob_type: int, lob_handle: bytes = b"") -> None:
        """Initialize a LOB object bound to a connection."""
        if lob_type not in (CCI_U_TYPE.BLOB, CCI_U_TYPE.CLOB):
            raise ValueError("lob_type must be CCI_U_TYPE.BLOB or CCI_U_TYPE.CLOB")
        self._connection = connection
        self._lob_type = lob_type
        self._lob_handle = lob_handle

    @classmethod
    def create(cls, connection: _ConnectionLike, lob_type: int) -> Lob:
        """Create a new LOB object on the server."""
        connection._ensure_connected()
        packet = LOBNewPacket(lob_type)
        connection._send_and_receive(packet)
        _LOGGER.debug("LOB created: type=%d handle=%d bytes", lob_type, len(packet.lob_handle))
        return cls(connection, lob_type, packet.lob_handle)

    def write(self, data: bytes, offset: int = 0) -> int:
        """Write bytes to the LOB starting from ``offset``."""
        self._connection._ensure_connected()
        packet = LOBWritePacket(self._lob_handle, offset, data)
        self._connection._send_and_receive(packet)
        _LOGGER.debug("LOB write: offset=%d size=%d", offset, len(data))
        return len(data)

    def read(self, length: int, offset: int = 0) -> bytes:
        """Read up to ``length`` bytes from the LOB starting from ``offset``."""
        self._connection._ensure_connected()
        packet = LOBReadPacket(self._lob_handle, offset, length)
        self._connection._send_and_receive(packet)
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
