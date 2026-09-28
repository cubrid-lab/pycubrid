"""Construction-only native-style connection over the pure Python driver.

Prepared cursors and the remaining native API are not implemented here.
"""

from __future__ import annotations

import logging
import re

from pycubrid.connection import Connection as _DriverConnection
from pycubrid.exceptions import InterfaceError, NotSupportedError

_LOGGER = logging.getLogger(__name__)
_HOST = re.compile(r"[A-Za-z0-9_.-]+\Z")
_PORT = re.compile(r"[0-9]+\Z")


def _parse_url(url: str) -> tuple[str, int, str]:
    if not isinstance(url, str):
        raise TypeError("url must be a string")
    if "\x00" in url:
        raise ValueError("url must not contain NUL")
    parts = url.split(":", 6)
    if len(parts) != 7:
        raise InterfaceError("invalid CUBRID DSN")
    backend, host, port_text, database, _dsn_user, _dsn_password, options = parts
    if backend.casefold() in {"cubrid-mysql", "cubrid-oracle"}:
        raise NotSupportedError("alternate CUBRID backends are not supported")
    if backend.casefold() != "cubrid":
        raise InterfaceError("invalid CUBRID DSN")
    if options:
        raise NotSupportedError("CCI DSN options are not supported")
    if not _HOST.fullmatch(host) or not _PORT.fullmatch(port_text) or not database:
        raise InterfaceError("invalid CUBRID DSN")
    port = int(port_text)
    if not 1 <= port <= 65535:
        raise InterfaceError("invalid CUBRID broker port")
    return host, port, database


class connection:
    """Own one sync transport; native cursor operations are not available yet."""

    def __init__(self, url: str, user: str = "public", passwd: str = "") -> None:  # nosec B107
        host, port, database = _parse_url(url)
        for name, value in (("user", user), ("passwd", passwd)):
            if not isinstance(value, str):
                raise TypeError(f"{name} must be a string")
            if "\x00" in value:
                raise ValueError(f"{name} must not contain NUL")

        # Retain the object if its initializer fails after acquiring a socket.
        # Calling the ordinary factory would lose the reference on that path.
        driver = _DriverConnection.__new__(_DriverConnection)
        try:
            _DriverConnection.__init__(
                driver,
                host=host,
                port=port,
                database=database,
                user=user,
                password=passwd,
                autocommit=True,
            )
        except BaseException:
            if hasattr(driver, "_socket"):
                try:
                    # Fail closed: a broker reply may be unread after setup fails.
                    driver._drop_connection()
                except BaseException:
                    # A secondary cleanup failure must not replace the original.
                    _LOGGER.warning("Failed to discard an incomplete compatibility session")
            raise
        self._driver = driver
        self._closed = False

    def close(self) -> None:
        """Close the owned transport once."""
        if self._closed:
            return
        self._driver.close()
        self._closed = True


def connect(url: str, user: str = "public", passwd: str = "") -> connection:  # nosec B107
    """Create the native-style construction-only connection."""
    return connection(url, user, passwd)


__all__ = ["connection", "connect"]
