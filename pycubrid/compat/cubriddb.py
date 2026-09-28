"""Construction-only CUBRIDdb-style wrapper over the explicit native surface."""

from __future__ import annotations

from typing import Any

from pycubrid.exceptions import NotSupportedError

from . import native


class Connection:
    """Own one native-style object; wrapper cursor operations are not yet available."""

    def __init__(
        self,
        dsn: str = "",
        user: str = "public",
        password: str = "",  # nosec B107
        charset: str = "utf8",
    ) -> None:
        if not isinstance(charset, str):
            raise TypeError("charset must be a string")
        if charset != "utf8":
            raise NotSupportedError("only the existing UTF-8 transport is supported")
        self._connection = native.connection(dsn, user, password)

    @property
    def connection(self) -> native.connection:
        """Return this wrapper's exact native-style connection object."""
        return self._connection

    def close(self) -> None:
        """Close the one underlying connection."""
        self._connection.close()


def Connect(*args: Any, **kwargs: Any) -> Connection:
    """Map up to three positional factory values over matching keywords."""
    if len(args) > 3:
        raise TypeError("Connect accepts at most three positional arguments")
    options = dict(kwargs)
    for name, value in zip(("dsn", "user", "password"), args):
        options[name] = value
    return Connection(**options)


def connect(*args: Any, **kwargs: Any) -> Connection:
    """Alias the wrapper factory call shape."""
    return Connect(*args, **kwargs)


connection = Connect

__all__ = ["Connection", "Connect", "connect", "connection"]
