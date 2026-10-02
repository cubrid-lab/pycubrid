"""Bounded CUBRIDdb-style construction and autocommit wrapper over native."""

from __future__ import annotations

from typing import Any

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
        # Validated by the driver before any socket work; CUBRID spellings
        # such as "euckr" are accepted (#86).
        self._connection = native.connection(dsn, user, password, charset=charset)

    @property
    def connection(self) -> native.connection:
        """Return this wrapper's exact native-style connection object."""
        return self._connection

    def close(self) -> None:
        """Close the one underlying connection."""
        self._connection.close()

    def set_autocommit(self, value: bool) -> None:
        """Change effective mode; unlike raw native member assignment."""
        if type(value) is not bool:
            raise ValueError("Parameter should be a boolean value")
        self._connection.set_autocommit(value)

    def get_autocommit(self) -> Any:
        """Return the native cached member, including a caller-assigned value."""
        return self._connection.autocommit

    autocommit = property(get_autocommit, set_autocommit)


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
