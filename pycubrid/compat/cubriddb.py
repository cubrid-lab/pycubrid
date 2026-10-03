"""Bounded CUBRIDdb-style connection and qualified row cursor wrapper."""

from __future__ import annotations

from typing import Any

from . import native
from .cursors import Cursor as _WrapperCursor
from .cursors import DictCursor as _WrapperDictCursor


class Connection:
    """Own one native-style object and its per-connection fetch converter."""

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
        self.fetch_value_converter: Any = None

    @property
    def connection(self) -> native.connection:
        """Return this wrapper's exact native-style connection object."""
        return self._connection

    def close(self) -> None:
        """Close the one underlying connection."""
        self._connection.close()

    def commit(self) -> None:
        """Commit through the owned native connection."""
        self._connection.commit()

    def rollback(self) -> None:
        """Roll back through the owned native connection."""
        self._connection.rollback()

    def server_version(self) -> str:
        """Return the exact owned native server-version result."""
        return self._connection.server_version()

    def ping(self) -> int:
        """Return the owned native query-ping result without bool conversion."""
        return self._connection.ping()

    def cursor(self, dictCursor: Any = None) -> _WrapperCursor | _WrapperDictCursor:
        """Choose exact-name dict rows for truthy values, tuple rows otherwise."""
        cls = _WrapperDictCursor if dictCursor else _WrapperCursor
        return cls(self)

    def set_fetch_value_converter(self, func: Any) -> None:
        """Store the current callback for cursors of this connection."""
        self.fetch_value_converter = func

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
