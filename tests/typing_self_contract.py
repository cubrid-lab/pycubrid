"""Static typing.Self contract for #689 (checked by mypy via `make typecheck`).

This module is checked by mypy but never instantiated by pytest,
so no __init__ overrides are needed and CodeQL finds no issues.
"""

from __future__ import annotations

from typing import assert_type

from pycubrid.aio.connection import AsyncConnection
from pycubrid.aio.cursor import AsyncCursor
from pycubrid.connection import Connection
from pycubrid.cursor import Cursor
from pycubrid.lob import Lob


class SubConnection(Connection):
    """Minimal subclass to verify Self return type contract."""

    def __init__(self) -> None:
        object.__init__(self)
        object.__setattr__(self, "_closed", False)

    def _ensure_connected(self) -> None:
        pass

    def close(self) -> None:
        pass


class SubCursor(Cursor):
    """Minimal subclass to verify Self return type contract."""

    def __init__(self, connection: object) -> None:
        super().__init__(connection)


class SubLob(Lob):
    """Minimal subclass to verify Self return type contract."""

    def __init__(self) -> None:
        object.__init__(self)
        object.__setattr__(self, "_closed", False)

    def close(self) -> None:
        pass


class SubAsyncConnection(AsyncConnection):
    """Minimal async subclass to verify Self return type contract."""

    def __init__(self) -> None:
        object.__init__(self)
        object.__setattr__(self, "_closed", False)

    def _ensure_connected(self) -> None:
        pass

    async def _close(self) -> None:
        pass


class SubAsyncCursor(AsyncCursor):
    """Minimal async cursor subclass to verify Self return type contract."""

    def __init__(self, connection: object) -> None:
        super().__init__(connection)


def _sync(conn: SubConnection, cur: SubCursor, lob: SubLob) -> None:
    """Verify sync context-manager and iterator returns preserve subclass type."""
    with conn as c:
        assert_type(c, SubConnection)
    with cur as k:
        assert_type(k, SubCursor)
    assert_type(iter(cur), SubCursor)
    with lob as b:
        assert_type(b, SubLob)


async def _async(conn: SubAsyncConnection, cur: SubAsyncCursor) -> None:
    """Verify async context-manager and iterator returns preserve subclass type."""
    async with conn as c:
        assert_type(c, SubAsyncConnection)
    async with cur as k:
        assert_type(k, SubAsyncCursor)
    assert_type(aiter(cur), SubAsyncCursor)
