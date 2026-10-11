"""Static ``typing.Self`` contract, checked by mypy via ``make typecheck``.

The module is type-checked only: pytest does not collect it (no ``test_`` prefix)
and nothing here is executed.
"""

from __future__ import annotations

from typing import assert_type

from pycubrid.aio.connection import AsyncConnection
from pycubrid.aio.cursor import AsyncCursor
from pycubrid.connection import Connection
from pycubrid.cursor import Cursor
from pycubrid.lob import Lob


class SubConnection(Connection): ...


class SubCursor(Cursor): ...


class SubLob(Lob): ...


class SubAsyncConnection(AsyncConnection): ...


class SubAsyncCursor(AsyncCursor): ...


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
