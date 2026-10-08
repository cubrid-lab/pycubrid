"""Subclass-preserving contract tests for typing.Self annotations (issue #689).

Verifies that subclasses of Connection, Cursor, Lob, AsyncConnection, and
AsyncCursor are returned by value (not base class) through the context-manager
and iterator protocols, so that type-annotation ``-> Self`` is upheld at runtime.

These are offline unit tests — no live CUBRID server is required.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from pycubrid.cursor import Cursor
from pycubrid.aio.cursor import AsyncCursor


def _make_minimal_cursor_subclass_instance() -> Cursor:
    """Build a minimal Cursor subclass instance without hitting a real DB."""
    mock_conn = MagicMock()
    mock_conn._fetch_size = 1
    mock_conn._timing = False
    return TypedSubCursor(mock_conn)


class TypedSubCursor(Cursor):
    """Cursor subclass used to verify Self return type through protocols."""

    def __init__(self, connection, **kwargs):
        super().__init__(connection, **kwargs)


class TypedSubAsyncCursor(AsyncCursor):
    """AsyncCursor subclass used to verify Self return type through async protocols."""

    def __init__(self, connection, **kwargs):
        super().__init__(connection, **kwargs)




# ── Sync Cursor ────────────────────────────────────────────────────────────────


class TestTypedSubCursor:
    """Verify Cursor.__iter__ and Cursor.__enter__ return the subclass type."""

    def test_iter_returns_self_type(self) -> None:
        """__iter__ must return the cursor instance itself (subclass)."""
        cursor = _make_minimal_cursor_subclass_instance()
        iterator = cursor.__iter__()
        assert type(iterator) is TypedSubCursor, "Cursor.__iter__ must return Self, not base Cursor"

    def test_enter_returns_self_type(self) -> None:
        """__enter__ must return the cursor instance (subclass) on context manager entry."""
        cursor = _make_minimal_cursor_subclass_instance()
        ctx = cursor.__enter__()
        assert type(ctx) is TypedSubCursor, "Cursor.__enter__ must return Self, not base Cursor"


# ── Sync Connection (mock-based) ──────────────────────────────────────────────


class TestTypedSubConnection:
    """Verify Connection.__enter__ returns the subclass type using a minimal mock."""

    def test_enter_returns_self_type(self) -> None:
        """__enter__ must return the connection instance (subclass)."""
        from pycubrid.connection import Connection

        class TypedSubConnection(Connection):
            def __init__(self) -> None:
                # Use object.__init__ to satisfy CodeQL "missing super().__init__";
                # we skip Connection.__init__ (which opens a socket) since this is a
                # minimal mock for type-contract testing only.
                object.__init__(self)
                object.__setattr__(self, "_closed", False)

            def _ensure_connected(self) -> None:
                pass

            def close(self) -> None:
                pass

        conn: Connection = TypedSubConnection()
        ctx = conn.__enter__()
        assert type(ctx) is TypedSubConnection, "Connection.__enter__ must return Self"


# ── Sync LOB (mock-based) ─────────────────────────────────────────────────────


class TestTypedSubLob:
    """Verify Lob.__enter__ returns the subclass type using a minimal mock."""

    def test_enter_returns_self_type(self) -> None:
        """__enter__ must return the LOB instance (subclass)."""
        from pycubrid.lob import Lob

        class TypedSubLob(Lob):
            def __init__(self) -> None:
                # Use object.__init__ to satisfy CodeQL "missing super().__init__";
                # we skip Lob.__init__ since this is a minimal mock for testing only.
                object.__init__(self)
                object.__setattr__(self, "_closed", False)

            def close(self) -> None:
                pass

        lob = TypedSubLob()
        ctx = lob.__enter__()
        assert type(ctx) is TypedSubLob, "Lob.__enter__ must return Self"


# ── Async Cursor ─────────────────────────────────────────────────────────────


def _make_minimal_acursor_instance() -> AsyncCursor:
    """Build a minimal AsyncCursor subclass instance without hitting a real DB."""
    mock_conn = MagicMock()
    mock_conn._fetch_size = 1
    mock_conn._timing = False
    return TypedSubAsyncCursor(mock_conn)


class TestTypedSubAsyncCursor:
    """Verify AsyncCursor.__aiter__ and AsyncCursor.__aenter__ return the subclass type."""

    def test_aiter_returns_self_type(self) -> None:
        """__aiter__ must return the cursor instance itself (subclass)."""
        cursor = _make_minimal_acursor_instance()
        iterator = cursor.__aiter__()
        assert type(iterator) is TypedSubAsyncCursor, "AsyncCursor.__aiter__ must return Self"

    @pytest.mark.asyncio
    async def test_aenter_returns_self_type(self) -> None:
        """__aenter__ must return the async cursor instance (subclass)."""
        cursor = _make_minimal_acursor_instance()
        ctx = await cursor.__aenter__()
        assert type(ctx) is TypedSubAsyncCursor, "AsyncCursor.__aenter__ must return Self"


# ── Async Connection (mock-based) ─────────────────────────────────────────────


class TestTypedSubAsyncConnection:
    """Verify AsyncConnection.__aenter__ returns the subclass type using a minimal mock."""

    @pytest.mark.asyncio
    async def test_aenter_returns_self_type(self) -> None:
        """__aenter__ must return the async connection instance (subclass)."""
        from pycubrid.aio.connection import AsyncConnection

        class TypedSubAsyncConnection(AsyncConnection):
            def __init__(self) -> None:
                # Use object.__init__ to satisfy CodeQL "missing super().__init__";
                # we skip AsyncConnection.__init__ since this is a minimal mock for testing only.
                object.__init__(self)
                object.__setattr__(self, "_closed", False)

            def _ensure_connected(self) -> None:
                pass

            async def _close(self) -> None:
                pass

        conn: AsyncConnection = TypedSubAsyncConnection()
        ctx = await conn.__aenter__()
        assert type(ctx) is TypedSubAsyncConnection, "AsyncConnection.__aenter__ must return Self"
