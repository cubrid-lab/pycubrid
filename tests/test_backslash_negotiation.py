"""Tests for automatic backslash-escape mode negotiation (issue #255).

CUBRID's ``no_backslash_escapes`` system parameter defaults to ``yes``
(a backslash is an ordinary literal character).  When the caller does not
pin the mode explicitly, the driver probes the live server with
``SELECT CHAR_LENGTH('\\\\')`` and derives the correct client-side escaping
behaviour, avoiding the silent doubling of backslashes.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from pycubrid.aio.connection import AsyncConnection
from pycubrid.connection import Connection
from pycubrid.exceptions import OperationalError


def _make_sync_conn(
    fetchone_result: object,
    *,
    preset: bool | None = None,
    raise_on_execute: bool = False,
) -> tuple[Connection, MagicMock]:
    conn = Connection.__new__(Connection)
    conn._no_backslash_escapes = preset
    mock_cursor = MagicMock()
    if raise_on_execute:
        mock_cursor.execute.side_effect = RuntimeError("boom")
    mock_cursor.fetchone.return_value = fetchone_result
    conn.cursor = MagicMock(return_value=mock_cursor)  # type: ignore[method-assign]
    conn.rollback = MagicMock()  # type: ignore[method-assign]
    return conn, mock_cursor


class TestSyncNegotiation:
    def test_probe_two_means_literal_mode(self) -> None:
        conn, cur = _make_sync_conn((2,))
        conn._negotiate_backslash_escapes()
        assert conn._no_backslash_escapes is True
        cur.execute.assert_called_once_with("SELECT CHAR_LENGTH('\\\\')")
        conn.rollback.assert_called_once()  # probe transaction discarded

    def test_probe_one_means_escape_mode(self) -> None:
        conn, _ = _make_sync_conn((1,))
        conn._negotiate_backslash_escapes()
        assert conn._no_backslash_escapes is False

    def test_probe_unexpected_raises(self) -> None:
        conn, _ = _make_sync_conn((7,))
        with pytest.raises(OperationalError, match="backslash-escape"):
            conn._negotiate_backslash_escapes()
        assert conn._no_backslash_escapes is None
        conn.rollback.assert_called_once()  # probe tx discarded even on failure

    def test_probe_none_row_raises(self) -> None:
        conn, _ = _make_sync_conn(None)
        with pytest.raises(OperationalError, match="backslash-escape"):
            conn._negotiate_backslash_escapes()
        assert conn._no_backslash_escapes is None
        conn.rollback.assert_called_once()  # probe tx discarded even on failure

    def test_execute_error_raises(self) -> None:
        conn, _ = _make_sync_conn(None, raise_on_execute=True)
        with pytest.raises(OperationalError, match="backslash-escape"):
            conn._negotiate_backslash_escapes()
        assert conn._no_backslash_escapes is None
        conn.rollback.assert_called_once()  # probe tx discarded even on error

    def test_explicit_true_is_not_overridden(self) -> None:
        conn, cur = _make_sync_conn((1,), preset=True)
        conn._negotiate_backslash_escapes()
        assert conn._no_backslash_escapes is True
        cur.execute.assert_not_called()
        conn.rollback.assert_not_called()  # no probe, no transaction to discard

    def test_explicit_false_is_not_overridden(self) -> None:
        conn, cur = _make_sync_conn((2,), preset=False)
        conn._negotiate_backslash_escapes()
        assert conn._no_backslash_escapes is False
        cur.execute.assert_not_called()


def _make_async_conn(
    fetchone_result: object,
    *,
    preset: bool | None = None,
    raise_on_execute: bool = False,
) -> tuple[AsyncConnection, AsyncMock]:
    conn = AsyncConnection.__new__(AsyncConnection)
    conn._no_backslash_escapes = preset
    mock_cursor = MagicMock()
    if raise_on_execute:
        mock_cursor.execute = AsyncMock(side_effect=RuntimeError("boom"))
    else:
        mock_cursor.execute = AsyncMock()
    mock_cursor.fetchone = AsyncMock(return_value=fetchone_result)
    mock_cursor.close = AsyncMock()
    conn.cursor = MagicMock(return_value=mock_cursor)  # type: ignore[method-assign]
    conn.rollback = AsyncMock()  # type: ignore[method-assign]
    return conn, mock_cursor


class TestAsyncNegotiation:
    @pytest.mark.asyncio
    async def test_probe_two_means_literal_mode(self) -> None:
        conn, cur = _make_async_conn((2,))
        await conn._negotiate_backslash_escapes()
        assert conn._no_backslash_escapes is True
        cur.execute.assert_awaited_once_with("SELECT CHAR_LENGTH('\\\\')")
        conn.rollback.assert_awaited_once()  # probe transaction discarded

    @pytest.mark.asyncio
    async def test_probe_one_means_escape_mode(self) -> None:
        conn, _ = _make_async_conn((1,))
        await conn._negotiate_backslash_escapes()
        assert conn._no_backslash_escapes is False

    @pytest.mark.asyncio
    async def test_execute_error_raises(self) -> None:
        conn, _ = _make_async_conn(None, raise_on_execute=True)
        with pytest.raises(OperationalError, match="backslash-escape"):
            await conn._negotiate_backslash_escapes()
        assert conn._no_backslash_escapes is None
        conn.rollback.assert_awaited_once()  # probe tx discarded even on error

    @pytest.mark.asyncio
    async def test_probe_unexpected_raises(self) -> None:
        conn, _ = _make_async_conn((7,))
        with pytest.raises(OperationalError, match="backslash-escape"):
            await conn._negotiate_backslash_escapes()
        assert conn._no_backslash_escapes is None
        conn.rollback.assert_awaited_once()  # probe tx discarded even on failure

    @pytest.mark.asyncio
    async def test_explicit_setting_is_not_overridden(self) -> None:
        conn, cur = _make_async_conn((1,), preset=True)
        await conn._negotiate_backslash_escapes()
        assert conn._no_backslash_escapes is True
        cur.execute.assert_not_awaited()
        conn.rollback.assert_not_awaited()  # no probe, no transaction to discard
