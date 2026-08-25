"""Shared test fixtures for pycubrid."""

from __future__ import annotations

import pytest

from pycubrid.aio.connection import AsyncConnection
from pycubrid.connection import Connection


@pytest.fixture(autouse=True)
def _skip_backslash_probe(request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch) -> None:
    """Skip live backslash-escape negotiation for unrelated tests.

    Most tests build ``Connection``/``AsyncConnection`` over a scripted fake
    socket that does not queue a ``CHAR_LENGTH`` probe response. Escape-mode
    negotiation now fails loud on an unreadable probe (issue #263), so pin the
    flag to its legacy default here instead of probing the exhausted socket.
    The dedicated ``test_backslash_negotiation.py`` module opts out to exercise
    the real probe.
    """
    if "test_backslash_negotiation" in str(request.fspath):
        return

    def _pin_sync(self: Connection) -> None:
        if self._no_backslash_escapes is None:
            self._no_backslash_escapes = False

    async def _pin_async(self: AsyncConnection) -> None:
        if self._no_backslash_escapes is None:
            self._no_backslash_escapes = False

    monkeypatch.setattr(Connection, "_negotiate_backslash_escapes", _pin_sync)
    monkeypatch.setattr(AsyncConnection, "_negotiate_backslash_escapes", _pin_async)
