"""Shared test fixtures for pycubrid."""

from __future__ import annotations

import os

import pytest
from hypothesis import HealthCheck, settings

from pycubrid.aio.connection import AsyncConnection
from pycubrid.connection import Connection

# Hypothesis profiles for the bug-hunt suites. "dev"/"pr" keep CI fast and
# deterministic; "nightly" widens exploration. Select with the env var
# HYPOTHESIS_PROFILE (defaults to "pr"). Per-test @settings still override the
# profile's max_examples where a test pins its own budget.
settings.register_profile("pr", max_examples=50, deadline=None)
settings.register_profile(
    "dev", max_examples=25, deadline=None, suppress_health_check=[HealthCheck.too_slow]
)
settings.register_profile("nightly", max_examples=1000, deadline=None)
settings.load_profile(os.environ.get("HYPOTHESIS_PROFILE", "pr"))


def _cubrid_is_configured() -> bool:
    """True when the environment points at a CUBRID test server.

    A live integration run is expected whenever CUBRID_TEST_URL or the
    per-field CUBRID_TEST_HOST override is set; otherwise a local `pytest`
    without any DB config just skips the integration-marked tests.
    """
    return bool(os.getenv("CUBRID_TEST_URL") or os.getenv("CUBRID_TEST_HOST"))


@pytest.fixture(autouse=True)
def _require_cubrid_for_integration(request: pytest.FixtureRequest) -> None:
    """Gate integration-marked tests on a configured, reachable CUBRID.

    Classification lives entirely in the ``integration`` marker. When no DB is
    configured the test skips (so a bare `pytest` stays green locally); when a
    DB *is* configured but unreachable it must not silently skip — that would
    hide a broken CI service — so the individual test's own connection attempt
    is left to fail loudly.
    """
    if request.node.get_closest_marker("integration") is None:
        return
    if not _cubrid_is_configured():
        pytest.skip("requires a live CUBRID server (set CUBRID_TEST_URL or run `make integration`)")


@pytest.fixture(autouse=True)
def _skip_backslash_probe(request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch) -> None:
    """Skip live backslash-escape negotiation for unrelated tests.

    Most tests build ``Connection``/``AsyncConnection`` over a scripted fake
    socket that does not queue a ``CHAR_LENGTH`` probe response. Escape-mode
    negotiation now fails loud on an unreadable probe (issue #263), so pin the
    flag to its legacy default here instead of probing the exhausted socket.
    The dedicated ``test_backslash_negotiation.py`` module opts out to exercise
    the real probe, and ``test_integration.py`` opts out because it negotiates
    against a live CUBRID server.
    """
    fspath = str(request.fspath)
    _live_optouts = (
        "test_backslash_negotiation",
        "test_integration",
        "test_property_live_values",
        "test_connection_state_machine",
        "test_metamorphic_parity",
    )
    if any(name in fspath for name in _live_optouts):
        return

    def _pin_sync(self: Connection) -> None:
        if self._no_backslash_escapes is None:
            self._no_backslash_escapes = False

    async def _pin_async(self: AsyncConnection) -> None:
        if self._no_backslash_escapes is None:
            self._no_backslash_escapes = False

    monkeypatch.setattr(Connection, "_negotiate_backslash_escapes", _pin_sync)
    monkeypatch.setattr(AsyncConnection, "_negotiate_backslash_escapes", _pin_async)
