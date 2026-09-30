"""Shared test fixtures for pycubrid."""

from __future__ import annotations

import os

import pytest
from hypothesis import HealthCheck, settings

from pycubrid.aio.connection import AsyncConnection
from pycubrid.connection import Connection

from ._cubrid_endpoint import is_configured, probe, resolve_endpoint

# Hypothesis profiles for the bug-hunt suites. "dev"/"pr" keep CI fast and
# deterministic; "nightly" widens exploration. Select with the env var
# HYPOTHESIS_PROFILE (defaults to "pr"). Per-test @settings still override the
# profile's max_examples where a test pins its own budget. All profiles set
# print_blob=True so a failing example prints a @reproduce_failure blob and
# persists in Hypothesis's .hypothesis/ database for one-command replay (#359).
settings.register_profile("pr", max_examples=50, deadline=None, print_blob=True)
settings.register_profile(
    "dev",
    max_examples=25,
    deadline=None,
    print_blob=True,
    suppress_health_check=[HealthCheck.too_slow],
)
settings.register_profile("nightly", max_examples=1000, deadline=None, print_blob=True)
settings.load_profile(os.environ.get("HYPOTHESIS_PROFILE", "pr"))


_UNCONFIGURED_REASON = (
    "requires a live CUBRID server (set CUBRID_TEST_URL or CUBRID_TEST_HOST, "
    "or run `make integration`)"
)
# Outcome of the one live probe per session: None = not probed yet, "" = reachable,
# otherwise the error message every plain integration test reports.
_probe_error: str | None = None


def _endpoint_error() -> str:
    global _probe_error
    if _probe_error is None:
        try:
            endpoint = resolve_endpoint()
        except ValueError as exc:
            _probe_error = f"CUBRID test endpoint is misconfigured: {exc}"
            return _probe_error
        try:
            probe(endpoint)
        except Exception as exc:  # noqa: BLE001 - any probe failure means "not reachable"
            _probe_error = (
                f"CUBRID test endpoint {endpoint.describe()} is configured but unreachable: "
                f"{type(exc).__name__}: {exc}. Start the server (or fix CUBRID_TEST_URL / "
                "CUBRID_TEST_HOST / CUBRID_TEST_PORT); unset both CUBRID_TEST_URL and "
                "CUBRID_TEST_HOST to skip the integration tests instead."
            )
        else:
            _probe_error = ""
    return _probe_error


def pytest_runtest_setup(item: pytest.Item) -> None:
    """Gate integration-marked tests on a configured, reachable CUBRID.

    Classification lives entirely in the ``integration`` marker; no test module
    probes the server at import time (#522). This hook runs after ``skipif``
    markers are evaluated but before any fixture is set up (including
    module-scoped connection fixtures):

    * no endpoint configured (neither ``CUBRID_TEST_URL`` nor ``CUBRID_TEST_HOST``):
      skip, so a bare ``pytest`` stays green locally;
    * endpoint configured but unreachable: error, never skip (#411, #522). The
      server is probed once per session and every plain integration test
      reports the same clear error instead of silently degrading to skips.

    ``tls`` tests talk to a separately configured SSL broker
    (``CUBRID_TLS_TEST_*``) that may refuse a plaintext probe, so they only get
    the unconfigured skip here and gate on their own TLS configuration.
    """
    if item.get_closest_marker("integration") is None:
        return
    if not is_configured():
        pytest.skip(_UNCONFIGURED_REASON)
    if item.get_closest_marker("tls") is not None:
        return
    error = _endpoint_error()
    if error:
        pytest.fail(error, pytrace=False)


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
        "test_transaction_matrix",
        "test_type_contract",
        "test_batch_semantics",
        "test_lob_adversarial",
        "test_async_cancellation",
        "test_cubriddb_differential",
        "test_resource_leaks",
        "test_pep249_runtime",
        "test_soak",
        "test_chaos",
        "test_version_differential",
        "test_replay_parity",
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
