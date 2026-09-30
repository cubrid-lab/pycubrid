"""Live TLS negative and lifecycle matrix against an ``SSL=ON`` broker (issue #350).

Runs in the dedicated ``integration and tls`` CI lane (``ci.yml`` and
``integration-full.yml``), which provisions CUBRID with ``SSL=ON`` on
``BROKER1`` and a self-signed ``localhost`` certificate. Each scenario runs
against both ``pycubrid.connect`` and ``pycubrid.aio.connect`` through
:class:`tests.helpers.tls_client.DriverClient`, so sync/async divergence shows
up as a failing parameter.

The deterministic, broker-independent part of the matrix (expired
certificates, interrupted handshakes, protocol-version floors, downgrade on
reconnect) lives in ``tests/test_tls_matrix_offline.py``; this module proves
the same invariants hold against a real CUBRID TLS broker:

* verification failures (unknown CA, hostname mismatch) surface as
  :class:`OperationalError` with the same OpenSSL verify code in both drivers;
* a plaintext client is refused by the TLS broker and a TLS client is refused
  by a plaintext broker (no silent downgrade in either direction);
* TLS 1.2 and TLS 1.3 both work when pinned, and ``ssl=True`` verifies the
  broker (certificate and hostname) through the default trust store;
* a read timeout or a dropped transport under TLS leaves the connection
  cleanly closed, and ``ping(reconnect=True)`` comes back over TLS;
* repeated failed and successful TLS connects leak no file descriptors.

Environment (see ``docs/DEVELOPMENT.md``): ``CUBRID_TLS_TEST_HOST/PORT/DB/
USER/PASSWORD``, ``CUBRID_TLS_TEST_CA_FILE`` (the broker certificate/CA),
``CUBRID_TLS_TEST_MISMATCH_HOST`` (reachable name *not* in the certificate),
and ``CUBRID_TLS_TEST_PLAIN_PORT`` (a broker on the same server with
``SSL=OFF``, e.g. the stock ``query_editor`` broker on 30000).
"""

from __future__ import annotations

import gc
import os
import ssl
from typing import Any

import pytest

from pycubrid.exceptions import OperationalError

from .helpers.tls_client import (
    HOSTNAME_MISMATCH_CODES,
    MODES,
    X509_V_ERR_DEPTH_ZERO_SELF_SIGNED_CERT,
    X509_V_ERR_UNABLE_TO_GET_ISSUER_CERT_LOCALLY,
    DriverClient,
    connect_error,
    verify_code,
)

TEST_HOST = os.environ.get("CUBRID_TEST_HOST", "localhost")
TEST_PORT = int(os.environ.get("CUBRID_TEST_PORT", "33000"))
TLS_HOST = os.environ.get("CUBRID_TLS_TEST_HOST", TEST_HOST)
TLS_PORT = int(os.environ.get("CUBRID_TLS_TEST_PORT", str(TEST_PORT)))
TLS_DB = os.environ.get("CUBRID_TLS_TEST_DB", os.environ.get("CUBRID_TEST_DB", "testdb"))
TLS_USER = os.environ.get("CUBRID_TLS_TEST_USER", os.environ.get("CUBRID_TEST_USER", "dba"))
TLS_PASSWORD = os.environ.get(
    "CUBRID_TLS_TEST_PASSWORD", os.environ.get("CUBRID_TEST_PASSWORD", "")
)
TLS_CA_FILE = os.environ.get("CUBRID_TLS_TEST_CA_FILE")
TLS_MISMATCH_HOST = os.environ.get("CUBRID_TLS_TEST_MISMATCH_HOST")
# ssl=True verifies against the default trust store; the lane points it at
# the broker certificate through SSL_CERT_FILE.
DEFAULT_TRUST_FILE = os.environ.get("SSL_CERT_FILE")
_plain_port = os.environ.get("CUBRID_TLS_TEST_PLAIN_PORT")
PLAIN_PORT = int(_plain_port) if _plain_port else None

pytestmark = [pytest.mark.integration, pytest.mark.tls]

_TIMEOUT = 5.0


def _ca_context(**versions: ssl.TLSVersion) -> ssl.SSLContext:
    """Verifying client context that trusts the broker's CA file only."""
    ctx = _empty_trust_context()
    assert TLS_CA_FILE is not None
    ctx.load_verify_locations(cafile=TLS_CA_FILE)
    for name, value in versions.items():
        setattr(ctx, name, value)
    return ctx


def _empty_trust_context() -> ssl.SSLContext:
    """Verifying client context that trusts nothing (independent of SSL_CERT_FILE).

    TLS 1.2 is the floor, as for ``ssl=True``.
    """
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.minimum_version = ssl.TLSVersion.TLSv1_2
    return ctx


def _kwargs(ssl_value: Any, **overrides: Any) -> dict[str, Any]:
    kwargs: dict[str, Any] = dict(
        host=TLS_HOST,
        port=TLS_PORT,
        database=TLS_DB,
        user=TLS_USER,
        password=TLS_PASSWORD,
        ssl=ssl_value,
        connect_timeout=_TIMEOUT,
        read_timeout=_TIMEOUT,
    )
    kwargs.update(overrides)
    return kwargs


@pytest.fixture(scope="session")
def tls_broker() -> None:
    """Skip only when no TLS broker is configured.

    Mirrors the ``integration`` gate in ``tests/conftest.py``: an unconfigured
    run skips, but a configured broker that is unreachable or not serving TLS
    must surface as failures from each test's own connection attempt, never
    as skips.
    """
    if TLS_CA_FILE is None:
        pytest.skip(
            "TLS-enabled CUBRID broker not configured; set CUBRID_TLS_TEST_* "
            "(including CUBRID_TLS_TEST_CA_FILE)"
        )


requires_tls_broker = pytest.mark.usefixtures("tls_broker")
requires_mismatch_host = pytest.mark.skipif(
    TLS_MISMATCH_HOST is None,
    reason="Set CUBRID_TLS_TEST_MISMATCH_HOST to a reachable host not in the broker certificate",
)


@pytest.fixture
def session(request: pytest.FixtureRequest) -> Any:
    """Factory for DriverClient sessions that are always shut down."""
    clients: list[DriverClient] = []

    def open_session(mode: str, ssl_value: Any = None, **overrides: Any) -> DriverClient:
        client = DriverClient(mode, **_kwargs(ssl_value or _ca_context(), **overrides))
        clients.append(client)
        client.connect()
        return client

    yield open_session
    for client in clients:
        try:
            client.shutdown()
        except Exception:  # noqa: BLE001 - teardown must not mask the test result
            pass


# ---------------------------------------------------------------------------
# Verification and negotiation
# ---------------------------------------------------------------------------


@requires_tls_broker
@pytest.mark.parametrize("mode", MODES)
@pytest.mark.parametrize(
    "version",
    [ssl.TLSVersion.TLSv1_2, ssl.TLSVersion.TLSv1_3],
    ids=["TLSv1.2", "TLSv1.3"],
)
def test_pinned_tls_version_round_trips_a_query(
    mode: str, version: ssl.TLSVersion, session: Any
) -> None:
    ctx = _ca_context(minimum_version=version, maximum_version=version)
    client = session(mode, ctx)

    assert client.tls_version() == ("TLSv1.2" if version is ssl.TLSVersion.TLSv1_2 else "TLSv1.3")
    assert client.scalar("SELECT 1") == 1


@requires_tls_broker
@pytest.mark.parametrize("mode", MODES)
def test_untrusted_broker_certificate_is_rejected(mode: str) -> None:
    exc = connect_error(mode, **_kwargs(_empty_trust_context()))

    assert verify_code(exc) in {
        X509_V_ERR_DEPTH_ZERO_SELF_SIGNED_CERT,
        X509_V_ERR_UNABLE_TO_GET_ISSUER_CERT_LOCALLY,
    }


@requires_tls_broker
@requires_mismatch_host
@pytest.mark.parametrize("mode", MODES)
def test_hostname_mismatch_is_rejected(mode: str) -> None:
    exc = connect_error(mode, **_kwargs(_ca_context(), host=TLS_MISMATCH_HOST))

    assert verify_code(exc) in HOSTNAME_MISMATCH_CODES


@requires_tls_broker
@requires_mismatch_host
def test_sync_and_async_report_identical_verification_failures() -> None:
    scenarios = [
        _kwargs(_empty_trust_context()),
        _kwargs(_ca_context(), host=TLS_MISMATCH_HOST),
    ]
    for kwargs in scenarios:
        outcomes = []
        for mode in MODES:
            exc = connect_error(mode, **kwargs)
            outcomes.append((type(exc), type(exc.__cause__), verify_code(exc)))
        assert outcomes[0] == outcomes[1], outcomes


requires_default_trust = pytest.mark.skipif(
    DEFAULT_TRUST_FILE is None,
    reason="Set SSL_CERT_FILE to the broker CA so ssl=True can verify the TLS broker",
)


@requires_tls_broker
@requires_default_trust
@pytest.mark.parametrize("mode", MODES)
def test_ssl_true_verifies_the_broker_against_the_default_trust_store(
    mode: str, session: Any
) -> None:
    client = session(mode, True)

    assert client.tls_version() is not None
    assert client.scalar("SELECT 1") == 1


@requires_tls_broker
@requires_default_trust
@requires_mismatch_host
@pytest.mark.parametrize("mode", MODES)
def test_ssl_true_still_rejects_a_hostname_mismatch(mode: str) -> None:
    exc = connect_error(mode, **_kwargs(True, host=TLS_MISMATCH_HOST))

    assert verify_code(exc) in HOSTNAME_MISMATCH_CODES


@requires_tls_broker
@pytest.mark.parametrize("mode", MODES)
def test_plaintext_client_is_refused_by_tls_broker(mode: str) -> None:
    exc = connect_error(mode, **_kwargs(None))

    assert "rejected handshake" in str(exc)
    assert "ssl=False" in str(exc)


@requires_tls_broker
@pytest.mark.skipif(
    PLAIN_PORT is None,
    reason="Set CUBRID_TLS_TEST_PLAIN_PORT to an SSL=OFF broker port on the TLS test server",
)
@pytest.mark.parametrize("mode", MODES)
def test_tls_client_is_refused_by_plaintext_broker(mode: str) -> None:
    exc = connect_error(mode, **_kwargs(_ca_context(), port=PLAIN_PORT))

    assert "rejected handshake" in str(exc)
    assert "ssl=True" in str(exc)


# ---------------------------------------------------------------------------
# Lifecycle after TLS errors
# ---------------------------------------------------------------------------


@requires_tls_broker
@pytest.mark.parametrize("mode", MODES)
def test_read_timeout_under_tls_closes_then_reconnects_over_tls(mode: str, session: Any) -> None:
    client = session(mode, read_timeout=1.0)

    with pytest.raises(OperationalError):
        client.scalar("SELECT SLEEP(3)")

    assert client.transport_released(), "a timed-out TLS session must be torn down"
    assert client.ping(reconnect=True) is True
    assert client.tls_version() is not None, "reconnect must re-establish TLS"
    assert client.scalar("SELECT 1") == 1


@requires_tls_broker
def test_aio_broker_closing_a_tls_session_logs_no_asyncio_warning(
    session: Any, caplog: pytest.LogCaptureFixture
) -> None:
    """Issue #514 against a real broker: the read-timeout teardown, reconnect
    and close cycle from the issue's reproduction must not log asyncio's
    "returning true from eof_received() has no effect when using ssl".

    Verified to fail against a CUBRID 11.4 ``SSL=ON`` broker without the fix;
    the offline ``test_aio_tls_peer_close_logs_no_asyncio_warning`` pins the
    peer-close path deterministically."""
    with caplog.at_level("WARNING", logger="asyncio"):
        client = session("aio", read_timeout=1.0)
        with pytest.raises(OperationalError):
            client.scalar("SELECT SLEEP(3)")
        assert client.ping(reconnect=True) is True
        assert client.scalar("SELECT 1") == 1
        client.close()

    assert not [r.getMessage() for r in caplog.records if "eof_received" in r.getMessage()]


@requires_tls_broker
@pytest.mark.parametrize("mode", MODES)
def test_dropped_tls_transport_reconnects_over_tls(mode: str, session: Any) -> None:
    client = session(mode)
    original = client.transport()

    client.kill_transport()

    assert client.ping(reconnect=True) is True
    assert client.transport() is not original
    assert client.tls_version() is not None, "reconnect must re-establish TLS"
    assert client.scalar("SELECT 1") == 1


@requires_tls_broker
@pytest.mark.parametrize("mode", MODES)
def test_close_after_tls_error_is_idempotent(mode: str, session: Any) -> None:
    client = session(mode, read_timeout=1.0)
    with pytest.raises(OperationalError):
        client.scalar("SELECT SLEEP(3)")

    client.close()
    client.close()

    assert client.transport_released()


@requires_tls_broker
def test_sync_out_tran_probe_keeps_tls_session(session: Any) -> None:
    """Sync twin of ``test_aio_ssl_out_tran_keeps_tls_session``.

    An OUT_TRAN CAS_INFO makes the driver CHECK_CAS-probe before the next
    request; a healthy CAS must keep the *same* TLS socket, not reconnect.
    """
    client = session("sync")
    conn = client.conn
    original = conn._socket
    conn._cas_info = bytes([conn._CAS_INFO_STATUS_INACTIVE, *conn._cas_info[1:]])

    assert client.scalar("SELECT 1") == 1
    assert conn._socket is original
    assert client.tls_version() is not None


# ---------------------------------------------------------------------------
# Resource hygiene
# ---------------------------------------------------------------------------


def _fd_count() -> int:
    gc.collect()
    try:
        return len(os.listdir("/proc/self/fd"))
    except OSError:
        pytest.skip("cannot count file descriptors on this platform")
        raise  # unreachable; satisfies the type checker


_FD_TOLERANCE = 4


@requires_tls_broker
@pytest.mark.parametrize("mode", MODES)
def test_tls_connect_failures_and_cycles_do_not_leak_fds(mode: str) -> None:
    def cycle() -> None:
        connect_error(mode, **_kwargs(_empty_trust_context()))
        connect_error(mode, **_kwargs(None))
        client = DriverClient(mode, **_kwargs(_ca_context()))
        try:
            client.connect()
            assert client.scalar("SELECT 1") == 1
        finally:
            client.shutdown()

    for _ in range(3):  # warm-up to steady state
        cycle()
    baseline = _fd_count()

    for _ in range(20):
        cycle()

    assert _fd_count() <= baseline + _FD_TOLERANCE
