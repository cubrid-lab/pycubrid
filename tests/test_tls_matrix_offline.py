"""Offline TLS negative and lifecycle matrix, sync and asyncio (issue #350).

Every case drives the driver's *real* TLS code path (``SSLContext.wrap_socket``
for sync, ``loop.start_tls`` plus the Python 3.10 preflight probe for asyncio)
against an in-process OpenSSL peer (:mod:`tests.helpers.tls_broker`). No
CUBRID server is needed, so the matrix runs in the fast offline suite; the
live counterpart against a real ``SSL=ON`` broker is
``tests/test_tls_matrix_integration.py`` (``integration`` + ``tls`` lane).

Invariants asserted for both drivers (issue #350 acceptance):

* **no plaintext fallback** — after TLS is requested the client only ever sends
  the ``CUBRS`` magic and a TLS record; the password never crosses the wire in
  the clear, even when the broker rejects TLS or answers in plaintext;
* **hostname validation is never silently disabled** — for ``ssl=True`` and for
  a caller ``SSLContext`` (which the driver must not mutate);
* **reconnect retains the requested security posture** — a reconnect re-runs
  the TLS upgrade and refuses a downgraded broker;
* **sync/async equivalent errors** — the same :class:`OperationalError` with
  the same underlying certificate-verification code;
* **failed TLS transport fully closed** — no socket reference kept and no
  file descriptor leaked after any failure.

The async connect hang on an interrupted handshake (#513) has its own focused
regression suite in ``tests/test_aio_tls_handshake_hang.py`` (task/warning
hygiene per failure point); this matrix only adds the security-posture checks
for those faults. Known follow-ups that are *not* asserted here: a sync TLS
handshake with ``read_timeout=None`` is unbounded, and a Python 3.10 peer reset
can leave the preflight probe's socket to the GC (both tracked in #535).
"""

from __future__ import annotations

import asyncio
import gc
import os
import ssl
import time
import warnings
from collections.abc import Callable, Collection, Iterator
from pathlib import Path
from typing import Any

import pytest

from pycubrid.exceptions import OperationalError

from .helpers.tls_broker import (
    CA_FILE,
    CLOSE_AFTER_TLS,
    CLOSE_BEFORE_TLS,
    CLOSE_MID_HANDSHAKE,
    EXPIRED_CERT,
    MALFORMED_RECORD,
    PLAINTEXT_REPLY,
    SELF_SIGNED_CERT,
    STALL_HANDSHAKE,
    TLS_HANDSHAKE_RECORD,
    TLS_OK,
    WRONG_HOST_CERT,
    TlsBroker,
    client_context,
    run_tls_broker,
    server_context,
)
from .helpers.tls_client import (
    HOSTNAME_MISMATCH_CODES,
    MODES,
    X509_V_ERR_CERT_HAS_EXPIRED,
    X509_V_ERR_DEPTH_ZERO_SELF_SIGNED_CERT,
    X509_V_ERR_UNABLE_TO_GET_ISSUER_CERT_LOCALLY,
    DriverClient,
    connect_error,
    verify_code,
)

HOST = "127.0.0.1"
# Distinctive password: if it ever shows up in the broker's plaintext capture,
# credentials crossed the wire unencrypted after TLS was requested.
CANARY_PASSWORD = "tls-canary-3f9a1c"
_TIMEOUT = 2.0


def _kwargs(
    port: int,
    ssl_value: bool | ssl.SSLContext | None,
    *,
    host: str = HOST,
    read_timeout: float = _TIMEOUT,
) -> dict[str, Any]:
    return dict(
        host=host,
        port=port,
        database="testdb",
        user="dba",
        password=CANARY_PASSWORD,
        ssl=ssl_value,
        connect_timeout=_TIMEOUT,
        read_timeout=read_timeout,
        # Skip the post-OPEN_DB escape probe: the fake broker answers every
        # request with a bare OK, not a result set.
        no_backslash_escapes=True,
    )


def Client(
    mode: str, port: int, ssl_value: bool | ssl.SSLContext | None, **kw: Any
) -> DriverClient:
    return DriverClient(mode, **_kwargs(port, ssl_value, **kw))


def _connect_error(
    mode: str, port: int, ssl_value: bool | ssl.SSLContext | None, **kw: Any
) -> OperationalError:
    return connect_error(mode, **_kwargs(port, ssl_value, **kw))


_verify_code = verify_code


def _assert_no_plaintext_fallback(broker: TlsBroker) -> None:
    assert broker.records, "the client never reached the broker"
    for record in broker.records:
        assert record.magic == b"CUBRS", "TLS was requested; the handshake must say so"
        assert CANARY_PASSWORD.encode() not in record.plaintext_after_handshake
        # Nothing beyond a TLS record may follow the CUBRS handshake.
        if record.first_post_handshake_byte is not None:
            assert record.first_post_handshake_byte == TLS_HANDSHAKE_RECORD


@pytest.fixture
def isolated_default_trust(monkeypatch: pytest.MonkeyPatch) -> None:
    """Make ``ssl=True`` see the platform trust store, never the test CA."""
    monkeypatch.delenv("SSL_CERT_FILE", raising=False)
    monkeypatch.delenv("SSL_CERT_DIR", raising=False)


@pytest.fixture
def trust_test_ca_by_default(monkeypatch: pytest.MonkeyPatch) -> None:
    """Point the default trust store (used by ``ssl=True``) at the test CA."""
    monkeypatch.setenv("SSL_CERT_FILE", str(CA_FILE))
    monkeypatch.delenv("SSL_CERT_DIR", raising=False)


# ---------------------------------------------------------------------------
# Certificate and hostname verification
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("mode", MODES)
@pytest.mark.parametrize(
    ("cert", "expected_codes"),
    [
        pytest.param(
            SELF_SIGNED_CERT, {X509_V_ERR_DEPTH_ZERO_SELF_SIGNED_CERT}, id="unknown-ca-self-signed"
        ),
        pytest.param(EXPIRED_CERT, {X509_V_ERR_CERT_HAS_EXPIRED}, id="expired"),
        pytest.param(
            WRONG_HOST_CERT,
            HOSTNAME_MISMATCH_CODES,
            id="hostname-mismatch",
        ),
    ],
)
def test_untrusted_certificate_is_rejected_with_verification_cause(
    mode: str, cert: Path, expected_codes: Collection[int]
) -> None:
    with run_tls_broker(tls_context=server_context(cert)) as broker:
        exc = _connect_error(mode, broker.port, client_context())

    assert _verify_code(exc) in expected_codes
    _assert_no_plaintext_fallback(broker)


@pytest.mark.parametrize(
    ("cert", "host"),
    [
        pytest.param(SELF_SIGNED_CERT, HOST, id="unknown-ca"),
        pytest.param(EXPIRED_CERT, HOST, id="expired"),
        pytest.param(WRONG_HOST_CERT, HOST, id="hostname-mismatch-ip"),
        pytest.param(WRONG_HOST_CERT, "localhost", id="hostname-mismatch-dns"),
    ],
)
def test_sync_and_async_report_identical_verification_failure(cert: Path, host: str) -> None:
    outcomes = []
    for mode in MODES:
        with run_tls_broker(tls_context=server_context(cert)) as broker:
            exc = _connect_error(mode, broker.port, client_context(), host=host)
        outcomes.append((type(exc), type(exc.__cause__), _verify_code(exc)))

    assert outcomes[0] == outcomes[1]


@pytest.mark.parametrize("mode", MODES)
def test_ssl_true_does_not_trust_private_ca(mode: str, isolated_default_trust: None) -> None:
    with run_tls_broker() as broker:
        exc = _connect_error(mode, broker.port, True)

    assert _verify_code(exc) == X509_V_ERR_UNABLE_TO_GET_ISSUER_CERT_LOCALLY
    _assert_no_plaintext_fallback(broker)


@pytest.mark.parametrize("mode", MODES)
def test_ssl_true_connects_over_tls_when_ca_is_trusted(
    mode: str, trust_test_ca_by_default: None
) -> None:
    with run_tls_broker() as broker:
        client = Client(mode, broker.port, True)
        try:
            client.connect()
            assert client.tls_version() is not None
        finally:
            client.shutdown()

    _assert_no_plaintext_fallback(broker)


@pytest.mark.parametrize("mode", MODES)
def test_ssl_true_still_checks_hostname_when_ca_is_trusted(
    mode: str, trust_test_ca_by_default: None
) -> None:
    with run_tls_broker(tls_context=server_context(WRONG_HOST_CERT)) as broker:
        exc = _connect_error(mode, broker.port, True)

    assert _verify_code(exc) in HOSTNAME_MISMATCH_CODES


@pytest.mark.parametrize("mode", MODES)
def test_caller_ssl_context_is_used_unmodified(mode: str) -> None:
    ctx = client_context(minimum_version=ssl.TLSVersion.TLSv1_2)
    before = (ctx.check_hostname, ctx.verify_mode, ctx.minimum_version, ctx.maximum_version)

    with run_tls_broker() as broker:
        client = Client(mode, broker.port, ctx)
        try:
            client.connect()
        finally:
            client.shutdown()
    with run_tls_broker(tls_context=server_context(WRONG_HOST_CERT)) as broker:
        _connect_error(mode, broker.port, ctx)

    assert (ctx.check_hostname, ctx.verify_mode, ctx.minimum_version, ctx.maximum_version) == (
        before
    )
    assert ctx.check_hostname is True
    assert ctx.verify_mode == ssl.CERT_REQUIRED


# ---------------------------------------------------------------------------
# Protocol versions
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("mode", MODES)
@pytest.mark.parametrize(
    "version",
    [ssl.TLSVersion.TLSv1_2, ssl.TLSVersion.TLSv1_3],
    ids=["TLSv1.2", "TLSv1.3"],
)
def test_pinned_tls_version_is_negotiated(mode: str, version: ssl.TLSVersion) -> None:
    tls_ctx = server_context(minimum_version=version, maximum_version=version)
    with run_tls_broker(tls_context=tls_ctx) as broker:
        client = Client(mode, broker.port, client_context())
        try:
            client.connect()
            negotiated = client.tls_version()
        finally:
            client.shutdown()

    expected = "TLSv1.2" if version is ssl.TLSVersion.TLSv1_2 else "TLSv1.3"
    assert negotiated == expected
    assert broker.records[-1].tls_version == expected


@pytest.mark.parametrize("mode", MODES)
def test_minimum_tls_version_mismatch_is_rejected(mode: str) -> None:
    tls_ctx = server_context(maximum_version=ssl.TLSVersion.TLSv1_2)
    with run_tls_broker(tls_context=tls_ctx) as broker:
        exc = _connect_error(
            mode, broker.port, client_context(minimum_version=ssl.TLSVersion.TLSv1_3)
        )

    assert isinstance(exc.__cause__, ssl.SSLError)
    _assert_no_plaintext_fallback(broker)


@pytest.mark.parametrize("mode", MODES)
def test_ssl_true_refuses_a_broker_capped_below_tls12(
    mode: str, trust_test_ca_by_default: None
) -> None:
    """``ssl=True`` pins TLS 1.2 as the floor; a TLS 1.1-only broker must not pass."""
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)  # TLSVersion.TLSv1_1
            tls_ctx = server_context(
                minimum_version=ssl.TLSVersion.TLSv1_1, maximum_version=ssl.TLSVersion.TLSv1_1
            )
        tls_ctx.set_ciphers("DEFAULT:@SECLEVEL=0")
    except (ssl.SSLError, ValueError):
        pytest.skip("local OpenSSL cannot build a TLS 1.1 server")
    with run_tls_broker(tls_context=tls_ctx) as broker:
        exc = _connect_error(mode, broker.port, True)

    assert isinstance(exc.__cause__, ssl.SSLError)


# ---------------------------------------------------------------------------
# Broker-side faults around the TLS upgrade
# ---------------------------------------------------------------------------

_FAULTS = [
    pytest.param(CLOSE_BEFORE_TLS, id="close-before-tls"),
    pytest.param(CLOSE_MID_HANDSHAKE, id="close-mid-handshake"),
    pytest.param(CLOSE_AFTER_TLS, id="close-after-handshake"),
    pytest.param(PLAINTEXT_REPLY, id="tls-to-non-tls-broker"),
    pytest.param(MALFORMED_RECORD, id="malformed-tls-record"),
]


@pytest.mark.parametrize("mode", MODES)
@pytest.mark.parametrize("behavior", _FAULTS)
def test_transport_fault_during_tls_upgrade_raises_operational_error(
    mode: str, behavior: str
) -> None:
    with run_tls_broker(behavior) as broker:
        started = time.monotonic()
        _connect_error(mode, broker.port, client_context())
        elapsed = time.monotonic() - started

    assert elapsed < _TIMEOUT * 2, "a closed/garbled peer must fail fast, not time out"
    _assert_no_plaintext_fallback(broker)


@pytest.mark.parametrize("mode", MODES)
def test_close_before_tls_happens_before_any_client_hello_byte(mode: str) -> None:
    """The pre-TLS close must not wait for the ClientHello; that would make it a
    mid-handshake close (covered separately)."""
    with run_tls_broker(CLOSE_BEFORE_TLS) as broker:
        _connect_error(mode, broker.port, client_context())

    assert broker.records, "the client never reached the broker"
    for record in broker.records:
        assert record.magic == b"CUBRS"
        assert record.first_post_handshake_byte is None
        assert not record.plaintext_after_handshake
        assert not record.tls_completed


@pytest.mark.parametrize("mode", MODES)
def test_stalled_tls_handshake_is_bounded_by_read_timeout(mode: str) -> None:
    with run_tls_broker(STALL_HANDSHAKE) as broker:
        started = time.monotonic()
        exc = _connect_error(mode, broker.port, client_context(), read_timeout=0.5)
        elapsed = time.monotonic() - started

    # asyncio.TimeoutError is only an OSError subclass from Python 3.11 on.
    assert isinstance(exc.__cause__, (OSError, asyncio.TimeoutError)), repr(exc.__cause__)
    assert elapsed < 3.0
    _assert_no_plaintext_fallback(broker)


@pytest.mark.parametrize("mode", MODES)
def test_broker_rejecting_tls_is_not_retried_in_plaintext(mode: str) -> None:
    with run_tls_broker(handshake_status=-1103) as broker:
        exc = _connect_error(mode, broker.port, client_context())

    assert "rejected handshake" in str(exc)
    assert "ssl=True" in str(exc)
    _assert_no_plaintext_fallback(broker)
    assert all(not record.plaintext_after_handshake for record in broker.records)


@pytest.mark.parametrize("mode", MODES)
def test_successful_session_sends_only_tls_after_handshake(mode: str) -> None:
    with run_tls_broker() as broker:
        client = Client(mode, broker.port, client_context())
        try:
            client.connect()
            assert client.ping(reconnect=False) is True
        finally:
            client.shutdown()

    _assert_no_plaintext_fallback(broker)
    assert broker.records[-1].tls_completed


# ---------------------------------------------------------------------------
# Lifecycle: reconnect, downgrade on reconnect, close after TLS errors
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("mode", MODES)
def test_reconnect_after_dropped_tls_session_upgrades_again(mode: str) -> None:
    with run_tls_broker() as broker:
        client = Client(mode, broker.port, client_context())
        try:
            client.connect()
            first = len(broker.records)
            broker.drop_active()

            assert client.ping(reconnect=True) is True
            assert client.tls_version() is not None
        finally:
            client.shutdown()

    assert len(broker.records) > first, "ping(reconnect=True) must open a new session"
    assert all(record.tls_completed for record in broker.records[first:])
    _assert_no_plaintext_fallback(broker)


def _downgrade_to_plaintext_broker(broker: TlsBroker) -> None:
    broker.behavior = PLAINTEXT_REPLY


def _downgrade_to_rejecting_broker(broker: TlsBroker) -> None:
    broker.handshake_status = -1103


def _downgrade_to_untrusted_certificate(broker: TlsBroker) -> None:
    broker.tls_context = server_context(SELF_SIGNED_CERT)


def _restore_healthy_broker(broker: TlsBroker) -> None:
    broker.behavior = TLS_OK
    broker.handshake_status = 0
    broker.tls_context = server_context()


@pytest.mark.parametrize("mode", MODES)
@pytest.mark.parametrize(
    "downgrade",
    [
        pytest.param(_downgrade_to_plaintext_broker, id="plaintext-broker"),
        pytest.param(_downgrade_to_rejecting_broker, id="broker-rejects-tls"),
        pytest.param(_downgrade_to_untrusted_certificate, id="untrusted-cert"),
    ],
)
def test_reconnect_refuses_downgraded_broker_then_recovers(
    mode: str, downgrade: Callable[[TlsBroker], None]
) -> None:
    with run_tls_broker() as broker:
        client = Client(mode, broker.port, client_context())
        try:
            client.connect()
            broker.drop_active()
            downgrade(broker)

            assert client.ping(reconnect=True) is False
            assert client.transport_released(), "failed TLS reconnect left a transport open"

            _restore_healthy_broker(broker)
            assert client.ping(reconnect=True) is True
            assert client.tls_version() is not None
        finally:
            client.shutdown()

    _assert_no_plaintext_fallback(broker)


@pytest.mark.parametrize("mode", MODES)
def test_close_after_tls_failure_is_safe_and_idempotent(mode: str) -> None:
    with run_tls_broker() as broker:
        client = Client(mode, broker.port, client_context())
        try:
            client.connect()
            broker.drop_active()
            broker.behavior = CLOSE_MID_HANDSHAKE
            assert client.ping(reconnect=True) is False

            client.close()
            client.close()
            assert client.transport_released()
        finally:
            client.shutdown()


# ---------------------------------------------------------------------------
# Resource hygiene
# ---------------------------------------------------------------------------


def _fd_dir() -> str:
    for candidate in ("/proc/self/fd", "/dev/fd"):
        if os.path.isdir(candidate):
            return candidate
    pytest.skip("cannot count file descriptors on this platform")
    raise AssertionError("unreachable")


def _fd_count(fd_dir: str) -> int:
    gc.collect()
    return len(os.listdir(fd_dir))


def _cycle_failures(mode: str) -> Iterator[Callable[[], None]]:
    scenarios: list[tuple[str, Path | None, float]] = [
        (TLS_OK, SELF_SIGNED_CERT, _TIMEOUT),
        (TLS_OK, WRONG_HOST_CERT, _TIMEOUT),
        (CLOSE_MID_HANDSHAKE, None, _TIMEOUT),
        (CLOSE_AFTER_TLS, None, _TIMEOUT),
        (PLAINTEXT_REPLY, None, _TIMEOUT),
        (STALL_HANDSHAKE, None, 0.2),
    ]
    for behavior, cert, read_timeout in scenarios:

        def attempt(
            behavior: str = behavior, cert: Path | None = cert, read_timeout: float = read_timeout
        ) -> None:
            tls_ctx = server_context(cert) if cert is not None else None
            with run_tls_broker(behavior, tls_context=tls_ctx) as broker:
                _connect_error(mode, broker.port, client_context(), read_timeout=read_timeout)

        yield attempt


@pytest.mark.parametrize("mode", MODES)
def test_failed_tls_connects_leak_no_file_descriptors(mode: str) -> None:
    fd_dir = _fd_dir()
    for attempt in _cycle_failures(mode):  # warm-up: imports, loop policy, caches
        attempt()
    baseline = _fd_count(fd_dir)

    for _ in range(2):
        for attempt in _cycle_failures(mode):
            attempt()

    assert _fd_count(fd_dir) <= baseline


# ---------------------------------------------------------------------------
# Permanent regressions for defects found by this matrix
# ---------------------------------------------------------------------------


def test_aio_tls_peer_close_logs_no_asyncio_warning(caplog: pytest.LogCaptureFixture) -> None:
    """Issue #514: a broker closing an upgraded TLS stream must not log a warning.

    The stream protocol was created for a plaintext transport and kept
    ``_over_ssl = False`` after ``start_tls``, so every TLS peer EOF made
    asyncio log "returning true from eof_received() has no effect when using
    ssl" at WARNING level.
    """
    with run_tls_broker() as broker:
        client = Client("aio", broker.port, client_context())
        try:
            client.connect()
            with caplog.at_level("WARNING", logger="asyncio"):
                broker.drop_active()
                assert client.ping(reconnect=True) is True
        finally:
            client.shutdown()

    assert not [r for r in caplog.records if "eof_received" in r.getMessage()]
