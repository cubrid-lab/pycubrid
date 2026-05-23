"""Offline coverage for the async TLS upgrade and handshake paths.

These tests close the coverage gap identified in
`#158 <https://github.com/cubrid-lab/pycubrid/issues/158>`_: the async
TLS upgrade path was previously only exercised end-to-end via
integration tests, so a regression silently disabling hostname
verification or breaking the redirect-skips-second-handshake invariant
could pass offline CI. Each test runs without a broker.
"""

from __future__ import annotations

import asyncio
import ssl as ssl_module
import struct
import sys
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from pycubrid.aio.connection import AsyncConnection
from pycubrid.exceptions import OperationalError


def _build_handshake_response(port: int = 0) -> bytes:
    return struct.pack(">i", port)


def _build_open_db_response(cas_info: bytes = b"\x01\x01\x02\x03", session_id: int = 1234) -> bytes:
    body = cas_info + struct.pack(">i", 0)
    body += b"\x00" * 8
    body += struct.pack(">i", session_id)
    data_length = struct.pack(">i", len(body) - 4)
    return data_length + body


def _make_stream_pair(read_chunks: list[bytes]) -> tuple[MagicMock, MagicMock]:
    reader = MagicMock(spec=asyncio.StreamReader)
    reader.readexactly = AsyncMock(side_effect=list(read_chunks))
    transport = MagicMock(name="transport")
    transport.get_protocol = MagicMock(return_value=MagicMock(name="protocol"))
    writer = MagicMock(spec=asyncio.StreamWriter)
    writer.transport = transport
    writer.write = MagicMock()
    writer.drain = AsyncMock()
    writer.close = MagicMock()
    writer.wait_closed = AsyncMock()
    writer.get_extra_info = MagicMock(return_value=None)
    return reader, writer


def _make_pre_tls_async_connection(
    *, host: str = "broker.example.com", read_timeout: float | None = 7.5
) -> tuple[AsyncConnection, MagicMock, MagicMock]:
    """Build an AsyncConnection sitting just before the TLS upgrade step.

    The connection has fresh reader/writer streams and a configured SSL
    context, mimicking the state inside :meth:`_do_connect_handshake` right
    after the plaintext CUBRS handshake completes successfully.
    """
    ctx = ssl_module.create_default_context()
    conn = AsyncConnection(
        host=host,
        port=33000,
        database="testdb",
        user="dba",
        password="",
        ssl=ctx,
        read_timeout=read_timeout,
    )
    reader, writer = _make_stream_pair([])
    conn._reader = reader
    conn._writer = writer
    return conn, reader, writer


@pytest.mark.asyncio
async def test_upgrade_to_tls_calls_start_tls_with_expected_kwargs() -> None:
    """Regression guard: ``_upgrade_to_tls`` must forward the configured
    ``SSLContext``, the connection host as ``server_hostname``, and the
    resolved ``ssl_handshake_timeout`` to ``loop.start_tls``. A silent
    removal of any kwarg here would disable hostname verification or
    re-introduce an unbounded handshake hang."""
    conn, _reader, writer = _make_pre_tls_async_connection(read_timeout=4.25)
    old_transport = writer.transport
    new_transport = MagicMock(name="new_tls_transport")

    fake_loop = MagicMock(name="loop")
    fake_loop.start_tls = AsyncMock(return_value=new_transport)

    with patch("pycubrid.aio.connection.asyncio.get_running_loop", return_value=fake_loop):
        await conn._upgrade_to_tls()

    fake_loop.start_tls.assert_awaited_once()
    args, kwargs = fake_loop.start_tls.call_args
    assert args[0] is old_transport
    assert args[1] is old_transport.get_protocol.return_value
    assert args[2] is conn._ssl_context
    assert kwargs == {
        "server_hostname": "broker.example.com",
        "ssl_handshake_timeout": 4.25,
    }
    assert conn._writer._transport is new_transport  # type: ignore[attr-defined]
    assert conn._reader._transport is new_transport  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_upgrade_to_tls_uses_default_handshake_timeout_when_unset() -> None:
    """When ``read_timeout`` is ``None``, the upgrade must still bound the
    handshake via the documented 10-second default; otherwise a stalled
    peer could hang the event loop indefinitely."""
    conn, _reader, writer = _make_pre_tls_async_connection(read_timeout=None)
    fake_loop = MagicMock(name="loop")
    fake_loop.start_tls = AsyncMock(return_value=MagicMock())

    with patch("pycubrid.aio.connection.asyncio.get_running_loop", return_value=fake_loop):
        await conn._upgrade_to_tls()

    _, kwargs = fake_loop.start_tls.call_args
    assert kwargs["ssl_handshake_timeout"] == 10.0


@pytest.mark.asyncio
async def test_upgrade_to_tls_aborts_transport_on_start_tls_failure() -> None:
    """Regression guard for the ``except BaseException`` cleanup block:
    when ``loop.start_tls`` raises, the pre-TLS transport must be aborted
    exactly once and the original exception must propagate unchanged so the
    outer connect-handshake error mapping (in ``_connect_locked``) can wrap
    it as ``OperationalError``."""
    conn, _reader, writer = _make_pre_tls_async_connection()
    old_transport = writer.transport
    boom = ssl_module.SSLError("handshake failed")

    fake_loop = MagicMock(name="loop")
    fake_loop.start_tls = AsyncMock(side_effect=boom)

    with patch("pycubrid.aio.connection.asyncio.get_running_loop", return_value=fake_loop):
        with pytest.raises(ssl_module.SSLError) as excinfo:
            await conn._upgrade_to_tls()

    assert excinfo.value is boom
    old_transport.abort.assert_called_once_with()


@pytest.mark.asyncio
async def test_upgrade_to_tls_raises_operational_error_on_none_transport() -> None:
    """``loop.start_tls`` is documented to return ``None`` only on
    cancellation, but the contract is enforced defensively: a ``None``
    return must abort the pre-TLS transport and surface as
    ``OperationalError`` rather than silently leaving an unwrapped socket."""
    conn, _reader, writer = _make_pre_tls_async_connection()
    old_transport = writer.transport
    fake_loop = MagicMock(name="loop")
    fake_loop.start_tls = AsyncMock(return_value=None)

    with patch("pycubrid.aio.connection.asyncio.get_running_loop", return_value=fake_loop):
        with pytest.raises(OperationalError, match="TLS upgrade returned no transport"):
            await conn._upgrade_to_tls()

    old_transport.abort.assert_called_once_with()


@pytest.mark.asyncio
async def test_connect_handshake_short_read_raises_operational_error() -> None:
    """Regression guard for incomplete-read on the initial 4-byte
    ``CUBRS``/``CUBRK`` broker status response: closing the socket mid-
    handshake or returning fewer than 4 bytes must surface as
    ``OperationalError`` and leave the connection disconnected, not hang."""
    reader, writer = _make_stream_pair([])
    reader.readexactly = AsyncMock(
        side_effect=asyncio.IncompleteReadError(partial=b"\x00\x00", expected=4)
    )

    async def fake_open_connection(host: str, port: int) -> tuple[MagicMock, MagicMock]:
        del host, port
        return reader, writer

    conn = AsyncConnection("broker", 33000, "testdb", "dba", "", ssl=False, read_timeout=1.0)
    with patch.object(conn, "_open_connection", side_effect=fake_open_connection):
        with pytest.raises(OperationalError):
            await conn.connect()

    assert conn._connected is False


@pytest.mark.asyncio
async def test_connect_handshake_eof_raises_operational_error() -> None:
    """Same contract as the short-read case, but for the broker closing
    the socket cleanly (zero-byte read) before delivering any handshake
    bytes. Both failure modes must produce ``OperationalError``."""
    reader, writer = _make_stream_pair([])
    reader.readexactly = AsyncMock(side_effect=asyncio.IncompleteReadError(partial=b"", expected=4))

    async def fake_open_connection(host: str, port: int) -> tuple[MagicMock, MagicMock]:
        del host, port
        return reader, writer

    conn = AsyncConnection("broker", 33000, "testdb", "dba", "", ssl=False, read_timeout=1.0)
    with patch.object(conn, "_open_connection", side_effect=fake_open_connection):
        with pytest.raises(OperationalError):
            await conn.connect()

    assert conn._connected is False
    assert conn._writer is None


@pytest.mark.asyncio
async def test_async_redirect_during_tls_connect_skips_second_handshake() -> None:
    """Async parity for ``test_connect_redirect_sends_no_second_handshake``
    in ``tests/test_ssl.py``: per CUBRID JDBC, when the broker returns a
    redirect (``new_connection_port > 0``) on the initial ``CUBRS`` TLS
    handshake, the client reconnects to the redirected CAS worker on the
    new port and proceeds directly to TLS upgrade + ``OPEN_DATABASE``
    *without* sending a second handshake. Re-introducing one here would
    break every TLS-enabled redirected deployment."""
    first_reader, first_writer = _make_stream_pair([_build_handshake_response(port=33100)])
    open_db_frame = _build_open_db_response()
    second_reader, second_writer = _make_stream_pair([open_db_frame[:4], open_db_frame[4:]])

    pairs = iter([(first_reader, first_writer), (second_reader, second_writer)])

    async def fake_open_connection(host: str, port: int) -> tuple[MagicMock, MagicMock]:
        del host, port
        return next(pairs)

    ctx = ssl_module.create_default_context()
    conn = AsyncConnection("broker", 33000, "testdb", "dba", "", ssl=ctx, read_timeout=2.0)

    async def fake_upgrade() -> None:
        return None

    with (
        patch.object(conn, "_open_connection", side_effect=fake_open_connection),
        patch.object(conn, "_upgrade_to_tls", side_effect=fake_upgrade) as upgrade_mock,
    ):
        await conn.connect()

    first_payload = b"".join(call.args[0] for call in first_writer.write.call_args_list)
    assert b"CUBRS" in first_payload, "TLS-requested handshake must use the CUBRS magic"

    second_payload = b"".join(call.args[0] for call in second_writer.write.call_args_list)
    assert b"CUBRS" not in second_payload, "redirected socket must not re-send a handshake"
    assert b"CUBRK" not in second_payload

    upgrade_mock.assert_awaited_once()
    assert conn._connected is True


@pytest.mark.asyncio
async def test_probe_tls_verification_is_noop_on_py311_plus() -> None:
    """The Py3.10-only preflight probe must not run on 3.11+ — those
    versions raise :class:`ssl.SSLCertVerificationError` promptly from
    :meth:`asyncio.AbstractEventLoop.start_tls`. A misfire here would
    add an extra TCP round-trip per connect on the entire supported
    matrix."""
    conn, _reader, _writer = _make_pre_tls_async_connection()
    fake_version = (3, 11, 0, "final", 0)

    with (
        patch("pycubrid.aio.connection.sys.version_info", fake_version),
        patch.object(AsyncConnection, "_probe_tls_verification_sync") as sync_probe,
    ):
        await conn._maybe_probe_tls_verification(effective_port=33000, followed_redirect=False)

    sync_probe.assert_not_called()


@pytest.mark.asyncio
async def test_probe_tls_verification_runs_in_executor_on_py310() -> None:
    """On Python 3.10 the probe must execute the synchronous
    :meth:`_probe_tls_verification_sync` helper via
    :meth:`asyncio.AbstractEventLoop.run_in_executor`, forwarding the
    effective post-redirect port, the configured ``SSLContext``, the
    ``needs_handshake_replay`` flag (inverse of ``followed_redirect``),
    and the resolved timeouts. Drift here would either re-introduce the
    3.10 hang or bypass cert verification on the workaround path."""
    conn, _reader, _writer = _make_pre_tls_async_connection(read_timeout=3.5)
    conn._connect_timeout = 2.0

    fake_loop = MagicMock(name="loop")
    fake_loop.run_in_executor = AsyncMock(return_value=None)
    fake_version = (3, 10, 9, "final", 0)

    with (
        patch("pycubrid.aio.connection.sys.version_info", fake_version),
        patch("pycubrid.aio.connection.asyncio.get_running_loop", return_value=fake_loop),
    ):
        await conn._maybe_probe_tls_verification(effective_port=33100, followed_redirect=True)

    fake_loop.run_in_executor.assert_awaited_once()
    args = fake_loop.run_in_executor.await_args.args
    assert args[0] is None, "default executor must be used"
    assert args[1] is AsyncConnection._probe_tls_verification_sync
    assert args[2] == "broker.example.com"
    assert args[3] == 33100
    assert args[4] is conn._ssl_context
    assert args[5] is False, "followed_redirect=True implies no handshake replay"
    assert args[6] == 2.0
    assert args[7] == 3.5


@pytest.mark.asyncio
async def test_probe_tls_verification_marks_replay_when_no_redirect() -> None:
    """The replay flag must be the inverse of ``followed_redirect`` so the
    sync probe replays the CUBRS handshake exactly when the real
    connection did. Mis-flagging it would either skip the handshake on
    broker-only paths (causing the broker to reject the raw TLS bytes)
    or replay on already-redirected worker ports (which expect TLS
    immediately)."""
    conn, _reader, _writer = _make_pre_tls_async_connection()
    fake_loop = MagicMock(name="loop")
    fake_loop.run_in_executor = AsyncMock(return_value=None)
    fake_version = (3, 10, 12, "final", 0)

    with (
        patch("pycubrid.aio.connection.sys.version_info", fake_version),
        patch("pycubrid.aio.connection.asyncio.get_running_loop", return_value=fake_loop),
    ):
        await conn._maybe_probe_tls_verification(effective_port=33000, followed_redirect=False)

    args = fake_loop.run_in_executor.await_args.args
    assert args[5] is True, "no-redirect path must replay the CUBRS handshake"


@pytest.mark.asyncio
async def test_handshake_invokes_probe_before_upgrade_on_py310() -> None:
    """End-to-end gating: on Python 3.10, ``_do_connect_handshake`` must
    invoke the preflight probe **after** redirect resolution and
    **before** ``_upgrade_to_tls``. If the order inverts, the probe
    cannot save us from the upstream hang because ``start_tls`` will
    already be in flight."""
    reader, writer = _make_stream_pair([_build_handshake_response(port=0), b"", b""])
    open_db_frame = _build_open_db_response()
    reader.readexactly = AsyncMock(
        side_effect=[_build_handshake_response(port=0), open_db_frame[:4], open_db_frame[4:]]
    )

    ctx = ssl_module.create_default_context()
    conn = AsyncConnection("broker", 33000, "testdb", "dba", "", ssl=ctx, read_timeout=2.0)

    call_order: list[str] = []

    async def fake_probe(*, effective_port: int, followed_redirect: bool) -> None:
        del effective_port, followed_redirect
        call_order.append("probe")

    async def fake_upgrade() -> None:
        call_order.append("upgrade")

    async def fake_open(host: str, port: int) -> tuple[MagicMock, MagicMock]:
        del host, port
        return reader, writer

    fake_version = (3, 10, 9, "final", 0)
    with (
        patch("pycubrid.aio.connection.sys.version_info", fake_version),
        patch.object(conn, "_open_connection", side_effect=fake_open),
        patch.object(conn, "_maybe_probe_tls_verification", side_effect=fake_probe),
        patch.object(conn, "_upgrade_to_tls", side_effect=fake_upgrade),
    ):
        await conn.connect()

    assert call_order == ["probe", "upgrade"]


def test_recv_exact_sync_returns_bytes_and_raises_on_eof() -> None:
    """Defensive read helper used by the preflight probe: a clean EOF
    mid-read must raise :class:`OSError` so the outer probe surfaces it
    as ``OperationalError`` rather than silently returning a short
    buffer that the broker handshake parser would then mis-decode."""
    full = struct.pack(">i", 1234)
    sock = MagicMock(spec=["recv"])
    sock.recv = MagicMock(side_effect=[full[:2], full[2:]])
    assert AsyncConnection._recv_exact_sync(sock, 4) == full

    sock_eof = MagicMock(spec=["recv"])
    sock_eof.recv = MagicMock(side_effect=[b"\x00", b""])
    with pytest.raises(OSError, match="connection closed"):
        AsyncConnection._recv_exact_sync(sock_eof, 4)


@pytest.mark.skipif(
    sys.version_info[:2] == (99, 99),
    reason="probe replay logic is version-agnostic; static-method coverage runs everywhere",
)
def test_probe_sync_replays_handshake_and_wraps_socket(monkeypatch: pytest.MonkeyPatch) -> None:
    """On the no-redirect path the sync probe must (1) replay the
    plaintext ``CUBRS`` handshake, (2) when the broker returns 0 (no
    redirect), proceed straight to :meth:`ssl.SSLContext.wrap_socket`
    on the same socket. Skipping step 1 would have the broker reject
    raw TLS bytes; skipping step 2 would defeat the verification probe
    entirely."""
    raw_sock = MagicMock(name="raw_sock")
    raw_sock.recv = MagicMock(side_effect=[struct.pack(">i", 0)])
    monkeypatch.setattr(
        "pycubrid.aio.connection.socket.create_connection", MagicMock(return_value=raw_sock)
    )

    ssock = MagicMock(name="ssock")
    ctx = MagicMock(spec=ssl_module.SSLContext)
    ctx.wrap_socket = MagicMock(return_value=ssock)

    AsyncConnection._probe_tls_verification_sync(
        "broker.example.com",
        33000,
        ctx,
        True,
        2.0,
        5.0,
    )

    ctx.wrap_socket.assert_called_once_with(raw_sock, server_hostname="broker.example.com")
    ssock.close.assert_called_once_with()
    raw_sock.sendall.assert_called_once()
    sent = raw_sock.sendall.call_args.args[0]
    assert b"CUBRS" in sent, "TLS-requested handshake replay must use CUBRS magic"


def test_probe_sync_no_replay_when_followed_redirect(monkeypatch: pytest.MonkeyPatch) -> None:
    """When the real connection already followed a broker redirect, the
    probe must skip the CUBRS handshake replay and call wrap_socket on
    the already-TLS-expecting worker port directly."""
    raw_sock = MagicMock(name="raw_sock")
    monkeypatch.setattr(
        "pycubrid.aio.connection.socket.create_connection", MagicMock(return_value=raw_sock)
    )

    ssock = MagicMock(name="ssock")
    ctx = MagicMock(spec=ssl_module.SSLContext)
    ctx.wrap_socket = MagicMock(return_value=ssock)

    AsyncConnection._probe_tls_verification_sync(
        "broker.example.com",
        33100,
        ctx,
        False,
        2.0,
        5.0,
    )

    raw_sock.sendall.assert_not_called()
    ctx.wrap_socket.assert_called_once_with(raw_sock, server_hostname="broker.example.com")


def test_probe_sync_follows_broker_redirect_during_replay(monkeypatch: pytest.MonkeyPatch) -> None:
    """If the broker returns ``new_connection_port > 0`` during the
    replay, the probe must close the first socket, reconnect to the
    redirected worker port, and wrap **that** socket. Otherwise it
    would TLS-wrap the broker port that just told it to go elsewhere."""
    first_sock = MagicMock(name="first_sock")
    first_sock.recv = MagicMock(side_effect=[struct.pack(">i", 33101)])
    second_sock = MagicMock(name="second_sock")
    create = MagicMock(side_effect=[first_sock, second_sock])
    monkeypatch.setattr("pycubrid.aio.connection.socket.create_connection", create)

    ssock = MagicMock(name="ssock")
    ctx = MagicMock(spec=ssl_module.SSLContext)
    ctx.wrap_socket = MagicMock(return_value=ssock)

    AsyncConnection._probe_tls_verification_sync(
        "broker.example.com",
        33000,
        ctx,
        True,
        2.0,
        5.0,
    )

    assert create.call_args_list[0].args[0] == ("broker.example.com", 33000)
    assert create.call_args_list[1].args[0] == ("broker.example.com", 33101)
    first_sock.close.assert_called_once_with()
    ctx.wrap_socket.assert_called_once_with(second_sock, server_hostname="broker.example.com")


def test_probe_sync_returns_on_broker_negative_status(monkeypatch: pytest.MonkeyPatch) -> None:
    """If the broker rejects the handshake (``new_connection_port < 0``)
    the probe must return cleanly without attempting wrap_socket — the
    rejection is a non-TLS problem and must be surfaced by the real
    handshake, not by the probe."""
    raw_sock = MagicMock(name="raw_sock")
    raw_sock.recv = MagicMock(side_effect=[struct.pack(">i", -42)])
    monkeypatch.setattr(
        "pycubrid.aio.connection.socket.create_connection", MagicMock(return_value=raw_sock)
    )

    ctx = MagicMock(spec=ssl_module.SSLContext)
    ctx.wrap_socket = MagicMock()

    AsyncConnection._probe_tls_verification_sync(
        "broker.example.com",
        33000,
        ctx,
        True,
        2.0,
        5.0,
    )

    ctx.wrap_socket.assert_not_called()
    raw_sock.close.assert_called_once_with()


def test_probe_sync_propagates_ssl_verification_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    """Cert verification failure inside ``wrap_socket`` must propagate as
    ``ssl.SSLError`` so the caller chain wraps it as
    ``OperationalError`` — the entire point of the workaround."""
    raw_sock = MagicMock(name="raw_sock")
    monkeypatch.setattr(
        "pycubrid.aio.connection.socket.create_connection", MagicMock(return_value=raw_sock)
    )

    boom = ssl_module.SSLError("CERTIFICATE_VERIFY_FAILED")
    ctx = MagicMock(spec=ssl_module.SSLContext)
    ctx.wrap_socket = MagicMock(side_effect=boom)

    with pytest.raises(ssl_module.SSLError) as excinfo:
        AsyncConnection._probe_tls_verification_sync(
            "broker.example.com",
            33100,
            ctx,
            False,
            2.0,
            5.0,
        )

    assert excinfo.value is boom
    raw_sock.close.assert_called_once_with()
