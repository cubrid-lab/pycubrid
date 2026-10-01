from __future__ import annotations

import asyncio
import os
import ssl as ssl_module

import pytest

import pycubrid
import pycubrid.aio
from pycubrid.aio.connection import AsyncConnection
from pycubrid.exceptions import OperationalError

from ._cubrid_endpoint import TEST_DB, TEST_HOST, TEST_PASSWORD, TEST_PORT, TEST_USER

TLS_HOST = os.environ.get("CUBRID_TLS_TEST_HOST", TEST_HOST)
TLS_PORT = int(os.environ.get("CUBRID_TLS_TEST_PORT", str(TEST_PORT)))
TLS_DB = os.environ.get("CUBRID_TLS_TEST_DB", TEST_DB)
TLS_USER = os.environ.get("CUBRID_TLS_TEST_USER", TEST_USER)
TLS_PASSWORD = os.environ.get("CUBRID_TLS_TEST_PASSWORD", TEST_PASSWORD)
TLS_CA_FILE = os.environ.get("CUBRID_TLS_TEST_CA_FILE")
TLS_MISMATCH_HOST = os.environ.get("CUBRID_TLS_TEST_MISMATCH_HOST")

DEFAULT_TRUST_FILE = os.environ.get("SSL_CERT_FILE")

# Configuration-only gates (#522): an unconfigured TLS broker skips, but a
# configured one that is unreachable or not serving TLS fails the test's own
# connection attempt instead of being probed away at import time. Mirrors
# ``tls_broker`` in ``tests/test_tls_matrix_integration.py``.
requires_tls_broker = pytest.mark.skipif(
    TLS_CA_FILE is None,
    reason=(
        "TLS-enabled CUBRID broker not configured; set CUBRID_TLS_TEST_* "
        "(including CUBRID_TLS_TEST_CA_FILE)"
    ),
)
requires_default_trust = pytest.mark.skipif(
    DEFAULT_TRUST_FILE is None,
    reason="Set SSL_CERT_FILE to the broker CA so ssl=True can verify the TLS broker",
)
TLS_MISMATCH_REASON = (
    "Set CUBRID_TLS_TEST_MISMATCH_HOST to a reachable alternate host/IP that is not covered "
    "by the broker certificate"
)

pytestmark = [pytest.mark.integration, pytest.mark.tls, pytest.mark.no_escape_pin]


def _custom_ssl_context() -> ssl_module.SSLContext:
    context = ssl_module.create_default_context()
    context.minimum_version = ssl_module.TLSVersion.TLSv1_2
    if TLS_CA_FILE:
        context.load_verify_locations(cafile=TLS_CA_FILE)
    return context


async def _connect_async(
    ssl_value: bool | ssl_module.SSLContext,
    *,
    host: str = TLS_HOST,
) -> AsyncConnection:
    return await pycubrid.aio.connect(
        host=host,
        port=TLS_PORT,
        database=TLS_DB,
        user=TLS_USER,
        password=TLS_PASSWORD,
        connect_timeout=5,
        read_timeout=5,
        ssl=ssl_value,
    )


async def _assert_select_one(conn: AsyncConnection) -> None:
    cur = conn.cursor()
    try:
        await cur.execute("SELECT 1")
        assert await cur.fetchone() == (1,)
    finally:
        await cur.close()


def _assert_tls_transport(conn: AsyncConnection) -> None:
    assert conn._writer is not None
    assert conn._writer.get_extra_info("ssl_object") is not None


def _require_mismatch_host() -> str:
    assert TLS_MISMATCH_HOST is not None
    return TLS_MISMATCH_HOST


@pytest.mark.asyncio
@requires_tls_broker
@requires_default_trust
async def test_aio_ssl_connect_default_context() -> None:
    conn = await _connect_async(True)
    try:
        _assert_tls_transport(conn)
        await _assert_select_one(conn)
    finally:
        await conn.close()


@pytest.mark.asyncio
@requires_tls_broker
async def test_aio_ssl_connect_custom_context() -> None:
    conn = await _connect_async(_custom_ssl_context())
    try:
        _assert_tls_transport(conn)
        await _assert_select_one(conn)
    finally:
        await conn.close()


@pytest.mark.asyncio
@requires_tls_broker
@pytest.mark.skipif(TLS_MISMATCH_HOST is None, reason=TLS_MISMATCH_REASON)
async def test_aio_ssl_handshake_failure() -> None:
    with pytest.raises(OperationalError) as excinfo:
        await _connect_async(_custom_ssl_context(), host=_require_mismatch_host())

    assert isinstance(excinfo.value.__cause__, ssl_module.SSLError)


@pytest.mark.asyncio
@requires_tls_broker
async def test_aio_ssl_out_tran_keeps_tls_session() -> None:
    conn = await _connect_async(_custom_ssl_context())
    try:
        original_writer = conn._writer
        _assert_tls_transport(conn)

        conn._cas_info = bytes([conn._CAS_INFO_STATUS_INACTIVE, *conn._cas_info[1:]])

        assert await conn.ping(reconnect=True) is True
        assert conn._writer is not None
        assert conn._writer is original_writer
        _assert_tls_transport(conn)
        await _assert_select_one(conn)
    finally:
        await conn.close()


@pytest.mark.asyncio
@requires_tls_broker
async def test_aio_ssl_clean_shutdown() -> None:
    conn = await _connect_async(_custom_ssl_context())

    await asyncio.wait_for(conn.close(), timeout=5)

    assert conn._reader is None
    assert conn._writer is None
