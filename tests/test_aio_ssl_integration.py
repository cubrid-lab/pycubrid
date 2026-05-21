from __future__ import annotations

import asyncio
import os
import ssl as ssl_module
import sys
from functools import lru_cache

import pytest

import pycubrid
import pycubrid.aio
from pycubrid.aio.connection import AsyncConnection
from pycubrid.exceptions import OperationalError

TEST_HOST = os.environ.get("CUBRID_TEST_HOST", "localhost")
TEST_PORT = int(os.environ.get("CUBRID_TEST_PORT", "33000"))
TEST_DB = os.environ.get("CUBRID_TEST_DB", "testdb")
TEST_USER = os.environ.get("CUBRID_TEST_USER", "dba")
TEST_PASSWORD = os.environ.get("CUBRID_TEST_PASSWORD", "")

TLS_HOST = os.environ.get("CUBRID_TLS_TEST_HOST", TEST_HOST)
TLS_PORT = int(os.environ.get("CUBRID_TLS_TEST_PORT", str(TEST_PORT)))
TLS_DB = os.environ.get("CUBRID_TLS_TEST_DB", TEST_DB)
TLS_USER = os.environ.get("CUBRID_TLS_TEST_USER", TEST_USER)
TLS_PASSWORD = os.environ.get("CUBRID_TLS_TEST_PASSWORD", TEST_PASSWORD)
TLS_CA_FILE = os.environ.get("CUBRID_TLS_TEST_CA_FILE")
TLS_MISMATCH_HOST = os.environ.get("CUBRID_TLS_TEST_MISMATCH_HOST")

TLS_DEFAULT_REASON = (
    "TLS-enabled CUBRID broker with default trust is not available; configure a trusted broker "
    "via CUBRID_TLS_TEST_* or SSL_CERT_FILE"
)
TLS_CUSTOM_REASON = (
    "TLS-enabled CUBRID broker is not available for custom SSLContext testing; configure "
    "CUBRID_TLS_TEST_*"
)
TLS_MISMATCH_REASON = (
    "Set CUBRID_TLS_TEST_MISMATCH_HOST to a reachable alternate host/IP that is not covered "
    "by the broker certificate"
)

pytestmark = pytest.mark.integration


def _custom_ssl_context() -> ssl_module.SSLContext:
    context = ssl_module.create_default_context()
    if TLS_CA_FILE:
        context.load_verify_locations(cafile=TLS_CA_FILE)
    return context


def _can_connect_with_ssl(ssl_value: bool | ssl_module.SSLContext, *, host: str) -> bool:
    try:
        conn = pycubrid.connect(
            host=host,
            port=TLS_PORT,
            database=TLS_DB,
            user=TLS_USER,
            password=TLS_PASSWORD,
            connect_timeout=5,
            read_timeout=5,
            ssl=ssl_value,
        )
        cur = conn.cursor()
        cur.execute("SELECT 1")
        assert cur.fetchone() == (1,)
        cur.close()
        conn.close()
        return True
    except Exception:
        return False


@lru_cache(maxsize=None)
def _can_connect_tls_default() -> bool:
    return _can_connect_with_ssl(True, host=TLS_HOST)


@lru_cache(maxsize=None)
def _can_connect_tls_custom() -> bool:
    return _can_connect_with_ssl(_custom_ssl_context(), host=TLS_HOST)


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
@pytest.mark.skipif(not _can_connect_tls_default(), reason=TLS_DEFAULT_REASON)
async def test_aio_ssl_connect_default_context() -> None:
    conn = await _connect_async(True)
    try:
        _assert_tls_transport(conn)
        await _assert_select_one(conn)
    finally:
        await conn.close()


@pytest.mark.asyncio
@pytest.mark.skipif(not _can_connect_tls_custom(), reason=TLS_CUSTOM_REASON)
async def test_aio_ssl_connect_custom_context() -> None:
    conn = await _connect_async(_custom_ssl_context())
    try:
        _assert_tls_transport(conn)
        await _assert_select_one(conn)
    finally:
        await conn.close()


@pytest.mark.asyncio
@pytest.mark.skipif(not _can_connect_tls_custom(), reason=TLS_CUSTOM_REASON)
@pytest.mark.skipif(TLS_MISMATCH_HOST is None, reason=TLS_MISMATCH_REASON)
@pytest.mark.skipif(
    sys.version_info < (3, 11),
    reason=(
        "Python 3.10 loop.start_tls() can hang on TLS cert verification failure "
        "(CPython gh-142352 family, fixed in 3.13/3.14 only). Tracked separately; "
        "the upgrade path itself is exercised by the other tests in this module."
    ),
)
async def test_aio_ssl_handshake_failure() -> None:
    with pytest.raises(OperationalError) as excinfo:
        await _connect_async(_custom_ssl_context(), host=_require_mismatch_host())

    assert isinstance(excinfo.value.__cause__, ssl_module.SSLError)


@pytest.mark.asyncio
@pytest.mark.skipif(not _can_connect_tls_custom(), reason=TLS_CUSTOM_REASON)
async def test_aio_ssl_reconnect_after_inactive() -> None:
    conn = await _connect_async(_custom_ssl_context())
    try:
        original_writer = conn._writer
        _assert_tls_transport(conn)

        conn._cas_info = bytes([conn._CAS_INFO_STATUS_INACTIVE, *conn._cas_info[1:]])

        assert await conn.ping(reconnect=True) is True
        assert conn._writer is not None
        assert conn._writer is not original_writer
        _assert_tls_transport(conn)
        await _assert_select_one(conn)
    finally:
        await conn.close()


@pytest.mark.asyncio
@pytest.mark.skipif(not _can_connect_tls_custom(), reason=TLS_CUSTOM_REASON)
async def test_aio_ssl_clean_shutdown() -> None:
    conn = await _connect_async(_custom_ssl_context())

    await asyncio.wait_for(conn.close(), timeout=5)

    assert conn._reader is None
    assert conn._writer is None
