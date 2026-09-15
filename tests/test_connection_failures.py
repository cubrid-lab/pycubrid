"""Connection failure-mode tests (#328).

Covers the three production-critical connection error paths:

1. Unreachable host  — verified without a live CUBRID; must raise
   ``OperationalError`` within the ``connect_timeout`` window.
2. Closed/wrong port — verified without a live CUBRID; connection is refused
   immediately with ``OperationalError``.
3. Wrong credentials — requires a live CUBRID; the broker rejects the login
   with a ``DatabaseError`` (``User "..." is invalid``).

All cases use a short ``connect_timeout`` so the suite stays well under the
30-second budget. Only the credential case is marked ``integration``; the
shared conftest guard skips it when no CUBRID is configured, so the two
network-failure tests still run in the offline suite.
"""

from __future__ import annotations

import os
import time

import pytest

import pycubrid
from pycubrid.exceptions import DatabaseError, OperationalError

TEST_HOST = os.environ.get("CUBRID_TEST_HOST", "localhost")
TEST_PORT = int(os.environ.get("CUBRID_TEST_PORT", "33000"))
TEST_DB = os.environ.get("CUBRID_TEST_DB", "testdb")
TEST_USER = os.environ.get("CUBRID_TEST_USER", "dba")
TEST_PASSWORD = os.environ.get("CUBRID_TEST_PASSWORD", "")

# RFC 5737 TEST-NET-1: guaranteed non-routable, so the connect attempt hangs
# until connect_timeout rather than being refused immediately.
UNREACHABLE_HOST = "192.0.2.1"

# A port in the IANA dynamic range with nothing listening on localhost, so the
# OS refuses the connection immediately.
CLOSED_PORT = 59999


def test_unreachable_host_times_out() -> None:
    start = time.monotonic()
    with pytest.raises(OperationalError):
        pycubrid.connect(
            host=UNREACHABLE_HOST,
            port=TEST_PORT,
            database=TEST_DB,
            user=TEST_USER,
            password=TEST_PASSWORD,
            connect_timeout=3,
        )
    elapsed = time.monotonic() - start
    assert elapsed < 15, f"connect took {elapsed:.1f}s — connect_timeout not honoured"


def test_closed_port_is_refused() -> None:
    start = time.monotonic()
    with pytest.raises(OperationalError):
        pycubrid.connect(
            host="127.0.0.1",
            port=CLOSED_PORT,
            database=TEST_DB,
            user=TEST_USER,
            password=TEST_PASSWORD,
            connect_timeout=3,
        )
    elapsed = time.monotonic() - start
    assert elapsed < 15, f"refused connect took {elapsed:.1f}s"


@pytest.mark.integration
def test_wrong_credentials_rejected() -> None:
    with pytest.raises(DatabaseError):
        pycubrid.connect(
            host=TEST_HOST,
            port=TEST_PORT,
            database=TEST_DB,
            user="pycubrid_no_such_user",
            password="wrong-password",
            connect_timeout=5,
        )
