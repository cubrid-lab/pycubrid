"""Live construction/authentication without creating database objects."""

from __future__ import annotations


import pytest

from pycubrid.compat import cubriddb, native

from ._cubrid_endpoint import TEST_DB, TEST_HOST, TEST_PORT

pytestmark = pytest.mark.integration


@pytest.mark.parametrize(
    ("mode", "expected_user"),
    [
        ("native_public", "public"),
        ("native_embedded_credentials", "public"),
        ("native_dba", "dba"),
        ("wrapper_dba", "dba"),
    ],
)
def test_compat_factory_owns_a_live_autocommitting_session(mode: str, expected_user: str) -> None:
    dsn = f"CUBRID:{TEST_HOST}:{TEST_PORT}:{TEST_DB}:::"
    if mode == "native_embedded_credentials":
        dsn = f"CUBRID:{TEST_HOST}:{TEST_PORT}:{TEST_DB}:dba:ignored:"
    if mode == "native_dba":
        conn = native.connect(dsn, "dba", "")
    elif mode == "wrapper_dba":
        conn = cubriddb.Connect(dsn, "dba", "")
    else:
        conn = native.connect(dsn)
    try:
        owned = conn.connection if isinstance(conn, cubriddb.Connection) else conn
        cursor = owned._driver.cursor()
        try:
            cursor.execute("SELECT CURRENT_USER")
            row = cursor.fetchone()
            assert row is not None
            assert row[0].casefold() == expected_user
            assert owned._driver.autocommit is True
        finally:
            cursor.close()
    finally:
        conn.close()
