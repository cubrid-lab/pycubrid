"""Live #439 gates for the explicitly selected sync prepared compatibility subset."""

from __future__ import annotations

import os

import pytest

from pycubrid.compat import native
from pycubrid.exceptions import InterfaceError

from ._parity_helpers import table_name

pytestmark = pytest.mark.integration


def _connect() -> native.connection:
    host = os.environ.get("CUBRID_TEST_HOST", "127.0.0.1")
    port = int(os.environ.get("CUBRID_TEST_PORT", "33000"))
    database = os.environ.get("CUBRID_TEST_DB", "testdb")
    user = os.environ.get("CUBRID_TEST_USER", "dba")
    password = os.environ.get("CUBRID_TEST_PASSWORD", "")
    return native.connect(f"CUBRID:{host}:{port}:{database}:::", user, password)


@pytest.mark.parametrize(
    ("sql", "values"),
    [
        ("SELECT CAST(? AS INTEGER)", ((-(2**31),), (0,), (2**31 - 1,), (None,))),
        ("SELECT CAST(? AS VARCHAR(40))", (("a'\\한",), ("",), (None,))),
    ],
    ids=["int-and-null", "char-and-null"],
)
def test_public_scalar_prepare_reexecutes_one_handle(
    sql: str, values: tuple[tuple[object], ...]
) -> None:
    conn = _connect()
    try:
        assert conn._driver._statement_pooling == 1
        generation = conn._driver._physical_generation
        cur = conn.cursor()
        try:
            cur.prepare(sql)
            handle = cur._handle
            for (value,) in values:
                cur.bind_param(1, value)
                assert cur.execute() == 1
                assert cur.fetch_row() == (value,)
                assert cur.fetch_row() is None
                assert cur._handle == handle
                assert conn._driver._physical_generation == generation
        finally:
            cur.close()
    finally:
        conn.close()


def test_manual_commit_keeps_multi_page_result_and_rollback_invalidates() -> None:
    conn = _connect()
    table = table_name("p439_fetch")
    payload = "x" * 8000  # Exceeds the broker's inline response buffer after a few rows.
    created = False
    try:
        assert conn._driver._statement_pooling == 1
        conn._driver.autocommit = False  # Private fixture until #467 exposes effective setter.
        ordinary = conn._driver.cursor()
        try:
            ordinary.execute(f"CREATE TABLE {table} (n INTEGER, payload VARCHAR(8192))")
            created = True
            conn.commit()
            for value in range(130):
                ordinary.execute(f"INSERT INTO {table} VALUES (?, ?)", (value, payload))
            conn.commit()
        finally:
            ordinary.close()

        cur = conn.cursor()
        try:
            cur.prepare(f"SELECT n, payload FROM {table} ORDER BY n")
            handle = cur._handle
            assert cur.execute() == 130
            assert cur.fetch_row() == (0, payload)
            assert 0 < cur._fetched_count < 130  # Continuation needs post-commit FC8.
            conn.commit()
            assert [cur.fetch_row() for _ in range(129)] == [(n, payload) for n in range(1, 130)]
            assert cur._fetched_count == 130
            assert cur.fetch_row() is None
            assert cur._handle == handle

            assert cur.execute() == 130
            assert cur.fetch_row() == (0, payload)
            conn.rollback()
            with pytest.raises(InterfaceError):
                cur.fetch_row()
            assert cur._handle == handle
            assert cur.execute() == 130
            assert cur.fetch_row() == (0, payload)
        finally:
            cur.close()
    finally:
        if created:
            conn.rollback()
            ordinary = conn._driver.cursor()
            try:
                ordinary.execute(f"DROP TABLE {table}")
                conn.commit()
            finally:
                ordinary.close()
        conn.close()


@pytest.mark.parametrize("autocommit", [False, True], ids=["manual", "auto"])
def test_repeated_prepared_dml_visibility_and_close_does_not_commit(autocommit: bool) -> None:
    conn = _connect()
    observer = _connect()
    table = table_name("p439_dml")
    created = False
    try:
        assert conn._driver._statement_pooling == 1
        conn._driver.autocommit = autocommit
        setup = conn._driver.cursor()
        try:
            setup.execute(f"CREATE TABLE {table} (n INTEGER)")
            created = True
            conn.commit()
        finally:
            setup.close()

        def visible_rows() -> int:
            cur = observer._driver.cursor()
            try:
                cur.execute(f"SELECT COUNT(*) FROM {table}")
                row = cur.fetchone()
                assert row is not None
                return int(row[0])
            finally:
                cur.close()

        cur = conn.cursor()
        cur.prepare(f"INSERT INTO {table} VALUES (?)")
        handle = cur._handle
        for value in (11, 12):
            cur.bind_param(1, value)
            assert cur.execute() == 1
            assert cur._handle == handle
            assert visible_rows() == (2 if autocommit and value == 12 else 1 if autocommit else 0)
        cur.close()
        assert visible_rows() == (2 if autocommit else 0)
        conn.commit()
        assert visible_rows() == 2
    finally:
        observer.close()
        if created:
            conn.rollback()
            cleanup = conn._driver.cursor()
            try:
                cleanup.execute(f"DROP TABLE {table}")
                conn.commit()
            finally:
                cleanup.close()
        conn.close()
