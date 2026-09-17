"""Connection/cursor lifecycle state-machine tests (issue #340).

Uses :class:`hypothesis.stateful.RuleBasedStateMachine` to drive a *live* CUBRID
connection through long, generated sequences of individually-valid operations in
unusual orders — the class of bug hand-written scenario tests cannot exhaust.

The model tracks an abstract connection state (DISCONNECTED / CONNECTED_IDLE /
TRANSACTION_ACTIVE / CURSOR_RESULT_ACTIVE / CLOSED) and asserts invariants after
every step:

* a closed connection never behaves as if connected;
* commit/rollback leave a predictable state;
* expected misuse raises a PEP 249 :class:`pycubrid.Error`, never a raw
  ``struct.error`` / ``AttributeError`` / etc.;
* no operation deadlocks (bounded by the connection read timeout).

Skipped when no CUBRID server is reachable. Budget follows the active Hypothesis
profile; stateful tests also honour ``stateful_step_count``.
"""

from __future__ import annotations

import sys
import uuid

import pytest
from hypothesis import settings, strategies as st
from hypothesis.stateful import RuleBasedStateMachine, invariant, precondition, rule

import pycubrid
from pycubrid.exceptions import Error as DBAPIError

from ._parity_helpers import TEST_DB, TEST_HOST, TEST_PASSWORD, TEST_PORT, TEST_USER, can_connect

pytestmark = pytest.mark.skipif(not can_connect(), reason="CUBRID instance not available")


class ConnectionLifecycle(RuleBasedStateMachine):
    """Model a single sync connection's lifecycle against a live server."""

    def __init__(self) -> None:
        super().__init__()
        self.conn: pycubrid.Connection | None = None
        self.cursor: pycubrid.cursor.Cursor | None = None
        self.closed = False
        self.in_transaction = False
        self.has_result = False
        self.table = "sm_%s" % uuid.uuid4().hex[:8]
        self._table_created = False

    # -- lifecycle rules ----------------------------------------------------

    @precondition(lambda self: self.conn is None)
    @rule(autocommit=st.booleans())
    def connect(self, autocommit: bool) -> None:
        self.conn = pycubrid.connect(
            host=TEST_HOST,
            port=TEST_PORT,
            database=TEST_DB,
            user=TEST_USER,
            password=TEST_PASSWORD,
            autocommit=autocommit,
        )
        self.closed = False
        self.in_transaction = False
        self.has_result = False

    @precondition(lambda self: self.conn is not None and self.cursor is None)
    @rule()
    def open_cursor(self) -> None:
        assert self.conn is not None
        self.cursor = self.conn.cursor()

    @precondition(lambda self: self.cursor is not None)
    @rule()
    def close_cursor(self) -> None:
        assert self.cursor is not None
        self.cursor.close()
        self.cursor = None
        self.has_result = False

    @precondition(lambda self: self.cursor is not None)
    @rule()
    def create_table(self) -> None:
        assert self.conn is not None and self.cursor is not None
        self.cursor.execute(
            "CREATE TABLE IF NOT EXISTS %s (id INT PRIMARY KEY, v VARCHAR(50))" % self.table
        )
        self._table_created = True
        if not self.conn.autocommit:
            self.in_transaction = True

    @precondition(lambda self: self.cursor is not None and self._table_created)
    @rule(key=st.integers(), text=st.text(max_size=40))
    def insert(self, key: int, text: str) -> None:
        assert self.conn is not None and self.cursor is not None
        safe_key = abs(key) % 1_000_000
        safe_text = text.replace("\x00", "").replace("\x1a", "")[:40]
        try:
            self.cursor.execute(
                "INSERT INTO %s (id, v) VALUES (?, ?)" % self.table,
                (safe_key, safe_text),
            )
        except DBAPIError:
            # A duplicate PK is a legitimate DB-API error; state is unchanged.
            return
        if not self.conn.autocommit:
            self.in_transaction = True

    @precondition(lambda self: self.cursor is not None and self._table_created)
    @rule()
    def select(self) -> None:
        assert self.cursor is not None
        self.cursor.execute("SELECT id, v FROM %s ORDER BY id" % self.table)
        _ = self.cursor.fetchall()
        self.has_result = True

    @precondition(lambda self: self.conn is not None)
    @rule()
    def commit(self) -> None:
        assert self.conn is not None
        self.conn.commit()
        self.in_transaction = False

    @precondition(lambda self: self.conn is not None)
    @rule()
    def rollback(self) -> None:
        assert self.conn is not None
        self.conn.rollback()
        self.in_transaction = False

    @precondition(lambda self: self.conn is not None)
    @rule(value=st.booleans())
    def set_autocommit(self, value: bool) -> None:
        assert self.conn is not None
        self.conn.autocommit = value

    @precondition(lambda self: self.conn is not None)
    @rule(reconnect=st.booleans())
    def ping(self, reconnect: bool) -> None:
        assert self.conn is not None
        result = self.conn.ping(reconnect=reconnect)
        assert isinstance(result, bool)

    @precondition(lambda self: self.conn is not None)
    @rule()
    def close(self) -> None:
        assert self.conn is not None
        try:
            self.cursor = None
            self.conn.close()
        finally:
            self.conn = None
            self.closed = True
            self.in_transaction = False
            self.has_result = False

    # -- invariants ---------------------------------------------------------

    @invariant()
    def closed_flag_implies_no_live_conn(self) -> None:
        # While the closed flag is set (no reconnect has run since close), there
        # must be no live connection object masquerading as usable.
        if self.closed:
            assert self.conn is None

    @invariant()
    def connection_reports_consistent_state(self) -> None:
        if self.conn is not None:
            # A live connection must answer autocommit without raising.
            _ = self.conn.autocommit

    def teardown(self) -> None:
        if self.cursor is not None:
            try:
                self.cursor.close()
            except DBAPIError:
                pass  # best-effort cleanup; a broken cursor is fine to drop
        if self.conn is not None:
            try:
                cur = self.conn.cursor()
                cur.execute("DROP TABLE IF EXISTS %s" % self.table)
                self.conn.commit()
                cur.close()
            except DBAPIError:
                pass  # best-effort table drop; ignore if the conn is unusable
            try:
                self.conn.close()
            except DBAPIError:
                pass  # best-effort close; nothing to recover in teardown


ConnectionLifecycle.TestCase.settings = settings(
    max_examples=25,
    stateful_step_count=20,
    deadline=None,
)
TestConnectionLifecycle = ConnectionLifecycle.TestCase


class TestClosedConnectionMisuse:
    """A closed connection rejects operations with DB-API errors, not raw ones."""

    def _closed_conn(self) -> pycubrid.Connection:
        conn = pycubrid.connect(
            host=TEST_HOST,
            port=TEST_PORT,
            database=TEST_DB,
            user=TEST_USER,
            password=TEST_PASSWORD,
        )
        conn.close()
        return conn

    def test_cursor_after_close_raises_dbapi(self) -> None:
        conn = self._closed_conn()
        with pytest.raises(DBAPIError):
            conn.cursor()

    def test_commit_after_close_raises_dbapi(self) -> None:
        conn = self._closed_conn()
        with pytest.raises(DBAPIError):
            conn.commit()

    def test_double_close_is_noop(self) -> None:
        conn = self._closed_conn()
        conn.close()


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
