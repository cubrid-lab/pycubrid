"""Offline tests for transaction example script."""

from __future__ import annotations

import importlib
from collections.abc import Sequence
from typing import Any

import pytest

import pycubrid


class FakeCursor:
    """Simulated cursor for offline transaction example verification."""

    def __init__(self, connection: FakeConnection) -> None:
        self.connection = connection
        self.closed = False
        self._last_result: list[tuple[int, str]] = []

    def execute(
        self,
        operation: str,
        parameters: Sequence[Any] | None = None,
    ) -> FakeCursor:
        if self.closed:
            raise pycubrid.ProgrammingError("Cursor is closed")
        self.connection.log.append(("execute", operation.strip(), parameters))

        if "INSERT INTO example_ledger" in operation:
            if parameters is not None and len(parameters) > 0:
                val = parameters[0]
                if val is None:
                    raise pycubrid.IntegrityError(
                        "Attempt to insert null value into NOT NULL column 'message'."
                    )
                self.connection._uncommitted_rows.append((self.connection._next_id, str(val)))
                self.connection._next_id += 1
        elif "SELECT id, message FROM example_ledger" in operation:
            self._last_result = list(self.connection._committed_rows)
        return self

    def fetchall(self) -> list[tuple[int, str]]:
        if self.closed:
            raise pycubrid.ProgrammingError("Cursor is closed")
        self.connection.log.append(("fetchall",))
        return list(self._last_result)

    def close(self) -> None:
        self.closed = True
        self.connection.log.append(("cursor.close",))


class FakeConnection:
    """Simulated connection for offline transaction example verification."""

    def __init__(self) -> None:
        self.closed = False
        self.log: list[tuple[Any, ...]] = []
        self._uncommitted_rows: list[tuple[int, str]] = []
        self._committed_rows: list[tuple[int, str]] = []
        self._next_id = 1
        self.created_cursors: list[FakeCursor] = []

    def cursor(self) -> FakeCursor:
        if self.closed:
            raise pycubrid.ProgrammingError("Connection is closed")
        self.log.append(("cursor",))
        cur = FakeCursor(self)
        self.created_cursors.append(cur)
        return cur

    def commit(self) -> None:
        if self.closed:
            raise pycubrid.ProgrammingError("Connection is closed")
        self.log.append(("commit",))
        self._committed_rows.extend(self._uncommitted_rows)
        self._uncommitted_rows.clear()

    def rollback(self) -> None:
        if self.closed:
            raise pycubrid.ProgrammingError("Connection is closed")
        self.log.append(("rollback",))
        self._uncommitted_rows.clear()

    def close(self) -> None:
        self.closed = True
        self.log.append(("connection.close",))


def test_transactions_example_deterministic_rollback_and_cleanup(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Verify commit, deterministic rollback, cleanup, and output."""
    fake_conn = FakeConnection()
    monkeypatch.setattr(pycubrid, "connect", lambda **kwargs: fake_conn)

    mod = importlib.import_module("examples.03_transactions")
    mod.main()

    # Verify connection and cursor cleanup
    assert fake_conn.closed is True
    assert len(fake_conn.created_cursors) == 1
    assert fake_conn.created_cursors[0].closed is True

    # Verify lifecycle action sequence
    actions = [entry[0] for entry in fake_conn.log]
    assert actions == [
        "cursor",
        "execute",  # CREATE TABLE
        "commit",
        "execute",  # INSERT 'start transaction'
        "execute",  # INSERT 'commit path'
        "commit",
        "execute",  # INSERT 'rollback path'
        "execute",  # INSERT [None] (fails with IntegrityError)
        "rollback",
        "execute",  # SELECT
        "fetchall",
        "cursor.close",
        "connection.close",
    ]

    # Verify no statement refers to missing_table
    executed_sqls = [entry[1] for entry in fake_conn.log if entry[0] == "execute"]
    for sql in executed_sqls:
        assert "missing_table" not in sql

    # Verify standard output
    captured = capsys.readouterr()
    assert "Commit path succeeded" in captured.out
    assert (
        "Rollback path executed: Attempt to insert null value into NOT NULL column 'message'."
        in captured.out
    )
    assert "Ledger rows: [(1, 'start transaction'), (2, 'commit path')]" in captured.out

    # Assert 'rollback path' row is not present in committed ledger rows
    ledger_output = captured.out.split("Ledger rows:")[1]
    assert "rollback path" not in ledger_output


def test_transactions_example_first_transaction_error_triggers_rollback_and_cleanup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify that an unexpected error during first transaction triggers rollback and cleanup."""

    class FailingCursor(FakeCursor):
        def execute(
            self,
            operation: str,
            parameters: Sequence[Any] | None = None,
        ) -> FakeCursor:
            if parameters is not None and "start transaction" in parameters:
                raise pycubrid.DatabaseError("Simulated database failure")
            return super().execute(operation, parameters)

    class FailingConnection(FakeConnection):
        def cursor(self) -> FakeCursor:
            if self.closed:
                raise pycubrid.ProgrammingError("Connection is closed")
            self.log.append(("cursor",))
            cur = FailingCursor(self)
            self.created_cursors.append(cur)
            return cur

    fake_conn = FailingConnection()
    monkeypatch.setattr(pycubrid, "connect", lambda **kwargs: fake_conn)

    mod = importlib.import_module("examples.03_transactions")
    with pytest.raises(pycubrid.DatabaseError, match="Simulated database failure"):
        mod.main()

    # Connection and cursor must still be closed by finally block
    assert fake_conn.closed is True
    assert fake_conn.created_cursors[0].closed is True
    actions = [entry[0] for entry in fake_conn.log]
    assert "rollback" in actions
