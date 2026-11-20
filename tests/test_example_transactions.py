"""Offline tests for transaction example script."""

from __future__ import annotations

import importlib
import re
from collections.abc import Sequence
from typing import Any

import pytest

import pycubrid


class FakeTable:
    """Simulated database table with schema constraints and row storage."""

    def __init__(self, name: str, columns: dict[str, bool]) -> None:
        self.name = name
        # Mapping from column name (lowercase) to is_not_null (bool)
        self.columns = {k.lower(): v for k, v in columns.items()}
        self.committed_rows: list[tuple[Any, ...]] = []
        self.uncommitted_rows: list[tuple[Any, ...]] = []
        self.next_id = 1


class FakeDatabase:
    """In-memory database simulating table schemas, constraints, and transactions."""

    def __init__(self) -> None:
        self.tables: dict[str, FakeTable] = {}
        self.log: list[tuple[Any, ...]] = []

    def commit(self) -> None:
        self.log.append(("commit",))
        for table in self.tables.values():
            table.committed_rows.extend(table.uncommitted_rows)
            table.uncommitted_rows.clear()

    def rollback(self) -> None:
        self.log.append(("rollback",))
        for table in self.tables.values():
            table.uncommitted_rows.clear()


class FakeCursor:
    """Schema-aware simulated cursor for transaction example verification."""

    def __init__(self, connection: FakeConnection) -> None:
        self.connection = connection
        self.closed = False
        self._last_result: list[tuple[Any, ...]] = []

    @property
    def db(self) -> FakeDatabase:
        return self.connection.db

    def execute(
        self,
        operation: str,
        parameters: Sequence[Any] | None = None,
    ) -> FakeCursor:
        if self.closed:
            raise pycubrid.ProgrammingError("Cursor is closed")
        self.db.log.append(("execute", operation.strip(), parameters))

        op = operation.strip()
        upper_op = op.upper()

        if upper_op.startswith("CREATE TABLE"):
            m = re.search(
                r"CREATE\s+TABLE\s+(IF\s+NOT\s+EXISTS\s+)?(\w+)\s*\((.*)\)",
                op,
                re.IGNORECASE | re.DOTALL,
            )
            if not m:
                raise pycubrid.ProgrammingError(f"Malformed CREATE TABLE: {op}")
            if_not_exists = bool(m.group(1))
            table_name = m.group(2)
            cols_def = m.group(3)

            if table_name in self.db.tables:
                if not if_not_exists:
                    raise pycubrid.DatabaseError(f"Table '{table_name}' already exists.")
                # IF NOT EXISTS: preserve existing table schema unchanged
                return self

            columns: dict[str, bool] = {}
            for col_str in cols_def.split(","):
                col_tokens = col_str.strip().split()
                if not col_tokens:
                    continue
                col_name = col_tokens[0].lower()
                is_not_null = "NOT NULL" in col_str.upper() or "PRIMARY KEY" in col_str.upper()
                columns[col_name] = is_not_null

            self.db.tables[table_name] = FakeTable(table_name, columns)
            return self

        if upper_op.startswith("INSERT INTO"):
            m = re.search(r"INSERT\s+INTO\s+(\w+)\s*\((.*?)\)\s*VALUES", op, re.IGNORECASE)
            if not m:
                raise pycubrid.ProgrammingError(f"Malformed INSERT: {op}")
            table_name = m.group(1)
            col_names = [c.strip().lower() for c in m.group(2).split(",")]

            if table_name not in self.db.tables:
                raise pycubrid.ProgrammingError(f"Table '{table_name}' does not exist.")
            tbl = self.db.tables[table_name]

            param_list = list(parameters) if parameters is not None else []
            for col_name, val in zip(col_names, param_list):
                if val is None and tbl.columns.get(col_name, False):
                    raise pycubrid.IntegrityError(
                        f"Attempt to insert null value into NOT NULL column '{col_name}'."
                    )

            val_message = param_list[0] if param_list else None
            tbl.uncommitted_rows.append((tbl.next_id, val_message))
            tbl.next_id += 1
            return self

        if upper_op.startswith("SELECT"):
            m = re.search(r"FROM\s+(\w+)", op, re.IGNORECASE)
            if not m:
                raise pycubrid.ProgrammingError(f"Malformed SELECT: {op}")
            table_name = m.group(1)
            if table_name not in self.db.tables:
                raise pycubrid.ProgrammingError(f"Table '{table_name}' does not exist.")
            self._last_result = list(self.db.tables[table_name].committed_rows)
            return self

        if upper_op.startswith("DROP TABLE"):
            m = re.search(r"DROP\s+TABLE\s+(IF\s+EXISTS\s+)?(\w+)", op, re.IGNORECASE)
            if not m:
                raise pycubrid.ProgrammingError(f"Malformed DROP TABLE: {op}")
            if_exists = bool(m.group(1))
            table_name = m.group(2)
            if table_name in self.db.tables:
                del self.db.tables[table_name]
            elif not if_exists:
                raise pycubrid.DatabaseError(f"Table '{table_name}' does not exist.")
            return self

        raise pycubrid.ProgrammingError(f"Unsupported statement in fake: {op}")

    def fetchall(self) -> list[tuple[Any, ...]]:
        if self.closed:
            raise pycubrid.ProgrammingError("Cursor is closed")
        self.db.log.append(("fetchall",))
        return list(self._last_result)

    def close(self) -> None:
        self.closed = True
        self.db.log.append(("cursor.close",))


class FakeConnection:
    """Simulated connection backed by FakeDatabase."""

    def __init__(self, db: FakeDatabase | None = None) -> None:
        self.db = db if db is not None else FakeDatabase()
        self.closed = False
        self.created_cursors: list[FakeCursor] = []

    def cursor(self) -> FakeCursor:
        if self.closed:
            raise pycubrid.ProgrammingError("Connection is closed")
        self.db.log.append(("cursor",))
        cur = FakeCursor(self)
        self.created_cursors.append(cur)
        return cur

    def commit(self) -> None:
        if self.closed:
            raise pycubrid.ProgrammingError("Connection is closed")
        self.db.commit()

    def rollback(self) -> None:
        if self.closed:
            raise pycubrid.ProgrammingError("Connection is closed")
        self.db.rollback()

    def close(self) -> None:
        self.closed = True
        self.db.log.append(("connection.close",))


def test_transactions_example_deterministic_rollback_and_cleanup(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Verify commit, deterministic rollback, cleanup, and output on a fresh table."""
    fake_conn = FakeConnection()
    monkeypatch.setattr(pycubrid, "connect", lambda **kwargs: fake_conn)

    mod = importlib.import_module("examples.03_transactions")
    mod.main()

    # Connection and cursor cleanup
    assert fake_conn.closed is True
    assert len(fake_conn.created_cursors) == 1
    assert fake_conn.created_cursors[0].closed is True

    # Scratch table is dropped at the end
    assert len(fake_conn.db.tables) == 0

    # Verify lifecycle action sequence
    actions = [entry[0] for entry in fake_conn.db.log]
    assert actions == [
        "cursor",
        "execute",  # CREATE TABLE (plain, no IF NOT EXISTS)
        "commit",
        "execute",  # INSERT 'start transaction'
        "execute",  # INSERT 'commit path'
        "commit",
        "execute",  # INSERT 'rollback path'
        "execute",  # INSERT [None] (fails with IntegrityError)
        "rollback",
        "execute",  # SELECT
        "fetchall",
        "execute",  # DROP TABLE IF EXISTS
        "commit",
        "cursor.close",
        "connection.close",
    ]

    # Verify no statement refers to missing_table or IF NOT EXISTS
    executed_sqls = [entry[1] for entry in fake_conn.db.log if entry[0] == "execute"]
    for sql in executed_sqls:
        assert "missing_table" not in sql
        if "CREATE TABLE" in sql:
            assert "IF NOT EXISTS" not in sql

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


def test_schema_aware_regression_with_preexisting_nullable_table(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Verify that a pre-existing nullable example_ledger table remains completely unchanged.

    The example must create and operate on a unique isolated table, ensuring
    the NOT NULL constraint is strictly enforced and the pre-existing table's
    schema and data are untouched.
    """
    db = FakeDatabase()
    # Pre-populate database with a preexisting nullable example_ledger table
    legacy_table = FakeTable("example_ledger", columns={"id": True, "message": False})
    legacy_table.committed_rows.append((100, "legacy message"))
    db.tables["example_ledger"] = legacy_table

    fake_conn = FakeConnection(db=db)
    monkeypatch.setattr(pycubrid, "connect", lambda **kwargs: fake_conn)

    mod = importlib.import_module("examples.03_transactions")
    mod.main()

    # Pre-existing table must remain intact and unmodified
    assert "example_ledger" in db.tables
    assert db.tables["example_ledger"].columns["message"] is False  # still nullable
    assert db.tables["example_ledger"].committed_rows == [(100, "legacy message")]
    assert db.tables["example_ledger"].uncommitted_rows == []

    # Rollback row 'rollback path' was never committed to example_ledger or any table
    for tbl in db.tables.values():
        for row in tbl.committed_rows:
            assert "rollback path" not in row

    # Scratch table was cleaned up
    assert set(db.tables.keys()) == {"example_ledger"}

    # Output confirms rollback succeeded
    captured = capsys.readouterr()
    assert "Commit path succeeded" in captured.out
    assert "Rollback path executed:" in captured.out
    assert "Ledger rows: [(1, 'start transaction'), (2, 'commit path')]" in captured.out


def test_schema_aware_fake_detects_naive_if_not_exists_bug(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Confirm the fake detects the determinism bug when IF NOT EXISTS reuses a nullable table.

    If an implementation naively uses CREATE TABLE IF NOT EXISTS against an existing
    nullable table, inserting None succeeds instead of failing, and the rollback path
    row erroneously commits.
    """
    db = FakeDatabase()
    legacy_table = FakeTable("example_ledger", columns={"id": True, "message": False})
    legacy_table.committed_rows.append((1, "existing"))
    db.tables["example_ledger"] = legacy_table

    fake_conn = FakeConnection(db=db)

    # Simulate the buggy pattern
    cur = fake_conn.cursor()
    cur.execute(
        "CREATE TABLE IF NOT EXISTS example_ledger "
        "(id INT AUTO_INCREMENT PRIMARY KEY, message VARCHAR(200) NOT NULL)"
    )
    fake_conn.commit()

    # In the buggy pattern, inserting None into the existing nullable table does NOT raise
    cur.execute("INSERT INTO example_ledger (message) VALUES (?)", ["rollback path"])
    cur.execute("INSERT INTO example_ledger (message) VALUES (?)", [None])
    fake_conn.commit()

    # The buggy pattern commits the rollback path row to the pre-existing table!
    committed_messages = [row[1] for row in db.tables["example_ledger"].committed_rows]
    assert "rollback path" in committed_messages


def test_transactions_example_failure_during_first_transaction_cleans_up(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify that an unexpected error during the first transaction cleans up scratch table."""
    db = FakeDatabase()

    class ErrorInjectingCursor(FakeCursor):
        def execute(
            self,
            operation: str,
            parameters: Sequence[Any] | None = None,
        ) -> FakeCursor:
            if parameters is not None and "start transaction" in parameters:
                raise pycubrid.DatabaseError("Simulated database failure")
            return super().execute(operation, parameters)

    class ErrorInjectingConnection(FakeConnection):
        def cursor(self) -> FakeCursor:
            if self.closed:
                raise pycubrid.ProgrammingError("Connection is closed")
            self.db.log.append(("cursor",))
            cur = ErrorInjectingCursor(self)
            self.created_cursors.append(cur)
            return cur

    fake_conn = ErrorInjectingConnection(db=db)
    monkeypatch.setattr(pycubrid, "connect", lambda **kwargs: fake_conn)

    mod = importlib.import_module("examples.03_transactions")
    with pytest.raises(pycubrid.DatabaseError, match="Simulated database failure"):
        mod.main()

    # Scratch table must be dropped even on failure
    assert len(db.tables) == 0

    # Connection and cursor must still be closed by finally block
    assert fake_conn.closed is True
    assert fake_conn.created_cursors[0].closed is True

    actions = [entry[0] for entry in db.log]
    assert "rollback" in actions
