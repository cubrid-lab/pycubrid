"""Offline tests pinning the execute profiler's DML helper call shapes (#888)."""

from __future__ import annotations

import importlib.util
from pathlib import Path
from typing import Any

import pytest

import pycubrid

_SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "profile_execute.py"
_SPEC = importlib.util.spec_from_file_location("_pycubrid_test_profile_execute", _SCRIPT)
assert _SPEC is not None and _SPEC.loader is not None
profile_execute = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(profile_execute)

_SQL = {
    name: template.format(table="_profile_tmp_deadbeef0123")
    for name, template in profile_execute._SQL.items()
}


class _RecordingCursor:
    """Fake cursor that records every statement and fetch without touching a database."""

    def __init__(self) -> None:
        self.executed: list[tuple[str, tuple[object, ...]]] = []
        self.fetchall_calls = 0

    def execute(self, sql: str, params: tuple[object, ...] = ()) -> None:
        self.executed.append((sql, params))

    def fetchall(self) -> list[tuple[object, ...]]:
        self.fetchall_calls += 1
        return []


def test_run_insert_executes_one_parameterized_insert_per_iteration() -> None:
    cursor = _RecordingCursor()

    profile_execute._run_insert(cursor, _SQL, 2)

    assert cursor.executed == [
        (_SQL["insert"], ("row-0",)),
        (_SQL["insert"], ("row-1",)),
    ]
    assert cursor.fetchall_calls == 0


def test_run_select_fetches_all_once_per_iteration() -> None:
    cursor = _RecordingCursor()

    profile_execute._run_select(cursor, _SQL, 2)

    assert cursor.executed == [(_SQL["select"], (0,))] * 2
    assert cursor.fetchall_calls == 2


def test_run_update_binds_a_fresh_value_and_the_key_per_iteration() -> None:
    cursor = _RecordingCursor()

    profile_execute._run_update(cursor, _SQL, 2)

    assert cursor.executed == [
        (_SQL["update"], ("upd-0", 0)),
        (_SQL["update"], ("upd-1", 0)),
    ]
    assert cursor.fetchall_calls == 0


def test_run_delete_keeps_initial_seed_and_per_iteration_reseed() -> None:
    cursor = _RecordingCursor()

    profile_execute._run_delete(cursor, _SQL, 2)

    assert cursor.executed == [
        (_SQL["insert"], ("seed",)),
        (_SQL["delete"], (0,)),
        (_SQL["insert"], ("seed",)),
        (_SQL["delete"], (0,)),
        (_SQL["insert"], ("seed",)),
    ]
    assert cursor.fetchall_calls == 0


def test_every_helper_issues_exactly_one_statement_per_iteration_except_delete() -> None:
    for name in ("insert", "select", "update"):
        cursor = _RecordingCursor()
        profile_execute._RUNNERS[name](cursor, _SQL, 3)
        assert len(cursor.executed) == 3, name

    delete_cursor = _RecordingCursor()
    profile_execute._RUNNERS["delete"](delete_cursor, _SQL, 3)
    # One initial seed plus a delete/re-seed pair per iteration.
    assert len(delete_cursor.executed) == 1 + 2 * 3


class _SeedingCursor:
    """Fake cursor that stores inserted rows and returns them for the profiled SELECT."""

    def __init__(self, db: _SeedingDatabase) -> None:
        self.db = db
        self._result: list[tuple[object, ...]] = []

    def execute(self, sql: str, params: tuple[object, ...] = ()) -> None:
        self.db.log.append(sql.split()[0])
        if sql.startswith("SELECT") and self.db.fail_select:
            raise RuntimeError("injected select failure")
        if sql.startswith("INSERT"):
            if self.db.fail_insert:
                raise RuntimeError("injected seed failure")
            self.db.rows.append((len(self.db.rows) + 1, params[0]))
        elif sql.startswith("SELECT"):
            self._result = list(self.db.rows)
            self.db.select_sizes.append(len(self._result))
        elif sql.startswith("DROP"):
            self.db.dropped = True

    def fetchall(self) -> list[tuple[object, ...]]:
        return self._result

    def close(self) -> None:
        pass


class _SeedingConnection:
    def __init__(self, db: _SeedingDatabase) -> None:
        self.db = db

    def cursor(self) -> _SeedingCursor:
        return _SeedingCursor(self.db)

    def commit(self) -> None:
        self.db.log.append("COMMIT")

    def rollback(self) -> None:
        self.db.log.append("ROLLBACK")

    def close(self) -> None:
        self.db.closed = True


class _SeedingDatabase:
    def __init__(self) -> None:
        self.rows: list[tuple[int, object]] = []
        self.log: list[str] = []
        self.select_sizes: list[int] = []
        self.fail_insert = False
        self.fail_select = False
        self.dropped = False
        self.closed = False


def _run_cycle(
    monkeypatch: pytest.MonkeyPatch, db: _SeedingDatabase, operation: str, iterations: int = 3
) -> None:
    def connect(**_: Any) -> _SeedingConnection:
        return _SeedingConnection(db)

    monkeypatch.setattr(pycubrid, "connect", connect)
    profile_execute.run_execute_cycle("h", 1, "d", "u", "", iterations, operation)


def test_select_only_seeds_before_the_profiled_loop_and_returns_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db = _SeedingDatabase()

    _run_cycle(monkeypatch, db, "select", iterations=3)

    rows = profile_execute.SELECT_SEED_ROWS
    assert rows > 0
    assert db.select_sizes == [rows] * 3
    statements = [entry for entry in db.log if entry != "COMMIT"]
    assert statements == ["CREATE"] + ["INSERT"] * rows + ["SELECT"] * 3 + ["DROP"]
    # The seed is committed before the first SELECT runs.
    assert db.log.index("COMMIT") < db.log.index("SELECT")
    assert db.dropped and db.closed


def test_all_mode_does_not_seed_a_second_time(monkeypatch: pytest.MonkeyPatch) -> None:
    db = _SeedingDatabase()

    _run_cycle(monkeypatch, db, "all", iterations=3)

    # The INSERT stage's 3 rows are what SELECT sees; no extra seed rows are added.
    assert db.select_sizes == [3] * 3
    assert db.log.count("INSERT") == 3 + 1 + 3  # insert stage, delete seed, delete re-seeds


@pytest.mark.parametrize("operation", ["insert", "update", "delete"])
def test_other_single_operations_are_not_seeded(
    monkeypatch: pytest.MonkeyPatch, operation: str
) -> None:
    db = _SeedingDatabase()

    _run_cycle(monkeypatch, db, operation, iterations=2)

    assert db.log.count("INSERT") == {"insert": 2, "update": 0, "delete": 3}[operation]


def test_seed_failure_propagates_and_scratch_table_is_still_dropped(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db = _SeedingDatabase()
    db.fail_insert = True

    with pytest.raises(RuntimeError, match="injected seed failure"):
        _run_cycle(monkeypatch, db, "select")

    assert "SELECT" not in db.log
    assert db.dropped and db.closed


def test_select_failure_propagates_and_scratch_table_is_still_dropped(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db = _SeedingDatabase()
    db.fail_select = True

    with pytest.raises(RuntimeError, match="injected select failure"):
        _run_cycle(monkeypatch, db, "select")

    assert db.log.count("INSERT") == profile_execute.SELECT_SEED_ROWS
    assert db.dropped and db.closed


def test_help_text_states_the_select_seed() -> None:
    help_text = profile_execute._build_parser().format_help()

    assert f"{profile_execute.SELECT_SEED_ROWS} rows" in " ".join(help_text.split())
