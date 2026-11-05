"""Offline tests pinning the execute profiler's DML helper call shapes (#888)."""

from __future__ import annotations

import importlib.util
from pathlib import Path

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
