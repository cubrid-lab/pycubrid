"""Offline tests pinning the fetch profiler's loop termination (#889)."""

from __future__ import annotations

import importlib.util
from pathlib import Path

_SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "profile_fetch.py"
_SPEC = importlib.util.spec_from_file_location("_pycubrid_test_profile_fetch", _SCRIPT)
assert _SPEC is not None and _SPEC.loader is not None
profile_fetch = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(profile_fetch)

_SELECT = "SELECT id, val FROM _profile_tmp_deadbeef0123 WHERE id > ?"


class _FakeCursor:
    """Deterministic cursor: every ``execute()`` rewinds the same bounded rows."""

    def __init__(self, rows: list[tuple[str, ...]]) -> None:
        self.rows = rows
        self.executed: list[tuple[str, tuple[int, ...]]] = []
        self.calls: list[str] = []
        self.fetchmany_sizes: list[int] = []
        self._pending: list[tuple[str, ...]] = []

    def execute(self, sql: str, params: tuple[int, ...] = ()) -> None:
        self.calls.append("execute")
        self.executed.append((sql, params))
        self._pending = list(self.rows)

    def fetchone(self) -> tuple[str, ...] | None:
        self.calls.append("fetchone")
        if not self._pending:
            return None
        return self._pending.pop(0)

    def fetchall(self) -> list[tuple[str, ...]]:
        self.calls.append("fetchall")
        rows, self._pending = self._pending, []
        return rows

    def fetchmany(self, size: int) -> list[tuple[str, ...]]:
        self.calls.append("fetchmany")
        self.fetchmany_sizes.append(size)
        batch, self._pending = self._pending[:size], self._pending[size:]
        return batch


def test_run_fetchone_issues_one_select_per_iteration_and_stops_at_none() -> None:
    cursor = _FakeCursor([("a-1",), ("a-2",)])

    profile_fetch.run_fetchone(cursor, 2, sql=_SELECT)

    assert cursor.executed == [(_SELECT, (0,)), (_SELECT, (0,))]
    # Two rows plus the terminating None, once per iteration.
    assert cursor.calls.count("fetchone") == 6
    assert cursor.fetchmany_sizes == []
    assert "fetchall" not in cursor.calls


def test_run_fetchone_on_empty_result_stops_after_the_first_none() -> None:
    cursor = _FakeCursor([])

    profile_fetch.run_fetchone(cursor, 1, sql=_SELECT)

    assert cursor.calls.count("fetchone") == 1


def test_run_fetchall_calls_fetchall_once_per_iteration() -> None:
    cursor = _FakeCursor([("a-1",), ("a-2",)])

    profile_fetch.run_fetchall(cursor, 3, sql=_SELECT)

    assert cursor.executed == [(_SELECT, (0,))] * 3
    assert cursor.calls.count("fetchall") == 3
    assert "fetchone" not in cursor.calls
    assert cursor.fetchmany_sizes == []


def test_run_fetchmany_forwards_batch_size_and_stops_at_empty_batch() -> None:
    cursor = _FakeCursor([("a-1",), ("a-2",), ("a-3",)])

    profile_fetch.run_fetchmany(cursor, 1, 2, sql=_SELECT)

    assert cursor.executed == [(_SELECT, (0,))]
    # Two rows, then the remaining row, then the empty batch that ends the loop.
    assert cursor.fetchmany_sizes == [2, 2, 2]
    assert "fetchone" not in cursor.calls
    assert "fetchall" not in cursor.calls


def test_run_fetchmany_with_exact_multiple_ends_on_an_empty_batch() -> None:
    cursor = _FakeCursor([("a-1",), ("a-2",)])

    profile_fetch.run_fetchmany(cursor, 2, 2, sql=_SELECT)

    assert cursor.executed == [(_SELECT, (0,))] * 2
    # A full batch never terminates the loop, so each iteration needs one more call.
    assert cursor.fetchmany_sizes == [2, 2, 2, 2]
