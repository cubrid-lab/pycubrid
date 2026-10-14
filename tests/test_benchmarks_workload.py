"""Offline checks of the live microbenchmark workloads in ``test_benchmarks.py`` (#922).

The workload functions run here against an in-memory SQLite database, which
accepts the same ``?`` SQL, through the real pytest-benchmark ``pedantic`` loop
and through the disabled-benchmark path. They prove that every round, warm-up
rounds included, starts from the same seeded state and is verified, and that a
missing or wrong row fails the benchmark instead of producing a time.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from types import SimpleNamespace
from typing import Any

import pytest

import pycubrid

from . import test_benchmarks as bench

# Rounds + warm-up rounds of each workload; a change must be deliberate, because it
# changes the reported series.
ROUNDS = {
    "insert_one_row": 105,
    "select_one_row_by_pk": 105,
    "update_one_row_by_pk": 105,
    "delete_one_row_by_pk": 105,
    "insert_100_rows": 11,
    "select_100_rows": 55,
    "execute_parameterized_loop_100": 11,
}


@pytest.fixture
def db() -> Iterator[sqlite3.Connection]:
    conn = sqlite3.connect(":memory:")
    conn.execute(bench.CREATE_SQL)
    try:
        yield conn
    finally:
        conn.close()


def _disabled() -> SimpleNamespace:
    return SimpleNamespace(disabled=True, extra_info={})


def _table(db: sqlite3.Connection) -> list[tuple[Any, ...]]:
    return db.execute(bench.SELECT_ALL_SQL + " ORDER BY id").fetchall()


def test_auto_increment_keys_do_not_restart_after_delete() -> None:
    """The pre-#922 hazard: DELETE keeps the counter, so ``WHERE id = 1`` misses.

    The old ``test_bench_single_select`` re-inserted one row after an earlier
    benchmark had inserted 100 and then selected the hardcoded ``id = 1``
    without checking ``fetchone()``. CUBRID 10.2 and 11.4 behave like SQLite's
    ``AUTOINCREMENT`` here (reproduced live in the #922 pull request).
    """
    conn = sqlite3.connect(":memory:")
    conn.execute(
        f"CREATE TABLE {bench.TABLE} "
        "(id INTEGER PRIMARY KEY AUTOINCREMENT, name VARCHAR(100), amount INT)"
    )
    insert = f"INSERT INTO {bench.TABLE} (name, amount) VALUES (?, ?)"
    for _ in range(100):
        conn.execute(insert, ("bench", 42))
    conn.execute(f"DELETE FROM {bench.TABLE}")
    conn.execute(insert, ("select_test", 1))
    assert conn.execute(f"SELECT id FROM {bench.TABLE}").fetchone() == (101,)
    assert conn.execute(bench.SELECT_BY_ID_SQL, (1,)).fetchone() is None
    conn.close()


def test_benchmark_table_has_explicit_keys() -> None:
    assert "AUTO_INCREMENT" not in bench.CREATE_SQL.upper()
    assert "id INT PRIMARY KEY" in bench.CREATE_SQL


@pytest.mark.parametrize("name", sorted(bench.WORKLOADS))
def test_workload_verifies_every_pedantic_round(name: str, benchmark, db) -> None:
    if benchmark.disabled:
        pytest.skip("needs the pedantic loop; run without --benchmark-disable")
    bench.WORKLOADS[name](benchmark, db.cursor(), db)
    assert benchmark.extra_info == {
        "workload_revision": bench.WORKLOAD_REVISION,
        "verified_rounds": ROUNDS[name],
    }


@pytest.mark.parametrize("name", sorted(bench.WORKLOADS))
def test_workload_verifies_when_benchmark_is_disabled(name: str, db) -> None:
    fake = _disabled()
    bench.WORKLOADS[name](fake, db.cursor(), db)
    assert fake.extra_info["verified_rounds"] == 1


@pytest.mark.parametrize("name", sorted(bench.WORKLOADS))
def test_back_to_back_runs_start_from_the_same_state(name: str, db) -> None:
    db.execute(bench.INSERT_SQL, (999, "leftover", 999))
    db.commit()
    bench.WORKLOADS[name](_disabled(), db.cursor(), db)
    first = _table(db)
    bench.WORKLOADS[name](_disabled(), db.cursor(), db)
    assert _table(db) == first
    assert (999, "leftover", 999) not in first


@pytest.mark.parametrize(
    ("name", "keys", "message"),
    [
        ("insert_one_row", (2,), "after INSERT"),
        ("select_one_row_by_pk", (2,), "SELECT by PK"),
        ("update_one_row_by_pk", (2,), "UPDATE rowcount"),
        ("delete_one_row_by_pk", (2,), "DELETE rowcount"),
        ("insert_100_rows", range(2, 102), "after 100 INSERTs"),
        ("execute_parameterized_loop_100", range(2, 102), "parameterized SELECT loop"),
    ],
)
def test_wrong_key_fails_verification(name: str, keys: Any, message: str, db) -> None:
    with pytest.raises(bench.WorkloadVerificationError, match=message):
        bench.WORKLOADS[name](_disabled(), db.cursor(), db, keys=keys)


def test_wrong_payload_fails_verification(db, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        bench, "SELECT_BY_ID_SQL", f"SELECT id, name, amount + 1 FROM {bench.TABLE} WHERE id = ?"
    )
    with pytest.raises(bench.WorkloadVerificationError, match="SELECT by PK"):
        bench.bench_select_one_row_by_pk(_disabled(), db.cursor(), db)


def test_missing_rows_in_a_scan_fail_verification(db, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(bench, "SELECT_ALL_SQL", bench.SELECT_ALL_SQL + " WHERE id > 1")
    with pytest.raises(bench.WorkloadVerificationError):
        bench.bench_select_100_rows(_disabled(), db.cursor(), db)


def test_benchmarks_connect_to_the_shared_test_endpoint(monkeypatch: pytest.MonkeyPatch) -> None:
    """Gate and connection both come from ``tests/_cubrid_endpoint.py``.

    Before #922 the module gated on ``CUBRID_TEST_URL`` but connected to
    ``CUBRID_HOST``/``CUBRID_PORT``/``CUBRID_DB``, so the destructive DDL could
    reach a server other than the configured test endpoint.
    """
    monkeypatch.setenv("CUBRID_TEST_URL", "cubrid://dba@testhost:34000/benchdb")
    monkeypatch.delenv("CUBRID_TEST_HOST", raising=False)
    monkeypatch.delenv("CUBRID_TEST_PORT", raising=False)
    monkeypatch.delenv("CUBRID_TEST_DB", raising=False)
    monkeypatch.setenv("CUBRID_HOST", "elsewhere")
    monkeypatch.setenv("CUBRID_PORT", "33000")
    monkeypatch.setenv("CUBRID_DB", "proddb")
    calls: list[dict[str, Any]] = []
    monkeypatch.setattr(pycubrid, "connect", lambda **kwargs: calls.append(kwargs))
    bench._connect()
    assert calls == [
        {"host": "testhost", "port": 34000, "database": "benchdb", "user": "dba", "password": ""}
    ]


def test_no_benchmark_claims_prepared_statement_reuse() -> None:
    """The parameterized loop is ordinary ``execute``; it must not be named "prepared"."""
    names = [name for name in dir(bench) if name.startswith(("test_bench", "bench_"))]
    names += list(bench.WORKLOADS)
    assert names
    assert not [name for name in names if "prepared" in name]
