"""Microbenchmarks for pycubrid driver performance profiling.

Tier 0 — Connection: open/close latency, handshake time
Tier 1 — Single-row CRUD by primary key: INSERT, SELECT, UPDATE, DELETE latency
Tier 1+ — 100-row INSERT/SELECT and a loop of 100 ordinary parameterized executes

These benchmarks require a live, disposable CUBRID database: they drop and
recreate the table ``bench_pycubrid_micro``. They run only when the shared test
endpoint is configured (``CUBRID_TEST_URL`` or ``CUBRID_TEST_HOST``) and connect
to exactly that endpoint (``tests/_cubrid_endpoint.py``), the same server the
integration tests use.

Run with: pytest tests/test_benchmarks.py -v --benchmark-enable

Skip with: pytest tests/ --ignore=tests/test_benchmarks.py

Workload revision ``micro-explicit-keys-v2`` (#922):

- The table has explicit primary keys and no ``AUTO_INCREMENT`` column, so a
  ``DELETE`` followed by a re-seed always produces the same keys.
- Every round, warm-up rounds included, starts from the same verified state,
  built by the untimed pytest-benchmark ``setup``.
- The timed region holds only the measured statements and, for writes, their
  ``commit()``. Results are only collected there (``fetchone()``, ``rowcount``).
- The untimed ``teardown`` checks every fetched row, every affected-row count
  and the full table contents. A missing or wrong row raises
  ``WorkloadVerificationError`` and fails the benchmark instead of reporting a
  time. ``extra_info`` records the revision and, for every benchmark that
  touches rows, the number of verified rounds.

Results of the earlier workload (before #922) are historical: its primary-key
SELECT looked up a row that did not exist, and other cases timed different
work than their names said. The benchmarks were renamed so
``scripts/bench_regression.py`` does not compare the two.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Any

import pytest

from ._cubrid_endpoint import is_configured, resolve_endpoint

try:
    import pycubrid
except ImportError:
    pycubrid = None  # type: ignore[assignment]

pytestmark = [
    pytest.mark.skipif(
        not is_configured(),
        reason="Set CUBRID_TEST_URL or CUBRID_TEST_HOST to run benchmarks",
    ),
    pytest.mark.benchmark,
    pytest.mark.no_escape_pin,
]

WORKLOAD_REVISION = "micro-explicit-keys-v2"
TABLE = "bench_pycubrid_micro"
CREATE_SQL = f"CREATE TABLE {TABLE} (id INT PRIMARY KEY, name VARCHAR(100), amount INT)"
INSERT_SQL = f"INSERT INTO {TABLE} (id, name, amount) VALUES (?, ?, ?)"
SELECT_BY_ID_SQL = f"SELECT id, name, amount FROM {TABLE} WHERE id = ?"
SELECT_BY_AMOUNT_SQL = f"SELECT id, name, amount FROM {TABLE} WHERE amount = ?"
SELECT_ALL_SQL = f"SELECT id, name, amount FROM {TABLE}"
UPDATE_SQL = f"UPDATE {TABLE} SET amount = ? WHERE id = ?"
DELETE_SQL = f"DELETE FROM {TABLE} WHERE id = ?"
UPDATED_AMOUNT = 100_000
BULK_ROWS = 100

Row = tuple[Any, ...]


class WorkloadVerificationError(AssertionError):
    """A benchmark round did not perform the intended database work."""


def _row(prefix: str, key: int) -> Row:
    return (key, f"{prefix}_{key:05d}", key)


def _rows(prefix: str, keys: Sequence[int]) -> list[Row]:
    return [_row(prefix, key) for key in keys]


def _expect_table(cursor: Any, expected: Sequence[Row], context: str) -> None:
    """Compare the whole table, in key order, with ``expected``."""
    cursor.execute(SELECT_ALL_SQL + " ORDER BY id")
    actual = [tuple(row) for row in cursor.fetchall()]
    if actual != list(expected):
        raise WorkloadVerificationError(
            f"{context}: expected {len(expected)} rows {list(expected)[:3]}..., "
            f"got {len(actual)} rows {actual[:3]}..."
        )


def _expect(observed: Sequence[Any], expected: Sequence[Any], context: str) -> None:
    if list(observed) != list(expected):
        raise WorkloadVerificationError(f"{context}: expected {list(expected)}, got {observed}")


def run_verified(
    benchmark: Any,
    cursor: Any,
    conn: Any,
    run: Callable[[list[Any]], None],
    verify: Callable[[list[Any]], None],
    *,
    seed: Sequence[Row],
    rounds: int,
    warmup_rounds: int,
) -> None:
    """Time ``run`` with an untimed seeded ``setup`` and an untimed ``verify`` per round.

    ``run`` receives a fresh list in which it collects results; ``verify`` checks
    that list and the table after the round.
    """
    benchmark.extra_info["workload_revision"] = WORKLOAD_REVISION
    benchmark.extra_info["verified_rounds"] = 0

    def setup() -> tuple[tuple[list[Any]], dict[str, Any]]:
        cursor.execute(f"DELETE FROM {TABLE}")
        for row in seed:
            cursor.execute(INSERT_SQL, row)
        conn.commit()
        _expect_table(cursor, seed, "seeded table before the round")
        return ([],), {}

    def teardown(observed: list[Any]) -> None:
        verify(observed)
        benchmark.extra_info["verified_rounds"] += 1

    if benchmark.disabled:
        # pytest-benchmark runs a disabled benchmark once and skips ``teardown``.
        args, _ = setup()
        run(*args)
        teardown(*args)
        return

    benchmark.pedantic(
        run, setup=setup, teardown=teardown, rounds=rounds, warmup_rounds=warmup_rounds
    )


def _connect() -> Any:
    return pycubrid.connect(**resolve_endpoint().connect_kwargs())


@pytest.fixture(scope="module")
def db_conn():
    """Create a module-scoped database connection and the benchmark table."""
    conn = _connect()
    cursor = conn.cursor()
    cursor.execute(f"DROP TABLE IF EXISTS {TABLE}")
    cursor.execute(CREATE_SQL)
    conn.commit()
    try:
        yield conn
    finally:
        cursor.execute(f"DROP TABLE IF EXISTS {TABLE}")
        conn.commit()
        cursor.close()
        conn.close()


@pytest.fixture
def cursor(db_conn):
    """A cursor; each benchmark's untimed setup brings the table to its state."""
    cur = db_conn.cursor()
    try:
        yield cur
    finally:
        cur.close()


# Workloads. ``keys`` overrides the keys the timed region addresses, so the
# offline tests can prove that a wrong key fails verification.


def bench_insert_one_row(benchmark, cursor, conn, keys: Sequence[int] = (1,)) -> None:
    def run(observed: list[Any]) -> None:
        for key in keys:
            cursor.execute(INSERT_SQL, _row("insert", key))
        conn.commit()

    def verify(observed: list[Any]) -> None:
        _expect_table(cursor, [_row("insert", 1)], "after INSERT")

    run_verified(benchmark, cursor, conn, run, verify, seed=[], rounds=100, warmup_rounds=5)


def bench_select_one_row_by_pk(benchmark, cursor, conn, keys: Sequence[int] = (1,)) -> None:
    seed = [_row("select", 1)]

    def run(observed: list[Any]) -> None:
        for key in keys:
            cursor.execute(SELECT_BY_ID_SQL, (key,))
            observed.append(cursor.fetchone())

    def verify(observed: list[Any]) -> None:
        _expect([None if r is None else tuple(r) for r in observed], seed, "SELECT by PK")
        _expect_table(cursor, seed, "after SELECT by PK")

    run_verified(benchmark, cursor, conn, run, verify, seed=seed, rounds=100, warmup_rounds=5)


def bench_update_one_row_by_pk(benchmark, cursor, conn, keys: Sequence[int] = (1,)) -> None:
    def run(observed: list[Any]) -> None:
        for key in keys:
            cursor.execute(UPDATE_SQL, (UPDATED_AMOUNT, key))
            observed.append(cursor.rowcount)
        conn.commit()

    def verify(observed: list[Any]) -> None:
        _expect(observed, [1], "UPDATE rowcount")
        _expect_table(cursor, [(1, "update_00001", UPDATED_AMOUNT)], "after UPDATE")

    run_verified(
        benchmark,
        cursor,
        conn,
        run,
        verify,
        seed=[_row("update", 1)],
        rounds=100,
        warmup_rounds=5,
    )


def bench_delete_one_row_by_pk(benchmark, cursor, conn, keys: Sequence[int] = (1,)) -> None:
    def run(observed: list[Any]) -> None:
        for key in keys:
            cursor.execute(DELETE_SQL, (key,))
            observed.append(cursor.rowcount)
        conn.commit()

    def verify(observed: list[Any]) -> None:
        _expect(observed, [1], "DELETE rowcount")
        _expect_table(cursor, [], "after DELETE")

    run_verified(
        benchmark,
        cursor,
        conn,
        run,
        verify,
        seed=[_row("delete", 1)],
        rounds=100,
        warmup_rounds=5,
    )


def bench_insert_100_rows(
    benchmark, cursor, conn, keys: Sequence[int] = range(1, BULK_ROWS + 1)
) -> None:
    def run(observed: list[Any]) -> None:
        for key in keys:
            cursor.execute(INSERT_SQL, _row("bulk", key))
        conn.commit()

    def verify(observed: list[Any]) -> None:
        _expect_table(cursor, _rows("bulk", range(1, BULK_ROWS + 1)), "after 100 INSERTs")

    run_verified(benchmark, cursor, conn, run, verify, seed=[], rounds=10, warmup_rounds=1)


def bench_select_100_rows(benchmark, cursor, conn) -> None:
    seed = _rows("scan", range(1, BULK_ROWS + 1))

    def run(observed: list[Any]) -> None:
        cursor.execute(SELECT_ALL_SQL)
        observed.extend(cursor.fetchall())

    def verify(observed: list[Any]) -> None:
        # The scan has no ORDER BY, as before #922; compare in key order.
        _expect(sorted(tuple(r) for r in observed), seed, "SELECT of 100 rows")
        _expect_table(cursor, seed, "after SELECT of 100 rows")

    run_verified(benchmark, cursor, conn, run, verify, seed=seed, rounds=50, warmup_rounds=5)


def bench_execute_parameterized_loop_100(
    benchmark, cursor, conn, keys: Sequence[int] = range(1, BULK_ROWS + 1)
) -> None:
    """100 ordinary ``execute(sql, params)`` calls, each one PREPARE_AND_EXECUTE.

    This is not server-side prepared-statement reuse, and not the opt-in
    ``executemany(..., prepared=True)`` (FC21) path.
    """
    seed = _rows("loop", range(1, BULK_ROWS + 1))

    def run(observed: list[Any]) -> None:
        for key in keys:
            cursor.execute(SELECT_BY_AMOUNT_SQL, (key,))
            observed.append(cursor.fetchone())

    def verify(observed: list[Any]) -> None:
        _expect(
            [None if r is None else tuple(r) for r in observed], seed, "parameterized SELECT loop"
        )
        _expect_table(cursor, seed, "after parameterized SELECT loop")

    run_verified(benchmark, cursor, conn, run, verify, seed=seed, rounds=10, warmup_rounds=1)


WORKLOADS: dict[str, Callable[..., None]] = {
    "insert_one_row": bench_insert_one_row,
    "select_one_row_by_pk": bench_select_one_row_by_pk,
    "update_one_row_by_pk": bench_update_one_row_by_pk,
    "delete_one_row_by_pk": bench_delete_one_row_by_pk,
    "insert_100_rows": bench_insert_100_rows,
    "select_100_rows": bench_select_100_rows,
    "execute_parameterized_loop_100": bench_execute_parameterized_loop_100,
}


def test_bench_connect_disconnect(benchmark):
    """Tier 0: Measure connection establishment and teardown latency."""
    benchmark.extra_info["workload_revision"] = WORKLOAD_REVISION

    def connect_cycle():
        conn = _connect()
        conn.close()

    benchmark.pedantic(connect_cycle, rounds=20, warmup_rounds=2)


def test_bench_insert_one_row(benchmark, cursor, db_conn):
    """Tier 1: one INSERT with an explicit key + commit, into an empty table."""
    bench_insert_one_row(benchmark, cursor, db_conn)


def test_bench_select_one_row_by_pk(benchmark, cursor, db_conn):
    """Tier 1: one SELECT by primary key + fetchone of an existing row."""
    bench_select_one_row_by_pk(benchmark, cursor, db_conn)


def test_bench_update_one_row_by_pk(benchmark, cursor, db_conn):
    """Tier 1: one UPDATE by primary key + commit, affecting exactly one row."""
    bench_update_one_row_by_pk(benchmark, cursor, db_conn)


def test_bench_delete_one_row_by_pk(benchmark, cursor, db_conn):
    """Tier 1: one DELETE by primary key + commit, affecting exactly one row."""
    bench_delete_one_row_by_pk(benchmark, cursor, db_conn)


def test_bench_insert_100_rows(benchmark, cursor, db_conn):
    """100 single-row INSERTs with explicit keys + one commit, into an empty table."""
    bench_insert_100_rows(benchmark, cursor, db_conn)


def test_bench_select_100_rows(benchmark, cursor, db_conn):
    """One SELECT + fetchall of all 100 seeded rows."""
    bench_select_100_rows(benchmark, cursor, db_conn)


def test_bench_execute_parameterized_loop_100(benchmark, cursor, db_conn):
    """100 ordinary parameterized SELECT executes + fetchone, each hitting one row."""
    bench_execute_parameterized_loop_100(benchmark, cursor, db_conn)
