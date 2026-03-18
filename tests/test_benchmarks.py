"""Microbenchmarks for pycubrid driver performance profiling.

These benchmarks require a live CUBRID database.
Run with: pytest tests/test_benchmarks.py -v --benchmark-enable

Skip with: pytest tests/ --ignore=tests/test_benchmarks.py
"""

from __future__ import annotations

import os

import pytest

try:
    import pycubrid
except ImportError:
    pycubrid = None  # type: ignore[assignment]

CUBRID_HOST = os.getenv("CUBRID_HOST", "localhost")
CUBRID_PORT = int(os.getenv("CUBRID_PORT", "33000"))
CUBRID_DB = os.getenv("CUBRID_DB", "demodb")

pytestmark = [
    pytest.mark.skipif(
        not os.getenv("CUBRID_TEST_URL"),
        reason="Set CUBRID_TEST_URL to run benchmarks",
    ),
    pytest.mark.benchmark,
]

TABLE = "bench_pycubrid_micro"


@pytest.fixture(scope="module")
def db_conn():
    """Create a module-scoped database connection."""
    conn = pycubrid.connect(
        host=CUBRID_HOST,
        port=CUBRID_PORT,
        database=CUBRID_DB,
        user="dba",
        password="",
    )
    cursor = conn.cursor()
    cursor.execute(f"DROP TABLE IF EXISTS {TABLE}")
    cursor.execute(
        f"CREATE TABLE {TABLE} (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(100), amount INT)"
    )
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
    """Create a cursor and clean the table before each benchmark."""
    cur = db_conn.cursor()
    cur.execute(f"DELETE FROM {TABLE}")
    db_conn.commit()
    yield cur


def test_bench_connect_disconnect(benchmark):
    """Measure connection establishment and teardown latency."""

    def connect_cycle():
        conn = pycubrid.connect(
            host=CUBRID_HOST,
            port=CUBRID_PORT,
            database=CUBRID_DB,
            user="dba",
            password="",
        )
        conn.close()

    benchmark.pedantic(connect_cycle, rounds=20, warmup_rounds=2)


def test_bench_single_insert(benchmark, cursor, db_conn):
    """Measure single row INSERT latency."""

    def single_insert():
        cursor.execute(
            f"INSERT INTO {TABLE} (name, amount) VALUES (?, ?)",
            ("bench", 42),
        )
        db_conn.commit()

    benchmark.pedantic(single_insert, rounds=100, warmup_rounds=5)


def test_bench_single_select(benchmark, cursor, db_conn):
    """Measure single row SELECT by PK latency."""
    cursor.execute(f"INSERT INTO {TABLE} (name, amount) VALUES (?, ?)", ("select_test", 1))
    db_conn.commit()

    def single_select():
        cursor.execute(f"SELECT id, name, amount FROM {TABLE} WHERE id = ?", (1,))
        cursor.fetchone()

    benchmark.pedantic(single_select, rounds=100, warmup_rounds=5)


def test_bench_bulk_insert_100(benchmark, cursor, db_conn):
    """Measure 100-row bulk INSERT throughput."""

    def bulk_insert():
        cursor.execute(f"DELETE FROM {TABLE}")
        for i in range(1, 101):
            cursor.execute(
                f"INSERT INTO {TABLE} (name, amount) VALUES (?, ?)",
                (f"bulk_{i:05d}", i),
            )
        db_conn.commit()

    benchmark.pedantic(bulk_insert, rounds=10, warmup_rounds=1)


def test_bench_select_all_100(benchmark, cursor, db_conn):
    """Measure full table scan of 100 rows."""
    for i in range(1, 101):
        cursor.execute(
            f"INSERT INTO {TABLE} (name, amount) VALUES (?, ?)",
            (f"scan_{i:05d}", i),
        )
    db_conn.commit()

    def select_all():
        cursor.execute(f"SELECT id, name, amount FROM {TABLE}")
        cursor.fetchall()

    benchmark.pedantic(select_all, rounds=50, warmup_rounds=5)


def test_bench_prepared_reuse(benchmark, cursor, db_conn):
    """Measure prepared statement reuse efficiency (same query, different params)."""
    for i in range(1, 101):
        cursor.execute(
            f"INSERT INTO {TABLE} (name, amount) VALUES (?, ?)",
            (f"prep_{i:05d}", i),
        )
    db_conn.commit()

    def prepared_loop():
        for i in range(1, 101):
            cursor.execute(f"SELECT id, name, amount FROM {TABLE} WHERE amount = ?", (i,))
            cursor.fetchone()

    benchmark.pedantic(prepared_loop, rounds=10, warmup_rounds=1)
