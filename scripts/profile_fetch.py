"""Profile result-fetching operations (fetchone, fetchall, fetchmany) for pycubrid.

Pre-inserts a configurable number of rows into its own scratch table
``_profile_tmp_<12 hex digits>``, then benchmarks each fetch variant over multiple
iterations. The table is created with a plain ``CREATE TABLE``, so the run never
reuses or drops an existing table, and only that table is dropped after the run,
also when the workload fails (#950). A run that is killed or loses its connection
can leave the table behind; drop it by hand.

Every run writes to the target database. Run it only against a disposable
database; the default is ``demodb``.

Usage
-----
    python scripts/profile_fetch.py [options]

Examples
--------
    # Default: 1000 rows, 50 fetch iterations, all fetch styles:
    python scripts/profile_fetch.py

    # 5000 rows, 20 iterations, fetchmany with batch size 100:
    python scripts/profile_fetch.py --rows 5000 --iterations 20 --fetch-size 100

    # Save .prof for snakeviz:
    python scripts/profile_fetch.py --output fetch.prof

    # Visualise saved profile:
    pip install snakeviz
    snakeviz fetch.prof
"""

from __future__ import annotations

import argparse
import cProfile
import pstats
import sys
from collections.abc import Mapping
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))

from _profile_scratch import run_with_scratch_table  # noqa: E402

# ``{table}`` is filled only with the run's validated scratch-table name.
_SQL = {
    "insert": "INSERT INTO {table} (val) VALUES (?)",
    "select": "SELECT id, val FROM {table} WHERE id > ?",
}


_WARNING = (
    "Writes to the target database: every run "
    "creates, fills and drops its own table _profile_tmp_<12 hex digits>. "
    "Run it only against a disposable database."
)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Profile pycubrid fetch operations (fetchone, fetchall, fetchmany).",
        epilog=_WARNING,
    )
    parser.add_argument("--host", default="localhost", help="CUBRID host (default: localhost)")
    parser.add_argument("--port", type=int, default=33000, help="CAS port (default: 33000)")
    parser.add_argument("--database", default="demodb", help="Database name (default: demodb)")
    parser.add_argument("--user", default="dba", help="DB user (default: dba)")
    parser.add_argument("--password", default="", help="DB password (default: empty)")
    parser.add_argument(
        "--rows",
        type=int,
        default=1000,
        help="Number of rows to pre-insert for fetching (default: 1000)",
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=50,
        help="Number of fetch iterations per fetch style (default: 50)",
    )
    parser.add_argument(
        "--fetch-size",
        type=int,
        default=50,
        dest="fetch_size",
        help="Batch size for fetchmany (default: 50)",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Save .prof file to this path for snakeviz (optional)",
    )
    return parser


def _seed_rows(cursor: Any, row_count: int, *, sql: str) -> None:
    """Insert *row_count* rows using executemany for speed."""
    params = [("seed-" + str(i),) for i in range(row_count)]
    cursor.executemany(sql, params)


def run_fetchone(cursor: Any, iterations: int, *, sql: str) -> None:
    """Execute a full-table SELECT and consume rows one by one."""
    for _ in range(iterations):
        cursor.execute(sql, (0,))
        while True:
            row = cursor.fetchone()
            if row is None:
                break


def run_fetchall(cursor: Any, iterations: int, *, sql: str) -> None:
    """Execute a full-table SELECT and consume all rows in one call."""
    for _ in range(iterations):
        cursor.execute(sql, (0,))
        cursor.fetchall()


def run_fetchmany(cursor: Any, iterations: int, fetch_size: int, *, sql: str) -> None:
    """Execute a full-table SELECT and consume rows in batches of *fetch_size*."""
    for _ in range(iterations):
        cursor.execute(sql, (0,))
        while True:
            batch = cursor.fetchmany(fetch_size)
            if not batch:
                break


def run_fetch_cycle(
    host: str,
    port: int,
    database: str,
    user: str,
    password: str,
    row_count: int,
    iterations: int,
    fetch_size: int,
) -> None:
    """Seed an owned scratch table, profile all three fetch styles, then tear down."""
    import pycubrid  # noqa: PLC0415 — intentional late import in profiling script

    conn = pycubrid.connect(
        host=host,
        port=port,
        database=database,
        user=user,
        password=password,
    )

    def workload(cursor: Any, sql: Mapping[str, str]) -> None:
        _seed_rows(cursor, row_count, sql=sql["insert"])
        conn.commit()

        print(f"  Profiling: fetchone  ({iterations} iterations × {row_count} rows)")
        run_fetchone(cursor, iterations, sql=sql["select"])

        print(f"  Profiling: fetchall  ({iterations} iterations × {row_count} rows)")
        run_fetchall(cursor, iterations, sql=sql["select"])

        print(
            f"  Profiling: fetchmany ({iterations} iterations × {row_count} rows, "
            f"batch={fetch_size})"
        )
        run_fetchmany(cursor, iterations, fetch_size, sql=sql["select"])

    run_with_scratch_table(conn, workload, _SQL)


def main() -> None:
    args = _build_parser().parse_args()

    profiler = cProfile.Profile()
    profiler.enable()

    try:
        run_fetch_cycle(
            host=args.host,
            port=args.port,
            database=args.database,
            user=args.user,
            password=args.password,
            row_count=args.rows,
            iterations=args.iterations,
            fetch_size=args.fetch_size,
        )
    finally:
        profiler.disable()

    if args.output:
        profiler.dump_stats(args.output)
        print(f"Profile saved to: {args.output}")

    stats = pstats.Stats(profiler, stream=sys.stdout)
    stats.sort_stats("cumulative")
    stats.print_stats(30)


if __name__ == "__main__":
    main()
