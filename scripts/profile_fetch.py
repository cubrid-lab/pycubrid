"""Profile result-fetching operations (fetchone, fetchall, fetchmany) for pycubrid.

Pre-inserts a configurable number of rows into a temporary table ``_profile_tmp``,
then benchmarks each fetch variant over multiple iterations.

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
from typing import Any


_CREATE_TABLE = (
    "CREATE TABLE IF NOT EXISTS _profile_tmp (id INT AUTO_INCREMENT PRIMARY KEY, val VARCHAR(64))"
)
_DROP_TABLE = "DROP TABLE IF EXISTS _profile_tmp"
_INSERT_SQL = "INSERT INTO _profile_tmp (val) VALUES (?)"
_SELECT_ALL_SQL = "SELECT id, val FROM _profile_tmp WHERE id > ?"


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Profile pycubrid fetch operations (fetchone, fetchall, fetchmany)."
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


def _seed_rows(cursor: Any, row_count: int) -> None:
    """Insert *row_count* rows using executemany for speed."""
    params = [("seed-" + str(i),) for i in range(row_count)]
    cursor.executemany(_INSERT_SQL, params)


def run_fetchone(cursor: Any, iterations: int) -> None:
    """Execute a full-table SELECT and consume rows one by one."""
    for _ in range(iterations):
        cursor.execute(_SELECT_ALL_SQL, (0,))
        while True:
            row = cursor.fetchone()
            if row is None:
                break


def run_fetchall(cursor: Any, iterations: int) -> None:
    """Execute a full-table SELECT and consume all rows in one call."""
    for _ in range(iterations):
        cursor.execute(_SELECT_ALL_SQL, (0,))
        cursor.fetchall()


def run_fetchmany(cursor: Any, iterations: int, fetch_size: int) -> None:
    """Execute a full-table SELECT and consume rows in batches of *fetch_size*."""
    for _ in range(iterations):
        cursor.execute(_SELECT_ALL_SQL, (0,))
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
    """Seed the temp table and profile all three fetch styles."""
    import pycubrid  # noqa: PLC0415 — intentional late import in profiling script

    conn = pycubrid.connect(
        host=host,
        port=port,
        database=database,
        user=user,
        password=password,
    )
    try:
        cursor = conn.cursor()
        cursor.execute(_DROP_TABLE)
        cursor.execute(_CREATE_TABLE)
        _seed_rows(cursor, row_count)
        conn.commit()

        print(f"  Profiling: fetchone  ({iterations} iterations × {row_count} rows)")
        run_fetchone(cursor, iterations)

        print(f"  Profiling: fetchall  ({iterations} iterations × {row_count} rows)")
        run_fetchall(cursor, iterations)

        print(
            f"  Profiling: fetchmany ({iterations} iterations × {row_count} rows, "
            f"batch={fetch_size})"
        )
        run_fetchmany(cursor, iterations, fetch_size)

        cursor.execute(_DROP_TABLE)
        conn.commit()
        cursor.close()
    finally:
        conn.close()


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
