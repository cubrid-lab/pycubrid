"""Profile prepare+execute cycles for INSERT/SELECT/UPDATE/DELETE in pycubrid.

Creates a temporary table ``_profile_tmp`` for the duration of the run, then drops it.

Usage
-----
    python scripts/profile_execute.py [options]

Examples
--------
    # All operations, 100 iterations each (default):
    python scripts/profile_execute.py

    # Only INSERT, 200 iterations, save .prof for snakeviz:
    python scripts/profile_execute.py --operation insert --iterations 200 --output exec.prof

    # SELECT only against a custom host:
    python scripts/profile_execute.py --host myhost --database testdb --operation select

    # Visualise saved profile:
    pip install snakeviz
    snakeviz exec.prof
"""

from __future__ import annotations

import argparse
import cProfile
import pstats
import sys
from typing import Any, Callable


OPERATIONS = ("insert", "select", "update", "delete")

_CREATE_TABLE = (
    "CREATE TABLE IF NOT EXISTS _profile_tmp (id INT AUTO_INCREMENT PRIMARY KEY, val VARCHAR(64))"
)
_DROP_TABLE = "DROP TABLE IF EXISTS _profile_tmp"
_INSERT_SQL = "INSERT INTO _profile_tmp (val) VALUES (?)"
_SELECT_SQL = "SELECT id, val FROM _profile_tmp WHERE id > ?"
_UPDATE_SQL = "UPDATE _profile_tmp SET val = ? WHERE id > ?"
_DELETE_SQL = "DELETE FROM _profile_tmp WHERE id > ?"


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Profile pycubrid prepare+execute cycles for DML operations."
    )
    parser.add_argument("--host", default="localhost", help="CUBRID host (default: localhost)")
    parser.add_argument("--port", type=int, default=33000, help="CAS port (default: 33000)")
    parser.add_argument("--database", default="demodb", help="Database name (default: demodb)")
    parser.add_argument("--user", default="dba", help="DB user (default: dba)")
    parser.add_argument("--password", default="", help="DB password (default: empty)")
    parser.add_argument(
        "--iterations",
        type=int,
        default=100,
        help="Number of execute cycles per operation (default: 100)",
    )
    parser.add_argument(
        "--operation",
        choices=[*OPERATIONS, "all"],
        default="all",
        help="Which operation(s) to profile (default: all)",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Save .prof file to this path for snakeviz (optional)",
    )
    return parser


def _run_insert(cursor: Any, iterations: int) -> None:
    for i in range(iterations):
        cursor.execute(_INSERT_SQL, ("row-" + str(i),))


def _run_select(cursor: Any, iterations: int) -> None:
    for _ in range(iterations):
        cursor.execute(_SELECT_SQL, (0,))
        cursor.fetchall()


def _run_update(cursor: Any, iterations: int) -> None:
    for i in range(iterations):
        cursor.execute(_UPDATE_SQL, ("upd-" + str(i), 0))


def _run_delete(cursor: Any, iterations: int) -> None:
    # Re-seed before deleting so there is always data
    cursor.execute(_INSERT_SQL, ("seed",))
    for _ in range(iterations):
        cursor.execute(_DELETE_SQL, (0,))
        # Re-insert one row so subsequent deletes have something to hit
        cursor.execute(_INSERT_SQL, ("seed",))


_RUNNERS: dict[str, Callable[..., None]] = {
    "insert": _run_insert,
    "select": _run_select,
    "update": _run_update,
    "delete": _run_delete,
}


def run_execute_cycle(
    host: str,
    port: int,
    database: str,
    user: str,
    password: str,
    iterations: int,
    operation: str,
) -> None:
    """Set up the temp table, run the chosen operation(s), then tear down."""
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
        conn.commit()

        ops_to_run = list(OPERATIONS) if operation == "all" else [operation]
        for op in ops_to_run:
            print(f"  Profiling: {op} ({iterations} iterations)")
            _RUNNERS[op](cursor, iterations)
            conn.commit()

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
        run_execute_cycle(
            host=args.host,
            port=args.port,
            database=args.database,
            user=args.user,
            password=args.password,
            iterations=args.iterations,
            operation=args.operation,
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
