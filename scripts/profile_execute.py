"""Profile prepare+execute cycles for INSERT/SELECT/UPDATE/DELETE in pycubrid.

Creates its own scratch table ``_profile_tmp_<12 hex digits>`` with a plain
``CREATE TABLE``, so it never reuses or drops an existing table, and drops only
that table after the run, also when the workload fails (#950). A run that is
killed or loses its connection can leave the table behind; drop it by hand.

Every run writes to the target database, ``--operation select`` included: it is
not a read-only command. Run it only against a disposable database; the default
is ``demodb``.

Usage
-----
    python scripts/profile_execute.py [options]

Examples
--------
    # All operations, 100 iterations each (default):
    python scripts/profile_execute.py

    # Only INSERT, 200 iterations, save .prof for snakeviz:
    python scripts/profile_execute.py --operation insert --iterations 200 --output exec.prof

    # SELECT only, against a disposable database on another host:
    python scripts/profile_execute.py --host myhost --database scratchdb --operation select

    # Visualise saved profile:
    pip install snakeviz
    snakeviz exec.prof
"""

from __future__ import annotations

import argparse
import cProfile
import pstats
import sys
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Callable

sys.path.insert(0, str(Path(__file__).resolve().parent))

from _profile_scratch import run_with_scratch_table  # noqa: E402

OPERATIONS = ("insert", "select", "update", "delete")

# ``{table}`` is filled only with the run's validated scratch-table name.
_SQL = {
    "insert": "INSERT INTO {table} (val) VALUES (?)",
    "select": "SELECT id, val FROM {table} WHERE id > ?",
    "update": "UPDATE {table} SET val = ? WHERE id > ?",
    "delete": "DELETE FROM {table} WHERE id > ?",
}


_WARNING = (
    "Writes to the target database: every run, --operation select included, "
    "creates and drops its own table _profile_tmp_<12 hex digits>. "
    "Run it only against a disposable database."
)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Profile pycubrid prepare+execute cycles for DML operations.",
        epilog=_WARNING,
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


def _run_insert(cursor: Any, sql: Mapping[str, str], iterations: int) -> None:
    for i in range(iterations):
        cursor.execute(sql["insert"], ("row-" + str(i),))


def _run_select(cursor: Any, sql: Mapping[str, str], iterations: int) -> None:
    for _ in range(iterations):
        cursor.execute(sql["select"], (0,))
        cursor.fetchall()


def _run_update(cursor: Any, sql: Mapping[str, str], iterations: int) -> None:
    for i in range(iterations):
        cursor.execute(sql["update"], ("upd-" + str(i), 0))


def _run_delete(cursor: Any, sql: Mapping[str, str], iterations: int) -> None:
    # Re-seed before deleting so there is always data
    cursor.execute(sql["insert"], ("seed",))
    for _ in range(iterations):
        cursor.execute(sql["delete"], (0,))
        # Re-insert one row so subsequent deletes have something to hit
        cursor.execute(sql["insert"], ("seed",))


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
    """Create an owned scratch table, run the chosen operation(s), then tear down."""
    import pycubrid  # noqa: PLC0415 — intentional late import in profiling script

    conn = pycubrid.connect(
        host=host,
        port=port,
        database=database,
        user=user,
        password=password,
    )

    def workload(cursor: Any, sql: Mapping[str, str]) -> None:
        ops_to_run = list(OPERATIONS) if operation == "all" else [operation]
        for op in ops_to_run:
            print(f"  Profiling: {op} ({iterations} iterations)")
            _RUNNERS[op](cursor, sql, iterations)
            conn.commit()

    run_with_scratch_table(conn, workload, _SQL)


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
