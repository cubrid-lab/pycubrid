"""Profile the connection open+close cycle for pycubrid.

Usage
-----
    python scripts/profile_connect.py [options]

Examples
--------
    # Basic run with defaults (100 iterations, localhost:33000/demodb):
    python scripts/profile_connect.py

    # Custom target, 50 iterations, save .prof for snakeviz:
    python scripts/profile_connect.py --host myhost --port 33000 --database testdb \\
        --user dba --password secret --iterations 50 --output connect.prof

    # Visualise saved profile:
    pip install snakeviz
    snakeviz connect.prof
"""

from __future__ import annotations

import argparse
import cProfile
import pstats
import sys


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Profile pycubrid connection open+close handshake cycle."
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
        help="Number of connect/close cycles to run (default: 100)",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Save .prof file to this path for snakeviz (optional)",
    )
    return parser


def run_connect_cycle(
    host: str, port: int, database: str, user: str, password: str, iterations: int
) -> None:
    """Open and immediately close a connection *iterations* times."""
    import pycubrid  # noqa: PLC0415 — intentional late import in profiling script

    for _ in range(iterations):
        conn = pycubrid.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password,
        )
        conn.close()


def main() -> None:
    args = _build_parser().parse_args()

    profiler = cProfile.Profile()
    profiler.enable()

    try:
        run_connect_cycle(
            host=args.host,
            port=args.port,
            database=args.database,
            user=args.user,
            password=args.password,
            iterations=args.iterations,
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
