#!/usr/bin/env python3
"""Run a Docker integration lane for ``make integration`` / ``make integration-tls`` (#1007).

The Makefile recipes are a single call to this script, which owns the lifecycle the
recipes used to spell out in shell:

1. ``docker compose up -d`` with the broker published on ``--port``;
2. ``scripts/wait_for_cubrid.py 36 5``, given the compose container ID in
   ``CUBRID_TEST_DOCKER_CONTAINER`` so a volume registered for a stale host fails
   fast with a repair hint (#1000);
3. pytest for the lane, writing JUnit XML to ``--results``;
4. ``scripts/check_integration_lanes.py --results ...``, which fails when every
   selected test skipped;
5. ``docker compose down`` (never ``-v``: the named data volumes are reused, and only
   ``make docker-clean`` deletes them), whatever happened before.

Exit status: the first failing step's status. A cleanup failure is reported and fails
an otherwise successful run, but never hides an earlier failure. SIGINT and SIGTERM
stop the running step, still run cleanup and exit 130.

The endpoint is pinned to the compose service (``localhost:<port>/testdb`` as ``dba``)
so a stray ``CUBRID_TEST_*`` export cannot redirect the run to another server.

Usage (see the Makefile)::

    python scripts/run_integration.py --lane normal --python python3 \
        --pytest "python3 -m pytest" --port 33000 --results integration-results.xml
"""

from __future__ import annotations

import argparse
import os
import shlex
import signal
import subprocess
import sys
from collections.abc import Sequence
from pathlib import Path
from types import FrameType

ROOT = Path(__file__).resolve().parents[1]
INTERRUPTED = 130
READINESS_ARGS = ("36", "5")
# Which tests each lane runs is unchanged from the former Makefile recipes.
LANES = {
    "normal": ("tests/", "-m", "integration and not tls", "-v"),
    "tls": ("tests/test_aio_ssl_integration.py", "-v"),
}
TLS_NOTE = (
    "NOTE: requires CUBRID_TLS_TEST_HOST/PORT/CA/DB/USER env vars and a broker with SSL=ON.\n"
    "      For an automated equivalent including SSL=ON flip + cert extraction,\n"
    "      see the 'integration-tls' job in .github/workflows/integration-full.yml."
)


class Interrupted(Exception):
    """Raised from the SIGINT/SIGTERM handler to unwind into cleanup."""


# Only the first signal unwinds the lane; later ones (including any during cleanup)
# are recorded, so the stop-and-clean-up path itself is never interrupted.
_signals: dict[str, object] = {"armed": False, "received": None}


def _interrupt(signum: int, frame: FrameType | None) -> None:
    name = signal.Signals(signum).name
    _signals["received"] = _signals["received"] or name
    if _signals["armed"]:
        _signals["armed"] = False
        raise Interrupted(name)


def _run(command: Sequence[str], env: dict[str, str] | None = None) -> int:
    """Run one step; on an interrupt, stop it before the interrupt propagates."""
    process = subprocess.Popen(command, cwd=ROOT, env=env)  # nosec B603 - fixed argv
    try:
        status = process.wait()
    except Interrupted:
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
        raise
    # A step killed by a signal reports -N; report it as a shell would (128 + N).
    return 128 - status if status < 0 else status


def _compose_container() -> str:
    """Return the compose ``cubrid`` container ID, or "" when it cannot be read."""
    completed = subprocess.run(  # nosec B603 B607 - fixed docker compose argv
        ["docker", "compose", "ps", "-q", "cubrid"],
        cwd=ROOT,
        stdout=subprocess.PIPE,
        text=True,
        check=False,
    )
    return completed.stdout.strip() if completed.returncode == 0 else ""


def _lane(args: argparse.Namespace, compose_env: dict[str, str]) -> int:
    status = _run(["docker", "compose", "up", "-d"], compose_env)
    if status:
        return status
    print("CUBRID container starting...", flush=True)
    python = shlex.split(args.python)
    test_env = {
        **compose_env,
        "CUBRID_TEST_URL": f"cubrid://dba@localhost:{args.port}/testdb",
        "CUBRID_TEST_HOST": "localhost",
        "CUBRID_TEST_DB": "testdb",
        "CUBRID_TEST_USER": "dba",
        "CUBRID_TEST_PASSWORD": "",
        "CUBRID_TEST_DOCKER_CONTAINER": _compose_container(),
    }
    steps = (
        [*python, "scripts/wait_for_cubrid.py", *READINESS_ARGS],
        [*shlex.split(args.pytest), *LANES[args.lane], f"--junitxml={args.results}"],
        [*python, "scripts/check_integration_lanes.py", "--results", args.results],
    )
    for step in steps:
        status = _run(step, test_env)
        if status:
            return status
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n", 1)[0])
    parser.add_argument("--lane", choices=sorted(LANES), required=True)
    parser.add_argument("--python", default=sys.executable, help="interpreter command")
    parser.add_argument("--pytest", required=True, help='pytest command, e.g. "python3 -m pytest"')
    parser.add_argument("--port", required=True, help="host port of the compose broker")
    parser.add_argument("--results", required=True, help="JUnit XML path")
    args = parser.parse_args(argv)

    if args.lane == "tls":
        print(TLS_NOTE, flush=True)
    compose_env = {**os.environ, "CUBRID_TEST_PORT": args.port}
    _signals["armed"] = True
    for sig in (signal.SIGINT, signal.SIGTERM):
        # Like a shell trap, keep a signal ignored on entry ignored: a background job
        # of a non-interactive shell starts with SIGINT ignored (POSIX).
        if signal.getsignal(sig) is not signal.SIG_IGN:
            signal.signal(sig, _interrupt)
    try:
        try:
            status = _lane(args, compose_env)
        finally:
            _signals["armed"] = False
    except Interrupted:
        status = INTERRUPTED
    if _signals["received"]:
        print(f"Interrupted by {_signals['received']}; cleaning up", file=sys.stderr, flush=True)
        status = INTERRUPTED

    cleanup = subprocess.run(  # nosec B603 B607 - fixed docker compose argv
        ["docker", "compose", "down"], cwd=ROOT, env=compose_env, check=False
    ).returncode
    if cleanup:
        print(f"ERROR: Docker cleanup failed with status {cleanup}", file=sys.stderr, flush=True)
        status = status or cleanup
    return status


if __name__ == "__main__":
    raise SystemExit(main())
