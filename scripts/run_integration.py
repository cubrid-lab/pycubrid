#!/usr/bin/env python3
"""Run a Docker integration lane for ``make integration`` / ``make integration-tls`` (#1007).

The Makefile recipes are a single call to this script, which owns the lifecycle the
recipes used to spell out in shell:

0. Pick a run-owned Compose project (``--project``, default
   ``pycubrid-it-<timestamp>-<pid>``) and refuse to run when it already has a
   container, volume or network, and hold an exclusive non-blocking ``flock`` on
   ``<tempdir>/pycubrid-integration-<project>.lock`` through cleanup, so a concurrent
   run of the same name is refused (POSIX only; skipped without ``fcntl``). The run
   never attaches to the ``make docker-up`` project or removes anything it did not
   create (#1021);
1. ``docker compose -p <project> up -d`` with the broker published on ``--port``;
2. ``scripts/wait_for_cubrid.py 36 5``, given the compose container ID in
   ``CUBRID_TEST_DOCKER_CONTAINER`` so a volume registered for a stale host fails
   fast with a repair hint (#1000);
3. pytest for the lane, writing JUnit XML to ``--results``;
4. ``scripts/check_integration_lanes.py --results ...``, which fails when every
   selected test skipped;
5. ``docker compose -p <project> down -v``, whatever happened before. The volumes
   belong to the run-owned project, so each run starts from an empty database and
   leaves nothing behind; the ``make docker-up`` project and its volumes are never
   touched (``make docker-clean`` remains the way to delete those).

Every ``docker compose`` call carries ``-p <project>``. Exit status: the first failing step's status. A cleanup failure is reported and fails
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
import re
import shlex
import signal
import subprocess
import sys
import tempfile
import time
from collections.abc import Sequence
from pathlib import Path
from types import FrameType

try:  # POSIX only: without fcntl (Windows) the same-project guard is skipped.
    import fcntl
except ImportError:  # pragma: no cover - platform dependent
    fcntl = None  # type: ignore[assignment]

ROOT = Path(__file__).resolve().parents[1]
INTERRUPTED = 130
READINESS_ARGS = ("36", "5")
PROJECT_PATTERN = re.compile(r"[a-z0-9][a-z0-9_-]*")
# Volumes of docker-compose.yml (#1000, #1002).
DATA_VOLUMES = ("cubrid-data", "cubrid-databases")
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


def _reserve(project: str) -> int | None:
    """Take an exclusive, non-blocking per-project lock; return its fd or None if held.

    The ownership probes are read-only, so two runs of the same project could both pass
    them and then share, and ``down -v``, each other's resources. ``main`` closes the
    descriptor, releasing the lock, after cleanup; it is not inherited by children.
    """
    if fcntl is None:  # pragma: no cover - platform dependent
        return -1
    fd = os.open(
        os.path.join(tempfile.gettempdir(), f"pycubrid-integration-{project}.lock"),
        os.O_CREAT | os.O_RDWR,
        0o600,
    )
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        os.close(fd)
        return None
    return fd


def _probe(command: Sequence[str]) -> list[str]:
    """Return the output lines of a read-only docker query, or exit with its status."""
    completed = subprocess.run(  # nosec B603 B607 - fixed docker argv
        command, cwd=ROOT, stdout=subprocess.PIPE, text=True, check=False
    )
    if completed.returncode:
        print(
            f"ERROR: ownership check failed with status {completed.returncode}; "
            "nothing was started.",
            file=sys.stderr,
            flush=True,
        )
        raise SystemExit(completed.returncode)
    return [line for line in completed.stdout.splitlines() if line]


def _owns_nothing(project: str) -> bool:
    """Return True when Compose project ``project`` has no resources yet."""
    label = f"label=com.docker.compose.project={project}"
    found = _probe(["docker", "ps", "-aq", "--filter", label])
    found += _probe(["docker", "volume", "ls", "-q", "--filter", label])
    found += _probe(["docker", "network", "ls", "-q", "--filter", label])
    names = {f"{project}_{volume}" for volume in DATA_VOLUMES}
    found += [name for name in _probe(["docker", "volume", "ls", "-q"]) if name in names]
    return not found


def _compose_container(project: str) -> str:
    """Return the compose ``cubrid`` container ID, or "" when it cannot be read."""
    completed = subprocess.run(  # nosec B603 B607 - fixed docker compose argv
        ["docker", "compose", "-p", project, "ps", "-q", "cubrid"],
        cwd=ROOT,
        stdout=subprocess.PIPE,
        text=True,
        check=False,
    )
    return completed.stdout.strip() if completed.returncode == 0 else ""


def _lane(args: argparse.Namespace, project: str, compose_env: dict[str, str]) -> int:
    status = _run(["docker", "compose", "-p", project, "up", "-d"], compose_env)
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
        "CUBRID_TEST_DOCKER_CONTAINER": _compose_container(project),
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
    parser.add_argument(
        "--project", default="", help="Compose project name (default: run-unique pycubrid-it-*)"
    )
    parser.add_argument("--results", required=True, help="JUnit XML path")
    args = parser.parse_args(argv)

    project = args.project
    if project and not PROJECT_PATTERN.fullmatch(project):
        print(
            f"ERROR: INTEGRATION_PROJECT must match ^[a-z0-9][a-z0-9_-]*$ (got '{project}'); "
            "nothing was started.",
            file=sys.stderr,
            flush=True,
        )
        return 2
    project = project or f"pycubrid-it-{time.strftime('%Y%m%d%H%M%S')}-{os.getpid()}"
    lock = _reserve(project)
    if lock is None:
        print(
            f"ERROR: another run owns project '{project}' (lock held); nothing was started "
            "or removed.",
            file=sys.stderr,
            flush=True,
        )
        return 1
    try:
        return _run_owned(args, project)
    finally:
        if lock >= 0:
            os.close(lock)  # releases the per-project lock after cleanup


def _run_owned(args: argparse.Namespace, project: str) -> int:
    """Run the lane in the reserved, run-owned project and always clean it up."""
    if not _owns_nothing(project):
        print(
            f"ERROR: Refusing to run: Compose project '{project}' already has containers, "
            "volumes or networks.\nmake integration only starts and removes a project it "
            "creates; nothing was started or removed.",
            file=sys.stderr,
            flush=True,
        )
        return 1
    print(f"Using run-owned Compose project '{project}' on host port {args.port}", flush=True)
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
            status = _lane(args, project, compose_env)
        finally:
            _signals["armed"] = False
    except Interrupted:
        status = INTERRUPTED
    if _signals["received"]:
        print(f"Interrupted by {_signals['received']}; cleaning up", file=sys.stderr, flush=True)
        status = INTERRUPTED

    cleanup = subprocess.run(  # nosec B603 B607 - fixed docker compose argv
        ["docker", "compose", "-p", project, "down", "-v"], cwd=ROOT, env=compose_env, check=False
    ).returncode
    if cleanup:
        print(f"ERROR: Docker cleanup failed with status {cleanup}", file=sys.stderr, flush=True)
        status = status or cleanup
    return status


if __name__ == "__main__":
    raise SystemExit(main())
