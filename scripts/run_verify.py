#!/usr/bin/env python3
"""Run ``make verify``'s phases and always write its summary (#1053).

The ``verify`` Make recipe is a single ``exec`` of this script, so the SIGTERM that make
passes on to its recipe (``timeout make verify``, ``kill <make pid>``) reaches it directly.
It runs ``make check``, ``make test`` and ``make integration`` one after another and stops
at the first failing phase; the phases after it are recorded as ``NOT_RUN``. The phases are
not Make prerequisites, so even ``make -j`` never starts a later phase early.

The phases stay in the caller's process group, so a terminal keeps them in the foreground
and Ctrl-C or a hangup reaches them as it would without this runner. The first SIGINT,
SIGTERM or SIGHUP is also forwarded, as SIGTERM, to the running phase's make: GNU make
passes only SIGTERM on to its recipe, which for ``make integration`` is an ``exec`` of
``scripts/run_integration.py``, so the live lane still removes its Compose project. The
runner waits for that phase to finish, starts no further phase, writes the summary and
exits 128 + the signal number. Later signals are only recorded. A signal already ignored
when the run starts stays ignored.

Afterwards, whatever happened, ``scripts/report_results.py``'s ``aggregate`` writes
``<results-dir>/summary.json``, ``junit/<lane>.xml`` for the lanes that ran and the live
lane's ``environment.json``, with the configured passwords masked. The exit status is that
of the first failing phase, 128 + N after signal N, or 1 when every phase exited 0 but the
summary is not ``PASS``.

Usage (see the Makefile)::

    python3 scripts/run_verify.py --make make --results-dir test-results/verify \\
        --test-results test-results/test \\
        --integration-junit test-results/integration/junit.xml
"""

from __future__ import annotations

import argparse
import importlib.util
import shutil
import signal
import subprocess
import sys
from collections.abc import Sequence
from pathlib import Path
from types import FrameType, ModuleType

ROOT = Path(__file__).resolve().parents[1]
PHASES = ("check", "test", "integration")
# SIGHUP does not exist on Windows.
HANDLED_SIGNALS = tuple(
    getattr(signal, name) for name in ("SIGINT", "SIGTERM", "SIGHUP") if hasattr(signal, name)
)


class _State:
    # The first handled signal; it decides the exit status.
    received: signal.Signals | None = None
    # The running phase's make, which the signal is forwarded to.
    active: subprocess.Popen[bytes] | None = None


def _forward() -> None:
    if _State.active is not None and _State.active.poll() is None:
        _State.active.send_signal(signal.SIGTERM)


def _on_signal(signum: int, frame: FrameType | None) -> None:
    if _State.received is not None:
        return
    _State.received = signal.Signals(signum)
    print(f"Received {_State.received.name}; stopping make verify", file=sys.stderr, flush=True)
    _forward()


def _run_phase(make: str, phase: str) -> int:
    """Run ``make <phase>``; return a shell-style status (128 + N when killed by N)."""
    try:
        process = subprocess.Popen(  # nosec B603 - argv from the Makefile, no shell
            [make, "--no-print-directory", phase]
        )
    except OSError as exc:
        print(f"{make}: {exc.strerror or exc}", file=sys.stderr, flush=True)
        return 127 if isinstance(exc, FileNotFoundError) else 126
    _State.active = process
    try:
        # A signal that arrived while the phase was starting is forwarded now.
        if _State.received is not None:
            _forward()
        returncode = process.wait()
    finally:
        _State.active = None
    return 128 - returncode if returncode < 0 else returncode


def _reporter() -> ModuleType:
    """Import scripts/report_results.py by path, so it works as a script and as a package."""
    spec = importlib.util.spec_from_file_location(
        "pycubrid_verify_reporter", ROOT / "scripts" / "report_results.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _clear(results: Path) -> None:
    """Remove the previous run's files, so a stale PASS cannot outlive this run."""
    for name in ("summary.json", "environment.json"):
        (results / name).unlink(missing_ok=True)
    shutil.rmtree(results / "junit", ignore_errors=True)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").split("\n\n", 1)[0])
    parser.add_argument("--make", required=True, help="the make program to run each phase")
    parser.add_argument("--results-dir", required=True, type=Path, help="verify results")
    parser.add_argument("--test-results", required=True, type=Path, help="make test results")
    parser.add_argument(
        "--integration-junit", required=True, type=Path, help="make integration JUnit file"
    )
    args = parser.parse_args(argv)

    for signum in HANDLED_SIGNALS:
        if signal.getsignal(signum) is not signal.SIG_IGN:
            signal.signal(signum, _on_signal)
    _clear(args.results_dir)

    status = 0
    exits: dict[str, int | None] = dict.fromkeys(PHASES)
    for phase in PHASES:
        if status or _State.received is not None:
            break
        exits[phase] = status = _run_phase(args.make, phase)
    if _State.received is not None:
        status = 128 + _State.received

    junits = {
        "test": args.test_results / "junit.xml",
        "integration": args.integration_junit,
    }
    phases = []
    copies = []
    for phase in PHASES:
        code = exits[phase]
        summary = junits[phase].parent / "summary.json" if phase in junits else None
        phases.append((phase, code, summary))
        # Only a lane that ran this time contributes its JUnit file.
        if phase in junits and code is not None:
            copies.append((phase, junits[phase]))
    environment = None
    if exits["integration"] is not None:
        environment = args.integration_junit.parent / "environment.json"
    try:
        summary_status = _reporter().aggregate(args.results_dir, phases, copies, environment)[
            "status"
        ]
    except Exception as exc:  # noqa: BLE001 - reporting must never hide the phase status
        print(f"ERROR: could not write the verify summary: {exc}", file=sys.stderr, flush=True)
        summary_status = "INCOMPLETE"
    # A signal that arrived while the summary was written still decides the exit status.
    if _State.received is not None:
        status = status or 128 + _State.received
    return status or (0 if summary_status == "PASS" else 1)


if __name__ == "__main__":
    raise SystemExit(main())
