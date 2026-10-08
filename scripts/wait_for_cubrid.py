#!/usr/bin/env python3
"""Wait for a live CUBRID broker, failing after the retry budget (issue #354/#358).

Used by the regular, full, and nightly bug-hunt workflows. The previous inline shell loop `break`-ed
on success but exited 0 even after all probes failed, letting the integration
suites skip silently and the job appear green without testing anything. This
exits non-zero when the broker never becomes ready, so an unavailable service is
a clear infrastructure failure. ``make integration`` uses it too (#522).

Environment: the endpoint is resolved exactly as the test suite resolves it
(``tests/_cubrid_endpoint.py``): per-field ``CUBRID_TEST_HOST`` / ``CUBRID_TEST_PORT`` /
``CUBRID_TEST_DB`` / ``CUBRID_TEST_USER`` / ``CUBRID_TEST_PASSWORD`` win, then the
matching component of ``CUBRID_TEST_URL``, then localhost:33000/testdb/dba/"".
``CUBRID_TEST_CONNECT_TIMEOUT`` / ``CUBRID_TEST_READ_TIMEOUT`` default to 5 seconds,
so a broker that accepts TCP but stalls cannot leave a probe waiting indefinitely.

Usage:
    python scripts/wait_for_cubrid.py [attempts] [sleep_seconds] [--server-info PATH]

Exit codes:
    0 - broker reachable
    1 - broker never became ready within the retry budget
"""

from __future__ import annotations

import importlib.util
import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

# Share the suite's endpoint resolution and probe so readiness and the tests can
# never disagree about which broker is under test. Loaded by path so neither the
# repository root nor a foreign ``tests`` package shadows anything on sys.path.
_SPEC = importlib.util.spec_from_file_location(
    "_pycubrid_wait_for_cubrid_endpoint",
    Path(__file__).resolve().parents[1] / "tests" / "_cubrid_endpoint.py",
)
assert _SPEC is not None and _SPEC.loader is not None
_endpoint = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = _endpoint  # dataclasses resolve annotations via sys.modules
_SPEC.loader.exec_module(_endpoint)

_REPRO_SPEC = importlib.util.spec_from_file_location(
    "_pycubrid_wait_repro", Path(__file__).with_name("collect_repro.py")
)
assert _REPRO_SPEC is not None and _REPRO_SPEC.loader is not None
_repro = importlib.util.module_from_spec(_REPRO_SPEC)
_REPRO_SPEC.loader.exec_module(_repro)


def _write_identity(path: Path, endpoint: str, result: dict[str, object]) -> None:
    info = {
        **result,
        "endpoint": endpoint,
        "github_sha": os.environ.get("GITHUB_SHA", ""),
        "observed_at": datetime.now(timezone.utc).isoformat(),
    }
    sanitized = {key: _repro.sanitize(str(value)) for key, value in info.items()}
    if any(
        sanitized.get(key, "") != str(info.get(key, ""))
        for key in ("version", "endpoint", "github_sha")
    ):
        sanitized.update(status="unavailable", reason="server identity requires redaction")
        sanitized.pop("version", None)
    try:
        path.write_text(json.dumps(sanitized, indent=2) + "\n", encoding="utf-8")
    except OSError as exc:
        print(_repro.sanitize(f"Server identity unavailable: {exc}"), file=sys.stderr)


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("attempts", nargs="?", type=int, default=30)
    parser.add_argument("sleep_seconds", nargs="?", type=float, default=5.0)
    parser.add_argument("--server-info", type=Path)
    args = parser.parse_args(argv[1:])
    attempts = args.attempts
    sleep_s = args.sleep_seconds
    if args.server_info is not None:
        try:
            args.server_info.unlink(missing_ok=True)
        except OSError as exc:
            print(
                _repro.sanitize(f"Stale server identity could not be removed: {exc}"),
                file=sys.stderr,
            )
    endpoint = _endpoint.resolve_endpoint()
    result: dict[str, object] = {"status": "unavailable", "reason": "readiness not completed"}
    if args.server_info is not None:
        _write_identity(args.server_info, endpoint.describe(), result)

    for i in range(1, attempts + 1):
        try:
            if args.server_info is None:
                _endpoint.probe(endpoint)
            else:
                _endpoint.probe(endpoint, result=result)
        except Exception as exc:  # noqa: BLE001 - readiness probe reports any failure
            result.clear()
            result.update(status="unavailable", reason=f"readiness failed: {exc}")
            if args.server_info is not None:
                _write_identity(args.server_info, endpoint.describe(), result)
            print(f"[{i}/{attempts}] CUBRID {endpoint.describe()} not ready: {exc}")
            time.sleep(sleep_s)
            continue
        print(f"CUBRID {endpoint.describe()} ready")
        if args.server_info is not None:
            _write_identity(args.server_info, endpoint.describe(), result)
        return 0

    print(f"CUBRID never became ready after {attempts} attempts", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
