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
    python scripts/wait_for_cubrid.py [attempts] [sleep_seconds]

Exit codes:
    0 - broker reachable
    1 - broker never became ready within the retry budget
"""

from __future__ import annotations

import importlib.util
import sys
import time
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


def main(argv: list[str]) -> int:
    attempts = int(argv[1]) if len(argv) > 1 else 30
    sleep_s = float(argv[2]) if len(argv) > 2 else 5.0
    endpoint = _endpoint.resolve_endpoint()

    for i in range(1, attempts + 1):
        try:
            _endpoint.probe(endpoint)
        except Exception as exc:  # noqa: BLE001 - readiness probe reports any failure
            print(f"[{i}/{attempts}] CUBRID {endpoint.describe()} not ready: {exc}")
            time.sleep(sleep_s)
            continue
        print(f"CUBRID {endpoint.describe()} ready")
        return 0

    print(f"CUBRID never became ready after {attempts} attempts", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
