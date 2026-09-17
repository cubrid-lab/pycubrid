#!/usr/bin/env python3
"""Wait for a live CUBRID broker, failing after the retry budget (issue #354/#358).

Used by the nightly bug-hunt workflow. The previous inline shell loop `break`-ed
on success but exited 0 even after all probes failed, letting the integration
suites (skipif-gated on ``can_connect``) skip silently and the job appear green
without testing anything. This exits non-zero when the broker never becomes
ready, so an unavailable service is a clear infrastructure failure.

Environment: ``CUBRID_TEST_HOST`` / ``CUBRID_TEST_PORT`` / ``CUBRID_TEST_DB`` /
``CUBRID_TEST_USER`` / ``CUBRID_TEST_PASSWORD`` (defaults localhost:33000/testdb/dba/"").

Usage:
    python scripts/wait_for_cubrid.py [attempts] [sleep_seconds]

Exit codes:
    0 - broker reachable
    1 - broker never became ready within the retry budget
"""

from __future__ import annotations

import os
import sys
import time

import pycubrid


def main(argv: list[str]) -> int:
    attempts = int(argv[1]) if len(argv) > 1 else 30
    sleep_s = float(argv[2]) if len(argv) > 2 else 5.0
    host = os.environ.get("CUBRID_TEST_HOST", "localhost")
    port = int(os.environ.get("CUBRID_TEST_PORT", "33000"))
    database = os.environ.get("CUBRID_TEST_DB", "testdb")
    user = os.environ.get("CUBRID_TEST_USER", "dba")
    password = os.environ.get("CUBRID_TEST_PASSWORD", "")

    for i in range(1, attempts + 1):
        try:
            conn = pycubrid.connect(
                host=host, port=port, database=database, user=user, password=password
            )
            cur = conn.cursor()
            cur.execute("SELECT 1")
            cur.close()
            conn.close()
        except Exception as exc:  # noqa: BLE001 - readiness probe reports any failure
            print(f"[{i}/{attempts}] CUBRID not ready: {exc}")
            time.sleep(sleep_s)
            continue
        print("CUBRID ready")
        return 0

    print(f"CUBRID never became ready after {attempts} attempts", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
