#!/usr/bin/env python3
"""Collect a minimal reproduction bundle after a bug-hunt failure (issue #359).

Randomized (Hypothesis) and live suites are only useful if every failure can be
reproduced. This script gathers, into ``bug-hunt-repro/``:

    metadata.json  - Python/CUBRID/driver versions, profile, env, failure info
    reproduce.md   - one-command replay instructions
    hypothesis/    - a copy of the Hypothesis example database (.hypothesis/)

The nightly bug-hunt workflow runs this on failure and uploads the directory as
a build artifact, so a maintainer can replay the exact failing example locally.

Security: any password embedded in ``CUBRID_TEST_URL`` (``user:password@host``)
is redacted before being written to the artifact, so a repro bundle never
discloses the database password.

Usage:
    python scripts/collect_repro.py

Exit codes:
    0 - bundle written (always; best-effort, never fails the job further)
"""

from __future__ import annotations

import json
import os
import platform
import shutil
import sys
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

REPRO_DIR = Path("bug-hunt-repro")
HYPOTHESIS_DB = Path(".hypothesis")


def _driver_version() -> str:
    try:
        import pycubrid

        return getattr(pycubrid, "__version__", "unknown")
    except Exception:  # noqa: BLE001 - best-effort collector, never abort
        return "unknown"


def _redact_url(url: str) -> str:
    """Strip any embedded password from a connection URL before persisting it."""
    if not url:
        return ""
    try:
        parts = urlsplit(url)
    except ValueError:
        return "<unparseable-url-redacted>"
    if parts.password is None:
        return url
    host = parts.hostname or ""
    if parts.port:
        host = f"{host}:{parts.port}"
    userinfo = f"{parts.username}:***@" if parts.username else "***@"
    return urlunsplit((parts.scheme, userinfo + host, parts.path, parts.query, parts.fragment))


def _metadata() -> dict[str, str]:
    # Failure context, when the caller exports it (the pytest run can set these
    # from a failure hook); otherwise the CI log holds the failing test id.
    return {
        "python_version": sys.version.split()[0],
        "platform": platform.platform(),
        "pycubrid_version": _driver_version(),
        "hypothesis_profile": os.environ.get("HYPOTHESIS_PROFILE", "pr"),
        "cubrid_test_host": os.environ.get("CUBRID_TEST_HOST", ""),
        "cubrid_test_port": os.environ.get("CUBRID_TEST_PORT", ""),
        "cubrid_test_db": os.environ.get("CUBRID_TEST_DB", ""),
        "cubrid_test_user": os.environ.get("CUBRID_TEST_USER", ""),
        "cubrid_test_url": _redact_url(os.environ.get("CUBRID_TEST_URL", "")),
        "failing_test_id": os.environ.get("BUG_HUNT_FAILING_TEST", ""),
        "failure_traceback": os.environ.get("BUG_HUNT_FAILURE_TRACEBACK", ""),
        "github_run_id": os.environ.get("GITHUB_RUN_ID", ""),
        "github_sha": os.environ.get("GITHUB_SHA", ""),
    }


_REPRODUCE_MD = """# Bug-hunt reproduction

A bug-hunt suite failed. To replay the exact failing Hypothesis example locally:

```bash
# 1. Restore the Hypothesis example database that captured the failure.
#    Replace the destination so it lands at .hypothesis (not .hypothesis/hypothesis).
rm -rf .hypothesis && cp -r bug-hunt-repro/hypothesis .hypothesis

# 2. Re-run the failing suite. Hypothesis replays the saved example first.
#    Set the same profile the failure used (see metadata.json -> hypothesis_profile).
#    The password is NOT stored in this bundle; export CUBRID_TEST_PASSWORD yourself.
HYPOTHESIS_PROFILE={profile} \\
CUBRID_TEST_HOST={host} CUBRID_TEST_PORT={port} CUBRID_TEST_DB={db} \\
CUBRID_TEST_USER={user} CUBRID_TEST_PASSWORD=... \\
  python -m pytest {target} -p no:cacheprovider
```

For a live-only failure, `metadata.json -> failing_test_id` (or the CI log) has
the failing test id; run it directly with the same `CUBRID_TEST_*` environment.

See `metadata.json` for the full environment. When a real defect is confirmed,
follow the Bug Discovery -> Regression Workflow in `CONTRIBUTING.md`.
"""


def main() -> int:
    REPRO_DIR.mkdir(exist_ok=True)
    meta = _metadata()

    (REPRO_DIR / "metadata.json").write_text(json.dumps(meta, indent=2) + "\n")
    target = meta["failing_test_id"] or "tests/ -m integration"
    (REPRO_DIR / "reproduce.md").write_text(
        _REPRODUCE_MD.format(
            profile=meta["hypothesis_profile"] or "pr",
            host=meta["cubrid_test_host"] or "localhost",
            port=meta["cubrid_test_port"] or "33000",
            db=meta["cubrid_test_db"] or "testdb",
            user=meta["cubrid_test_user"] or "dba",
            target=target,
        )
    )

    if HYPOTHESIS_DB.is_dir():
        dest = REPRO_DIR / "hypothesis"
        if dest.exists():
            shutil.rmtree(dest)
        shutil.copytree(HYPOTHESIS_DB, dest)
        print(f"Copied {HYPOTHESIS_DB} -> {dest}")
    else:
        print("No .hypothesis/ database found (live-only failure); metadata still written.")

    print(f"Reproduction bundle written to {REPRO_DIR}/")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
