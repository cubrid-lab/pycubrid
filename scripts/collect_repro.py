#!/usr/bin/env python3
"""Collect a minimal reproduction bundle after a bug-hunt failure (issue #359).

Randomized (Hypothesis) and live suites are only useful if every failure can be
reproduced. This script gathers, into ``bug-hunt-repro/``:

    metadata.json  - Python/CUBRID/driver versions, profile, sync/async, env
    reproduce.md   - one-command replay instructions
    hypothesis/    - a copy of the Hypothesis example database (.hypothesis/)

The nightly bug-hunt workflow runs this on failure and uploads the directory as
a build artifact, so a maintainer can replay the exact failing example locally.

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

REPRO_DIR = Path("bug-hunt-repro")
HYPOTHESIS_DB = Path(".hypothesis")


def _driver_version() -> str:
    try:
        import pycubrid

        return getattr(pycubrid, "__version__", "unknown")
    except Exception:  # noqa: BLE001 - best-effort collector, never abort
        return "unknown"


def _metadata() -> dict[str, str]:
    return {
        "python_version": sys.version.split()[0],
        "platform": platform.platform(),
        "pycubrid_version": _driver_version(),
        "hypothesis_profile": os.environ.get("HYPOTHESIS_PROFILE", "pr"),
        "cubrid_test_host": os.environ.get("CUBRID_TEST_HOST", ""),
        "cubrid_test_port": os.environ.get("CUBRID_TEST_PORT", ""),
        "cubrid_test_db": os.environ.get("CUBRID_TEST_DB", ""),
        "cubrid_test_url": os.environ.get("CUBRID_TEST_URL", ""),
        "github_run_id": os.environ.get("GITHUB_RUN_ID", ""),
        "github_sha": os.environ.get("GITHUB_SHA", ""),
    }


_REPRODUCE_MD = """# Bug-hunt reproduction

A bug-hunt suite failed. To replay the exact failing Hypothesis example locally:

```bash
# 1. Restore the Hypothesis example database that captured the failure.
cp -r bug-hunt-repro/hypothesis .hypothesis

# 2. Re-run the failing suite. Hypothesis replays the saved example first.
#    Set the same profile the failure used (see metadata.json -> hypothesis_profile).
HYPOTHESIS_PROFILE={profile} \\
CUBRID_TEST_HOST={host} CUBRID_TEST_PORT={port} CUBRID_TEST_DB={db} \\
  python -m pytest tests/ -m integration -p no:cacheprovider
```

For a non-Hypothesis (live-only) failure, the failing test id is in the CI log;
run it directly with the same `CUBRID_TEST_*` environment.

See `metadata.json` for the full environment. When a real defect is confirmed,
follow the Bug Discovery -> Regression Workflow in `CONTRIBUTING.md`.
"""


def main() -> int:
    REPRO_DIR.mkdir(exist_ok=True)
    meta = _metadata()

    (REPRO_DIR / "metadata.json").write_text(json.dumps(meta, indent=2) + "\n")
    (REPRO_DIR / "reproduce.md").write_text(
        _REPRODUCE_MD.format(
            profile=meta["hypothesis_profile"] or "pr",
            host=meta["cubrid_test_host"] or "localhost",
            port=meta["cubrid_test_port"] or "33000",
            db=meta["cubrid_test_db"] or "testdb",
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
