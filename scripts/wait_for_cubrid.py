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

With ``CUBRID_TEST_DOCKER_CONTAINER`` set (``make integration`` and ``make
integration-tls`` set it), a -353 "Failed to connect to database server" naming only
hosts that ``getent hosts`` cannot resolve inside that container fails at once with a
repair hint, instead of retrying: the database volume records a host that no container
will resolve again (#1000). The hosts it records are ``localhost`` or a container ID,
both answered from ``/etc/hosts``, so a failed lookup is not a transient DNS error.

Usage:
    python scripts/wait_for_cubrid.py [attempts] [sleep_seconds] [--server-info PATH]

Exit codes:
    0 - broker reachable
    1 - broker never became ready within the retry budget, or its database is
        registered for a host the container cannot resolve
"""

from __future__ import annotations

import importlib.util
import argparse
import json
import os
import re
import subprocess
import sys
import time
from datetime import UTC, datetime
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
        "observed_at": datetime.now(UTC).isoformat(),
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


# CUBRID -353 (ER_NET_CANT_CONNECT_SERVER) lists the hosts databases.txt names.
_CANT_CONNECT_SERVER = -353
_SERVER_HOSTS = re.compile(r"on the following host\(s\):(?P<hosts>.*)\Z", re.DOTALL)

_STALE_HOST_HINT = (
    "CUBRID database server host(s) {hosts} do not resolve inside container {container}. "
    "The data volume registers its database for the host of the container that created it; "
    "before #1000 that host was the creating container's ID, which a recreated container "
    "does not have. Repair the volume in place, keeping its data: make docker-fix-db-host. "
    "Or delete the volume and all its data: make docker-down."
)


def _server_hosts(exc: BaseException) -> list[str]:
    if getattr(exc, "errno", None) != _CANT_CONNECT_SERVER:
        return []
    match = _SERVER_HOSTS.search(str(getattr(exc, "msg", "")))
    if match is None:
        return []
    return [host for host in re.split(r"[\s,:]+", match.group("hosts")) if host]


def _resolves_in_container(container: str, host: str) -> bool | None:
    """True/False when ``getent hosts`` answers inside the container; None if it cannot run."""
    try:
        done = subprocess.run(
            ["docker", "exec", container, "getent", "hosts", host],
            capture_output=True,
            timeout=10,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    if done.returncode == 0:
        return True
    # getent exits 2 when the key is not found; anything else is not an answer.
    return False if done.returncode == 2 else None


def _stale_host_hint(exc: BaseException) -> str | None:
    container = os.environ.get("CUBRID_TEST_DOCKER_CONTAINER", "").strip()
    hosts = _server_hosts(exc)
    if not container or not hosts:
        return None
    if all(_resolves_in_container(container, host) is False for host in hosts):
        return _STALE_HOST_HINT.format(hosts=", ".join(hosts), container=container[:12])
    return None


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
            hint = _stale_host_hint(exc)
            if hint is not None:
                sys.stdout.flush()
                print(hint, file=sys.stderr)
                return 1
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
