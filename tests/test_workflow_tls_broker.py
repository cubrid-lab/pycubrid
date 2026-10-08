"""The TLS lanes' broker restart survives a transient stop failure (#767).

The SSL=ON setup script runs inside ``docker exec ... bash -lc "..."``. It is
extracted from each workflow and executed against a stub ``cubrid`` command so
the retry contract is exercised without Docker or a CUBRID server.
"""

from __future__ import annotations

import re
import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
pytestmark = pytest.mark.repo_tooling

WORKFLOWS = ("ci.yml", "integration-full.yml")

# Stub `cubrid`: broker1 keeps running until `broker stop` has been called
# STOP_FAILS+1 times (or forever when STOP_FAILS is "never"); the database
# server always reports itself up so the rest of the script proceeds.
STUB = r"""#!/usr/bin/env bash
state="$STATE_DIR"
case "$1 $2" in
  "broker stop")
    n=$(( $(cat "$state/stops" 2>/dev/null || echo 0) + 1 )); echo "$n" > "$state/stops"
    if [ "$STOP_FAILS" = never ] || [ "$n" -le "$STOP_FAILS" ]; then
      echo "Cannot inactivate broker [broker1]"; exit 1
    fi
    rm -f "$state/running"; echo "broker stopped" ;;
  "broker start") touch "$state/running"; touch "$state/started" ;;
  "broker status") [ -e "$state/running" ] && echo "% broker1" || echo "not running" ;;
  "server status") echo "Server testdb (rel 11.4)" ;;
  "server start") : ;;
esac
"""


def _setup_script(wf: str) -> str:
    jobs = yaml.safe_load((ROOT / ".github/workflows" / wf).read_text())["jobs"]
    run = next(
        s["run"]
        for job in jobs.values()
        for s in job.get("steps", [])
        if "cubrid broker stop" in s.get("run", "")
    )
    body = re.search(r'bash -lc "\n(.*?)\n\s*"\n', run, re.S)
    assert body, f"{wf}: SSL=ON docker exec script not found"
    # Undo the escaping the outer double-quoted string applies.
    return body.group(1).replace("\\$", "$").replace('\\"', '"')


def _run(wf: str, tmp_path: Path, stop_fails: str) -> subprocess.CompletedProcess:
    if shutil.which("bash") is None:
        pytest.skip("workflow shell requires bash")
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    stub = bin_dir / "cubrid"
    stub.write_text(STUB)
    stub.chmod(0o755)
    (tmp_path / "running").touch()
    conf = tmp_path / "conf"
    conf.mkdir()
    (conf / "cubrid_broker.conf").write_text("[%BROKER1]\nSERVICE=ON\nSSL=OFF\n")
    script = _setup_script(wf).replace("sleep 3", "sleep 0")
    env = {
        "PATH": f"{bin_dir}:/usr/bin:/bin",
        "CUBRID": str(tmp_path),
        "STATE_DIR": str(tmp_path),
        "STOP_FAILS": stop_fails,
    }
    return subprocess.run(
        ["bash", "-c", script], env=env, text=True, capture_output=True, timeout=30
    )


@pytest.mark.parametrize("wf", WORKFLOWS)
def test_transient_stop_failure_is_retried(wf: str, tmp_path: Path) -> None:
    done = _run(wf, tmp_path, stop_fails="1")
    assert done.returncode == 0, done.stdout + done.stderr
    assert (tmp_path / "started").exists()
    assert (tmp_path / "conf/cubrid_broker.conf").read_text().count("SSL=ON") == 1


@pytest.mark.parametrize("wf", WORKFLOWS)
def test_persistent_stop_failure_fails_loudly(wf: str, tmp_path: Path) -> None:
    done = _run(wf, tmp_path, stop_fails="never")
    assert done.returncode != 0
    assert "broker1 did not stop" in done.stdout
    assert not (tmp_path / "started").exists()


@pytest.mark.parametrize("wf", WORKFLOWS)
def test_clean_restart_succeeds(wf: str, tmp_path: Path) -> None:
    done = _run(wf, tmp_path, stop_fails="0")
    assert done.returncode == 0, done.stdout + done.stderr
    assert (tmp_path / "started").exists()
