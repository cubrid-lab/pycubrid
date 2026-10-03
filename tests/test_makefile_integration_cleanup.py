"""Stub-based regressions for Makefile integration teardown.

These tests run the real Makefile with command stubs so they do not need
Docker or a CUBRID instance. They cover success, pytest failure, and
docker-down failure without changing integration-local.
"""

from __future__ import annotations

import os
import stat
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
MAKEFILE = ROOT / "Makefile"


def _run_make(
    tmp_path: Path,
    target: str,
    *,
    pytest_exit: int = 0,
    down_exit: int = 0,
    readiness_exit: int = 0,
) -> subprocess.CompletedProcess[str]:
    log = tmp_path / "docker.log"
    docker = tmp_path / "docker"
    docker.write_text(
        "#!/bin/sh\n"
        'printf "%s\\n" "$*" >> "$REVIEW_LOG"\n'
        'case " $* " in\n'
        '  *" compose down "*) exit "$REVIEW_DOWN_EXIT" ;;\n'
        "esac\n"
        "exit 0\n"
    )
    docker.chmod(docker.stat().st_mode | stat.S_IEXEC)

    sleep = tmp_path / "sleep"
    sleep.write_text("#!/bin/sh\nexit 0\n")
    sleep.chmod(sleep.stat().st_mode | stat.S_IEXEC)

    stub_pytest = tmp_path / "pytest-stub"
    stub_pytest.write_text('#!/bin/sh\nexit "$REVIEW_PYTEST_EXIT"\n')
    stub_pytest.chmod(stub_pytest.stat().st_mode | stat.S_IEXEC)

    stub_python = tmp_path / "python-stub"
    stub_python.write_text(
        '#!/bin/sh\ncase "$1" in\n'
        '  *wait_for_cubrid.py) exit "$REVIEW_READINESS_EXIT" ;;\n'
        "esac\nexit 0\n"
    )
    stub_python.chmod(stub_python.stat().st_mode | stat.S_IEXEC)
    env = dict(os.environ)
    env["PATH"] = str(tmp_path) + os.pathsep + env.get("PATH", "")
    env["REVIEW_LOG"] = str(log)
    env["REVIEW_PYTEST_EXIT"] = str(pytest_exit)
    env["REVIEW_DOWN_EXIT"] = str(down_exit)
    env["REVIEW_READINESS_EXIT"] = str(readiness_exit)

    result = subprocess.run(
        [
            "make",
            "-f",
            str(MAKEFILE),
            target,
            f"PYTEST={stub_pytest}",
            f"PYTHON={stub_python}",
        ],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
    )
    setattr(result, "log_text", log.read_text() if log.exists() else "")
    return result


@pytest.mark.parametrize("target", ["integration", "integration-tls"])
def test_cleanup_runs_after_pytest_failure(tmp_path: Path, target: str) -> None:
    result = _run_make(tmp_path, target, pytest_exit=1)
    assert result.returncode != 0
    assert "compose up -d" in result.log_text
    assert "compose down" in result.log_text


@pytest.mark.parametrize("target", ["integration", "integration-tls"])
def test_cleanup_runs_after_pytest_success(tmp_path: Path, target: str) -> None:
    result = _run_make(tmp_path, target, pytest_exit=0)
    assert result.returncode == 0
    assert "compose up -d" in result.log_text
    assert "compose down" in result.log_text


@pytest.mark.parametrize("target", ["integration", "integration-tls"])
def test_cleanup_failure_does_not_hide_pytest_failure(tmp_path: Path, target: str) -> None:
    result = _run_make(tmp_path, target, pytest_exit=7, down_exit=1)
    assert result.returncode != 0
    assert "compose down" in result.log_text
    assert "Docker cleanup failed" in result.stderr + result.stdout
    assert "Error 7" in result.stderr


@pytest.mark.parametrize("target", ["integration", "integration-tls"])
def test_cleanup_failure_fails_successful_run(tmp_path: Path, target: str) -> None:
    result = _run_make(tmp_path, target, pytest_exit=0, down_exit=3)
    assert result.returncode != 0
    assert "Error 3" in result.stderr
    assert "compose down" in result.log_text
    assert "Docker cleanup failed" in result.stderr + result.stdout


def test_integration_local_without_url_does_not_call_docker(tmp_path: Path) -> None:
    log = tmp_path / "docker.log"
    docker = tmp_path / "docker"
    docker.write_text('#!/bin/sh\nprintf "%s\\n" "$*" >> "$REVIEW_LOG"\nexit 0\n')
    docker.chmod(docker.stat().st_mode | stat.S_IEXEC)
    stub_pytest = tmp_path / "pytest-stub"
    stub_pytest.write_text("#!/bin/sh\nexit 0\n")
    stub_pytest.chmod(stub_pytest.stat().st_mode | stat.S_IEXEC)
    env = dict(os.environ)
    env["PATH"] = str(tmp_path) + os.pathsep + env.get("PATH", "")
    env["REVIEW_LOG"] = str(log)
    env.pop("CUBRID_TEST_URL", None)
    env.pop("CUBRID_TEST_HOST", None)
    result = subprocess.run(
        ["make", "-f", str(MAKEFILE), "integration-local", f"PYTEST={stub_pytest}"],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
    )
    assert result.returncode != 0
    assert not log.exists()


def test_integration_local_with_url_does_not_call_docker(tmp_path: Path) -> None:
    log = tmp_path / "docker.log"
    docker = tmp_path / "docker"
    docker.write_text('#!/bin/sh\nprintf "%s\\n" "$*" >> "$REVIEW_LOG"\nexit 0\n')
    docker.chmod(docker.stat().st_mode | stat.S_IEXEC)
    stub_pytest = tmp_path / "pytest-stub"
    stub_pytest.write_text("#!/bin/sh\nexit 0\n")
    stub_pytest.chmod(stub_pytest.stat().st_mode | stat.S_IEXEC)
    env = dict(os.environ)
    env["PATH"] = str(tmp_path) + os.pathsep + env.get("PATH", "")
    env["REVIEW_LOG"] = str(log)
    env["CUBRID_TEST_URL"] = "cubrid://dba@127.0.0.1:33000/testdb"
    result = subprocess.run(
        ["make", "-f", str(MAKEFILE), "integration-local", f"PYTEST={stub_pytest}"],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0
    assert not log.exists()


@pytest.mark.parametrize("target", ["integration", "integration-tls"])
def test_readiness_failure_still_preserves_volumes(tmp_path: Path, target: str) -> None:
    result = _run_make(tmp_path, target, readiness_exit=9)
    assert result.returncode != 0
    assert "compose down" in result.log_text
    assert "-v" not in result.log_text


@pytest.mark.parametrize("target", ["integration", "integration-tls"])
def test_automatic_cleanup_never_removes_reused_volumes(tmp_path: Path, target: str) -> None:
    result = _run_make(tmp_path, target)
    assert result.returncode == 0
    assert "compose down" in result.log_text
    assert "-v" not in result.log_text
