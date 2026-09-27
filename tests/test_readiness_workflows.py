"""Offline contract checks for the integration readiness gate."""

from __future__ import annotations

import runpy
import os
import shlex
import shutil
import subprocess
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

import pycubrid

ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize("workflow", ["ci.yml", "integration-full.yml"])
def test_integration_workflow_uses_fail_closed_probe(workflow: str) -> None:
    content = (ROOT / ".github" / "workflows" / workflow).read_text()
    readiness = content.split("- name: Wait for CUBRID readiness\n", 1)[1]
    step = readiness.split("      - name:", 1)[0]
    assert step.strip() == "run: python scripts/wait_for_cubrid.py"


def test_readiness_probe_uses_connection_fields_and_stops_on_success(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    main = runpy.run_path(str(ROOT / "scripts" / "wait_for_cubrid.py"))["main"]
    for key, value in {
        "CUBRID_TEST_HOST": "test-broker",
        "CUBRID_TEST_PORT": "33001",
        "CUBRID_TEST_DB": "probe_db",
        "CUBRID_TEST_USER": "probe_user",
        "CUBRID_TEST_PASSWORD": "probe_password",
    }.items():
        monkeypatch.setenv(key, value)
    conn = MagicMock()
    connect = MagicMock(return_value=conn)
    sleep = MagicMock()
    monkeypatch.setattr(pycubrid, "connect", connect)
    monkeypatch.setattr("time.sleep", sleep)

    assert main(["wait_for_cubrid.py", "3", "0"]) == 0

    connect.assert_called_once_with(
        host="test-broker",
        port=33001,
        database="probe_db",
        user="probe_user",
        password="probe_password",
        connect_timeout=5.0,
        read_timeout=5.0,
    )
    conn.cursor.return_value.execute.assert_called_once_with("SELECT 1")
    conn.cursor.return_value.close.assert_called_once_with()
    conn.close.assert_called_once_with()
    sleep.assert_not_called()


def test_readiness_probe_returns_failure_after_exhausted_retries(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    main = runpy.run_path(str(ROOT / "scripts" / "wait_for_cubrid.py"))["main"]
    connect = MagicMock(side_effect=pycubrid.OperationalError("broker unavailable"))
    sleep = MagicMock()
    monkeypatch.setattr(pycubrid, "connect", connect)
    monkeypatch.setattr("time.sleep", sleep)

    assert main(["wait_for_cubrid.py", "2", "0"]) == 1
    assert connect.call_count == 2
    assert sleep.call_count == 2
    assert "never became ready after 2 attempts" in capsys.readouterr().err


def test_readiness_probe_retries_until_delayed_success(monkeypatch: pytest.MonkeyPatch) -> None:
    main = runpy.run_path(str(ROOT / "scripts" / "wait_for_cubrid.py"))["main"]
    conn = MagicMock()
    connect = MagicMock(side_effect=[OSError("starting"), OSError("starting"), conn])
    sleep = MagicMock()
    monkeypatch.setattr(pycubrid, "connect", connect)
    monkeypatch.setattr("time.sleep", sleep)
    monkeypatch.setenv("CUBRID_TEST_CONNECT_TIMEOUT", "1.5")
    monkeypatch.setenv("CUBRID_TEST_READ_TIMEOUT", "2.5")

    assert main(["wait_for_cubrid.py", "5", "0.25"]) == 0
    assert connect.call_count == 3
    assert sleep.call_count == 2
    sleep.assert_called_with(0.25)
    assert connect.call_args.kwargs["connect_timeout"] == 1.5
    assert connect.call_args.kwargs["read_timeout"] == 2.5
    conn.cursor.return_value.execute.assert_called_once_with("SELECT 1")
    conn.cursor.return_value.close.assert_called_once_with()
    conn.close.assert_called_once_with()


def test_connected_broker_with_failed_select_is_not_ready_and_resources_close(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    main = runpy.run_path(str(ROOT / "scripts" / "wait_for_cubrid.py"))["main"]
    connections = [MagicMock(), MagicMock()]
    for conn in connections:
        conn.cursor.return_value.execute.side_effect = pycubrid.OperationalError("SELECT failed")
    monkeypatch.setattr(pycubrid, "connect", MagicMock(side_effect=connections))
    monkeypatch.setattr("time.sleep", MagicMock())

    assert main(["wait_for_cubrid.py", "2", "0"]) == 1
    for conn in connections:
        conn.cursor.return_value.close.assert_called_once_with()
        conn.close.assert_called_once_with()
    captured = capsys.readouterr()
    assert "CUBRID ready" not in captured.out
    assert "never became ready after 2 attempts" in captured.err


def test_cursor_close_failure_still_closes_connection(monkeypatch: pytest.MonkeyPatch) -> None:
    main = runpy.run_path(str(ROOT / "scripts" / "wait_for_cubrid.py"))["main"]
    conn = MagicMock()
    conn.cursor.return_value.close.side_effect = OSError("close failed")
    monkeypatch.setattr(pycubrid, "connect", MagicMock(return_value=conn))
    monkeypatch.setattr("time.sleep", MagicMock())

    assert main(["wait_for_cubrid.py", "1", "0"]) == 1
    conn.close.assert_called_once_with()


@pytest.mark.parametrize("workflow", ["ci.yml", "integration-full.yml"])
def test_workflow_shell_stops_after_probe_cli_failure(workflow: str, tmp_path: Path) -> None:
    if shutil.which("bash") is None:
        pytest.skip("GitHub workflow shell requires bash")
    content = (ROOT / ".github" / "workflows" / workflow).read_text()
    step = content.split("- name: Wait for CUBRID readiness\n", 1)[1].split("      - name:", 1)[0]
    command = step.strip().removeprefix("run: ")
    command = command.replace("python ", shlex.quote(sys.executable) + " ", 1)
    (tmp_path / "pycubrid.py").write_text(
        "import time\ntime.sleep = lambda seconds: None\n"
        "def connect(**kwargs):\n    raise RuntimeError('broker unavailable')\n"
    )
    result = subprocess.run(
        [
            "bash",
            "--noprofile",
            "--norc",
            "-eo",
            "pipefail",
            "-c",
            command + "\nprintf 'DOWNSTREAM_TESTS_STARTED\\n'",
        ],
        cwd=ROOT,
        env={**os.environ, "PYTHONPATH": str(tmp_path)},
        text=True,
        capture_output=True,
        timeout=10,
    )
    assert result.returncode == 1
    assert "never became ready" in result.stderr
    assert "DOWNSTREAM_TESTS_STARTED" not in result.stdout
