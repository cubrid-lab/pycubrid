"""Offline contract checks for the integration readiness gate."""

from __future__ import annotations

import runpy
import json
import os
import shlex
import shutil
import subprocess
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

import pycubrid
from pycubrid.protocol import GetEngineVersionPacket

pytestmark = pytest.mark.repo_tooling

ROOT = Path(__file__).resolve().parents[1]


def test_readiness_helper_change_selects_required_code_lanes() -> None:
    import yaml

    workflow = yaml.safe_load((ROOT / ".github/workflows/ci.yml").read_text())
    jobs = workflow["jobs"]
    filters = yaml.safe_load(jobs["detect-changes"]["steps"][-1]["with"]["filters"])
    assert "scripts/**" in filters["risk"]
    assert "scripts/**" in filters["official"]
    assert "outputs.live" in jobs["integration-tests"]["if"]
    assert "outputs.official" in jobs["official-differential"]["if"]


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


@pytest.mark.parametrize("metadata_failure", [False, True])
def test_readiness_sidecar_records_fresh_identity_without_extra_connect(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, metadata_failure: bool
) -> None:
    main = runpy.run_path(str(ROOT / "scripts" / "wait_for_cubrid.py"))["main"]
    monkeypatch.setenv("GITHUB_SHA", "current-sha")
    monkeypatch.setenv("CUBRID_TEST_HOST", "identity-broker")
    monkeypatch.setenv("CUBRID_TEST_PORT", "33001")
    monkeypatch.setenv("CUBRID_TEST_DB", "identity_db")
    monkeypatch.setenv("CUBRID_TEST_USER", "identity_user")
    monkeypatch.setenv("CUBRID_TEST_PASSWORD", "private-password")
    sidecar = tmp_path / "attempt-2-server.json"
    sidecar.write_text(json.dumps({"status": "observed", "version": "stale-version"}))
    conn = MagicMock()
    conn._physical_generation = 9
    conn.autocommit = False

    def version(packet: GetEngineVersionPacket, **kwargs: object) -> None:
        assert kwargs == {"allow_reconnect": False, "expected_generation": 9}
        if metadata_failure:
            raise OSError("private-password version unavailable")
        packet.engine_version = "10.2.18.9024"

    conn._send_and_receive.side_effect = version
    connect = MagicMock(return_value=conn)
    monkeypatch.setattr(pycubrid, "connect", connect)
    sleep = MagicMock()
    monkeypatch.setattr("time.sleep", sleep)
    result = main(["wait_for_cubrid.py", "2", "0", "--server-info", str(sidecar)])
    assert result == 0
    info = json.loads(sidecar.read_text())
    assert info["github_sha"] == "current-sha"
    assert info["endpoint"] == "identity_user@identity-broker:33001/identity_db"
    assert info["observed_at"]
    assert "private-password" not in sidecar.read_text()
    if metadata_failure:
        assert info["status"] == "unavailable"
        assert info["reason"]
        assert not info.get("version")
    else:
        assert info["status"] == "observed"
        assert info["version"] == "10.2.18.9024"
    assert connect.call_count == 1
    conn._send_and_receive.assert_called_once()
    conn.get_server_version.assert_not_called()
    conn._connect.assert_not_called()
    conn.cursor.return_value.close.assert_called_once_with()
    conn.close.assert_called_once_with()
    sleep.assert_not_called()


def test_readiness_exhaustion_replaces_stale_identity_without_masking_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    main = runpy.run_path(str(ROOT / "scripts" / "wait_for_cubrid.py"))["main"]
    monkeypatch.setenv("GITHUB_SHA", "current-sha")
    monkeypatch.setenv("CUBRID_TEST_PASSWORD", "private-password")
    sidecar = tmp_path / "attempt-3-server.json"
    sidecar.write_text(json.dumps({"status": "observed", "version": "stale-version"}))
    connect = MagicMock(side_effect=OSError("private-password broker unavailable"))
    monkeypatch.setattr(pycubrid, "connect", connect)
    sleep = MagicMock()
    monkeypatch.setattr("time.sleep", sleep)
    result = main(["wait_for_cubrid.py", "2", "0", "--server-info", str(sidecar)])
    assert result == 1
    info = json.loads(sidecar.read_text())
    assert info["status"] == "unavailable"
    assert info["github_sha"] == "current-sha"
    assert not info.get("version")
    assert info["reason"]
    assert "private-password" not in sidecar.read_text()
    assert connect.call_count == 2
    assert sleep.call_count == 2
    assert "never became ready after 2 attempts" in capsys.readouterr().err


def test_sidecar_write_failure_does_not_change_readiness_success(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    main = runpy.run_path(str(ROOT / "scripts" / "wait_for_cubrid.py"))["main"]
    sidecar = tmp_path / "absent" / "server.json"
    conn = MagicMock()
    conn._physical_generation = 4
    conn.autocommit = False
    conn._send_and_receive.side_effect = OSError("version unavailable")
    connect = MagicMock(return_value=conn)
    monkeypatch.setattr(pycubrid, "connect", connect)
    result = main(["wait_for_cubrid.py", "1", "0", "--server-info", str(sidecar)])
    assert result == 0
    assert connect.call_count == 1
    assert "CUBRID" in capsys.readouterr().out
    conn.close.assert_called_once_with()


def test_property_job_wires_three_xunit1_reports_and_fresh_identity() -> None:
    import yaml

    workflow = yaml.safe_load((ROOT / ".github/workflows/bug-hunt.yml").read_text())
    job = next(
        job
        for job in workflow["jobs"].values()
        if any(
            step.get("name") == "Collect repro artifacts on failure"
            for step in job.get("steps", [])
        )
    )
    steps = {step.get("name"): step for step in job["steps"]}
    readiness = steps["Wait for CUBRID readiness"]["run"]
    collector = steps["Collect repro artifacts on failure"]
    assert collector["if"] == "failure()"
    assert "--server-info" in readiness
    assert "github.run_id" in readiness and "github.run_attempt" in readiness
    command = collector["run"]
    for report in ("normal-results.xml", "slow-results.xml", "offline-results.xml"):
        assert f"--junit {report}" in command
    assert "--server-info" in command
    pytest_steps = [
        step["run"] for step in job["steps"] if "python -m pytest" in step.get("run", "")
    ]
    assert len(pytest_steps) == 3
    assert all("junit_family=xunit1" in command for command in pytest_steps)
    assert "--junitxml=offline-results.xml" in pytest_steps[-1]
    assert all("|| true" not in command for command in pytest_steps)


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
