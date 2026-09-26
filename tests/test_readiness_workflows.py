"""Offline contract checks for the integration readiness gate."""

from __future__ import annotations

import runpy
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
