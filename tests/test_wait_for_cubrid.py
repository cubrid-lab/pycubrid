"""Offline tests for the CUBRID readiness command."""

from __future__ import annotations

import json
import runpy
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Callable, cast
from unittest.mock import MagicMock, call

import pytest

pytestmark = pytest.mark.repo_tooling

ROOT = Path(__file__).resolve().parents[1]


def _load_wait_command() -> tuple[Callable[[list[str]], int], ModuleType]:
    namespace = runpy.run_path(str(ROOT / "scripts" / "wait_for_cubrid.py"))
    return cast(Callable[[list[str]], int], namespace["main"]), cast(
        ModuleType, namespace["_endpoint"]
    )


def _endpoint() -> SimpleNamespace:
    return SimpleNamespace(describe=lambda: "dba@broker:33000/testdb")


def test_first_probe_success_returns_ready_without_sleep(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    main, endpoint_helpers = _load_wait_command()
    endpoint = _endpoint()
    probe = MagicMock()
    sleep = MagicMock()
    monkeypatch.setattr(endpoint_helpers, "resolve_endpoint", MagicMock(return_value=endpoint))
    monkeypatch.setattr(endpoint_helpers, "probe", probe)
    monkeypatch.setattr("time.sleep", sleep)

    assert main(["wait_for_cubrid.py", "3", "0.25"]) == 0

    probe.assert_called_once_with(endpoint)
    sleep.assert_not_called()
    assert "CUBRID dba@broker:33000/testdb ready" in capsys.readouterr().out


def test_probe_retries_twice_before_success(monkeypatch: pytest.MonkeyPatch) -> None:
    main, endpoint_helpers = _load_wait_command()
    endpoint = _endpoint()
    probe = MagicMock(side_effect=[OSError("starting"), OSError("still starting"), None])
    sleep = MagicMock()
    monkeypatch.setattr(endpoint_helpers, "resolve_endpoint", MagicMock(return_value=endpoint))
    monkeypatch.setattr(endpoint_helpers, "probe", probe)
    monkeypatch.setattr("time.sleep", sleep)

    assert main(["wait_for_cubrid.py", "5", "0.25"]) == 0

    assert probe.call_count == 3
    assert sleep.call_args_list == [call(0.25), call(0.25)]


def test_exhausted_probes_return_failure_without_ready_message(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    main, endpoint_helpers = _load_wait_command()
    endpoint = _endpoint()
    probe = MagicMock(side_effect=OSError("broker unavailable"))
    sleep = MagicMock()
    monkeypatch.setattr(endpoint_helpers, "resolve_endpoint", MagicMock(return_value=endpoint))
    monkeypatch.setattr(endpoint_helpers, "probe", probe)
    monkeypatch.setattr("time.sleep", sleep)

    assert main(["wait_for_cubrid.py", "3", "0"]) == 1

    assert probe.call_count == 3
    captured = capsys.readouterr()
    assert "CUBRID dba@broker:33000/testdb ready\n" not in captured.out
    assert "never became ready after 3 attempts" in captured.err


def test_failed_server_info_is_unavailable_and_sanitized(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    main, endpoint_helpers = _load_wait_command()
    endpoint = _endpoint()
    secret = "readiness-test-password"
    probe = MagicMock(side_effect=OSError(f"authentication failed for {secret}"))
    sleep = MagicMock()
    sidecar = tmp_path / "server-info.json"
    monkeypatch.delenv("CUBRID_TEST_URL", raising=False)
    monkeypatch.setenv("CUBRID_TEST_PASSWORD", secret)
    monkeypatch.setattr(endpoint_helpers, "resolve_endpoint", MagicMock(return_value=endpoint))
    monkeypatch.setattr(endpoint_helpers, "probe", probe)
    monkeypatch.setattr("time.sleep", sleep)

    assert main(["wait_for_cubrid.py", "2", "0", "--server-info", str(sidecar)]) == 1

    assert probe.call_count == 2
    for probe_call in probe.call_args_list:
        assert probe_call.args == (endpoint,)
        assert probe_call.kwargs.keys() == {"result"}
    info = json.loads(sidecar.read_text(encoding="utf-8"))
    assert info["status"] == "unavailable"
    assert info["endpoint"] == "dba@broker:33000/testdb"
    assert info["reason"] == "readiness failed: authentication failed for ***"
    assert secret not in sidecar.read_text(encoding="utf-8")
    assert "version" not in info
