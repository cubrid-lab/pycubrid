"""Offline tests for the connection profiling helper."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from typing import Any

import pycubrid
import pytest

_SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "profile_connect.py"
_SPEC = importlib.util.spec_from_file_location("_pycubrid_test_profile_connect", _SCRIPT)
assert _SPEC is not None and _SPEC.loader is not None
profile_connect = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(profile_connect)


def test_run_connect_cycle_closes_every_connection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FakeConnection:
        def __init__(self) -> None:
            self.close_calls = 0

        def close(self) -> None:
            self.close_calls += 1

    calls: list[dict[str, Any]] = []
    connections: list[FakeConnection] = []

    def fake_connect(**kwargs: Any) -> FakeConnection:
        calls.append(kwargs)
        connection = FakeConnection()
        connections.append(connection)
        return connection

    monkeypatch.setattr(pycubrid, "connect", fake_connect)

    profile_connect.run_connect_cycle(
        host="db.example",
        port=33001,
        database="profiledb",
        user="profile_user",
        password="secret",
        iterations=3,
    )

    expected = {
        "host": "db.example",
        "port": 33001,
        "database": "profiledb",
        "user": "profile_user",
        "password": "secret",
    }
    assert calls == [expected, expected, expected]
    assert [connection.close_calls for connection in connections] == [1, 1, 1]


@pytest.mark.parametrize("iterations", [1, 3, 100])
def test_parser_accepts_documented_iteration_and_output_options(iterations: int) -> None:
    args = profile_connect._build_parser().parse_args(
        ["--iterations", str(iterations), "--output", "connect.prof"]
    )

    assert args.iterations == iterations
    assert args.output == "connect.prof"


def test_parser_keeps_default_iterations() -> None:
    assert profile_connect._build_parser().parse_args([]).iterations == 100


@pytest.mark.parametrize("iterations", ["0", "-1", "invalid", "1.5"])
def test_parser_rejects_invalid_iterations(
    iterations: str, capsys: pytest.CaptureFixture[str]
) -> None:
    with pytest.raises(SystemExit) as exc_info:
        profile_connect._build_parser().parse_args(["--iterations", iterations])

    assert exc_info.value.code == 2
    captured = capsys.readouterr()
    assert "argument --iterations: must be a positive integer" in captured.err
    assert captured.out == ""


@pytest.mark.parametrize("iterations", ["0", "-1"])
def test_main_rejects_invalid_iterations_before_profiling(
    iterations: str, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    output = tmp_path / "connect.prof"

    def unexpected_work(*args: Any, **kwargs: Any) -> None:
        pytest.fail("Invalid iterations must not start profiling or database work")

    monkeypatch.setattr(
        sys, "argv", [str(_SCRIPT), "--iterations", iterations, "--output", str(output)]
    )
    monkeypatch.setattr(profile_connect.cProfile, "Profile", unexpected_work)
    monkeypatch.setattr(pycubrid, "connect", unexpected_work)

    with pytest.raises(SystemExit) as exc_info:
        profile_connect.main()

    assert exc_info.value.code == 2
    assert not output.exists()
