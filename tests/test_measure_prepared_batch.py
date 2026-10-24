"""Offline tests for the prepared-batch measurement CLI."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from typing import Any

import pytest

_SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "measure_prepared_batch.py"
_SPEC = importlib.util.spec_from_file_location("_pycubrid_test_measure_prepared_batch", _SCRIPT)
assert _SPEC is not None and _SPEC.loader is not None
measure_prepared_batch = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = measure_prepared_batch
_SPEC.loader.exec_module(measure_prepared_batch)


@pytest.mark.parametrize("rows", ["0", "-1"])
def test_nonpositive_rows_fail_before_measurement(
    rows: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    def fail_measure(mode: str, count: int) -> dict[str, Any]:
        raise AssertionError("measure must not run for a nonpositive row count")

    monkeypatch.setattr(measure_prepared_batch, "measure", fail_measure)

    with pytest.raises(SystemExit) as excinfo:
        measure_prepared_batch.main(["--rows", rows, "--modes", "sync"])

    assert excinfo.value.code == 2
    error = capsys.readouterr().err
    assert "usage:" in error
    assert "must be a positive integer" in error


def test_positive_rows_reach_measurement_without_database(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    calls: list[tuple[str, int]] = []

    def fake_measure(mode: str, count: int) -> dict[str, Any]:
        calls.append((mode, count))
        return {"mode": mode, "rows": count}

    monkeypatch.setattr(measure_prepared_batch, "measure", fake_measure)

    assert measure_prepared_batch.main(["--rows", "3", "--modes", "sync"]) == 0
    assert calls == [("sync", 3)]
    assert capsys.readouterr().out == '{"mode": "sync", "rows": 3}\n'
