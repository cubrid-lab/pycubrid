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
    def fail_measure(mode: str, count: int, *options: Any) -> dict[str, Any]:
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
    calls: list[tuple[Any, ...]] = []

    def fake_measure(mode: str, count: int, *options: Any) -> dict[str, Any]:
        calls.append((mode, count, *options))
        return {"mode": mode, "rows": count}

    monkeypatch.setattr(measure_prepared_batch, "measure", fake_measure)

    assert measure_prepared_batch.main(["--rows", "3", "--modes", "sync"]) == 0
    assert calls == [("sync", 3, None, measure_prepared_batch.DEFAULT_VALUE_BYTES)]
    assert capsys.readouterr().out == '{"mode": "sync", "rows": 3}\n'


def test_chunk_and_width_options_reach_measurement(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[Any, ...]] = []

    def fake_measure(mode: str, count: int, *options: Any) -> dict[str, Any]:
        calls.append((mode, count, *options))
        return {}

    monkeypatch.setattr(measure_prepared_batch, "measure", fake_measure)

    argv = ["--rows", "3", "--modes", "sync", "--chunk-size", "2", "--value-bytes", "100"]
    assert measure_prepared_batch.main(argv) == 0
    assert calls == [("sync", 3, 2, 100)]


@pytest.mark.parametrize("option", ["--chunk-size", "--value-bytes"])
def test_nonpositive_options_fail_before_measurement(option: str) -> None:
    with pytest.raises(SystemExit):
        measure_prepared_batch.main(["--rows", "3", option, "0"])


def test_chunks_keep_every_row_in_order() -> None:
    chunks = measure_prepared_batch._chunks
    assert list(chunks(iter(range(5)), 2)) == [[0, 1], [2, 3], [4]]
    assert list(chunks(iter(range(4)), 2)) == [[0, 1], [2, 3]]
    assert list(chunks(iter(()), 2)) == []


def test_no_chunk_size_passes_the_input_through_unchanged() -> None:
    rows = iter(range(3))
    assert list(measure_prepared_batch._chunks(rows, None)) == [rows]


def test_rows_are_padded_to_the_requested_width() -> None:
    rows = list(measure_prepared_batch._rows(2, 20))
    assert [row[0] for row in rows] == [0, 1]
    assert {len(row[1]) for row in rows} == {20}
    assert next(measure_prepared_batch._rows(1))[1] == "v000000000"
