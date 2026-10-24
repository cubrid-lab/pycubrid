"""Offline tests for the benchmark regression report."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pytest

_SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "bench_regression.py"
_SPEC = importlib.util.spec_from_file_location("_pycubrid_test_bench_regression", _SCRIPT)
assert _SPEC is not None and _SPEC.loader is not None
bench_regression = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(bench_regression)


def _write_benchmarks(path: Path, medians: dict[str, float]) -> None:
    path.write_text(
        json.dumps(
            {
                "benchmarks": [
                    {"name": name, "stats": {"median": median}} for name, median in medians.items()
                ]
            }
        )
    )


def _run_report(
    tmp_path: Path,
    baseline: dict[str, float],
    current: dict[str, float],
    *,
    threshold: float,
) -> int:
    baseline_path = tmp_path / "baseline.json"
    current_path = tmp_path / "current.json"
    _write_benchmarks(baseline_path, baseline)
    _write_benchmarks(current_path, current)
    return bench_regression.main(
        [
            "--baseline",
            str(baseline_path),
            "--current",
            str(current_path),
            "--fail-threshold",
            str(threshold),
        ]
    )


def test_regression_strictly_above_threshold_fails(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    assert _run_report(tmp_path, {"select": 0.1}, {"select": 0.121}, threshold=20) == 1

    captured = capsys.readouterr()
    assert "+21.0%  <-- REGRESSION" in captured.out
    assert "worst change: +21.0%" in captured.out
    assert "FAIL: 1 benchmark(s) regressed beyond 20%" in captured.err


@pytest.mark.parametrize(
    ("current_median", "expected_change"),
    [(0.08, "-20.0%"), (0.12, "+20.0%")],
)
def test_improvement_and_exact_threshold_pass(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    current_median: float,
    expected_change: str,
) -> None:
    assert _run_report(tmp_path, {"select": 0.1}, {"select": current_median}, threshold=20) == 0

    captured = capsys.readouterr()
    assert expected_change in captured.out
    assert "REGRESSION" not in captured.out
    assert captured.err == ""


def test_no_common_benchmarks_warn_without_regression(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    assert _run_report(tmp_path, {"select": 0.1}, {"insert": 0.2}, threshold=0) == 0

    captured = capsys.readouterr()
    assert captured.out == "warning: no benchmarks in common between baseline and current\n"
    assert "REGRESSION" not in captured.out
    assert captured.err == ""


def test_malformed_json_exits_with_input_error(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    baseline_path = tmp_path / "baseline.json"
    current_path = tmp_path / "current.json"
    baseline_path.write_text("{not json")
    _write_benchmarks(current_path, {"select": 0.1})

    with pytest.raises(SystemExit) as excinfo:
        bench_regression.main(
            [
                "--baseline",
                str(baseline_path),
                "--current",
                str(current_path),
            ]
        )

    assert excinfo.value.code == 2
    captured = capsys.readouterr()
    assert captured.out == ""
    assert f"error: cannot read benchmark JSON {baseline_path}:" in captured.err
