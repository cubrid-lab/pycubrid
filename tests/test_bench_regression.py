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


def _write_raw_benchmarks(path: Path, entries: list[dict[str, object]]) -> None:
    path.write_text(json.dumps({"benchmarks": entries}))


def _run_report(
    tmp_path: Path,
    baseline: dict[str, float],
    current: dict[str, float],
    *,
    threshold: float | None,
) -> int:
    baseline_path = tmp_path / "baseline.json"
    current_path = tmp_path / "current.json"
    _write_benchmarks(baseline_path, baseline)
    _write_benchmarks(current_path, current)
    argv = ["--baseline", str(baseline_path), "--current", str(current_path)]
    if threshold is not None:
        argv += ["--fail-threshold", str(threshold)]
    return bench_regression.main(argv)


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


def test_load_medians_falls_back_to_fullname_and_skips_unusable_entries(
    tmp_path: Path,
) -> None:
    path = tmp_path / "bench.json"
    _write_raw_benchmarks(
        path,
        [
            {"name": "named", "stats": {"median": 0.1}},
            {"fullname": "tests/test_x.py::test_y", "stats": {"median": 0.2}},
            {"name": "", "fullname": "empty-name", "stats": {"median": 0.3}},
            {"name": "no_stats"},
            {"name": "string_median", "stats": {"median": "0.1"}},
            {"name": "null_median", "stats": {"median": None}},
            {"name": "zero_median", "stats": {"median": 0}},
        ],
    )

    assert bench_regression._load_medians(path) == {
        "named": 0.1,
        "tests/test_x.py::test_y": 0.2,
        "empty-name": 0.3,
        "zero_median": 0.0,
    }


@pytest.mark.parametrize("baseline_median", [0.0, -0.1])
def test_pct_change_with_non_positive_baseline_is_zero(baseline_median: float) -> None:
    assert bench_regression._pct_change(baseline_median, 0.5) == 0.0


def test_zero_baseline_never_reports_a_regression(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    assert _run_report(tmp_path, {"zero": 0.0}, {"zero": 0.5}, threshold=10) == 0

    captured = capsys.readouterr()
    assert "+0.0%" in captured.out
    assert "worst change: +0.0%" in captured.out
    assert "REGRESSION" not in captured.out
    assert captured.err == ""


def test_dropped_and_new_benchmarks_are_listed_sorted(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    assert (
        _run_report(
            tmp_path,
            {"z_dropped": 0.1, "keep": 0.1, "a_dropped": 0.1},
            {"b_new": 0.1, "keep": 0.1, "a_new": 0.1},
            threshold=None,
        )
        == 0
    )

    captured = capsys.readouterr()
    assert "benchmarks only in baseline (dropped?): a_dropped, z_dropped" in captured.out
    assert "benchmarks only in current (new?): a_new, b_new" in captured.out
    assert "REGRESSION" not in captured.out
    assert captured.err == ""


def test_report_only_without_threshold_passes_large_slowdown(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    assert _run_report(tmp_path, {"select": 0.1}, {"select": 0.5}, threshold=None) == 0

    captured = capsys.readouterr()
    assert "+400.0%" in captured.out
    assert "worst change: +400.0%" in captured.out
    assert "REGRESSION" not in captured.out
    assert captured.err == ""
