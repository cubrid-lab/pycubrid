#!/usr/bin/env python3
"""Detect performance regressions from pytest-benchmark JSON output (issue #357).

Compares a fresh ``--benchmark-json`` run against a stored baseline and reports,
per benchmark, the percentage change in median time. Microbenchmarks are noisy,
so this is a *trend* tool for nightly runs, not a hard PR gate: by default it
prints a report and exits 0. Pass ``--fail-threshold PCT`` to exit non-zero when
any benchmark regresses by more than PCT percent (used by the nightly trend
alert, e.g. >20% across consecutive runs).

Usage:
    # Produce a run:
    pytest tests/test_benchmarks.py --benchmark-enable \\
        --benchmark-json=current.json

    # Compare against a baseline:
    python scripts/bench_regression.py --baseline baseline.json --current current.json
    python scripts/bench_regression.py --baseline baseline.json --current current.json \\
        --fail-threshold 20

Exit codes:
    0 - report printed; no regression over the fail threshold (or none set)
    1 - a benchmark regressed beyond --fail-threshold
    2 - usage / input error
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


def _load_medians(path: Path) -> dict[str, float]:
    """Map benchmark name -> median seconds from a pytest-benchmark JSON file."""
    try:
        data = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        print(f"error: cannot read benchmark JSON {path}: {exc}", file=sys.stderr)
        raise SystemExit(2) from exc

    medians: dict[str, float] = {}
    for bench in data.get("benchmarks", []):
        name = bench.get("name") or bench.get("fullname")
        stats = bench.get("stats", {})
        median = stats.get("median")
        if name and isinstance(median, (int, float)):
            medians[name] = float(median)
    return medians


def _pct_change(baseline: float, current: float) -> float:
    """Percentage change of current vs baseline; positive means slower."""
    if baseline <= 0:
        return 0.0
    return (current - baseline) / baseline * 100.0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", type=Path, required=True)
    parser.add_argument("--current", type=Path, required=True)
    parser.add_argument(
        "--fail-threshold",
        type=float,
        default=None,
        help="Exit 1 if any benchmark regresses by more than this percent.",
    )
    args = parser.parse_args(argv)

    baseline = _load_medians(args.baseline)
    current = _load_medians(args.current)

    common = sorted(set(baseline) & set(current))
    if not common:
        print("warning: no benchmarks in common between baseline and current")
        return 0

    print(f"{'benchmark':45}  {'baseline(ms)':>12}  {'current(ms)':>12}  {'change':>8}")
    print("-" * 82)
    worst = 0.0
    regressions: list[tuple[str, float]] = []
    for name in common:
        b = baseline[name]
        c = current[name]
        delta = _pct_change(b, c)
        worst = max(worst, delta)
        flag = ""
        if args.fail_threshold is not None and delta > args.fail_threshold:
            flag = "  <-- REGRESSION"
            regressions.append((name, delta))
        print(f"{name:45}  {b * 1000:12.4f}  {c * 1000:12.4f}  {delta:+7.1f}%{flag}")

    only_baseline = sorted(set(baseline) - set(current))
    only_current = sorted(set(current) - set(baseline))
    if only_baseline:
        print(f"\nbenchmarks only in baseline (dropped?): {', '.join(only_baseline)}")
    if only_current:
        print(f"benchmarks only in current (new?): {', '.join(only_current)}")

    print(f"\nworst change: {worst:+.1f}%")

    if args.fail_threshold is not None and regressions:
        print(
            f"\nFAIL: {len(regressions)} benchmark(s) regressed beyond {args.fail_threshold:.0f}%",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
