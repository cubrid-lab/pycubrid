#!/usr/bin/env python3
"""Measure one large ``executemany(prepared=True)`` batch on a live CUBRID (#854).

The prepared batch encodes every row and sends them in one ``EXECUTE_ARRAY``
frame (#839). This measures, per mode and row count, what that costs the
client: peak RSS, the Python heap peak (``tracemalloc``), the bytes of the
``PREPARE`` and ``EXECUTE_ARRAY`` requests, the elapsed time of the batch and
of its commit, and ``rowcount`` checked against ``COUNT(*)``.

Each case runs twice in a fresh child process: once untraced for the time and
RSS, once with ``tracemalloc`` for the heap peak (tracing slows the batch, so
its time is not reported). Rows are ``(i, "v%09d" % i, i)`` into
``(id INT PRIMARY KEY, v VARCHAR(32), n INT)``, autocommit off, one commit.

Modes: ``sync`` and ``async`` pass a list built before the measurement;
``sync-generator`` and ``async-generator`` pass a one-shot generator, so the
rows are created while they are encoded.

The endpoint is resolved as the test suite does (``tests/_cubrid_endpoint.py``:
``CUBRID_TEST_URL`` or ``CUBRID_TEST_HOST`` / ``_PORT`` / ``_DB`` / ``_USER`` /
``_PASSWORD``). RSS comes from ``resource.getrusage`` (Unix only) and is in KiB on Linux.

Usage:
    python scripts/measure_prepared_batch.py [--rows N ...] [--modes M ...] [--json PATH]

Example:
    CUBRID_TEST_URL=cubrid://dba@localhost:33000/testdb \\
        python scripts/measure_prepared_batch.py --rows 10000 100000
"""

from __future__ import annotations

import argparse
import asyncio
import importlib.util
import json
import os
import subprocess
import sys
import time
import tracemalloc
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pycubrid
import pycubrid.aio
from pycubrid.protocol import ExecuteArrayPacket, PreparePacket

MODES = ("sync", "sync-generator", "async", "async-generator")
DEFAULT_ROWS = (10_000, 100_000)


def _positive_int(value: str) -> int:
    """Parse a positive integer for a user-facing command-line option."""
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be a positive integer") from exc
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return parsed


def _endpoint_kwargs() -> dict[str, Any]:
    spec = importlib.util.spec_from_file_location(
        "_pycubrid_measure_endpoint",
        Path(__file__).resolve().parents[1] / "tests" / "_cubrid_endpoint.py",
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module  # dataclasses resolve annotations via sys.modules
    spec.loader.exec_module(module)
    kwargs: dict[str, Any] = module.resolve_endpoint().connect_kwargs()
    return kwargs


def _rows(count: int) -> Iterator[tuple[int, str, int]]:
    for i in range(count):
        yield (i, "v%09d" % i, i)


def _record_request_sizes() -> dict[str, int]:
    """Record the length of every PREPARE and EXECUTE_ARRAY frame written."""
    sizes = {"prepare_bytes": 0, "execute_array_bytes": 0}
    for cls, key in ((PreparePacket, "prepare_bytes"), (ExecuteArrayPacket, "execute_array_bytes")):
        original = cls.write

        def write(self: Any, cas_info: bytes, _original: Any = original, _key: str = key) -> bytes:
            data: bytes = _original(self, cas_info)
            sizes[_key] += len(data)
            return data

        setattr(cls, "write", write)
    return sizes


def _max_rss_kib() -> int:
    import resource  # Unix only; imported here so the module imports everywhere

    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss


def _child(mode: str, count: int, trace: bool) -> dict[str, Any]:
    kwargs = _endpoint_kwargs()
    table = "p854_" + uuid.uuid4().hex[:12]
    sizes = _record_request_sizes()
    generator = mode.endswith("generator")
    result: dict[str, Any] = {
        "mode": mode,
        "rows": count,
        "traced": trace,
        "pycubrid": pycubrid.__version__,
        "pycubrid_path": str(Path(pycubrid.__file__).resolve().parent),
    }

    def measure_start() -> Any:
        rows: Any = _rows(count) if generator else list(_rows(count))
        result["rss_before_kib"] = _max_rss_kib()
        if trace:
            tracemalloc.start()
        return rows

    def measure_stop(started: float, executed: float) -> None:
        result["executemany_ms"] = round((executed - started) * 1000, 1)
        result["commit_ms"] = round((time.perf_counter() - executed) * 1000, 1)
        if trace:
            result["tracemalloc_peak_kib"] = round(tracemalloc.get_traced_memory()[1] / 1024)
            tracemalloc.stop()
        result["rss_peak_kib"] = _max_rss_kib()

    sql = f"INSERT INTO {table} (id, v, n) VALUES (?, ?, ?)"
    if mode.startswith("async"):

        async def run() -> None:
            async with await pycubrid.aio.connect(**kwargs) as conn:
                cursor = conn.cursor()
                await cursor.execute(
                    f"CREATE TABLE {table} (id INT PRIMARY KEY, v VARCHAR(32), n INT)"
                )
                await conn.commit()
                try:
                    rows = measure_start()
                    started = time.perf_counter()
                    await cursor.executemany(sql, rows, prepared=True)
                    executed = time.perf_counter()
                    await conn.commit()
                    measure_stop(started, executed)
                    result["rowcount"] = cursor.rowcount
                    await cursor.execute(f"SELECT COUNT(*) FROM {table}")
                    row = await cursor.fetchone()
                    assert row is not None
                    result["count"] = row[0]
                finally:
                    await cursor.execute(f"DROP TABLE IF EXISTS {table}")
                    await conn.commit()

        asyncio.run(run())
    else:
        with pycubrid.connect(**kwargs) as conn:
            cursor = conn.cursor()
            cursor.execute(f"CREATE TABLE {table} (id INT PRIMARY KEY, v VARCHAR(32), n INT)")
            conn.commit()
            try:
                rows = measure_start()
                started = time.perf_counter()
                cursor.executemany(sql, rows, prepared=True)
                executed = time.perf_counter()
                conn.commit()
                measure_stop(started, executed)
                result["rowcount"] = cursor.rowcount
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                row = cursor.fetchone()
                assert row is not None
                result["count"] = row[0]
            finally:
                cursor.execute(f"DROP TABLE IF EXISTS {table}")
                conn.commit()
    result.update(sizes)
    result["load_average_1m"] = round(os.getloadavg()[0], 2)
    return result


def measure(mode: str, count: int) -> dict[str, Any]:
    """Run one case in two fresh child processes and merge their results."""
    # Measure the checkout this script belongs to, even without an editable install.
    env = dict(os.environ)
    root = str(Path(__file__).resolve().parents[1])
    env["PYTHONPATH"] = os.pathsep.join(filter(None, [root, env.get("PYTHONPATH")]))
    runs: list[dict[str, Any]] = []
    for trace in (False, True):
        completed = subprocess.run(  # nosec B603 - this interpreter and this script only
            [sys.executable, __file__, "--child", mode, str(count), "1" if trace else "0"],
            check=True,
            env=env,
            capture_output=True,
            text=True,
        )
        runs.append(json.loads(completed.stdout.strip().splitlines()[-1]))
    plain, traced = runs
    for key in ("rowcount", "count", "prepare_bytes", "execute_array_bytes"):
        if plain[key] != traced[key]:
            raise RuntimeError(f"{mode} {count}: {key} differs between runs")
    plain["tracemalloc_peak_kib"] = traced["tracemalloc_peak_kib"]
    plain["rss_growth_kib"] = plain["rss_peak_kib"] - plain["rss_before_kib"]
    del plain["traced"]
    return plain


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--rows", type=_positive_int, nargs="+", default=list(DEFAULT_ROWS))
    parser.add_argument("--modes", nargs="+", choices=MODES, default=list(MODES))
    parser.add_argument("--json", type=Path, help="also write the results to this file")
    parser.add_argument(
        "--child", nargs=3, metavar=("MODE", "ROWS", "TRACE"), help=argparse.SUPPRESS
    )
    args = parser.parse_args(argv)
    if args.child:
        mode, count, trace = args.child
        print(json.dumps(_child(mode, int(count), trace == "1"), sort_keys=True))
        return 0
    results = []
    for count in args.rows:
        for mode in args.modes:
            result = measure(mode, count)
            results.append(result)
            print(json.dumps(result, sort_keys=True), flush=True)
    if args.json is not None:
        args.json.write_text(json.dumps(results, indent=2, sort_keys=True) + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
