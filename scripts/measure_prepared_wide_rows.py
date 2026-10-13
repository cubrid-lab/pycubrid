#!/usr/bin/env python3
"""Measure wide-value and two-connection ``executemany(prepared=True)`` batches (#881).

#854 measured narrow rows (``scripts/measure_prepared_batch.py``). This measures
the client cost of rows with larger values, and of two batches that run at the
same time on two connections (separate physical sessions), on a live CUBRID:

* profiles (one value column next to ``id INT PRIMARY KEY``):

  - ``text-1k``: ASCII text of 1,000 characters into ``VARCHAR(1000)``;
  - ``text-utf8-8k``: 2,700 Hangul characters (8,100 UTF-8 bytes, a new string
    per row) into
    ``VARCHAR(8192)`` (wide enough in characters or bytes, whatever the database charset);
  - ``bytes-1k``: 1,024 bytes into ``BIT VARYING(8192)``;
  - ``sequence-50``: a ``pycubrid.types.Sequence`` of 50 short strings into
    ``SEQUENCE(VARCHAR(16))``;
  - ``blob``: one ``Lob`` from ``create_lob()`` bound in every row (sync only,
    autocommit off, as the prepared path requires);

* modes: ``sync`` and ``async`` (one connection), ``sync-2conn`` (two threads,
  one connection each) and ``async-2conn`` (two connections on one event loop).

Each case runs in a fresh child process twice: once untraced for time and peak
RSS, once with ``tracemalloc`` for the Python heap peak. Reported: rows per
connection, ``EXECUTE_ARRAY`` request bytes (summed over connections), heap peak
per row, RSS growth and the row count checked with ``COUNT(*)``. Times
(#914): ``executemany_ms`` and ``commit_ms`` per connection, the batch window
from the first ``executemany()`` start to the last end, the spread of the start
times, and ``elapsed_ms`` for the whole child workload (connect, ``CREATE
TABLE``, rows, batch, commit, ``COUNT(*)``, ``DROP TABLE``). Both two-connection
modes synchronize readiness: each connection waits at a barrier
(``threading.Barrier`` for the threads, ``asyncio.Barrier`` for the tasks) once
it has built its rows. The threads then start ``executemany()`` within a few
milliseconds; the tasks start one after the other, because the first encodes
its rows without yielding to the event loop (#882). ``start_skew_ms`` records
the difference. Every batch is committed and its table dropped. The rows are built as a
list in the measured process, so the heap peak and RSS growth include them. RSS comes from ``resource.getrusage`` (Unix only, KiB on
Linux). The endpoint is resolved as the test suite does
(``tests/_cubrid_endpoint.py``).

Usage:
    python scripts/measure_prepared_wide_rows.py [--rows N] [--profiles P ...]
        [--modes M ...] [--json PATH]
"""

from __future__ import annotations

import argparse
import asyncio
import importlib.util
import json
import os
import subprocess
import sys
import threading
import time
import tracemalloc
import uuid
from pathlib import Path
from typing import Any

import pycubrid
import pycubrid.aio
from pycubrid.constants import CUBRIDDataType
from pycubrid.protocol import ExecuteArrayPacket
from pycubrid.types import Sequence

PROFILES = ("text-1k", "text-utf8-8k", "bytes-1k", "sequence-50", "blob")
MODES = ("sync", "async", "sync-2conn", "async-2conn")
DEFAULT_ROWS = 10_000
_COLUMN = {
    "text-1k": "VARCHAR(1000)",
    "text-utf8-8k": "VARCHAR(8192)",
    "bytes-1k": "BIT VARYING(8192)",
    "sequence-50": "SEQUENCE(VARCHAR(16))",
    "blob": "BLOB",
}


def _endpoint_kwargs() -> dict[str, Any]:
    spec = importlib.util.spec_from_file_location(
        "_pycubrid_measure_endpoint",
        Path(__file__).resolve().parents[1] / "tests" / "_cubrid_endpoint.py",
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    kwargs: dict[str, Any] = module.resolve_endpoint().connect_kwargs()
    return kwargs


def _value(profile: str, i: int) -> Any:
    if profile == "text-1k":
        return ("%09d" % i) + "x" * 991
    if profile == "text-utf8-8k":
        # A fresh string per row (the first character varies), so the input is
        # not one constant shared by every row.
        return chr(0xAC00 + i % 11172) + "가" * 2699
    if profile == "bytes-1k":
        return bytes((i + k) % 256 for k in range(1024))
    if profile == "sequence-50":
        return Sequence(["e%02d-%09d" % (k, i) for k in range(50)])
    raise ValueError(profile)


# Packed LOB handle length bound by each sync connection (one handle per batch).
_BLOB_HANDLE_BYTES: list[int] = []
_HANDLE_LOCK = threading.Lock()


def _record_execute_array_bytes() -> list[int]:
    sizes: list[int] = []
    lock = threading.Lock()
    original = ExecuteArrayPacket.write

    def write(self: Any, cas_info: bytes) -> bytes:
        data: bytes = original(self, cas_info)
        with lock:
            sizes.append(len(data))
        return data

    setattr(ExecuteArrayPacket, "write", write)
    return sizes


def _max_rss_kib() -> int:
    import resource  # Unix only

    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss


def _timed(started: float, executed: float, committed: float, count: int) -> dict[str, Any]:
    return {
        "count": count,
        "started": started,
        "executed": executed,
        "executemany_ms": round((executed - started) * 1000, 1),
        "commit_ms": round((committed - executed) * 1000, 1),
    }


def _sync_batch(
    kwargs: dict[str, Any], profile: str, count: int, start: threading.Barrier | None
) -> dict[str, Any]:
    table = "p881_" + uuid.uuid4().hex[:12]
    with pycubrid.connect(**kwargs) as conn:
        cursor = conn.cursor()
        cursor.execute(f"CREATE TABLE {table} (id INT PRIMARY KEY, v {_COLUMN[profile]})")
        conn.commit()
        try:
            if profile == "blob":
                lob = conn.create_lob(CUBRIDDataType.BLOB)
                lob.write(b"\x5a" * 1024)
                with _HANDLE_LOCK:
                    _BLOB_HANDLE_BYTES.append(len(lob.lob_handle))
                rows: list[tuple[Any, ...]] = [(i, lob) for i in range(count)]
            else:
                rows = [(i, _value(profile, i)) for i in range(count)]
            if start is not None:
                start.wait()
            started = time.perf_counter()
            cursor.executemany(f"INSERT INTO {table} (id, v) VALUES (?, ?)", rows, prepared=True)
            executed = time.perf_counter()
            conn.commit()
            committed = time.perf_counter()
            assert cursor.rowcount == count
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            row = cursor.fetchone()
            assert row is not None
            return _timed(started, executed, committed, int(row[0]))
        finally:
            cursor.execute(f"DROP TABLE IF EXISTS {table}")
            conn.commit()


async def _async_batch(
    kwargs: dict[str, Any], profile: str, count: int, start: asyncio.Barrier | None
) -> dict[str, Any]:
    table = "p881_" + uuid.uuid4().hex[:12]
    async with await pycubrid.aio.connect(**kwargs) as conn:
        cursor = conn.cursor()
        await cursor.execute(f"CREATE TABLE {table} (id INT PRIMARY KEY, v {_COLUMN[profile]})")
        await conn.commit()
        try:
            rows = [(i, _value(profile, i)) for i in range(count)]
            if start is not None:
                # Wait until both connections are open, have their tables and have
                # built their rows, as the sync threads do. The first task then
                # encodes without yielding (#882), so the second starts later.
                await start.wait()
            started = time.perf_counter()
            await cursor.executemany(
                f"INSERT INTO {table} (id, v) VALUES (?, ?)", rows, prepared=True
            )
            executed = time.perf_counter()
            await conn.commit()
            committed = time.perf_counter()
            assert cursor.rowcount == count
            await cursor.execute(f"SELECT COUNT(*) FROM {table}")
            row = await cursor.fetchone()
            assert row is not None
            return _timed(started, executed, committed, int(row[0]))
        finally:
            await cursor.execute(f"DROP TABLE IF EXISTS {table}")
            await conn.commit()


def _child(profile: str, mode: str, count: int, trace: bool) -> dict[str, Any]:
    kwargs = _endpoint_kwargs()
    sizes = _record_execute_array_bytes()
    rss_before = _max_rss_kib()
    if trace:
        tracemalloc.start()
    started = time.perf_counter()
    batches: list[dict[str, Any]]
    if mode == "sync":
        batches = [_sync_batch(kwargs, profile, count, None)]
    elif mode == "sync-2conn":
        barrier = threading.Barrier(2)
        results: list[dict[str, Any]] = []
        errors: list[Exception] = []

        def run() -> None:
            try:
                results.append(_sync_batch(kwargs, profile, count, barrier))
            except Exception as exc:  # noqa: BLE001 - re-raised below
                errors.append(exc)
                barrier.abort()

        threads = [threading.Thread(target=run) for _ in range(2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        if errors:
            raise errors[0]
        batches = results
    elif mode == "async":
        batches = [asyncio.run(_async_batch(kwargs, profile, count, None))]
    else:

        async def both() -> list[dict[str, Any]]:
            start = asyncio.Barrier(2)
            return list(
                await asyncio.gather(
                    _async_batch(kwargs, profile, count, start),
                    _async_batch(kwargs, profile, count, start),
                )
            )

        batches = asyncio.run(both())
    elapsed = time.perf_counter() - started
    result: dict[str, Any] = {
        "profile": profile,
        "mode": mode,
        "rows_per_connection": count,
        "counts": sorted(b["count"] for b in batches),
        # Per connection: the executemany() call alone and the commit after it.
        "executemany_ms": sorted(b["executemany_ms"] for b in batches),
        "commit_ms": sorted(b["commit_ms"] for b in batches),
        # From the first executemany() start to the last one's end.
        "batch_window_ms": round(
            (max(b["executed"] for b in batches) - min(b["started"] for b in batches)) * 1000, 1
        ),
        # Spread of the executemany() start times (0 for one connection).
        "start_skew_ms": round(
            (max(b["started"] for b in batches) - min(b["started"] for b in batches)) * 1000, 1
        ),
        # The whole child workload: connect, CREATE TABLE, rows, batch, commit,
        # COUNT(*) and DROP TABLE.
        "elapsed_ms": round(elapsed * 1000, 1),
        "execute_array_bytes": sum(sizes),
        "execute_array_frames": sorted(sizes),
        "blob_handle_bytes": sorted(_BLOB_HANDLE_BYTES),
        "rss_growth_kib": _max_rss_kib() - rss_before,
        "load_average_1m": round(os.getloadavg()[0], 2),
    }
    if trace:
        result["tracemalloc_peak_kib"] = round(tracemalloc.get_traced_memory()[1] / 1024)
        tracemalloc.stop()
    return result


def measure(profile: str, mode: str, count: int) -> dict[str, Any]:
    """Run one case in two fresh child processes and merge their results."""
    env = dict(os.environ)
    root = str(Path(__file__).resolve().parents[1])
    env["PYTHONPATH"] = os.pathsep.join(filter(None, [root, env.get("PYTHONPATH")]))
    runs: list[dict[str, Any]] = []
    for trace in (False, True):
        completed = subprocess.run(  # nosec B603 - this interpreter and this script only
            [sys.executable, __file__, "--child", profile, mode, str(count), "1" if trace else "0"],
            check=True,
            env=env,
            capture_output=True,
            text=True,
        )
        runs.append(json.loads(completed.stdout.strip().splitlines()[-1]))
    plain, traced = runs
    if (
        plain["counts"] != traced["counts"]
        or plain["execute_array_bytes"] != traced["execute_array_bytes"]
    ):
        raise RuntimeError(f"{profile} {mode}: the two runs differ")
    rows = count * len(plain["counts"])
    plain["tracemalloc_peak_kib"] = traced["tracemalloc_peak_kib"]
    plain["heap_peak_bytes_per_row"] = round(traced["tracemalloc_peak_kib"] * 1024 / rows)
    plain["request_bytes_per_row"] = round(plain["execute_array_bytes"] / rows)
    return plain


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--rows", type=int, default=DEFAULT_ROWS, help="rows per connection")
    parser.add_argument("--profiles", nargs="+", choices=PROFILES, default=list(PROFILES))
    parser.add_argument("--modes", nargs="+", choices=MODES, default=list(MODES))
    parser.add_argument("--json", type=Path, help="also write the results to this file")
    parser.add_argument("--child", nargs=4, help=argparse.SUPPRESS)
    args = parser.parse_args(argv)
    if args.child:
        profile, mode, count, trace = args.child
        print(json.dumps(_child(profile, mode, int(count), trace == "1"), sort_keys=True))
        return 0
    results = []
    for profile in args.profiles:
        for mode in args.modes:
            if profile == "blob" and mode not in ("sync", "sync-2conn"):
                continue  # async connections have no LOB support
            result = measure(profile, mode, args.rows)
            results.append(result)
            print(json.dumps(result, sort_keys=True), flush=True)
    if args.json is not None:
        args.json.write_text(json.dumps(results, indent=2, sort_keys=True) + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
