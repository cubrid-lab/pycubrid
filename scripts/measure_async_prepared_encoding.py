#!/usr/bin/env python3
"""Measure how long async ``executemany(prepared=True)`` holds the event loop (#882).

The async prepared batch encodes every row synchronously before its first
request (#839), so no other task on the loop runs during that time. This
measures, per row count and input shape:

* ``encode_ms``: ``_encode_prepared_rows()`` alone, with no I/O;
* ``executemany_ms``: the whole ``executemany(prepared=True)`` call, including
  ``PREPARE`` and ``EXECUTE_ARRAY`` on a live server;
* ``max_loop_gap_ms``: the longest time a second task on the same loop waited
  for its turn during that call (it re-schedules itself with ``sleep(0)``);
* ``after_encoding_ms``: ``executemany_ms - encode_ms``, the rest of the call:
  setup checks, ``PREPARE``, building and sending the ``EXECUTE_ARRAY`` frame and
  waiting for both replies. It is not pure network time.

Shapes: ``list`` is built before the measurement; ``generator`` is a one-shot
generator, so the rows are created while they are encoded. Rows are
``(i, "v%09d" % i, i)`` into ``(id INT PRIMARY KEY, v VARCHAR(32), n INT)``,
autocommit off, rolled back after each case. The endpoint is resolved as the
test suite does (``tests/_cubrid_endpoint.py``).

Usage:
    python scripts/measure_async_prepared_encoding.py [--rows N ...] [--json PATH]
"""

from __future__ import annotations

import argparse
import asyncio
import importlib.util
import json
import platform
import sys
import time
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pycubrid
import pycubrid.aio
from pycubrid._prepared_binding import _encode_prepared_rows

DEFAULT_ROWS = (1_000, 10_000, 100_000)
SHAPES = ("list", "generator")


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


def _rows(count: int) -> Iterator[tuple[int, str, int]]:
    for i in range(count):
        yield (i, "v%09d" % i, i)


def _input(shape: str, count: int) -> Any:
    return _rows(count) if shape == "generator" else list(_rows(count))


def _reject_lob(_value: Any) -> None:
    return None


def _encode_ms(shape: str, count: int) -> float:
    rows = _input(shape, count)
    started = time.perf_counter()
    _encode_prepared_rows(rows, "utf-8", _reject_lob)
    return (time.perf_counter() - started) * 1000


async def _measure(kwargs: dict[str, Any], shape: str, count: int) -> dict[str, Any]:
    table = "p882_" + uuid.uuid4().hex[:12]
    async with await pycubrid.aio.connect(**kwargs) as conn:
        cursor = conn.cursor()
        await cursor.execute(f"CREATE TABLE {table} (id INT PRIMARY KEY, v VARCHAR(32), n INT)")
        await conn.commit()
        try:
            rows = _input(shape, count)
            done = asyncio.Event()
            max_gap = 0.0

            async def ticker() -> None:
                nonlocal max_gap
                while not done.is_set():
                    before = time.perf_counter()
                    await asyncio.sleep(0)
                    max_gap = max(max_gap, time.perf_counter() - before)

            tick = asyncio.create_task(ticker())
            await asyncio.sleep(0)  # let the ticker start before the batch
            started = time.perf_counter()
            await cursor.executemany(
                f"INSERT INTO {table} (id, v, n) VALUES (?, ?, ?)", rows, prepared=True
            )
            elapsed = (time.perf_counter() - started) * 1000
            done.set()
            await asyncio.wait_for(tick, timeout=10)
            rowcount = cursor.rowcount
            await conn.rollback()
        finally:
            await cursor.execute(f"DROP TABLE {table}")
            await conn.commit()
    encode = _encode_ms(shape, count)
    return {
        "shape": shape,
        "rows": count,
        "rowcount": rowcount,
        "encode_ms": round(encode, 1),
        "executemany_ms": round(elapsed, 1),
        "after_encoding_ms": round(elapsed - encode, 1),
        "max_loop_gap_ms": round(max_gap * 1000, 1),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--rows", type=int, nargs="+", default=list(DEFAULT_ROWS))
    parser.add_argument("--json", type=Path)
    args = parser.parse_args()
    kwargs = _endpoint_kwargs()
    results = []
    for count in args.rows:
        for shape in SHAPES:
            result = asyncio.run(_measure(kwargs, shape, count))
            results.append(result)
            print(json.dumps(result))
    if args.json is not None:
        meta = {
            "pycubrid": pycubrid.__version__,
            "python": platform.python_version(),
            "platform": platform.platform(),
        }
        args.json.write_text(json.dumps({"meta": meta, "results": results}, indent=2) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
