"""Offline microbenchmark of FETCH reply parsing (#559).

Each workload is a synthetic, well-formed FC8 FETCH reply built with the fuzz
seed builders in ``tests/helpers/cas_reply.py``, so no server is needed and the
bytes are identical on every run. Every benchmark first checks the parsed rows
against the builders' exact oracle, so a faster parser cannot pass with
different results.

Timing runs only on request, keeping noisy thresholds out of required CI::

    pytest tests/test_bench_fetch_parsing.py --benchmark-enable \\
        --benchmark-json=fetch-parse.json

Without ``--benchmark-enable`` (or ``--benchmark-only``) each workload is
parsed once as a correctness smoke test. Compare two JSON runs with
``scripts/bench_regression.py``. Peak allocation of one parse (tracemalloc)
is recorded in each benchmark's ``extra_info``.
"""

from __future__ import annotations

import sys
import tracemalloc
from collections.abc import Callable
from typing import Any

import pytest

from pycubrid.constants import CUBRIDDataType as T
from pycubrid.protocol import FetchPacket

from .helpers.cas_reply import (
    Column,
    ResultSet,
    Value,
    bigint,
    collection,
    datetime_,
    double,
    fetch_reply,
    int_,
    numeric,
    short,
    text,
)

ROWS = 2000
ROUNDS = 30
WARMUP_ROUNDS = 3


def _scalar_row(i: int) -> tuple[Value | None, ...]:
    return (int_(i), bigint(i * 1_000_003), double(i / 7), short(i % 32_000), int_(-i))


def _text_row(i: int) -> tuple[Value | None, ...]:
    return tuple(text(f"row-{i:05d}-col-{c}-abcdefghij") for c in range(5))


def _mixed_row(i: int) -> tuple[Value | None, ...]:
    return (
        int_(i),
        text(f"name-{i:05d}"),
        datetime_(2026, 1 + i % 12, 1 + i % 28, i % 24, i % 60, i % 60, i % 1000),
        numeric(f"{i}.{i % 100:02d}"),
        None if i % 3 == 0 else text("optional"),
    )


def _collection_row(i: int) -> tuple[Value | None, ...]:
    return (
        int_(i),
        collection(T.SET, T.INT, [int_(i + k) for k in range(4)]),
        collection(T.SEQUENCE, T.STRING, [text(f"e{i}-{k}") for k in range(3)]),
    )


WORKLOADS: dict[str, tuple[tuple[Column, ...], Callable[[int], tuple[Value | None, ...]]]] = {
    "scalar": (
        (
            Column("c_int", T.INT),
            Column("c_bigint", T.BIGINT),
            Column("c_double", T.DOUBLE),
            Column("c_short", T.SHORT),
            Column("c_int2", T.INT),
        ),
        _scalar_row,
    ),
    "text": (tuple(Column(f"c_{c}", T.STRING, precision=64) for c in range(5)), _text_row),
    "mixed": (
        (
            Column("c_id", T.INT),
            Column("c_name", T.STRING, precision=32),
            Column("c_dt", T.DATETIME),
            Column("c_num", T.NUMERIC, scale=2, precision=10),
            Column("c_opt", T.STRING, precision=16),
        ),
        _mixed_row,
    ),
    "collection": (
        (
            Column("c_id", T.INT),
            Column("c_set", T.SET, element_type=T.INT),
            Column("c_seq", T.SEQUENCE, element_type=T.STRING),
        ),
        _collection_row,
    ),
}

CASES = [
    ("scalar", False),
    ("text", False),
    ("mixed", False),
    ("collection", True),
    ("collection", False),
]


def _build(name: str) -> ResultSet:
    columns, make_row = WORKLOADS[name]
    return ResultSet(f"bench-{name}", columns, tuple(make_row(i) for i in range(ROWS)))


def _benchmarks_enabled(config: pytest.Config) -> bool:
    return bool(config.getoption("benchmark_enable") or config.getoption("benchmark_only"))


def _peak_allocation(parse: Callable[[], Any]) -> int:
    tracemalloc.start()
    try:
        parse()
        return tracemalloc.get_traced_memory()[1]
    finally:
        tracemalloc.stop()


@pytest.mark.parametrize(
    ("workload", "decode_collections"),
    CASES,
    ids=[f"{name}-{'decoded' if decode else 'raw'}" for name, decode in CASES],
)
def test_bench_fetch_parse(
    benchmark: Any, request: pytest.FixtureRequest, workload: str, decode_collections: bool
) -> None:
    rs = _build(workload)
    data = fetch_reply(rs).data
    columns = rs.metadata()
    expected = rs.expected(decode_collections=decode_collections, json_loads=False)

    def parse() -> list[tuple[Any, ...]]:
        packet = FetchPacket(1, 0, columns=columns, decode_collections=decode_collections)
        packet.parse(data)
        return packet.rows

    assert parse() == expected
    if not _benchmarks_enabled(request.config):
        return
    benchmark.extra_info.update(
        rows=ROWS,
        columns=len(columns),
        reply_bytes=len(data),
        python=sys.version.split()[0],
        peak_alloc_bytes=_peak_allocation(parse),
    )
    benchmark.pedantic(parse, rounds=ROUNDS, warmup_rounds=WARMUP_ROUNDS)
