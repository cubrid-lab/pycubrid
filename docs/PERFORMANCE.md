# Performance Guide

This guide covers the performance of `pycubrid` running on CUBRID: how to measure it, what is
measured for the current release, and how to profile and tune it.

It does not compare CUBRID with other database engines. A comparison across engines mixes
server-engine differences with driver overhead, so it cannot tell you how fast the driver is.

---

## Table of Contents

- [Performance Overview](#performance-overview)
- [Benchmark Methodology](#benchmark-methodology)
- [Current Release Baseline](#current-release-baseline)
- [Performance Regression](#performance-regression)
- [Sync and Async Characteristics](#sync-and-async-characteristics)
- [Batch Processing](#batch-processing)
- [Fetch and Memory Performance](#fetch-and-memory-performance)
- [Profiling and Optimization](#profiling-and-optimization)
- [Known Limitations](#known-limitations)
- [Reproduction Guide](#reproduction-guide)

---

## Performance Overview

`pycubrid` is a pure Python DBAPI2 driver that talks to CUBRID over the CAS binary protocol.

```mermaid
flowchart LR
    App[Python Application] --> Driver[pycubrid\nPure Python DBAPI2]
    Driver --> CAS[CAS Binary Protocol over TCP]
    CAS --> Broker[CUBRID Broker / CAS]
    Broker --> Server["(CUBRID Server)"]
```

```mermaid
flowchart TD
    Q[SQL + Parameters] --> Encode[Python object encoding]
    Encode --> Packet[CAS packet serialization]
    Packet --> Net[TCP round-trip]
    Net --> Exec[Server execution]
    Exec --> Decode[Row decoding to Python objects]
```

- `pycubrid` is pure Python, so packet encode/decode and row conversion run in the interpreter.
- CAS uses a binary protocol with explicit packet framing and parsing; this adds per-request work.
- Small, chatty queries amplify Python-level and round-trip overhead.
- Throughput improves when calls are batched and transaction boundaries are controlled.

Driver performance is tracked against two references, both on the same CUBRID server:

- **Release to release** — the current `pycubrid` release against an earlier release.
- **Driver overhead** — `pycubrid` against the official `CUBRIDdb` driver.

---

## Benchmark Methodology

A benchmark result is only useful when the driver is the one thing that changes.

- **Same server.** Run every driver or release under test against the same CUBRID server, on
  the same host, with the same schema and data. Do not compare results across database
  engines.
- **Same transaction mode.** Set `autocommit` explicitly on every connection under test. Drivers
  have different defaults, and a commit per statement changes the result.
- **Record the environment.** CPU, OS, Python version, CUBRID version, `pycubrid` version and
  `fetch_size`.
- **Repeat.** Run several rounds and report the median and spread, not a single run.

Tools in this repository:

| Tool | What it measures | Needs a server |
|---|---|---|
| `tests/test_benchmarks.py` | Connect, single-row CRUD, 100-row insert/select and prepared reuse (pytest-benchmark) | Yes |
| `tests/test_bench_fetch_parsing.py` | FETCH reply parsing of synthetic 2000-row replies | No |
| `scripts/bench_regression.py` | Median change between two pytest-benchmark JSON files | No |
| `scripts/profile_*.py` | cProfile of connect, execute and fetch | Yes |

Larger workloads and the harness for comparing drivers live in
[cubrid-benchmark](https://github.com/cubrid-lab/cubrid-benchmark).

---

## Current Release Baseline

Measured on 2026-10-11 for [#797](https://github.com/cubrid-lab/pycubrid/issues/797) with the
[`pycubrid-release-baseline`](https://github.com/cubrid-lab/cubrid-benchmark/tree/main/experiments/pycubrid-release-baseline)
experiment in `cubrid-benchmark`. That experiment also holds the raw results, the
reproduction command and the full tables.

| Item | Value |
|---|---|
| CPU | Intel Core i5-9400F @ 2.90 GHz, 6 cores (1 thread per core) |
| Memory | 31 GiB |
| OS | Ubuntu 22.04.5 LTS, Linux 5.15.0-198-generic, x86_64 |
| Python | CPython 3.12.13 |
| CUBRID | 11.4.6 (`cubrid/cubrid:11.4` in Docker 29.2.1, localhost) |
| `pycubrid` | `main` at `2228d66`, and 1.10.0 from PyPI |
| `CUBRIDdb` | cubrid-python `v11.3.0.51` (`c63ab9c`), C extension over CCI |
| Transaction mode | `autocommit=False` on every connection (`connect(autocommit=False)` for `pycubrid`), explicit `commit()` per write |
| `fetch_size` | 100 (default), plus 1,000 and 10,000 for the fetch cases |
| Data | 10,000 rows of `(INT PK, VARCHAR(100), VARCHAR(200), INT)` |
| Repetitions | 5 trials per driver in rotating order; samples pooled; warm-up discarded |
| Load | Shared machine with other users active, so results are noisy |

Every reported iteration was checked for the right row counts and values. Times are pooled
medians in milliseconds per operation; the full report also has p95, IQR and the per-trial
spread.

### Current release against 1.10.0

**No regression was detected.** A regression here means the `main` median is more than 5%
slower and the interquartile ranges do not overlap. The second condition means that only a
slowdown larger than the spread of the samples can be detected. Given the measured spread, the
rule detects slowdowns of at least:

| Operation | Minimum detectable slowdown |
|---|---:|
| `fetchone` / `fetchmany` / `fetchall`, 10,000 rows | 6-8% |
| `executemany()` 10,000 / 1,000 / 100 rows | 5% / 18% / 172% |
| connect + close | 22% |
| SELECT by primary key, single-row writes + commit | 25-26% |
| single-row execute without commit | 271-307% |

The per-trial medians of `main` and 1.10.0 overlap in every case except `executemany()` of
10,000 rows, where `main` is about 0.7% slower in every trial. That is well below the 5%
threshold.

| Operation | `main` | 1.10.0 | Ratio |
|---|---:|---:|---:|
| connect + close | 1.50 | 1.49 | 1.01 |
| SELECT by primary key | 0.60 | 0.60 | 1.00 |
| INSERT + commit | 34.5 | 33.9 | 1.02 |
| UPDATE + commit | 33.8 | 33.9 | 1.00 |
| DELETE + commit | 41.2 | 41.3 | 1.00 |
| `executemany()` 100 / 1,000 / 10,000 rows | 125 / 360 / 3,018 | 125 / 358 / 2,998 | 1.00 / 1.00 / 1.01 |
| `fetchone` / `fetchmany` / `fetchall`, 10,000 rows | 51.2 / 48.4 / 49.7 | 50.6 / 48.9 / 49.6 | 1.01 / 0.99 / 1.00 |

### `pycubrid` against `CUBRIDdb` on the same server

| Operation | `pycubrid` | `CUBRIDdb` | `pycubrid` / `CUBRIDdb` |
|---|---:|---:|---:|
| connect + close | 1.50 | 1.00 | 1.49× |
| SELECT by primary key | 0.60 | 0.55 | 1.10× |
| INSERT / UPDATE + commit | 34.5 / 33.8 | 33.5 / 33.6 | 1.03× / 1.01× |
| `executemany()` 100 / 1,000 / 10,000 rows | 125 / 360 / 3,018 | 109 / 266 / 2,150 | 1.15× / 1.35× / 1.40× |
| `fetchone` / `fetchmany` / `fetchall`, 10,000 rows | 51.2 / 48.4 / 49.7 | 40.6 / 40.7 / 39.4 | 1.26× / 1.19× / 1.26× |

- Single-row writes are bound by the server commit. Latency falls into clusters at about 33,
  41, 75 and 83 ms for both drivers, so driver overhead is a small share of a write.
  (DELETE + commit measured 41.2 ms against 33.8 ms only because the median fell into a
  different cluster.)
- `executemany()` sends one batch request, but it is still slower than `CUBRIDdb`, which runs
  one prepared execute per row; the time is spent on the server, see
  [where the gap comes from](#executemany-against-cubriddb-where-the-gap-comes-from).
- Fetching 10,000 rows takes 19-26% longer than with the C extension.

### Sync against async

PK SELECT throughput, in queries per second. Each worker has its own connection and runs 200
queries.

| Workers | Sync (threads) | Async (`pycubrid.aio` tasks) | `CUBRIDdb` (threads) |
|---:|---:|---:|---:|
| 1 | 1,510 | 1,414 | 1,725 |
| 8 | 5,336 | 5,672 | 981 |
| 32 | 5,173 | 5,693 | 956 |

On one connection, async is about 6% slower than sync. With 8-32 concurrent connections it
is 6-10% faster. From 1 to 8 workers, sync scales about 3.5× and async about 4.0×; both level
off after that. Per-query p95 at 32 workers is 13.0 ms for sync and 8.8 ms for async.
`CUBRIDdb` throughput drops when more than one thread uses it, because its C extension does
not release the GIL during CCI calls.

### `executemany()` batch throughput

| Rows | Time incl. commit (ms) | Rows/s | Peak Python heap |
|---:|---:|---:|---:|
| 100 | 125 | 803 | 42 KiB |
| 1,000 | 360 | 2,780 | 364 KiB |
| 10,000 | 3,018 | 3,314 | 3.6 MiB |

Client memory grows by about 380 bytes per row, which confirms
[Known Limitations](#known-limitations).

### `executemany()` against `CUBRIDdb`: where the gap comes from

The gap above was investigated in
[#822](https://github.com/cubrid-lab/pycubrid/issues/822) with the
[`pycubrid-executemany-overhead`](https://github.com/cubrid-lab/cubrid-benchmark/tree/main/experiments/pycubrid-executemany-overhead)
experiment (CUBRID 11.4.6, five trials, medians, commit included). The numbers come from a
shared, loaded machine; only the medians and the phase shares are reliable.

| Batch of | `pycubrid` / `CUBRIDdb` INSERT | UPDATE | DELETE | MERGE |
|---|---:|---:|---:|---:|
| 100 rows | 1.13× | 1.13× | 1.13× | 1.49× |
| 1,000 rows | 1.39× | 1.65× | 1.48× | 2.64× |
| 10,000 rows | 1.54× | 1.56× | 1.47× | 4.72× |

- **The gap is on the server, not in the driver.** For 10,000 INSERT rows, 95% of the
  statement phase is waiting for the broker's reply. Rendering parameters into SQL is 4% and
  packet write and parse together are under 1%. The broker SQL log shows the CAS busy for
  2,734 of the 2,865 ms the client measured.
- **One round trip does not make a batch cheaper.** The server spends about 275 µs on each
  literal statement in the batch, against about 131 µs on each execute of a statement
  prepared once. A round trip on loopback costs about 76 µs, so sending 10,000 prepared
  executes still finishes sooner than one batch of 10,000 literal statements.
- **Client optimisation cannot close it.** Caching the placeholder tokenisation and similar
  work would recover at most about 2-4% of the statement phase.
- **MERGE is the worst case.** Literal MERGE statements get few plan-cache hits (548-702 of
  1,000 in the broker log), which is why the batch is up to 4.72× `CUBRIDdb`.
- **Memory:** about 360 bytes per row while the batch is built; it is released after the
  call.

What you can do:

- Keep committing once per batch, not once per row. Commit is already 53% of a 100-row batch
  and identical for both drivers.
- For large MERGE batches, or when a batch is much slower than your own row-by-row
  measurement, consider a prepared loop with the explicit `pycubrid.compat.native` cursor
  (one `PREPARE`, then one execute per row). It reached 0.99-1.31× of `CUBRIDdb` for INSERT,
  UPDATE and DELETE and 1.17× for MERGE at 10,000 rows. The measurement ran with autocommit off
  and one explicit commit per batch; the harness set the driver's autocommit flag directly,
  and the public way to get the same mode is `connection.set_autocommit(False)` followed by
  `commit()`.
  Measure it on your own workload first:
  `bind_param()` takes only scalar INT, string and `NULL` values (collections and LOBs use the
  dedicated `bind_set()` and `bind_lob()` APIs), and the error behaviour differs. A prepared
  loop stops at the first failing row. A batch sends every statement to the server even after
  one fails; `executemany()` raises the first returned error and, since #841, reports every
  failed row on it (`batch_index`, `batch_errors`, `batch_rowcounts`; see
  [API_REFERENCE.md](API_REFERENCE.md#partial-failure-which-rows-failed)).
- Do not use `execute()` in a loop as a faster alternative. It was 1.6-6.1× `CUBRIDdb`'s
  `executemany()`, because every row is a new prepare plus extra requests.

Not established: the gap against `CUBRIDdb` on CUBRID 10.2, 11.0 and 11.2, and how the server
time splits between the CAS and the engine. Prepared batch paths were measured on all four
versions; see the next section. The placeholder tokenisation is now cached per call; see
[below](#placeholder-tokenisation-cached-per-executemany-call).

### Placeholder tokenisation cached per `executemany()` call

Since [#837](https://github.com/cubrid-lab/pycubrid/issues/837), the literal batch path of
`executemany()` splits the SQL on its `?` placeholders once per call instead of once per
parameter set. An exact `tuple` or `list` parameter set also skips the `typing.Sequence`
check; any other type takes the full check. The rendered SQL is byte-identical:
`tests/test_executemany_bind_cache.py` compares 390 statements (every literal type family
including subclasses, IANA time zones and the repeated DST hour, quotes, comments,
identifiers, both escape modes) with SQL that `executemany()` rendered before the change, and
the binding contract in [PARAMETER_BINDING.md](PARAMETER_BINDING.md) is unchanged.

10,000 rows of five parameters (`INT`, `VARCHAR`, `DOUBLE`, `DATETIME`, `NULL`), Python 3.12,
Intel Core i5-9400F, medians. "Offline" times `Cursor.executemany()` with
`_send_and_receive` mocked to answer the batch, as `render_with_cursor` in that test does;
"live" runs against a local Docker server with autocommit off and the commit outside the
CPU measurement:

| Measurement | Before | After |
|---|---:|---:|
| Client-side rendering, offline (server reply mocked) | 145 ms | 38.5 ms |
| Client CPU inside a live `executemany()`, CUBRID 11.4 | 194-198 ms | 52-98 ms |
| Client CPU inside a live `executemany()`, CUBRID 10.2 | 195-196 ms | 85-87 ms |
| `split_on_placeholders` time in a live cProfile | 0.12-0.13 s | 0.000 s |

The wall time of the live statement (about 2-6 s on this shared machine) is dominated by the
server and varied by more than the client gain from run to run, so no wall-clock speedup is
claimed. The `SELECT` per-row loop of `executemany()` still tokenises once per row, because
each row is a separate `execute()`.

### Prepared batch paths: the `EXECUTE_ARRAY` evaluation

[#836](https://github.com/cubrid-lab/pycubrid/issues/836) measured two prepared alternatives to
the literal batch with the same workload, in the
[`pycubrid-prepared-batch`](https://github.com/cubrid-lab/cubrid-benchmark/tree/main/experiments/pycubrid-prepared-batch)
experiment. Autocommit was off, the commit was included, and the medians come from five trials on
11.4 and three on the other versions, on a shared machine:

- a prepared loop with `pycubrid.compat.native`: one `PREPARE`, then one execute per row;
- a prototype of the `EXECUTE_ARRAY` request (function code 21): one `PREPARE`, one request
  carrying every row's bound values, one close. The prototype lived in the benchmark harness;
  pycubrid now sends this request with `executemany(..., prepared=True)` (#839, below).

Time of the `EXECUTE_ARRAY` prototype as a fraction of `executemany()` at 10,000 rows:

| CUBRID | INSERT | UPDATE | DELETE | MERGE |
|---|---:|---:|---:|---:|
| 11.4.6 | 0.40 | 0.33 | 0.35 | 0.15 |
| 11.2.9 | 0.44 | 0.38 | 0.39 | 0.16 |
| 11.0.16 | 0.55 | 0.50 | 0.51 | 0.16 |
| 10.2.18 | 0.53 | 0.51 | 0.51 | 0.61 |

- **`EXECUTE_ARRAY` is the fastest path on every version.** On 11.4 the statement phase for 10,000
  rows falls from 2,887 to 954 ms for INSERT and from 13,020 to 1,739 ms for MERGE. It is also
  0.41-0.54× of the prepared loop for INSERT, UPDATE and DELETE.
- **The prepared loop is not faster everywhere.** It is 0.79-1.06× of `executemany()` on 11.4 and
  11.2, but 1.13-1.22× on 11.0 and 10.2 for INSERT, UPDATE and DELETE. MERGE is faster with it on
  every version.
- **Transactions, partial failures and `rowcount` match the current batch.** On all four versions,
  when one row fails `EXECUTE_ARRAY` still executes the other rows, as `executemany()` does today.
  Only the failing row is undone. With autocommit on, each successful row is committed. With
  autocommit off, `rollback()` removes every row and `commit()` keeps the successful ones. The
  per-row counts and their sum are identical, and both paths report the same failed rows on
  the raised exception (#841). The prepared loop stops at the first failing row instead.
- **Types and coercion differ.** At the time of #836 the prepared encoders took only a 32-bit
  `int`, `str`, `None`, INT/STRING collections and LOB handles; #840 widened them to every scalar
  type the literal path renders. Values are coerced as host variables, not literals, so server
  error text differs. With autocommit on, a LOB handle fails after the first row's commit.

Decision: `executemany()` keeps the literal batch by default, because a default switch would change
which values are accepted and how they are converted. The `EXECUTE_ARRAY` path ships as an opt-in,
`executemany(..., prepared=True)`
([#839](https://github.com/cubrid-lab/pycubrid/issues/839)), on the wider prepared type set of
[#840](https://github.com/cubrid-lab/pycubrid/issues/840). Since
[#841](https://github.com/cubrid-lab/pycubrid/issues/841) the raised exception reports every failed
row on both paths; see [API_REFERENCE.md](API_REFERENCE.md#partial-failure-which-rows-failed).

<a id="shipped-prepared-batch"></a>

#### The shipped opt-in: `executemany(..., prepared=True)`

The same comparison through the driver itself, after #839: 10,000 rows of a three-column table
with a primary key, autocommit off, commit included, median of three trials (order alternated),
one container per version on a shared machine (one-minute load average 2.1-2.6). Times are the
literal `executemany()` and then `prepared=True`, in milliseconds:

| CUBRID | INSERT | UPDATE | DELETE | MERGE |
|---|---:|---:|---:|---:|
| 11.4.6 | 3,044 → 1,159 (0.38) | 3,141 → 1,121 (0.36) | 3,116 → 1,117 (0.36) | 10,047 → 2,012 (0.20) |
| 11.2 | 2,942 → 1,300 (0.44) | 3,615 → 1,196 (0.33) | 5,042 → 1,191 (0.24) | 17,708 → 2,164 (0.12) |
| 11.0 | 1,876 → 1,067 (0.57) | 2,131 → 1,094 (0.51) | 1,817 → 1,125 (0.62) | 8,979 → 1,860 (0.21) |
| 10.2 | 2,054 → 1,102 (0.54) | 2,384 → 1,090 (0.46) | 2,092 → 984 (0.47) | 10,426 → 1,842 (0.18) |

The ratios match the prototype for INSERT, UPDATE and DELETE. MERGE gains more than the prototype
measured on 10.2 (0.18 against 0.61) with this statement shape (`MERGE ... USING db_root ON
(t.id = CAST(? AS INT))` on a primary key). The literal 11.2 DELETE time is noisier than the
others. The integration tests (`tests/test_integration_execute_array.py`) check the transaction,
partial-failure, `rowcount`, NULL, mixed-type, collection and LOB behaviour on all four versions.
See [API_REFERENCE.md](API_REFERENCE.md) for the contract.

### Fetch throughput and peak memory by `fetch_size`

10,000 rows over localhost:

| Method | `fetch_size` 100 | 1,000 | 10,000 | Peak Python heap (`fetch_size` 100) |
|---|---:|---:|---:|---:|
| `fetchone()` loop | 51.2 ms | — | — | 130 KiB |
| `fetchmany(100)` loop | 48.4 ms | 49.0 ms | 49.5 ms | 132 KiB |
| `fetchall()` | 49.7 ms | 48.9 ms | 48.1 ms | 2.1 MiB (the result list) |

About 200,000 rows/s in every case. Over localhost, `fetch_size` makes no measurable
difference; it may matter on a network with higher latency, which was not measured. Peak memory
was measured only at the default `fetch_size` of 100.

### Not measured

- Async for `CUBRIDdb`, which has no async API.
- A `fetch_size` sweep for `CUBRIDdb`, which does not expose a fetch size.
- Native (C) memory of `CUBRIDdb`. `tracemalloc` only sees the Python heap.
- Peak memory at `fetch_size` 1,000 and 10,000.
- Per-operation RSS. Only the process peak was recorded (about 39 MiB).
- `executemany()` against `CUBRIDdb` on CUBRID versions other than 11.4, and the split of
  `executemany()` server time between the CAS and the engine.
- The extra `lastrowid` round trip.
- Remote networks, TLS, LOB and collection types.
- Python versions other than 3.12, and CUBRID versions other than 11.4 (except the prepared batch
  evaluation, which ran on 10.2-11.4).

### Historical Results (2026-03, pycubrid 0.5.0)

The earliest published `pycubrid` measurements are kept, unchanged, in the
[`baseline-multilang` experiment](https://github.com/cubrid-lab/cubrid-benchmark/tree/main/experiments/baseline-multilang)
of `cubrid-benchmark` (run `2026-03-16_initial`: `pycubrid` 0.5.0, CPython 3.10.12,
CUBRID 11.2 in Docker).

They are **historical** and are not a baseline for the current release. Known limitations:

- They predate the cursor memory-bounding fix
  ([#203](https://github.com/cubrid-lab/pycubrid/issues/203),
  [PR #207](https://github.com/cubrid-lab/pycubrid/pull/207), released in 1.6.0). Before that
  fix, `Cursor` and `AsyncCursor` kept the whole result set in their row buffer, so fetch
  numbers from that period do not reflect current behaviour.
- They predate the fetch fix released in 0.6.0 (commit `bb687dc`). In 0.5.0, `fetchall()`
  returned only the first fetch batch, so SELECT scenarios that read more than one batch did
  not read the whole result.
- That experiment compares two database engines. This guide does not reproduce it, for the
  reason given at the top of the page.

---

## Performance Regression

The weekly **Bug Hunt** workflow (`.github/workflows/bug-hunt.yml`, job `perf-trend`) runs
`tests/test_benchmarks.py` against CUBRID 11.4 and compares the medians with the previous run
using `scripts/bench_regression.py`. It is a trend report: it does not set a fail threshold and
does not gate pull requests or releases.

The release-to-release comparison is published in
[Current Release Baseline](#current-release-baseline): `main` against 1.10.0.

Investigate when:

- The trend report shows a median increase over the previous run.
- A CI run flags a deviation from the previous run's numbers.
- You are about to submit a change to the hot path (protocol.py, packet.py, cursor.py).

### Comparing Two Runs

```bash
# Run the live microbenchmarks on the old and the new code:
pytest tests/test_benchmarks.py --benchmark-enable --benchmark-json=before.json
pytest tests/test_benchmarks.py --benchmark-enable --benchmark-json=after.json

# Report the median change per benchmark (add --fail-threshold PCT to fail on a regression):
python scripts/bench_regression.py --baseline before.json --current after.json
```

### Fetch Reply Parsing (Offline)

`tests/test_bench_fetch_parsing.py` times FC8 FETCH reply parsing with no
server (#559). It covers 2000-row synthetic replies for scalar, text, mixed
(with NULLs) and collection workloads, built with the fuzz seed builders, and
checks every parse against their exact expected rows. Without
`--benchmark-enable` each workload is parsed once as a correctness test, so
required CI has no timing threshold.

```bash
# Time it (30 rounds after 3 warmup rounds), with peak allocation in extra_info:
pytest tests/test_bench_fetch_parsing.py --benchmark-enable \
    --benchmark-json=fetch-parse.json

# Compare two runs:
python scripts/bench_regression.py --baseline before.json --current after.json
```

---

## Sync and Async Characteristics

- `pycubrid.connect()` returns a blocking `Connection`; `pycubrid.aio.connect()` returns an
  `AsyncConnection` that awaits network I/O instead of blocking.
- Both build and parse the same CAS packets, so the per-request encode/decode work is the same.
- An `AsyncConnection` serialises its requests with an `asyncio.Lock`: one connection runs one
  request at a time. Async helps when many connections or other I/O run concurrently on one
  event loop, not when a single connection runs one query after another.
- Async timings from the [timing hooks](#timing--profiling-hooks) include event-loop scheduling
  latency.

Measured sync and async throughput is in [Sync against async](#sync-against-async).

---

## Batch Processing

`executemany()` on a sync or async cursor uses a batch path when the statement starts with
`INSERT`, `UPDATE`, `DELETE` or `MERGE`:

1. The driver renders every parameter set into a complete SQL string on the client (see
   [Parameter Binding](PARAMETER_BINDING.md)).
2. It sends all the strings in a single `BatchExecutePacket`, so the whole batch takes one
   round trip instead of one per row.
3. `rowcount` is the sum of the affected rows.

Other statements, such as `SELECT`, fall back to calling `execute()` once per parameter set.
With `prepared=True`, an INSERT, UPDATE, DELETE or MERGE is instead sent as one `PREPARE` and one
`EXECUTE_ARRAY` request with typed values, which takes about a third to a half of the literal
batch's time (a fifth or less for MERGE); see
[the shipped opt-in](#shipped-prepared-batch).
`executemany_batch(sql_list)` sends a list of SQL strings you have already rendered in the same
single request.

Practical advice:

- Group a write burst in one explicit transaction instead of committing per statement.
- Split very large parameter sequences into chunks; see
  [Known Limitations](#known-limitations) for why.

Measured batch throughput is in [`executemany()` batch throughput](#executemany-batch-throughput).
How the batch compares with `CUBRIDdb`, and why it can be slower, is in
[where the gap comes from](#executemany-against-cubriddb-where-the-gap-comes-from).

---

## Fetch and Memory Performance

`fetch_size` sets how many rows the driver asks the CAS broker for in each fetch round trip.
It defaults to `100`. Set it per connection with `pycubrid.connect(..., fetch_size=N)` or per
cursor with `cursor.fetch_size = N` (an integer of at least 1).
`fetch_size` is not `arraysize`: `arraysize` is only the default row count of `fetchmany()`.

Since 1.6.0 ([#203](https://github.com/cubrid-lab/pycubrid/issues/203)), each fetch replaces
the cursor's row buffer instead of adding to it, so the buffer holds at most one fetch batch:

- `fetchone()`, `fetchmany()` and iteration keep memory bounded by `fetch_size`.
- `fetchall()` builds one list with every remaining row, so its memory grows with the result
  set. It releases the internal buffer when it finishes.
- A larger `fetch_size` means fewer round trips and more memory per batch; a smaller one means
  the reverse.

Measured fetch throughput and peak memory for different `fetch_size` values are in
[Fetch throughput and peak memory by `fetch_size`](#fetch-throughput-and-peak-memory-by-fetch_size).

### Profiling Fetch

`scripts/profile_fetch.py` profiles `fetchone`, `fetchmany` and `fetchall` against a live
server.

```bash
# 1000 rows, 50 fetch iterations (default):
python scripts/profile_fetch.py

# 5000 rows, 20 iterations, fetchmany batch size 100:
python scripts/profile_fetch.py --rows 5000 --iterations 20 --fetch-size 100

# Save .prof for snakeviz:
python scripts/profile_fetch.py --output fetch.prof
```

---

## Profiling and Optimization

### Optimization Tips

- Use explicit transactions for write bursts instead of per-statement commits.
- Batch inserts and updates with `executemany()` where possible.
- Reuse long-lived connections to avoid repeated handshake cost.
- Select only required columns and avoid unnecessary full scans.
- Keep hot predicates indexed and validate plans in CUBRID.

```mermaid
flowchart TD
    Start[Slow query path] --> Batching{Batchable workload?}
    Batching -->|Yes| ExecMany[Use executemany / multi-row patterns]
    Batching -->|No| Index{Index coverage good?}
    Index -->|No| AddIdx[Add or tune index]
    Index -->|Yes| Txn{Too many commits?}
    Txn -->|Yes| GroupTxn[Group statements in one transaction]
    Txn -->|No| Net[Profile network and CAS round-trips]
```

### Performance Investigation

Use this workflow when a benchmark detects a measurable regression. The goal is to reproduce,
profile, fix, and verify — without hardcoding thresholds that age badly.

```mermaid
flowchart TD
    Detect[Benchmark detects a regression] --> Issue[File a Performance issue\nusing the issue template]
    Issue --> Profile[Run profiling scripts\nto isolate the hot path]
    Profile --> Optimize["Apply targeted fix\n(see Optimization Tips)"]
    Optimize --> Verify[Re-run profiling scripts\nand benchmarks]
    Verify --> Close[Attach results to issue\nand close]
```

1. **File an issue** — use the
   [Performance Investigation template](../.github/ISSUE_TEMPLATE/performance.yml).
   Paste the benchmark output and link the CI run that triggered this.

2. **Profile the affected operation** — pick the script that matches the slow operation:

   | Operation | Script |
   |-----------|--------|
   | Connection handshake | `scripts/profile_connect.py` |
   | INSERT / SELECT / UPDATE / DELETE | `scripts/profile_execute.py` |
   | Row fetching (fetchone/fetchall/fetchmany) | `scripts/profile_fetch.py` |

3. **Optimise** — guided by cProfile's cumulative time, focus changes on the top frames.
   Keep patches targeted; avoid speculative refactors.

4. **Verify** — re-run the profiling script and the benchmarks.
   Attach before/after numbers to the issue.

### Running the Profiling Scripts

All scripts require a live CUBRID instance. Defaults target `localhost:33000/demodb` with
user `dba`. For `scripts/profile_fetch.py`, see [Profiling Fetch](#profiling-fetch).

#### Connection handshake

```bash
# 100 connect/close cycles (default):
python scripts/profile_connect.py

# Custom target, 50 iterations, save .prof:
python scripts/profile_connect.py \
    --host myhost --port 33000 --database testdb \
    --user dba --password secret \
    --iterations 50 --output connect.prof
```

#### Statement execution

```bash
# All DML operations, 100 iterations each (default):
python scripts/profile_execute.py

# INSERT only, 200 iterations:
python scripts/profile_execute.py --operation insert --iterations 200

# Save .prof for snakeviz:
python scripts/profile_execute.py --output exec.prof
```

#### Visualising .prof files with snakeviz

```bash
pip install snakeviz
snakeviz profile_output.prof
```

snakeviz opens an interactive flame graph in the browser, making it easy to drill into
nested call stacks.

### Timing & Profiling Hooks

For lightweight in-process diagnosis you can opt into the driver's built-in timing
instrumentation instead of running the cProfile-based scripts above. Hooks are **off by
default** — when disabled the timing module is never imported and the hot path runs
unchanged.

#### When to use which

| Use case | Tool |
|---|---|
| "Where is wall-clock time going across `connect` / `execute` / `fetch` / `close` in my application?" | `enable_timing=True` (this section) |
| "Which Python frames inside `cursor.execute` are hot?" | `scripts/profile_execute.py` (cProfile) |
| "Did this change make the driver slower than the previous run?" | [Performance Regression](#performance-regression) |

#### Enabling

Pass the `enable_timing=True` keyword to `pycubrid.connect()`:

```python
import pycubrid

conn = pycubrid.connect(
    host="localhost", port=33000, database="testdb", user="dba",
    enable_timing=True,
)
```

Or set the environment variable so timing is enabled for every connection in a process —
useful in benchmark harnesses and CI jobs:

```bash
export PYCUBRID_ENABLE_TIMING=1   # also accepts true / yes (case-insensitive)
python my_workload.py
```

The explicit keyword always wins over the environment variable. Async connections support
the same keyword on `pycubrid.aio.connect()`.

#### Reading the stats

```python
cur = conn.cursor()
cur.executemany(
    "INSERT INTO bench (n) VALUES (?)",
    [(i,) for i in range(1000)],
)
cur.execute("SELECT n FROM bench")
cur.fetchall()

stats = conn.timing_stats
print(stats)
# TimingStats(connect=1 calls, 12.345ms total, 12.345ms avg,
#             execute=2 calls, 18.700ms total, 9.350ms avg,
#             fetch=1 calls, 4.200ms total, 4.200ms avg,
#             close=0 calls)

# Programmatic access (nanoseconds, ints):
exec_avg_ms = stats.execute_total_ns / stats.execute_count / 1_000_000
print(f"average execute: {exec_avg_ms:.3f} ms")

# Reset between phases
stats.reset()
```

`Connection.timing_stats` is `None` when timing is disabled, so guard accordingly:

```python
if conn.timing_stats is not None:
    print(conn.timing_stats)
```

#### Categories and granularity

| Category | What it covers |
|---|---|
| `connect` | TCP socket setup + CAS broker handshake + database open. Recorded even on failure. |
| `execute` | `Cursor.execute()` and `executemany()` — wraps prepare-and-execute round-trip. |
| `fetch` | `fetchone()` / `fetchmany()` / `fetchall()` combined. |
| `close` | `Connection.close()` — `CloseDatabasePacket` round-trip + socket teardown. |

All cursors created from a connection report into the same `TimingStats`. Stats are
**per-connection**, cumulative since the last `reset()`.

#### Overhead and thread-safety

- **Disabled** — the timing module is not imported; per-call cost is a single attribute
  read (`self._timing is None`).
- **Enabled** — two `time.perf_counter_ns()` calls plus a lock-protected accumulator
  update per hook (~hundreds of nanoseconds).
- The `threading.Lock` inside `TimingStats` lets a monitoring thread safely read counters
  while a worker thread drives the connection. The connection itself remains
  `threadsafety = 1` (one connection per thread).

#### Timing limitations

- Async timings include event-loop scheduling latency; treat them as client-side
  end-to-end latency, not pure server time.
- Counters are cumulative only — there is no per-statement history. If you need
  per-statement breakdowns, run the cProfile-based scripts in
  [Performance Investigation](#performance-investigation).
- `ping()` and `commit()` / `rollback()` are not currently timed.

---

## Known Limitations

- **Pure Python.** Packet encode/decode and row conversion run in the interpreter; there is no
  C extension.
- **`executemany()` holds the whole batch in memory.** The batch path renders every parameter
  set into a SQL string before sending, then serialises all of them into one request. Client
  memory grows with the number and size of the parameter sets; split very large sequences into
  chunks.
- **`fetchall()` materialises the result.** Use `fetchone()`, `fetchmany()` or iteration for
  large result sets.
- **Async timing includes the event loop.** See
  [Sync and Async Characteristics](#sync-and-async-characteristics).
- **The published 2026-03 numbers are [historical](#historical-results-2026-03-pycubrid-050).**
  Use the [Current Release Baseline](#current-release-baseline) instead.

---

## Reproduction Guide

1. Start a CUBRID server. The repository's `docker compose up -d` starts one on
   `localhost:33000` with database `testdb` (see [Development](DEVELOPMENT.md)).
2. Run the microbenchmarks on each version you compare, on the same host and server, and save
   the JSON output.
3. Compare the runs with `scripts/bench_regression.py`.
4. For larger workloads, or to compare `pycubrid` with `CUBRIDdb` on the same server, use the
   [cubrid-benchmark](https://github.com/cubrid-lab/cubrid-benchmark) harness. Set
   `autocommit` explicitly for every driver.
5. Report the result in a [Performance issue](../.github/ISSUE_TEMPLATE/performance.yml) with
   the environment listed under [Benchmark Methodology](#benchmark-methodology).

```bash
docker compose up -d
export CUBRID_TEST_URL="cubrid://dba@localhost:33000/testdb"
pytest tests/test_benchmarks.py --benchmark-enable --benchmark-json=current.json
python scripts/bench_regression.py --baseline baseline.json --current current.json
```
