# Live microbenchmark reference run: `micro-explicit-keys-v2`

The first run of the corrected `tests/test_benchmarks.py` workload (#922). These files are
kept unchanged; record a later run in a new directory.

| | |
|---|---|
| Workload revision | `micro-explicit-keys-v2` (each result's `extra_info.workload_revision`) |
| Source | pycubrid commit `b7f94d0640` on the #922 pull request branch, clean tree (`commit_info` in each file) |
| Date | 2026-10-14 |
| Host | Intel Core i5-9400F, 6 cores, Linux 5.15, one host for client and server |
| Python | CPython 3.12.13, pytest-benchmark 5.3.0 |
| CUBRID 11.4 | 11.4.6.1963, `cubrid/cubrid@sha256:1248b77ad39888df9e064655937188ab99fe056ed7e35b57d06155a015dc1791` |
| CUBRID 10.2 | 10.2.18.9024, `cubrid/cubrid@sha256:fee7c63b57159bed0827588b5678c611beec6dac07633cf3e13729247456b02f` |
| Server | one Docker container per version, database `testdb`, default configuration |

Command, once per server:

```bash
docker run -d --name pycubrid922-cubrid-114 -e CUBRID_DB=testdb -p 34114:33000 cubrid/cubrid:11.4
CUBRID_TEST_URL=cubrid://dba@localhost:34114/testdb \
    pytest tests/test_benchmarks.py --benchmark-enable --benchmark-json=cubrid-11.4.json
```

Medians in milliseconds (one timed call; rounds after warm-up rounds):

| Benchmark | Rounds | CUBRID 11.4 | CUBRID 10.2 |
|---|---:|---:|---:|
| `test_bench_connect_disconnect` | 20 + 2 | 1.546 | 1.660 |
| `test_bench_insert_one_row` | 100 + 5 | 52.665 | 54.418 |
| `test_bench_select_one_row_by_pk` | 100 + 5 | 4.559 | 3.458 |
| `test_bench_update_one_row_by_pk` | 100 + 5 | 52.465 | 54.221 |
| `test_bench_delete_one_row_by_pk` | 100 + 5 | 60.540 | 54.248 |
| `test_bench_insert_100_rows` | 10 + 1 | 573.080 | 383.484 |
| `test_bench_select_100_rows` | 50 + 5 | 5.736 | 4.694 |
| `test_bench_execute_parameterized_loop_100` | 10 + 1 | 473.129 | 369.008 |

Every round of every CRUD benchmark, warm-up rounds included, passed its row check
(`extra_info.verified_rounds`). The single-row write cases are dominated by the server's
commit on this host. Compare only with a run of the same workload revision on the same host and server; the
two server columns are not a version comparison.
