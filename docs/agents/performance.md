# Performance guide

> Moved from the root [AGENTS.md](../../AGENTS.md) (#926). Owner: pycubrid maintainers.
> Every rule in the root file still applies. Scope: Where performance and roadmap priorities live, and the driver-focused performance goals.

## Performance and Roadmap Pointers

Performance work is tracked in issues, not in this file. Before treating any
performance or roadmap item as current priority, check the live sources:

- [ROADMAP.md](../../ROADMAP.md) — public roadmap and current project baseline (the
  linked org project board may require cubrid-lab membership).
- [docs/PERFORMANCE.md](../PERFORMANCE.md) — benchmark methodology, regression
  checks, profiling scripts, timing hooks and how to reproduce a measurement.
- [cubrid-benchmark](https://github.com/cubrid-lab/cubrid-benchmark) — the
  benchmark harness and its published results. Its 2026-03 results are historical
  (pycubrid 0.5.0) and compare database engines; do not use them as a target.

Performance goals are driver-focused and measured on the same CUBRID server:

- **Release-to-release regression** — the current pycubrid release must not be
  slower than the previous release on the same workload.
- **Overhead vs the official `CUBRIDdb` driver** — measure and reduce the gap
  between pycubrid and `CUBRIDdb` against the same server.

Do not use a comparison with another database engine or its driver as a
performance goal. Do not copy benchmark ratios, phase plans or issue tables into
this file.
