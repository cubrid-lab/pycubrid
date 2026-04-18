# Roadmap

> **Last updated**: 2026-03-20
>
> This roadmap reflects current priorities. For the ecosystem-wide view, see the
> [CUBRID Labs Ecosystem Roadmap](https://github.com/cubrid-labs/.github/blob/main/ROADMAP.md).

## Links

- 📋 [GitHub Milestones](https://github.com/cubrid-labs/pycubrid/milestones)
- 🗂️ [Org Project Board](https://github.com/orgs/cubrid-labs/projects/2)
- 🌐 [Ecosystem Roadmap](https://github.com/cubrid-labs/.github/blob/main/ROADMAP.md)

## Next Release — v0.6.0 — Performance

- Profile and optimize hot paths in CAS protocol parsing
- Address 4.5–6× performance gap vs MySQL drivers
- Optimize data type conversion and serialization
- Add connection pool benchmarks

## Future — v1.0.0 — Stable Release

- Full PEP 249 compliance verification
- API freeze and stability guarantees
- Comprehensive error handling and connection pooling
- Published to PyPI via Trusted Publisher

## Compatibility

Python 3.10+, CUBRID 10.2–11.4

## Completed

### Async Support (v1.1.0)
- Native asyncio API via `pycubrid.aio` module
- `AsyncConnection` and `AsyncCursor` with full async/await support
- Non-blocking socket I/O using `loop.sock_*`
