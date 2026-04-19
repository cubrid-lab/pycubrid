# Roadmap

> **Last updated**: 2026-04-19
>
> This roadmap reflects current priorities. For the ecosystem-wide view, see the
> [CUBRID Labs Ecosystem Roadmap](https://github.com/cubrid-labs/.github/blob/main/ROADMAP.md).

## Links

- 📋 [GitHub Milestones](https://github.com/cubrid-labs/pycubrid/milestones)
- 🗂️ [Org Project Board](https://github.com/orgs/cubrid-labs/projects/2)
- 🌐 [Ecosystem Roadmap](https://github.com/cubrid-labs/.github/blob/main/ROADMAP.md)

## Next Release — v1.2.0 — Type Safety & Protocol

- Server-side parameter binding via PREPARE + EXECUTE (experimental opt-in)
- Async LOB read/write support in `pycubrid.aio`
- Connection pool integration patterns

## Future

- Full CUBRID 12.x support
- Connection pool implementation
- Prepared statement caching

## Compatibility

Python 3.10+, CUBRID 10.2–11.4

## Completed

### Type Safety & Protocol (v1.2.0-dev)
- Native `Connection.ping()` via CHECK_CAS (FC=32)
- `errno`/`sqlstate` on all `DatabaseError` subclasses
- JSON type decoding (opt-in, protocol v8)
- Collection type decoding: SET/MULTISET/SEQUENCE (opt-in)
- Hardened parameter binding security

### Async Support (v1.1.0)
- Native asyncio API via `pycubrid.aio` module
- `AsyncConnection` and `AsyncCursor` with full async/await support
- Non-blocking socket I/O using `loop.sock_*`
