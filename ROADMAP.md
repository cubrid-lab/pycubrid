# Roadmap

> **Last updated**: 2026-10-04
>
> This roadmap reflects current priorities. For the ecosystem-wide view, see the
> [CUBRID Labs Ecosystem Roadmap](https://github.com/cubrid-lab/.github/blob/main/ROADMAP.md).

## Reviewed backlog priorities

The [dated priority review (#566)](https://github.com/cubrid-lab/pycubrid/issues/566)
records execution order and historical verification. Prioritize reproduced
correctness failures as high, bounded verification and release preparation as
medium, and optional compatibility extensions as low. Size estimates effort
independently of urgency. Preserve existing contributor ownership and use one
focused acceptance contract per PR.

The remaining larger work in the 2026-10-04 snapshot is:

| Order | Issue | Priority / size | Completion boundary |
|---|---|---|---|
| 1 | [#634](https://github.com/cubrid-lab/pycubrid/issues/634) — release-please integration audit | medium / L | Migration is integrated; reviewed candidate and publisher/recovery evidence remain separate. |
| 2 | [#336](https://github.com/cubrid-lab/pycubrid/issues/336) — adversarial verification audit | medium / XL | Reconcile actual runs, measurements and known limitations; passing child PRs do not complete the parent. |
| 3 | [#610](https://github.com/cubrid-lab/pycubrid/issues/610) — wrapper collection call shapes | low / M | Decide and verify the explicit compatibility contract before implementation. |
| 4 | [#678](https://github.com/cubrid-lab/pycubrid/issues/678) — positive-only fetch-size migration | low / M | Decide a major-release migration while preserving the shipped nonpositive-integer contract in 1.x. |

Small release-preparation work is tracked by
[#371](https://github.com/cubrid-lab/pycubrid/issues/371) (fetch-size validation)
and [#327](https://github.com/cubrid-lab/pycubrid/issues/327) (runnable README
examples). Their issue/PR records carry completion evidence.
[#675](https://github.com/cubrid-lab/pycubrid/issues/675), the unfiled upstream
CUBRID crash report, is excluded from this release-preparation scope and remains
open. This snapshot is an index, not a claim that all issues are complete or that
a release has passed its publication gates. Use the
[live open issue list](https://github.com/cubrid-lab/pycubrid/issues?q=is%3Aissue+is%3Aopen)
for current status; dependencies govern completion, not investigation.

## Python 3.10 retirement schedule

- Upstream Python 3.10 support ended on 2026-10-01 ([PEP 619](https://peps.python.org/pep-0619/#310-lifespan)).
- 1.8.x and 1.9.x: Python >=3.10 is the installation requirement.
- 1.9.0 (published 2026-10-04): carried the advance notice in CHANGELOG Deprecated
  and the support matrix; Python 3.10 stays supported throughout the 1.9.x line.
- Done on `main` for the next minor release (planned 1.10.0): package metadata
  requires Python >=3.11, the 3.10 classifier is removed and CI matrices start at
  3.11 (#684). Tooling targets, 3.10-only code and the remaining documentation
  follow in the child issues of #682.
- Users should migrate Python and recreate/test their virtual environment before
  upgrading to the removal release. No release date or publication is claimed.

## Python 3.15 support preparation

- As of 2026-10-03, 3.15.0rc3 is a preview; final is scheduled for 2026-10-09
  ([upstream release notes](https://www.python.org/downloads/release/python-3150rc3/)).
- Add one manually dispatched Ubuntu/standard-GIL preview lane for full offline
  regressions, distribution validation and fresh wheel/sdist installations.
  Routine PRs do not gain a matrix cell or automatic preview run.
- Before official support: record successful final-runtime/dependency, packaging,
  offline and real CUBRID integration results at the candidate SHA.
- Then align the Python 3.15 classifier, full release matrix, local tooling where
  present, release notes and English/Korean supported-version docs in a follow-up.
  This preparation changes no official support declaration or Python minimum.

## Links

- 📋 [GitHub Milestones](https://github.com/cubrid-lab/pycubrid/milestones)
- 🗂️ [Org Project Board](https://github.com/orgs/cubrid-lab/projects/2)
- 🌐 [Ecosystem Roadmap](https://github.com/cubrid-lab/.github/blob/main/ROADMAP.md)

## Current Baseline

Current source version: [`pycubrid.__version__`](pycubrid/__init__.py).
Published releases and dated history: [CHANGELOG](CHANGELOG.md).

- Stable sync DB-API 2.0 surface plus native asyncio API (`pycubrid.aio`)
- Current async transport uses `asyncio.open_connection()` with
  `StreamReader`/`StreamWriter`; see [`aio/connection.py`](pycubrid/aio/connection.py).
- JSON / collection decoding, `ping()`, `nextset()`, and sync + async TLS (TLS 1.2 minimum)
- 1.x release policy enforced by an automated `compat-check` CI gate against `api-baseline.json`
- Supported runtimes: Python 3.11–3.14 (3.10 until 1.9.x), CUBRID 10.2–11.4

_See **Completed** for the per-release milestone history._

## Future

- Full CUBRID 12.x support
- Higher-level LOB helpers for fetched handles
- Prepared statement caching

## Compatibility

Python 3.11+ (3.10 until 1.9.x), CUBRID 10.2–11.4

## Completed

### Release Policy & Runtime Coverage (v1.5.0 / v1.6.2)
- 1.x release policy with an automated `compat-check` CI gate against `api-baseline.json` (v1.5.0)
- Python 3.10 async-TLS preflight verification probe (v1.5.0)
- Python 3.14 support validated in CI (v1.6.2 baseline)

### Type Safety & Protocol (v1.2.0)
- Native `Connection.ping()` via CHECK_CAS (FC=32)
- `errno`/`sqlstate` on all `DatabaseError` subclasses
- JSON type decoding (opt-in, protocol v8)
- Collection type decoding: SET/MULTISET/SEQUENCE (opt-in)
- Hardened parameter binding security

### Async Support (v1.1.0)
- Native asyncio API via `pycubrid.aio` module
- `AsyncConnection` and `AsyncCursor` with full async/await support
- Non-blocking socket I/O using `loop.sock_*`

### TLS & Transport (v1.3.0 / v1.4.0)
- Sync TLS via `ssl=True` or custom `ssl.SSLContext` (v1.3.0)
- Async TLS via `pycubrid.aio.connect(ssl=...)` using CUBRID's STARTTLS-style upgrade (plaintext `CUBRS` handshake → `loop.start_tls()`; v1.4.0, corrected in #154)
- Default TLS context enforces TLS 1.2 minimum (v1.4.0)
- Expanded Python 3.14 / CUBRID 10.2–11.4 CI coverage
