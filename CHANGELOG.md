# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/).

## [Unreleased]

### Fixed
- **Backslash-escape mode is now auto-negotiated from the server, fixing silent string corruption (#255)** — CUBRID's `no_backslash_escapes` system parameter defaults to `yes` (a backslash is an ordinary literal character), but pycubrid defaulted its client-side flag to `False` and unconditionally doubled backslashes. Against a stock server this silently corrupted data: `C:\temp\file` (12 chars) was stored as `C:\\temp\\file` (14 chars), and `regex \d+` became `regex \\d+`. `Connection`/`AsyncConnection` now probe the live server once at connect time with `SELECT CHAR_LENGTH('\\')` when `no_backslash_escapes` is not passed explicitly: a result of `2` selects literal mode (`True`, no doubling), `1` selects escape-processing mode (`False`), and any other value or a probe error raises `OperationalError` (see below). Passing `no_backslash_escapes=True|False` explicitly skips the probe. LIKE metacharacters (`%`, `_`) were never escaped and remain untouched. See [docs/PARAMETER_BINDING.md](docs/PARAMETER_BINDING.md#escape-mode-negotiation).

### Changed
- **Ruff lint rule selection now declared explicitly (#247)** — `pyproject.toml` configured ruff but never set `[tool.ruff.lint] select`, so `ruff check` inherited ruff's implicit defaults. Ruff expanded that default set in 0.16 (59 → 413 rules against this repo's config), which is why #245 (`0.15.22 → 0.16.1`) failed lint with 237 errors in untouched code. Pinning the ruff *version* in #236 stopped unpinned installs from drifting, but could not survive the bump itself — the rule set is now pinned too, via `select = ["E4", "E7", "E9", "F"]`, which is exactly what ruff selected by default through 0.15.x (same 59 rules under both versions).
- **Backslash-escape negotiation now fails loud instead of silently defaulting to `False` (#263)** — when the `SELECT CHAR_LENGTH('\\')` probe raises or returns an unexpected length (neither `1` nor `2`), `Connection`/`AsyncConnection` previously set `no_backslash_escapes=False` (the legacy value) and logged a warning. Because the CUBRID default maps to `True`, that silent fallback could pick the *wrong* escaping mode and corrupt string literals (or enable SQL injection). Negotiation failure now raises `OperationalError`; pass `no_backslash_escapes` explicitly to skip detection when the probe cannot run. **Behavior change:** connections that previously succeeded with a mis-detected mode now raise at connect time.

## [1.6.2] - 2026-08-06

### Fixed
- **Write-path serialization overflow now raises `DataError`, not raw `struct.error` (#223)** — `_send_and_receive` (sync) and `_do_send_and_receive` (async) called `packet.write(self._cas_info)` inside a block that only caught `OSError`. An oversized outbound value (e.g. a huge LOB offset/length, or a large `executemany` batch) trips `struct.pack`'s int32 range check and raised a bare `struct.error`, escaping the PEP 249 contract entirely — the write-side counterpart of the parse-side hardening already done in #201/#205. Both `packet.write()` call sites now catch `struct.error` and raise `DataError("parameter value too large to serialize into CAS request")`; since nothing was sent to the socket yet, the connection is left open and usable rather than torn down.
- **`AsyncConnection(autocommit=True)` no longer silently dropped (#224)** — constructing `AsyncConnection` directly (bypassing the `pycubrid.aio.connect()` factory) with `autocommit=True` swallowed the flag into `**kwargs` with no error and no effect, unlike sync `Connection`, which has always accepted `autocommit` in its own constructor. `AsyncConnection.__init__` now accepts a keyword-only `autocommit: bool = False` parameter, applied via `await self.set_autocommit(True)` the first time `connect()` completes. The `pycubrid.aio.connect()` factory no longer needs its own separate `set_autocommit()` call — `autocommit` just flows straight through to the constructor now.
- **`Decimal('NaN')`/`Decimal('Infinity')` now rejected like their `float` equivalents (#225)** — `format_parameter()`'s `Decimal` branch returned `str(value)` unconditionally, before ever reaching the NaN/Inf guard that already protects the `int`/`float` branch below it. A `Decimal('NaN')` or `Decimal('Infinity')` parameter was silently formatted as the bare token `NaN`/`Infinity` and sent straight to the server instead of raising the documented `ProgrammingError("nan and inf are not supported by CUBRID")`. The `Decimal` branch now checks `value.is_nan() or value.is_infinite()` first.
- **NUMERIC field parsing no longer leaks `decimal.InvalidOperation` (#231)** — `PacketReader._parse_numeric` constructed a `Decimal` straight from the wire string with no guard. An empty or corrupt NUMERIC field raised `decimal.InvalidOperation` (an `ArithmeticError`, not a `ValueError`), slipping past the malformed-response handlers in both sync and async `_send_and_receive`. This left the socket open and desynced for reuse. `_parse_numeric` now catches `InvalidOperation` and re-raises as `ValueError`, so parse errors flow through the existing handler: socket closed, `_connected = False`, `OperationalError` raised with cause chained.
- **CI lint now uses pinned ruff version (#236)** — the lint job used `pip install ruff` (unpinned), which installed the latest ruff. When ruff 0.16.x introduced new rules, every PR started failing lint. Now installs from `.[dev]` extras to match the pinned `ruff==0.15.22` in `pyproject.toml`.

## [1.6.1] - 2026-07-18

### Fixed
- **`executemany()` batch error handling (#186)** — `executemany_batch()` in both sync `Cursor` and `AsyncCursor` consumed `packet.results` but never checked `packet.errors`, silently swallowing per-statement batch failures. Partial failures (e.g. one INSERT in a batch of 10 hits a unique constraint violation) were invisible to the caller — data integrity risk. Now raises the first batch error using the same CAS error code dispatch as `protocol._raise_error()` (PR #208), mapping to the correct PEP 249 exception class (IntegrityError, ProgrammingError, OperationalError, etc.).
- **DATA_LENGTH broker response validation (#188)** — `_send_and_receive()` in both sync `Connection` and `AsyncConnection` unpacked the 4-byte `DATA_LENGTH` header and immediately allocated `bytearray(data_length)` without bounds checking. A malformed or hostile broker response with a negative value would raise a raw `ValueError` from `bytearray()`, and an oversized value could trigger unbounded memory allocation (OOM). Added `_validate_data_length()` that rejects negative values and values exceeding `DataSize.MAX_PACKET_SIZE` (256 MiB) with a clean `OperationalError`, applied at both the handshake and main send/receive paths.


## [1.6.0] - 2026-07-18

### Fixed
- **Cursor memory bounding for large result sets (#203, PR #207)** — both sync `Cursor` and `AsyncCursor` previously accumulated the entire result set in `_rows` via `_rows.extend(packet.rows)` on every server fetch. Iterating a 10K-row result set via `fetchone()` would buffer all 10K rows in memory even though only one row was needed at a time. Fixed by decoupling the server-side fetch position (`_fetched_count`, tracking total rows received) from the local buffer cursor (`_row_index`, indexing into `_rows`). `_fetch_more_rows()` now REPLACES the buffer with the new batch instead of extending it, keeping memory bounded by the configurable `fetch_size` (default 100). `fetchall()` additionally clears the buffer after consuming all rows. This fixes a latent dual-purpose bug where `_row_index` was used both as buffer index AND server fetch position, which would have caused infinite re-fetching if any naive trim scheme had been applied.
- **CAS error code dispatch (#204)** — `_raise_error()` in `protocol.py` previously classified exceptions by text substring matching (looking for keywords like "unique", "syntax", "duplicate" in the error message). This was fragile across CUBRID versions. Replaced with deterministic CAS error code dispatch: `CAS_ERROR_TO_EXCEPTION` mapping in `error_codes.py` maps 16 specific codes to the correct PEP 249 exception class (IntegrityError, ProgrammingError, OperationalError, InternalError, DataError). Text heuristics are now only used as a fallback for code -1 (ER_DBMS passthrough), where CAS wraps server-engine errors with a generic code. Also fixed a duplicate `return error_message` statement in `_add_error_hints()`.
## [1.5.1] - 2026-07-18

### Fixed
- **Sync `_send_and_receive` parse-error exception parity (#201)** — the sync connection path at `connection.py:_send_and_receive` previously caught only `OSError`, meaning malformed CAS broker responses that raised `struct.error`, `ValueError`, `IndexError`, or `UnicodeDecodeError` would bypass socket cleanup and leave the connection in a dirty state for reuse. The async path at `aio/connection.py:615-621` already handled these. Ported the full exception catch list to the sync path so parse errors now close the socket, mark `_connected = False`, and raise `OperationalError("malformed response from broker")` with the original cause chained.
- **LOB write server ACK verification (#202)** — `Lob.write()` returned `len(data)` unconditionally without checking whether the server actually wrote all the bytes. Under disk-full / quota-exceeded conditions, the server could write fewer bytes and the caller would never know — silent data truncation. `LOBWritePacket.parse()` now extracts `bytes_written` from the CAS response (the response code doubles as the byte count on success, matching `LOBReadPacket`'s existing pattern). `Lob.write()` compares `bytes_written` against `len(data)` and raises `OperationalError` on mismatch.


## [1.5.0] - 2026-05-23

### Policy
- **`RELEASE_POLICY.md` added; public API surface is now CI-gated.** The
  project's previously implicit 1.x semantic-versioning contract is now an
  explicit, machine-checkable document at the repository root. The CI workflow
  gains a `compat-check` job that runs `scripts/check_public_api.py` against
  the committed `api-baseline.json`; any change to the public surface
  (functions, classes, methods, parameters of `pycubrid.connect`,
  `pycubrid.aio.connect`, `Connection`, `Cursor`, `AsyncConnection`,
  `AsyncCursor`, `Lob`, exception classes, and the PEP 249 contract constants
  `paramstyle`/`apilevel`/`threadsafety` whose literal values are part of the
  contract) fails CI unless the developer regenerates the baseline in the same
  change, surfacing the surface diff for explicit human review. Type objects
  (e.g. `STRING`, `BINARY`) are tracked by their presence and type name only —
  identity-level changes are intentionally **not** flagged, per
  `RELEASE_POLICY.md` §2 "What the gate does *not* detect". The 1.2.0
  minor-release breaking change (removal of `Mapping` parameter style from
  `_bind_parameters`) is acknowledged in `RELEASE_POLICY.md` §4 as a historical
  violation rather than silently rewritten; the new gate exists to prevent any
  recurrence.
  README status line updated from "Beta" to "Stable (1.x)" across English
  and all five translated READMEs to align with the 1.0.0 declaration and the
  `Production/Stable` PyPI classifier already shipped since 1.0.0.

### CI
- **Per-PR `integration-tls` lane added** — `ci.yml` now includes an `integration-tls` job (Python 3.14 × CUBRID 11.4) that runs on every PR touching `pycubrid/aio/**`, `pycubrid/connection.py`, `pycubrid/_connection_common.py`, or `.github/workflows/**`. Mirrors the broker-provisioning logic from `integration-full.yml` so TLS regressions are caught BEFORE merge instead of waiting for the nightly full matrix. Path-gating is implemented via `dorny/paths-filter@v3.0.2` (SHA-pinned), and the `ci-gate` job treats `integration-tls.result == 'skipped'` as acceptable when no TLS-relevant files changed (closes #159).
- **TLS readiness probe now verifies the broker certificate** — both probe scripts in `integration-full.yml::integration-tls` previously called `ctx.check_hostname = False; ctx.verify_mode = ssl.CERT_NONE`, which silently accepted any TLS endpoint on `localhost:33000` and weakened the regression signal. The probes now use the verified default context built from `CUBRID_TLS_TEST_CA_FILE` directly; a misconfigured or untrusted broker cert fails the probe loudly (closes #157).
- **TLS skip allow-list tightened to full pytest node id** — the `grep -v 'test_aio_ssl_handshake_failure'` filter in `integration-full.yml` is replaced with the full nodeid `tests/test_aio_ssl_integration.py::test_aio_ssl_handshake_failure`. A future rename of the test no longer silently re-introduces the "unexpected skip" bug (closes #159).
- **Automated `SSL=ON` CUBRID broker provisioning** — `integration-full.yml` now includes an `integration-tls` job (Python {3.10, 3.14} × CUBRID 11.4) that starts a manually-managed CUBRID container, flips `BROKER1 SSL=OFF` → `SSL=ON`, extracts the broker's self-signed certificate, probes the TLS handshake, and runs `tests/test_aio_ssl_integration.py` with `CUBRID_TLS_TEST_*` env vars wired up. The job fails loudly if any TLS test is skipped, ensuring the live TLS path is exercised on every nightly/tag-push run instead of silently skipping (closes #147, #155)

### Fixed
- **Async TLS verification failures now surface promptly on Python 3.10** — `AsyncConnection._do_connect_handshake` now runs a narrow Python-3.10-only **preflight TLS verification probe** in the default executor immediately before `loop.start_tls()`. The probe opens a separate TCP socket to the same effective endpoint (replaying the `CUBRS` handshake on the no-redirect path, going straight to TLS on the redirect path), then performs a synchronous `ssl.SSLContext.wrap_socket()` with the **same** `SSLContext` and `server_hostname=self._host` as the real upgrade. Any `ssl.SSLError` propagates as `OperationalError`, matching the 3.11+ failure surface. Works around the known CPython 3.10 `asyncio` bug (gh-142352 family, fixed in 3.13/3.14) where `loop.start_tls()` hangs indefinitely on TLS-handshake-internal verification failures because `ssl_handshake_timeout` only bounds peer-unresponsive hangs. No-op on Python 3.11+; the 3.10 path incurs one extra TCP round-trip per connect (closes #156).
- **TLS handshake now matches CUBRID's STARTTLS-style upgrade** — both sync and async `connect()` previously wrapped the socket in TLS before any bytes were exchanged, which never worked against a real `SSL=ON` CUBRID broker. The driver now (1) opens a plaintext TCP socket, (2) sends the 10-byte ClientInfoExchange handshake using the SSL magic string `"CUBRS"` (vs `"CUBRK"` for plain), (3) reads the 4-byte broker status (negative codes now raise `OperationalError` instead of silently falling through), (4) reconnects to the redirected CAS worker on `new_connection_port > 0` without re-handshaking (matches upstream JDBC `BrokerHandler.connectBroker`), and (5) upgrades the connection to TLS before sending `OPEN_DATABASE`. The async path uses `loop.start_tls()` for Python 3.10 compatibility. Validated end-to-end against CUBRID 11.4 with `SSL=ON` and a self-signed broker certificate (#154)

### Documentation
- **Parameter binding contract documented** — added `docs/PARAMETER_BINDING.md` formalizing the driver-side literal-binding semantics for 1.x: per-type SQL-literal mapping (with `_cursor_common.py` line citations and pinned tests), the `escape_string` default and `no_backslash_escapes` modes (NUL rejection, single-quote doubling, backslash and CR/LF/`\x1a` handling), the placeholder tokenizer's behavior across quoted strings/identifiers/line and block comments, and the explicit non-guarantees (no server-side prepared statements, identifiers are not escaped, no `IN`-clause expansion, exception-message text is not contract). Linked from `README.md` and `docs/index.md`.
- **TLS/handshake documentation aligned with implementation** — corrected `AGENTS.md` protocol version (8/10.2) and OpenDatabase payload framing, fixed OpenDatabase response field order across `CONNECTION.md`/`ARCHITECTURE.md`, rewrote the CAS reconnection diagram to reflect actual `connect()` re-entry through the broker, corrected the `MAGIC_STRING_SSL` constant name in `PROTOCOL.md`, surfaced TLS 1.2 minimum on sync rows in `SUPPORT_MATRIX.md`, added a new "Async TLS Handshake Hangs on Python 3.10" troubleshooting section, and added the Python 3.10 async TLS caveat to `README.md` and all five translations (#160, #163)
- **TLS docs polish** — added TLS examples to `EXAMPLES.md`, an SSL/TLS TOC entry to `CONNECTION.md`, a Transport Security section to `SECURITY.md`, local TLS integration-test instructions to `CONTRIBUTING.md`, expanded `Connection.connect`/`AsyncConnection`/`_do_connect_handshake` docstrings with the STARTTLS flow, added a TLS field to the bug-report issue template, expanded `pyproject.toml` keywords, and added a `make integration-tls` target. Resolves remaining items from the TLS Phase 4 audit (#161, #162)
- **Async TLS handshake hang on Python 3.10 documented as a known limitation** — a known CPython asyncio TLS handshake bug on Python 3.10 causes `loop.start_tls()` to hang on cert-verify failures on 3.10 only (fixed in 3.13/3.14); pycubrid documents the workaround and skips the negative-path test on 3.10 (#156)

### Tests
- **Async TLS upgrade and handshake paths covered offline** — added `tests/test_aio_ssl_offline.py` with 7 mocked tests guarding the regressions enumerated in #158: `_upgrade_to_tls()` argument forwarding (incl. `ssl_context`, `server_hostname=self._host`, `ssl_handshake_timeout` with the documented 10-second default), `loop.start_tls()` failure cleanup (`old_transport.abort()` exactly once, exception re-raised unchanged, defensive `None`-return handling), incomplete-read and EOF on the initial 4-byte `CUBRS`/`CUBRK` broker status response, and async parity for `test_connect_redirect_sends_no_second_handshake` (redirect during TLS connect reconnects on the new port **without** a second handshake). These run without a broker so a regression silently disabling hostname verification or re-introducing a double-handshake no longer slips through offline CI (closes #158).
- **Sync/async lifecycle parity coverage expanded** — integration parity tests now share adapter-driven scenarios and cover connection lifecycle APIs including `ping()`, CAS-inactive reconnect, auto-commit transitions, insert identity helpers, batch rowcount semantics, close ordering, and the explicit `AsyncConnection` `create_lob` `AttributeError` contract (closes #140)
- **Async TLS integration coverage** — added `tests/test_aio_ssl_integration.py` with
  live async TLS success/failure/reconnect/shutdown coverage gated behind an
  explicitly configured TLS-enabled broker (#155)

### Validated
- **Native `Connection.ping()` causally validated at application layer** — Tier 2 ORM benchmark in [cubrid-benchmark `2026-04-22_native-ping-hotpath`](https://github.com/cubrid-lab/cubrid-benchmark/tree/main/experiments/orm-overhead/runs/2026-04-22_native-ping-hotpath) (paired same-version A/B vs forced `SELECT 1`, 7 trials, bootstrap 95% CI) confirms native CHECK_CAS ping is **+279.9% throughput** on raw ping_only [+278.0, +283.9] and **+587.8% on SQLAlchemy `checkout_only`** [+581.8, +603.8] with `pool_pre_ping=True`. Performance Loop ping propagation gap closed.

### Added (transport contract lock — #167)
- **Session state restoration on transparent reconnect** — When the broker
  signals ``CAS_INFO_STATUS_INACTIVE`` and pycubrid reconnects transparently
  (matching JDBC's ``UClientSideConnection.checkReconnect``), any session
  setting the caller has **explicitly** set on the connection (currently
  ``autocommit``) is now re-emitted on the new CAS worker via
  ``SetDbParameterPacket``. Settings the caller has never touched are left at
  the broker default to avoid spurious round-trips. The same restoration runs
  on the ``ping(reconnect=True)`` recovery path. Restore failures tear down
  the connection and chain the underlying transport error via PEP 3134
  ``__cause__`` so callers can diagnose them. Both sync ``Connection.ping()``
  and async ``AsyncConnection.ping()`` attempt the reconnect+restore at most
  **once per call** — the preflight ``_check_reconnect`` runs first and the
  ``CHECK_CAS`` request is sent with ``allow_reconnect=False`` so a restore
  failure cannot trigger a second attempt via ``_send_and_receive`` (PR #3
  Item 1).
- **Mid-fetch reconnect raises ``OperationalError``** — When the broker
  releases the CAS worker while a cursor still has rows pending on the
  server, the server-side query handle is no longer valid. Cursors now mark
  themselves as reconnect-invalidated and ``fetchone``/``fetchmany``/
  ``fetchall`` raise :class:`OperationalError` (``result set lost due to
  broker reconnect mid-fetch``) once the buffered rows are exhausted, instead
  of silently returning a truncated result set. Rows already buffered in the
  cursor remain accessible. ``execute()`` and ``close()`` reset the
  invalidation flag (PR #3 Item 2).

### Fixed
- **PEP 3134 ``__cause__`` preserved on async transport timeouts** —
  ``AsyncConnection._connect_locked`` (handshake timeout) and
  ``AsyncConnection._send_and_receive_locked`` (read timeout) now use
  ``raise OperationalError(...) from exc`` instead of ``from None``, so the
  underlying ``asyncio.TimeoutError`` is preserved on the chained exception
  for diagnostic tooling (PR #3 Item 3).

### Documentation
- **Reconnect contract documented in ``docs/CONNECTION.md``** — added a
  "Session-state restoration on transparent reconnect" section listing which
  settings are restored and which are not, plus a correction to the
  ``autocommit`` default note: pycubrid sends ``auto_commit`` per-statement
  on every ``PrepareAndExecute``, so the broker's own ``CUBRID_AUTO_COMMIT``
  setting is effectively overridden by the driver-side value.
- **Cursor mid-reconnect behaviour documented in ``docs/API_REFERENCE.md``** —
  ``fetchone``/``fetchmany``/``fetchall`` now document the
  :class:`OperationalError` raised when a transparent reconnect invalidates
  a partially-consumed result set.
- **Python 3.10 async-TLS caveat citation normalized** — the upstream issue
  reference (``gh-142352``) was removed from ``README.md``, all five README
  translations (``docs/README.{ko,zh,hi,de,ru}.md``), ``SECURITY.md``,
  ``CHANGELOG.md``, ``CONTRIBUTING.md``, ``docs/CONNECTION.md``,
  ``docs/TROUBLESHOOTING.md``, ``docs/DEVELOPMENT.md``, ``docs/EXAMPLES.md``,
  ``docs/SUPPORT_MATRIX.md``, ``tests/test_aio_ssl_integration.py``, and
  ``pycubrid/aio/connection.py`` because that issue describes a different
  ``start_tls()`` regression on 3.13/3.14/3.15 (PROXY-protocol buffered-data
  loss), not the 3.10 cert-verify hang pycubrid observes. The caveat is now
  described as a "known CPython async-TLS handshake bug on Python 3.10"
  tracked as pycubrid #156.

### Tests
- **12 new tests in ``tests/test_network_edge_cases.py``** — four new test
  classes covering: ``__cause__`` chaining on sync/async transport timeouts,
  explicit/unset session-state restore on reconnect (sync + async),
  restore-failure tear-down, mid-fetch ``OperationalError`` (sync + async),
  ``execute``/``close`` resetting the invalidation flag, and
  ``CancelledError`` propagation in ``AsyncConnection._close_streams``. Total
  offline tests: 858.

## [1.4.0] - 2026-05-13

### Added
- **TLS/SSL support for async connections** — `AsyncConnection` now supports `ssl=True`, `ssl=False`, or `ssl=ssl.SSLContext(...)` via `asyncio.open_connection(ssl=...)` with `StreamReader`/`StreamWriter` transport (#129, #136)
- **`mypy --strict` CI gate** — typecheck job added to CI workflow to enforce strict typing (#130)
- **Sync/async parity integration tests** — expanded test coverage for bytes, datetime, fetch_size, JSON, and edge-case scenarios (#134)

### Changed
- **`ConnectionCommonMixin` extracted** — deduplicated ~70% of shared logic between `Connection` and `AsyncConnection` into a common mixin (#133, #135)
- **`CursorParamsMixin` extracted** — eliminated sync/async cursor parameter-handling duplication (#123, #127)
- **Driver-side binding semantics documented** — README, ARCHITECTURE, and PRD updated to clarify that `?` placeholders are interpolated locally, not via server-side prepared statements (#131)

### Fixed
- **`fetch_size` validation** — `Connection` and `AsyncConnection` constructors now reject non-positive `fetch_size` values (#132)
- **Dead `backports.zoneinfo` fallback removed** — eliminated unused Python 3.8 compatibility code
- **Typed locals in async module** — replaced `str()` coercion with properly typed local variables
- **`mypy --strict` errors resolved** — full strict-mode compliance across the codebase
- **`asyncio.run()` for Python 3.14** — replaced deprecated `get_event_loop()` usage

## [1.3.2] - 2026-04-21

### Added
- **Native async `AsyncConnection.ping()`** using `CHECK_CAS` (FC=32) for lightweight CAS-level liveness checks. Native `CHECK_CAS` now performs a round trip regardless of `CAS_INFO` status, while `reconnect=False` suppresses implicit broker-handoff reconnect via `_send_and_receive(..., allow_reconnect=False)` (#95, #70)

### Fixed
- **Sync `Connection.ping(reconnect=False)` now honors broker handoff correctly** — native `CHECK_CAS` runs regardless of `CAS_INFO` status, while `reconnect=False` suppresses implicit broker-handoff reconnect via the new `_send_and_receive(..., allow_reconnect=False)` flag (#95, #70)

## [1.3.1] - 2026-04-21

### Documentation
- **Oracle audit fixes completed** — documentation gaps from the Oracle review were closed across the main guides, with no runtime or public API changes in `pycubrid/`.
- **Driver-level timing hooks documented** — `enable_timing=True` keyword and `PYCUBRID_ENABLE_TIMING` environment variable, `Connection.timing_stats` property, and the `TimingStats` accumulator are now covered in `docs/API_REFERENCE.md` and `docs/PERFORMANCE.md` (closes #16). The implementation has shipped since 1.0.0; this completes the "API documented" acceptance criterion.
- **Async parity wording clarified** — sync vs. async capability differences are now described consistently, including async-specific wording cleanups in the Korean docs.
- **`executemany()` guidance expanded** — bulk operation documentation now explains `executemany()` behavior and usage more clearly.
- **README translations synchronized** — Korean, German, Russian, Chinese, and Hindi READMEs were refreshed to match the current English documentation.

## [1.3.0] - 2026-04-20

### Added
- **SSL/TLS support for sync connections** — `ssl=True` (verified context), `ssl=False`/`None` (disabled), or `ssl=ssl.SSLContext(...)` for custom config on `pycubrid.connect()` (#85)
- **Reconnect / network edge case test suite** — 17 tests covering connection reset, timeout, broken pipe, partial reads, reconnect-after-failure (#87)
- **Concurrency stress tests** — threaded (16 workers × 25 inserts, 32 readers) and asyncio.gather (16 workers, 32 readers) with own-Connection isolation
- **Standalone version check script** — `scripts/check_version.py` AST-based pyproject/`__init__.py` consistency check, replaces fragile inline grep in CI (#88)
- **PyPI classifiers** — `Operating System :: OS Independent`, `Typing :: Typed`, `Programming Language :: Python :: 3 :: Only` (#89)
- **Character encoding documentation** — UTF-8-only contract documented in `docs/CONNECTION.md` (#86)

### Fixed
- **PEP 639 license conflict** — removed redundant `License ::` classifier; SPDX `license = "MIT"` is the single source of truth (follow-up #89)
- **`test_ping_reconnect_also_fails` dual-stack fragility** — patches `socket.create_connection` instead of `socket.socket`

### Deferred
- **#90 Sync/async deduplication** — refactor deferred per Oracle review (high regression risk vs. maintainability gain)

### Async SSL
SSL/TLS for async connections raises `NotSupportedError` — `asyncio.loop.sock_*` APIs reject `SSLSocket`. Use the sync interface for TLS, or async without encryption. Tracked for future asyncio integration.

## [1.2.0] - 2026-04-19

### Added
- **Native `Connection.ping()`** using CHECK_CAS (FC=32) — lightweight CAS-level health check without SQL execution (#70)
- **`errno`/`sqlstate` on `DatabaseError`** — all protocol errors now populate structured error metadata with standard SQLSTATE codes (#71)
- **JSON type decoding** — opt-in `json_deserializer` parameter on `connect()`, CAS protocol bumped to v8, `CUBRIDDataType.JSON = 34` (#72)
- **Collection type decoding** — opt-in `decode_collections` parameter on `connect()`, SET → frozenset, MULTISET → list, SEQUENCE → list (#73)
- **SQLSTATE mapping table** (`error_codes.CAS_ERROR_TO_SQLSTATE`) for 19 common CUBRID error codes
- **Async cursor parity** — sync and async cursors now share identical `_escape_string` and parameter binding logic (#76, #77)
- **Timezone datetime parsing** — `DATETIMETZ`/`TIMESTAMPTZ` wire format decoding with IANA timezone keys (#78)
- **`cursor.nextset()`** for PEP 249 completeness (#79)
- **Configurable `fetch_size`** — pass `fetch_size=N` to `connect()` instead of hardcoded 100 (#81)
- **Async `read_timeout`** — `asyncio.wait_for` wrapping in `_send_and_receive` (#82)
- **Async dual-stack address fallback** — `getaddrinfo` iteration for IPv4/IPv6 in `_create_socket_nonblocking` (#83)
- **`_format_parameter()` hardening** — reject `float('nan')`/`float('inf')` with `ProgrammingError`, `DATETIMETZ` literals for tz-aware datetime (IANA key preferred, UTC offset fallback), `bytearray` support alongside `bytes` (#74)

### Security
- **Hardened parameter binding** — escape backslashes, reject null bytes, escape control characters (\r, \n, \x1a) in client-side SQL interpolation (#74)

### Fixed
- **Cursor registration dedup** — cursors no longer self-register in `__init__`; only `Connection.cursor()` registers (#76)
- **`Cursor.close()` best-effort** — narrowed exception handling to `InterfaceError`/`OperationalError`/`OSError` only (#80)
- **Sync `read_timeout`** — uses `socket.create_connection` for proper timeout enforcement
- **Sync IPv6 dual-stack** — `create_connection` handles address fallback automatically
- **Unreachable return removed** — dead `DATETIMETZ` return path in `_format_parameter()` cleaned up
- **Test isolation** — `_CursorClass` global cache no longer leaks between unit/integration tests
- **Benchmark `demodb` default** — changed to `testdb` matching Docker fixture

### Changed
- CAS protocol version bumped from 7 to 8 (enables native JSON type recognition)
- **BREAKING**: `_bind_parameters()` now only accepts `Sequence` (tuple/list) — `Mapping` (dict) parameter style removed. Use positional `?` parameters only.

## [1.1.0] - 2026-04-18

### Added
- **Native asyncio support** via `pycubrid.aio` module
  - `pycubrid.aio.connect()` — async connection factory
  - `AsyncConnection` — async context manager, commit, rollback, cursor creation
  - `AsyncCursor` — async execute, fetch (one/many/all), iterate, executemany
  - Uses `loop.sock_*` non-blocking socket I/O — reuses existing protocol/packet layers
- 30 new async offline tests (`tests/test_async.py`)

## [1.0.0] - 2026-04-11

### Compatibility Policy

This release establishes the 1.x compatibility contract: the public API follows semantic versioning,
and breaking changes will only occur in major version bumps (2.0+).

### Supported Environments

- **Python**: 3.10, 3.11, 3.12, 3.13
- **CUBRID**: 11.2, 11.4
- **Protocol**: CAS wire protocol version 8 (since CUBRID 10.2+)

### Fixed
- Resolve all mypy errors: explicit `str` return types in `get_server_version`
  and `get_last_insert_id` (`connection.py`)
- Resolve all pyright errors: initialize `response_code` in `PrepareAndExecutePacket`
  and `PreparePacket.__init__` (`protocol.py`); guard `_CursorClass` optional call (`connection.py`)

### Changed
- Development Status classifier updated from "Beta" to "Production/Stable"
- Version bumped to 1.0.0

## [0.7.0] - 2026-04-04

### Added
- `docs/SUPPORT_MATRIX.md`: Comprehensive support matrix documenting Python versions,
  CUBRID versions, PEP 249 compliance, data type mappings, driver features, and known
  limitations — defines the 1.0 support boundary
- Connection pooling section in `docs/CONNECTION.md` clarifying that pycubrid has no
  built-in pool and recommending SQLAlchemy or external pooling

### Fixed
- README documentation table: Removed incorrect "connection pool" reference from
  Connection guide description — pycubrid has no driver-level connection pool

### Changed
- Version bumped to 0.7.0 (stabilization release on path to 1.0)

## [0.6.0] - 2026-03-28

### Added
- Transparent CAS reconnection when broker signals `CAS_INFO_STATUS=INACTIVE`,
  matching the official CUBRID JDBC driver's `UClientSideConnection.checkReconnect()` behaviour
- `_check_reconnect()` method inspects `CAS_INFO[0]` before every request and
  reconnects automatically when the CAS process has been released (`KEEP_CONNECTION=AUTO`)
- `_invalidate_query_handles()` clears stale cursor query handles after
  commit/rollback to prevent `CloseQueryPacket` on dead sockets
- `CAS_INFO` is now updated from every server response so the status byte is always current

### Changed
- `_send_and_receive()` now calls `_check_reconnect()` instead of `_ensure_connected()`
  for automatic reconnection support

### Performance
- Pre-compiled `struct` objects in `packet.py` — eliminates repeated `struct.Struct()`
  instantiation on every read/write call
- Dict-based type dispatch table `_TYPE_READERS` in `protocol.py` — replaces
  long if/elif chain in `_read_value()` for O(1) type dispatch
- Slice-based `fetchall()`/`fetchmany()` in `cursor.py` — replaces per-row
  `fetchone()` loop with direct list slicing
- `executemany()` DML batch path — pre-renders all parameter sets into SQL
  strings and sends a single `BatchExecutePacket` instead of N round-trips
- `recv_into()` in `_recv_exact()` — writes directly into a pre-allocated
  buffer via `memoryview`, avoiding temporary `bytes` allocations
- `TCP_NODELAY` and `SO_KEEPALIVE` socket options on connection creation
- Module-level `_CursorClass` cache — eliminates `importlib.import_module()`
  + `getattr()` on every `Connection.cursor()` call
- SELECT 10K rows fetch: 96ms → 78ms (−19%)
- Connection establishment: 2.24ms → 1.66ms (−26%)
- INSERT execute: 7.81ms → 7.10ms (−9%)

### Fixed
- DDL statements (CREATE TABLE, ALTER TABLE) followed by DML on the same
  connection no longer fail with "connection lost during receive" (closes #23)

## [0.5.0] - 2026-03-12

### Added
- SQLAlchemy integration via `sqlalchemy-cubrid` v2.1.0 (`cubrid+pycubrid://` URL scheme)
- Updated README with SQLAlchemy usage examples

### Changed
- Version bumped to 0.5.0

## [0.4.0] - 2026-03-12

### Added
- `Lob` class for BLOB/CLOB Large Object support (create, write, read)
- `Connection.create_lob()` helper for server-side LOB creation
- `Connection.get_schema_info()` for schema introspection via CAS protocol
- `Cursor.executemany_batch()` for batch execution of multiple SQL statements
- Exported `Lob` from package `__init__.py`

## [0.3.0] - 2026-03-12

### Added
- PEP 249 `Connection` class with full CAS handshake lifecycle
  (`ClientInfoExchange` → `OpenDatabase` → `CloseDatabase`)
- TCP socket management with partial-read handling
- `commit()`, `rollback()`, `close()`, `cursor()` methods
- `autocommit` property for transaction control
- `get_server_version()` and `get_last_insert_id()` helper methods
- Context manager protocol (`with conn:` auto-close)
- PEP 249 `Cursor` class with full query execution
  (`execute`, `executemany`, `fetchone`, `fetchmany`, `fetchall`)
- Client-side parameter binding (str, int, float, None, bool, bytes,
  date, time, datetime, Decimal)
- `description` and `rowcount` attributes per PEP 249 spec
- Iterator protocol and context manager for Cursor
- `callproc()`, `setinputsizes()`, `setoutputsize()` stubs

### Fixed
- Double-parse bug in `_send_and_receive()` — now correctly passes
  response body (without data_length prefix) to packet.parse()

## [0.2.0] - 2026-03-12

### Added
- Wire protocol `PacketWriter` and `PacketReader` for CAS binary frame
  serialization/deserialization (big-endian, length-prefixed fields)
- 18 CAS protocol packet classes (`ClientInfoExchangePacket`, `OpenDatabasePacket`,
  `PreparePacket`, `ExecutePacket`, `PrepareAndExecutePacket`, `FetchPacket`,
  `CloseQueryPacket`, `CommitPacket`, `RollbackPacket`, `CloseDatabasePacket`,
  `GetEngineVersionPacket`, `BatchExecutePacket`, `GetSchemaPacket`,
  `SetDbParameterPacket`, `GetDbParameterPacket`, `GetLastInsertIdPacket`,
  `LOBNewPacket`, `LOBWritePacket`, `LOBReadPacket`)
- Response parsing helpers: `_raise_error`, `_parse_column_metadata`,
  `_parse_result_infos`, `_parse_row_data`, `_read_value`
- `ColumnMetaData` and `ResultInfo` dataclasses for structured query metadata
- Full wire-level value deserialization for all 27+ CUBRID data types

## [0.1.0] - 2026-03-12

### Added
- Initial project scaffolding
- PEP 249 exception hierarchy (Warning, Error, InterfaceError, DatabaseError, DataError,
  OperationalError, IntegrityError, InternalError, ProgrammingError, NotSupportedError)
- PEP 249 type objects (STRING, BINARY, NUMBER, DATETIME, ROWID) and constructors (Date, Time,
  Timestamp, DateFromTicks, TimeFromTicks, TimestampFromTicks, Binary)
- CAS protocol constants (41 function codes, 27+ data types, isolation levels)
