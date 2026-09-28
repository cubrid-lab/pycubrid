# Release Policy

This document specifies the versioning, compatibility, and release rules that
the pycubrid project follows starting from version 1.0.0. It exists so that
users, downstream integrators (e.g. `sqlalchemy-cubrid`), and contributors can
make decisions with predictable expectations about backward compatibility.

The policy is enforced both by human review and by an automated CI gate
(`scripts/check_public_api.py` + `api-baseline.json`). Any change to the
declared public API surface fails CI unless the baseline is regenerated and
committed in the same change, which forces every surface change to surface
explicitly in pull-request review.

## 1. Public API Surface

The **public API** of pycubrid is exactly the union of:

1. Every name listed in `pycubrid.__all__`.
2. Every name listed in `pycubrid.aio.__all__`.
3. The following classes, which users receive as return values from public
   factory functions and therefore depend on transitively:
   - `pycubrid.connection.Connection`
   - `pycubrid.cursor.Cursor`
   - `pycubrid.aio.connection.AsyncConnection`
   - `pycubrid.aio.cursor.AsyncCursor`
   - `pycubrid.lob.Lob`
4. For each public class, every public attribute (name not starting with `_`),
   every public method, and the user-facing dunder allow-list:
   `__init__`, `__enter__`, `__exit__`, `__aenter__`, `__aexit__`, `__iter__`,
   `__aiter__`, `__next__`, `__anext__`, `__repr__`, `__str__`.

The exact, machine-checkable definition is encoded in `scripts/check_public_api.py`
and serialized as `api-baseline.json` at the repository root.

### Out of Scope

Nothing else is part of the public API. In particular, the following are
explicitly excluded and may change in any release, including patch releases,
without notice:

- Any name starting with an underscore, anywhere.
- Any module under `pycubrid` whose name starts with `_` (e.g. `_cursor_common`,
  `_connection_common`).
- The wire protocol classes in `pycubrid.protocol` and `pycubrid.packet`.
  They implement the CAS wire protocol and may evolve as CUBRID itself
  evolves; users must not import from them directly.
- `pycubrid.constants` (CAS function codes, data-type codes, framing constants).
  These are tied to the wire protocol and are not a user-facing surface.
- The `pycubrid.error_codes.CAS_ERROR_TO_SQLSTATE` mapping table is exposed via
  `get_error_description`; the mapping data itself may grow over time.
- The `pycubrid.timing` module beyond the `TimingStats` re-export from the top
  level. The internal accumulator implementation may change.
- Type annotations on public methods. Refining a type (e.g. `Any` → a more
  specific union, or adding `| None` to make a previously implicit case
  explicit) is not considered a breaking change.
- Exception messages and SQLSTATE codes returned in error metadata. The
  exception **class** is part of the public surface; the textual message
  attached to an instance is not.
- Behavior that is not documented in `docs/` or covered by an offline test.

## 2. Semantic Versioning Rules

pycubrid follows [Semantic Versioning 2.0](https://semver.org/). Starting from
**1.0.0**, version components have the following meaning:

- **MAJOR** (`x.0.0`) — May contain breaking changes to the public API surface
  as defined in §1. The next major version is `2.0.0`.
- **MINOR** (`1.x.0`) — Adds functionality in a backward-compatible manner.
  May add new public functions, methods, classes, parameters with defaults,
  or `__all__` entries. May not remove, rename, or change the structural
  signature of anything already on the public surface.
- **PATCH** (`1.x.y`) — Backward-compatible bug fixes only. Must not add new
  public API.

A "structural signature change" includes any of the following on any public
callable:

- Adding a required parameter (one without a default).
- Removing any parameter (positional or keyword).
- Renaming any keyword-accepting parameter.
- Changing a parameter's kind (e.g. positional-or-keyword → keyword-only, or
  positional-only → positional-or-keyword).
- Reordering positional parameters, or inserting an optional positional
  parameter before any existing positional parameter (binding by position
  silently changes).
- Removing the default value from an existing parameter (turns an optional
  parameter into a required one for callers that did not supply it).
- Removing or renaming a method, property, classmethod, or staticmethod on
  a public class.
- Removing an entry from `__all__` of a tracked module.
- Changing the value of a public scalar constant such as `paramstyle`,
  `apilevel`, or `threadsafety`. These are part of the PEP 249 contract; the
  `compat-check` gate captures their values, not just their types.

Adding optional parameters with defaults *at the end of the parameter list*,
adding new methods, adding new exception subclasses, and adding new public
modules are all permitted in minor releases.

### Planned explicit compatibility namespaces (#438)

The selected [additive design](docs/UPSTREAM_COMPATIBILITY.md#selected-additive-contract-438)
plans `pycubrid.compat.cubriddb` and `pycubrid.compat.native`; neither exists yet.
This documentation-only decision changes no current API, default or support line.
Future explicit APIs are **MINOR** additions only while ordinary behavior stays
unchanged; documented ordinary bug corrections remain **PATCH**. Their foundation
PR must declare explicit submodule exports, extend §1 and the existing API
checker's tracked modules/returned classes, and regenerate the baseline together.
Regenerating today's ordinary-only baseline alone cannot protect new namespaces.
This design does not authorize a default replacement, 2.0 migration, new dependency,
version/tag/PyPI publication.

### What the gate does *not* detect

The `compat-check` CI gate captures the structural surface — names,
descriptor kinds, parameter shapes, and scalar constant values. It
intentionally does **not** detect, and therefore the reviewer must catch
in code review:

- Behavioral changes that keep the signature intact (e.g. `commit()` now
  rolls back on certain errors that previously raised).
- Default *value* changes on parameters (e.g. `fetch_size=100` → `fetch_size=200`).
- Type annotation changes (these are routinely refined without affecting
  behavior; the gate ignores them by design).
- Identity changes of public type objects such as `STRING`/`BINARY`/`NUMBER`/
  `DATETIME`/`ROWID` (the gate records the type tag, not the value identity).
- Exception message text or SQLSTATE values returned at runtime.

## 3. Breaking-Change Process

Breaking changes are only permitted in major version bumps. The full process
for landing one is:

1. Open an issue tagged `breaking-change` describing the motivation and
   migration path before any code is written.
2. Implement the change on a topic branch.
3. Regenerate the API baseline:

   ```bash
   python scripts/check_public_api.py --update
   ```

4. Commit `api-baseline.json` together with the source change so the surface
   diff is auditable in pull-request review.
5. Add a `### Breaking Changes` section to the relevant `CHANGELOG.md` entry
   describing what changed, why, and how users migrate. The entry must include
   a `Migration` subsection with concrete before/after code.
6. Bump the major version in both `pyproject.toml` and `pycubrid/__init__.py`
   (the existing `version-check` CI job enforces these stay in sync).
7. Land the change on `main`. Tag and release as `vX.0.0`.

The CI gate (`compat-check` job) will fail any pull request that changes the
public surface without also updating `api-baseline.json`, which is exactly
how this policy is enforced in practice.

## 4. Acknowledged Historical Violation

Version **1.2.0** (2026-04-19) removed dict (mapping) parameter style from
`_bind_parameters()` in a minor release, in violation of the policy declared
in 1.0.0. The change was documented in the changelog with a `**BREAKING**`
marker but no major version bump occurred.

This release policy and the `compat-check` CI gate are introduced specifically
to prevent any further occurrences of this pattern. The 1.2.0 violation is
acknowledged in `CHANGELOG.md` and not silently rewritten.

The project remains on the 1.x line; the violation is treated as one-off rather
than an excuse to abandon the contract going forward.

## 5. Yanking and Security Releases

- A release containing a serious bug or security regression may be yanked from
  PyPI via `pip yank`. A yanked release remains discoverable but pip refuses
  to install it by default; a replacement patch release is published.
- Security fixes are released as soon as a fix is available, on whichever
  patch line of the current `1.x` series is affected. Older minor lines are
  not back-ported automatically; users should track the latest minor release
  on the current major.
- Vulnerability reports go through `SECURITY.md`, not public issues.

## 6. Python and CUBRID Support Windows

- **Python**: pycubrid supports the Python versions declared in
  `pyproject.toml` `requires-python` and tested in CI. Dropping a Python
  version is considered a breaking change and requires a major version bump,
  with one exception: a Python version may be dropped in a minor release if
  **all three** conditions are satisfied:
    1. The Python Software Foundation has marked the version end-of-life.
    2. The drop is announced at least one minor release in advance in
       `CHANGELOG.md` (under a `### Deprecated` heading) and in
       `docs/SUPPORT_MATRIX.md`.
    3. `ROADMAP.md` records the drop schedule before the deprecating release
       ships.
- **CUBRID**: pycubrid targets the CUBRID CAS protocol version 8 (CUBRID 10.2
  and newer). Adding support for a future CAS protocol version is additive
  and lands in a minor release. Removing support for a CUBRID version
  currently exercised in CI is a breaking change.

## 7. Documentation Contract

For every change that affects user-visible behavior, the following must be
updated in the same pull request or as an immediate follow-up:

- `CHANGELOG.md` — entry under `[Unreleased]`.
- The relevant document under `docs/` (e.g. `CONNECTION.md`, `API_REFERENCE.md`,
  `TYPES.md`, `SUPPORT_MATRIX.md`).
- If the change affects the public surface, `api-baseline.json` (regenerated).
- If the change affects the wire protocol or handshake, `docs/PROTOCOL.md` and
  `AGENTS.md`.

Code without a corresponding documentation update is considered incomplete.

### Behavior-change classifications

Backward-compatible bug fixes ship in a **PATCH** release (§2). Recorded here so
the documented release contract stays complete alongside `CHANGELOG.md`:

- **Owned schema rows (#456)** — MINOR / additive methods and optional keyword-only
  `arg2=None`. The three existing getter positional arguments/defaults and raw
  packet return/query_handle/tuple_count remain. Correct FC9 layout is activated
  together with owning eager fetch and explicit abandonment, immutable original
  handle metadata, and deterministic retirement. Schema FETCH/CLOSE do not
  reconnect, replay or implicitly commit; transaction boundaries close active
  schema handles, including before auto-committing cursor/batch statements and
  connection-level version lookup with autocommit enabled.
  Non-`Exception` interruptions during async schema FETCH discard the uncertain
  session without sending CLOSE over a pending response.
  No holdability/native-profile choice or new dependencies.
  Initial #456 CLASS/ATTRIBUTE coverage is extended by the #457 seven-family
  live matrix on CUBRID 10.2/11.4; it still does not certify all schema codes.
- **Empty bytes LOB writes avoid broker I/O (#394)** — PATCH / correction to the
  documented bytes-written contract, matching the existing zero-length read
  precedent. Open-LOB, negative-offset, connection and wire argument validation
  still run before returning `0`; nonempty ACK checks and other data-type paths
  retain existing behavior. No public signatures, strict argument policy (#449),
  async LOB support, dependencies or supported versions change.

- **Unfinished-result invalidation reports an error (#395)** — PATCH / correction
  of silent partial fetch success after a transaction boundary. A missing handle
  with broker-delivered rows below the advertised total raises `InterfaceError`
  at the next required FETCH. Cached rows and fully received/exhausted results
  retain their behavior; reconnect-specific `OperationalError` is unchanged.
  No new public surface, replay, holdability, transaction policy, dependency or
  supported-version change is introduced.

- **Native NOT NULL and foreign-key error classification (#390)** — PATCH /
  correction to the documented PEP 249 integrity-error contract. Codes `-631`
  and `-922` raise the existing `IntegrityError` with SQLSTATE `23000`, and
  batch failures retain the original code in `errno` as single statements do.
  Public names/signatures, transaction semantics, generic-code text fallback,
  runtime dependencies and supported versions are unchanged.

- **Native syntax/semantic/communication meanings (#391)** — PATCH / correction
  of verified error-code metadata and classification. `-493` and `-494` remain
  `ProgrammingError` with generic `42000`; `-671` becomes `OperationalError` /
  `08S01`, not a foreign-key error. Batch errors reuse the existing known-code
  SQLSTATE lookup, preserving unknown-code defaults and native identities.
  Callers must not infer missing tables from generic parser codes; downstream
  reflection is tracked separately in sqlalchemy-cubrid #454. No API signatures,
  defaults, transaction semantics or dependencies change.

- **Description nullability (#431, #398)** — PATCH / bug correction. The broker's
  non-null flag is inverted when deriving the documented PEP 249 `null_ok` value.
  Public signatures, optional size fields, collection codes and default return
  shapes are unchanged; this is not a new native-driver compatibility profile.

- **Collection metadata decoding (#403, #410)** — PATCH / bug correction restoring
  the documented collection types and opt-in decoding behavior. Default raw bytes,
  public signatures and unsupported collection parameter binding are unchanged.

- **Quality-tool pin and scope consistency (#416)** — PATCH / development and CI
  maintenance. Shared lint/format targets include maintained scripts/demos, and
  declared pins, hook revisions, installed versions, and scopes are checked together.
  Strict typechecking remains package-only; no driver behavior, public API, runtime
  dependency, or supported-version changes.

- **Marker-based integration lane coverage (#397)** — PATCH / CI bug correction.
  Normal, TLS, and slow workloads have executable workflow paths and a dynamic
  collection/skip audit. Missing optional native-comparison dependencies and
  unsupported `/proc` platforms are explicitly classified. No driver public API,
  SQL behavior, dependency, or supported-version changes.

- **Integration readiness gates fail closed (#411)** — PATCH / CI bug correction.
  Regular and full workflows reuse the bounded shared probe; failed SELECT or
  exhausted retries prevents the test step. Probe resources close on every exit.
  No driver public API, SQL behavior, or supported-version changes.

- **Cached broker insert identity survives transaction boundaries (#381)** — PATCH /
  bug correction to the documented identity convenience method. Successful return
  values remain `str`; unavailable identities now explicitly return `None` instead
  of the ambiguous empty string. This is a runtime unavailable-result correction,
  not merely an annotation refinement. Migration: replace `value == ""` with
  `value is None` and guard `int(value)`; `cursor.lastrowid` stays `int | None`.
  The cache is an observation of broker state, not proof that the latest INSERT
  generated an identity or that a row exists after rollback. INSERT attempts,
  nonempty batches, and physical connection changes invalidate it. No public
  names or structural signatures change; the API baseline remains unchanged.
  This cursor-INSERT snapshot does not observe CALL, stored-procedure INSERTs,
  or out-of-band operations; callers must return/query those identities explicitly.

- **Empty `executemany()` clears prior result state (#376)** — PATCH /
  backward-compatible bug fix. Public signatures are unchanged. Empty input
  executes no SQL, closes an active query handle, and leaves `rowcount=0`,
  `description=None`, `lastrowid=None`, and no fetchable rows, matching the
  existing empty-batch row-count convention. A failed query close preserves
  the tracked handle and propagates its exception.

- Failed batch execution clears stale cursor result state (#375) — PATCH / backward-compatible bug fix. Public signatures are unchanged; per-statement, transport, and response-parse error paths no longer expose result metadata, row counts, or last-insert IDs from the previous operation. Failure to close the previous query handle aborts the batch without discarding that handle.

- **`Cursor.arraysize` rejects non-integer values in sync and async cursors (#370)** —
  PATCH / backward-compatible bug fix. The public signatures are unchanged;
  validation now enforces the documented positive-integer row-count contract,
  including rejection of floats and booleans. Valid positive integers retain
  their behavior, and invalid assignments leave the previous value unchanged.

- **`executemany_batch()` now closes an active query handle before the batch (#374)** — PATCH /
  backward-compatible bug fix. Public signatures are unchanged; batch execution now matches
  `execute()` by releasing a previous result-set handle before starting another operation.

- **Large integer parameter formatting no longer raises `OverflowError` (#368)** —
  PATCH / backward-compatible bug fix. Integers are rendered directly as decimal
  strings, restoring the documented binding contract without float conversion.
  Float NaN/infinity rejection, boolean formatting, and public signatures are unchanged.

- **`Lob.read(n)` now returns the full requested length (#362)** — PATCH /
  backward-compatible bug fix. The public signature is unchanged; the method
  previously under-returned (silently capped at ~81908 bytes) and now loops to
  satisfy the request, and `read(0)` returns `b""` without a server round-trip.
  Callers that already worked receive strictly more-correct data; no caller
  relying on the documented "read up to `length` bytes" contract is broken.

- **Unknown connection options now emit `UnknownConnectionOptionWarning`
  (#377)** — MINOR / additive. Adds one `__all__` entry
  (`UnknownConnectionOptionWarning`) and no required parameter, so the surface
  change is purely additive (§2). The behavior change is confined to keywords
  that were previously *silently discarded*: they are still discarded, they now
  additionally warn. No supported option changes meaning, and no previously
  working call starts failing under the default warning filters.

  Rejecting unknown options with a `TypeError` was considered and deliberately
  deferred: it would break wrapper layers that forward keywords (connection
  pools, ORM dialects such as `sqlalchemy-cubrid`) and therefore qualifies as a
  breaking change under §3 — it may only land on a major version, via an issue
  tagged `breaking-change` with a migration path. Until then, callers who want
  that strictness opt in per-process with
  `warnings.simplefilter("error", pycubrid.UnknownConnectionOptionWarning)`.

## 8. How to Update the Baseline

The baseline is intentionally checked into the repository so that surface
changes appear as a reviewable diff. Workflow:

```bash
# After an intentional surface change:
python scripts/check_public_api.py --update
git add api-baseline.json
git diff --cached api-baseline.json   # sanity-check the diff
git commit
```

If `compat-check` fails on a pull request that did not intend to change the
surface, the failure is signaling an accidental break — fix the code, do not
update the baseline.
