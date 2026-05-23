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
