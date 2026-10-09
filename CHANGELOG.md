# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/).

## [Unreleased]

### Fixed
- **`scripts/collect_repro.py` classifies bracketed hosts the same way on every
  Python (#753)** — a `CUBRID_TEST_URL` whose bracketed host is not an IPv6
  literal (for example `[bad]` or `[127.0.0.1]`) is now recorded as
  `<unparseable-url-redacted>` on all supported interpreters. Before, early
  3.11 patch releases such as 3.11.1 accepted the host and recorded a
  password-redacted URL instead. The password was redacted either way. Valid
  IPv6 literals such as `[::1]` still parse and keep their host and port. This
  is a contributor tooling fix; the driver is unchanged.

### Security
- **`scripts/collect_repro.py` redacts more forms of the password of a
  `CUBRID_TEST_URL` that `urllib` splits in the wrong place (#777)** — a URL
  without `://` (`u:pw@host/db`, `cubrid:u:pw@host/db`, `cubrid:/u:pw@host/db`),
  or one whose password holds `/`, `?`, `#` or `@` or whose query holds `@`
  (`cubrid://u:Pa@ss/word@host/db`, `cubrid://u:pw@host/db?opt=a@b`), was
  written with all or part of its password in plain text to `metadata.json`
  and `reproduce.md`. `reproduce.md` also quoted endpoint-resolver errors such
  as `invalid port: ... 'Syn7h'`, which echo part of the password. The
  collector now records any URL with an `@` outside the authority `urllib`
  parsed, or longer than 2048 characters, as `<unparseable-url-redacted>`,
  and reports endpoint-resolution failures with a fixed message. For such a
  URL `sanitize()` redacts, raw and percent-encoded/decoded in free text such
  as JUnit failure details, the text after each `:` before each `@` and each
  piece and suffix of it split at `/`, `?`, `#` and `@` that is at least 3
  characters long (the whole text and the `urllib` password are kept at any
  length), plus, within the first 2048 characters, the text after the first
  and the last `:` before the first `@` and each piece of the userinfo up to
  the last `@`. Each URL-derived candidate is also redacted, as written, with CR/CRLF
  turned into LF (as XML parsing does), with tabs, CRs and LFs removed (as
  `urllib` does) and `repr()`-escaped (as tracebacks do), and its lowercased
  form of 3 or more characters is redacted as a whole word, so the lowercased host that the
  integration-test gate quotes in every JUnit error
  (`dba@kc9qmz7:33000/testdb`) no longer reaches `metadata.json`. Each
  host-shaped piece (split at `/`, `?`, `#`, `@`, `%`, `[` and `]`, without a
  trailing `:port`) is lowercased the same way, because `urllib` rewrites the
  port, drops IPv6 brackets and lowercases only the part before a `%zone`. These
  derived fragments may over-redact unrelated diagnostic text; shorter ones
  are not redacted. A URL holding a tab, CR or LF, which `urllib` strips, gets
  the same enumeration. A URL above 2048 characters, or one that would yield
  more than 1024 candidate fragments or 8192 candidate characters, keeps the candidates found in its first 2048
  characters before the limit and is also redacted as a whole string,
  together with the password `urllib` parses from it and any
  `scheme://user:password@` password it contains. Both limits are a hard cap,
  checked before each candidate is added, so a single long tail can no longer
  take seconds to enumerate. `CUBRID_TEST_PASSWORD` stays
  an exact match without these variants, and a password transformed in other
  ways (for example lowercased inside a longer word) is not guaranteed to be
  redacted. The endpoint resolver is no longer trusted for a URL recorded as
  `<unparseable-url-redacted>`: `urllib` could put a password fragment into the
  lowercased host, which reached `reproduce.md` (`CUBRID_TEST_HOST=...`) and the
  `server_identity` endpoint. Endpoint fields and the readiness endpoint are
  also rejected when they hold a known password case-insensitively. Redaction
  patterns are compiled once per configured URL and password. Known limit: a
  JDBC-style URL without `@` (`jdbc:cubrid:host:33000:db:u:PW:`) is not
  recognised, so its password is recorded unless `CUBRID_TEST_PASSWORD` also
  holds it. Valid `cubrid://user:pw@host:port/db` URLs of up to 2048 characters
  are still recorded as `cubrid://user:***@host:port/db`. This is a contributor
  tooling fix; the driver is unchanged.

### Documentation
- **`AGENTS.md` drops stale planning context and volatile counts (#749)** — the
  old "Project Context — Performance Loop System" snapshot (R2/R3 phases, the
  Week 8 decision gate, the #14–#22 issue table and fixed PyMySQL ratios) is
  replaced by short pointers to `ROADMAP.md`, `docs/PERFORMANCE.md` and the
  `cubrid-benchmark` repository. Packet, function-code, data-type and exception
  counts, the Python minimum and the CI Python/CUBRID versions now point to
  their canonical sources (`pycubrid/constants.py`, `pyproject.toml`,
  `docs/CI_POLICY.md`) instead of being copied. CAS protocol invariants,
  workflow, labelling, release and commit guidance are unchanged. Docs only;
  the driver is unchanged.

### CI
- **Cookbook release verification pinned to the cookbook SHA with the PyPI wait** — the `verify-cookbook` call in `.github/workflows/publish-pypi.yml` is pinned to `32e80c6ea9ae78324f764d7b873ffb64b000ddcc` (cubrid-cookbook-python#274), the same commit in pycubrid, sqlalchemy-cubrid and cubrid-mcp-server. The cookbook now waits up to 10 minutes for PyPI to serve the exact requested version before installing, and reports what PyPI served if it times out; this fixes the 1.10.0 post-publish stale-CDN failure that blocked release-please. No runtime change.
- **CHANGELOG lint applies the duplicate-heading check only after the cutoff** —
  `scripts/lint_changelog.py` now gates rule 5 (no repeated `###` heading within one
  version section) by `SECTION_POLICY_CUTOFF` like the section policy: it applies in
  `[Unreleased]` and releases after 1.10.0, and released history up to 1.10.0 is never
  rewritten. Before, the check also ran on older releases. The script is now identical
  to the one in sqlalchemy-cubrid except for the cutoff value. This is contributor
  tooling; the driver is unchanged.
- **CHANGELOG tooling treats fenced code as content** — `scripts/lint_changelog.py` and
  `scripts/compose_release_changelog.py` no longer read a `###` line inside a fenced
  code block as a heading, so a code example in a CHANGELOG entry is no longer rejected
  as an unknown section. The fence lines stay part of the entry. Two cases now fail
  closed in both: an unclosed fence, and a `## [` release header inside an open fence
  (`scripts/extract_release_notes.py` is not fence-aware and would truncate the Release
  body). Only column-0 triple-backtick fences are recognised. Entries without fenced
  code compose and lint exactly as before. `scripts/lint_changelog.py` now has the
  same section parsing as cubrid-mcp-server; the same rules reach that repo and
  cubrid-cookbook-python in follow-up PRs. Contributor tooling only; the driver is
  unchanged.
- **GitHub Release naming and standard release-note sections** — `AGENTS.md` gains a
  "GitHub Release Policy" section: a Release title is exactly its tag `vX.Y.Z`, drafts
  included; tags are never moved or recreated to fix a title; stale drafts are
  classified against the tag and PyPI history and changed only with maintainer
  approval; release notes come from `CHANGELOG.md`. `publish-pypi.yml` keeps creating
  Releases with `--title "$TAG"`, and on resume or recovery it now fails closed through
  the new `scripts/check_release_title.py` when the existing Release has another
  title, instead of reusing it; it never renames a Release.
  `scripts/extract_release_notes.py` appends exactly one
  `**Full Changelog**: …/compare/<previous>...<tag>` line after the unchanged CHANGELOG
  section (none for the first release or when the section already has a compare
  link). `scripts/lint_changelog.py` requires the standard `###` sections (Upgrade
  notes, Added, Changed, Deprecated, Removed, Fixed, Security, Performance,
  Documentation, CI, Tests), each with content and in that order, in `[Unreleased]`
  and releases after 1.10.0; 1.10.0 and older keep their headings. release-please
  `changelog-sections` map commit types to those headings with the same hidden types
  as before, and `scripts/compose_release_changelog.py` merges generated notes into
  the standard headings instead of a `### Conventional commits` block. The
  `[Unreleased]` sections are reordered to the standard order; no entry changed.
  `scripts/check_release_please.cjs` reads the current manifest version instead of the
  stale 1.8.0 bootstrap value. No runtime change.
- **Cookbook release verification pinned to the shared cookbook SHA** — the `verify-cookbook` call to `cubrid-cookbook-python`'s `smoke-test.yml` in `.github/workflows/publish-pypi.yml` is pinned to `bd6749093813d3a447f993fec72ac733adbeae63`, the same commit as the other two package repositories (pycubrid, sqlalchemy-cubrid, cubrid-mcp-server). Since the previous pin the cookbook adds the Python 3.11 cell on release calls (`Smoke Tests (CUBRID 11.4, Python 3.11)`), so the release verification report now needs all three cells (11.2/3.12, 11.4/3.12, 11.4/3.11). `RELEASING.md` lists the third job. No runtime change.
- **Documentation site build on pull requests (#786)** — `ci.yml` gains a
  `docs-build` job that runs the `docs.yml` build (install `docs/requirements.txt`,
  `scripts/generate_llms_full.py`, `mkdocs build --strict`) when `docs/**`,
  `mkdocs.yml`, `scripts/generate_llms_full.py`, `docs.yml` or `ci.yml` change. The new
  `site` path filter selects it, `CI Gate` expects it exactly when selected, and it
  never uploads or deploys anything. Previously a broken strict build or docs-tool pin
  bump only failed on `main`.
- **Pinned docs tools and scan concurrency (#782, #783)** — `docs.yml` installs
  `mkdocs`, `mkdocs-material` and `pymdown-extensions` from the pinned
  `docs/requirements.txt`, which Dependabot now updates. `codeql.yml` and
  `security.yml` gain caller-level `concurrency` that cancels superseded
  pull-request runs only; every other event uses a unique per-run group, since GitHub
  replaces a pending run within one group even without `cancel-in-progress`, so main and
  scheduled runs are never cancelled or dropped. `security.yml` and `maintenance.yml`
  install the `bandit[toml]==1.9.4` pinned in `pyproject.toml`.
- **Scheduled and release validation without duplicate work (#750)** —
  `docs/CI_POLICY.md` now records measured job counts, runner minutes and
  failure yield per event, and which workflow owns each kind of coverage.
  - **Weekly `ci.yml` run.** It no longer repeats the lanes that a successful
    push run of the exact same SHA already ran. Its `detect-changes` job gains
    read-only `actions: read` to look that up. A docs-only head commit, a head
    push run that failed or was cancelled, or a failed lookup selects the lanes
    as before.
  - **`bug-hunt.yml`.** It moves from Monday 04:00 UTC to Thursday 04:00 UTC,
    off the crowded Monday schedules, and keeps its 7-day activity guard.
  - **Release.** `integration-full.yml` runs the Python 3.11/3.14 offline suite
    with the 95% coverage floor (`offline-endpoints`) at the release SHA, and
    `full-matrix-result` requires it. The release previously relied on a
    `ci.yml` push run that it never checked and that a later merge could
    cancel.
  - Pull requests and main pushes are unchanged.
- **Offline tests run on the oldest and newest supported Python (#745)** — the
  `offline-tests` job of `ci.yml` now picks its Python matrix from the event.
  Main pushes, the weekly Monday schedule and manual dispatch run the full
  offline suite (`not integration and not repo_tooling`, 95% coverage floor) on
  Python 3.11 and 3.14 instead of 3.12 alone. Risk-selected PRs run the full
  offline regressions on 3.11 and 3.14. Ordinary PRs keep the single Python
  3.12 smoke cell. Coverage reports, the new coverage artifact and the Codecov
  flag carry the Python version (`coverage-py<version>.xml`,
  `offline-coverage-py<version>`, `offline-py<version>`). `CI Gate` still fails
  when any endpoint cell fails, is cancelled or never runs. Timeouts, pinned
  actions, read-only permissions and the immutable checkout SHA are unchanged.
- **The workflow-reader guard catches split paths and indirect readers (#770)** —
  `tests/test_workflow_path_impact.py` now finds workflow references with the
  regex `\.github['"]?\)?\s*[/,]\s*['"]?workflows` over each module's AST, so
  split paths such as `ROOT / ".github" / "workflows"` and
  `Path(".github") / "workflows"` count, while docstrings and comments do not.
  In a module without a module-level `repo_tooling` mark, module-level
  constants, helpers and fixtures bound to a workflow path are resolved, and
  every test function, `async` test and test-class method that uses one must
  carry `repo_tooling` as a function, class or module mark. Test classes include
  `unittest.TestCase` subclasses of any name (such as `PrTitleValidatorTest` and
  `DocsReasonWorkflowTests`, which read workflows through a `WORKFLOW` constant
  and were previously invisible) and nested `Test*` classes, which honour their
  own marks. A module mark set inside `try`/`except`/`else` or `if` blocks (the
  documented `try: import pytest ... else: pytestmark = ...` pattern) or by an
  annotated `pytestmark: ... = ...` assignment is recognised. Probe cases cover
  each of these shapes, plus docstring and comment mentions that must not
  trigger. Two-step path constants, glob patterns, imported names and tests
  defined inside top-level `if`/`try` blocks remain out of scope. No test
  module needed a new mark; this is a test-only change.

### Tests
- **Makefile signal-cleanup test no longer depends on how pytest was launched
  (#775)** — `test_signal_cleanup_preserves_volumes[SIGINT-*]` failed every time
  pytest ran as a background job of a non-interactive shell (for example two
  suites started with `&`). Such jobs inherit SIGINT as ignored, and POSIX does
  not let a shell trap a signal that was ignored on entry, so the recipe's
  `trap ... INT TERM` never ran. The test now starts `make` with SIGINT and
  SIGTERM at their default dispositions, passes the pytest stub so a regression
  cannot fall through to a real pytest run, and raises its wait bounds from 5 s
  to 60 s. The Python stub now blocks only for the `wait_for_cubrid.py`
  readiness probe and exits 0 for any other call, so a skipped trap fails fast
  (under 1 s) with the real assertion instead of hanging until the 60 s wait
  expires. The assertions are unchanged, and the `Makefile` is unchanged.

## [1.10.0] - 2026-10-08

### Upgrade notes
- **Python 3.11 or newer is required (#684).** Python 3.10 reached upstream end
  of life on 2026-10-01 and its retirement was announced in 1.9.0. On Python 3.10
  `pip` keeps installing 1.9.x, the last line that supports it; only the latest
  release line receives fixes, so 1.9.x gets no further releases. Upgrade Python,
  recreate your virtual environment and validate your application before upgrading.

### Removed
- **Python 3.10 support (#684)** — `requires-python` is `>=3.11` and the 3.10
  classifier is gone. CI no longer tests 3.10: the oldest integration pair is
  Python 3.11 × CUBRID 10.2, the full matrix covers Python 3.11–3.14 and the TLS
  lanes run on 3.11 and 3.14. No driver behavior changes on Python 3.11 or newer.

### Changed
- **The Python 3.10-only async TLS preflight probe is removed (#685)** —
  `AsyncConnection` no longer carries the blocking certificate-verification probe
  that ran before `loop.start_tls()` on Python 3.10 (#156). It already returned
  immediately on Python 3.11 and newer, so nothing changes there: the async TLS
  upgrade, its handshake bound and its error surface are the same. The private
  helpers `_maybe_probe_tls_verification`, `_probe_tls_verification_sync` and
  `_recv_exact_sync` are gone with their Python 3.10-only tests.
- **Async deadlines handle the built-in `TimeoutError` and keep
  `asyncio.wait_for()` (#687, #744)** — the async TCP connect, connect handshake
  and request deadlines catch the built-in `TimeoutError` (`asyncio.TimeoutError`
  is the same class since Python 3.11). They stay `asyncio.wait_for()` calls
  rather than `asyncio.timeout()` blocks while Python 3.11.0–3.11.2 are
  supported: there, a `timeout()` block entered by a task that was already
  cancelled (for example cleanup after a caught `CancelledError`) re-raises
  `CancelledError` instead of `TimeoutError` when it expires
  ([python/cpython#102780](https://github.com/python/cpython/issues/102780),
  fixed in 3.11.3), so a driver deadline would have surfaced as a cancellation
  instead of `OperationalError`. Behavior is the same as 1.9.x: a driver deadline
  raises `OperationalError` with the same messages, a transport-raised
  `TimeoutError` is still a socket failure that retires the session, a
  `TimeoutError` from a callback after a complete reply still propagates
  unchanged, and a new caller cancellation still raises `asyncio.CancelledError`
  after retiring the session. `None` means no deadline. Zero keeps its
  `wait_for()` meaning: the fresh, not-yet-started operation is cancelled before
  it runs, so `connect_timeout=0` fails before the TCP connect is attempted and
  `read_timeout=0` fails before any handshake bytes are sent, both with
  `OperationalError`.
- **Ruff and mypy target Python 3.11 (#688)** — `target-version = "py311"` and
  `python_version = "3.11"` in `pyproject.toml`, matching the minimum supported
  version. The rule selection is unchanged and no source needed a fix.

### Tests
- **CI fails when a Korean document drifts from its English source (#716)** —
  `scripts/check_docs_translation.py` compares every `docs/<name>.md` with
  `docs/ko/<name>.md` and fails on a missing Korean file or a different number
  of headings (levels 2-4), fenced code blocks or table rows; the lint job runs
  it on every event. Until now only README drift was checked, and six guides
  had drifted unnoticed. Documents that are deliberately English-only are
  listed in the script with a reason. The check compares structure, not wording.
- **The official-driver differential lane runs on Python 3.11 (#683)** — the
  pinned CUBRIDdb oracle (cubrid-python `e75ec36`, CCI `7d1eb8f`) was built and
  compared on Python 3.10 only. The `official-differential` job in `ci.yml` and
  `integration-full.yml`, the claims fixture and the generated compatibility
  summaries now use Python 3.11, so the lane no longer depends on the
  interpreter that 1.10.0 retires. Claims, source pins and the required servers
  (10.2 and 11.4) are unchanged; earlier evidence recorded on Python 3.10.12
  stays in `docs/UPSTREAM_COMPATIBILITY.md` as history.

### Documentation
- **License inventory check tolerates exact-pin bumps (#771 follow-up)** —
  `tests/test_third_party_licenses.py` now checks exact `==` dev pins for
  presence only, so a Dependabot bump such as `ruff==0.16.11` no longer fails
  the repository-tooling tests until `THIRD_PARTY_LICENSES.md` is regenerated;
  declared ranges (e.g. `mutmut>=3.8,<4`) are still enforced, and the reviewed
  docutils row must keep its "Needs review" category.
- **Reproducible third-party license inventory (#735)** — `THIRD_PARTY_LICENSES.md`
  is regenerated from fresh `.[dev]` and `.[mutation]` environments at a recorded
  commit, Python and OS by the new standard-library
  `scripts/generate_third_party_licenses.py`. MPL-2.0 packages (`certifi`,
  `hypothesis`, `pathspec`) are now classified separately from permissive
  licenses instead of under a blanket "no copyleft" statement, GPL-family
  metadata and any partly unrecognised license expression are flagged for review
  (docutils is resolved from its own `COPYING`),
  and the document separates what pycubrid uses from what it distributes. The
  Windows-only `tzdata` runtime dependency stays explicit.
  `tests/test_third_party_licenses.py` fails when a declared dependency is missing,
  a recorded version falls outside its declared range, or a row's category
  disagrees with the generator. No runtime change.
- **The README and quickstart state Python 3.11 or later (#699)** — the
  requirement lines, the supported range (3.11–3.14) and the FAQ answer in
  `README.md` and `docs/README.ko.md`, and the prerequisites in
  `docs/quickstart.md` and `docs/ko/quickstart.md`. The Python 3.10 retirement
  notice in both READMEs now describes the requirement as in effect from 1.10.0.
- **Python 3.10 async TLS probe guidance is removed (#686)** — the preflight
  probe is gone (#685), so the README, `SECURITY.md`, the connection, protocol,
  development, example, API, PRD and support-matrix pages and their Korean
  counterparts no longer describe it or the Python 3.10 `start_tls()` hang it
  worked around. The troubleshooting section "Async TLS Handshake Hangs on
  Python 3.10" is deleted, with the links to it in the translated READMEs; the
  sync driver's Python 3.10 `wrap_socket()` reset note goes with it.
- **In-page links work on the documentation site (#728)** — the site used the
  default heading slugifier, which drops non-ASCII characters, so Korean
  headings got ids such as `_2` and every Korean table of contents entry was
  dead; English links written for GitHub's slugs (`#tls--ssl`) missed as well
  (198 broken anchors). `mkdocs.yml` now uses the Unicode-aware slugifier from
  `pymdown-extensions`, the two anchors that were still wrong are fixed, and
  `validation.links.anchors: warn` makes the strict build fail on a new broken
  anchor.
- **Korean pages reviewed against the English wording (#726)** — the structure
  check cannot see a missing or outdated sentence. A sentence-level review of
  seven Korean guides corrected 72 passages: `API_REFERENCE` (21, including the
  missing `Lob.read()` paragraph on repeated `LOB_READ` round-trips),
  `UPSTREAM_COMPATIBILITY` (20), `DEVELOPMENT` (13, including the async TLS
  matrix tests), `PROTOCOL` (8), `PARAMETER_BINDING` (6, replacing an outdated
  description of escape-mode negotiation), `TROUBLESHOOTING` (4) and
  `CONNECTION` (2). The translations of `PRD` and `PREPARED_BINDING_DESIGN`
  added in this release were reviewed the same way (1 and 15 corrections,
  mostly literal renderings that read ambiguously). The banner on every Korean
  page now says CI checks the structure. English is unchanged.
- **PRD no longer quotes figures that every release makes false (#724)** —
  `docs/PRD.md` said version 1.8.0, "770 offline tests / 811 total", "97.29%
  coverage", "10 modules", "6 guide files" and "release workflow on tag". The
  version, test and coverage figures now link to PyPI, the test tree and
  Codecov, the per-file test counts are dropped, and the release row describes
  the release-PR flow. The documentation home page states SQLAlchemy 2.0–2.1 for
  sqlalchemy-cubrid, as that project does. English and Korean.
- **PRD and the typed CAS binding design are available in Korean (#715)** —
  `docs/ko/PRD.md` and `docs/ko/PREPARED_BINDING_DESIGN.md` are added, both PRD
  pages join the site navigation, and the Korean home page and CI policy carry
  the same translation header as the other Korean pages. With these, every
  document under `docs/` has a Korean counterpart and the structure check has no
  exception left.
- **Korean documentation home page and one Korean navigation section (#714)** —
  `docs/ko/index.md` is the Korean landing page, linked from the English home
  page and back. The Korean guides were spread over three navigation groups;
  they are now one "한국어" section with the same grouping as the English
  navigation, and every Korean page is reachable from it.
- **Korean support matrix and CI policy match the English documents (#713)** —
  `docs/ko/SUPPORT_MATRIX.md` gains the CI matrix table, the "Server Behavior
  Differences Between CUBRID Versions" table and the unknown-option row, and
  loses a Korean-only closing section; `docs/ko/CI_POLICY.md` is a full
  translation instead of a summary. Both CI policy pages are in the site
  navigation. The full-integration count in the support matrix is corrected to
  16 (four Python versions × four CUBRID versions) in both languages.
- **Korean connection, development and troubleshooting guides are in sync again
  (#712)** — four sections existed only in English: unknown connection options
  (`docs/ko/CONNECTION.md`), "Connection Option Has No Effect"
  (`docs/ko/TROUBLESHOOTING.md`), and the backslash-escape-mode pin and mutation
  testing (`docs/ko/DEVELOPMENT.md`). They are translated; the English text is
  unchanged.
- **`get_error_description()` is documented, and the Korean API reference is
  complete again (#711)** — `docs/API_REFERENCE.md` covered every public name
  except `pycubrid.get_error_description`; it now has its own section in English
  and Korean. The Korean reference also gains the sections it was missing:
  unknown connection options, `UnknownConnectionOptionWarning`, the native
  prepared example and the wrapper cursor's `next()` alias (also added to the
  English text). No behavior change.

### Conventional commits

#### Features

* **python:** require Python 3.11 or later ([#708](https://github.com/cubrid-lab/pycubrid/issues/708)) ([5b3d9ec](https://github.com/cubrid-lab/pycubrid/commit/5b3d9ecfe8632c5194d1201e9540e4fdf8b8d98e)), closes [#684](https://github.com/cubrid-lab/pycubrid/issues/684)


#### Bug Fixes

* **aio:** restore asyncio.wait_for for driver deadlines on Python 3.11.0–3.11.2 ([#754](https://github.com/cubrid-lab/pycubrid/issues/754)) ([1a44790](https://github.com/cubrid-lab/pycubrid/commit/1a44790cd5e3a6c37357fa42783ef86533106fab))


#### Documentation

* **agents:** align contributor ownership and good-first-issue guardrails ([#752](https://github.com/cubrid-lab/pycubrid/issues/752)) ([fb3da8c](https://github.com/cubrid-lab/pycubrid/commit/fb3da8c94b86cf1ad576989f84c6ba465b9e0894)), closes [#743](https://github.com/cubrid-lab/pycubrid/issues/743)
* **api:** document get_error_description and close the Korean API reference gaps ([#717](https://github.com/cubrid-lab/pycubrid/issues/717)) ([221339e](https://github.com/cubrid-lab/pycubrid/commit/221339eec34912ec0a0adec665673a25541fccdf)), closes [#711](https://github.com/cubrid-lab/pycubrid/issues/711)
* **ci:** correct what enable-cache auto does on the release path ([#764](https://github.com/cubrid-lab/pycubrid/issues/764)) ([e0e475b](https://github.com/cubrid-lab/pycubrid/commit/e0e475b62741f22a3d6deeed56c7e8ba1cbf3007)), closes [#759](https://github.com/cubrid-lab/pycubrid/issues/759)
* **i18n:** add the Korean documentation home page ([#720](https://github.com/cubrid-lab/pycubrid/issues/720)) ([73e4ea7](https://github.com/cubrid-lab/pycubrid/commit/73e4ea756a1efda3d09e43e22b6ff13aa585174a)), closes [#714](https://github.com/cubrid-lab/pycubrid/issues/714)
* **i18n:** correct Korean sections that fell behind the English wording ([#727](https://github.com/cubrid-lab/pycubrid/issues/727)) ([ed6e7d9](https://github.com/cubrid-lab/pycubrid/commit/ed6e7d934e2931ebd286761e5b9a69e327ce9db9)), closes [#726](https://github.com/cubrid-lab/pycubrid/issues/726)
* **i18n:** synchronize the Korean CONNECTION, DEVELOPMENT and TROUBLESHOOTING guides ([#718](https://github.com/cubrid-lab/pycubrid/issues/718)) ([16a0665](https://github.com/cubrid-lab/pycubrid/commit/16a06659597ea9a93b899291a480d0a2e1050d8b)), closes [#712](https://github.com/cubrid-lab/pycubrid/issues/712)
* **i18n:** synchronize the Korean SUPPORT_MATRIX and translate CI_POLICY in full ([#719](https://github.com/cubrid-lab/pycubrid/issues/719)) ([66e2c5e](https://github.com/cubrid-lab/pycubrid/commit/66e2c5e346c3f270bff3cfc0c0b2797648bccc2a)), closes [#713](https://github.com/cubrid-lab/pycubrid/issues/713)
* **i18n:** translate PRD and PREPARED_BINDING_DESIGN into Korean ([#722](https://github.com/cubrid-lab/pycubrid/issues/722)) ([328720d](https://github.com/cubrid-lab/pycubrid/commit/328720d4044d53db8288f95f74f8f9e5894b6a36)), closes [#715](https://github.com/cubrid-lab/pycubrid/issues/715)
* **licenses:** make the third-party license inventory reproducible and accurate ([#771](https://github.com/cubrid-lab/pycubrid/issues/771)) ([aacb5b5](https://github.com/cubrid-lab/pycubrid/commit/aacb5b588be7a15f4126a76a20f77334828fd5a4))
* **prd:** replace stale version, test and coverage figures with links ([#725](https://github.com/cubrid-lab/pycubrid/issues/725)) ([8a3304b](https://github.com/cubrid-lab/pycubrid/commit/8a3304bc7acebeb4368e24d3275940e6ef3839ed)), closes [#724](https://github.com/cubrid-lab/pycubrid/issues/724)
* **python:** state Python 3.11 or later in the README and quickstart ([#739](https://github.com/cubrid-lab/pycubrid/issues/739)) ([27e35be](https://github.com/cubrid-lab/pycubrid/commit/27e35be9eee6518c1c673e51195fa25954dd5da4)), closes [#699](https://github.com/cubrid-lab/pycubrid/issues/699)
* **site:** generate heading anchors that match the links in the documents ([#729](https://github.com/cubrid-lab/pycubrid/issues/729)) ([c2a4f1d](https://github.com/cubrid-lab/pycubrid/commit/c2a4f1db875c65a3c497daf6427711d643401dec)), closes [#728](https://github.com/cubrid-lab/pycubrid/issues/728)
* **tls:** remove Python 3.10 preflight probe guidance ([#737](https://github.com/cubrid-lab/pycubrid/issues/737)) ([4df5db0](https://github.com/cubrid-lab/pycubrid/commit/4df5db0331c2072440d22b0b08ed20323ecaec93)), closes [#686](https://github.com/cubrid-lab/pycubrid/issues/686)

## [1.9.0] - 2026-10-04

### Upgrade notes
Behavior changes you may notice (details in the entries below):
- **`pycubrid.compat` is provisional.** Most of the opt-in compatibility
  namespaces is new in this release and still follows measurements of the
  official driver. Names first released in 1.9.0 or later may change or be
  removed in a later MINOR release, announced here; names already in 1.8.0, and
  all of `pycubrid` and `pycubrid.aio`, keep the normal 1.x guarantee. See
  [RELEASE_POLICY.md](RELEASE_POLICY.md#2-semantic-versioning-rules).
- A `DELETE`/`UPDATE` of a parent row that a foreign key still references
  (`-924`) and a `TRUNCATE` of a referenced parent table (`-1284`) now raise
  `IntegrityError` (SQLSTATE `23000`, still a `DatabaseError` subclass) instead
  of a generic `DatabaseError`. (#493)
- On ordinary sync and async cursors, a complete reply holding a value Python
  cannot represent now raises `DataError` and keeps the session, instead of
  `OperationalError('malformed response from broker')` and a closed connection:
  zero `DATE`/`DATETIME`/`TIMESTAMP` values (#512), undecodable character
  values, and invalid JSON decoded by the built-in `json.loads` (#543). Errors
  from a caller-supplied `json_deserializer` are not reclassified, and the
  prepared API in `pycubrid.compat.native` stays fail-closed: it still raises
  `OperationalError` and closes the session.
- `CALL`, `callproc()` and `EVALUATE` results and `NULL`-typed columns return
  decoded values (`42`, a `datetime`) instead of raw `bytes`. (#542)
- An `execute()` that fails after the previous query handle was closed no
  longer leaves the previous statement's result on the cursor: `description` is
  `None`, `rowcount` is `-1` and nothing is fetchable. If closing the previous
  handle itself fails, `execute()` raises and the buffered result is kept.
  (#373)
- `decimal.Decimal` parameters are sent in plain fixed-point notation, so values
  such as `Decimal("1E-7")` stay `NUMERIC` instead of coming back as `float`.
- Negative, NaN and infinite `connect_timeout`/`read_timeout` values are
  rejected before a socket is opened. (#367)
- `Lob.read()`/`Lob.write()` raise `InterfaceError` for an `offset` or `length`
  that is not a plain `int`, including `bool`. (#449)
- `DBAPIType` no longer compares equal to `bool` values: `STRING == True` is
  `False`. (#369)
- Ordinary sync and async `fetchmany(size)` require a plain Python `int`
  when a size is supplied: floats, booleans and integer subclasses now raise
  `ProgrammingError` before consuming rows or requesting a page. Use a plain
  integer row count instead. Omitted/`None` sizes still use `arraysize`, and
  zero/negative integers still return `[]`. (#371) Positive-only validation
  is reserved for a future major release ([#678](https://github.com/cubrid-lab/pycubrid/issues/678)).
- Sync `connect(..., ssl=...)` without `read_timeout` gives up on a stalled TLS
  handshake after 10 seconds with `OperationalError` instead of waiting forever.
  (#535)
- Python 3.10 support is deprecated; see Deprecated below.

### Deprecated
- **Python 3.10 support** — advance notice for the 1.9.x release. Python
  3.10 reached upstream end of life on 2026-10-01 ([PEP 619](https://peps.python.org/pep-0619/#310-lifespan)).
  The current 1.8.x line and 1.9.x retain Python 3.10 support. After this notice
  ships in 1.9.0, the following minor release (planned 1.10.0) will require
  Python >=3.11. Upgrade Python, recreate your virtual environment and validate
  your application before upgrading. Current package metadata, CI coverage and
  runtime behavior remain unchanged; see [support matrix](docs/SUPPORT_MATRIX.md)
  and [roadmap](ROADMAP.md).

### Release automation

- Replace the release PR preparer with pinned release-please; preserve curated Upgrade notes and guarded publication, and freeze reviewed candidates before editing.

### Added
- **Runnable README Quick Start examples (#327)** — add short parameterized CRUD,
  commit/rollback and `ProgrammingError` examples with shared scratch-table setup
  and cleanup, mirrored in all five maintained translations and verified on
  CUBRID 11.4. No driver behavior, API, dependency or support change.
- **Python 3.15 preview preparation** — add a manual-only Ubuntu/standard-GIL
  offline and wheel/sdist installation lane at an immutable commit. Python 3.15
  is not yet officially supported; final-runtime and live CUBRID evidence are
  required before promotion. Routine PR test frequency and supported versions
  remain unchanged.
- **Native connection utilities (#666)** — add opt-in zero-argument full
  `server_version()`, truthful frozen own-driver `client_version()` and integer
  query `ping()`, plus the wrapper's two server/ping delegates. Requests stay on
  the exact live physical owner without retry/reconnect, use effective mode and
  respect private schema/result cleanup guards. Client identities are not a
  matching-ID or version-format promise. Ordinary/async APIs and defaults remain
  unchanged; this additive subset does not certify full native error/recovery parity.
- **Real terminal walkthroughs (#320)** — replace the illustrative README GIF
  with a recorded sync/async query run and add a Quick Start CRUD/context-manager
  video. Editable VHS tapes, asserted demo code and source-wheel provenance
  distinguish local-build evidence from a PyPI release or full compatibility
  certificate. No driver API, dependency, support or publication change.
- **Wrapper transaction delegates (#662)** — the opt-in sync CUBRIDdb-style
  connection gains zero-argument `commit()` and `rollback()`, forwarding once to
  its native owner with `None` returns and unchanged error propagation. Existing
  successful-boundary result rules apply; rollback can invalidate fetching while
  wrapper rowcount/description snapshots remain. Manual mode is explicit, and
  defaults, ordinary/async APIs, recovery logic and thread-sharing promises are
  unchanged.
- **Native LOB file transfer (#443)** — sync native holders gain positional
  `imports(file, type="B")` and `export(file)` for raw BLOB/CLOB bytes in
  bounded chunks, preserving byte position. Import stages a replacement until
  the complete input closes; export publishes a sibling temporary file only
  after successful write/flush/close. Exact-string paths, local error codes,
  empty populated output and same-session ownership are explicit safety
  differences from the official C extension. Pinned owned 10.2/11.4 comparisons
  cover successful stored-column transfers; empty output is candidate-only
  evidence, not official empty-export parity. No general filesystem, unsafe
  C-failure or whole-driver parity is claimed. Ordinary/async/wrapper APIs,
  dependencies, defaults and release publication are unchanged.
- **Native cursor positioning (#444)** — sync native cursors gain
  `data_seek`, `row_seek` and `row_tell` with the official safe dual counters,
  relative-boundary clamp/error behavior and one retained response page. Cache
  misses fetch an absolute position without query replay or reconnect. Explicit
  manual autocommitFalse before preparation is the measured broker-backed
  cross-page prerequisite; unchanged defaultTrue can release a server result
  after its last batch. Ordered 257/1537-row and tuple/dict wrapper `_cs` cases
  distinguish public results from candidate-only page/stream metrics. Local
  args, selector `__index__` callback/overflow and owner/result safety differences are explicit.
  Metadata, ordinary/async APIs, wrapper public names, dependencies and release
  publication are unchanged.
- **Native extended column metadata (#445)** — the opt-in sync native cursor
  gains `result_info([n])`, returning cached 15-field tuples with measured CCI
  types, integer constraint flags and actual names/default strings. All/one
  selection, zero-row SELECT, zero-column DML and C-int32/closed-cursor argument
  precedence are explicit. The getter leaves row position and descriptions
  unchanged; same-owner metadata remains after EOF/commit/rollback, while failed
  execution attempts hide it. Local InterfaceError `.code` retains the existing
  message-only args as a classified native difference. Owned 10.2/11.4 and pinned
  official comparisons cover this subset, including collection/JSON types and
  Unicode metadata, not non-UTF-8 or complete driver parity. Ordinary/async APIs,
  dependencies and release publication are unchanged.
- **Discoverable Korean performance guide (#313)** — the Korean README now
  links to the existing `docs/ko/PERFORMANCE.md` translation, with working
  section anchors and a benchmark table identical to the English source.
  Historical benchmark values, methodology and profiling commands are retained;
  this documentation update adds no new performance measurements.
- **Qualified wrapper row cursors (#466)** — `cubriddb.Connection.cursor()` now
  selects tuple or exact-name dictionary rows; direct qualified
  `pycubrid.compat.cursors.Cursor/DictCursor` construction and connection-local
  fetch conversion are available. SELECT uses the official seven-field
  description, preserving Unicode, SQL NULL versus empty text, duplicate-name
  overwrite, and the official falsey bulk-fetch versus `None` iteration stop.
  The small `execute(query, args=None, set_type=None)` bridge reuses existing
  native INT32/string/NULL prepared binding; non-None `set_type`, mappings,
  collections/LOB arguments and `executemany` are not added. Non-SELECT
  description stays safely `None` instead of reproducing the official
  extension's missing-attribute state after reprepare. Ordinary and async
  APIs, dependencies and release publication are unchanged. Collected wrapper
  cursors now attempt best-effort same-session native-handle cleanup without
  commit or reconnect; explicit close remains the deterministic path. Offline, owned
  CUBRID 10.2/11.4 and pinned official differential cases cover this subset.
- **Native cached settings and effective mode setters (#467)** — the explicit
  sync `pycubrid.compat.native.connection` now exposes writable cached
  `autocommit`, `isolation_level`, `lock_timeout` and `max_string_len` members;
  direct assignment sends no packet. Separate positional-only
  `set_autocommit(bool)` and `set_isolation_level(4/5/6)` change the effective
  mode and symbolic cache only on success. Matching pinned CCI, autocommit
  changes are local unless the actual mode changes during an active
  transaction, when it commits first; an isolation change does not commit.
  Initial lock/max/isolation values come from the broker, with only a complete
  max-string server error falling back to 0. The pinned official extension's
  fresh level-4 `UNKNOWN` text quirk is retained and corrected by its setter.
  The `compat.cubriddb` wrapper adds a bool-validated effective autocommit
  setter/property and a raw-cache getter. Manual-mode LOB fetches retain their
  conservative non-transferable provenance after later commit. Ordinary
  driver defaults, ordinary/async setting APIs, dependencies and release
  publication are unchanged. Offline and owned CUBRID 10.2/11.4 cases plus a
  pinned official-driver differential record the bounded behavior.
- **Explicit native LOB byte-position stream (#442)** — `pycubrid.compat.native.lob`
  now adds sync-only `write(data, type="B")`, `read(length=0)` and
  `seek(offset, whence=SEEK_CUR)`, plus the three `SEEK_*` constants. BLOB and
  CLOB text is UTF-8 as in the official Python 3 extension; seek positions
  count bytes and SEEK_END subtracts its offset. Writes create a BLOB or CLOB
  lazily and append only at the tracked end; reads span the broker's chunk cap.
  A successful short read advances the byte position immediately, even if a
  later LOB_READ reply fails, so a still-live session resumes without repeating
  accepted bytes.
  The existing physical-session/origin fences apply to read/write as well as
  bind, and created temporary handles remain single-use after autocommit bind.
  Deliberate safe differences are local rejection of non-end writes and invalid
  positions, empty/EOF returning `""`, and clamping oversized reads; terminal
  close remains unchanged. Ordinary `Lob.read(length, offset=0)` and
  `write(bytes, offset=0)`, async, file import/export and dependencies are
  unchanged. Focused offline tests, CUBRID 10.2/11.4 live storage checks and
  safe official C-extension differential cases cover the new surface.
- **Advisory nightly downstream corpus (#356)** — `bug-hunt.yml` now runs three
  isolated CUBRID 11.4 dogfood lanes against the exact pycubrid workflow commit:
  SQLAlchemy ORM and pool tests, MCP live tool tests, and Cookbook AI-agent and
  async-worker tests. The selected test workloads use separate JUnit reports;
  missing, failed or all-skipped workloads fail their advisory lane, while
  legitimate empty-schema MCP skips are reported. Each lane verifies the
  installed driver's Git origin/commit and import path after all dependencies
  are installed, records the downstream commit and server version, and uploads
  evidence even on failure. The MCP step also requires a separate no-skip
  `mcp-concurrency.xml` report from simultaneous in-process tool handlers sharing
  one physical `Database` connection under its existing `RLock`. Either pytest
  run failing keeps the step failed. This bounded check covers trace/query
  serialization, distinct correct responses and real cursor cleanup, not stdio
  concurrency, pooling or per-request transaction isolation. PR, release and
  full-matrix gates, driver behavior and dependencies are unchanged.
- **Deferred-close flush safety matrix (#585)** — real TCP replay checks four
  generation/reconnect/native-error/transport-error properties for sync and
  async commit and rollback. Removing the generation filter or flush guards
  now fails these regressions; production behavior and dependencies are unchanged.
- **Shared scalar-formatting coverage and observable escape recovery (#563)** —
  Duplicate async scalar examples now use the existing pure-function golden
  matrix under both escape modes, with small sync/async adapter wiring checks.
  Explicit-mode recovery cases observe bound SQL and TCP sessions instead of
  incidental private flags. Hostile inputs, unknown-mode rejection, generation
  fences and malformed-reply safety tests remain; runtime behavior is unchanged.
- **Native LOB handle fetch and bind (#441)** — `pycubrid.compat.native`
  fetches and binds BLOB/CLOB handles with the official names:
  `connection.lob()` (or `native.lob(conn)`), `cursor.fetch_lob(col, lob, /)`
  and `cursor.bind_lob(index, lob, /)`, plus a local-only `lob.close()`, sync
  only. `fetch_lob()` consumes the next row like `fetch_row()` and takes the
  BLOB/CLOB type from the requested column (the official driver reads column
  1); a non-int column raises `TypeError` first and, at the end of the
  result, it returns `None` before the column range or type or the lob's
  state is checked, as official; otherwise a non-LOB column raises
  `ProgrammingError` without consuming the row, and a NULL cell leaves the
  lob empty. `bind_lob()` sends the official bind bytes (when the official
  lob type matches the column); a non-lob argument raises `TypeError` as
  official. A fetched handle of a committed row may be bound again, on
  another connection and after its own connection closed or reconnected, as
  official; the server stores a copy. An empty or closed lob raises
  `InterfaceError` before any request, and `fetch_lob()` fills only an open
  lob of its own connection. Arguments are checked in the official order
  (`TypeError` for a non-int index or column first). A complete reply whose
  cell is not a handle of the column's LOB type raises `DataError` without
  consuming the row and keeps the session; damaged handle framing raises
  `OperationalError` and retires the uncertain physical session. Live 10.2/11.4 round trips cover BLOB and
  UTF-8/CJK CLOB values from 0 bytes to 1 MB (above the broker's single-read
  cap), NULL and mixed columns, repeated execution, cross-connection binds
  and reconnect. Fourteen new official differential claims (eleven match, three
  classified deviations) pass
  on CUBRID 10.2 and 11.4. LOB write/read/seek and files remain #442/#443.
- **Native collection binding (#440)** — `pycubrid.compat.native` binds
  SET, MULTISET and SEQUENCE parameter values with the official names:
  `connection.set()` (or `native.set(conn)`), `set.imports(data, type, /, *,
  kind=SET)` and `cursor.bind_set(index, s, /)`, sync only. As in the
  official driver, every element is sent as a STRING element whatever the
  element type code (any code except BIT/VARBIT, which raise
  `NotSupportedError`), and the default `kind=SET` sends the official request
  bytes, checked against a captured official request. `kind=MULTISET` keeps
  duplicates and `kind=SEQUENCE` keeps duplicates and order; MULTISET is sent
  as SEQUENCE because CUBRID 10.2/11.4 brokers reject the MULTISET bind kind
  (-454). `data` must be a tuple (`InterfaceError` otherwise, as official);
  `None` is a NULL element, and the text `'NULL'`, empty strings and Python
  `int` elements (INT only, signed 64-bit) are accepted, while an element
  with a NUL is rejected, as classified deviations. A set that was never
  imported binds SQL NULL, as official. Invalid input fails before I/O and
  leaves the set and the bound slot unchanged; a server conversion error
  keeps the prepared handle usable. Fourteen new official differential
  claims (seven match, seven classified deviations, including the error
  classes) pass on CUBRID 10.2 and 11.4. The wrapper
  `execute(..., set_type)`/`executemany` collection shapes are not provided
  (#610).
- **`charset` connection option (#86)** — `pycubrid.connect()`,
  `pycubrid.aio.connect()`, `pycubrid.compat.native.connect()` and
  `cubriddb.Connection(charset=...)` (previously `"utf8"` only) accept
  `charset` (default `"utf-8"`): a Python codec or the CUBRID names `utf8`,
  `euckr`, `iso88591`. It is validated before any socket work (`TypeError` for
  a non-string; `ValueError` for an unknown codec, CUBRID `binary` or a codec
  that is not ASCII-transparent, such as UTF-16/32, Shift_JIS, Big5, GBK or
  CP949; `DataError` for unencodable credentials) and kept across reconnects.
  SQL text with rendered parameters, batch and schema-info arguments, prepared
  strings and `OPEN_DATABASE` credentials are encoded with it before anything
  is sent; an unencodable character raises `DataError` naming the codec and
  position, nothing of that request is sent and the session stays usable.
  Character values, `ENUM` and collection elements, column/table names and
  defaults are decoded strictly (`DataError` naming the codec), error messages
  with replacement. Fetched `JSON` stays UTF-8 (JSON parameters are SQL text); `NUMERIC`, timezone names, version
  strings and LOB contents are unaffected (`CLOB` bytes are in the column
  charset). The broker does no conversion, so the codec must match the
  database charset; a `CHARSET utf8` column in an EUC-KR database raises
  `DataError` under `charset="euckr"` (convert with `CAST(... CHARSET euckr)`).
  With the default UTF-8 codec, request bytes are unchanged except two edge
  cases: a column name that is not valid UTF-8 now raises `DataError` and an
  ordinary cursor keeps the session (previously `OperationalError('malformed response from broker')`
  and a closed connection), and a database/user/password longer than its
  32-byte `OPEN_DATABASE` field is cut on a character boundary instead of
  mid-character. With `euc_kr`, Hangul outside KS X 1001 (such as 똠), which
  Python would send as an 8-byte makeup sequence, is rejected as unencodable,
  and stored Hangul filler (U+3164) and jamo read back as separate characters,
  as CUBRID stores them.
  LOB file locators, which embed the table name, decode with the connection
  codec and `errors="replace"`. `charset=None` means the default, and a CUBRID
  locale such as `"ko_KR.euckr"` is accepted. `get_schema_info()` checks its arguments before sending, so an
  unencodable table or column pattern no longer closes the connection. A new `integration-charset` CI job runs the live round trips
  against CUBRID 11.4 created with `CUBRID_LOCALE=ko_KR.euckr`.
- **Official-driver differential gate (#446)** — official behavior that
  pycubrid claims is now listed in `tests/fixtures/official_differential_claims.json`.
  Each claim is a `match`, or a `deviation` with a reason, an issue and both
  drivers' observations, and names its API inventory and upstream scenario ids.
  The first 18 claims are the #344 stored-type fetches, a static scalar row and
  its `description`, and the #439 prepared INT/string subset. They include
  three classified deviations: MONETARY, description size/null_ok, and native
  `bind_param(None)`. `tests/test_official_differential.py` (replacing
  `tests/test_cubriddb_differential.py`) runs one live case per claim through
  pycubrid and the official driver. `scripts/build_official_oracle.py` builds
  that driver from verified cubrid-python `e75ec36` and CCI `7d1eb8f` pins
  with CMake directly, without patching upstream, and records the extension
  SHA-256. A new required `official-differential` CI job (Python 3.10, CUBRID
  10.2 and 11.4, cached oracle, skipped only for docs-only changes) is part of
  the CI Gate and of the nightly/release full matrix. In that job, a missing
  driver, zero cases, any skip, a mismatch or an unclassified divergence fails.
  Evidence is uploaded as the `official-differential-evidence` artifact.
  `scripts/check_official_differential.py` validates the ledger offline and
  the evidence in CI, and generates the claim counts in the compatibility
  guides. `check_integration_lanes.py` gains an `official` lane, and
  `--lane official` accepts no skip.
- **Typed collection parameters for ordinary cursors (#567)** — new
  `pycubrid.types.Set`, `Multiset` and `Sequence` (also exported from
  `pycubrid`) wrap an immutable tuple of elements and bind through `execute()`
  and `executemany()` on ordinary sync and async cursors as `SET{...}`,
  `MULTISET{...}` and `SEQUENCE{...}` literals, so SQLAlchemy and other DB-API
  callers can bind CUBRID collections (cubrid-lab/sqlalchemy-cubrid#484).
  Every element goes through the hardened scalar renderer (#518, #528), so
  element types are the scalar parameter types and overridden methods on an
  element subclass never reach the SQL; nested collections raise
  `ProgrammingError`, and the classes cannot be subclassed. Plain Python
  `set`/`list`/`tuple` parameters stay rejected (the message now names the
  typed classes). Fetching is unchanged: with `decode_collections=True`
  collections still decode to `frozenset`/`list`. Round trips, including
  MULTISET duplicates and SEQUENCE order, run live on CUBRID 10.2 and 11.4,
  and a replay scenario pins sync/async parity. The official driver has no
  equivalent ordinary-execute API (its wrapper binds plain lists through
  native prepared `bind_set`), so no differential claim is made.
  Construction happens entirely in `__new__`; re-invoking `__init__` on an
  existing instance (`obj.__init__(...)`) is a no-op and cannot mutate it or
  change its hash (#568 review). The instances are safe to `copy.copy()`
  (returns the same object; sharing element references either way is already
  what a shallow copy means), `copy.deepcopy()` (returns the same object when
  every element is itself immutable, which `copy.deepcopy()` of the elements
  tuple already detects; an independent copy, with its own independently
  copied elements, when an element such as `bytearray` is mutable, so
  mutating the copy cannot alias back into the original) and `pickle`
  (`__reduce__` round-trips through the public constructor instead of
  pickle's default slot restore, which would otherwise call `setattr()` on
  the immutable instance and raise). `format_parameter()` raises
  `ProgrammingError` instead of leaking `AttributeError` for an instance that
  bypassed `__new__` (for example `object.__new__(Set)`). A `dict` argument
  is rejected (`TypeError`) by all three classes — iterating it would use
  only its keys and silently drop the values — and `Sequence` additionally
  rejects a `set`/`frozenset` argument (`TypeError`), since its iteration
  order is not guaranteed and would make `Sequence`'s element order
  nondeterministic; `Set` and `Multiset` still accept a `set`/`frozenset`.
- **Internal: typed collection binding wire contract (#482)** — not
  user-visible. Prepared FC3 requests can carry an immutable
  SET/SEQUENCE/MULTISET value of INT or STRING elements (NULL elements, empty
  collections), with exact per-element framing and rejection of mixed,
  nested and unsupported elements before any bytes are built. There is no public collection API yet (#440); ordinary sync/async
  execution and the public API are unchanged. Live 10.2/11.4 evidence: the
  broker rejects the MULTISET kind (error -454), and a SET value stored into a
  MULTISET column drops duplicates while a SEQUENCE value keeps them.
- **Internal: LOB-handle binding wire contract (#441)** — not user-visible.
  Prepared FC3 requests can carry an immutable BLOB/CLOB handle binding whose
  bytes equal the official driver's `bind_lob()` request (checked against a
  captured official request). The handle framing and its BLOB/CLOB type are
  validated before any bytes are built. Each binding records the connection
  and physical session it was made for, and the native prepared cursor sends
  it only on that session (never on a replacement session or another
  connection that happens to have the same generation number). There is no
  public LOB binding API yet; ordinary sync/async execution is unchanged.

### Changed
- Select the existing repository-tooling CI lane when its official-fixture or
  scenario-ledger tests, or their upstream scenario CSV, change (#651); retain
  the single Linux/Python lane and documentation-only skips.
- Reduce routine PR CI to one representative offline lane and targeted live checks;
  move full compatibility matrices to explicit dispatch/releases and schedule representative checks weekly.
  Keep shell gate tests portable when Bash is unavailable and synchronize workflow
  cadence, representative matrix guidance and support-section headings in EN/KO docs.
  Conservatively validate new driver/test paths with one offline regression lane;
  bind manual validation to a requested SHA and current PR head before/after testing.
- **Private CCI metadata type evidence (#631)** — full column metadata retains
  the exact CCI extended-type value from legacy or two-byte type headers.
  Ordinary type codes, descriptions, row/schema parsing and public APIs are
  unchanged. This internal prerequisite adds no `result_info` API or parity
  claim; that remains separate work under #445.
- **Shared simple CAS reply prefix (#560)** — ten simple packet parsers reuse
  one private helper for reader creation, CAS_INFO skipping, response-code
  parsing and server-error dispatch. Packet-specific payload parsing, encoding,
  error lengths and failure behavior are preserved; handshake, database-open,
  row/metadata packets and CHECK_CAS retain their distinct parsing paths.
- **Direct cursor imports and unused-helper cleanup (#561)** — the sync
  connection imports the real `Cursor` directly instead of maintaining a lazy
  module-global class cache. The unused async connect-dispatch helper is removed,
  and cursor exception imports are centralized at module scope. Intentional SSL
  and cursor helper aliases, cursor ownership, timing, error metadata, public
  APIs and runtime behavior are preserved.
- **Faster FETCH row parsing (#559)** — a new offline microbenchmark,
  `tests/test_bench_fetch_parsing.py` (2000-row scalar, text, mixed and
  collection replies), guided two changes to the common row loop. The SET
  conversion now runs only for SET columns, because every other type returned
  the value unchanged. Each cell's size word is read inline instead of through
  a method call. Median parse time drops by 13–23% for the scalar, text, mixed
  and raw-collection workloads and by 5–6% for decoded collections (CPython
  3.10, two runs); peak allocation is unchanged. Parsed values, errors and malformed-reply handling are unchanged. A
  differential run of the old and new parser over 10,490 seed, truncated and
  byte-mutated FETCH replies gave identical rows and identical exception types
  and messages. Without `--benchmark-enable`, the benchmark runs only as a
  correctness check.
- **Internal: explicit per-reply session verification and one escape-mode policy (#525)** —
  refactor with no behavior change in either driver. Whether an OUT_TRAN reply
  still needed a `CHECK_CAS` probe was decided by comparing CAS_INFO object
  identity with the last verified reply. Each reply is now recorded explicitly
  and is marked verified only after `OPEN_DATABASE`, a successful `CHECK_CAS`
  or a healthy `ping()`. Verification stays per reply, not per session: the CAS
  may close the socket after any OUT_TRAN reply, so every one still gets its own
  probe. A reconnect-only generation counter (the original proposal) would skip
  needed probes, and no `CHECK_CAS` is removed. The 46 sync/async replay
  scenarios, including the #557 round-trip budgets, send identical request
  sequences before and after the change. The backslash-escape probe SQL, result
  interpretation and error messages, previously copied in the sync connect,
  async connect and async recovery paths, are now one shared helper.
- **Autocommit cursor handles are released with the next statement (#488)** —
  in autocommit mode only `commit()`/`rollback()` sent `CLOSE_REQ` for unclosed
  cursors, the connection kept every cursor alive, and an explicit close cost a
  `CLOSE_REQ` plus the `CHECK_CAS` probe of its OUT_TRAN predecessor. Live on
  CUBRID 11.4.6 (statement pooling on), 5000 unclosed autocommit SELECTs pushed
  handle ids past 1100 and forced 4-5 CAS memory restarts per run (each silently resets
  SQL-set session state). Connections now track cursors weakly. When the broker
  reports statement pooling, an autocommit `close()` or re-`execute()` and,
  in any mode, a cursor collected without `close()` queue the handle id, and the
  next `PREPARE_AND_EXECUTE` carries it as an extra prepare argument that CAS
  frees before preparing (the wire mechanism of JDBC's deferred close; unlike
  JDBC, which closes SELECT/CALL/EVALUATE handles at once, result-set handles
  are deferred too). Measured live, sync and async:
  5000 unclosed SELECTs keep the handle id at 1-2 with no restart; a SELECT then
  `close()` takes 2 requests (1 `CHECK_CAS`) instead of 4 (2 `CHECK_CAS`); a reused
  cursor's SELECT then INSERT takes 5 requests instead of 8. One statement
  carries at most 256 ids (an explicit release while 256 are queued sends
  `CLOSE_REQ` at once; collected cursors ride on later statements), and
  `commit()`/`rollback()` close every id still queued with `CLOSE_REQ`, as they
  closed unreferenced cursors before. The queue belongs
  to one physical session and is dropped when that session is retired or
  replaced, so no stale id is sent after a reconnect. Probes are unchanged, and a
  request with queued ids is never replayed. Each handle keeps the generation of
  the session that opened it, explicit releases during session setup are never
  deferred, and a shard proxy
  (which ignores the extra arguments) keeps immediate `CLOSE_REQ`. Without
  statement pooling CAS frees
  handles at every commit, so `CLOSE_REQ` is still sent at once and a collected
  cursor's handle is left to that commit (in manual-commit mode it was
  previously closed by the next `commit()`/`rollback()`).

- Earlier release workflow unification with the sibling repos: new `RELEASING.md`; `make release`
  replaced by the read-only `make release-check VERSION=x.y.z`; `publish-pypi.yml` was
  manual-dispatch only and then dispatched the cookbook smoke test after a successful
  publish (replacing `notify-cookbook.yml`); CI lints `CHANGELOG.md`.
- **PyPI publish fails closed on duplicate files (#494)** — `publish-pypi.yml` no longer
  passes `skip-existing: true`. The new stdlib-only `scripts/pypi_duplicate_guard.py`
  compares the SHA-256 of every verified file with the file PyPI already serves under the
  same name: an identical file (a partial upload recovered with `gh run rerun --failed`)
  is dropped from the upload, and a different hash or an unreachable PyPI fails the job.
  `RELEASING.md` documents the bounded recovery; offline tests cover the guard.
- **CI: releases happen automatically when a reviewed release PR is merged (#539)** —
  `prepare-release.yml` opens the `chore: release vX.Y.Z` PR (moves `[Unreleased]` into a
  dated section, bumps `__version__`, runs `make release-check`). On every push to `main`,
  the guarded workflow introduced as `release.yml` decides from git facts only (`scripts/release_detect.py`: version
  changed against the first parent, dated CHANGELOG section, tag absent or at the same
  commit) and then runs, pinned to the merge SHA: release check, the full
  `integration-full.yml` matrix (now also a `workflow_call` workflow, no longer run on tag
  pushes), one build with SHA-256 hashes, the annotated tag, a draft GitHub Release with
  SBOM, the PyPI upload through the duplicate guard, and the cookbook verification of that
  exact version, with one run summary. The cookbook smoke test runs inside the release run
  as a reusable workflow pinned to a cookbook commit, so it needs no cross-repository token
  or secret; the release fails unless it reports the requested version installed (#544). `create-release.yml` and the manual
  `publish-pypi.yml` were removed; a narrow recovery dispatch (`resume`, `verify-only`,
  `dry-run`) remains. The guarded workflow now uses `publish-pypi.yml` to match the
  registered PyPI identity; the former manual publisher is not restored. Curated
  CHANGELOG notes remain alongside release-please generated notes.
- Ruff/Mypy pre-commit hooks are now `repo: local` / `language: system` hooks that
  invoke `python3 -m ruff`/`python3 -m mypy` from the active `.[dev]` environment
  instead of separately versioned mirror repos, so there is a single source of
  truth (the `pyproject.toml` dev pin) for each tool's version.
  `scripts/check_quality_tools.py` was updated to match. This fixes Dependabot's
  routine `pip`-ecosystem Ruff/Mypy bumps, which previously left the pre-commit
  hook revision stale and failed the quality-tool consistency gate (#476).

### Documentation
- **Reviewed backlog priorities (#566)** — reconcile the dated roadmap index with
  current release, verification and compatibility work. Preserve contributor
  ownership and distinguish upstream reporting from release preparation; no
  driver behavior, supported-version or publication-policy change.
- **CAS session-loss contract and upstream follow-up (#614)** — retain
  `OperationalError` for an incomplete CAS reply and document session retirement,
  explicit recovery without statement replay, and the differential harness's
  isolation/exclusion evidence. CUBRID's collection/NUMERIC `IF` SIGSEGV remains
  unfixed; upstream reporting is tracked separately in #675. No driver behavior,
  public API or supported-version change.
- **`TIME` binding precision is stated in the type reference** — `docs/TYPES.md`
  (English and Korean) now says that a bound `datetime.time` loses its
  microseconds and `tzinfo` silently, as `docs/PARAMETER_BINDING.md` already
  did. CUBRID `TIME` has second precision; the driver keeps truncating rather
  than raising `DataError`, so existing 1.x callers are unaffected.
- **Korean contribution guide (#329)** — translate the current contribution
  procedures and preserve command examples, English GitHub artifacts,
  contributor/maintainer responsibilities and release boundaries. Add discovery
  from the Korean README and existing docs navigation; English policy is unchanged.
- **Renewed CAS error-code evaluation (#505)** — document numeric-code-first
  dispatch, inner-code `-1`-only text fallback and raw `code`/`errno` preservation.
  Retain the flag-zero compatibility contract despite the bounded four-build
  renewed-code producer proof; numeric-only normalization is unsafe because
  legacy CAS and engine numbers overlap. Negotiation, consumer migration,
  handshake/API behavior and the separate holdable feature remain unchanged.
- **Current verification guidance (#415)** — replace volatile current version,
  module and test/coverage counts with existing source, measured coverage and
  CI policy references in the README/translations, agent and development guides,
  roadmap, support matrix and canonical LLM index. Full coverage retains the
  95% floor; routine PR smoke is not a coverage or full-matrix claim. Historical
  release/transport milestones and support/security/release policies are unchanged.

- Align SECURITY.md with latest-minor support and current Python 3.10 TLS preflight guidance while retaining disclosure instructions (#422).
- **`llms.txt` no longer advertises prepared statements, and the two entry points are single-sourced (#414)** — the root `llms.txt` claimed prepared statements and a `Cursor.prepare()` method, which ordinary cursors do not have, listed an incomplete exception hierarchy, hardcoded test and coverage counts and linked to the retired `cubrid-cookbook/python` paths, while `docs/llms.txt` was a separately maintained, differing index. `docs/llms.txt` is now the only maintained index, checked against the code: driver-side literal binding and its documented limits, the opt-in sync-only `pycubrid.compat.native` prepared subset, sync and async (`pycubrid.aio`) feature parity, the full PEP 249 exception list and `cubrid-cookbook-python` links. `scripts/generate_llms_full.py` copies it byte-for-byte to the root `llms.txt`, and the CI `lint` job now fails when either `docs/llms-full.txt` or `llms.txt` is stale. `docs/SUPPORT_MATRIX.md` and `docs/TROUBLESHOOTING.md` (+ Korean) no longer describe `cursor.execute(sql, params)` as server-side `PREPARE_AND_EXECUTE` binding (the section is renamed "Parameterized Query Issues"), and the support matrix notes that `nextset()` raises `NotSupportedError`; the Korean, German, Hindi, Russian and Chinese READMEs now describe driver-side binding like the English README. `CONTRIBUTING.md` documents the workflow.

### Fixed

- **PyPI Trusted Publisher filename** — rename the sole guarded release
  orchestrator from `release.yml` to the registered `publish-pypi.yml` identity
  with environment `pypi`, correcting the filename mismatch behind `invalid-publisher`.
  Retain release/full-matrix/artifact/tag/cookbook gates and synchronize recovery
  commands and successful-publication label reconciliation. No driver behavior,
  version, dependency or support change.
- **`fetchmany()` validates integer size before fetching (#371)** —
  ordinary sync and async cursors raise `ProgrammingError` for non-integer
  types, including floats and booleans, before buffer consumption or a FETCH
  request. Positive sizes and omitted/`None` defaults keep their behavior;
  existing zero/negative integer no-ops and the separate compatibility wrapper
  are preserved. This type tightening is classified MINOR, since booleans
  and nonpositive floats previously completed without error.
- Preserve existing Docker volumes during automatic integration/TLS cleanup, including readiness and test failures (#501).
- **Connection timeouts are validated before socket creation (#367)** — sync and async connections reject negative, NaN, and infinite `connect_timeout`/`read_timeout` values during common initialization, preventing invalid timeout values from leaking a newly opened socket; `None`, zero, and finite positive values retain their existing semantics.
- **TLS preflight fatal alerts and timeout context (#592)** — the Python 3.10
  async certificate probe sends queued fatal alert bytes best effort within
  its existing deadline, then re-raises the original TLS error even if alert
  sending fails. Probe I/O timeouts retain the same error object, message and
  errno while suppressing internal WantRead context in displayed traces. The
  existing trickle-peer regression now covers sync default and explicit
  handshake budgets too. Socket cleanup, required final-flight failures,
  optional shutdown bounds, defaults, dependencies and public APIs are unchanged.
- **Hostile timezone errors and pure-Python temporal fallback (#530)** — datetime
  parameter timezone callbacks, key lookup and offset-field errors now raise
  `ProgrammingError("invalid tzinfo on datetime parameter")` with the original
  exception as cause, without formatting hostile exception text. Invalid key
  values retain their specific error, and an offset of `None` still renders
  naive. Without the active C `_datetime` implementation, temporal subclasses
  and returned `timedelta` subclasses are rejected before driver descriptor
  reads can consume forged fields. Exact fallback values, C-backed subclasses,
  ordinary literals, public APIs and dependencies are unchanged. Offline
  regressions include fresh subprocesses with `_datetime` and `_zoneinfo`
  disabled, alongside hostile callback and offset cases.
- **Reserved-word hints describe the diagnostic position (#509)** — an
  `unexpected 'VARCHAR'` syntax message no longer claims that `VARCHAR` is the
  offending identifier. The appended hint says an identifier at or before the
  reported token may be reserved, with the existing quoting advice and link.
  Original server text, error metadata, hint triggers and SQL behavior are
  unchanged. Offline regressions use messages captured on CUBRID 10.2, 11.2
  and 11.4 brokers.
- **Interrupted deferred CLOSE flush retains unsent handles (#601)** — sync
  and async `commit()`/`rollback()` no longer remove the entire deferred-close
  queue before sending its first `CLOSE_REQ`. Each same-session queued ID is
  consumed at the existing send boundary; a caller interrupt before the next
  send keeps unsent IDs in FIFO order, while completed/uncertain sends are not
  replayed. Stale-session IDs are discarded, GC additions during a flush stay
  behind its original batch, and an uncertain transport still retires the
  session without `END_TRAN`. Public APIs and SQL results are unchanged.
- **Repeated native prepared execution errors retain the real server code
  (#611)** — after a complete broker error, the opt-in sync prepared cursor
  returns that failure unchanged. Only the next explicit caller `execute()`
  closes and re-prepares a non-LOB statement on the same physical CAS session
  before sending one FC3 with its current scalar/collection bindings. This
  prevents a repeated bad SET(INTEGER) value from surfacing stale-plan `-1024`
  instead of conversion `-494`, without replaying a possibly effective
  statement inside its failing call. Close/prepare/count/session failures
  abort before FC3; temporary LOB bindings require explicit reprepare and
  rebind. Ordinary FC41, async execution and public signatures are unchanged.
- `DBAPIType` comparison no longer reads `bool` values as integer type codes:
  `STRING == True` and `STRING != False` are now `False` and `True`. Integer
  subclasses such as `enum.IntEnum` still compare equal by value, so
  `STRING == CUBRIDDataType.STRING` is unchanged. (#369)
- **`Lob.write()` keeps the handle's size field current (#441)** — the packed
  handle returned by `Lob.lob_handle` embeds the LOB size, which the server
  trusts when the handle is sent back. `write()` left it at the size the
  handle had when it was created (usually 0), so a bound written handle would
  store the wrong length. It now raises the field to `offset + bytes written`
  after each write, never lowering it, as CCI does (also after a short
  write; a reply that claims more bytes than were sent raises before the
  handle changes). Later `write()`/`read()` requests carry the updated
  handle, as CCI's do; return values and errors are unchanged. Live 10.2/11.4
  evidence: written BLOB/CLOB handles up to 1 MB, bound through the internal
  binding below, store their full length.
- **`Lob.read()`/`Lob.write()` reject non-int and boolean offset/length
  (#449)** — `offset` (`read`/`write`) and `length` (`read`) must now be a
  concrete Python `int`: `type(value) is not int` is rejected, so `bool` (a
  subclass of `int`), `float`, `str` and any other `int` subclass (including
  an `IntEnum` member) raise `InterfaceError("<name> must be an int, got
  <type>")` before `_ensure_connected()` or any packet is built; the existing
  non-negative check is unchanged. Previously only `value < 0` was checked,
  so `lob.write(b"", offset=True)` silently returned `0` through the #394
  empty-write shortcut, and a `float` offset passed that check and only
  failed later, inside wire serialization, with `DataError`. Both now raise
  `InterfaceError` before any I/O. Valid non-negative `int` offsets/lengths,
  the empty-write shortcut's return value, and the `DataError` raised for an
  in-range `int` too large to serialize (e.g. `offset=2**63`) are unchanged.
  The regression suite's closed-LOB-precedes-invalid-arguments ordering test
  is adapted from heyadhithya's PR #458.
- **Missing-timezone test fixtures isolate cached wheel resources (#605)** —
  Offline and integration helpers hide cached `tzdata.*` modules as well as
  system timezone paths, then restore the original modules and exact caller
  path. Preloaded-resource and cleanup regressions prevent order-dependent
  successes in tests that require missing data; production timezone policy
  and dependency declarations are unchanged.
- **Collection conversion errors cannot hide malformed later elements (#595)** —
  Known decoded collection members validate length words, payload bounds and
  exact decoder consumption. The first complete `DataError` is saved while
  subsequent members still run their decoders; later structural damage retires
  the sync/async session. Complete collections keep the first error and cause.
  NULL-only, opaque/disabled decoding and unsupported nested layouts are unchanged.
- **Pooling-off autocommit replies retire freed handle ownership (#584)** —
  Direct CUBRID CAS reuses handle IDs after an automatic transaction end.
  Sync and async drivers now invalidate cursor/schema ownership on the actual
  known-boundary OUT_TRAN reply, before parsing or a later INSERT identity RPC.
  Current FC41 results cannot re-adopt already freed IDs on success or DataError.
  Buffered rows and normal completed EOF remain available; unfinished results
  fail explicitly rather than closing or fetching another cursor's reused ID.
  Final ordinary autocommit FETCH and error replies follow the same rule.
  Physical-session generation, pooling-on/manual/schema behavior and liveness
  probes are preserved; arbitrary OUT_TRAN echoes and batch replies are excluded.
- **Metadata text errors cannot hide damaged FC41/FC3 tails (#591)** —
  Undecodable metadata retains column types for validation of remaining
  counts, fields and inline rows before the original `DataError` is raised.
  Later structural damage closes sync/async connections with `OperationalError`.
  This error path excludes application JSON hooks and continues through later
  cells after an unrepresentable row value, including collection and LOB
  validation. Negative collection element counts and partial inline-fetch
  headers are rejected; absent optional headers and undeclared trailing bytes
  retain their contracts. Bounds re-walks use checked reader marks.
- **Column metadata finishes its framing check before `DataError`; FC41 count gaps closed (#581)** —
  column metadata text that is not valid in the connection codec raised
  `DataError` at once in FC2, FC3 (refreshed columns) and FC41 replies, so
  later metadata was never checked: a reply that also had a negative,
  overrunning or truncated field in a later column kept the session. The
  remaining metadata is now walked by its declared lengths first, and such a
  reply raises `OperationalError('malformed response from broker')` and closes
  the connection; a complete reply still raises `DataError` and keeps it. FC41
  now rejects a negative bind count, `total_tuple_count` or inline tuple count
  (previously ignored, passed through, or read as zero rows) and a column count
  the reply cannot hold, as FC2 already did; FC3 rejects a negative inline
  tuple count. Sync and async behave the same.
- **Sync TLS handshake is bounded without `read_timeout`; the Python 3.10 TLS preflight probe closes its socket (#535)** —
  `pycubrid.connect(..., ssl=...)` without `read_timeout` waited forever when the
  broker (or a proxy in front of it) stalled during the TLS handshake. The
  handshake now gives up after 10 seconds, the same default the async driver
  passes as `ssl_handshake_timeout`, and raises `OperationalError`; the socket is
  blocking again once the handshake is done, so later requests stay unbounded
  as before. To allow a handshake longer than 10 seconds, set a larger
  `read_timeout`, for example `30.0`; this also sets later request read timeouts
  to 30 seconds (sync per-receive, async round-trip). With `read_timeout` set
  nothing changes, and `connect_timeout`
  still bounds only the TCP connect. On Python 3.10 the async driver's
  certificate preflight probe now runs its handshake over memory BIOs on a
  socket it owns, so a broker reset just before the ClientHello no longer
  leaves the probe socket to the garbage collector (`ResourceWarning`).
  Every probe I/O and handshake completion share one deadline, and failures
  sending the final handshake flight propagate instead of being suppressed.
  Best-effort close_notify cannot extend that deadline (#593). The
  same CPython 3.10 `ssl` behavior can still affect the sync driver's
  `wrap_socket()` upgrade on 3.10; `docs/CONNECTION.md` (+ Korean) documents it.
- **Transport failures retire cursor handles; async timeout errors name their cause (#556)** —
  an uncertain transport failure closed the connection but left cursors
  holding the dead session's query handle ids: the sync socket-error and
  malformed-reply paths and the async timeout, socket-error and malformed-reply
  paths (also `ping()`, CHECK_CAS recovery and failed session restores) closed
  the streams and marked the connection closed without invalidating handles,
  and the async invalidation could be skipped entirely when `wait_closed()`
  failed or was cancelled. Every such path now retires the connection and all
  cursor and schema handles first, then shuts the stream down (async still
  awaits `wait_closed()`; its failure is logged, and cancellation still raises
  `CancelledError`). An interrupt (a non-`Exception` `BaseException` such as
  `KeyboardInterrupt`) while a sync reply is outstanding now retires the
  session too, as async cancellation already did. Buffered rows remain
  readable and the next required FETCH fails explicitly; nothing is replayed.
  The sync prepared-generation fence and pre-send local failures are
  unchanged. The async `OperationalError('read timeout')` was raised for any
  `TimeoutError`, including a transport `ETIMEDOUT` with `read_timeout` unset;
  it now reads `read timeout: no complete round trip within read_timeout=<n>s` only when that
  deadline expired, and `socket communication timed out` for a transport
  timeout. Python 3.10's distinct `asyncio.TimeoutError` follows the same
  transport/callback distinction. `__cause__` is preserved. An `OSError` (including `TimeoutError`)
  raised by a `json_deserializer` callback after the whole reply was read was
  treated as a transport failure by both drivers (session closed, wrapped in
  `OperationalError`); it now propagates unchanged and the session stays open.
  A `ValueError`-family error from a custom deserializer (orjson, simplejson)
  is still treated as a malformed reply and retires the session.
- **Negative FC41 column metadata lengths and column counts are rejected
  (#555)** — a `PREPARE_AND_EXECUTE` reply whose column name, real name, table
  name or default length was negative decoded that field as an empty string,
  and a negative column count decoded as a result with no columns, although
  `PREPARE` (FC2) and refreshed `EXECUTE` (FC3) metadata already rejected both.
  The shared column-metadata parser now checks every length and the column
  count before reading, so such a reply raises `OperationalError('malformed
  response from broker')` on the sync and async connections and closes the
  connection, like other framing damage (#383, #533). A normal server does not
  send such replies. Zero-length metadata, valid FC2/FC3/FC41 replies and the
  session-keeping `DataError` for a complete reply (#492, #512) are unchanged.
- **Async setup failure no longer leaks into waiting tasks (#554)** — while
  `AsyncConnection.connect()` configured a new session, other tasks waiting on
  the setup gate re-raised the setup owner's exception instance, so cancelling
  the task running `connect()` also cancelled every waiting task and appended
  their frames to one shared traceback. Each waiter now raises a fresh
  exception: a pycubrid error keeps its class (or the nearest
  `pycubrid.exceptions` class when a subclass has a different constructor),
  `code`, `errno` and `sqlstate` (the original chained as `__cause__`), any
  other error becomes `OperationalError` naming it by `repr()`, and a cancelled or interrupted setup becomes
  `OperationalError("connection setup was cancelled or interrupted in another
  task; retry operation")`. The setup owner still raises its own exception
  (including `CancelledError`), a waiter's own cancellation is unchanged, and
  the failed session is still discarded before the gate opens. This covers
  `connect()`, including the reconnect of `ping(reconnect=True)`.
- **Invalid JSON text in a complete reply raises `DataError` (#543)** — a
  `JSON` column value that is not valid JSON, decoded with
  `json_deserializer=json.loads`, raised `json.JSONDecodeError` — a
  `ValueError` subclass — so the connection layer reported it as
  `OperationalError('malformed response from broker')` and closed the
  connection, although the reply had been read in full. Under the #492/#512
  contract a complete reply holding a value the client cannot represent is a
  data problem: `PacketReader._parse_json` now raises `DataError` (the
  `JSONDecodeError` chained as `__cause__`), and the existing complete-reply
  bounds check (#383) applies before it is re-raised, so an ordinary
  connection and cursor stay usable, both on `execute()` (the cursor has no
  result set, `description` is `None`, but still owns and releases its server
  handle) and on a later fetch page (rows already collected are kept, #507); a
  truncated reply around the same cell is still reported as `OperationalError`
  and closes the connection. The explicit prepared API
  (`pycubrid.compat.native`), which threads the same `json_deserializer`,
  stays fail-closed as for invalid UTF-8 and zero dates: it raises
  `OperationalError` and retires the session. A caller-supplied
  `json_deserializer` is not wrapped: only the built-in `json.loads` path is
  reclassified.
- **The autocommit setter keeps `SET_DB_PARAMETER` and `COMMIT` on one CAS session (#551)** —
  in both drivers, `conn.autocommit = v` / `await conn.set_autocommit(v)` sent
  `COMMIT` with implicit reconnect after an OUT_TRAN `SET_DB_PARAMETER` reply,
  so a CAS recycled between the two sent only the `COMMIT` to the replacement
  session: `autocommit` reported the new value while that session had the
  broker default (first change) or the previous value (later change). The new
  value is now recorded before the `COMMIT`, so the `COMMIT`'s single
  `CHECK_CAS` reconnect restores it on the replacement first. A call replaces
  the session at most once: if `SET_DB_PARAMETER` itself needed a reconnect, the
  `COMMIT` is not allowed another one. If the `COMMIT` fails (including a
  native error), the connection is closed, the previous value is kept and
  `OperationalError` is raised with the cause chained; a rejected
  `SET_DB_PARAMETER` still raises its native error and keeps the session.
  Found by the offline sync/async replay parity suite (#521), whose strict
  `xfail` scenario now passes.
- **Sync `Connection.connect()` after `close()` restores an explicit `autocommit` (#520)** —
  reopening a closed sync connection did not re-send `SET_DB_PARAMETER`
  (`AUTO_COMMIT`), so the new CAS session kept the broker default while
  `conn.autocommit` still reported the value the caller had set; the async
  driver already restored it. `connect()` now re-applies an explicitly set
  `autocommit` whenever it opens a new physical session after an earlier one,
  which also covers `ping(reconnect=True)` recovery and the reconnect after a
  failed `CHECK_CAS` probe (each still restores exactly once). A connection
  whose `autocommit` was never set explicitly sends nothing extra. Found by the
  offline sync/async replay parity suite (#521).
- **Sync `connect(autocommit=True)` applies autocommit on one CAS session, like async (#521)** —
  the sync constructor applied `autocommit=True` through the public property
  setter, which probes an OUT_TRAN reply with `CHECK_CAS` and may reconnect in
  between: a CAS recycled right after `SET_DB_PARAMETER` made it send the
  `COMMIT` on a new session that never received `AUTO_COMMIT=1`, while
  `conn.autocommit` reported `True`. It now sends both requests on the session
  it just opened with implicit reconnect disabled, as `pycubrid.aio` does; any
  failure closes the connection and raises `OperationalError` (the native
  error is its `__cause__`; previously the native `DatabaseError` escaped and
  the socket stayed open). A healthy connect no longer sends the extra
  `CHECK_CAS` between the two requests.
- **`connect()` verifies an OUT_TRAN session before applying autocommit (#521)** —
  with automatic `no_backslash_escapes` detection the escape probe ends with a
  `ROLLBACK`, so a new session is OUT_TRAN when the constructor's
  `autocommit=True` or the restore of an explicit `autocommit` is sent without
  implicit reconnect. A CAS recycled right after that `ROLLBACK` made the async
  `connect()` and the reconnect restore fail (the sync constructor survived it
  only through the reconnecting property setter replaced above). Both drivers now send
  `CHECK_CAS` first in that case and, if it fails, replace the session once and
  apply the setting there; a healthy or verified session sends nothing extra.
  A session replaced during the escape probe itself is configured once by
  that recovery and not again by `connect()`, and an interrupted sync setup
  retires the new session instead of leaving it half-configured.
  The async escape probe of that replacement also now carries the connection's
  autocommit flag, like every other escape probe in both drivers.
- **Sync `ping(reconnect=False)` closes a session whose `CHECK_CAS` failed, like async (#521)** —
  when `CHECK_CAS` returned a negative code (broken CAS-to-DB link), the sync
  driver returned `False` but kept the session, and the next request probed it
  again and silently reconnected; `pycubrid.aio` closes it. Both now close the
  confirmed-broken session: `ping(reconnect=False)` still returns `False`
  without reconnecting, and later calls raise `InterfaceError` until
  `connect()` or `ping(reconnect=True)`. A healthy ping, a closed connection
  and `ping(reconnect=True)` are unchanged.
- Clear previous results in synchronous and asynchronous `execute()` calls
  after closing the old query handle (#373). A subsequent binding or request
  failure leaves `description=None`, `rowcount=-1`, `lastrowid=None` and no
  fetchable rows or held fetch-page error. If closing the old handle fails,
  `execute()` keeps the buffered result and its page error; connection invalidation
  or reconnect handling may still retire the handle.
- **`CALL` and `EVALUATE` results and `NULL`-typed columns decode their values (#542)** —
  under CAS protocol 8 (CUBRID 10.2+) each such cell starts with the two-byte
  type header `0x80 | collection bits | charset`, type, the layout of column
  metadata, but the row parser read one byte, took `0x83` as an unknown type and
  returned the rest of the cell as raw `bytes`. `CALL` of a stored function or
  method, `callproc()` and `EVALUATE` now return the decoded value (for example
  `42` instead of `b'\x08\x00\x00\x00*'`, `'OID:@897|1|0'` for
  `CALL find_user('dba') ON CLASS db_user`, a `datetime` for `DATETIME`), sync
  and async; collection values keep their collection kind. The single-byte
  header of older brokers is still accepted. A header longer than its cell is a
  malformed reply (`OperationalError('malformed response from broker')`, the
  connection closes), and the re-walk before `DataError` (#523) reads the same
  header. Verified live on CUBRID 10.2 and 11.4. Documented in
  `docs/API_REFERENCE.md` and `docs/PROTOCOL.md` (+ Korean).
- **Row cells whose value does not use exactly their declared size are rejected (#523)** —
  the readers for fixed-width values (`SHORT`, `INT`, `BIGINT`, `FLOAT`,
  `DOUBLE`, `MONETARY`, `DATE`, `TIME`, `DATETIME`, `TIMESTAMP`, `OBJECT`, and
  the fixed part of the TZ types) ignored a cell's size word, so a FETCH or
  inline execute row whose cell declared more bytes than the reply held (for
  example an `INT` declaring 1000 bytes at the end of the reply), or a size
  that disagreed with the value's width, was decoded as if it were complete.
  Every row cell must now use exactly its declared size, like the
  length-prefixed values since #383 (checked before the value is read, and
  when a reply is re-walked before `DataError`); otherwise the reply raises
  `OperationalError('malformed response from broker')` and closes the
  connection, sync and async. A normal server always sends the exact size, so
  valid replies, the `DataError` classification of complete replies (#492,
  #512) and SQL `NULL` cells (a non-positive size) are unchanged. A negative
  FETCH tuple count, which read as an empty page and silently ended the result
  set early, is rejected the same way. Documented in `docs/PROTOCOL.md` and
  `docs/TROUBLESHOOTING.md` (+ Korean).
- **Tests: offline sync/async replay parity (#521)** —
  `tests/test_replay_parity.py` replays scripted broker replies through a real
  sync `Connection` and a real `AsyncConnection`, each against its own
  in-process multi-session broker (`tests/helpers/replay_broker.py`) over a real
  socket, and compares step outcomes, the exact requests sent, whether the
  connection stays usable and the number of sessions. Scenarios cover
  connect/close, reconnect after close, autocommit set/restore, handle
  invalidation at commit/rollback, the OUT_TRAN `CHECK_CAS` probe and one
  recovery, SQL bound to a replaced session, failed pings, malformed and
  truncated replies and the `DataError` contracts (#512, #536). Intended
  differences are listed per scenario with a reason and documented, with the
  four unintended ones it found (fixed above), in `docs/DEVELOPMENT.md`
  (+ Korean). `prepare_and_execute_reply()` in `tests/helpers/cas_reply.py`
  gains a `total` keyword for replies that leave rows to later FETCH pages.
- **Tests: protocol fuzzing seeds realistic replies (#523)** — every
  `tests/test_protocol_fuzz.py` seed used to carry zero columns, so no fuzz
  case reached column metadata or row cells. Seeds built by
  `tests/helpers/cas_reply.py` now cover `PREPARE_AND_EXECUTE`, `PREPARE` and
  `EXECUTE` replies with metadata for string, numeric, `NUMERIC`, temporal and
  TZ, `BIT`/`VARBIT`, OID, collection, LOB and `JSON` columns; multi-row FETCH
  replies (including CALL and `NULL`-typed layouts); and schema, batch and LOB
  replies. Unmutated seeds must decode to exactly their values; mutations aim
  at truncation at field boundaries, length and count words, and collection
  element types, and the oracle admits only structural errors (reported as
  `OperationalError`), server errors and `DataError` for complete replies,
  also through the sync and async connections. Thirteen tests whose only
  assertion was `is not None` now check the expected value, and two unittest
  guards use `self.fail()` instead of a narrowing `assert`.
- **Tests: a configured but unreachable CUBRID now errors instead of skipping (#522, #432)** —
  16 integration modules probed the server at import time and called
  `skipif("CUBRID instance not available")`, so pointing the suite at a dead
  endpoint produced hundreds of silent skips despite `tests/conftest.py`
  promising fail-closed behavior. Those probes (and the TLS module's import-time
  TLS probes) are gone: one gate in `tests/conftest.py` skips integration tests
  when neither `CUBRID_TEST_URL` nor `CUBRID_TEST_HOST` is set, and otherwise
  probes the endpoint once per session and makes every plain integration test
  error with the endpoint and connection error. Every integration module, the
  gate and `scripts/wait_for_cubrid.py` now resolve the endpoint through one
  helper, `tests/_cubrid_endpoint.py`: per-field `CUBRID_TEST_*` variables win,
  then the components of `CUBRID_TEST_URL` (a scheme-less value such as `1`
  stays a pure on/off switch; a malformed URL errors the integration tests
  without breaking offline collection), then `localhost:33000/testdb` as
  `dba` — so a URL naming another host or port is no longer silently ignored in
  favor of whatever listens on `localhost:33000`. CI, which exports both, is
  unchanged. `make integration` waits with `wait_for_cubrid.py` instead of
  `sleep 10`, runs `integration and not tls`, fails when the JUnit audit
  (`check_integration_lanes.py --results`) finds an all-skipped run, always
  removes the container, and accepts `CUBRID_TEST_PORT=<port>` (also used by
  `docker-compose.yml`) to avoid a busy port 33000. `make integration-tls` also
  waits for readiness instead of sleeping, audits its JUnit report and always
  removes the container.
- **Rows fetched before a failing page are no longer lost (#507)** — when a
  later FETCH page raised a data-level `DataError` (invalid text #492, an
  unresolved zone #413, a zero date #512), `fetchall()` and `fetchmany()`
  dropped the rows they had already collected in that call, and because the
  fetch position did not advance, every retry requested the same page again:
  it failed again, or, in autocommit mode once the broker had closed the
  result after its last page, raised `DatabaseError` with CAS error `-1012`.
  The call that reaches the page still raises `DataError` and the whole page is
  withheld, but the rows it had collected stay buffered and the next
  `fetchone()`/`fetchmany()`/`fetchall()` (or iteration) returns them without
  contacting the server. After that every fetch raises the same `DataError`
  again, without requesting the page, until `execute()` or `close()`, so no
  row of or past the failing page is returned and retries do not loop on the
  server. The connection stays usable and the cursor keeps its handle, sync
  and async alike. Documented in `docs/API_REFERENCE.md`, `docs/TYPES.md` and
  `docs/TROUBLESHOOTING.md` (+ Korean); live-tested against CUBRID 11.4 with a
  zero `DATE` several FETCH pages into the result.
- **`pycubrid.aio` no longer logs an asyncio warning whenever a TLS broker closes the connection (#514)** — after the in-place `loop.start_tls()` upgrade the stream protocol still believed it was on a plaintext transport, so every TLS peer close (broker restart, CAS recycle, idle timeout, dropped session before a reconnect) made asyncio log `WARNING returning true from eof_received() has no effect when using ssl`. The upgrade now marks the stream protocol as running over TLS, as `StreamWriter.start_tls()` does on Python 3.11+. Log output only; connection state, errors and the sync driver are unchanged.
- **`pycubrid.aio.connect(..., ssl=...)` no longer hangs forever when the TLS handshake is interrupted (#513)** — if the broker stalled or reset the connection before the TLS handshake completed, `read_timeout` (or the 10-second `ssl_handshake_timeout`) fired as intended, but connect cleanup then awaited `StreamWriter.wait_closed()` on a stream that asyncio never marks closed (its `SSLProtocol` drops `connection_lost` while still handshaking), so the call never returned on Python 3.11+. The failed upgrade now notifies the stream protocol itself after aborting the transport, and connect raises `OperationalError` within `read_timeout` and closes the socket. The sync driver was not affected. `docs/CONNECTION.md` and `docs/TROUBLESHOOTING.md` (+ Korean) now state which timeout bounds the TLS handshake (`read_timeout`; `connect_timeout` covers only the TCP connect).
- **Reads past the end of a broker reply are rejected (#383)** — a length
  field that ran past the end of a reply was cut short by a Python slice and
  returned as if complete: a `BIT`/`VARBIT` cell declaring 8 bytes but carrying
  2 returned those 2 bytes, a `LOB_READ` reply declaring 10 bytes with 3 in
  the payload set `bytes_read = 10`, and strings, `NUMERIC`, `JSON`, raw
  collections and LOB handles and locators behaved the same way. A negative
  length moved the reader backwards. Every length-prefixed read now checks
  `0 <= length <= remaining` before it moves, and a decoded collection's
  elements must fill its declared size exactly (previously elements could run
  into the next column). Such a reply raises
  `OperationalError('malformed response from broker')` and closes the
  connection, sync and async, like other framing damage; `DataError` stays for
  complete replies (#492, #512). A `LOB_READ` count below the requested length
  is still a valid short read (#362), and bytes after the last value a reply
  declares are still ignored. Documented in `docs/PROTOCOL.md` and
  `docs/TROUBLESHOOTING.md` (+ Korean).
- **Zero `DATE`/`DATETIME`/`TIMESTAMP` values no longer close the connection
  (#512)** — CUBRID accepts zero values such as `DATE'0000-00-00'`,
  `DATETIME'0000-00-00 00:00:00'` and zero `TIMESTAMP`, `TIMESTAMPTZ`,
  `TIMESTAMPLTZ`, `DATETIMETZ` and `DATETIMELTZ` values, but Python's
  `datetime` has no year 0. The decoder's raw `ValueError` was treated as a
  framing failure: `OperationalError('malformed response from broker')`, the
  socket closed, and every later call raised `InterfaceError('connection is
  closed')`. The value now raises `DataError` naming the CUBRID type and fields
  (`CUBRID DATE value (0, 0, 0) cannot be represented in Python: year 0 is out
  of range`) and the session stays usable, on `execute()` and on a later fetch
  page, sync and async, with the same cursor state as invalid UTF-8 (#492).
  Any other temporal field Python cannot hold (such as a `TIME` hour of 25)
  in a complete reply is reported the same way.
  A row value that raises `DataError` (#492, #413, #512) is now reported only
  after the rest of the row data is checked against the reply length, so a
  reply cut short still raises `OperationalError` and closes the connection,
  and so does a temporal field whose declared size does not match its type,
  or a collection element that runs past the collection.
  The explicit prepared API (`pycubrid.compat.native`) stays fail-closed.
  There is no option to return zero dates as `None` or text;
  `docs/TYPES.md` and `docs/TROUBLESHOOTING.md` (+ Korean) document SQL
  workarounds (`NULLIF(d, DATE'0000-00-00')`, `CASE`, `TO_CHAR`). Found by
  the CUBRID 10.2-11.4 version differential (#351); the behavior was the same
  on 10.2, 11.0, 11.2 and 11.4.
- **Security: `str`, `bytes`, date and time parameters are rendered without
  calling overridable methods (#528)** — `format_parameter()` escaped `str`
  parameters with `value.replace(...)` and `"\x00" in value`, rendered
  `bytes`/`bytearray` with `value.hex()` and dates and times with
  `value.strftime(...)`, all of which a subclass can override, and spliced a
  `tzinfo.key` into `DATETIMETZ` literals unescaped. A `str` subclass whose
  `replace()` returned `x'; DROP TABLE users; --` had that text sent
  unescaped; an overridden `hex()` or `strftime()`, or a `tzinfo.key`
  containing `'`, injected SQL the same way. A `str` subclass is now copied to
  a plain `str` through the base class before the NUL/Ctrl-Z checks and
  escaping, `bytes`/`bytearray` are rendered with `bytes.hex(value)` /
  `bytearray.hex(value)`, and date/time literals are built from the integer
  fields read through the base-class descriptors (the UTC offset through
  `datetime.datetime.utcoffset()` and the `timedelta` descriptors). A
  non-empty `tzinfo.key` must be a plain `str` matching `[A-Za-z0-9_+/-]+`
  (every IANA name does), otherwise `ProgrammingError`. Parameters are
  dispatched on `type(value)`, so an object that only claims a supported type
  through `__class__` (including transparent proxies) raises
  `ProgrammingError("unsupported parameter type")` instead of a raw
  `TypeError` or being rendered through the proxy; `escape_string()` raises
  `ProgrammingError` for a non-`str` argument. When the C `decimal` module is
  unavailable (pure-Python `_pydecimal` fallback), `Decimal` subclasses raise
  `ProgrammingError`, because that module copies their value through
  attributes a subclass can forge. Output for plain `str`, `bytes`,
  `bytearray`, `date`, `datetime` and `time` values is byte-identical except
  for the year padding below, and sync and async cursors share the change.
- **Years below 1000 are zero-padded in `DATE`/`DATETIME`/`DATETIMETZ`
  literals (#519)** — the year was rendered with `strftime("%Y")`, which does
  not pad on Linux, and CUBRID reads `DATE'99-01-02'` as 1999-01-02, so
  `date(99, 1, 2)` and `datetime(99, ...)` were silently stored and compared as
  year 1999. Years are now always four digits (`DATE'0099-01-02'`); years 1,
  99, 999 and 1000 round-trip on CUBRID 10.2 and 11.4.
- **Security: `int`, `float` and `Decimal` subclasses are bound by value
  (#518)** — `format_parameter()` rendered `int` and `float` parameters with
  `str(value)` and `Decimal` with `format(value, "f")`, which dispatch to
  methods a subclass can override. A subclass with a custom `__str__`/`__format__`
  could therefore inject arbitrary text into the SQL sent to the server (a
  `__str__` returning `1; DROP TABLE t` was sent verbatim), and on Python 3.10
  `enum.IntEnum`/`enum.IntFlag` members were sent as `Color.RED` / `Perm.R|W`
  instead of their values. Values are now rendered through the base-class
  methods (`int.__repr__`, `float.__repr__`, and a plain `Decimal` copy for the
  `NaN`/`Infinity` and 38-digit checks and `format(..., "f")`), so `Color.RED`
  is sent as `1` and `Perm.R | Perm.W` as `6`. Output for plain `int`, `float`
  and `Decimal` values is unchanged, `bool` still renders as `1`/`0`, and
  sync and async cursors share the change.
- `decimal.Decimal` parameters are now rendered in plain fixed-point notation
  instead of `str(value)`, which switched to E notation (`Decimal("1E-7")` was
  sent as `1E-7`). CUBRID parses an E-notation literal as `DOUBLE`, so such
  values silently came back as `float` and lost digits when inserted into
  `NUMERIC` columns; they now stay `NUMERIC` with their sign, trailing zeros
  and scale (`Decimal("0.0000001")` is sent as `0.0000001`, `Decimal("1E+5")`
  as `100000`). A `Decimal` whose plain literal needs more than 38 digits
  (CUBRID's `NUMERIC` maximum precision; leading fractional zeros count), such
  as `Decimal("1E-39")` or a 39-significant-digit value, raises `DataError`
  before anything is sent instead of becoming `DOUBLE`; CUBRID itself rejects
  such plain literals. `NaN`/`Infinity` still raise `ProgrammingError`, and
  integral Decimals written without an exponent (`Decimal("42")`) render as the
  same integer literal as before. Sync and async cursors share the change. (#517)
- `callproc()` now rejects procedure names with empty or invalid dot-separated
  segments before executing SQL, in both sync and async cursors. Valid single
  and qualified identifiers continue to work. (#372)
- With `decode_collections=True`, a nonempty `SET`/`MULTISET`/`SEQUENCE`
  (`LIST`) whose elements are all SQL NULL, such as `{NULL}` or
  `{NULL, NULL}`, now decodes to `[None, ...]` (a `SET` becomes
  `frozenset({None})`) instead of raw `bytes`. CUBRID 10.2 and 11.4 send
  these with element type NULL, the element count and a `-1` length per
  element; only an empty collection was handled before. A NULL-type header
  (including an empty collection) whose count does not match the payload
  size, or whose element lengths are not NULL markers, raises `OperationalError('malformed response from
  broker')`. Default raw-bytes mode, empty and mixed collections, and public
  signatures are unchanged. (#483)
- Invalid UTF-8 in a fully received broker reply no longer raises
  `OperationalError('malformed response from broker')` and closes the
  connection. Server error messages (and batch per-statement error messages)
  are decoded with `errors="replace"`, so the real CUBRID error surfaces with
  its `errno`/`sqlstate`; this happens when CUBRID cuts an echoed value in the
  middle of a multi-byte character. A `CHAR`/`VARCHAR`/`NCHAR`/`ENUM`/`JSON`
  value (including a collection element) that is not valid UTF-8 now raises
  `DataError` and the session stays usable; `execute()` keeps the server
  handle so it is released normally. CUBRID 10.2 can store such a value when
  it truncates an oversized string by bytes. Truncated packets and invalid
  UTF-8 in protocol metadata still raise the connection-level
  `OperationalError`, as does the explicit prepared API (`compat.native`),
  which retires the session on any non-server failure. (#492)
- A `DELETE`/`UPDATE` of a parent row that a foreign key still references
  (native `-924`, `ER_FK_RESTRICT`) and a `TRUNCATE` of a referenced parent
  table (`-1284`, `ER_TRUNCATE_PK_REFERRED` on CUBRID 11.4; 10.2 reports
  `-924`) now raise `IntegrityError` with SQLSTATE `23000` instead of a
  generic `DatabaseError`, for single statements and batch failures alike.
  `IntegrityError` is still a `DatabaseError` subclass. Dropping a referenced
  primary key (`-923`) is a schema error and stays `DatabaseError`. (#493)
- A `TIMESTAMPTZ`/`TIMESTAMPLTZ`/`DATETIMETZ`/`DATETIMELTZ` value whose
  region the client's IANA time zone database cannot resolve now raises
  `DataError` naming the zone, with a hint to install `tzdata`, instead of
  logging a warning per value and returning a naive `datetime`. The session
  stays usable. This mostly affects clients without a time zone database
  (Windows without `tzdata`, minimal container images), where every region
  value, including the LTZ types' `UTC`, silently lost its zone. An offset
  that is malformed or not strictly within ±24 hours also raises `DataError`
  instead of `OperationalError('malformed response from broker')` with a
  closed connection. The explicit prepared API (`pycubrid.compat.native`)
  stays fail-closed, as in #492. Offsets, resolvable regions and an empty
  zone suffix decode as before. pycubrid now
  depends on `tzdata` on Windows only (`tzdata; sys_platform == 'win32'`).
  (#413)
- A region value in the repeated hour when daylight saving time ends now
  honors the abbreviation CUBRID sends: `America/New_York EST` at
  2026-11-01 01:30 decodes with `fold=1` (UTC-05:00) instead of the EDT
  instant an hour earlier. A missing or unknown abbreviation, or one both
  occurrences share, keeps `fold=0`. (#413)

### Tests
- **Offline tests pin the backslash-escape mode to the server default** — the
  autouse `_skip_backslash_probe` fixture pinned scripted connections to
  `no_backslash_escapes=False`, the non-default mode, so cursor-level tests
  bound strings in a mode an unconfigured CUBRID does not use. It now pins
  `True`. The offline suite passes unchanged with either value; tests about a
  specific mode already set it explicitly, and `no_escape_pin` modules still
  negotiate for real.
- **The version differential no longer generates a conditional that crashes
  every supported CUBRID (#614)** — `SELECT IF(1=0, SET{1}, 0.000)` ends `csql`
  with SIGSEGV on 10.2, 11.0, 11.2 and 11.4; original CI observed CAS session
  loss on 10.2, without directly confirming CAS loss on the other versions.
  The trigger is a collection branch beside a selected NUMERIC branch of
  scale 2 or more. The
  expression grammar drew that pairing in about 5% of 50-example runs and 75%
  of 1000-example runs, so the full matrix, which is also the release gate,
  failed on a server defect that says nothing about pycubrid. `IF` and
  `CASE WHEN` now leave the collection/numeric pairing out
  (`_is_fatal_conditional`); only `IF` was measured, `CASE WHEN` is excluded
  with it unverified. Not validated against live servers in this change. The
  crash itself is unchanged; upstream reporting is tracked separately in #675.
- **A fatal statement now fails one test and names every version it affects,
  instead of cascading and reporting only the first endpoint to die (#614)** —
  two separate defects. First, the `servers` fixture is module-scoped and every
  test reused one connection per endpoint, so when a statement left a session
  unusable that test failed and the remaining 15 in the module then failed with
  `InterfaceError: connection is closed`. Second, `compare()` built its
  observations in a dict comprehension, so the first endpoint to raise aborted
  the rest: with CUBRID 10.2 first in the matrix, `SELECT IF(1=0, SET{1}, 0.000)`
  was attributed to 10.2 alone, while `csql` reproduces the same SIGSEGV
  deterministically on 10.2.18.9024, 11.0.16.0419, 11.2.9.0866 and 11.4.6.1963 —
  the suite's own structure concealed that the crash affects every supported
  version. `Server` now opens its session through `_open()`; a session that died
  without any statement reporting it fails visibly through
  `_require_live_session()` rather than being healed in silence. Its liveness
  probe uses `ping(reconnect=False)`, so SQL auto-recovery cannot hide the dead
  prior session; and
  `_assert_session_survives()` reopens before raising the new
  `SessionLost(AssertionError)`. `compare()` runs every endpoint, collects the
  endpoints that lost a session, and raises one report that lists each of them
  with the versions that completed. The contract is unchanged: losing a session
  still fails its own test, and the message now also reports when reopening
  failed. Scratch tables survive a reopen (DDL runs with autocommit on), so
  `created` stays accurate and `ensure_table` still skips them. New offline
  module `tests/test_version_differential_isolation.py` drives `Server` and
  `compare()` against scripted connections, since the real lane needs four live
  servers: a fatal statement fails once, the next workload runs on a fresh
  session, three consecutive kills open exactly three replacements, an error the
  session survives opens none, a reopen does not recreate scratch tables, an
  unreported dead session fails visibly even if SQL could transparently reconnect,
  a four-endpoint matrix names all four
  when all are fatal, and names only the affected one when a single version is.
  The underlying CUBRID crash is not fixed here — it is a server-side defect to
  report upstream, tracked separately in #675.
- **`tests/test_docs_reason.py` runs the docs-sync script in-process instead
  of spawning a fresh `python -` subprocess per fixture case, and the fake
  `git` shim is a shell script instead of a Python one (#429)** — the event
  JSON regression test took ~8.7s of the offline suite's ~22s runtime; it now
  runs in well under a second, exercising the exact script text extracted
  from the workflow file against the same fake `git` subprocess, just without
  the per-case interpreter startup cost.
- **`scripts/check_docs_reason.py` reduces empty emphasis inside a caption
  before deciding it is populated (#429)** — a reason whose only content is a
  caption like `[**<!-- empty -->**](/issue)` rendered no visible
  explanation, but the `**` emphasis delimiters around the (invisible)
  comment were counted as real content and the reason was accepted. Of the
  four cases Codex reported against the final head of #425 (`e94ee3d`), this
  was the only one still reproducible on `main`; the other three (an
  unmatched backtick pairing across a type-6 HTML block, bracket-bearing
  HTML-only captions, and compound empty-caption markup) were already fixed
  by later commits before #425 merged. All four now have a regression
  fixture. This is the smaller "harden the existing structure" option the
  issue offered as an alternative to rewriting the helper; see the issue for
  the recorded decision. The helper is shared byte-for-byte with
  sqlalchemy-cubrid and cubrid-cookbook-python (#429), so the same fix needs
  the same follow-up PR in each.
- **The backslash-escape-mode pin opts out on an explicit marker, not a
  filename guess (#524)** — `tests/conftest.py`'s autouse fixture used to skip
  the pin for any module whose path matched one of 17 hardcoded filename
  substrings; `"test_integration"` is a prefix of every `test_integration_*.py`
  module, so all of them opted out whether or not they actually negotiate
  against a live server. Opt-out is now `pytest.mark.no_escape_pin`
  (registered in `pyproject.toml`), carried directly by every module that
  needs it — alongside the existing `integration` marker for the ones that
  also gate on a live server. `tests/test_integration_lanes.py` (a workflow-YAML
  regression test that never builds a `Connection`) no longer opts out; every
  other previously-opted-out module keeps the same behavior. Auditing every
  `integration`-marked module (not just the ones the old filename list
  happened to catch) found eight more that open real connections without an
  explicit `no_backslash_escapes` and were silently pinned instead of
  negotiating: `test_parity_integration.py`, `test_stress_concurrency.py`,
  `test_compat_prepared_integration.py`, `test_compat_factories_integration.py`,
  `test_tls_matrix_integration.py`, `test_aio_ssl_integration.py`, and one
  function each in `test_cas_session_persistence.py` and
  `test_connection_failures.py`; these now carry the marker too (a real
  behavior change, fixing a latent bug predating this PR). Two further
  `integration`-marked modules, `test_schema_integration.py` and
  `test_schema_matrix.py`, were checked and correctly excluded: every
  connection they open passes `no_backslash_escapes=True` explicitly, so the
  pin was always a no-op for them. A follow-up review pass widened the audit
  beyond `integration`-marked modules to every test that opens a live
  connection: `test_benchmarks.py` (`pytest.mark.benchmark`, gated on
  `CUBRID_TEST_URL` rather than `integration`) also negotiates for real and
  now carries the marker too. `test_fault_broker.py` and
  `test_aio_tls_handshake_hang.py` were checked and correctly excluded: both
  talk to an in-process fake local server, not the configured live CUBRID
  endpoint, and either pass `no_backslash_escapes` explicitly or only
  exercise failure paths that never reach negotiation.
- **Per-operation round-trip budgets for the sync/async replay harness
  (#557)** — `tests/test_replay_parity.py` scenarios compared whole-session
  request sequences, so both drivers growing the same extra request on a
  single operation would stay green. `Observation.step_functions(i)` now
  exposes the exact, ordered CAS functions sent while running one scenario
  step alone, separate from connect/setup and every other step. Seven new
  scenarios assert named budgets — `FIRST_INSERT_BUDGET`,
  `REUSED_CURSOR_INSERT_BUDGET`, `SELECT_TO_INSERT_BUDGET`,
  `MANUAL_INSERT_EXECUTE_BUDGET` / `MANUAL_INSERT_COMMIT_BUDGET`,
  `FETCH_PAGINATION_BUDGET`, `ESCAPE_EXPLICIT_*` / `ESCAPE_AUTOMATIC_*` — for
  a fresh cursor's first autocommitting INSERT, a second INSERT reusing the
  same cursor, an autocommitting INSERT after a SELECT on the same cursor, a
  manual-transaction INSERT and its explicit `commit()`, paginated `FETCH`
  over a small `fetch_size`, and backslash-escape-mode negotiation resolved
  explicitly versus automatically. Each budget is exact-list equality, so a
  dropped safety request (e.g. a missing `CHECK_CAS` liveness probe) fails
  the same as an added round trip; neither can pass as an "optimization".
  Budget scenarios also require successful outcomes, a reusable session and
  expected fetched rows, so malformed replies and wrong results cannot pass
  solely by preserving the request count.
  Existing scenarios, their checks and the sync/async parity and
  reconnect/no-replay coverage are unchanged. No production behavior changes
  in this PR; these scenarios are the reproducibility baseline that later
  round-trip-reduction work (#419/#488/#525) must not silently regress.
- **Repository policy/tooling checks run in a separate required CI job
  instead of the default offline-tests matrix (#558)** — the offline suite
  mixed mocked driver-behavior tests with subprocess-/importlib-heavy
  repository policy checks (docs-sync, PR-title, release scripts,
  workflow-YAML contracts, the shared quality gate, and similar), which
  unnecessarily lengthened routine driver feedback: on this machine, the
  default offline run dropped from 79.9s to 50.2s (2,671 tests), with the
  305 moved tests taking 29.0-30.6s of either figure, run count unchanged
  (2,976 passed both before and after). The fifteen modules in question
  (`test_docs_reason.py`, `test_pr_title.py`, `test_quality_tools.py`,
  `test_release_detect.py`, `test_readiness_workflows.py`,
  `test_prepare_release.py`, `test_release_summary.py`,
  `test_release_workflows.py`, `test_pypi_duplicate_guard.py`,
  `test_upstream_scenario_ledger.py`, `test_check_public_api.py`,
  `test_issue_metadata.py`, `test_collect_repro.py`,
  `test_integration_lanes.py`, `test_check_official_differential.py`) now
  carry an explicit `pytestmark = pytest.mark.repo_tooling` (the marker is
  registered in `pyproject.toml`, the same explicit-marker convention used
  elsewhere in the suite) instead of relying on file location; nothing moved
  on disk, so recursive pytest discovery still collects them and no check
  silently disappears. `offline-tests` now runs
  `-m "not integration and not repo_tooling"`; a new `repo-tooling-tests` CI
  job runs `-m "repo_tooling"` on a 2-OS (ubuntu, macos) x 1-Python matrix,
  keeping shell-dependent checks covered on both platforms without repeating
  all five Python versions. `repo-tooling-tests` is a required job in the CI
  Gate, alongside `offline-tests`, `lint`, `typecheck`, `packaging-smoke-test`
  and `compat-check` — no CI requirement is weakened or dropped.
  `docs/DEVELOPMENT.md` (and its Korean translation) documents the fast-driver,
  repository-tooling and combined offline commands.

### CI
- **Bounded bug-hunt failure context (#655)** — the existing property job
  supplies its three xunit1 reports and an optional same-probe readiness sidecar
  to the diagnostic collector. Metadata retains explicit report/identity/error
  states; replay uses only available targets and quoted tokens, with known
  credentials redacted before byte-limited details. Collection is best effort,
  not a passing-job signal, and binary Hypothesis examples still require trusted
  review before sharing. Matrix/gates and public driver APIs are unchanged.
- **Mutation lane migrated to mutmut 3 after 14 consecutive crashed runs (#612)** —
  `[tool.mutmut]` still used the 2.x keys. With `mutmut>=3.0` resolving to 3.8,
  `tests_dir` (a string) was concatenated onto a list at
  `mutmut/configuration.py:155`, raising `TypeError: can only concatenate list
  (not "str") to list` from the first `config()` call, which runs at CLI import
  time — so every subcommand died, `mutmut --version` included, and the nightly
  `mutation testing (driver core, offline)` job produced no measurement from
  2026-09-18 through 2026-10-01. A second defect was hiding behind it: in
  `pyproject.toml` mutmut returns TOML values verbatim, so the comma-joined
  `paths_to_mutate` string became one `Path` per character (66 of them), meaning
  the mutated-file list had never been read as intended either. The config now
  uses TOML arrays with the mutmut 3 keys (`source_paths`, `only_mutate`,
  `also_copy`, `pytest_add_cli_args`, `pytest_add_cli_args_test_selection`,
  `process_isolation`), and the requirement is `mutmut>=3.8,<4`. Three follow-on
  problems were found by running the lane rather than by inspection, and each is
  documented inline where it is configured: mutmut copies only `source_paths`
  plus a built-in list into `mutants/`, so the offline suite needs `scripts`,
  `.github` and `docs` in `also_copy`; two tests in
  `tests/test_unknown_options.py` inspect pycubrid's own source (warning
  `stacklevel`, `inspect.signature`) and cannot hold against mutmut's function
  trampolines, so they are deselected for this lane only; and default `fork`
  isolation forks from a process that has already run the suite, which trips
  Hypothesis `HealthCheck.differing_executors`, so the lane uses `forkserver`.
  `tests/test_compat_factories.py`'s public-API gate moved to the `repo_tooling`
  marker, where it belongs — it compares `api-baseline.json` against the live
  surface, which a mutated package can never match. That moves one test from
  CI's offline job to its repo-tooling job (3316 to 3315, and 322 to 323); `make
  test` still runs it. Also cleaned up 2.x leftovers: the workflow uploaded
  `.mutmut-cache` (mutmut 3 writes `mutants/`), `.gitignore` did not list
  `mutants/` so the sandbox could have been committed, and `make clean` left it
  behind. Measured on Python 3.12 with mutmut 3.8.0: 5509 of 6878 mutants
  killed (80.1%), 1301 survived, 53 timed out, 15 reached no test; the run took
  about 58 minutes wall clock at 1.97 mutations/second on 8 workers, which is the
  lane's first known cost and worth weighing against the nine-file `only_mutate`
  scope.
  Offline coverage of the nine mutated files is 97.66% (3962/4057 statements),
  unchanged by this commit. The job's failure policy is unchanged and is not what
  this commit claims to set: the `mutation` job carries no `continue-on-error`,
  so a failure turns the nightly run red, though `bug-hunt.yml` has no gate job
  and is schedule/on-demand only, so nothing blocks a PR or a release. Whether
  the lane should become advisory is a policy decision recorded in #612, not
  something this migration changes.
- **CI pip download caching and readiness path-filter fix (#564)** —
  baseline measurements found 19 expanded jobs making separate editable dev
  installs; `cache: pip` on the 10 `setup-python` YAML steps now permits
  matching OS/Python jobs to reuse downloaded wheels without skipping the
  installs. A warm same-head run had an observed cache hit and took 295s,
  versus a 306s baseline and a 328s cold first attempt; runner and Docker
  variance prevent attributing the entire difference to caching. Added
  `scripts/wait_for_cubrid.py` to the `code:` filter so helper-only changes
  select the required integration and official-differential lanes. A real
  official comparison failure previously failed `CI Gate`, and new tests
  preserve that fail-closed behavior and the docs-only skip exception. No
  job, endpoint, release/nightly gate or required-check context changed.
  Detailed data and limitations are in `docs/DEVELOPMENT.md`.

### Conventional commits

#### Features

* add charset connection option ([#510](https://github.com/cubrid-lab/pycubrid/issues/510)) ([3874252](https://github.com/cubrid-lab/pycubrid/commit/387425245e70c63e6b5d99265d5e8cbca61affb0))
* **compat:** add fixed-session connection utilities ([#667](https://github.com/cubrid-lab/pycubrid/issues/667)) ([9fbbc05](https://github.com/cubrid-lab/pycubrid/commit/9fbbc05b494e52197907bea138b36a3380ff6421)), closes [#666](https://github.com/cubrid-lab/pycubrid/issues/666)
* **compat:** add native cursor seek and tell ([ff8eee7](https://github.com/cubrid-lab/pycubrid/commit/ff8eee7fd4c8c403b48000747293bf55af3bb3c3)), closes [#444](https://github.com/cubrid-lab/pycubrid/issues/444) [#444](https://github.com/cubrid-lab/pycubrid/issues/444)
* **compat:** bind collections with official native set API ([#607](https://github.com/cubrid-lab/pycubrid/issues/607)) ([c316c39](https://github.com/cubrid-lab/pycubrid/commit/c316c39a3c7ef993c541b93114380dc10ed778dc))
* **compat:** expose cached result_info without moving rows ([#642](https://github.com/cubrid-lab/pycubrid/issues/642)) ([ee62e28](https://github.com/cubrid-lab/pycubrid/commit/ee62e28a5f316e779da20cfe575a1cd0ed2dda30))
* **compat:** expose wrapper transaction boundaries ([#664](https://github.com/cubrid-lab/pycubrid/issues/664)) ([44aafb9](https://github.com/cubrid-lab/pycubrid/commit/44aafb9ed7cf413eb558d0a3709c88fcbb19e38c)), closes [#662](https://github.com/cubrid-lab/pycubrid/issues/662)
* **compat:** fetch and bind LOB handles with official native API ([#616](https://github.com/cubrid-lab/pycubrid/issues/616)) ([068325e](https://github.com/cubrid-lab/pycubrid/commit/068325e75abb068a6b9d4f9bc841038cb7743a1e))
* **compat:** preserve official wrapper rows on supported scalars ([2215d0a](https://github.com/cubrid-lab/pycubrid/commit/2215d0a739252a25e93a399a9e3fc84a96501f48))
* **compat:** separate cached native settings from effective setters ([7a22678](https://github.com/cubrid-lab/pycubrid/commit/7a226783103136fa518f5a479622799b2bea8b4d))
* **compat:** separate native LOB streams from ordinary offset APIs ([bebef4d](https://github.com/cubrid-lab/pycubrid/commit/bebef4d3b9abdc65feef62d770d014e8ececff4c))
* **driver:** add typed Set, Multiset and Sequence parameters for ordinary cursors ([#568](https://github.com/cubrid-lab/pycubrid/issues/568)) ([17cc5dc](https://github.com/cubrid-lab/pycubrid/commit/17cc5dca98ee33ad052ce5ba8d04bfe6b9826812))
* preserve native LOB file workflows on failure ([#653](https://github.com/cubrid-lab/pycubrid/issues/653)) ([5d40a0f](https://github.com/cubrid-lab/pycubrid/commit/5d40a0f8bfc1f51ff43ab4b1c8bd3b119783a999)), closes [#443](https://github.com/cubrid-lab/pycubrid/issues/443)
* **protocol:** add the internal typed collection binding wire contract ([#569](https://github.com/cubrid-lab/pycubrid/issues/569)) ([bf53d2c](https://github.com/cubrid-lab/pycubrid/commit/bf53d2ca45290533ad8d6b47cbb54c4a9ba533c5))


#### Bug Fixes

* **aio:** bound async TLS connect when the handshake stalls or is reset ([#534](https://github.com/cubrid-lab/pycubrid/issues/534)) ([bd878fe](https://github.com/cubrid-lab/pycubrid/commit/bd878fe1af7b4e0eb5e76e47c9da3407d5394944))
* **async:** isolate setup cancellation from waiting operations ([#578](https://github.com/cubrid-lab/pycubrid/issues/578)) ([7f5fe22](https://github.com/cubrid-lab/pycubrid/commit/7f5fe229000ebec13bc14427f0dfe277acfb5b09))
* **binding:** keep invalid timezone failures within DB-API errors ([4fcd9c8](https://github.com/cubrid-lab/pycubrid/commit/4fcd9c82be27e56fcdf1ab8d992c56d1d8b0c994))
* **changelog:** reject duplicate release subsections ([#648](https://github.com/cubrid-lab/pycubrid/issues/648)) ([3c57bb9](https://github.com/cubrid-lab/pycubrid/commit/3c57bb9ffab347f5c57e27d080d4a9b205c64083))
* **ci:** report a skipped cookbook verification as failed in the release summary ([#548](https://github.com/cubrid-lab/pycubrid/issues/548)) ([45f3374](https://github.com/cubrid-lab/pycubrid/commit/45f3374b4cfa9d191316c935b3801e827aa94b1b))
* classify foreign-key restrict failures (-924, -1284) as IntegrityError ([95a34e6](https://github.com/cubrid-lab/pycubrid/commit/95a34e6fcb6dcca27ed10a11e24aadb18cb34c2d))
* clear previous results after failed execute ([#531](https://github.com/cubrid-lab/pycubrid/issues/531)) ([32195b1](https://github.com/cubrid-lab/pycubrid/commit/32195b1f856b8d499a6e45ebfb89ee4a7ebc57ea))
* **compat:** refresh failed plans only on the next explicit execution ([e39e00e](https://github.com/cubrid-lab/pycubrid/commit/e39e00e8e61b2c04f3e8250a7b82cde1a3f6a82a))
* **connection:** preserve deferred handle ownership across interrupts ([1686920](https://github.com/cubrid-lab/pycubrid/commit/16869201ca04a8b9e0b4ee5e73cb996bb38c0625))
* **connection:** retire cursor handles on transport failures and clarify timeout errors ([3d72216](https://github.com/cubrid-lab/pycubrid/commit/3d72216ff807f9a1fe6d7292cf754f44359b2626)), closes [#556](https://github.com/cubrid-lab/pycubrid/issues/556)
* **cursor:** retire pooling-off handles at transaction replies ([#598](https://github.com/cubrid-lab/pycubrid/issues/598)) ([1251165](https://github.com/cubrid-lab/pycubrid/commit/125116574a7eeb05c942bcc24563be28eef9ee2b)), closes [#584](https://github.com/cubrid-lab/pycubrid/issues/584)
* DBAPIType should not treat bool values as integer type ([#479](https://github.com/cubrid-lab/pycubrid/issues/479)) ([4efe599](https://github.com/cubrid-lab/pycubrid/commit/4efe599d29965e606e3ce77682c162be9830fcd7)), closes [#369](https://github.com/cubrid-lab/pycubrid/issues/369) [#369](https://github.com/cubrid-lab/pycubrid/issues/369)
* decode nonempty NULL-only collection results ([c588055](https://github.com/cubrid-lab/pycubrid/commit/c588055f9bbe31ae6872430bd75aede26060cbec))
* decode nonempty NULL-only collection results ([e379116](https://github.com/cubrid-lab/pycubrid/commit/e379116e618791b8a65a060c108139954ff3fea9)), closes [#483](https://github.com/cubrid-lab/pycubrid/issues/483)
* **driver:** keep already fetched rows when a later fetch page raises DataError ([#536](https://github.com/cubrid-lab/pycubrid/issues/536)) ([ef815f8](https://github.com/cubrid-lab/pycubrid/commit/ef815f853b5167daae016416c05701b4a420332e))
* **driver:** keep the autocommit setter's requests on one CAS session ([#552](https://github.com/cubrid-lab/pycubrid/issues/552)) ([7ac5a3e](https://github.com/cubrid-lab/pycubrid/commit/7ac5a3e528efb97516f638d607e5f3547fc3d69b))
* **driver:** render Decimal parameters in plain notation so CUBRID keeps them NUMERIC ([#526](https://github.com/cubrid-lab/pycubrid/issues/526)) ([a7a3563](https://github.com/cubrid-lab/pycubrid/commit/a7a3563e4ee2fecc06c9208ecbe42392f575749f))
* **driver:** render int, float and Decimal subclasses by value instead of str() ([#527](https://github.com/cubrid-lab/pycubrid/issues/527)) ([0577cd8](https://github.com/cubrid-lab/pycubrid/commit/0577cd82e1de3858b3694b0105f3f13c622082f9)), closes [#518](https://github.com/cubrid-lab/pycubrid/issues/518)
* **driver:** render str, bytes, date and time parameters without calling overridable methods ([#529](https://github.com/cubrid-lab/pycubrid/issues/529)) ([dde9a07](https://github.com/cubrid-lab/pycubrid/commit/dde9a071f44ae275a5db486dea8ecf1521889761))
* **integration:** preserve volumes during automatic Docker cleanup ([#647](https://github.com/cubrid-lab/pycubrid/issues/647)) ([f3b2e69](https://github.com/cubrid-lab/pycubrid/commit/f3b2e6983e62ae3d5d72149188d3357de96e5445))
* keep the session when a complete broker reply carries invalid UTF-8 ([734fb29](https://github.com/cubrid-lab/pycubrid/commit/734fb294276927fa50c6887e99871a82a2c8eebc))
* **lob:** keep the handle size current and add internal LOB-handle binding ([#615](https://github.com/cubrid-lab/pycubrid/issues/615)) ([35d4e2c](https://github.com/cubrid-lab/pycubrid/commit/35d4e2cbedbea85bbdac0d76d047572e5cffb6ff))
* **protocol:** avoid blaming following tokens for reserved identifiers ([5fde41f](https://github.com/cubrid-lab/pycubrid/commit/5fde41f37dd030f7c6098f3a7a8d70a9ecbc5f4a))
* **protocol:** decode CALL and NULL-typed cells with the two-byte type header ([#549](https://github.com/cubrid-lab/pycubrid/issues/549)) ([e329a11](https://github.com/cubrid-lab/pycubrid/commit/e329a117c153d891e85b53a1cfd24580ca38766e))
* **protocol:** finish framing validation before raising DataError and close remaining FC41 count gaps ([#588](https://github.com/cubrid-lab/pycubrid/issues/588)) ([d19a981](https://github.com/cubrid-lab/pycubrid/commit/d19a9819067262d64f3c0e2f09a009edd7a5fc71)), closes [#581](https://github.com/cubrid-lab/pycubrid/issues/581)
* **protocol:** keep collection errors from hiding malformed elements ([#597](https://github.com/cubrid-lab/pycubrid/issues/597)) ([1a93ecf](https://github.com/cubrid-lab/pycubrid/commit/1a93ecf6b69012dd8a3a85be9d16a65898d5ec60)), closes [#591](https://github.com/cubrid-lab/pycubrid/issues/591) [#595](https://github.com/cubrid-lab/pycubrid/issues/595)
* **protocol:** raise DataError for invalid JSON in a complete reply ([#571](https://github.com/cubrid-lab/pycubrid/issues/571)) ([c1a5de5](https://github.com/cubrid-lab/pycubrid/commit/c1a5de55db5a5ecca164bb6826e55726dd56f8aa))
* **protocol:** raise DataError for zero DATE and DATETIME values instead of closing the connection ([#532](https://github.com/cubrid-lab/pycubrid/issues/532)) ([5357f66](https://github.com/cubrid-lab/pycubrid/commit/5357f660a39ef048e4d90f06d38e91a47b742bc8))
* **protocol:** reject byte reads past the end of a broker reply ([#533](https://github.com/cubrid-lab/pycubrid/issues/533)) ([4605671](https://github.com/cubrid-lab/pycubrid/commit/4605671725dce24c895996770ed29d51bb1adb25))
* **protocol:** reject negative FC41 metadata lengths and column counts ([#574](https://github.com/cubrid-lab/pycubrid/issues/574)) ([a341cec](https://github.com/cubrid-lab/pycubrid/commit/a341cec58f36d33b8ae75734b3854619c7287911)), closes [#555](https://github.com/cubrid-lab/pycubrid/issues/555)
* **protocol:** validate reply tails before metadata DataError ([#596](https://github.com/cubrid-lab/pycubrid/issues/596)) ([62f5866](https://github.com/cubrid-lab/pycubrid/commit/62f5866898818faa9146249712bc40a0fef80f4f)), closes [#591](https://github.com/cubrid-lab/pycubrid/issues/591)
* raise DataError for unresolvable TZ zones and honor DST abbreviations ([180dd56](https://github.com/cubrid-lab/pycubrid/commit/180dd56b1b48c9fb6649285b109d8d7dbfaa752b))
* raise DataError for unresolvable TZ zones and honor DST abbreviations ([209b201](https://github.com/cubrid-lab/pycubrid/commit/209b20154591783e80dbb5c758ef4629dfc3367d)), closes [#413](https://github.com/cubrid-lab/pycubrid/issues/413)
* reject malformed fetch sizes before rows or pages are consumed ([#679](https://github.com/cubrid-lab/pycubrid/issues/679)) ([8dd1198](https://github.com/cubrid-lab/pycubrid/commit/8dd11989167594f36774da824224064073883240)), closes [#371](https://github.com/cubrid-lab/pycubrid/issues/371)
* reject malformed qualified callproc names ([#515](https://github.com/cubrid-lab/pycubrid/issues/515)) ([601a879](https://github.com/cubrid-lab/pycubrid/commit/601a879f52b951ccdc366d95fde47b3d2d84b75a)), closes [#372](https://github.com/cubrid-lab/pycubrid/issues/372) [#372](https://github.com/cubrid-lab/pycubrid/issues/372)
* **test:** fail instead of skip when a configured CUBRID endpoint is unreachable ([#541](https://github.com/cubrid-lab/pycubrid/issues/541)) ([68851c9](https://github.com/cubrid-lab/pycubrid/commit/68851c9aa9b125496250f7fff2ae6c4a3821f2b5))
* tighten TZ fold selection and reject out-of-range offsets ([98d8150](https://github.com/cubrid-lab/pycubrid/commit/98d8150403a525dc27f7d6003d8f5944984cdadc))
* **timeouts:** validate configuration before transport acquisition ([#646](https://github.com/cubrid-lab/pycubrid/issues/646)) ([7577e4d](https://github.com/cubrid-lab/pycubrid/commit/7577e4d5c33911ee7c7ca7707fbd37e2ad9e04d4))
* **tls:** bound sync TLS handshakes without read_timeout and close sockets on the Python 3.10 pre-connect path ([#586](https://github.com/cubrid-lab/pycubrid/issues/586)) ([3af12ae](https://github.com/cubrid-lab/pycubrid/commit/3af12ae7d97ea6e1c5e30e616babc0f54fc7ddd0))
* **tls:** preserve probe failures while notifying TLS peers ([fa7b22a](https://github.com/cubrid-lab/pycubrid/commit/fa7b22ad4707510830d8cccb1518c3a875d1c330))
* **tls:** preserve the total verification probe deadline ([#594](https://github.com/cubrid-lab/pycubrid/issues/594)) ([7dddab3](https://github.com/cubrid-lab/pycubrid/commit/7dddab31c22f8bb8b837c30d6e942d483abe5f10)), closes [#593](https://github.com/cubrid-lab/pycubrid/issues/593)
* **types:** make typed collection parameters truly immutable, copyable and picklable ([#580](https://github.com/cubrid-lab/pycubrid/issues/580)) ([882b704](https://github.com/cubrid-lab/pycubrid/commit/882b704165ac7d1b614c2831995a8071c3b654ac))
* validate LOB offset and length types before serialization ([#516](https://github.com/cubrid-lab/pycubrid/issues/516)) ([dad392e](https://github.com/cubrid-lab/pycubrid/commit/dad392e8396ae82c7db962671c1c714b3dfe2dd5))
* validate size of empty NULL-type collections ([775f38c](https://github.com/cubrid-lab/pycubrid/commit/775f38c1d2b2422617ae8546ce11b3ccd445bc7f))


#### Performance Improvements

* **protocol:** benchmark fetch parsing and trim per-cell overhead ([#602](https://github.com/cubrid-lab/pycubrid/issues/602)) ([cfecfba](https://github.com/cubrid-lab/pycubrid/commit/cfecfba3cc1251b38465669427352579691e3b17)), closes [#559](https://github.com/cubrid-lab/pycubrid/issues/559)
* release unclosed cursor handles in autocommit mode with a deferred close queue ([fe222fb](https://github.com/cubrid-lab/pycubrid/commit/fe222fb51ebb6fccf69f4d88941a1ecff9d82479)), closes [#488](https://github.com/cubrid-lab/pycubrid/issues/488)


#### Reverts

* release 1.9.0 ([#672](https://github.com/cubrid-lab/pycubrid/issues/672)) ([cf2fe9d](https://github.com/cubrid-lab/pycubrid/commit/cf2fe9d474a02a56234e13c5992fbcd8e5c362a1))


#### Documentation

* anchor current verification claims to maintained sources ([#654](https://github.com/cubrid-lab/pycubrid/issues/654)) ([127c6a1](https://github.com/cubrid-lab/pycubrid/commit/127c6a1f5bcf67eeefbcd6623a577887745b304e)), closes [#415](https://github.com/cubrid-lab/pycubrid/issues/415)
* CHANGELOG, RELEASE_POLICY, PROTOCOL (EN+KO), regenerated llms-full. ([a341cec](https://github.com/cubrid-lab/pycubrid/commit/a341cec58f36d33b8ae75734b3854619c7287911))
* **contributing:** keep issue specifications current and assign owners ([ea32de4](https://github.com/cubrid-lab/pycubrid/commit/ea32de472b1319182af73826af52ecdd3917be99))
* **demo:** replace simulated walkthroughs with verified recordings ([#665](https://github.com/cubrid-lab/pycubrid/issues/665)) ([a44e250](https://github.com/cubrid-lab/pycubrid/commit/a44e2502568a9492017a4655402eefd10bf07b0a)), closes [#320](https://github.com/cubrid-lab/pycubrid/issues/320)
* keep the release backlog review grounded in current issue scopes ([#680](https://github.com/cubrid-lab/pycubrid/issues/680)) ([4a9c598](https://github.com/cubrid-lab/pycubrid/commit/4a9c598555f2d3daa42e0055d64c9dbcf43d1e24)), closes [#566](https://github.com/cubrid-lab/pycubrid/issues/566)
* let the mitigated differential lane ship without claiming an upstream fix ([#676](https://github.com/cubrid-lab/pycubrid/issues/676)) ([14f7398](https://github.com/cubrid-lab/pycubrid/commit/14f739866040c1bb9efc47d7426b50661007f3bd)), closes [#614](https://github.com/cubrid-lab/pycubrid/issues/614)
* make current contributor procedures accessible in Korean ([ac89eb3](https://github.com/cubrid-lab/pycubrid/commit/ac89eb36fefd8c2b6a61b0b3cb11ed6dc9615440)), closes [#329](https://github.com/cubrid-lab/pycubrid/issues/329)
* make the quick start usable without missing table columns ([#677](https://github.com/cubrid-lab/pycubrid/issues/677)) ([518aa8b](https://github.com/cubrid-lab/pycubrid/commit/518aa8be4e2209b05e1a0c0faf68f9f3b7853df0)), closes [#327](https://github.com/cubrid-lab/pycubrid/issues/327) [#327](https://github.com/cubrid-lab/pycubrid/issues/327)
* narrow the plan-reuse wording and catch deleted llms artifacts in CI ([48aeefc](https://github.com/cubrid-lab/pycubrid/commit/48aeefc4ea049d7a764ca8cdd01e5cdb1cf7dc06))
* note that LTZ values follow the session time zone kept across commit ([2f47b69](https://github.com/cubrid-lab/pycubrid/commit/2f47b699ddae5274b4a3e2557cb1b263fca7b274))
* note that LTZ values follow the session time zone kept across commit ([d4b370a](https://github.com/cubrid-lab/pycubrid/commit/d4b370aed70563ab898a701b970d307d18d114a9))
* **performance:** make the Korean guide discoverable and navigable ([5b394b7](https://github.com/cubrid-lab/pycubrid/commit/5b394b70fd178fc529d2482e45faa91ac20f0384))
* pin cookbook smoke-test fallback to the exact release ([dd621c4](https://github.com/cubrid-lab/pycubrid/commit/dd621c427b883b7b987563049ce8d398f35ee2ed))
* pin cookbook smoke-test fallback to the exact release ([330682c](https://github.com/cubrid-lab/pycubrid/commit/330682c6d1c18900a0a81dd6a3133dc90bfe7d4a))
* **protocol:** retain raw errno after renewed-code evaluation ([#656](https://github.com/cubrid-lab/pycubrid/issues/656)) ([eed27ab](https://github.com/cubrid-lab/pycubrid/commit/eed27abf8f4d350b84baaa6d7562812d90fc0aff)), closes [#505](https://github.com/cubrid-lab/pycubrid/issues/505)
* provide Korean contributor procedures ([#663](https://github.com/cubrid-lab/pycubrid/issues/663)) ([ac89eb3](https://github.com/cubrid-lab/pycubrid/commit/ac89eb36fefd8c2b6a61b0b3cb11ed6dc9615440))
* **python:** announce Python 3.10 support retirement ([#658](https://github.com/cubrid-lab/pycubrid/issues/658)) ([ba58b80](https://github.com/cubrid-lab/pycubrid/commit/ba58b80f5cc7af26f664f06a4b5c5e78bade827b))
* **release:** add 1.9.0 upgrade notes and mark new compat names provisional ([#671](https://github.com/cubrid-lab/pycubrid/issues/671)) ([7fba209](https://github.com/cubrid-lab/pycubrid/commit/7fba209adb241e7873bbc2671299eb516d73a21e)), closes [#396](https://github.com/cubrid-lab/pycubrid/issues/396)
* **release:** document release-please review and recovery ([fc922fe](https://github.com/cubrid-lab/pycubrid/commit/fc922fe1ebee46696124c12d2b5dad6a68ca64ef))
* **release:** fix create-release recovery command; make SBOM re-upload idempotent ([9784b0a](https://github.com/cubrid-lab/pycubrid/commit/9784b0a9677172a5fb1346d6f3011729c1640800))
* replace fixed Docker startup sleeps ([8154952](https://github.com/cubrid-lab/pycubrid/commit/8154952a475a772aca7dae96505d52b02eee551f))
* scope the time zone persistence to a retained CAS session ([8814081](https://github.com/cubrid-lab/pycubrid/commit/881408120342f51868805fc61563ab9d9a4a27f1))
* **security:** align maintenance and TLS guidance ([#649](https://github.com/cubrid-lab/pycubrid/issues/649)) ([ac63fd3](https://github.com/cubrid-lab/pycubrid/commit/ac63fd38fc946b6ad3cfab91333784cafc76a45f))
* single-source llms.txt and remove unsupported prepared-statement claims ([6039dbe](https://github.com/cubrid-lab/pycubrid/commit/6039dbec69f88091578083011d8a93924317b8b6))
* single-source llms.txt and remove unsupported prepared-statement claims ([8957eaf](https://github.com/cubrid-lab/pycubrid/commit/8957eaf6e6c92e127f0768af72df219200cb544d)), closes [#414](https://github.com/cubrid-lab/pycubrid/issues/414)
* **types:** state that a bound datetime.time drops microseconds ([#670](https://github.com/cubrid-lab/pycubrid/issues/670)) ([8332517](https://github.com/cubrid-lab/pycubrid/commit/833251756ff92c1300d59fde59e2b9015776054d))
* warn that pre-commit hooks need an active .[dev] environment ([acb4caa](https://github.com/cubrid-lab/pycubrid/commit/acb4caac56c2cbd479471f5a06124040be4ac29d))

## [1.8.0] - 2026-09-29

### Upgrade notes
Behavior changes you may notice (details in the entries below):
- Native NOT NULL (`-631`) and invalid foreign-key (`-922`) violations now raise
  `IntegrityError` (SQLSTATE `23000`, still a `DatabaseError` subclass) instead
  of a generic `DatabaseError`. (#390)
- Fetching from an unfinished SELECT result after `commit()`/`rollback()`
  invalidated its handle now raises `InterfaceError` instead of silently
  returning a partial result as if exhausted. Already received rows remain
  readable; no replay or holdable-result guarantee is added. (#395)
- `cursor.description` `null_ok` was inverted and is now correct: `True` for
  nullable columns, `False` for NOT NULL/primary-key columns. (#431)
- SET/MULTISET/SEQUENCE columns report their collection type codes and decode
  as collections with `decode_collections=True` (raw bytes when disabled). (#430)
- Normal `commit()`, `rollback()` and autocommit requests keep the same CAS
  session, so isolation level and session variables survive transaction
  boundaries. (#468, #472)
- A session time zone set with `SET TIME ZONE` now also survives
  `commit()`/`rollback()`. On 1.7.x a transaction boundary could transparently
  reconnect and silently fall back to the server default zone, so
  `DATETIMELTZ`/`TIMESTAMPLTZ` values read after a commit came back in that
  zone (often `+00:00`). On 1.8.0 they come back in the session zone you set:
  the same instant with a different UTC offset. Compare instants rather than
  offsets or wall-clock fields if your code or expected output relied on the
  old offset. If the CAS itself closes the socket, the zone is lost like other
  SQL session state (see the next note). (#468, #472)
- If the CAS closed the socket after a transaction boundary (CAS restart,
  CHANGE CLIENT, `cubrid broker reset`), the driver probes with `CHECK_CAS`
  and reconnects once before the next request. Driver-owned settings (escape
  mode unless pinned, explicit autocommit) are restored; session state set with
  SQL (isolation, session variables) is not, so re-apply it. SQL bound for a
  replaced session is never sent to the new one; a retryable `OperationalError`
  is raised instead. Requests after a boundary cost one extra round trip. (#485)
- `commit()`/`rollback()` now close server handles held by unclosed cursors. In
  autocommit mode there is no such boundary: close cursors yourself, or their
  handles stay open until commit/rollback/close. (#485)
- `get_last_insert_id()` returns `None` instead of `""` when no identity is
  available; replace `value == ""` checks with `value is None`. (#381)
- Unknown connection keyword arguments now emit
  `pycubrid.UnknownConnectionOptionWarning` (still ignored otherwise). Use
  `warnings.simplefilter("error", pycubrid.UnknownConnectionOptionWarning)` to
  reject them. (#377)

New explicit, staged APIs (additive; ordinary 1.x connect/cursor behavior is
unchanged):
- `pycubrid.compat.native` and `pycubrid.compat.cubriddb` factories construct
  and close an owned sync connection (#465). `compat.native` also offers a sync
  prepared scalar cursor limited to INT32, UTF-8 CHAR and SQL NULL bindings with
  tuple-only rows (#439). Neither is full official-driver/DB-API parity; there is
  no async preparation, and `compat.cubriddb` provides no cursor execution.
- `Connection.fetch_schema_info()` / `close_schema_info()` (sync and async)
  eagerly fetch and close owned schema rows (#456). Handles are closed at
  transaction boundaries and are never reconnected or replayed. Live
  verification covers CLASS/VCLASS/ATTRIBUTE/CONSTRAINT/PRIMARY_KEY/
  IMPORTED_KEYS/EXPORTED_KEYS on CUBRID 10.2 and 11.4, not all schema codes
  (#457).

### Added
- Explicit `pycubrid.compat.native` sync prepared scalar cursor (#439): one
  physical-session-owned FC2 handle supports repeated typed FC3 execution of
  INT32, UTF-8 CHAR and SQL NULL, tuple-only row fetch, current-generation
  FC6 close, and connection commit/rollback result hooks. Pooling-off/unknown
  sessions fail before FC2. HOLDABLE SELECT results continue across commit
  and invalidate across rollback; ordinary sync/async FC41 remains unchanged.
  Live scalar, DML and 130-row multi-FETCH gates pass on CUBRID 10.2, 11.0,
  11.2 and 11.4. Pinned official-native comparisons on 10.2/11.4 match the
  selected non-NULL scalar/DML results; native `bind_param(None)` crashes and
  is a documented safety deviation, not a NULL parity pass. This is an
  additive MINOR subset, not full native/DB-API
  parity, public async preparation, effective settings, or a release.
- Internal FC2/FC3 scalar packet groundwork (#475) now serializes validated
  INT32, UTF-8 CHAR and SQL NULL bindings, preserves the authoritative FC2
  bind count, and parses refreshed FC3 column metadata before shard/FETCH.
  Error records fail closed. This has no public prepared cursor or owner
  lifecycle yet; ordinary sync/async FC41 literal execution is unchanged and
  #439 remains the public implementation gate.
- Construction-only official-driver compatibility factories (#465): explicit
  `pycubrid.compat.native` and `pycubrid.compat.cubriddb` namespaces validate
  CUBRID/UTF-8 DSNs, preserve the source's public/empty credential defaults and
  start one owned pure-Python sync connection with autocommit enabled. Wrapper
  aliases and close are available; cursor execution, prepared binding, sharing,
  configurable charset and HA are not. Ordinary 1.x defaults and async behavior
  are unchanged. MINOR/additive public surface, protected by the API baseline.
- Owned schema rows (#456): corrected FC9 requests/condensed metadata now ship with sync/async eager `fetch_schema_info(packet)` and idempotent `close_schema_info(packet)`. Existing getter positional arguments and raw packet fields remain; keyword-only `arg2=None` adds the second filter. Immutable original-session ownership prevents forged/retired handle RPCs; explicit transaction boundaries and auto-committing cursor statements/batches and version lookup with connection autocommit enabled close schema handles before the boundary, while connection teardown and I/O failures retire resources. Schema FETCH/CLOSE do not reconnect, replay, implicitly commit or return partial rows as success. Initial live coverage was CLASS/ATTRIBUTE on 10.2/11.4; the #457 live matrix extends it to CLASS/VCLASS/ATTRIBUTE/CONSTRAINT/PRIMARY_KEY/IMPORTED_KEYS/EXPORTED_KEYS, not all schema codes or native parity. MINOR/additive surface; API baseline regenerated.
- **Unknown connection options are now surfaced instead of silently ignored (#377)** — `Connection.__init__`/`AsyncConnection.__init__` read a fixed set of options out of `**kwargs` and discarded everything else without a word, so a typo such as `read_timout=30` or `connectTimeout=5` was accepted, had no effect, and gave the caller no signal. Any keyword outside the supported set now emits a new `pycubrid.UnknownConnectionOptionWarning` (a `UserWarning` subclass, **not** part of the PEP 249 exception hierarchy) naming the offending option, suggesting the closest supported spelling when there is one, and listing the full supported set. Known options behave exactly as before, and the warning is emitted before any socket work so a mis-spelled option is reported even when the connection then fails. It covers `pycubrid.connect()`, `pycubrid.aio.connect()`, and direct `Connection(...)`/`AsyncConnection(...)` construction, and points at the caller's own line rather than pycubrid's internals.

  A warning rather than a hard `TypeError` is deliberate: wrapper layers (connection pools, ORM dialects such as `sqlalchemy-cubrid`) legitimately forward extra keywords, so rejecting them would be a breaking change under `RELEASE_POLICY.md` §3 and cannot land on the 1.x line. Callers choose their own strictness with the standard `warnings` machinery — `warnings.simplefilter("error", pycubrid.UnknownConnectionOptionWarning)` to reject unknown options, `"ignore"` to silence them. Additive surface change (`api-baseline.json` regenerated).

### Documentation
- Define the bounded typed-CAS prepared binding design (#418) for a future
  sync-only compatibility scalar slice (#439): exact FC2/FC3/FC6 framing,
  session-owned handle/result states, explicit pooling-on evidence limits,
  and failing-first test IDs. This is design evidence, not a shipped prepared
  API, ordinary 1.x behavior change, or full native-parity claim.
- Verify owned CLASS/VCLASS/ATTRIBUTE/index/composite PK/FK schema rows on CUBRID 10.2/11.4 in both sync and async modes, with real multi-FETCH/close evidence. Correct schema examples to consume/close results and supply ATTRIBUTE's second filter; document row-order/qualifier/index-family boundaries without claiming native parity. (#457)
- Select a conservative additive compatibility design (#438): separate wrapper/native namespaces, unchanged ordinary 1.x/SQLAlchemy contracts, classified safe deviations and focused migration/acceptance boundaries. This design preceded the construction-only #465 slice and does not authorize a 2.0/default or release migration.
- Record the pinned official-driver source declaration inventory and reviewed assertion subcases in a scenario ledger, keeping unknown/duplicate candidates and execution evidence separate; validate candidate links against ledger declarations. This accounting does not certify functional parity. (#437)
- Private FC9 request/condensed-column groundwork (#455) preceded atomic getter activation and owned row consumption in #456. Its wire fixtures alone were not live-getter or native-parity certification.
- Add a source-referenced official-driver public API inventory and compatibility guide; catalog consistency checks do not certify functional parity. (#436)
- Added a README "First contribution" guide (with Korean translation) pointing newcomers to the right sibling repo for their first PR, and documented the `good first issue` → `status: in progress` label lifecycle in AGENTS.md.
- Acknowledge CUBRID/cubrid-python's reference test scenarios in the README, NOTICE and third-party provenance notes, with source links and explicit licensing-verification limits.
- Clarify contributor and maintainer review/label/translation responsibilities, validate populated standalone docs exceptions with executable event-JSON checks, and pin the two verified shared workflow callers. CI code/security/release gates and security support policy are unchanged.

### Fixed
- Recover when the CAS closes the socket after a transaction boundary, and
  release open query handles at END_TRAN (#485). Since #468 the session survives
  commit/rollback, but the CAS may still close the socket right after an OUT_TRAN
  reply (CAS memory restart at `APPL_SERVER_MAX_SIZE`, `cubrid broker reset`,
  CHANGE CLIENT with more clients than CAS processes), and the next request then
  failed with `OperationalError: connection lost during receive`. Sync and async
  connections now send one `CHECK_CAS` before a request that follows an OUT_TRAN
  reply, like JDBC `checkReconnect`. A live CAS keeps the same session, so
  session variables and isolation level still survive normal boundaries. Only a
  failed probe replaces the session, once per request and before that request is
  first sent: the escape mode is re-probed unless pinned and explicit autocommit is
  restored, and the replacement is verified once more before the request. Requests
  tied to the lost session are not sent to the new one (a CLOSE_REQ is skipped;
  FETCH, last-insert-id, LOB read/write and native prepared requests fail; a
  `lastrowid` lost after an autocommit INSERT is `None` and logged at WARNING),
  and cursors probe before rendering parameters, so SQL rendered for a session
  that is then replaced is rejected before send with the retryable
  `OperationalError`, never sent to the new session (sync and async). Async cursor
  FETCH/CLOSE_REQ requests whose handle another task's boundary released while
  they waited are no longer sent. No SQL is replayed. SQL-level session state
  of the lost CAS is not carried over, so layers that set isolation or session variables with SQL must
  keep re-applying them on a new session (sqlalchemy-cubrid#527). A failed
  replacement raises `OperationalError` and leaves the connection disconnected
  for `ping(reconnect=True)`. Commit and rollback first send `CLOSE_REQ` for
  handles still held by unclosed cursors, so server handles no longer accumulate
  until the CAS exceeds its memory limit; in autocommit mode there is no such
  boundary, so close cursors.
  Already received rows stay readable; unfinished results still raise
  `InterfaceError` (#395). Requests after an OUT_TRAN reply cost one extra round
  trip, including each statement in autocommit mode. This supersedes the #468
  entry's statement that only an explicit `ping(reconnect=True)` may recover:
  normal boundaries keep the session, and only a CAS that fails the probe
  triggers the automatic reconnect.
- Fence future prepared FC3/FC6 requests to their owning physical CAS
  generation inside the synchronous transport boundary (#478). A stale
  handle is rejected before send even when a replacement server reuses its
  number; uncertain post-send failure retires the session without replay.
  This is internal groundwork for #439, not a public prepared API or a
  change to the declared `threadsafety=1` contract.
- Re-probe automatically detected `no_backslash_escapes` on each new physical
  session, including explicit ping recovery (#471). Explicit `True`/`False`
  remains pinned; healthy same-session ping does not probe. Probe failure
  retires the replacement and returns `False` from ping, while direct connect
  raises. Async parameterized SQL bound against an older session generation is
  rejected before send, not silently rebound or replayed. PATCH correction;
  no dynamic `SET` or heterogeneous-failover guarantee. This supersedes the
  historical #264 note that recovery never re-probes.
- Treat `CAS_INFO[0]=0` as OUT_TRAN, not a released CAS session (#468). Normal
  commit, rollback, and autocommit requests keep the physical connection and
  session state; only an explicit `ping(reconnect=True)` may recover from a
  disconnected socket, a negative `CHECK_CAS` response, or a CHECK_CAS
  transport/protocol error. `ping(reconnect=False)` still reports failure without
  reconnecting. Other SQL is never replayed after an uncertain transport
  failure. Connection, API, architecture, and support documentation now
  describe this boundary consistently.
- Async schema FETCH now discards the session without sending CLOSE when
  `KeyboardInterrupt` or `SystemExit` interrupts a pending reply; the original
  interruption is preserved. Ordinary FETCH errors retain their cleanup behavior.
- Empty `bytes` LOB writes return `0` without a broker request after existing object, offset, connection and wire argument checks. BLOB/CLOB data and handles stay unchanged; nonempty ACK checks and existing bool/other-data paths are preserved. No new strict type policy or async LOB feature is introduced. (#394)
- Native syntax (`-493`), semantic (`-494`) and communication (`-671`) errors now carry their verified meanings: generic `ProgrammingError` / `42000` for parser errors, `OperationalError` / `08S01` for communication. Batch dispatch reuses known-code SQLSTATE lookup rather than discarding it in favor of a class default; unknown-code defaults are unchanged. Missing-table inference from `-493` alone is unsupported. (#391)
- Unfinished SELECT results invalidated by commit/rollback no longer silently look exhausted: sync and async fetch methods raise `InterfaceError` when another broker FETCH is required without a valid handle. Already received rows remain readable, fully buffered/exhausted results retain normal EOF, and reconnect-specific `OperationalError` stays distinct. No transparent replay or holdable-result guarantee is added. (#395)
- Native NOT NULL (`-631`) and invalid foreign-key (`-922`) errors now raise `IntegrityError` with SQLSTATE `23000` by code, independent of message language. Single-statement and batch paths preserve the native value in `code` and `errno`; the shared batch error helper no longer drops errno. Sync/async regressions verify constrained inserts and connection reuse after rollback. (#390)
- `cursor.description` now reports `null_ok=True` for nullable columns and `False` for NOT NULL/primary-key columns. The CAS byte is an `is_non_null` flag, previously interpreted backwards. Full per-type metadata and sync/async nullability regressions preserve existing size fields and collection codes. (#431, #398, #408)
- Collection column metadata retains CAS collection-kind flags instead of treating the element type as the column type. SET/MULTISET/SEQUENCE, including empty collections with a NULL element-type header, return their documented containers with `decode_collections=True`, or raw bytes when disabled, in both sync and async queries. Real-header regressions cover initial and subsequent fetches. (#403, #410)
- Development quality checks synchronize Ruff/Mypy hook revisions with the exact dev pins, reject installed-tool/configuration drift, and lint/format maintained scripts and demos through shared local/CI Make targets. The Mypy hook explicitly checks the package instead of running only stub installation. (#416)
- Full integration validation selects current pytest markers instead of filename globs. Normal, TLS, and nightly slow lanes cover the declared integration inventory, including concurrency stress; unknown skips and missing workflow paths fail the lane audit. TLS provisioning runs broker commands as the service owner. (#397)
- Integration CI now uses the shared CUBRID readiness probe with host/port connection fields and fails before running tests when all retries are exhausted. (#411)
- **`Connection.get_last_insert_id()` / `AsyncConnection.get_last_insert_id()` no longer return an ambiguous empty string after `commit()` (#381)** — cache the broker identity captured after INSERT so it survives commit/rollback and SELECT. Successful values remain strings; unavailable identities return `None`. A new INSERT attempt, nonempty batch, or physical connection change clears the cache; failed, empty, or malformed identity retrieval leaves it unavailable. The broker can report an earlier identity after a non-auto-increment INSERT, so an ID does not prove the current statement generated it or that a row exists after rollback. Migration: replace `value == ""` with `value is None` and check for `None` before `int(value)`; `cursor.lastrowid` remains `int | None`. This is a documented bug correction, not an annotation-only change.
- **Empty `executemany()` clears previous results (#376)** — sync and async cursors
  release the previous query handle and reset result state to `description=None`,
  `rowcount=0`, and `lastrowid=None`. No SQL is executed; immediate query-close
  failures propagate without discarding the handle. With deferred close (#488),
  an eligible previous handle is queued without a request and an empty call
  does not flush the queue; FC20 batches likewise do not carry queued IDs (#585).
- Failed batch execution no longer exposes stale cursor result state (#375): sync and async executemany_batch clear prior result metadata, row counts, and last-insert IDs before the batch request, including per-statement, transport, and response-parse failure paths. If closing the previous query handle fails, no batch is sent and the handle remains tracked.
- **`Cursor.arraysize` now rejects non-integer values in sync and async cursors (#370).** Floats, booleans, and other non-integers raise `ProgrammingError` without changing the previous value; positive integers remain valid.
- **Batch execution closes an existing query handle (#374)** — `executemany_batch()` now releases an active server-side query handle before sending a batch request, matching `execute()` and preventing the prior result-set handle from leaking. Sync and async cursors keep the same behavior.
- Format very large integer parameters as decimal strings without converting them to floats, avoiding `OverflowError`. Float NaN/infinity rejection and boolean formatting are unchanged. (#368)

## [1.7.1] - 2026-09-18

### Fixed
- **`Lob.read(n)` no longer silently truncates large reads (#362)** — the CUBRID broker caps each `LOB_READ` response at a fixed size (~81908 bytes), so a single request returned a short buffer for any LOB larger than that, with no error (silent partial-read data loss). `Lob.read` now loops, advancing the offset by the bytes the broker actually returned, until the full requested length is collected or the broker signals end-of-LOB. As a side benefit, `read(0)` now short-circuits with no server round-trip (previously it triggered a server-side transaction abort).
- **create-release.yml: dropped `--target` from `gh release create`** — with an already-pushed tag (the normal tag-push trigger) `--verify-tag` already guarantees the tag exists, and passing `target_commitish` for an existing tag makes the Releases API return `422 Validation Failed`, so the first tag-triggered run of this workflow always failed. Verified live by the v0.4.0 tag attempt in cubrid-mcp-server.

### Documentation
- **Demo GIF embedded in README** — auto-generated terminal demo showing pip install → connect → query → zero dependencies. Rendered from `demos/pycubrid-demo.json` via `demos/render_gif.py`.
- **한국어 문서 페이지 — 배치 3 완결 (#317)** — TROUBLESHOOTING(1,246줄) 번역으로 13페이지 전체 완성. #317의 배치 작업 종료.
- **한국어 문서 페이지 — 배치 3 (Project 축, #317)** — DEVELOPMENT(개발 가이드) 번역 추가. TROUBLESHOOTING만 남음.
- **한국어 문서 페이지 — 배치 3 (Reference/Ops 축 1차, #317)** — SUPPORT_MATRIX·ARCHITECTURE·PERFORMANCE 번역 추가. TROUBLESHOOTING·DEVELOPMENT는 후속.
- **한국어 문서 페이지 — 배치 2 완결 (#317)** — API 참조(최대 문서, 1,298줄) 번역 추가로 Usage 축 전체(4페이지) 완성.
- **한국어 문서 페이지 — 배치 2 (Usage 축 2차, #317)** — CAS 프로토콜 참조 번역 추가. Usage 축 마지막(API_REFERENCE)은 후속 배치.
- **한국어 문서 페이지 — 배치 2 (Usage 축 1차, #317)** — PARAMETER_BINDING·TYPES의 한국어 번역을 `docs/ko/`에 추가. Usage 축 나머지(PROTOCOL·API_REFERENCE)는 후속 배치.
- **한국어 문서 페이지 — 배치 1/3 (#317)** — Getting Started 축 4페이지(quickstart·CONNECTION·EXAMPLES·faq)의 한국어 번역을 `docs/ko/`에 추가하고 Project → Translations → 한국어 문서로 노출. 나머지 9페이지(Usage/Reference/Operations/Project 축)는 후속 배치. 페이지 번역은 경고 수준 동기화, README.ko 하드 게이트 유지.
- **Korean/multi-language docs governance** — every `docs/README.<lang>.md` translation now carries a sync marker, and docs-sync gained a `translation-sync` job that fails a PR when `README.md` changes without any translation changing (escape hatch: the `translations-deferred` label).
- **Docs site information architecture unified across the ecosystem** — nav reorganized to the shared six-tab skeleton (Home / Getting Started / Usage / Reference / Operations / Project), the five README translations (ko/de/hi/ru/zh) are now reachable via Project → Translations (previously URL-only), palette unified to blue with search-suggest, and the homepage gains an Ecosystem section linking the three sibling sites.
- **CUBRID server license relationship documented; copyright notice unified (#309)** — `docs/ARCHITECTURE.md` gains a section stating the verified upstream licensing (server engine Apache-2.0, APIs/connectors BSD per CUBRID's `COPYING`; the often-cited GPL v2+ no longer applies) and that pycubrid is an independent wire-protocol client with no server code included or linked. `THIRD_PARTY_LICENSES.md` carries the same one-paragraph statement. LICENSE/NOTICE copyright lines now read `Yeongseon Choe, Gyeongjun Paik` (2025-2026), reflecting the two primary authors.

- **Added `THIRD_PARTY_LICENSES.md`** — pip-licenses-generated inventory of the development toolchain's licenses. pycubrid itself has zero runtime dependencies, so nothing in the table ships in the wheel. Documentation only.

## [1.7.0] - 2026-09-02

### Documentation
- **Added an Acknowledgments section and a `NOTICE` file crediting the CUBRID Node.js driver ([node-cubrid](https://github.com/CUBRID/node-cubrid), © 2008–2012 Search Solution Corporation, BSD-3-Clause)** — during pycubrid's initial development, node-cubrid was consulted as a reference implementation to understand CUBRID's CAS wire protocol (packet structure and function codes). This is recorded as an acknowledgment in `README.md`, `docs/README.ko.md`, and the new `NOTICE` file. Documentation only; no code or runtime behavior change.

### Added
- **DB-API 2.0 exception classes are now exposed as attributes on `Connection`/`AsyncConnection` (#282)** — PEP 249's optional extension recommends that the standard exception classes (`Warning`, `Error`, `InterfaceError`, `DatabaseError`, `DataError`, `OperationalError`, `IntegrityError`, `InternalError`, `ProgrammingError`, `NotSupportedError`) be accessible as attributes of the `Connection` object, so multi-connection code can catch errors specific to a driver without importing its module. All ten classes are now set as class attributes on the shared `ConnectionCommonMixin`, so both sync `Connection` and `AsyncConnection` (and their instances) expose them with identity preserved — e.g. `conn.IntegrityError is pycubrid.IntegrityError`. Additive, backward-compatible surface change (`api-baseline.json` regenerated).
- **`Lob` now supports client-side lifecycle management — `close()`, context-manager (`with`), and a closed-state guard (#268)** — the CUBRID CAS wire protocol has **no** LOB release/free/close opcode (function codes are contiguous: `LOB_NEW=35`, `LOB_WRITE=36`, `LOB_READ=37`, `END_SESSION=38`), and the reference JDBC driver's `Blob.free()` is likewise purely client-side. LOB handles are therefore connection/session-scoped and reclaimed only when the owning connection/session closes. `Lob.close()` mirrors this: it is idempotent, performs **no** network I/O, and simply invalidates the Python object so subsequent `read()`/`write()` raise `InterfaceError("LOB is closed")`. `Lob` is now usable as a context manager (`__exit__` closes without suppressing exceptions). The `lob_handle`/`lob_type` properties remain readable after close for introspection. The class docstring documents the connection-scoped semantics.
- **`Lob` now rejects async connections instead of silently producing un-awaited coroutines (#266)** — `Lob` drives its connection *synchronously* (`_send_and_receive` with no `await`), but `AsyncConnection._send_and_receive` is a coroutine. A `Lob` bound to an `AsyncConnection` (via direct construction — `Lob` is exported) therefore called an async method without awaiting it, silently getting a coroutine object back and emitting "coroutine was never awaited" warnings. `Lob.__init__` and `Lob.create()` now detect an async connection (`inspect.iscoroutinefunction`) and raise `NotSupportedError` *before* any packet is sent; async LOB support is not implemented. `AsyncConnection.create_lob()` (previously absent, raising `AttributeError`) now exists and raises the same `NotSupportedError` for a clear, discoverable failure.
### Fixed
- **`TIMESTAMPTZ`/`TIMESTAMPLTZ` reads no longer crash with "malformed response from broker" (#289)** — `PacketReader._parse_timestamptz` decoded the timezone-aware timestamp types using the 7-short / 14-byte `DATETIMETZ` layout (which carries a millisecond field). But `TIMESTAMPTZ`/`TIMESTAMPLTZ` are **second-precision**: their wire layout is 6 shorts (12 bytes, no millisecond field) followed by the timezone string. The parser therefore over-read the first 2 bytes of the timezone string as a `ms` value and computed `ms * 1000`, which overflowed `datetime`'s "microsecond must be in 0..999999" range whenever those bytes were non-trivial (e.g. a leading `"As"` from `Asia/Seoul` = 16755 → 16755000 µs). The resulting `ValueError` surfaced as `OperationalError("malformed response from broker")`, making any `SELECT` of a `TIMESTAMPTZ`/`TIMESTAMPLTZ` column unreadable. `_parse_timestamptz` now reads the correct 12-byte second-precision layout (microsecond fixed to 0), and `DATETIMETZ`/`DATETIMELTZ` get their own `_parse_datetimetz` reader that keeps the 14-byte millisecond layout; a shared `_attach_timezone_suffix` helper parses the trailing timezone string for both. See [docs/TYPES.md](docs/TYPES.md).
- **`CUBRIDIsolationLevel.DEFAULT` corrected from `0x01` to `0x04` (READ COMMITTED) (#285)** — the enum advertised `DEFAULT = 0x01`, which aliases the lowest legacy pre-MVCC level (`COMMIT_CLASS_UNCOMMIT_INSTANCE`), not CUBRID's actual server default. CUBRID's default transaction isolation is **READ COMMITTED**, i.e. `REP_CLASS_COMMIT_INSTANCE = 0x04` (verified on live CUBRID 11.2: `GET TRANSACTION ISOLATION LEVEL` returns `4` on a fresh connection). `DEFAULT` now aliases `0x04`. The accompanying test that asserted `DEFAULT == 0x01` was fossilizing the wrong value and is corrected. A class-docstring note also records that CUBRID's MVCC engine (10.0+) only accepts levels `0x04`/`0x05`/`0x06`; the lower codes remain for protocol/back-compat reference only. This is a constant used for reference; the driver's connection logic does not consume it, so no runtime path changes.
- **Backslash-escape mode is now auto-negotiated from the server, fixing silent string corruption (#255)** — CUBRID's `no_backslash_escapes` system parameter defaults to `yes` (a backslash is an ordinary literal character), but pycubrid defaulted its client-side flag to `False` and unconditionally doubled backslashes. Against a stock server this silently corrupted data: `C:\temp\file` (12 chars) was stored as `C:\\temp\\file` (14 chars), and `regex \d+` became `regex \\d+`. `Connection`/`AsyncConnection` now probe the live server once at connect time with `SELECT CHAR_LENGTH('\\')` when `no_backslash_escapes` is not passed explicitly: a result of `2` selects literal mode (`True`, no doubling), `1` selects escape-processing mode (`False`), and any other value or a probe error raises `OperationalError` (see below). Passing `no_backslash_escapes=True|False` explicitly skips the probe. LIKE metacharacters (`%`, `_`) were never escaped and remain untouched. See [docs/PARAMETER_BINDING.md](docs/PARAMETER_BINDING.md#escape-mode-negotiation).
- **Async connection setup now matches the sync ordering and closes a negotiation race (#264)** — `AsyncConnection.connect()` applied the constructor's autocommit *before* negotiating the backslash-escape mode, and ran that negotiation probe **outside** the connection lock. This diverged from the sync driver (which negotiates *before* autocommit) and left a race window: a query issued on another task could execute before the escape mode was pinned, silently using the wrong escaping. Setup now runs the sync order exactly — connect → negotiate escapes → apply autocommit — behind a one-time setup gate that fences other tasks' public operations until the escape mode is pinned, while the setup task's own probe bypasses the gate (no deadlock) and a setup failure propagates to any waiting task. Transparent reconnect is unaffected and never re-probes.
- **Backslash-escape negotiation probe no longer leaves an open transaction on new connections (#292)** — `Connection`/`AsyncConnection` setup runs the `SELECT CHAR_LENGTH('\\')` probe in the default manual-commit mode, which opens a driver-owned transaction *before* the constructor's `autocommit` setting is applied. The probe never rolled that transaction back, so a freshly opened connection — including one created with `autocommit=True` — was handed to the caller with a lingering, driver-started transaction. Downstream this could cause CUBRID to reject a follow-up operation that requires a clean transaction boundary (e.g. SQLAlchemy setting `isolation_level` on a new pooled connection). Both the sync and async `_negotiate_backslash_escapes()` now roll back the probe transaction on *every* exit path — success, an unexpected probe result, or a probe error — via a best-effort `rollback()` in a `finally` block, so a failed negotiation cannot leave the driver-started transaction open either (passing `no_backslash_escapes` explicitly still skips the probe entirely and opens no transaction). In the async path the rollback is safe during setup because the setup-owning task bypasses `_wait_for_setup_if_needed()`. No public API change.
- **Backslash-escape negotiation no longer swallows a probe-transaction rollback failure (#300)** — the `finally` block that rolls back the `SELECT CHAR_LENGTH('\\')` probe transaction caught and discarded any rollback error (`except Exception: pass`). If the server-side rollback failed without closing the socket, the probe transaction could remain open and the connection was still handed back to the caller (or pool) in an unknown transaction state. Both sync and async `_negotiate_backslash_escapes()` now, on a rollback failure, close the connection so it can never be reused and raise `OperationalError` — unless a probe error is already propagating, in which case that original error is preserved (the connection is still closed). The success path is unchanged when rollback succeeds. No public API change.
- **Placeholder tokenizer is now CUBRID-dialect-aware (#265)** — `split_on_placeholders()` (which `bind_parameters()` uses to locate `?` placeholders) only understood ANSI single-quoted strings (`''` doubling), double-quoted identifiers, `--` line comments, and `/* */` block comments. A `?` inside CUBRID-specific lexical contexts was mis-detected as a real placeholder, throwing `wrong number of parameters` or interpolating into the wrong position. The tokenizer now also honours: backslash escapes inside single-quoted strings when the connection's `no_backslash_escapes` is `False` (so `\'` does not terminate the literal and `\\` is a literal backslash — `''` doubling stays valid in both modes; the standalone default `no_backslash_escapes=True` preserves the previous behaviour and matches CUBRID's server default), `//` C++-style line comments, and backtick (`` `id` ``) and bracket (`[id]`) quoted identifiers (first closing delimiter terminates). `bind_parameters()` now threads the connection's `no_backslash_escapes` through to the tokenizer for consistency with `escape_string()`. **Known limitation:** double quotes are treated as identifier delimiters, correct for CUBRID's default `ansi_quotes=yes`; SQL relying on `ansi_quotes=no` (where `"` delimits strings) is not specially handled, as pycubrid does not track that parameter.
- **Closed a `bool | None` type hole around the negotiated escape mode (#267)** — the cursor parameter-binding mixin declared `_connection: Any`, which masked from mypy that `Connection._no_backslash_escapes` is `bool | None` (unset until negotiated) while `bind_parameters()`/`format_parameter()` require a concrete `bool`. `CursorParamsMixin` is now `Generic` over the concrete connection type (bound to a minimal `_EscapeModeSource` protocol), `Cursor`/`AsyncCursor` specialise it (`CursorParamsMixin[Connection]` / `[AsyncConnection]`), and a new `_resolve_escape_mode()` narrows the value — raising `InterfaceError` if the mode is read before negotiation instead of silently mis-escaping. `AsyncCursor.__init__` is also tightened from `connection: Any` to `AsyncConnection`. No public behaviour change; mypy now catches misuse.
- **`Lob.read()` now rejects overlong server responses and invalid arguments (#268)** — `read()` returned `packet.lob_data` without checking it against the requested `length`, so a misbehaving server or corrupted transport could hand back more bytes than asked for. `read()` now raises `OperationalError` when the returned length exceeds the requested length, and both `read()`/`write()` validate that `offset` (and, for `read`, `length`) are non-negative, raising `InterfaceError` before any packet is sent.
- **`format_parameter` now rejects the Ctrl-Z (`0x1A`) byte in string parameters instead of emitting a raw control byte (#271)** — `escape_string()` previously prefixed `0x1A` with a backslash while leaving the raw control byte in the literal, even though CUBRID's SQL grammar defines **no** safe literal escape for it (there is no MySQL-style `\Z`, and the escape had no effect under CUBRID's default `no_backslash_escapes=yes`). Embedding `0x1A` is now rejected with `ProgrammingError("string parameter contains Ctrl-Z (0x1A) byte")` in **both** escape modes, consistent with the existing null-byte rejection. **Behavior change:** strings containing `0x1A` that previously produced a (malformed) literal now raise at bind time. The issue's two other claims were investigated and **rejected as invalid** against the CUBRID manual: `TIME` has second resolution (its literal grammar allows only `'HH:MI:SS'`), so dropping microseconds is correct; and CUBRID accepts scientific/exponential numeric literals (an `E`-form number is parsed as `DOUBLE`), so `str()`'s exponent output (e.g. `1e+20`) is a valid literal. Both are now pinned by tests.
- **`_extract_first_keyword()` now strips CUBRID C++-style `//` line comments (#256)** — the leading-comment stripper used by `executemany()` to detect the statement's DML verb only skipped `/* */` block comments and `--` ANSI line comments, so a statement prefixed with a `//` line comment (a valid CUBRID comment style) had its verb mis-detected and could be excluded from batch execution. `_RE_LEADING_COMMENTS` now also skips `//` line comments to EOL/EOF, keeping it consistent with the comment styles already honoured by `split_on_placeholders()` (#265).

### Changed
- **Public escape helpers now share a single `no_backslash_escapes=True` default (#293)** — `escape_string()`, `format_parameter()`, `bind_parameters()`, and `CursorParamsMixin._escape_string()` defaulted the keyword to `False` while `split_on_placeholders()` already defaulted to `True`. A caller that reached for one helper but omitted the keyword could silently get the opposite (non-server-consistent) escaping behavior from another. All five helpers now default to `True`, matching CUBRID's server default (`no_backslash_escapes=yes`, a backslash is an ordinary literal character). Internal driver paths always pass the connection's negotiated value explicitly, so this is a no-op for driver-mediated binding; it only affects code that calls these helpers directly without the keyword, which now gets the correct literal-backslash behavior. A regression test pins the shared default. See [docs/PARAMETER_BINDING.md](docs/PARAMETER_BINDING.md).
- **Ruff lint rule selection now declared explicitly (#247)** — `pyproject.toml` configured ruff but never set `[tool.ruff.lint] select`, so `ruff check` inherited ruff's implicit defaults. Ruff expanded that default set in 0.16 (59 → 413 rules against this repo's config), which is why #245 (`0.15.22 → 0.16.1`) failed lint with 237 errors in untouched code. Pinning the ruff *version* in #236 stopped unpinned installs from drifting, but could not survive the bump itself — the rule set is now pinned too, via `select = ["E4", "E7", "E9", "F"]`, which is exactly what ruff selected by default through 0.15.x (same 59 rules under both versions).
- **Backslash-escape negotiation now fails loud instead of silently defaulting to `False` (#263)** — when the `SELECT CHAR_LENGTH('\\')` probe raises or returns an unexpected length (neither `1` nor `2`), `Connection`/`AsyncConnection` previously set `no_backslash_escapes=False` (the legacy value) and logged a warning. Because the CUBRID default maps to `True`, that silent fallback could pick the *wrong* escaping mode and corrupt string literals (or enable SQL injection). Negotiation failure now raises `OperationalError`; pass `no_backslash_escapes` explicitly to skip detection when the probe cannot run. **Behavior change:** connections that previously succeeded with a mis-detected mode now raise at connect time.
- **Corrected the `Documentation` project URL and added a `Changelog` URL in packaging metadata (#269)** — `[project.urls].Documentation` pointed at the GitHub source tree (`.../tree/main/docs`) rather than the published docs site; it now points to `https://cubrid-lab.github.io/pycubrid/` (matching the README badge and `mkdocs.yml`). A `Changelog` URL (`.../blob/main/CHANGELOG.md`) was also added, mirroring sqlalchemy-cubrid. Packaging metadata only; no code or runtime behavior change.
- **Renamed the example table `cookbook_users` to `users` consistently across all documentation surfaces (#270)** — the placeholder name appeared in 81 places across `README.md`, all five translated READMEs (`docs/README.{ko,zh,hi,de,ru}.md`), `docs/CONNECTION.md`, `docs/EXAMPLES.md`, `docs/TYPES.md`, and the generated `docs/llms-full.txt`. Documentation only; no code or runtime behavior change.
- **Binding a Python collection as a single parameter now raises an actionable error message (#287)** — passing a `list`, `tuple`, `set`, `frozenset`, or `dict` as one bound value previously raised the generic `ProgrammingError("unsupported parameter type")`. It now raises `ProgrammingError` with a message that names the collection case and points at the fix: pycubrid does **not** auto-expand `IN (?, ?, ...)`, so placeholders must be expanded explicitly in the SQL. The exception class is unchanged (still `ProgrammingError`) and the message text remains non-contractual; other unsupported types keep the generic message. See [docs/PARAMETER_BINDING.md](docs/PARAMETER_BINDING.md#type-mapping-guarantees).

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
- **Cross-platform CI**: offline tests now run on **Ubuntu + Windows + macOS** (Python 3.10–3.14 matrix, 15 OS×Python combinations). pycubrid is pure Python so the code was already cross-platform — this proves it.
- **Per-PR `integration-tls` lane added** — `ci.yml` now includes an `integration-tls` job (Python 3.14 × CUBRID 11.4) that runs on every PR touching `pycubrid/aio/**`, `pycubrid/connection.py`, `pycubrid/_connection_common.py`, or `.github/workflows/**`. Mirrors the broker-provisioning logic from `integration-full.yml` so TLS regressions are caught BEFORE merge instead of waiting for the nightly full matrix. Path-gating is implemented via `dorny/paths-filter@v3.0.2` (SHA-pinned), and the `ci-gate` job treats `integration-tls.result == 'skipped'` as acceptable when no TLS-relevant files changed (closes #159).
- **TLS readiness probe now verifies the broker certificate** — both probe scripts in `integration-full.yml::integration-tls` previously called `ctx.check_hostname = False; ctx.verify_mode = ssl.CERT_NONE`, which silently accepted any TLS endpoint on `localhost:33000` and weakened the regression signal. The probes now use the verified default context built from `CUBRID_TLS_TEST_CA_FILE` directly; a misconfigured or untrusted broker cert fails the probe loudly (closes #157).
- **TLS skip allow-list tightened to full pytest node id** — the `grep -v 'test_aio_ssl_handshake_failure'` filter in `integration-full.yml` is replaced with the full nodeid `tests/test_aio_ssl_integration.py::test_aio_ssl_handshake_failure`. A future rename of the test no longer silently re-introduces the "unexpected skip" bug (closes #159).
- **Automated `SSL=ON` CUBRID broker provisioning** — `integration-full.yml` now includes an `integration-tls` job (Python {3.10, 3.14} × CUBRID 11.4) that starts a manually-managed CUBRID container, flips `BROKER1 SSL=OFF` → `SSL=ON`, extracts the broker's self-signed certificate, probes the TLS handshake, and runs `tests/test_aio_ssl_integration.py` with `CUBRID_TLS_TEST_*` env vars wired up. The job fails loudly if any TLS test is skipped, ensuring the live TLS path is exercised on every nightly/tag-push run instead of silently skipping (closes #147, #155)

### Fixed
- **Async TLS verification failures now surface promptly on Python 3.10** — `AsyncConnection._do_connect_handshake` now runs a narrow Python-3.10-only **preflight TLS verification probe** in the default executor immediately before `loop.start_tls()`. The probe opens a separate TCP socket to the same effective endpoint (replaying the `CUBRS` handshake on the no-redirect path, going straight to TLS on the redirect path), then performs a synchronous `ssl.SSLContext.wrap_socket()` with the **same** `SSLContext` and `server_hostname=self._host` as the real upgrade. Any `ssl.SSLError` propagates as `OperationalError`, matching the 3.11+ failure surface. Works around the known CPython 3.10 `asyncio` bug (gh-142352 family, fixed in 3.13/3.14) where `loop.start_tls()` hangs indefinitely on TLS-handshake-internal verification failures because `ssl_handshake_timeout` only bounds peer-unresponsive hangs. No-op on Python 3.11+; the 3.10 path incurs one extra TCP round-trip per connect (closes #156).
- **TLS handshake now matches CUBRID's STARTTLS-style upgrade** — both sync and async `connect()` previously wrapped the socket in TLS before any bytes were exchanged, which never worked against a real `SSL=ON` CUBRID broker. The driver now (1) opens a plaintext TCP socket, (2) sends the 10-byte ClientInfoExchange handshake using the SSL magic string `"CUBRS"` (vs `"CUBRK"` for plain), (3) reads the 4-byte broker status (negative codes now raise `OperationalError` instead of silently falling through), (4) reconnects to the redirected CAS worker on `new_connection_port > 0` without re-handshaking (matches upstream JDBC `BrokerHandler.connectBroker`), and (5) upgrades the connection to TLS before sending `OPEN_DATABASE`. The async path uses `loop.start_tls()` for Python 3.10 compatibility. Validated end-to-end against CUBRID 11.4 with `SSL=ON` and a self-signed broker certificate (#154)

- **PEP 3134 ``__cause__`` preserved on async transport timeouts** —
  ``AsyncConnection._connect_locked`` (handshake timeout) and
  ``AsyncConnection._send_and_receive_locked`` (read timeout) now use
  ``raise OperationalError(...) from exc`` instead of ``from None``, so the
  underlying ``asyncio.TimeoutError`` is preserved on the chained exception
  for diagnostic tooling (PR #3 Item 3).

### Documentation
- **Parameter binding contract documented** — added `docs/PARAMETER_BINDING.md` formalizing the driver-side literal-binding semantics for 1.x: per-type SQL-literal mapping (with `_cursor_common.py` line citations and pinned tests), the `escape_string` default and `no_backslash_escapes` modes (NUL rejection, single-quote doubling, backslash and CR/LF/`\x1a` handling), the placeholder tokenizer's behavior across quoted strings/identifiers/line and block comments, and the explicit non-guarantees (no server-side prepared statements, identifiers are not escaped, no `IN`-clause expansion, exception-message text is not contract). Linked from `README.md` and `docs/index.md`.
- **TLS/handshake documentation aligned with implementation** — corrected `AGENTS.md` protocol version (8/10.2) and OpenDatabase payload framing, fixed OpenDatabase response field order across `CONNECTION.md`/`ARCHITECTURE.md`, rewrote the CAS reconnection diagram to reflect actual `connect()` re-entry through the broker, corrected the `MAGIC_STRING_SSL` constant name in `PROTOCOL.md`, surfaced TLS 1.2 minimum on sync rows in `SUPPORT_MATRIX.md`, added a new "Async TLS Handshake Hangs on Python 3.10" troubleshooting section, and added the Python 3.10 async TLS caveat to `README.md` and all five translations (#160, #163)
- **TLS docs polish** — added TLS examples to `EXAMPLES.md`, an SSL/TLS TOC entry to `CONNECTION.md`, a Transport Security section to `SECURITY.md`, local TLS integration-test instructions to `CONTRIBUTING.md`, expanded `Connection.connect`/`AsyncConnection`/`_do_connect_handshake` docstrings with the STARTTLS flow, added a TLS field to the bug-report issue template, expanded `pyproject.toml` keywords, and added a `make integration-tls` target. Resolves remaining items from the TLS Phase 4 audit (#161, #162)
- **Async TLS handshake hang on Python 3.10 documented as a known limitation** — a known CPython asyncio TLS handshake bug on Python 3.10 causes `loop.start_tls()` to hang on cert-verify failures on 3.10 only (fixed in 3.13/3.14); pycubrid documents the workaround and skips the negative-path test on 3.10 (#156)

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
- **Async TLS upgrade and handshake paths covered offline** — added `tests/test_aio_ssl_offline.py` with 7 mocked tests guarding the regressions enumerated in #158: `_upgrade_to_tls()` argument forwarding (incl. `ssl_context`, `server_hostname=self._host`, `ssl_handshake_timeout` with the documented 10-second default), `loop.start_tls()` failure cleanup (`old_transport.abort()` exactly once, exception re-raised unchanged, defensive `None`-return handling), incomplete-read and EOF on the initial 4-byte `CUBRS`/`CUBRK` broker status response, and async parity for `test_connect_redirect_sends_no_second_handshake` (redirect during TLS connect reconnects on the new port **without** a second handshake). These run without a broker so a regression silently disabling hostname verification or re-introducing a double-handshake no longer slips through offline CI (closes #158).
- **Sync/async lifecycle parity coverage expanded** — integration parity tests now share adapter-driven scenarios and cover connection lifecycle APIs including `ping()`, CAS-inactive reconnect, auto-commit transitions, insert identity helpers, batch rowcount semantics, close ordering, and the explicit `AsyncConnection` `create_lob` `AttributeError` contract (closes #140)
- **Async TLS integration coverage** — added `tests/test_aio_ssl_integration.py` with
  live async TLS success/failure/reconnect/shutdown coverage gated behind an
  explicitly configured TLS-enabled broker (#155)

- **12 new tests in ``tests/test_network_edge_cases.py``** — four new test
  classes covering: ``__cause__`` chaining on sync/async transport timeouts,
  explicit/unset session-state restore on reconnect (sync + async),
  restore-failure tear-down, mid-fetch ``OperationalError`` (sync + async),
  ``execute``/``close`` resetting the invalidation flag, and
  ``CancelledError`` propagation in ``AsyncConnection._close_streams``. Total
  offline tests: 858.

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
