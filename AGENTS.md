# AGENTS.md

Project knowledge base for AI coding agents.

## Project Overview

**pycubrid** is a Pure Python DB-API 2.0 (PEP 249) driver for the CUBRID relational database.
It communicates with CUBRID via the CAS wire protocol over TCP/IP, requiring no C extensions
or native CCI library.

- **Language**: Python; minimum version is `requires-python` in [`pyproject.toml`](pyproject.toml).
  Supported Python and CUBRID versions are listed in the
  [support matrix](docs/SUPPORT_MATRIX.md); do not copy them here.
- **APIs**: sync `pycubrid.connect()`, asyncio `pycubrid.aio.connect()`, and the opt-in
  `pycubrid.compat` namespaces (`compat.cubriddb`, `compat.native`) for code written
  against the official CUBRIDdb driver
- **Protocol**: CUBRID CAS binary protocol (version 8, since CUBRID 10.2+)
- **License**: MIT
- **Version source**: `pycubrid.__version__` in [`pycubrid/__init__.py`](pycubrid/__init__.py)

## Task Routing

Every section of this file applies to every change. Task-specific detail lives in the
guides under `docs/agents/`, listed in the table below. Agent tools load this file but
not the documents it links, so open the guide for your task before editing. The guides are
English-only contributor guidance: `scripts/check_docs_translation.py` checks only the pages
directly under `docs/`, and `scripts/check_agents_docs.py` checks their links and commands.

| Task | Read before editing | Run before review |
|---|---|---|
| CAS protocol: packets, reply parsing, binds, LOB handles | [Wire protocol guide](docs/agents/protocol.md), [docs/PROTOCOL.md](docs/PROTOCOL.md), [Architecture guide](docs/agents/architecture.md) | `make test`, `make check`, `make integration` |
| Connection, reconnect, transactions, async | [Wire protocol guide](docs/agents/protocol.md#packet-format) (CAS_INFO, reconnect, handle release), [docs/CONNECTION.md](docs/CONNECTION.md), [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) | `make test`, `make check`, `make integration` |
| Public API, types, parameter binding, `compat` | [Architecture guide](docs/agents/architecture.md), [docs/API_REFERENCE.md](docs/API_REFERENCE.md), [docs/TYPES.md](docs/TYPES.md), [docs/PARAMETER_BINDING.md](docs/PARAMETER_BINDING.md), [RELEASE_POLICY.md](RELEASE_POLICY.md) | `make test`, `make check` |
| Tests and CI workflows | [Testing and CI guide](docs/agents/testing-ci.md), [docs/CI_POLICY.md](docs/CI_POLICY.md), [docs/DEVELOPMENT.md](docs/DEVELOPMENT.md) | `make test`, `make check` |
| Documentation and contributor guidance | [Documentation guide](docs/agents/documentation.md), [Documentation definition of done](#documentation-definition-of-done), [CONTRIBUTING.md](CONTRIBUTING.md) | `python scripts/generate_llms_full.py`, `mkdocs build --strict`, `python scripts/check_docs_links.py`, `python scripts/check_agents_docs.py` |
| Release | [Release Process](#release-process), [GitHub Release Policy](#github-release-policy), [RELEASING.md](RELEASING.md) | `make release-check VERSION=x.y.z` (read-only; never release by hand) |
| Agent task evaluations (`agent-evals/`) | [Agent evaluation guide](agent-evals/README.md) | `make agent-eval-check` |
| Performance and roadmap | [Performance guide](docs/agents/performance.md), [docs/PERFORMANCE.md](docs/PERFORMANCE.md) | the measurement steps in docs/PERFORMANCE.md |

## Development

### Setup

```bash
git clone https://github.com/cubrid-lab/pycubrid.git
cd pycubrid
make install          # pip install -e . --group dev (pip 25.1+)
```

### Key Commands

```bash
make doctor           # Required tools present and working (fails otherwise)
make check            # Static gate: lint, format-check, typecheck, security, docs-reason-check
make test             # Offline tests with 95% coverage threshold
make verify           # check + test; no Docker
make lint             # ruff check only; make format-check for ruff format --check
make format           # Auto-fix lint/format (the only mutating quality target)
make integration      # scripts/run_integration.py: Docker → readiness → tests → skip audit → cleanup
make help             # Every target, grouped by purpose (check-all is a deprecated alias of check)
```

### Test Commands (manual)

```bash
# Offline (no DB needed)
pytest tests/ -v -m "not integration" \
  --cov=pycubrid --cov-report=term-missing --cov-fail-under=95

# Integration (requires Docker; waits for readiness, always cleans up)
make integration CUBRID_TEST_PORT=33522   # any free port; default 33000
# Existing server: CUBRID_TEST_HOST/CUBRID_TEST_PORT (win over CUBRID_TEST_URL).
# Configured but unreachable -> tests error; unconfigured -> tests skip.
```

### Test Stats

- Use `make test` for the current marker-selected offline regression suite.
- [Measured coverage](https://codecov.io/gh/cubrid-lab/pycubrid) is reported per run;
  the enforced floor is 95% on full coverage runs, not routine PR smoke.
- Selection and full versus representative validation: [CI execution policy](docs/CI_POLICY.md).

## Code Conventions

### Style

- **Linter/Formatter**: Ruff
- **Line length**: 100 characters
- **Target Python**: the `requires-python` minimum (Ruff `target-version` matches it)
- **Imports**: `from __future__ import annotations` in every module
- **Type hints**: Full typing; PEP 561 compliant (`py.typed`)
- **super()**: Always `super().__init__()`, never `super(ClassName, self)`

### Anti-Patterns (Never Do)

- No type suppression (`as any`, `@ts-ignore`, etc.)
- No f-string interpolation in SQL queries
- No `super(ClassName, self)` — use `super()` only
- No Python 2 constructs
- No empty `except` blocks

## Development Workflow (cubrid-lab org standard)

Every contribution follows the **same review criteria**, whether implemented
by a person, OpenCode, Codex, Claude Code, ChatGPT, Copilot, or another tool.
No particular AI reviewer or pre/post-review sequence is a prerequisite.
See [CONTRIBUTING.md](CONTRIBUTING.md) for this repository's validation commands.

1. **Scope and implement:** keep the PR focused, add relevant tests, update
   affected documentation, and identify unrun checks and remaining risks.
2. **GitHub CI:** use required GitHub Actions checks as the authoritative
   validation of the final PR head, including lint, unit, integration/live,
   security, docs and release checks wherever applicable. Local checks are
   encouraged but are not an additional mandatory merge gate.
3. **Code review:** examine functionality, tests, security, regression risk,
   and maintainability. Oracle, Copilot and `agent-review` are optional,
   **advisory** inputs—not approval or alternate gates based on coding tool.
4. **Final decision:** address or disposition findings, verify the final
   source and required CI, and leave merging to the responsible maintainer.
   Preserve independent approval for high-risk security, CI permissions
   and release/publishing paths under existing project safeguards.

Do not weaken branch protections, live CUBRID checks, protected code paths,
security constraints or release/publishing controls through a documentation
simplification.

## Agent PR scope and review guardrails

These guardrails apply regardless of the implementation tool:
- Define the acceptance criteria, affected files, non-goals and validation
  before substantial implementation; keep changes independently reviewable.
- Evaluate review findings against reproduced behavior, risk and impact.
  AI-generated severity labels alone never justify scope expansion.
- Fix verified issues and run relevant checks on the final code state.
  AI review rounds are optional; avoid duplicate review requests and bot spam.
- Maintain contributor credit and preserve existing PR discussion history.
  For external contributions, request a new review after the author updates
  the relevant head.
- Do not merge unresolved critical/security defects or failed required CI.

## Review disposition and merge evidence

- Record the tested head, required GitHub CI results, checks not run,
  remaining risks, reviewer findings and maintainer merge decision.
- Resolve each actionable finding by a focused fix plus verification,
  a reasoned rejection, or an explicitly tracked follow-up. Maintain
  separate handling for confidential security findings.
- Add one final reply per relevant inline thread. Reviewers/maintainers
  confirm the updated code before marking threads resolved; a reply,
  outdated diff or bot verdict is not proof of resolution.
- Keep one concise, editable status comment when automated agents assist.
  Treat Oracle, Copilot and `agent-review` output as optional context only.
- The responsible maintainer checks the final relevant head, required CI
  and any independent high-risk approval before merging.

## Release Process

Version is single-sourced from `pycubrid/__init__.py` → `__version__ = "x.y.z"`
(`pyproject.toml` reads it dynamically). Merging a reviewed release PR is the only
normal way to release: `release-please.yml` opens it (curated + Conventional Commit dated CHANGELOG section +
version bump, checked by `make release-check VERSION=x.y.z`), and after the
squash-merge `publish-pypi.yml` detects the version change and runs consistency → full
matrix → build → candidate verification (the cookbook smoke test in candidate mode on
the built wheel; `publish` needs it) → tag/Release/PyPI → cookbook verification (the
cookbook smoke test called as a pinned reusable workflow, no token) → summary on its own.
Freeze a release candidate with `autorelease: review` before branch-only note edits; wait for in-flight preparation to finish. Unfrozen notes come from main Unreleased. GITHUB_TOKEN updates require maintainer-triggered CI at the final head; before merging a release PR, complete the release review checklist in `RELEASING.md` (candidate head, per-check CI state, impact, release purpose). Pending labels are reconciled only after successful publication and cookbook verification.
Ordinary PRs never change `__version__` or date a CHANGELOG section. Never push
tags or publish by hand; the only manual entry point is the narrow recovery
dispatch of `publish-pypi.yml`. A `last-release-sha` override is temporary:
Prepare release stops once a later release containing it is verified, and a
normal PR must remove it (never edit it away inside a release PR). Procedure, failure matrix and recovery:
[`RELEASING.md`](RELEASING.md).

## GitHub Release Policy

These rules add to the release procedure above; the procedures and gates in
`RELEASING.md` stay authoritative.

### Titles

- A Release title equals its tag exactly. Stable tags and titles are `vMAJOR.MINOR.PATCH`.
- No package name, feature, date or suffix in a title (not `pycubrid 1.2.3`,
  `v1.2.3 — Foo`, `v1.2.3 (corrected)` or `Release v1.2.3`). Drafts follow the same rule.
- Never move, delete or recreate a tag to fix a title. Never delete and recreate a
  published Release.
- Metadata edits keep the notes, assets, published state and prerelease state. When
  editing a draft through the API, always resend `tag_name`: a PATCH without it resets
  the draft's tag to `untagged-…`. `gh release edit` resends `tag_name` automatically;
  raw `gh api` PATCHes must include it.
- Automation enforces these rules: `publish-pypi.yml` creates Releases with
  `--title "$TAG"` and, on resume or recovery, fails closed through
  `scripts/check_release_title.py` when an existing Release has another title. It never
  renames one. Agents verify these rules whenever they touch release automation.

### Stale drafts

- Inspect drafts before preparing a release. Never assume a draft is pending.
- Classify each draft against its tag and the PyPI history:
  - Already shipped (tag and PyPI version both exist): publish it with
    `make_latest=false`, or remove it, only after maintainer approval.
  - Never shipped: never publish it. Delete it only after maintainer approval.
- Never delete drafts automatically. Preserve their notes and assets.

### Release notes

- `CHANGELOG.md` is the single source of truth. The Release body is the extracted
  CHANGELOG section plus one `**Full Changelog**` compare link
  (`scripts/extract_release_notes.py`).
- Allowed `###` sections, in this order, only when they have content: Upgrade notes,
  Added, Changed, Deprecated, Removed, Fixed, Security, Performance, Documentation, CI,
  Tests. `scripts/lint_changelog.py` enforces this for `[Unreleased]` and for releases
  after 1.10.0.
- Use `Documentation`, not `Docs`. Put release automation and tooling entries under
  `CI` or `Changed`.
- Never bulk-rewrite historical notes or regenerate them from current `main`. A
  selective fix needs a dry-run diff and maintainer approval. Never invent PR or commit
  references. Note formatting never changes tags, dates, artifacts or publish state.

## Issue specification and ownership

Issues and implementation acceptance are repository-local: file work in the
repository whose code, workflow, or documentation must change. Each repository
owns its scope, priority, assignee, validation, release decision and closure.
Every issue must be understandable without opening another repository's issue.
When several repositories need changes, create a separate, self-contained issue
and PR in each repository. Never use an issue here as an organization-wide
tracker, a shared implementation contract or an approval gate for other projects.
Link another repository's issue only when its concrete technical behavior is
essential evidence for local reproduction or verification; do not cross-link
implementation issues by default or inherit another repository's acceptance
criteria, ownership or closure requirements.

Before publishing or editing an issue, inspect its title and full body:
- Describe this repository's problem, evidence, affected files and actual Make
  targets, expected behavior and checkable completion criteria.
- Exclude contest/judging, assessment, submission, presentation rehearsal,
  evaluation-tag/SHA freezes and event-timed implementation gates. Commit or
  release identifiers belong only where needed to reproduce technical behavior.
- Do not invent `make spec-*` targets, assume a sibling project's commands apply
  here or prescribe a universal command/report format without checking this
  repository's Makefile and CI.
- Search for duplicate issues and conflicting target semantics before posting;
  reread the saved title/body and correct misplaced scope or references.
Never use issue auto-closing keywords with another repository's issue; its
maintainer controls closure. The existing required PR title check rejects
direct foreign-repository closing references in the PR body, the PR title and
commit messages while allowing Refs links. `main` is squash-merged with the PR
title and the commit messages; an edit to the squash message at merge time is
not checked, so do not add foreign closing keywords there.

An issue body is the current work specification, not a session transcript.
Before implementation, maintainers/agents must ensure it states the problem
and impact, evidence with a revision/environment, reproducible steps or the
investigation question, expected behavior, scope/non-goals, relevant files,
verifiable completion criteria, validation method and actual dependencies.
Say "not verified" when evidence is missing; never invent a reproduction,
server result, release availability or test command. Small docs tasks need
only the applicable fields. Research issues close on a recorded decision;
implementation follows the agreed contract.

- Keep priority/size in canonical labels and execution order in the backlog
  tracker. Do not prepend repeated triage banners to individual issue bodies.
- Put dated progress, pause/resume instructions and review outcomes in issue
  comments. Keep historical reproductions and source links in the body with
  their original revision/date and clear evidence limits.
- Reconcile completed work and closed dependencies when scope changes, a
  related PR merges, work is handed over or the issue is closed. Closed work
  is reference evidence, not a blocker. A merged upstream PR is not proof
  that a compatible release is available.
- **Maintainer-owned work and autonomous agents:** assign the actual implementer in GitHub Assignees before starting; a comment alone does not constitute an assignment. Obtain issue-specific authorization before an autonomous agent takes a newcomer-reserved `good first issue`.
- **Human-led outside contributions (including AI-assisted work):** do not require assignment rights or prior maintainer permission to work on an unclaimed issue or open a PR. Check existing Assignees, comments and PRs first; a coordination comment is welcome but optional. Maintainers handle assignment and label changes, and contributors must coordinate before overlapping someone else's claimed work.
- Preserve a contributor's work and evidence on handoff; maintainers update Assignees and unassign returned work. Release-blocker handoffs require explicit maintainer decisions.

- Before saving an issue edit, check for contradictory current statuses,
  stale dependency/checklist entries, duplicate criteria and unproven claims.
  Re-read the issue after saving. Never mark a tracker complete just because
  its children merged; confirm its integration acceptance separately.

## Issue Labeling (cubrid-lab org standard)

Write GitHub issues, PRs and comments in English; localized documentation remains
welcome, and no specific translation tool is required.

Maintainers and triagers own label policy. An agent filing or triaging on behalf
of a maintainer may apply the canonical labels when authorized. Maintainers or
triagers assign exactly one `priority: <value>` label and exactly one `size: <value>` label for each new issue,
alongside a type label (`bug`/`enhancement`/`documentation`/`chore`/`ci`/…) and an
`area:` label when applicable. These must be GitHub labels, not just text in the
issue title or body. Reporters describe urgency and effort without needing label
permissions. Maintainer-created issues receive these labels at creation; permissionless
reports receive them during initial maintainer triage.

Issue titles use the same `type(scope): description` format as pull request
titles (see [CONTRIBUTING.md](CONTRIBUTING.md#pull-request-and-commit-titles)).
`.github/workflows/issue-triage.yml` flags incomplete human-submitted issue
titles or labels as `status: needs triage` without posting a comment or
guessing priority/size. Maintainers remove that label once triage is complete.
Agents and workflows creating issues through CLI/API must supply canonical
metadata at creation; `GITHUB_TOKEN`-created issues do not retrigger this guard.

Use the following exact names, with **one space after the colon**:

- Priority: `priority: critical`, `priority: high`, `priority: medium`, `priority: low`.
- Size: `size: XS`, `size: S`, `size: M`, `size: L`, `size: XL`.

Do not introduce variants such as `priority:high`, `priority-high`, `P1`, or
`size:S`. Reuse the repository's canonical labels; if a required label is missing,
maintainers create it with the exact name above before filing or triaging the issue. This policy governs
new issue creation, not bulk renaming or relabeling existing issues unless
explicitly requested.

Priority reflects urgency and impact; size estimates implementation effort and
helps contributors pick appropriately scoped work.

| Label | Meaning | Rough guide |
|-------|---------|-------------|
| `size: XS` | Trivial change | < ~10 lines; single-file typo/config/one-liner |
| `size: S` | Small change | One file or one focused function; a single test or doc page |
| `size: M` | Medium change | A few files; a new test module, a bug fix with tests, a CI job |
| `size: L` | Large change | Cross-cutting change across many files; multi-artifact (e.g. demo GIF + video + docs) |
| `size: XL` | Very large | Consider splitting into smaller issues before starting |

Rules:

1. **Size reflects effort, not importance** — a one-line fix for a critical bug is still `size: XS`.
2. **Maintainers assign both `priority:` and `size:` at creation/initial triage.** If scope or impact
   is uncertain, use a provisional estimate, explain the uncertainty in the body,
   and add `status: needs triage` (or the repo's equivalent). Refine the estimates
   during triage rather than omitting either required label.
3. **`good first issue` should be `size: XS` or `size: S`.** If a good-first-issue grows
   past `size: S`, re-scope it or drop the `good first issue` label.
4. **`size: XL` is a signal to split**, not a green light to start a sprawling change.

### Good first issue lifecycle

- **Human-led contributions, including AI-assisted coding under a human contributor's direction:** an unassigned `good first issue` is open for participation. No prior maintainer authorization, GitHub Assignee permission, or internal agent tooling is required to start work or submit a pull request.
- **Coordination, not permission:** before starting, check current Assignees, comments, and open PRs. A short comment expressing interest is welcome but optional; it does not grant exclusive ownership. If someone already owns or is working on the issue, coordinate first and do not silently take over.
- **Maintainer-operated autonomous coding agents:** implementing a newcomer-reserved `good first issue` requires explicit maintainer authorization naming that specific issue and an actual GitHub assignment before implementation. General instructions to clear the backlog, review code, or prepare a release do not grant this authorization.
- Maintainers update Assignees and labels as needed. Outside contributors lacking assignment rights may still submit PRs; reviewer approval, CI, and the existing merge policy remain unchanged.
- Unclaimed: retain `good first issue`.
- When a PR opens: maintainers remove `good first issue` and add `status: in progress`, coordinating the issue owner.
- When a PR merges: close the issue.
- When a PR closes without merging: check that no other open PR still owns the work before restoring `good first issue` and removing `status: in progress`.
- Keep at least three genuinely unclaimed newcomer issues per repository. They should remain XS/S, small in blast radius, with a reference implementation or pattern.
- Release/blocker handoff to an autonomous agent needs a recorded maintainer decision after checking assignees, comments and existing PRs.

## Documentation definition of done

Any change that affects public behavior, compatibility, installation, configuration, APIs, supported versions, error handling, or SQL behavior MUST update the matching documentation in the **same PR**. At minimum keep in sync: `CHANGELOG.md`, the relevant files under `docs/` (e.g. `PARAMETER_BINDING.md`), and the `RELEASE_POLICY.md` behavior/release classification.

If no documentation change is needed, provide a populated standalone physical source line
beginning `Docs: not needed -`, or obtain the existing maintainer `docs-not-needed`
label exception. Empty reasons, `<reason>`, quotes, comments and fenced examples
are rejected by docs-sync. The line may be adjacent to ordinary prose without its
own paragraph. This exception applies only to the docs gate.

Contributors may request translation help in the PR body with missing language(s)
and a reason. Only explicit maintainer approval through `translations-deferred`
authorizes deferral; maintainers own the recorded follow-up. Korean synchronization
remains required and other community translations remain advisory. Keep executed
commands/results, checks not run with reasons, and optional AI review feedback distinct.

Do not mark work complete until code, tests, and documentation are consistent.

## Commit Convention

Issue titles, pull request titles and commit subjects follow
[CONTRIBUTING.md - Pull request and commit titles](CONTRIBUTING.md#pull-request-and-commit-titles):
`type(scope)!: description` with types `feat`, `fix`, `docs`, `test`, `perf`,
`refactor`, `ci`, `build`, `chore`, `style`, `revert`; English, lowercase start
unless the first word is an API name, acronym, or proper noun; no trailing
period, no issue numbers in pull request titles (use `Closes #N` /
`Refs #N` in the body). Pull requests are squash-merged and the pull request
title becomes the commit title (GitHub appends `(#N)`). The `PR title` check enforces it.

The type also drives release-please: `feat` proposes a minor release; `fix`, `perf`,
`docs`, `deps` and `revert` a patch; `!` or a `BREAKING CHANGE` footer a major one; `ci`,
`test`, `refactor`, `chore`, `build` and `style` alone create no release. There is no
`deprecate` type; a deprecation is a `feat` with a `### Deprecated` entry in `CHANGELOG.md`.
The mapping to CHANGELOG headings is `changelog-sections` in
[`release-please-config.json`](release-please-config.json) (see [RELEASING.md](RELEASING.md)).

```
type(scope): description

<body: what changed and why>

Refs #N

<tool attribution trailer, only for work produced with that tool>
```

Preserve actual contributor authorship and existing `Co-authored-by:` trailers. Add a
tool attribution trailer only for commits actually produced with that tool, using that
tool's own trailer (for example `Co-Authored-By: Claude <model> <noreply@anthropic.com>`
for Claude Code, or `Co-authored-by: Sisyphus <clio-agent@sisyphuslabs.ai>` for
OpenCode/Sisyphus). It is not a required footer for outside contributors' commits.
