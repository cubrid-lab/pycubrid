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
| Tests and CI workflows | [Testing and CI guide](docs/agents/testing-ci.md), [docs/CI_POLICY.md](docs/CI_POLICY.md), [docs/DEVELOPMENT.md](docs/DEVELOPMENT.md) | `make test`, `make check-all` |
| Documentation and contributor guidance | [Documentation guide](docs/agents/documentation.md), [Documentation definition of done](#documentation-definition-of-done), [CONTRIBUTING.md](CONTRIBUTING.md) | `python scripts/generate_llms_full.py`, `mkdocs build --strict`, `python scripts/check_docs_links.py`, `python scripts/check_agents_docs.py` |
| Release | [Release Process](#release-process), [GitHub Release Policy](#github-release-policy), [RELEASING.md](RELEASING.md) | `make release-check VERSION=x.y.z` (read-only; never release by hand) |
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
make test             # Offline tests with 95% coverage threshold
make lint             # ruff check + format
make typecheck        # mypy
make format           # Auto-fix lint/format
make integration      # Docker → readiness wait → integration tests → skip audit → cleanup
make help             # Every target, including check, check-all, security and mutation
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

Choose the required review route from the tool that **actually developed** the
change; record it in the PR. A review request, green CI, self-review, or an
automated "approval recommended" summary is not a completed review.

**OpenCode with Oracle** — for non-trivial work:

1. Obtain an Oracle **pre-implementation design review** of the intended scope.
2. Implement the agreed change and its tests.
3. Update the affected behavior, API, support and release documentation with the change.
4. Obtain an Oracle **post-implementation code review** of the resulting code,
   address material findings and record both review outcomes and validation.

Trivial OpenCode edits may skip the Oracle reviews only with a recorded reason.
The two Oracle reviews satisfy this tool's review route; an additional GitHub
Copilot review is not automatically required. Existing repository/branch
protection, maintainer and CI requirements still apply.

**Standalone agents (Claude Code, Codex, ChatGPT, or other agent tools)** —
implement and validate first, then open a GitHub PR and **wait for a substantive
submitted GitHub Copilot review or a maintainer's review**, recorded on the PR,
before merging (maintainer decision, 2026-10-15). A reviewer agent that the
implementer runs may be an extra check, but it does not satisfy this route.
A Copilot `COMMENTED` review with an actual code assessment can count;
an automated suggestion without substantive assessment does not.
Review-requested/pending status never counts.
Triage the findings and verify accepted fixes against the final relevant head.
If no qualifying review arrives, leave the PR unmerged (Draft/pending review);
do not substitute Oracle reviews that were not actually performed.

**`agent-review` check (advisory only):** `.github/workflows/agent-review.yml`
reports from GitHub's own review records whether this route is met (rules in
[CONTRIBUTING.md](CONTRIBUTING.md#agent-review-evidence-gate); a copy of the
cubrid-mcp-server gate with only its constants changed). It counts a substantive
Copilot review of the head within the two-round budget, or a maintainer's
`APPROVED` review, or `COMMENTED` review that opens an inline thread; never
PR-body checkboxes, pending requests, self-review, other people's or other AI
agents' reviews, summary-only bot text or a review of an older commit (unless the
PR's change against `main` is identical at both commits). Protected paths (the
binding, protocol, packet, connection/TLS and `compat` modules, every workflow,
the release and lane-audit scripts, and the gate itself) need a maintainer's
`APPROVED` review. A PR can publish a check with the same name, so the check is
a reading aid and must not be made a required status check until it is posted
from an identity a PR cannot impersonate (#945).

**Human contributions** — use the repository's ordinary contributor and review
requirements. This route is not a fallback for unnamed standalone agents, which
use the route above. Maintainers coordinate tooling, integration and release
classification. No outside contributor is required to install OpenCode, Oracle,
Claude Code, Codex or Copilot. Preserve authorship and use tool attribution only
for work actually produced with that tool.

For maintainer-operated autonomous agents, broad requests to review, fix or prepare a release do not authorize taking an assigned issue or a newcomer-reserved `good first issue`. Human-led community contributions follow the open participation rules below.

## Agent PR scope and review guardrails

- Before editing, record one acceptance contract, affected files, explicit non-goals
  and the validation plan. Keep each PR to one independently reviewable change.
  Separate contributor guidance, CI configuration and new validator behavior.
- Triage AI findings against that contract, a supported-environment reproduction
  and impact. AI severity is not authority to add capabilities or widen the contract;
  obtain explicit maintainer direction or defer out-of-scope work to a separate issue.
- Batch accepted fixes locally and run relevant checks before publishing a review
  head. Deduplicate agent-initiated review requests by head SHA and review purpose.
- Default to two published AI review rounds total per scoped PR/task: the initial
  review and one corrective re-review. New commits do not reset this budget.
  Further rounds or scope expansion require explicit maintainer direction.
- If unresolved work needs another round at the limit, stop automatic revisions;
  keep the PR Draft and report incomplete work, blockers and a proposed split.
  Never merge with unresolved critical/security defects or failed required CI.
- Maintain one editable, agent-owned English status comment. Avoid bot mentions in
  routine updates, per-finding progress replies and repeated review requests.
  Preserve contributor history; revisit external PRs only after an author-updated head SHA.

## Review disposition and merge evidence

- Record the development/review route, reviewer or Oracle evidence (with links or
  an identifiable record), reviewed head SHA, and whether review is pending.
  Do not describe a Copilot "approval recommended" summary as a GitHub APPROVED
  review; a submitted COMMENTED review is distinct from formal approval.
- Triage each actionable finding as fixed, rejected with evidence, or deferred
  with a linked issue and rationale. For fixes, cite the commit and focused
  verification; for rejections, explain the contract or reproduction evidence.
- Reply once in each relevant inline thread with its **final** disposition, not
  repeated progress updates. A maintainer or reviewer resolves the thread only
  after checking the updated code/evidence. An outdated or replied-to thread is
  not automatically resolved. Do not bulk-resolve historical conversations.
- Maintain one editable agent-owned English PR status comment for the overall
  outcome, pending review, required CI, and unrun checks. Avoid repeated bot
  mentions and duplicated test summaries across inline threads.
- Verify the applicable review route and the final relevant source state before
  merge. Never merge with unresolved critical/security findings, failed required
  CI, or the required GitHub review still pending. Nonblocking unresolved findings
  need a recorded maintainer disposition. This policy does not change branch
  protection or authorize automatic merging.

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
dispatch of `publish-pypi.yml`. Procedure, failure matrix and recovery:
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
Cross-repository links are allowed as technical references (Refs owner/repo#N),
but do not transfer work or require another repository to close its issue.
For changes to several repositories, create independent local issues and PRs.
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
