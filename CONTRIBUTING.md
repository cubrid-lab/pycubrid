# Contributing to pycubrid

Thank you for your interest in contributing to `pycubrid`.

Write GitHub issues, PRs and comments in English; localized documentation remains
welcome, and no specific translation tool is required.

## Development Setup

### Prerequisites

- Python 3.11+
- Git
- Docker (for integration tests)

### Installation

```bash
git clone https://github.com/cubrid-lab/pycubrid.git
cd pycubrid

python3 -m venv venv
source venv/bin/activate

# Contributor tools are PEP 735 dependency groups in pyproject.toml, not extras,
# so `--group` needs pip 25.1 or later (`uv pip install -e . --group dev` also works).
python -m pip install --upgrade pip
pip install -e . --group dev
```

The `dev` group includes the `test`, `lint`, `security` and `release` groups.
Install a single group when only that tool set is needed, for example
`pip install --group docs` for the documentation site or
`pip install -e . --group dev --group mutation` for mutation testing. The former
`pip install -e ".[dev]"` extra no longer exists; it is replaced by
`pip install -e . --group dev` (#784).

## Running Tests

### Offline tests

```bash
make test
```

### Integration tests

```bash
make integration                          # Docker broker on localhost:33000
make integration CUBRID_TEST_PORT=33522   # use a free port if 33000 is taken
```

`make integration` waits for readiness, fails if the broker never comes up or
if every selected test skips, and always removes the container. To test an
already-running server, set the endpoint explicitly:

```bash
CUBRID_TEST_HOST=127.0.0.1 CUBRID_TEST_PORT=33522 \
  pytest tests/ -m "integration and not slow and not tls" -v
```

`CUBRID_TEST_URL` or `CUBRID_TEST_HOST` *enables* integration tests; the
endpoint comes from the per-field `CUBRID_TEST_*` variables, then the URL, then
`localhost:33000/testdb` as user `dba`. With nothing configured, integration
tests skip; with an endpoint configured but unreachable, they **error** instead
of skipping. See [Integration Tests](docs/DEVELOPMENT.md#integration-tests).

### Async TLS integration tests (optional)

The dedicated CI TLS lane selects `integration and tls` against an SSL-enabled
broker. For local broker/certificate setup, follow the existing
[async TLS instructions](docs/DEVELOPMENT.md#async-tls-integration-tests).
Record live checks not run and their reason; maintainers coordinate missing
broker/version coverage for connection or protocol changes.

## Code Style

This project uses Ruff for linting and formatting.

```bash
make lint
```

To auto-fix:

```bash
make format
```

Activate the project development environment (`pip install -e . --group dev`) before
running Make or pre-commit. The Ruff and Mypy pre-commit hooks are `repo: local`,
`language: system` hooks that invoke `python3 -m ruff`/`python3 -m mypy` from that
same active environment, so there is a single source of truth for each tool's
version: the exact pin in `pyproject.toml`. Activate that environment (or a venv
where it's installed) whenever a commit should run the hooks; otherwise
Ruff/Mypy are missing or a stale/global version silently runs instead of the
pinned one. `make tooling-check` verifies the installed Ruff/Mypy versions
against those pins and confirms the hooks are wired to run through the active
environment. Make and CI share `LINT_PATHS` (`pycubrid tests scripts demos
examples`) with explicit Python/pyi discovery and hook types, so Markdown is not
reformatted; strict Mypy (the package plus the `tests/typing_self_contract.py`
static contract in `make typecheck`) remains separate, and its pre-commit hook
explicitly checks `pycubrid/`.

When updating either tool, change its pin in the `lint` dependency group of `pyproject.toml`,
reinstall the `dev` group, then run `make check-all` and `pre-commit run --all-files`; there is no
separate hook revision to edit. The drift gate rejects missing/ambiguous pins,
an installed version that no longer matches the pin, and narrowed scopes.
Dependabot's `pip` ecosystem can bump the `pyproject.toml` pin on its own and CI
stays green, since the pre-commit hooks always run whatever is installed.

## Pull Request Guidelines

Maintainer/agent-authored PRs use the tool-specific review route in
[AGENTS.md](AGENTS.md#development-workflow-cubrid-lab-org-standard):
non-trivial OpenCode work uses documented Oracle design and post-implementation
reviews; trivial OpenCode edits may skip those reviews with a recorded reason.
Standalone Claude Code, Codex, ChatGPT or other agent tools wait for a substantive
submitted GitHub Copilot review or another person's review before merge.
Reviewer requests, self-review, green CI and automated approval suggestions
alone do not complete the review route. The final outcome, addressed findings
and unresolved threads are recorded as described in AGENTS.md. Human
contributions follow ordinary contributor review; no contributor is required
to install or use any agent or internal review tool.

1. Keep changes focused and explain the motivation in the PR description.
2. Add or update tests for behavior changes.
3. Run `make check-all` and `make test`; report commands/results and checks not run.
4. Run integration tests for connection/protocol-related updates.
5. Update `CHANGELOG.md` for user-visible changes.

`.github/CODEOWNERS` requests maintainer review automatically for release, CI,
security-policy and wire-protocol/connection paths. It only routes reviews: it is
not a security boundary, and other changes do not wait on a code owner.

Outside contributors provide motivation, implementation, tests and affected docs.
Maintainers coordinate internal Oracle/agent reviews, integration coverage and
release classification. These project tools are not an installation prerequisite
for external contributions. Preserve contributor authorship; add tool attribution
only when that tool actually produced a commit.

If docs are unnecessary, put a real reason on a standalone physical source line beginning
`Docs: not needed -`. Empty text, `<reason>`, quotations, comments and fenced examples
do not grant an exemption. The line may be adjacent to ordinary prose; it does not
need its own paragraph. The existing `docs-not-needed` label is a separate
maintainer-controlled exception; neither docs exception bypasses code, security or
release checks.
Up to three leading spaces are allowed; tab/four-space code examples and raw HTML
`blockquote`/`pre`/`code` blocks do not grant an exemption.

For translation help, name the missing language(s) and explain the constraint in
the PR body. That request does not authorize deferral. Maintainers explicitly approve
the existing `translations-deferred` label and own the recorded follow-up. Korean
README synchronization remains required; other translations remain advisory.

When changing docs, regenerate `docs/llms-full.txt` with
`python scripts/generate_llms_full.py` (it also copies the canonical
`docs/llms.txt` index to the root `llms.txt`; edit only `docs/llms.txt`, and CI
fails if either generated file is stale) and run the existing site check
`mkdocs build --strict` after installing its documented tooling
(`pip install --group docs`). AI review feedback is separate from commands
actually executed; report both accurately, including gaps and existing warnings.

Maintainers update shared workflow callers through a reviewed upstream commit SHA:
verify the target workflow and its `workflow_call` inputs at that commit, update
callers together and run required checks. The shared doc-lint workflow still fetches
main-based configuration/scanner assets, so caller pinning does not freeze those assets.

## Repository-local issue ownership

Issues own implementation and acceptance in the repository whose files change.
Provide a self-contained local problem, scope, owner, tests, and closure
decision. Sibling repositories may be cited with Refs owner/repo#N when they
supply a real dependency or compatibility contract; their priorities and
completion are not inherited. Close only this repository's issues (Closes #N).
PRs with no local issue may state N/A and a reason. The existing PR title check
rejects direct cross-repository issue-closing keywords, including emphasized or
linked ones, in the PR body, the PR title and commit messages. The title and
commit messages make up the squash commit and are read as plain text; an edit to
the squash message at merge time is not checked. In the body, the check ignores
fenced and indented code blocks and HTML comment blocks only where it can follow
the Markdown exactly, so anything uncertain is checked: code and comment blocks
inside list items, footnotes and block quotes are always checked; after a fence
it cannot track (in a list item, block quote or raw HTML, on an indented line,
or as a line that only looks like a fence), later fences and comments are
checked; after anything that may start an HTML tag, even in prose or inline
code, later comments are checked; and after a raw-text element name such as
`<textarea>` or `<select>`, nothing is ignored. Inline code and inline comments
are always checked. External contributors do not need to install internal agent
tools or create duplicate work.

### Good first issues and claiming work

An unassigned `good first issue` is open to **human contributors**, including contributors who direct and review AI-assisted code. You may begin work and open a PR **without prior maintainer permission, GitHub Assignee access, or any internal agent tool**.

Before working, check the issue's Assignees, comments and open PRs. A brief comment that you plan to contribute is encouraged but optional: it is coordination, not an approval requirement or an exclusive reservation. If the issue is already claimed or a PR is in progress, ask before duplicating or taking over that work. Maintainers manage Assignees and labels; lack of self-assignment privileges does **not** block a human contributor's PR.

**Maintainer-operated autonomous agents** are different from human-directed AI-assisted contributions: an agent may not consume a newcomer-reserved `good first issue` unless a maintainer explicitly authorizes **that specific issue** and assigns the implementer before coding. Broad backlog-fixing or release requests do not count.

Normal testing, docs and review requirements still apply. Maintainers decide whether and when to merge.

## Pull request and commit titles

This rule covers issue titles, pull request titles and commit subjects in every
cubrid-lab repository. Pull requests are squash-merged and the pull request
title becomes the commit title on `main`, so the pull request title is the one
that must be right. The `PR title` check enforces it.

```text
type: description
type(scope): description
type!: description
type(scope)!: description
```

- **type** (lowercase, exactly one of): `feat`, `fix`, `docs`, `test`, `perf`,
  `refactor`, `ci`, `build`, `chore`, `style`, `revert`.
- **scope** is optional: lowercase letters, digits, `-` or `_`, such as
  `compiler`, `aio`, `deps` or `release`.
- **`!`** before the colon marks a breaking change. Follow the repository's
  release policy for breaking changes as well.
- Exactly **one space** after the colon.
- **description**: English and specific (name the function, type or behavior
  that changed). Start with a lowercase letter unless the first word is an API
  name, acronym or proper noun. No trailing period.
- No bracket, status or priority prefixes (`[Bug]`, `[WIP]`, `Track:`,
  `epic:`, `P1`). Open a draft pull request for unfinished work; priority and
  size are labels.
- No issue or pull request numbers in the title. Put `Closes #123` or
  `Refs #123` in the pull request body. GitHub appends the pull request
  number, for example `(#456)`, to the squash commit by itself.

| Type | Use for |
|------|---------|
| `feat` | A new user-facing capability |
| `fix` | Corrects wrong behavior, including security fixes |
| `docs` | Documentation only |
| `test` | Tests only |
| `perf` | Faster or lighter with no behavior change |
| `refactor` | Restructuring with no behavior change |
| `ci` | CI workflows and their configuration |
| `build` | Packaging and the build system |
| `chore` | Maintenance: releases, dependency bumps, housekeeping |
| `style` | Formatting only |
| `revert` | Reverts an earlier change; name it in the description |

Examples:

```text
fix(protocol): keep the CAS session after OUT_TRAN
feat(aio): add a charset connection option
docs: document JSON as_numeric() input limits
chore(deps): bump ruff from 0.16.8 to 0.16.9
chore: release v1.9.0
refactor(compiler)!: drop legacy LIMIT rendering
```

Issue forms prefill a type prefix; keep it and write the rest of the title the
same way. A tracking issue (epic) uses the type of the work it tracks.

Maintainers merge with **squash merge only** and keep the pull request title as
the commit title. Branch commits are squashed into the commit body, so keep
their messages meaningful and keep any `Co-authored-by:` trailers intact.

## Releases

Contributors never release. Add user-visible changes under `## [Unreleased]` in
`CHANGELOG.md`, and do not change `__version__` or add a dated `## [X.Y.Z]`
section in an ordinary PR: a merged version change is what starts an automatic
release. Maintainers use the PR-only `release-please.yml` generator; curated Unreleased and Upgrade notes remain reviewed in CHANGELOG. Freeze the candidate with `autorelease: review` before editing its notes, and start CI at the final bot-updated head; see
[`RELEASING.md`](RELEASING.md).

## Reporting Issues

Search for an existing issue first, then use the closest issue form. Keep its
prefilled title prefix; for a custom issue, pick the type from
[Pull request and commit titles](#pull-request-and-commit-titles), for example
`fix(protocol): ...`.

Reporters describe impact and reproduction; they do **not** need permission
to apply GitHub labels. Maintainers assign a type label, one
`priority: <value>` and one `size: <value>` label (plus `area:` when relevant).
Topical labels such as `testing` may also be present.
Human-submitted CLI/API issues with incomplete metadata receive
`status: needs triage`. The maintainer corrects the metadata and removes
that label. Workflows creating issues with `GITHUB_TOKEN` must set the title
and labels themselves: GitHub does not start another workflow from that event.

When filing an issue, include:

- Python version
- CUBRID server version
- Minimal reproduction snippet
- Full traceback or error output

Describe urgency and estimated effort. Maintainers or triagers assign the actual
canonical `priority:`/`size:` GitHub labels; reporter label permissions are not required.

## Bug Discovery → Regression Workflow

pycubrid runs an adversarial "Bug Hunt" test layer (property fuzzing,
protocol fuzzing, state machines, metamorphic parity, fault injection,
differential, mutation, resource/soak). Every defect surfaced by any of
these — or by manual review or downstream dogfooding — MUST follow this
cycle before the fix lands:

```
Bug
 → Minimal reproduction
 → GitHub Issue (label: bug + area:)
 → Failing regression test (committed FIRST, red)
 → Fix
 → Permanent regression contract (the test is now green and kept)
```

Rules:

1. **No bug fix without a regression test** unless it is technically
   impossible to write one; if impossible, say so explicitly in the PR.
2. **`xfail` requires a linked issue.** Use `@pytest.mark.xfail(strict=True,
   reason="issue #NNN: ...")` for a known-but-unfixed defect so the guard
   flips to a hard failure (XPASS) the moment the bug is fixed, forcing the
   `xfail` marker to be removed in the fixing PR.
3. **No blanket `|| true`** or bare `except: pass` to hide failures in test
   or CI code.
4. **An unexpected `XPASS` is a signal**, not noise: the referenced bug is
   fixed — remove the `xfail` and assert the correct behavior in the same PR.
5. **Classify known limitations** as driver / server / upstream so a reader
   can tell whether the divergence is pycubrid's responsibility. Documented
   server-behavior and implementation-difference divergences are pinned in
   the relevant test (e.g. the CUBRIDdb differential suite) rather than left
   as silent skips.

Worked example: issue #362 (`Lob.read` silent truncation) was found by the
LOB adversarial suite, filed with a minimal repro, guarded by a strict
`xfail` regression test, then fixed — the `xfail` became a passing assertion
in the fixing PR.

See [`RELEASE_POLICY.md`](RELEASE_POLICY.md) §7 for where behavior-change
classifications are recorded.

### CUBRID version differential

`tests/test_version_differential.py` (#351) runs the same Hypothesis-generated
values and statements against CUBRID 10.2, 11.0, 11.2 and 11.4 at once and
compares what pycubrid exposes: error class/`errno`/`sqlstate`, `rowcount`,
`lastrowid`, `description`, and each fetched value's Python type and value.
It runs in the `version-differential` job of `integration-full.yml` (release
`workflow_call` and manual `workflow_dispatch`, not per PR). Locally, start one container per
version and point the suite at them:

```bash
CUBRID_VERSION_MATRIX="10.2=127.0.0.1:33102,11.0=127.0.0.1:33110,11.2=127.0.0.1:33112,11.4=127.0.0.1:33114" \
CUBRID_TEST_HOST=127.0.0.1 CUBRID_TEST_PORT=33114 \
  python -m pytest tests/ -m "integration and version_matrix"
```

A divergence passes only when a `VersionDifference` in
`tests/helpers/version_matrix.py` explains it: a reason, a link to the CUBRID
change, the versions that differ, the fields allowed to differ, and a tag the
generator attaches to workloads that can hit it. Each entry also has a
deterministic probe that fails when the difference stops reproducing. Any
other divergence is a driver bug (fix it or file it) or an undocumented
server change (confirm it outside pycubrid, e.g. with `csql`, then document
it).

## Minimum PR validation

Follow the [CI execution policy](docs/CI_POLICY.md). PR smoke is representative,
not full-suite/coverage evidence. Run relevant regression tests locally and report
commands/results; request exact-head full validation where compatibility requires it.

## Keeping issue descriptions actionable

Before coding, agree the issue's problem, expected behavior, scope, completion
criteria and validation method. Keep the body as the current specification;
record dated progress in comments. Maintainers reconcile closed dependencies
and completed checklist items after a related merge or handoff. Preserve the
original reproduction's revision and limits: older evidence is not proof of
current behavior. Priorities/sizes belong in labels, execution order in the
backlog tracker. Research closes on a documented decision, not an implied
promise to implement every proposed option.

For human-led contributions to an unclaimed issue, GitHub Assignee access and
maintainer preapproval are not required. Check existing Assignees, comments and
open PRs, and coordinate before overlapping another contributor's work.
Maintainers manage assignment and handoffs. Maintainer-operated autonomous
agents must be assigned before implementation and need explicit authorization
for each newcomer-reserved `good first issue`.
A reviewer does not need to be an issue assignee.
