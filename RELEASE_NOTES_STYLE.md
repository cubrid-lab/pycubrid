# Release notes style for pycubrid

GitHub Releases explain **what changed for Python developers**, not every implementation step or raw commit subject. These rules apply to this repository's existing `CHANGELOG.md` → `scripts/extract_release_notes.py` → GitHub Release path. They do not create another publishing mechanism or change versioning policy.

## Public format

- The GitHub Release title is exactly its tag, such as `v1.17.1`.
- Start the body with **one or two sentences** stating the changes users can observe and any action they need to take. For a documentation/test-only release, say explicitly that the ordinary driver behavior is unchanged when that claim is verified.
- Use only nonempty standard `###` sections, in the existing order: `Upgrade notes`, `Added`, `Changed`, `Deprecated`, `Removed`, `Fixed`, `Security`, `Performance`, `Documentation`, `CI`, `Tests`. Keep `Documentation` rather than `Docs`.
- For a release **with a preceding tag**, finish with exactly one `**Full Changelog**` comparison link supplied by `scripts/extract_release_notes.py`. For the first release, omit that link: there is no earlier tag to compare. Do not manually duplicate it in the canonical version section.
- Do **not** add separate `Highlights`, `Contributors` or `New Contributors` sections. The summary and inline credits serve those purposes without hiding changes in a second list.

## Write for readers

- Lead an entry with the **observable result**, the affected API or workflow, and why it matters. Prefer one concise paragraph to internal helper names, test matrix details and a chronicle of PR implementation steps. Detailed proof can remain in linked PRs or docs.
- Preserve significant limits and user actions, especially PEP 249 behavior, sync/async differences, transaction/autocommit semantics, CUBRID server and supported Python versions, TLS and security, source-install dependencies, and migration instructions. Never shorten a warning until it becomes misleading.
- Distinguish a **database server version**, a **Python driver/package version** and an unpublished release candidate. A failed test is not evidence that an entire database or driver is unsupported.
- Put migration/breaking changes near the beginning under `Upgrade notes`. A feature removal must not be disguised as `Documentation` or silently shipped in a PATCH.
- Combine curated and generated descriptions of the same change into one entry, preserving the relevant source references and verified contributors. `RELEASE_CHANGELOG.md` is raw generator output; the composed `CHANGELOG.md` is the publication source.
- Leave one blank line **between top-level entries** for readable GitHub Markdown. Keep continuation text within its bullet. Do not split a Markdown link, `#123` reference, backtick identifier or `by @username` credit across a physical line; avoid accidental hard line breaks.

## Contributors and links

- Put `by @username` **beside the relevant change**, including documentation, tests and CI changes, only when the contributor is verified from actual merged PRs or commit/co-author evidence in **pycubrid**.
- A verified first merged contribution may be marked `by @username (first contribution 🎉)` after checking earlier merged PR history. Omit that marker if uncertain.
- Exclude bots, automated agents and publishing service identities from **human** credits, even when GitHub labels their account type as `User`. People who used AI assistance are still eligible for verified human credit. Do not invent an author or call an issue reporter the change author without evidence.
- Link actual PRs as PRs and issues as issues. A `refs` relation must not be presented as `closes`; never imply another repository's issue is closed by this release.

## Example (illustrative, not an actual release)

This maintenance release fixes source builds and improves setup instructions. Normal query and transaction behavior is unchanged.

### Fixed

- **Source builds** — Updated the minimum supported build backend so non-isolated builds can process package metadata. Existing published-wheel installs are unaffected (PR #123) by @alice.

### Documentation

- **Connection setup** — Clarified how to start and connect to a local CUBRID broker (PR #124) by @bob (first contribution 🎉).

### Tests

- **Transaction regressions** — Added tests for commit/rollback visibility without changing runtime code (PR #125) by @charlie.

In the example above, PR numbers and usernames are fictional. A real release includes validated hyperlinks and only verified credits.

## Review and publication

1. Compare every statement with the actual version's tagged changes, relevant merged PRs, source history and supported behavior; verify human and first-contribution credits independently.
2. Review the **rendered**, extracted `CHANGELOG.md` version body, including the summary, section ordering, distinct change entries, links, paragraph spacing, bylines and single comparison link where a preceding tag exists. Do not approve a candidate based only on its shorter Release Please PR body or `RELEASE_CHANGELOG.md`.
3. Run the existing `scripts/lint_changelog.py` checks and `make release-check VERSION=X.Y.Z` on the finished candidate, alongside all required final-head CI. No separate heavyweight test lane or release-note publisher is introduced.
4. Historical improvements require their own version-scoped review, exact original-body backup, evidence-based corrections and guarded publication of **body text only**. Keep release titles, tags, original publication dates, draft/prerelease states, assets and PyPI packages unchanged. Source PR merge is not proof that a public Release body was updated.

See [RELEASING.md](RELEASING.md) for release gates and [RELEASE_POLICY.md](RELEASE_POLICY.md) for pycubrid-specific compatibility policy.
