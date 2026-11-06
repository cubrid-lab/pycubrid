<!--
Title: `type: description` or `type(scope): description`; add `!` before the colon
for a breaking change. Types: feat, fix, docs, test, perf, refactor, ci, build,
chore, style, revert. English, lowercase start unless the first word is an API
name, acronym, or proper noun. No trailing period, no issue numbers (put
"Closes #123" in Related Issues). The title becomes the squash commit title.
See CONTRIBUTING.md#pull-request-and-commit-titles.
-->
## Summary

<!-- Brief description of changes -->

## Changes

<!-- List the specific changes made -->

-

## Scope and Non-goals

<!-- What this PR deliberately does not change; defer out-of-scope findings to a separate issue -->

## Public API and product-scope decision (if applicable)

<!-- If adding a public API, materially changing its behavior, adding a runtime
dependency, or changing support policy, link the related issue's explicit
maintainer decision and explain the independent ordinary-driver use case,
existing alternatives, and long-term stability/maintenance implications.
Triage labels and green CI are not product approval. Removing Compat does not
require replacing every Compat feature. Maintainer-directed implementations
need this decision before starting; external contributors may submit a
proposal/PR without prior approval, but maintainers resolve product scope
before merge. For ordinary bug fixes/docs within an existing contract, N/A
with a reason is enough. Do not assert an unapproved future release version. -->

Decision / issue link: pending or N/A (reason)

Independent use case, alternatives and continuing API cost: pending or N/A (reason)

Release target: unreleased until a release is reviewed

## Premise and scope countercheck (for feature proposals or claimed gaps)

<!-- State the observation and the inference separately. Check a concrete
counterexample or the closest existing API, SQL, documentation or downstream
recipe, especially before claiming a capability is unavailable. A failed
syntax is not evidence that all supported routes fail (see #1129/#1133).
For server-dependent claims, report real commands, CUBRID/Python versions,
results and untested cases. If the premise is disproved, do not merge a
needless feature. Ordinary bug/doc changes may state N/A with a reason. -->

Observed problem, exact evidence and versions: pending or N/A (reason)

Strongest existing alternative or counterexample checked, with result: pending or N/A (reason)

<!-- For changes to public API, compare the new api-baseline.json to main and
record which exports/methods/signatures changed. A green compat-check verifies
the committed baseline, not whether adding the API is justified. For
high-impact changes the maintainer challenges the rationale and scope,
independently of code/tests; optional agent reviews are advisory, and no
additional reviewer or CI approval gate is introduced. -->

Public API baseline diff and contract impact: none or describe changed entries

Maintainer scope challenge and decision: pending or N/A (reason)

## Type of Change

<!-- Check the relevant option -->

- [ ] Bug fix (non-breaking change that fixes an issue)
- [ ] New feature (non-breaking change that adds functionality)
- [ ] Breaking change (fix or feature that would cause existing functionality to change; add `!` to the title)
- [ ] Documentation update
- [ ] Refactoring (no functional changes)
- [ ] Chore (maintenance, dependencies, CI, etc.)

## Checklist

- [ ] I checked existing Assignees, claims and PRs; maintainer-operated autonomous agents taking `good first issue` work have issue-specific approval and assignment. Human-led outside contributors need no preapproval or Assignee rights.
- [ ] The linked issue has current scope, verifiable completion criteria and validation instructions; completed dependencies are not still blockers
- [ ] I checked related open PRs and recent merged changes and recorded any replacement or unresolved prerequisite below

- [ ] My code follows the project's code style
- [ ] I have documented required GitHub CI results and any recommended local checks (or why they were not run)
- [ ] I have added tests for new functionality (if applicable)
- [ ] I have updated affected behavior/API/version/config docs, or provided a genuine standalone docs-not-needed reason / requested the existing maintainer label exception
- [ ] I have recorded missing translations and requested maintainer help if needed; this request does not authorize deferral
- [ ] I have reported relevant warnings, limitations and remaining validation gaps

<!-- Docs exceptions need real text after "Docs: not needed -" on a standalone physical source line outside quotes/comments/fences. The line may be adjacent to ordinary prose without its own paragraph. Do not leave the template placeholder as a reason. Maintainers coordinate internal reviews, labels and approved translation follow-up. -->

## Review and Final Disposition

<!-- Same review criteria for human-authored and AI-assisted changes. -->
- Required GitHub CI at final PR head: **pending** (link checks and relevant result).
- Local checks performed (optional): commands/results, or "not run" with reason.
- Optional Oracle/Copilot/agent-review findings: link and dispositions, or N/A.
- Finding dispositions: fixed commit plus verification / rejected with reason / deferred issue.
- Open threads and blocking security issues: list or "none" after review.
- Maintainer merge decision: **pending** until final code, required CI and risk are checked.
<!-- For sensitive connection/TLS, wire protocol, release, workflow permissions
     or other protected paths, retain explicit independent maintainer scrutiny.
     No coding tool or optional AI reviewer grants automatic approval. -->

## Validation Evidence

Commands actually run and results:

Checks not run and reasons:

Live CUBRID evidence (connection/protocol/SQL changes; CUBRID version and command, or "not applicable"):

Optional additional AI review (tool/findings; advisory only, separate from executed test evidence):

## Related Issues

<!-- Use Closes #N for a completed LOCAL issue (or N/A and a reason).
     Cite sibling dependencies as Refs owner/repo#N, not closing keywords.
     Each repo manages its own work and closure; the PR title check enforces
     no direct auto-closing references to a foreign repository. -->

<!-- For this repository, cite Supersedes #N / Depends on #N /
     Parallel to #N, with one reason, or None. Recheck before merge. -->
Related PRs and precedence: None
Unresolved decision or dependency: None
