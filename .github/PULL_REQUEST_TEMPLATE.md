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
