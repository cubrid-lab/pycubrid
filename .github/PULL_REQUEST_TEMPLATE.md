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
- [ ] I have run `make check` and `make test` (or `make verify`), or recorded checks not run and reasons below
- [ ] I have added tests for new functionality (if applicable)
- [ ] I have updated affected behavior/API/version/config docs, or provided a genuine standalone docs-not-needed reason / requested the existing maintainer label exception
- [ ] I have recorded missing translations and requested maintainer help if needed; this request does not authorize deferral
- [ ] I have reported relevant warnings, limitations and remaining validation gaps

<!-- Docs exceptions need real text after "Docs: not needed -" on a standalone physical source line outside quotes/comments/fences. The line may be adjacent to ordinary prose without its own paragraph. Do not leave the template placeholder as a reason. Maintainers coordinate internal reviews, labels and approved translation follow-up. -->

## Review Route and Final Disposition

<!-- Record the route actually used; human contributors do not need a specific AI tool. -->
- Development route: OpenCode + Oracle pre/post / trivial OpenCode with recorded Oracle skip / standalone Claude Code / standalone Codex / standalone ChatGPT or other agent (name it) / human (select one).
- Trivial OpenCode exception, if used: skipped Oracle phases and reason, or not applicable.
- Review evidence: reviewer or Oracle design/post-review references, reviewed head SHA, or **pending**.
- Finding dispositions: fixed commit + verification / rejected with reason / deferred issue.
- Unresolved review threads and reason, or "none" after verification.
- Merge decision: **pending**; maintainer to confirm the applicable review route, final code, required CI and thread dispositions.
<!-- A review request, green CI, or automated "approval recommended" text alone does not complete the review route.
     The advisory `agent-review` check reads only GitHub's submitted reviews of the head commit;
     this section and its checkboxes are records for maintainers, never evidence. Protected paths
     (binding, protocol, packet, connection/TLS and compat modules, every workflow, the release and
     lane-audit scripts, the gate) need a maintainer's APPROVED review.
     See CONTRIBUTING.md#agent-review-evidence-gate. -->

## Validation Evidence

Commands actually run and results:

Checks not run and reasons:

Live CUBRID evidence (connection/protocol/SQL changes; CUBRID version and command, or "not applicable"):

Optional additional AI review (tool/findings; separate from executed tests and not a replacement for the required review route):

## Related Issues

<!-- Use Closes #N for a completed LOCAL issue (or N/A and a reason).
     Cite sibling dependencies as Refs owner/repo#N, not closing keywords.
     Each repo manages its own work and closure; the PR title check enforces
     no direct auto-closing references to a foreign repository. -->
