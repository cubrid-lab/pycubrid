# Agent task evaluations

This directory holds reproducible evaluations of how a coding agent handles a
real pycubrid task: whether it kept to the task's scope, added a regression
test that detects the defect, updated the documentation, avoided SQL built by
string formatting, and reported honestly what it did and did not run (#925).

The evaluations measure an attempt; they do not measure the driver. The test
suite, the official differential checks, the full integration matrix and the
release gates stay the only source for whether pycubrid's code is correct, and
nothing here changes them or the default CI matrices.

## What is here

| Path | Content |
|---|---|
| `selftest/task.toml` | A synthetic task that the grader's own fixtures use |
| `selftest/candidates/<case>/` | Hand-written candidates, one per grader check, each with its `expected.json` |
| `../scripts/agent_eval.py` | The offline grader |

Real task manifests, built from earlier pycubrid bugs and fixes, and the
opt-in runner that applies an attempt in a pinned worktree are added in later
pull requests of #925.

## Task manifest (format 1)

A task is a directory with a `task.toml`:

| Key | Meaning |
|---|---|
| `format` | `1` |
| `id`, `title` | A stable task ID and a short title |
| `base_sha` | The full commit the attempt starts from |
| `[problem] statement` | The problem as the agent receives it |
| `[problem] evidence` | Links to the original issue, pull request or commit (required) |
| `[scope] allowed` | Glob patterns of the files the change may touch |
| `[scope] non_goals` | What the change must leave alone |
| `[scope] required_docs` | Files the change must update, such as `CHANGELOG.md` |
| `[validation] commands` | The commands the attempt must run, or report as not run |
| `[validation] regression_tests` | pytest node IDs that the patch must add, that fail on the base and pass on the patch |
| `[expected] behavior` | The reference behavior a correct change produces |
| `[limits]` | `max_files`, `max_changed_lines`, `max_minutes` |
| `[rubric] checks` | The checks to apply; all of them by default |

## Candidate

A candidate is a directory with what one attempt produced:

| File | Content |
|---|---|
| `patch.diff` | The attempt's unified diff against `base_sha` |
| `report.json` | The attempt's claims: `tests_run` (each a `command` and its `result`, `passed` or `failed`) and `unrun_checks` |
| `evidence.json` | What a runner actually executed: a list of `command`, `tree` (`base` or `candidate`) and `exit_code` |

A regression test is checked through the command
`python -m pytest -q <node id>`, run on both trees.

The patch must be what `git diff` produces. Renames, mode changes and binary
files count as changed files even without content lines, and a copy counts only its destination; a path that Git
quotes, a hunk that does not match its line counts, or a malformed report or
evidence file makes the input unusable (exit code 2) instead of being
skipped.

## Grading

```bash
python scripts/agent_eval.py grade TASK_DIR CANDIDATE_DIR --output grade.json
```

The grader reads only the three candidate files and the manifest. It runs no
model, test or network request. It grades the final patch and the recorded
evidence, never the attempt's explanation alone. Each check passes or fails
with its reasons:

| Check | Fails when |
|---|---|
| `scope` | the patch touches a file outside `[scope] allowed` |
| `limits` | the patch changes more files or lines than `[limits]` allows |
| `regression_tests` | a named test is not added by the patch, does not pass on the patched tree, or does not fail as a test on the base (pytest exit 1; a collection error or a missing test proves nothing) |
| `docs` | a `required_docs` file has no content change (a mode change does not count), or is deleted or renamed away |
| `sql_interpolation` | added Python code builds SQL with an f-string (also triple-quoted or wrapped over lines), `%` or `.format()`; whole Python string literals are read, so quotes inside SQL text do not confuse it |
| `validation_claims` | the report claims a command passed (or failed) that the evidence shows otherwise or never ran, lists a validation command as neither run nor unrun, or a validation command fails on the patched tree, whatever the report says |

The verdict passes only when every enabled check passes. The output is sorted
JSON that records the SHA-256 of every input, so grading the same candidate
again gives identical bytes. There is deliberately no single score: when an
agent is tried several times, report the distribution of verdicts and failed
checks, not a success rate.

## Self-test

```bash
make agent-eval-check
```

This grades every self-test candidate twice, checks that both grades are
identical and that each matches its `expected.json`, and runs
`tests/test_agent_eval.py`. The candidates cover a correct solution, a missing
regression test, a misleading validation claim, an unrelated-file change, SQL
built by string formatting, a test that already passes on the base, and a
missing documentation update with an unreported validation command.
