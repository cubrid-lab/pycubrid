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
| `tasks/<id>/task.toml` | A real task, rebuilt from an earlier pycubrid issue and its merged fix |
| `tasks/<id>/reference.diff` | That merged fix, `git diff --no-renames <base_sha> <reference_fix>`: the oracle |
| `../scripts/agent_eval.py` | The offline grader |

The opt-in runner that applies an attempt in a pinned worktree is added in a
later pull request of #925.

## Tasks

| Task | From | What it measures |
|---|---|---|
| `reply-bounds-383` | #383, #533 | A malformed reply length must close the session without reading past the frame |
| `lob-generation-932` | #932, #934 | A LOB handle of a replaced physical session must fail before anything is sent |
| `lab-volume-964` | #964, #966 | A documentation fix, checked by `acceptance.py`, whose macOS check cannot run here and must be reported as not run |

Each task's regression tests were checked against its reference: on the base
commit plus the reference's test files each one exits 1, and on the reference
fix each one passes. `tests/test_agent_eval.py` checks that every task loads and
that its reference passes the scope, limits, documentation and SQL checks and
defines its regression tests.

`lab-volume-964` has no pytest test, so `acceptance.py` checks the tutorial
pages instead: every `docker rm` of the lab forces the removal and passes `-v`
(bundled forms such as `-vf` count), each page still shows
the command, and each Finish Safely page explains `-v` with the volume. It
exits 1 on the base and 0 on the fix, so a patch that only touches the
required documents fails. The wording itself stays a human review action.

`lob-generation-932` turns `sql_interpolation` off in its `[rubric]`: the merged
fix formats a generated table name into DDL with `%`, following its test
file, and the scanner cannot tell an identifier from a value.

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
| `[validation] acceptance` | Scripts in the task directory that check the result where no pytest test fits, such as documentation text |
| `[validation] manual` | Checks only a person can do, such as a live server or another operating system; the attempt must list them as not run |
| `[expected] behavior` | The reference behavior a correct change produces |
| `[expected] reference_fix` | Optional: the full SHA of the merged fix the task was built from |
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
`python -m pytest -q <node id>`, run on both trees. The base tree is
`base_sha` with the candidate's test files applied, so a new test exists there
and has to fail; the candidate tree is `base_sha` with the whole patch.
An acceptance script is run as `python <task dir>/<script> <tree>` on both
trees and recorded in the evidence under the script name; it exits 1 when it
finds the defect and 0 when it does not.

The patch must be what `git diff` produces. Renames, mode changes and binary
files count as changed files even without content lines, and a copy counts only its destination; a path that Git
quotes, a hunk that does not match its line counts, or a malformed report or
evidence file makes the input unusable (exit code 2) instead of being
skipped.

## Grading

```bash
python scripts/agent_eval.py grade TASK_DIR CANDIDATE_DIR --output grade.json
```

With `--output` the grade goes to the file and only the verdict (`pass` or
`fail`) is printed; without it the grade is printed.

The grader reads only the three candidate files and the manifest. It runs no
model, test or network request. It grades the final patch and the recorded
evidence, never the attempt's explanation alone. Each check passes or fails
with its reasons:

| Check | Fails when |
|---|---|
| `scope` | the patch touches a file outside `[scope] allowed` |
| `limits` | the patch changes more files or lines than `[limits]` allows |
| `regression_tests` | a named test is not added by the patch, does not pass on the patched tree, or does not fail as a test on the base (pytest exit 1; a collection error or a missing test proves nothing); or an acceptance script did not exit 1 on the base and 0 on the patched tree |
| `docs` | a `required_docs` file has no content change (a mode change does not count), or is deleted or renamed away |
| `sql_interpolation` | added Python code builds SQL with an f-string (also triple-quoted or wrapped over lines), `%` or `.format()`; whole Python string literals are read, so quotes inside SQL text do not confuse it |
| `validation_claims` | the report claims a command passed (or failed) that the evidence shows otherwise or never ran, lists a validation command as neither run nor unrun, or a validation command fails on the patched tree, whatever the report says; or a manual check is claimed as run or not listed as not run |

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
