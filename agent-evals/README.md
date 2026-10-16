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
| `../scripts/agent_eval_run.py` | The opt-in runner that prepares a pinned checkout and collects an attempt |

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

## Running an attempt

The runner is opt-in and runs no model: a person, or a tool they control,
makes the attempt between its two steps. Ordinary CI never runs it.

```bash
python scripts/agent_eval_run.py prepare agent-evals/tasks/reply-bounds-383 /tmp/attempt-1
# The attempt works in /tmp/attempt-1/worktree, following /tmp/attempt-1/prompt.md,
# leaves its changes uncommitted and writes /tmp/attempt-1/report.json.
python scripts/agent_eval_run.py collect agent-evals/tasks/reply-bounds-383 /tmp/attempt-1 --meta meta.json
python scripts/agent_eval_run.py summarize /tmp/attempt-*
```

`prepare` makes `worktree/`, a new repository holding only the task's base
commit (a depth-1 fetch, so the reference fix and later history are not
there), and `prompt.md`, the problem, scope and validation without the
expected behavior, the reference fix or the evidence links.

`collect` takes the attempt's uncommitted changes, new files included, as
`candidate/patch.diff` and builds two trees from `git archive` of the base: the
base tree with the patch's `tests/` files and the candidate tree with the
whole patch. A patch that does not apply is unusable input. It then runs the
validation commands on the candidate tree, and the regression tests and
acceptance scripts on both, and records their exit codes in
`candidate/evidence.json`, with one log per command under `logs/`. It grades
the candidate (`grade.json`) and writes `run.json`. A failed `collect` writes
nothing into `OUT_DIR`, so it can run again.

`collect` takes the task directory, and the source repository (`--repo`, by
default this checkout), from the caller and never from `OUT_DIR`, which the
attempt can write: `attempt.json` only has to agree with them (the same
`task.toml` digest and base commit), so editing it cannot change which task
is graded. The runner assumes that no attempt process is still running when
`collect` starts; stop the agent tool first.

The optional `--meta` file holds what the operator observed: `model`,
`model_version`, `tool`, `tool_version`, `prompt_id` (default: the task ID),
`seed`, `tokens`, `cost` and `human_review_actions`. Anything missing is
recorded as `"unknown"`, never guessed. `run.json` adds the base commit, the
task digest, the attempt's duration and whether it went over `max_minutes`,
whether the attempt moved `HEAD` (read from `.git/HEAD`), the verdict and failed checks, the commands
that passed, the out-of-scope edits, the regression escapes (regression-test
and acceptance failures), the human review actions including every manual
check, the Python version, the environment variable names, the sandbox mode
and the SHA-256 of every runner output (`attempt.json`, `prompt.md`,
`report.json`, `grade.json`, `candidate/` and `logs/`; the checkout and kept
trees hold attempt-controlled files and are never hashed). Grading the same artifacts again gives the
same `grade.json`.

`summarize` reports, per task, how many attempts there were and the
distribution of verdicts, failed checks and models. There is no single
success rate, and nothing is scored in CI.

### Sandbox

- Each check, and the import check before it, runs in new user, mount, PID
  and network namespaces (`unshare`) inside a private root. That root holds
  the system directories (`/usr`, `/bin`, `/lib`, `/etc` and their siblings)
  and the interpreter's prefixes, the task directory and the `python`
  wrapper read-only (non-recursive binds, so no submount stays writable), a
  fresh 512 MB `/tmp`, a minimal `/dev` and `/proc`, and the tree under test
  as the only writable path. The caller's home directory
  (credential files included), host sockets under `/run`, the other tree and
  the runner's own artifacts are not visible, and there is no network.
  Before the check starts, every capability of the namespace root is dropped
  (`setpriv`, empty bounding set, no new privileges), so it can neither
  `chroot` out of the root nor change its mounts. `prlimit` caps the
  address space at 8 GiB, a written file at 2 GiB and the number of
  processes at the caller's current count plus 1024. These limits are per
  process and per file: the total memory and disk a check uses is bounded
  only by `--timeout` and the 512 MB `/tmp`. Where systemd is available, run
  `collect` under `systemd-run --user --scope -p MemoryMax=... -p TasksMax=...`
  for a total bound.
- An interpreter prefix is bound only when it does not expose the home
  directory: a prefix that is or contains the home directory is refused,
  and one inside it must be a virtual environment or a known interpreter
  install (uv, pyenv, mise, asdf, rye, hatch). The check protects the home
  directory only, so do not point `--python` at an interpreter whose prefix
  is a broad directory such as `/opt` or `/srv` that holds other data.
- Where the sandbox cannot be built (no `unshare`, or unprivileged user
  namespaces are disabled), `collect` refuses to run unless `--no-sandbox` is
  passed, and `run.json` records `"sandbox": "none"` instead of
  `"namespaces"`.
- Commands get only an allowlisted environment (`LANG`, `LC_ALL`, `TERM`,
  `TZ`, a `PATH` with the chosen interpreter, `HOME=/tmp`, `PYTHONPATH` set to
  the tree, `PIP_NO_INDEX=1` and an unusable proxy). Release tokens, deploy
  credentials and `CUBRID_TEST_*` settings of the calling shell do not reach
  them, so live integration tests skip.
- Logs are redacted: credential-shaped text (GitHub and PyPI tokens, AWS
  access keys, `Authorization` headers, `password=`-style values), the values
  of the caller's secret-named variables and the home directory are replaced.
- Each command, and the import check, has a timeout (`--timeout`, 900 seconds
  by default). A command that times out is recorded as exit code 124, and its
  whole process group (all PID-namespace processes, in the sandbox) is
  killed. An import check that times out makes the input unusable.
- Git never runs in the attempt's checkout, whose configuration the attempt
  controls (`core.fsmonitor`, filters, external diff and textconv drivers).
  `collect` copies its files, without `.git`, over a fresh depth-1 clone of
  the base and diffs them there with `--no-ext-diff --no-textconv`. Every
  host-side Git call ignores the caller's global and system configuration
  and attributes (`GIT_CONFIG_GLOBAL=/dev/null`, `GIT_CONFIG_NOSYSTEM`,
  `GIT_ATTR_NOSYSTEM`), so an attempt cannot select a filter driver, such as
  Git LFS, that the caller configured.
- The attempt can write in `OUT_DIR`, so `collect` keeps all its scratch
  (trees, the clone, the wrapper directory, the sandbox root and the outputs
  while they are built and graded) in a new private temporary directory. It
  opens `OUT_DIR`, `worktree` and `worktree/.git` as held directory
  descriptors without following links, reads `attempt.json`, `report.json`
  and `.git/HEAD` only as size-capped regular files (a symlink or a FIFO is
  unusable input), copies the checkout through its held descriptor and
  refuses it if `worktree` was replaced meanwhile, copies only regular files
  and symlinks (as links) from it, refuses an existing `candidate`, `logs`,
  `grade.json` or `run.json`, and writes those through held descriptors,
  creating each entry without following or replacing anything. These guard
  against planted links, not against a process that is still running and
  racing the runner, which the runner assumes there is none of.
- The trees are built outside any repository, so `git apply` applies every
  path even when `OUT_DIR` is inside a checkout.
- The runner never commits, pushes, merges or publishes. Its scratch is
  deleted after the run; `--keep-trees` keeps the trees and records their
  path as `kept_trees` in `run.json`.
- With `--no-sandbox`, a process that leaves the command's process group
  (`setsid`) survives the timeout; the runner stops reading its output
  after 10 seconds instead of waiting, but the process may keep running.
- The attempt's own sandbox, such as what the agent tool may run or reach
  while it works, is the operator's responsibility; the runner isolates only
  the checks it runs.

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
`tests/test_agent_eval.py` and `tests/test_agent_eval_run.py`. The candidates
cover a correct solution, a missing regression test, a misleading validation
claim, an unrelated-file change, SQL built by string formatting, a test that
already passes on the base, and a missing documentation update with an
unreported validation command.
