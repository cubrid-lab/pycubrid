#!/usr/bin/env python3
"""Grade an agent's patch for a pycubrid task, offline and deterministically (#925).

A task manifest (``agent-evals/tasks/<id>/task.toml``, format 1, described in
``agent-evals/README.md``) states the starting commit, the problem, the files
the change may touch, the validation commands and the regression tests that
must exist. A candidate is a directory holding what an attempt produced:

* ``patch.diff``: the unified diff against the task's base commit;
* ``report.json``: what the attempt claims it ran and did not run;
* ``evidence.json``: the commands a runner actually executed on the base and
  the patched tree, with their exit codes.

The grader reads only these files. It never runs a model, a test or the
network, and the same inputs always give byte-identical output, so a grade
can be replayed. It grades the final code and the recorded evidence, not the
attempt's explanation: a claim in ``report.json`` counts only when
``evidence.json`` backs it.

Usage::

    python scripts/agent_eval.py grade TASK_DIR CANDIDATE_DIR [--output FILE]
    python scripts/agent_eval.py self-test [--root agent-evals/selftest]

``grade`` prints the grade as JSON and exits 0 for a passing verdict, 1 for a
failing one and 2 for unusable input. ``self-test`` grades every fixture
candidate under the self-test task and compares the result with its
``expected.json``.
"""

from __future__ import annotations

import argparse
import fnmatch
import hashlib
import json
import re
import sys
import tomllib
from dataclasses import dataclass, field
from pathlib import Path

FORMAT = 1
CHECKS = ("scope", "limits", "regression_tests", "docs", "sql_interpolation", "validation_claims")
ROOT = Path(__file__).resolve().parent.parent
SELFTEST = ROOT / "agent-evals" / "selftest"


class InputError(Exception):
    """A manifest or candidate file is missing or malformed."""


@dataclass
class FileChange:
    path: str
    # Each run of consecutive added lines is one block, so a check can read an
    # expression that spans lines.
    blocks: list[list[str]] = field(default_factory=list)
    removed: int = 0
    gone: bool = False  # deleted, or renamed away: absent after the patch
    binary: bool = False

    @property
    def added(self) -> list[str]:
        return [line for block in self.blocks for line in block]


def _require(table: dict, key: str, kind: type, where: str) -> None:
    if key not in table:
        raise InputError(f"{where}: missing '{key}'")
    if not isinstance(table[key], kind):
        raise InputError(f"{where}: '{key}' must be {kind.__name__}")


def _strings(table: dict, key: str, where: str, required: bool = False) -> None:
    """``table[key]``, if present (or ``required``), is a list of strings."""
    if key not in table and not required:
        return
    _require(table, key, list, where)
    if not all(isinstance(item, str) for item in table[key]):
        raise InputError(f"{where}: '{key}' must be a list of strings")


def load_task(task_dir: Path) -> dict:
    """Read and validate ``task.toml``; return it as a plain dictionary."""
    path = task_dir / "task.toml"
    try:
        task = tomllib.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise InputError(f"{path}: not found") from exc
    except tomllib.TOMLDecodeError as exc:
        raise InputError(f"{path}: {exc}") from exc
    where = str(path)
    if task.get("format") != FORMAT:
        raise InputError(f"{where}: 'format' must be {FORMAT}")
    for key, kind in (("id", str), ("base_sha", str), ("problem", dict), ("scope", dict)):
        _require(task, key, kind, where)
    for key in ("validation", "expected"):
        _require(task, key, dict, where)
    for key in ("limits", "rubric"):
        if key in task:
            _require(task, key, dict, where)
    if not re.fullmatch(r"[0-9a-f]{40}", task["base_sha"]):
        raise InputError(f"{where}: 'base_sha' must be a full 40-character commit SHA")
    _require(task["problem"], "statement", str, f"{where} [problem]")
    _strings(task["problem"], "evidence", f"{where} [problem]", required=True)
    if not task["problem"]["evidence"]:
        raise InputError(f"{where} [problem]: 'evidence' must link the prior issue or commit")
    _strings(task["scope"], "allowed", f"{where} [scope]", required=True)
    for key in ("non_goals", "required_docs"):
        _strings(task["scope"], key, f"{where} [scope]")
    _strings(task["validation"], "commands", f"{where} [validation]", required=True)
    _strings(task["validation"], "regression_tests", f"{where} [validation]")
    for key, value in task.get("limits", {}).items():
        if key not in ("max_files", "max_changed_lines", "max_minutes"):
            raise InputError(f"{where} [limits]: unknown limit '{key}'")
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise InputError(f"{where} [limits]: '{key}' must be a non-negative integer")
    rubric = task.get("rubric", {})
    _strings(rubric, "checks", f"{where} [rubric]")
    unknown = sorted(set(rubric.get("checks", CHECKS)) - set(CHECKS))
    if unknown:
        raise InputError(f"{where} [rubric]: unknown checks {unknown}")
    return task


_HUNK = re.compile(r"^@@ -\d+(?:,(\d+))? \+\d+(?:,(\d+))? @@")
_GIT_HEADER = re.compile(r"^diff --git a/(\S+) b/(\S+)$")


def parse_diff(text: str) -> dict[str, FileChange]:
    """Return the files a unified (``git diff``) patch changes.

    ``---``/``+++`` lines count as file headers only outside a hunk, whose
    remaining line counts are tracked, so changed text that starts with
    ``++`` or ``--`` stays content. Git headers also inventory renames, mode
    changes and binary files, which have no ``---``/``+++`` lines. A path
    that Git quotes (special characters) is rejected as unsupported.
    """
    changes: dict[str, FileChange] = {}
    current: FileChange | None = None
    old_left = new_left = 0
    old_path = ""
    copy_source = ""  # a copy leaves its source unchanged, so it is not counted
    adding = False

    def touch(path: str) -> FileChange:
        return changes.setdefault(path, FileChange(path))

    for line in text.splitlines():
        if old_left > 0 or new_left > 0:
            if current is None:
                raise InputError("patch.diff: hunk without a file header")
            if line.startswith("\\"):
                continue  # "\ No newline at end of file"
            mark, body = line[:1], line[1:]
            if mark == "+" and new_left > 0:
                if not adding:
                    current.blocks.append([])
                current.blocks[-1].append(body)
                new_left, adding = new_left - 1, True
                continue
            adding = False
            if mark == "-" and old_left > 0:
                current.removed, old_left = current.removed + 1, old_left - 1
            elif mark in (" ", "") and old_left > 0 and new_left > 0:
                old_left, new_left = old_left - 1, new_left - 1
            else:
                raise InputError(f"patch.diff: line does not fit its hunk: {line!r}")
            continue
        adding = False
        if line.startswith("diff --git "):
            match = _GIT_HEADER.match(line)
            if match is None:
                raise InputError(f"patch.diff: unsupported (quoted) path in {line!r}")
            old_path, current, copy_source = match.group(1), touch(match.group(2)), ""
        elif line.startswith("rename from "):
            touch(line[len("rename from ") :]).gone = True
        elif line.startswith("rename to ") or line.startswith("copy to "):
            current = touch(line.split(" ", 2)[2])
        elif line.startswith("copy from "):
            copy_source = line[len("copy from ") :]
        elif line.startswith("deleted file mode") and current is not None:
            current.gone = True
        elif line.startswith("Binary files ") or line == "GIT binary patch":
            if current is not None:
                current.binary = True
        elif line.startswith("--- "):
            old_path = line[4:].strip()
            old_path = old_path[2:] if old_path.startswith("a/") else old_path
        elif line.startswith("+++ "):
            new_path = line[4:].strip()
            new_path = new_path[2:] if new_path.startswith("b/") else new_path
            if new_path == "/dev/null":
                current = touch(old_path)
                current.gone = True
            else:
                current = touch(new_path)
                if old_path not in ("/dev/null", new_path, copy_source):
                    touch(old_path).gone = True  # a rename
        else:
            hunk = _HUNK.match(line)
            if hunk is not None:
                if current is None:
                    raise InputError("patch.diff: hunk without a file header")
                old_left = int(hunk.group(1) or "1")
                new_left = int(hunk.group(2) or "1")
    if old_left > 0 or new_left > 0:
        raise InputError("patch.diff: the last hunk is truncated")
    return changes


def _matches(path: str, patterns: list[str]) -> bool:
    return any(fnmatch.fnmatchcase(path, pattern) for pattern in patterns)


def _result(failures: list[str], notes: list[str] | None = None) -> dict:
    return {"status": "fail" if failures else "pass", "details": failures + (notes or [])}


def check_scope(task: dict, changes: dict[str, FileChange]) -> dict:
    allowed = task["scope"]["allowed"]
    outside = sorted(path for path in changes if not _matches(path, allowed))
    return _result([f"out-of-scope change: {path}" for path in outside])


def check_limits(task: dict, changes: dict[str, FileChange]) -> dict:
    limits = task.get("limits", {})
    failures = []
    files = len(changes)
    lines = sum(len(change.added) + change.removed for change in changes.values())
    if "max_files" in limits and files > limits["max_files"]:
        failures.append(f"{files} files changed, limit {limits['max_files']}")
    if "max_changed_lines" in limits and lines > limits["max_changed_lines"]:
        failures.append(f"{lines} lines changed, limit {limits['max_changed_lines']}")
    return _result(failures, [f"{files} files, {lines} changed lines"])


def _runs(evidence: list[dict], command: str, tree: str) -> list[dict]:
    return [run for run in evidence if run["command"] == command and run["tree"] == tree]


def check_regression_tests(
    task: dict, changes: dict[str, FileChange], evidence: list[dict]
) -> dict:
    """Each named test is new in the patch, fails on the base and passes on the patch."""
    failures = []
    for node in task["validation"].get("regression_tests", []):
        path, _, name = node.partition("::")
        function = re.sub(r"\[.*\]$", "", name.split("::")[-1])
        change = changes.get(path)
        defines = change is not None and any(
            re.match(rf"\s*(async\s+)?def\s+{re.escape(function)}\s*\(", line)
            for line in change.added
        )
        if not defines:
            failures.append(f"missing regression test: {node} is not added by the patch")
            continue
        command = f"python -m pytest -q {node}"
        base, patched = _runs(evidence, command, "base"), _runs(evidence, command, "candidate")
        if not patched:
            failures.append(f"no evidence that {node} ran on the patched tree")
        elif any(run["exit_code"] != 0 for run in patched):
            failures.append(f"{node} fails on the patched tree")
        if not base:
            failures.append(f"no evidence that {node} ran on the base tree")
        elif any(run["exit_code"] == 0 for run in base):
            failures.append(f"{node} passes on the base tree, so it does not detect the defect")
        elif any(run["exit_code"] != 1 for run in base):
            # pytest exits 1 only when a collected test failed; 2 to 5 mean an
            # interruption, a usage error or no such test, which proves nothing.
            codes = sorted({run["exit_code"] for run in base})
            failures.append(f"{node} did not fail as a test on the base tree (exit {codes})")
    return _result(failures)


def check_docs(task: dict, changes: dict[str, FileChange]) -> dict:
    failures = []
    for doc in task["scope"].get("required_docs", []):
        if doc not in changes:
            failures.append(f"required documentation not updated: {doc}")
        elif changes[doc].gone:
            failures.append(f"required documentation deleted or renamed away: {doc}")
        elif not (changes[doc].blocks or changes[doc].removed or changes[doc].binary):
            failures.append(f"required documentation has no content change: {doc}")
    return _result(failures)


# SQL built by string formatting instead of ``?`` parameters (AGENTS.md: no
# f-string interpolation in SQL queries). Each block of consecutive added lines
# is scanned for complete Python string literals, so quotes inside SQL text do
# not end a string, and a triple-quoted or wrapped expression counts.
_SQL_START = re.compile(
    r"\s*(SELECT|INSERT|UPDATE|DELETE|MERGE|CREATE|DROP|ALTER|TRUNCATE|CALL)\b", re.IGNORECASE
)
_LITERAL_START = re.compile(r"""(?<![\w"'])([rRbBuUfF]{0,2})("\"\"|'''|"|')""")
_FORMATTED = re.compile(r"\s*(%(?!=)|\.format\()")
_EXECUTE = re.compile(r"\bexecute(many)?\(\s*$")


def _string_literals(text: str) -> list[tuple[int, int, str, str]]:
    """Return ``(start, end, prefix, body)`` of each Python string literal in ``text``."""
    found = []
    index = 0
    while True:
        match = _LITERAL_START.search(text, index)
        if match is None:
            return found
        prefix, quote = match.group(1).lower(), match.group(2)
        position = match.end()
        while position < len(text):
            if text[position] == "\\" and "r" not in prefix:
                position += 2
            elif text.startswith(quote, position):
                break
            elif len(quote) == 1 and text[position] == "\n":
                break  # an unterminated single-quoted string ends at the line
            else:
                position += 1
        end = min(position + len(quote), len(text))
        found.append((match.start(), end, prefix, text[match.end() : position]))
        index = end


def _unsafe_sql(text: str) -> bool:
    for start, end, prefix, body in _string_literals(text):
        is_sql = _SQL_START.match(body) is not None
        if "f" in prefix and (is_sql or _EXECUTE.search(text[:start])):
            return True
        if (is_sql or _EXECUTE.search(text[:start])) and _FORMATTED.match(text, end):
            return True
    return False


def check_sql_interpolation(changes: dict[str, FileChange]) -> dict:
    failures = []
    for path in sorted(changes):
        if not path.endswith(".py"):
            continue
        for block in changes[path].blocks:
            if _unsafe_sql("\n".join(block)):
                first = next((line.strip() for line in block if line.strip()), "")
                failures.append(f"SQL built by string formatting in {path}: {first}")
    return _result(failures)


def check_validation_claims(task: dict, report: dict, evidence: list[dict]) -> dict:
    """Every command the attempt claims it ran is backed by the runner's evidence."""
    failures = []
    claimed = report.get("tests_run", [])
    unrun = set(report.get("unrun_checks", []))
    for claim in claimed:
        command, result = claim.get("command", ""), claim.get("result", "")
        runs = _runs(evidence, command, "candidate")
        if command in unrun:
            failures.append(f"'{command}' is reported both as run and as not run")
        if not runs:
            failures.append(f"claimed '{command}' {result}, but the runner never ran it")
        elif result == "passed" and any(run["exit_code"] != 0 for run in runs):
            codes = sorted({run["exit_code"] for run in runs})
            failures.append(f"claimed '{command}' passed, but it exited {codes}")
        elif result == "failed" and all(run["exit_code"] == 0 for run in runs):
            failures.append(f"claimed '{command}' failed, but it exited 0")
    claimed_commands = {claim.get("command", "") for claim in claimed}
    for command in task["validation"]["commands"]:
        if command not in claimed_commands and command not in unrun:
            failures.append(
                f"validation command neither reported run nor reported unrun: {command}"
            )
        # The runner's result counts whatever the attempt reported, so a
        # failing command cannot be hidden by calling it unrun.
        codes = sorted({run["exit_code"] for run in _runs(evidence, command, "candidate")})
        if any(code != 0 for code in codes):
            failures.append(
                f"validation command fails on the patched tree (exit {codes}): {command}"
            )
    return _result(failures)


def _read_json(path: Path) -> object:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise InputError(f"{path}: not found") from exc
    except json.JSONDecodeError as exc:
        raise InputError(f"{path}: {exc}") from exc


def load_report(path: Path) -> dict:
    report = _read_json(path)
    if not isinstance(report, dict):
        raise InputError(f"{path}: must be an object")
    claims = report.get("tests_run", [])
    unrun = report.get("unrun_checks", [])
    if not isinstance(claims, list) or not isinstance(unrun, list):
        raise InputError(f"{path}: 'tests_run' and 'unrun_checks' must be lists")
    for claim in claims:
        if (
            not isinstance(claim, dict)
            or not isinstance(claim.get("command"), str)
            or claim.get("result") not in ("passed", "failed")
        ):
            raise InputError(
                f"{path}: each 'tests_run' entry needs 'command' and 'result' (passed|failed)"
            )
    if not all(isinstance(item, str) for item in unrun):
        raise InputError(f"{path}: 'unrun_checks' must be a list of strings")
    return report


def load_evidence(path: Path) -> list[dict]:
    runs = _read_json(path)
    if not isinstance(runs, list):
        raise InputError(f"{path}: must be a list of runs")
    for run in runs:
        if (
            not isinstance(run, dict)
            or not isinstance(run.get("command"), str)
            or run.get("tree") not in ("base", "candidate")
            or not isinstance(run.get("exit_code"), int)
            or isinstance(run.get("exit_code"), bool)
        ):
            raise InputError(
                f"{path}: each run needs 'command', 'tree' (base|candidate), 'exit_code'"
            )
    return runs


def _digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def grade(task_dir: Path, candidate_dir: Path) -> dict:
    """Return the grade of one candidate; raise ``InputError`` for unusable input."""
    task = load_task(task_dir)
    patch_path = candidate_dir / "patch.diff"
    try:
        changes = parse_diff(patch_path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise InputError(f"{patch_path}: not found") from exc
    report = load_report(candidate_dir / "report.json")
    evidence = load_evidence(candidate_dir / "evidence.json")
    enabled = task.get("rubric", {}).get("checks", list(CHECKS))
    results = {}
    for name in CHECKS:
        if name not in enabled:
            continue
        if name == "scope":
            results[name] = check_scope(task, changes)
        elif name == "limits":
            results[name] = check_limits(task, changes)
        elif name == "regression_tests":
            results[name] = check_regression_tests(task, changes, evidence)
        elif name == "docs":
            results[name] = check_docs(task, changes)
        elif name == "sql_interpolation":
            results[name] = check_sql_interpolation(changes)
        else:
            results[name] = check_validation_claims(task, report, evidence)
    inputs = {
        "task.toml": _digest(task_dir / "task.toml"),
        "patch.diff": _digest(patch_path),
        "report.json": _digest(candidate_dir / "report.json"),
        "evidence.json": _digest(candidate_dir / "evidence.json"),
    }
    verdict = "pass" if all(r["status"] == "pass" for r in results.values()) else "fail"
    return {
        "format": FORMAT,
        "task": task["id"],
        "base_sha": task["base_sha"],
        "inputs": inputs,
        "checks": results,
        "verdict": verdict,
    }


def dumps(grade_result: dict) -> str:
    """The canonical JSON text of a grade: sorted keys, so replays are byte-identical."""
    return json.dumps(grade_result, indent=2, sort_keys=True) + "\n"


def self_test(root: Path) -> list[str]:
    """Grade every candidate under ``root/candidates`` and compare with its ``expected.json``."""
    problems = []
    candidates = sorted(p for p in (root / "candidates").iterdir() if p.is_dir())
    if not candidates:
        return [f"{root}: no self-test candidates"]
    for candidate in candidates:
        first = dumps(grade(root, candidate))
        if dumps(grade(root, candidate)) != first:
            problems.append(f"{candidate.name}: two grades of the same input differ")
        result = json.loads(first)
        expected = _read_json(candidate / "expected.json")
        if not isinstance(expected, dict):
            raise InputError(f"{candidate / 'expected.json'}: must be an object")
        if result["verdict"] != expected["verdict"]:
            problems.append(
                f"{candidate.name}: verdict {result['verdict']}, expected {expected['verdict']}"
            )
        for name, status in sorted(expected.get("checks", {}).items()):
            actual = result["checks"].get(name, {}).get("status")
            if actual != status:
                problems.append(f"{candidate.name}: check {name} is {actual}, expected {status}")
        for name, needle in sorted(expected.get("details", {}).items()):
            details = result["checks"].get(name, {}).get("details", [])
            if not any(needle in detail for detail in details):
                problems.append(f"{candidate.name}: check {name} does not report '{needle}'")
    return problems


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = parser.add_subparsers(dest="command", required=True)
    grade_cmd = sub.add_parser("grade", help="grade one candidate")
    grade_cmd.add_argument("task_dir", type=Path)
    grade_cmd.add_argument("candidate_dir", type=Path)
    grade_cmd.add_argument("--output", type=Path, help="also write the grade to this file")
    test_cmd = sub.add_parser("self-test", help="check the grader against its fixtures")
    test_cmd.add_argument("--root", type=Path, default=SELFTEST)
    args = parser.parse_args(argv)
    try:
        if args.command == "self-test":
            problems = self_test(args.root)
            for problem in problems:
                print(f"FAIL: {problem}")
            count = len([p for p in (args.root / "candidates").iterdir() if p.is_dir()])
            print("OK" if not problems else "FAILED", f"({count} self-test candidates)")
            return 1 if problems else 0
        text = dumps(grade(args.task_dir, args.candidate_dir))
    except InputError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    if args.output:
        args.output.write_text(text, encoding="utf-8")
    sys.stdout.write(text)
    return 0 if json.loads(text)["verdict"] == "pass" else 1


if __name__ == "__main__":
    sys.exit(main())
