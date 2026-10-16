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
    added: list[str] = field(default_factory=list)
    removed: int = 0


def _require(table: dict, key: str, kind: type, where: str) -> None:
    if key not in table:
        raise InputError(f"{where}: missing '{key}'")
    if not isinstance(table[key], kind):
        raise InputError(f"{where}: '{key}' must be {kind.__name__}")


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
    if not re.fullmatch(r"[0-9a-f]{40}", task["base_sha"]):
        raise InputError(f"{where}: 'base_sha' must be a full 40-character commit SHA")
    _require(task["problem"], "statement", str, f"{where} [problem]")
    _require(task["problem"], "evidence", list, f"{where} [problem]")
    if not task["problem"]["evidence"]:
        raise InputError(f"{where} [problem]: 'evidence' must link the prior issue or commit")
    _require(task["scope"], "allowed", list, f"{where} [scope]")
    _require(task["validation"], "commands", list, f"{where} [validation]")
    checks = task.get("rubric", {}).get("checks", list(CHECKS))
    unknown = sorted(set(checks) - set(CHECKS))
    if unknown:
        raise InputError(f"{where} [rubric]: unknown checks {unknown}")
    return task


def parse_diff(text: str) -> dict[str, FileChange]:
    """Return the files a unified diff changes, with their added lines."""
    changes: dict[str, FileChange] = {}
    current: FileChange | None = None
    old_path = ""
    for line in text.splitlines():
        if line.startswith("diff --git "):
            current = None
        elif line.startswith("--- "):
            old_path = line[4:].strip()
            old_path = old_path[2:] if old_path.startswith("a/") else old_path
        elif line.startswith("+++ "):
            new_path = line[4:].strip()
            new_path = new_path[2:] if new_path.startswith("b/") else new_path
            path = old_path if new_path == "/dev/null" else new_path
            current = changes.setdefault(path, FileChange(path))
            if old_path not in ("/dev/null", path):
                changes.setdefault(old_path, FileChange(old_path))  # a rename touches both
        elif current is not None and line.startswith("+"):
            current.added.append(line[1:])
        elif current is not None and line.startswith("-"):
            current.removed += 1
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
    return _result(failures)


def check_docs(task: dict, changes: dict[str, FileChange]) -> dict:
    missing = [doc for doc in task["scope"].get("required_docs", []) if doc not in changes]
    return _result([f"required documentation not updated: {doc}" for doc in missing])


# SQL built by string formatting instead of ``?`` parameters (AGENTS.md: no
# f-string interpolation in SQL queries).
_SQL_WORD = r"(SELECT|INSERT|UPDATE|DELETE|MERGE|CREATE|DROP|ALTER|TRUNCATE|CALL)\b"
_UNSAFE_SQL = (
    re.compile(r"""\bexecute(many)?\(\s*[rbu]*f["']""", re.IGNORECASE),
    re.compile(rf"""\b[rbu]*f["']\s*{_SQL_WORD}""", re.IGNORECASE),
    re.compile(rf"""["']\s*{_SQL_WORD}[^"']*["']\s*(%|\.format\()""", re.IGNORECASE),
    re.compile(r"""\bexecute(many)?\([^#]*["']\s*(%\s|\.format\()""", re.IGNORECASE),
)


def check_sql_interpolation(changes: dict[str, FileChange]) -> dict:
    failures = []
    for path in sorted(changes):
        if not path.endswith(".py"):
            continue
        for line in changes[path].added:
            if any(pattern.search(line) for pattern in _UNSAFE_SQL):
                failures.append(f"SQL built by string formatting in {path}: {line.strip()}")
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
    return _result(failures)


def _read_json(path: Path) -> object:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise InputError(f"{path}: not found") from exc
    except json.JSONDecodeError as exc:
        raise InputError(f"{path}: {exc}") from exc


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
    report = _read_json(candidate_dir / "report.json")
    if not isinstance(report, dict):
        raise InputError(f"{candidate_dir / 'report.json'}: must be an object")
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
