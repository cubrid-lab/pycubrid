#!/usr/bin/env python3
"""Run one agent evaluation attempt in a pinned, isolated checkout (#925).

This is the opt-in runner for the tasks under ``agent-evals/tasks/``. It never
runs a model. A person, or a tool they control, makes the attempt between
``prepare`` and ``collect``:

``prepare TASK_DIR OUT_DIR``
    Makes ``OUT_DIR/worktree``, a fresh repository that holds only the task's
    base commit (a depth-1 fetch, so the fix and later history are absent),
    and writes ``OUT_DIR/prompt.md`` with the problem, the scope and the
    validation the attempt must run or report. The prompt leaves out the
    expected behavior, the reference fix and the evidence links.

``collect OUT_DIR [--report REPORT] [--meta META]``
    Takes the attempt's uncommitted changes as ``patch.diff``, builds the base
    tree (base commit plus the patch's ``tests/`` files) and the candidate tree
    (base commit plus the whole patch) from ``git archive``, runs the task's
    validation commands, regression tests and acceptance scripts there in a
    reduced environment without network access, writes ``evidence.json`` and
    redacted logs, grades the candidate with ``scripts/agent_eval.py`` and
    writes ``run.json``. It exits 0 for a passing verdict, 1 for a failing one
    and 2 for unusable input.

``summarize OUT_DIR...``
    Reports repeated attempts as distributions of verdicts and failed checks
    per task, never as one success rate.

The runner never commits, pushes, merges or publishes anything. Commands get
only an allowlisted environment, so release tokens and credentials of the
calling shell do not reach them, and they run in a network namespace without
network access (``unshare --net``) unless ``--allow-network`` is given, which
``run.json`` records.
"""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import os
import re
import shlex
import shutil
import subprocess
import sys
import tarfile
import time
from datetime import UTC, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import agent_eval  # noqa: E402 - the grader next to this script
from agent_eval import InputError  # noqa: E402

RUNNER_VERSION = 1
ROOT = agent_eval.ROOT
UNKNOWN = "unknown"
META_KEYS = (
    "model",
    "model_version",
    "tool",
    "tool_version",
    "prompt_id",
    "seed",
    "tokens",
    "cost",
    "human_review_actions",
)
PASSED_ENV = ("LANG", "LC_ALL", "TERM", "TZ")
SECRET_NAME = re.compile(r"TOKEN|SECRET|PASSWORD|PASSWD|CREDENTIAL|API_?KEY|PRIVATE", re.I)
SECRET_TEXT = (
    re.compile(r"\bgh[pousr]_[A-Za-z0-9]{20,}"),
    re.compile(r"\bgithub_pat_[A-Za-z0-9_]{20,}"),
    re.compile(r"\bpypi-[A-Za-z0-9_-]{20,}"),
    re.compile(r"\bAKIA[0-9A-Z]{16}\b"),
    re.compile(r"(?i)(authorization:\s*(?:bearer|token|basic)\s+)\S+"),
    re.compile(r"(?i)\b((?:token|password|passwd|secret|api_?key)\s*[=:]\s*)\S+"),
)
ISOLATE = ("unshare", "--net", "--map-root-user")


def _git(*args: str, cwd: Path | None = None) -> str:
    result = subprocess.run(["git", *args], cwd=cwd, capture_output=True, text=True, check=False)
    if result.returncode != 0:
        raise InputError(f"git {' '.join(args)}: {result.stderr.strip()}")
    return result.stdout


def _digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _now() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def redact(text: str, environ: dict[str, str] | None = None) -> str:
    """Hide credential-shaped text and the values of secret-named variables."""
    environ = os.environ if environ is None else environ
    for name, value in environ.items():
        if SECRET_NAME.search(name) and len(value) >= 8:
            text = text.replace(value, f"[REDACTED {name}]")
    for pattern in SECRET_TEXT:
        text = pattern.sub(lambda m: (m.group(1) if m.groups() else "") + "[REDACTED]", text)
    return text


def prompt(task: dict, report_path: str) -> str:
    """The task as the attempt receives it, without the oracle."""
    validation = task["validation"]

    def items(values: list[str]) -> str:
        return "".join(f"- `{value}`\n" for value in values) or "- none\n"

    return (
        f"# {task.get('title', task['id'])}\n\n"
        f"Task `{task['id']}`, base commit `{task['base_sha']}`.\n\n"
        f"## Problem\n\n{task['problem']['statement'].strip()}\n\n"
        f"## Files you may change\n\n{items(task['scope']['allowed'])}\n"
        f"## Non-goals\n\n{items(task['scope'].get('non_goals', []))}\n"
        f"## Documentation to update\n\n{items(task['scope'].get('required_docs', []))}\n"
        f"## Validation\n\nRun these commands, or report them as not run:\n\n"
        f"{items(validation['commands'])}\n"
        f"Add these regression tests; each must fail before your change:\n\n"
        f"{items(validation.get('regression_tests', []))}\n"
        f"These checks need a person; report them as not run:\n\n"
        f"{items(validation.get('manual', []))}\n"
        "## Finishing\n\n"
        "Leave your changes uncommitted in this checkout. Do not commit, push or "
        f"publish. Write `{report_path}` as JSON: `tests_run` (each a `command` "
        "and its `result`, `passed` or `failed`) and `unrun_checks` (strings).\n"
    )


def prepare(task_dir: Path, out_dir: Path, repo: Path = ROOT) -> Path:
    """Make the pinned checkout and the prompt; return the checkout."""
    task = agent_eval.load_task(task_dir)
    if out_dir.exists() and any(out_dir.iterdir()):
        raise InputError(f"{out_dir}: must be new or empty")
    worktree = out_dir / "worktree"
    worktree.mkdir(parents=True)
    _git("init", "-q", str(worktree))
    _git("fetch", "-q", "--depth", "1", repo.resolve().as_uri(), task["base_sha"], cwd=worktree)
    _git("checkout", "-q", "--detach", task["base_sha"], cwd=worktree)
    report = out_dir.resolve() / "report.json"
    (out_dir / "prompt.md").write_text(prompt(task, str(report)), encoding="utf-8")
    attempt = {
        "format": agent_eval.FORMAT,
        "task_dir": str(task_dir.resolve()),
        "task_digest": _digest(task_dir / "task.toml"),
        "base_sha": task["base_sha"],
        "prepared_at": _now(),
        "prepared_epoch": time.time(),
    }
    (out_dir / "attempt.json").write_text(json.dumps(attempt, indent=2) + "\n", encoding="utf-8")
    return worktree


def _patch(worktree: Path, base_sha: str) -> str:
    """The attempt's changes against the base, including new files, as ``git diff`` text."""
    untracked = _git("ls-files", "--others", "--exclude-standard", "-z", cwd=worktree)
    names = [name for name in untracked.split("\0") if name]
    if names:
        _git("add", "--intent-to-add", "--", *names, cwd=worktree)
    return _git("diff", "--no-color", "--no-renames", base_sha, cwd=worktree)


def _sections(patch: str, keep: str) -> str:
    """The file sections of ``patch`` whose path starts with ``keep``."""
    parts = re.split(r"(?m)^(?=diff --git )", patch)
    return "".join(part for part in parts if part.startswith(f"diff --git a/{keep}"))


def _tree(worktree: Path, base_sha: str, dest: Path, patch: str) -> None:
    archive = subprocess.run(
        ["git", "archive", "--format=tar", base_sha], cwd=worktree, capture_output=True, check=True
    ).stdout
    dest.mkdir(parents=True)
    with tarfile.open(fileobj=io.BytesIO(archive)) as tar:
        # The archive is the repository's own base commit; the data filter,
        # where this Python has it, also refuses links that leave dest.
        if hasattr(tarfile, "data_filter"):
            tar.extractall(dest, filter="data")
        else:  # pragma: no cover - Python 3.11 before 3.11.4
            tar.extractall(dest)  # noqa: S202
    if patch:
        applied = subprocess.run(
            ["git", "apply", "--whitespace=nowarn", "-"],
            cwd=dest,
            input=patch.encode(),
            capture_output=True,
            check=False,
        )
        if applied.returncode != 0:
            raise InputError(f"the patch does not apply to {dest.name}: {applied.stderr.decode()}")


def _environment(home: Path, tree: Path, python: Path) -> dict[str, str]:
    bin_dir = home / "bin"
    bin_dir.mkdir(parents=True, exist_ok=True)
    # A wrapper, not a symlink: a virtual environment's interpreter finds its
    # environment from the path it is started by.
    for name in ("python", "python3"):
        wrapper = bin_dir / name
        wrapper.write_text(f'#!/bin/sh\nexec {shlex.quote(str(python))} "$@"\n', encoding="utf-8")
        wrapper.chmod(0o755)
    env = {name: os.environ[name] for name in PASSED_ENV if name in os.environ}
    env.update(
        {
            "PATH": f"{bin_dir}:/usr/local/bin:/usr/bin:/bin",
            "HOME": str(home),
            "PYTHONPATH": str(tree),
            "PYTHONDONTWRITEBYTECODE": "1",
            "PIP_NO_INDEX": "1",
            "http_proxy": "http://127.0.0.1:9",
            "https_proxy": "http://127.0.0.1:9",
        }
    )
    return env


def _isolation(allow_network: bool) -> tuple[str, ...]:
    if allow_network:
        return ()
    probe = subprocess.run([*ISOLATE, "true"], capture_output=True, check=False)
    if probe.returncode != 0:
        raise InputError(
            "network isolation (unshare --net --map-root-user) is unavailable here; "
            "pass --allow-network to run without it, which run.json records"
        )
    return ISOLATE


def collect(
    out_dir: Path,
    report_path: Path | None = None,
    meta_path: Path | None = None,
    python: Path = Path(sys.executable),
    timeout: int = 900,
    allow_network: bool = False,
    keep_trees: bool = False,
) -> dict:
    """Run the attempt's checks and grade it; return ``run.json``."""
    started = time.time()
    attempt = agent_eval._read_json(out_dir / "attempt.json")
    if not isinstance(attempt, dict) or attempt.get("format") != agent_eval.FORMAT:
        raise InputError(f"{out_dir}/attempt.json: not made by prepare")
    task_dir = Path(attempt["task_dir"])
    if _digest(task_dir / "task.toml") != attempt["task_digest"]:
        raise InputError(f"{task_dir}/task.toml changed after prepare")
    task = agent_eval.load_task(task_dir)
    meta = load_meta(meta_path)
    report = agent_eval.load_report(report_path or out_dir / "report.json")
    worktree = out_dir / "worktree"
    base_sha = task["base_sha"]
    head = _git("rev-parse", "HEAD", cwd=worktree).strip()
    prefix = _isolation(allow_network)

    candidate = out_dir / "candidate"
    if candidate.exists():
        raise InputError(f"{candidate}: collect already ran for this attempt")
    candidate.mkdir()
    trees, logs, home = out_dir / "trees", out_dir / "logs", out_dir.resolve() / "home"
    try:
        patch = _patch(worktree, base_sha)
        (candidate / "patch.diff").write_text(patch, encoding="utf-8")
        (candidate / "report.json").write_text(
            json.dumps(report, indent=2) + "\n", encoding="utf-8"
        )

        _tree(worktree, base_sha, trees / "base", _sections(patch, "tests/"))
        _tree(worktree, base_sha, trees / "candidate", patch)
        logs.mkdir()
        validation = task["validation"]
        plan: list[tuple[str, str, list[str]]] = []
        for command in validation["commands"]:
            plan.append(("candidate", command, shlex.split(command)))
        for tree_name in ("base", "candidate"):
            for node in validation.get("regression_tests", []):
                command = f"python -m pytest -q {node}"
                plan.append((tree_name, command, shlex.split(command)))
            for script in validation.get("acceptance", []):
                tree = str((trees / tree_name).resolve())
                plan.append((tree_name, script, ["python", str(task_dir / script), tree]))

        evidence, timings = [], []
        checked: set[str] = set()
        env: dict[str, str] = {}
        for number, (tree_name, command, argv) in enumerate(plan, start=1):
            tree = (trees / tree_name).resolve()
            env = _environment(home, tree, python)
            if tree_name not in checked:
                _check_import(tree, env, prefix)
                checked.add(tree_name)
            began = time.time()
            try:
                run = subprocess.run(
                    [*prefix, *argv],
                    cwd=tree,
                    env=env,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    timeout=timeout,
                    check=False,
                )
                code, output = run.returncode, run.stdout.decode(errors="replace")
            except subprocess.TimeoutExpired as exc:
                code = 124
                output = (exc.stdout or b"").decode(errors="replace") + f"\n[timeout {timeout}s]\n"
            except FileNotFoundError as exc:
                code, output = 127, f"{exc}\n"
            log = logs / f"{number:02d}-{tree_name}.log"
            log.write_text(
                redact(f"$ {command}\n{output}\n[exit {code}]\n").replace(str(Path.home()), "~"),
                encoding="utf-8",
            )
            evidence.append({"command": command, "tree": tree_name, "exit_code": code})
            timings.append({"log": log.name, "seconds": round(time.time() - began, 1)})
        (candidate / "evidence.json").write_text(
            json.dumps(evidence, indent=2) + "\n", encoding="utf-8"
        )
    except BaseException:
        # Leave the attempt as prepare left it, so collect can run again.
        for path in (candidate, trees, logs, home):
            shutil.rmtree(path, ignore_errors=True)
        raise
    if not keep_trees:
        shutil.rmtree(trees)

    grade = agent_eval.grade(task_dir, candidate)
    (out_dir / "grade.json").write_text(agent_eval.dumps(grade), encoding="utf-8")
    minutes = round((started - attempt["prepared_epoch"]) / 60, 1)
    max_minutes = task.get("limits", {}).get("max_minutes")
    checks = grade["checks"]
    result = {
        "format": RUNNER_VERSION,
        "task": task["id"],
        "task_digest": attempt["task_digest"],
        "base_sha": base_sha,
        **meta,
        "prompt_id": meta["prompt_id"] if meta["prompt_id"] != UNKNOWN else task["id"],
        "prepared_at": attempt["prepared_at"],
        "collected_at": _now(),
        "attempt_minutes": minutes,
        "over_max_minutes": max_minutes is not None and minutes > max_minutes,
        "network": "allowed" if allow_network else "isolated",
        "environment": sorted(env),
        "python": subprocess.run(
            [str(python), "-c", "import sys; print(sys.version.split()[0])"],
            capture_output=True,
            text=True,
            check=False,
        ).stdout.strip(),
        "head_moved": head != base_sha,
        "verdict": grade["verdict"],
        "failed_checks": sorted(
            name for name, check in checks.items() if check["status"] != "pass"
        ),
        "passing_commands": sorted(
            {
                run["command"]
                for run in evidence
                if run["tree"] == "candidate" and run["exit_code"] == 0
            }
        ),
        "out_of_scope_edits": checks.get("scope", {}).get("details", []),
        "regression_escapes": checks.get("regression_tests", {}).get("details", []),
        "human_review_actions": [
            *meta["human_review_actions"],
            *(f"manual check: {check}" for check in validation.get("manual", [])),
        ],
        "timings": timings,
    }
    shutil.rmtree(out_dir / "home", ignore_errors=True)
    result["artifacts"] = {
        str(path.relative_to(out_dir)): _digest(path)
        for path in sorted(out_dir.rglob("*"))
        if path.is_file() and worktree not in path.parents and path.name != "run.json"
    }
    (out_dir / "run.json").write_text(
        json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return result


def _check_import(tree: Path, env: dict[str, str], prefix: tuple[str, ...]) -> None:
    """Fail when the tree's own ``pycubrid`` would be shadowed by an installed one."""
    if not (tree / "pycubrid" / "__init__.py").is_file():
        return
    found = subprocess.run(
        [*prefix, "python", "-c", "import pycubrid; print(pycubrid.__file__)"],
        cwd=tree,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    ).stdout.strip()
    if not found or tree not in Path(found).resolve().parents:
        raise InputError(f"pycubrid imports from {found or 'nowhere'}, not from {tree}")


def load_meta(path: Path | None) -> dict:
    """What the operator observed about the attempt; anything missing is ``unknown``."""
    meta = {} if path is None else agent_eval._read_json(path)
    if not isinstance(meta, dict):
        raise InputError(f"{path}: must be an object")
    unknown = sorted(set(meta) - set(META_KEYS))
    if unknown:
        raise InputError(f"{path}: unknown keys {unknown}")
    actions = meta.get("human_review_actions", [])
    if not isinstance(actions, list) or not all(isinstance(a, str) for a in actions):
        raise InputError(f"{path}: 'human_review_actions' must be a list of strings")
    for key in ("tokens", "cost"):
        value = meta.get(key, UNKNOWN)
        if value != UNKNOWN and (isinstance(value, bool) or not isinstance(value, (int, float))):
            raise InputError(f"{path}: '{key}' must be a number or \"unknown\"")
    result = {key: meta.get(key, UNKNOWN) for key in META_KEYS}
    result["human_review_actions"] = actions
    return result


def summarize(out_dirs: list[Path]) -> dict:
    """Verdict and failed-check distributions per task over repeated attempts."""
    tasks: dict[str, dict] = {}
    for out_dir in out_dirs:
        run = agent_eval._read_json(out_dir / "run.json")
        if not isinstance(run, dict) or "task" not in run or "verdict" not in run:
            raise InputError(f"{out_dir}/run.json: not made by collect")
        entry = tasks.setdefault(
            run["task"], {"attempts": 0, "verdicts": {}, "failed_checks": {}, "models": {}}
        )
        entry["attempts"] += 1
        entry["verdicts"][run["verdict"]] = entry["verdicts"].get(run["verdict"], 0) + 1
        for name in run.get("failed_checks", []):
            entry["failed_checks"][name] = entry["failed_checks"].get(name, 0) + 1
        model = str(run.get("model", UNKNOWN))
        entry["models"][model] = entry["models"].get(model, 0) + 1
    return {"format": RUNNER_VERSION, "tasks": tasks}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = parser.add_subparsers(dest="command", required=True)
    prep = sub.add_parser("prepare", help="make the pinned checkout and the prompt")
    prep.add_argument("task_dir", type=Path)
    prep.add_argument("out_dir", type=Path)
    col = sub.add_parser("collect", help="run the checks and grade the attempt")
    col.add_argument("out_dir", type=Path)
    col.add_argument("--report", type=Path, help="default: OUT_DIR/report.json")
    col.add_argument("--meta", type=Path)
    col.add_argument("--python", type=Path, default=Path(sys.executable))
    col.add_argument("--timeout", type=int, default=900, help="seconds per command")
    col.add_argument("--allow-network", action="store_true")
    col.add_argument("--keep-trees", action="store_true")
    summ = sub.add_parser("summarize", help="distributions over repeated attempts")
    summ.add_argument("out_dirs", type=Path, nargs="+")
    args = parser.parse_args(argv)
    try:
        if args.command == "prepare":
            print(prepare(args.task_dir, args.out_dir))
            return 0
        if args.command == "summarize":
            print(json.dumps(summarize(args.out_dirs), indent=2, sort_keys=True))
            return 0
        result = collect(
            args.out_dir,
            args.report,
            args.meta,
            args.python,
            args.timeout,
            args.allow_network,
            args.keep_trees,
        )
    except InputError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    print(result["verdict"])
    return 0 if result["verdict"] == "pass" else 1


if __name__ == "__main__":
    sys.exit(main())
