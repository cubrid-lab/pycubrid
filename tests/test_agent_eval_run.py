"""The opt-in agent-evaluation runner (#925)."""

from __future__ import annotations

import importlib.util
import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

import pytest

pytestmark = pytest.mark.repo_tooling

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
_SPEC = importlib.util.spec_from_file_location("agent_eval_run", ROOT / "scripts/agent_eval_run.py")
assert _SPEC is not None and _SPEC.loader is not None
runner = importlib.util.module_from_spec(_SPEC)
sys.modules["agent_eval_run"] = runner
_SPEC.loader.exec_module(runner)
InputError = runner.InputError

TEST = "def test_add():\n    from calc import add\n\n    assert add(2, 3) == 5\n"


def _git(repo: Path, *args: str) -> str:
    return subprocess.run(
        ["git", "-c", "user.name=t", "-c", "user.email=t@example.com", *args],
        cwd=repo,
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()


@pytest.fixture
def project(tmp_path: Path) -> tuple[Path, Path, str]:
    """A tiny repository with a bug, a later fix commit, and a task for it."""
    repo = tmp_path / "repo"
    (repo / "tests").mkdir(parents=True)
    (repo / "calc.py").write_text("def add(a, b):\n    return a - b\n")
    (repo / "tests" / "test_other.py").write_text("def test_other():\n    assert True\n")
    (repo / "CHANGELOG.md").write_text("# Changelog\n")
    _git(repo, "init", "-q")
    _git(repo, "add", "-A")
    _git(repo, "commit", "-q", "-m", "base")
    base = _git(repo, "rev-parse", "HEAD")
    (repo / "calc.py").write_text("def add(a, b):\n    return a + b  # the secret fix\n")
    _git(repo, "commit", "-q", "-am", "fix")
    fix = _git(repo, "rev-parse", "HEAD")
    task = tmp_path / "task"
    task.mkdir()
    (task / "task.toml").write_text(
        f'''format = 1
id = "calc-add"
title = "Fix add"
base_sha = "{base}"

[problem]
statement = "add() subtracts."
evidence = ["https://example.com/issue/1"]

[scope]
allowed = ["calc.py", "tests/test_*.py", "CHANGELOG.md"]
required_docs = ["CHANGELOG.md"]

[validation]
commands = ["python -m pytest -q tests"]
regression_tests = ["tests/test_calc.py::test_add"]
manual = ["check the result by hand"]

[expected]
behavior = "THE-ORACLE: add() returns the sum."
reference_fix = "{fix}"

[limits]
max_files = 3
max_changed_lines = 20
max_minutes = 60
'''
    )
    return repo, task, fix


def _attempt(out: Path, fixed: bool = True) -> None:
    worktree = out / "worktree"
    if fixed:
        (worktree / "calc.py").write_text("def add(a, b):\n    return a + b\n")
    (worktree / "tests" / "test_calc.py").write_text(TEST)
    (worktree / "CHANGELOG.md").write_text("# Changelog\n\n- add() adds.\n")
    report = {
        "tests_run": [{"command": "python -m pytest -q tests", "result": "passed"}],
        "unrun_checks": ["check the result by hand"],
    }
    (out / "report.json").write_text(json.dumps(report))


def test_prepare_pins_only_the_base_commit(project: tuple[Path, Path, str], tmp_path: Path) -> None:
    repo, task, fix = project
    out = tmp_path / "out"
    worktree = runner.prepare(task, out, repo)
    assert _git(worktree, "rev-parse", "HEAD") == _git(repo, "rev-parse", "HEAD~1")
    missing = subprocess.run(["git", "cat-file", "-e", fix], cwd=worktree, check=False)
    assert missing.returncode != 0  # the fix is not in the attempt's history
    text = (out / "prompt.md").read_text()
    assert "add() subtracts." in text and "tests/test_calc.py::test_add" in text
    assert str((out / "report.json").resolve()) in text
    for hidden in ("THE-ORACLE", "example.com", fix):
        assert hidden not in text
    with pytest.raises(InputError, match="must be new or empty"):
        runner.prepare(task, out, repo)


def test_a_correct_attempt_is_graded_from_what_the_runner_ran(
    project: tuple[Path, Path, str], tmp_path: Path
) -> None:
    repo, task, _ = project
    out = tmp_path / "out"
    runner.prepare(task, out, repo)
    _attempt(out)
    meta = tmp_path / "meta.json"
    meta.write_text(json.dumps({"model": "m", "tokens": 12, "human_review_actions": ["read"]}))
    result = runner.collect(out, meta_path=meta, no_sandbox=True)
    assert result["verdict"] == "pass", (out / "grade.json").read_text()
    evidence = json.loads((out / "candidate" / "evidence.json").read_text())
    assert evidence == [
        {"command": "python -m pytest -q tests", "tree": "candidate", "exit_code": 0},
        {
            "command": "python -m pytest -q tests/test_calc.py::test_add",
            "tree": "base",
            "exit_code": 1,
        },
        {
            "command": "python -m pytest -q tests/test_calc.py::test_add",
            "tree": "candidate",
            "exit_code": 0,
        },
    ]
    assert result["model"] == "m" and result["tokens"] == 12
    assert result["cost"] == "unknown" and result["seed"] == "unknown"
    assert result["prompt_id"] == "calc-add" and result["sandbox"] == "none"
    assert result["human_review_actions"] == ["read", "manual check: check the result by hand"]
    assert result["head_moved"] is False and result["over_max_minutes"] is False
    assert "patch.diff" not in result["artifacts"]
    assert set(result["artifacts"]) >= {
        "grade.json",
        "candidate/patch.diff",
        "logs/01-candidate.log",
    }
    assert not (out / "trees").exists() and not (out / "home").exists()
    # Nothing was committed: the checkout still holds only the base commit.
    assert _git(out / "worktree", "rev-list", "--count", "HEAD") == "1"
    with pytest.raises(InputError, match="collect already ran"):
        runner.collect(out, no_sandbox=True)


def test_an_unfixed_attempt_fails_on_the_runner_s_evidence(
    project: tuple[Path, Path, str], tmp_path: Path
) -> None:
    repo, task, _ = project
    out = tmp_path / "out"
    runner.prepare(task, out, repo)
    _attempt(out, fixed=False)  # adds the test but not the fix, and claims a pass
    result = runner.collect(out, no_sandbox=True)
    assert result["verdict"] == "fail"
    assert result["failed_checks"] == ["regression_tests", "validation_claims"]
    assert "tests/test_calc.py::test_add fails on the patched tree" in result["regression_escapes"]
    assert runner.main(["summarize", str(out)]) == 0


def test_commands_get_no_secrets_and_logs_are_redacted(
    project: tuple[Path, Path, str], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo, task, _ = project
    toml = task / "task.toml"
    command = "python -c \"import os; print(os.environ.get('MY_API_TOKEN'), 'ghp_' + 'a' * 30)\""
    toml.write_text(toml.read_text().replace('"python -m pytest -q tests"', json.dumps(command)))
    monkeypatch.setenv("MY_API_TOKEN", "do-not-leak-this-value")
    out = tmp_path / "out"
    runner.prepare(task, out, repo)
    _attempt(out)
    runner.collect(out, no_sandbox=True)
    log = (out / "logs" / "01-candidate.log").read_text()
    assert "None [REDACTED]" in log
    assert "do-not-leak-this-value" not in log


def test_a_failed_collect_can_be_run_again(
    project: tuple[Path, Path, str], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo, task, _ = project
    out = tmp_path / "out"
    runner.prepare(task, out, repo)
    _attempt(out)

    def broken(*args: object) -> None:
        raise InputError("the patch does not apply")

    with monkeypatch.context() as patched:
        patched.setattr(runner, "_tree", broken)
        with pytest.raises(InputError, match="does not apply"):
            runner.collect(out, no_sandbox=True)
    assert not (out / "candidate").exists()
    assert runner.collect(out, no_sandbox=True)["verdict"] == "pass"


def test_a_slow_command_is_recorded_as_a_timeout(
    project: tuple[Path, Path, str], tmp_path: Path
) -> None:
    repo, task, _ = project
    toml = task / "task.toml"
    slow = 'python -c "import time; time.sleep(30)"'
    toml.write_text(toml.read_text().replace('"python -m pytest -q tests"', json.dumps(slow)))
    out = tmp_path / "out"
    runner.prepare(task, out, repo)
    _attempt(out)
    runner.collect(out, timeout=1, no_sandbox=True)
    evidence = json.loads((out / "candidate" / "evidence.json").read_text())
    assert evidence[0] == {"command": slow, "tree": "candidate", "exit_code": 124}


def test_a_task_changed_after_prepare_is_refused(
    project: tuple[Path, Path, str], tmp_path: Path
) -> None:
    repo, task, _ = project
    out = tmp_path / "out"
    runner.prepare(task, out, repo)
    _attempt(out)
    toml = task / "task.toml"
    toml.write_text(toml.read_text().replace("max_minutes = 60", "max_minutes = 61"))
    with pytest.raises(InputError, match="changed after prepare"):
        runner.collect(out, no_sandbox=True)


def test_a_missing_sandbox_fails_closed(
    project: tuple[Path, Path, str], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo, task, _ = project
    out = tmp_path / "out"
    runner.prepare(task, out, repo)
    _attempt(out)
    for unshare in (("false",), ("no-such-unshare-binary",)):
        monkeypatch.setattr(runner, "UNSHARE", unshare)
        with pytest.raises(InputError, match="pass --no-sandbox"):
            runner.collect(out)
        assert sorted(p.name for p in out.iterdir()) == [
            "attempt.json",
            "prompt.md",
            "report.json",
            "worktree",
        ]


def _sandboxed(scratch: Path) -> bool:
    scratch.mkdir()
    try:
        runner._sandbox(scratch, [], False)
    except InputError:
        return False
    return True


def test_a_sandboxed_check_sees_only_its_tree(
    project: tuple[Path, Path, str], tmp_path: Path
) -> None:
    repo, task, _ = project
    out = tmp_path / "out"
    secret = tmp_path / "secret.txt"  # outside the tree, like a credential file
    secret.write_text("hidden")
    probe = (
        "import os, socket\n"
        "open('written', 'w').write('ok')\n"
        f"for path in ({str(secret)!r}, {str(out / 'attempt.json')!r}):\n"
        "    assert not os.path.exists(path), path\n"
        "try:\n"
        "    open('/usr/agent-eval-write', 'w')\n"
        "    raise SystemExit('the system directories are writable')\n"
        "except OSError:\n"
        "    pass\n"
        "status = open('/proc/self/status').read()\n"
        "assert 'CapEff:\\t0000000000000000' in status, status\n"
        "assert 'CapBnd:\\t0000000000000000' in status, status\n"
        "try:\n"
        "    os.chroot('.')\n"
        "    raise SystemExit('chroot is still allowed')\n"
        "except PermissionError:\n"
        "    pass\n"
        "import subprocess\n"
        "remount = subprocess.run(['mount', '-o', 'remount,rw', '/usr'], capture_output=True)\n"
        "assert remount.returncode != 0, 'the read-only mounts can be changed'\n"
        "try:\n"
        "    socket.create_connection(('192.0.2.1', 80), 2)\n"
        "    raise SystemExit('the network is reachable')\n"
        "except OSError:\n"
        "    pass\n"
    )
    (tmp_path / "probe.py").write_text(probe)
    toml = task / "task.toml"
    shutil.copy(tmp_path / "probe.py", task / "probe.py")
    toml.write_text(
        toml.read_text().replace(
            'manual = ["check the result by hand"]',
            'manual = ["check the result by hand"]\nacceptance = ["probe.py"]',
        )
    )
    if not _sandboxed(tmp_path / "probe"):
        pytest.skip("unprivileged user, mount and network namespaces are not available here")
    runner.prepare(task, out, repo)
    _attempt(out)
    result = runner.collect(out, keep_trees=True)
    assert result["sandbox"] == "namespaces"
    log = (out / "logs" / "05-candidate.log").read_text()
    assert "[exit 0]" in log, log
    assert (out / "trees" / "candidate" / "written").read_text() == "ok"
    assert result["verdict"] == "fail"  # the probe passes on the base tree too


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("token ghp_" + "b" * 36, "token [REDACTED]"),
        ("github_pat_" + "c" * 30, "[REDACTED]"),
        ("pypi-" + "d" * 40, "[REDACTED]"),
        ("Authorization: Bearer abc.def", "Authorization: Bearer [REDACTED]"),
        ("password=hunter2 rest", "password=[REDACTED] rest"),
        ("nothing secret here", "nothing secret here"),
    ],
)
def test_redaction(text: str, expected: str) -> None:
    assert runner.redact(text, {}) == expected


def test_redaction_hides_secret_named_variables() -> None:
    environ = {"DEPLOY_SECRET": "s3cr3t-value", "PATH": "/usr/bin/long-enough"}
    assert runner.redact("x s3cr3t-value /usr/bin/long-enough", environ) == (
        "x [REDACTED DEPLOY_SECRET] /usr/bin/long-enough"
    )


@pytest.mark.parametrize(
    ("meta", "message"),
    [
        ({"temperature": 0}, "unknown keys"),
        ({"tokens": "many"}, "'tokens' must be a number"),
        ({"cost": True}, "'cost' must be a number"),
        ({"human_review_actions": "read"}, "list of strings"),
    ],
)
def test_metadata_is_validated(tmp_path: Path, meta: dict, message: str) -> None:
    path = tmp_path / "meta.json"
    path.write_text(json.dumps(meta))
    with pytest.raises(InputError, match=message):
        runner.load_meta(path)


def test_metadata_defaults_to_unknown() -> None:
    meta = runner.load_meta(None)
    assert meta["human_review_actions"] == []
    assert {value for key, value in meta.items() if key != "human_review_actions"} == {"unknown"}


def test_summarize_reports_distributions(tmp_path: Path) -> None:
    runs = [("pass", []), ("fail", ["docs"]), ("fail", ["docs", "scope"])]
    dirs = []
    for number, (verdict, failed) in enumerate(runs):
        out = tmp_path / str(number)
        out.mkdir()
        run = {"task": "t", "verdict": verdict, "failed_checks": failed, "model": "m"}
        (out / "run.json").write_text(json.dumps(run))
        dirs.append(out)
    assert runner.summarize(dirs)["tasks"]["t"] == {
        "attempts": 3,
        "verdicts": {"pass": 1, "fail": 2},
        "failed_checks": {"docs": 2, "scope": 1},
        "models": {"m": 3},
    }
    (dirs[0] / "run.json").write_text("{}")
    with pytest.raises(InputError, match="not made by collect"):
        runner.summarize(dirs)


def test_an_output_directory_inside_a_repository_still_applies_the_patch(
    project: tuple[Path, Path, str],
) -> None:
    repo, task, _ = project
    out = repo / "attempts" / "1"  # git apply must not use the enclosing repository
    runner.prepare(task, out, repo)
    _attempt(out)
    assert runner.collect(out, no_sandbox=True)["verdict"] == "pass"


def test_a_relative_interpreter_path_works(
    project: tuple[Path, Path, str], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo, task, _ = project
    out = tmp_path / "out"
    runner.prepare(task, out, repo)
    _attempt(out)
    monkeypatch.chdir(tmp_path)
    relative = Path(os.path.relpath(sys.executable, tmp_path))
    assert runner.collect(out, python=relative, no_sandbox=True)["verdict"] == "pass"


def test_a_timeout_kills_the_command_s_children(
    project: tuple[Path, Path, str], tmp_path: Path
) -> None:
    repo, task, _ = project
    toml = task / "task.toml"
    spawn = (
        "python -c \"import subprocess, sys, time; subprocess.Popen([sys.executable, '-c', "
        "'import time; time.sleep(2); open(chr(108), chr(119))']); time.sleep(30)\""
    )
    toml.write_text(toml.read_text().replace('"python -m pytest -q tests"', json.dumps(spawn)))
    out = tmp_path / "out"
    runner.prepare(task, out, repo)
    _attempt(out)
    runner.collect(out, timeout=1, no_sandbox=True, keep_trees=True)
    time.sleep(3)
    assert not (out / "trees" / "candidate" / "l").exists()


def test_a_grading_error_leaves_the_attempt_collectable(
    project: tuple[Path, Path, str], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo, task, _ = project
    out = tmp_path / "out"
    runner.prepare(task, out, repo)
    _attempt(out)

    def rejected(*args: object) -> None:
        raise InputError("patch.diff: quoted paths are not supported")

    with monkeypatch.context() as patched:
        patched.setattr(runner.agent_eval, "grade", rejected)
        with pytest.raises(InputError, match="quoted paths"):
            runner.collect(out, no_sandbox=True)
    assert not (out / "candidate").exists() and not (out / "logs").exists()
    assert runner.collect(out, no_sandbox=True)["verdict"] == "pass"


def test_a_blocking_import_is_timed_out(project: tuple[Path, Path, str], tmp_path: Path) -> None:
    repo, task, _ = project
    out = tmp_path / "out"
    runner.prepare(task, out, repo)
    _attempt(out)
    package = out / "worktree" / "pycubrid"
    package.mkdir()
    (package / "__init__.py").write_text("import time\ntime.sleep(30)\n")
    with pytest.raises(InputError, match="took longer than 1s"):
        runner.collect(out, timeout=1, no_sandbox=True)


def test_git_helpers_configured_by_the_attempt_never_run(
    project: tuple[Path, Path, str], tmp_path: Path
) -> None:
    repo, task, _ = project
    out = tmp_path / "out"
    runner.prepare(task, out, repo)
    _attempt(out)
    marker = tmp_path / "helper-ran"
    helper = tmp_path / "helper.sh"
    helper.write_text(f"#!/bin/sh\ntouch {marker}\n")
    helper.chmod(0o755)
    worktree = out / "worktree"
    for key, value in (
        ("diff.external", str(helper)),
        ("core.fsmonitor", str(helper)),
        ("diff.calc.textconv", str(helper)),
        ("filter.calc.clean", str(helper)),
    ):
        _git(worktree, "config", key, value)
    (worktree / ".gitattributes").write_text("*.py diff=calc filter=calc\n")
    result = runner.collect(out, no_sandbox=True)
    assert not marker.exists()
    assert (
        "def add(a, b):\n-    return a - b\n+    return a + b"
        in (out / "candidate" / "patch.diff").read_text()
    )
    assert result["out_of_scope_edits"] == ["out-of-scope change: .gitattributes"]


def test_kept_trees_are_not_hashed_and_links_are_not_followed(
    project: tuple[Path, Path, str], tmp_path: Path
) -> None:
    repo, task, _ = project
    out = tmp_path / "out"
    runner.prepare(task, out, repo)
    _attempt(out)
    unreadable = tmp_path / "unreadable"
    unreadable.write_text("x")
    unreadable.chmod(0)
    (out / "worktree" / "tests" / "link_test.py").symlink_to(unreadable)
    try:
        result = runner.collect(out, no_sandbox=True, keep_trees=True)
    finally:
        unreadable.chmod(0o600)
    assert (out / "trees" / "candidate" / "tests" / "link_test.py").is_symlink()
    assert not any(name.startswith(("trees/", "worktree/")) for name in result["artifacts"])
