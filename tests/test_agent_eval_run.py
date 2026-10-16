"""The opt-in agent-evaluation runner (#925)."""

from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
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
    result = runner.collect(out, meta_path=meta, allow_network=True)
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
    assert result["prompt_id"] == "calc-add" and result["network"] == "allowed"
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
        runner.collect(out, allow_network=True)


def test_an_unfixed_attempt_fails_on_the_runner_s_evidence(
    project: tuple[Path, Path, str], tmp_path: Path
) -> None:
    repo, task, _ = project
    out = tmp_path / "out"
    runner.prepare(task, out, repo)
    _attempt(out, fixed=False)  # adds the test but not the fix, and claims a pass
    result = runner.collect(out, allow_network=True)
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
    runner.collect(out, allow_network=True)
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
            runner.collect(out, allow_network=True)
    assert not (out / "candidate").exists()
    assert runner.collect(out, allow_network=True)["verdict"] == "pass"


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
    runner.collect(out, timeout=1, allow_network=True)
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
        runner.collect(out, allow_network=True)


def test_missing_network_isolation_fails_closed(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(runner, "ISOLATE", ("false",))
    with pytest.raises(InputError, match="pass --allow-network"):
        runner._isolation(False)
    assert runner._isolation(True) == ()


def test_isolated_commands_have_no_network() -> None:
    try:
        prefix = runner._isolation(False)
    except InputError:
        pytest.skip("unprivileged network namespaces are not available here")
    probe = "import socket; socket.create_connection(('192.0.2.1', 80), 2)"
    run = subprocess.run(
        [*prefix, sys.executable, "-c", probe], capture_output=True, text=True, check=False
    )
    assert run.returncode != 0 and "unreachable" in run.stderr.lower()


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
