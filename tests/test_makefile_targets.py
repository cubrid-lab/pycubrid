"""Makefile target semantics (#1007).

The tests run the real Makefile with a ``PYTHON`` stub that records each call, so
they need none of the tools installed: ``doctor`` fails when a tool is missing,
``lint``/``format-check`` never modify files, ``check`` is the complete static gate
with ``check-all`` as a deprecated alias, ``verify`` is ``check``, ``test`` and then
the managed live ``integration`` lane (#1022), every tool goes through ``$(PYTHON) -m`` and stays overridable, and
``help`` groups the targets.
"""

from __future__ import annotations

import os
import re
import stat
import subprocess
from pathlib import Path

import pytest

pytestmark = pytest.mark.skipif(os.name != "posix", reason="command stubs need a POSIX shell")

ROOT = Path(__file__).resolve().parents[1]
MAKEFILE = (ROOT / "Makefile").read_text()
TOOLS = {
    "PIP": "pip",
    "PYTEST": "pytest",
    "RUFF": "ruff",
    "MYPY": "mypy",
    "BANDIT": "bandit",
    "PRE_COMMIT": "pre_commit",
}


def _python_stub(tmp_path: Path) -> tuple[Path, Path]:
    """A PYTHON that logs its arguments and fails when they contain $REVIEW_FAIL."""
    log = tmp_path / "python.log"
    stub = tmp_path / "python"
    stub.write_text(
        "#!/bin/sh\n"
        f'printf "%s\\n" "$*" >> "{log}"\n'
        'if [ -n "$REVIEW_FAIL" ]; then case " $* " in *" $REVIEW_FAIL "*) exit 1 ;; esac; fi\n'
        "exit 0\n"
    )
    stub.chmod(stub.stat().st_mode | stat.S_IXUSR)
    return stub, log


def _make(
    *args: str, env: dict[str, str] | None = None, dry_run: bool = False
) -> subprocess.CompletedProcess[str]:
    base = {k: v for k, v in os.environ.items() if k not in {"PYTHON", *TOOLS}}
    return subprocess.run(
        ["make", "-s", "-C", str(ROOT), *(["-n"] if dry_run else []), *args],
        env={**base, **(env or {})},
        capture_output=True,
        text=True,
        check=False,
    )


def _calls(tmp_path: Path, target: str) -> list[str]:
    stub, log = _python_stub(tmp_path)
    result = _make(target, f"PYTHON={stub}")
    assert result.returncode == 0, result.stdout + result.stderr
    return log.read_text().splitlines()


@pytest.mark.parametrize(("variable", "module"), sorted(TOOLS.items()))
def test_every_tool_variable_runs_through_python_and_is_overridable(
    variable: str, module: str
) -> None:
    assert re.search(r"^PYTHON \?= python3$", MAKEFILE, re.MULTILINE)
    assert re.search(rf"^{variable} \?= \$\(PYTHON\) -m {module}$", MAKEFILE, re.MULTILINE), (
        variable
    )
    # No recipe calls a tool by its bare executable name.
    assert not re.search(rf"^\t@?{module.replace('_', '-')} ", MAKEFILE, re.MULTILINE)


def test_tool_variables_follow_python_from_the_command_line_and_environment() -> None:
    from_command_line = _make("lint", "PYTHON=/opt/py", dry_run=True).stdout
    assert "/opt/py -m ruff check " in from_command_line
    from_environment = _make("lint", env={"PYTHON": "/opt/py"}, dry_run=True).stdout
    assert "/opt/py -m ruff check " in from_environment
    assert "my-ruff check " in _make("lint", "RUFF=my-ruff", dry_run=True).stdout


def test_doctor_passes_when_every_tool_answers(tmp_path: Path) -> None:
    stub, log = _python_stub(tmp_path)
    result = _make("doctor", f"PYTHON={stub}")
    assert result.returncode == 0, result.stderr
    assert result.stdout.rstrip().endswith("All checks passed!")
    assert log.read_text().splitlines() == [
        "--version",
        "-m pytest --version",
        "-m ruff --version",
        "-m mypy --version",
        "-m bandit --version",
        "-m pre_commit --version",
    ]


@pytest.mark.parametrize("module", ["ruff", "bandit", "pre_commit"])
def test_doctor_fails_without_a_success_line_when_a_tool_fails(tmp_path: Path, module: str) -> None:
    stub, log = _python_stub(tmp_path)
    result = _make("doctor", f"PYTHON={stub}", env={"REVIEW_FAIL": module})
    assert result.returncode != 0
    assert "All checks passed!" not in result.stdout + result.stderr
    assert f"-m {module} --version' failed" in result.stderr
    # Every tool is still checked, so one run reports everything missing.
    assert len(log.read_text().splitlines()) == 6


def test_doctor_fails_when_a_tool_is_missing(tmp_path: Path) -> None:
    stub, _ = _python_stub(tmp_path)
    result = _make("doctor", f"PYTHON={stub}", f"BANDIT={tmp_path / 'no-such-bandit'}")
    assert result.returncode != 0
    assert "All checks passed!" not in result.stdout + result.stderr
    assert "no-such-bandit --version' failed" in result.stderr


def test_lint_and_format_check_never_modify_files(tmp_path: Path) -> None:
    lint = [c for c in _calls(tmp_path, "lint") if c.startswith("-m ruff")]
    (tmp_path / "python.log").unlink()
    format_check = [c for c in _calls(tmp_path, "format-check") if c.startswith("-m ruff")]
    assert len(lint) == 1 and lint[0].startswith("-m ruff check ")
    assert "--fix" not in lint[0]
    assert len(format_check) == 1 and format_check[0].startswith("-m ruff format --check ")


def test_format_is_the_mutating_target(tmp_path: Path) -> None:
    ruff = [c for c in _calls(tmp_path, "format") if c.startswith("-m ruff")]
    assert [c.split(" ")[2:4] for c in ruff] == [["check", "--fix"], ["format", "pycubrid"]]


CHECK_CALLS = [
    "scripts/check_quality_tools.py",
    "-m ruff check",
    "-m ruff format --check",
    "-m mypy",
    "-m bandit",
    "-m doctest scripts/check_docs_reason.py",
    "-m unittest discover",
]


def _assert_check_calls(calls: list[str]) -> None:
    for expected in CHECK_CALLS:
        assert any(call.startswith(expected) for call in calls), (expected, calls)
    assert not any("pytest" in call for call in calls), calls


def test_check_is_the_complete_static_gate(tmp_path: Path) -> None:
    _assert_check_calls(_calls(tmp_path, "check"))


def test_check_all_is_a_deprecated_alias_of_check(tmp_path: Path) -> None:
    stub, log = _python_stub(tmp_path)
    result = _make("check-all", f"PYTHON={stub}")
    assert result.returncode == 0, result.stderr
    _assert_check_calls(log.read_text().splitlines())
    assert "'make check-all' is deprecated" in result.stderr
    assert "use 'make check'" in result.stderr
    assert "deprecated" not in result.stdout


def test_verify_runs_check_then_offline_tests_then_the_live_lane(tmp_path: Path) -> None:
    calls = _calls(tmp_path, "verify")
    live = [c for c in calls if c.startswith("scripts/run_integration.py")]
    reports = [c for c in calls if c.startswith("scripts/report_results.py")]
    static = [c for c in calls if "pytest" not in c and c not in reports and c not in live]
    tests = [c for c in calls if c.startswith("-m pytest")]
    _assert_check_calls(static)
    assert len(tests) == 1 and "-m not integration" in tests[0]
    # The offline result summaries (#1024) follow pytest, and the managed live lane
    # (#1022) runs last, only after the offline tests.
    assert len(reports) == 2 and len(live) == 1
    assert calls[-4:] == [tests[0], *reports, live[0]]
    assert "--lane normal" in live[0]


@pytest.mark.parametrize("jobs", [[], ["-j3"]], ids=["serial", "parallel"])
def test_verify_stops_before_the_live_lane_when_offline_tests_fail(
    tmp_path: Path, jobs: list[str]
) -> None:
    stub, log = _python_stub(tmp_path)
    result = _make(*jobs, "verify", f"PYTHON={stub}", env={"REVIEW_FAIL": "pytest"})
    assert result.returncode != 0
    assert not any(c.startswith("scripts/run_integration.py") for c in log.read_text().splitlines())


def test_test_stays_offline() -> None:
    recipe = _make("test", dry_run=True).stdout
    assert "docker" not in recipe and "run_integration" not in recipe


def test_help_groups_targets_by_purpose() -> None:
    result = _make("help")
    assert result.returncode == 0
    text = re.sub(r"\x1b\[[0-9;]*m", "", result.stdout)
    groups: dict[str, list[str]] = {}
    current = None
    for line in text.splitlines():
        if line and not line.startswith(" "):
            current = line.strip()
            groups[current] = []
        elif line.strip():
            assert current is not None, line
            groups[current].append(line.split()[0])
    assert {"Setup", "Tests", "Integration (Docker)"} <= set(groups)
    quality = next(name for name in groups if name.startswith("Quality"))
    assert {"lint", "format-check", "format", "typecheck", "security", "check"} <= set(
        groups[quality]
    )
    assert {"test", "verify"} <= set(groups["Tests"])
    assert {
        "integration",
        "integration-tls",
        "docker-up",
        "docker-down",
        "docker-clean",
    } <= set(groups["Integration (Docker)"])
    documented = re.findall(r"^([a-zA-Z_-]+):.*?## ", MAKEFILE, re.MULTILINE)
    assert sorted(t for targets in groups.values() for t in targets) == sorted(documented)


def test_test_writes_results_and_keeps_pytest_status(tmp_path: Path) -> None:
    """`make test` passes pytest's status through and always summarizes it (#1024)."""
    stub, log = _python_stub(tmp_path)
    result = _make("test", f"PYTHON={stub}", env={"REVIEW_FAIL": "--cov-fail-under=95"})
    assert result.returncode != 0
    calls = log.read_text().splitlines()
    assert "--junitxml=test-results/test/junit.xml" in calls[0]
    assert "--cov-report=xml:test-results/test/coverage.xml" in calls[0]
    assert [c.split()[1] for c in calls[1:]] == ["environment", "summarize"]
    assert "--exit-code 1" in calls[2]
    assert "--coverage test-results/test/coverage.xml" in calls[2]


def test_clean_removes_test_results() -> None:
    assert "test-results/" in _make("clean", dry_run=True).stdout
    assert "test-results/" in (ROOT / ".gitignore").read_text().splitlines()
