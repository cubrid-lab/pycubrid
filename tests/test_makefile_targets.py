"""Makefile target semantics (#1007).

The tests run the real Makefile with a ``PYTHON`` stub that records each call, so
they need none of the tools installed: ``doctor`` fails when a tool is missing,
``lint``/``format-check`` never modify files, ``check`` is the complete static gate
with ``check-all`` as a deprecated alias, ``verify`` is ``check``, ``test`` and then
the managed live ``integration`` lane (#1022) and always writes an aggregate summary
(#1053), every tool goes through ``$(PYTHON) -m`` and stays overridable, and
``help`` groups the targets.
"""

from __future__ import annotations

import json
import os
import re
import signal
import stat
import subprocess
import sys
import time
from pathlib import Path

import pytest

from scripts import report_results as rr

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
# An outer make (e.g. `make test` running this suite) must not leak into the tested make.
MAKE_VARIABLES = {"MAKEFLAGS", "MFLAGS", "MAKELEVEL", "MAKEOVERRIDES"}


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
    base = {k: v for k, v in os.environ.items() if k not in {"PYTHON", *TOOLS, *MAKE_VARIABLES}}
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


def _verify_results(tmp_path: Path) -> list[str]:
    """Result locations under tmp_path, so a verify run never touches test-results/."""
    return [
        f"TEST_RESULTS_DIR={tmp_path}/test",
        f"INTEGRATION_RESULTS={tmp_path}/integration/junit.xml",
        f"VERIFY_RESULTS={tmp_path}/verify",
    ]


def _verify_stub(tmp_path: Path) -> tuple[Path, Path]:
    """A PYTHON that logs every call and runs the real verify runner and reporter (#1053).

    A call containing $REVIEW_FAIL exits 1. pytest writes $REVIEW_JUNIT as its JUnit file.
    The live runner writes $REVIEW_JUNIT, environment.json ($REVIEW_ENVIRONMENT) and a
    summary.json with $REVIEW_LIVE_STATUS (none when it is empty) next to --results, and
    exits $REVIEW_LIVE_EXIT; with $REVIEW_BLOCK it first blocks until SIGTERM, which it logs.
    """
    log = tmp_path / "python.log"
    stub = tmp_path / "python"
    stub.write_text(
        "#!/bin/sh\n"
        f'printf "%s\\n" "$*" >> "{log}"\n'
        'case " $* " in *" scripts/run_verify.py "*|*" scripts/report_results.py "*)\n'
        f'  exec "{sys.executable}" "$@" ;;\nesac\n'
        'if [ -n "$REVIEW_FAIL" ]; then case " $* " in *" $REVIEW_FAIL "*) exit 1 ;; esac; fi\n'
        'case " $* " in\n'
        '  *" scripts/run_integration.py "*)\n'
        '    prev=; for a in "$@"; do [ "$prev" = --results ] && res="$a"; prev="$a"; done\n'
        '    if [ -n "$REVIEW_BLOCK" ]; then\n'
        "      sleep 30 & pid=$!\n"
        f"      trap 'kill $pid; echo TERMINATED >> \"{log}\"; exit 130' TERM\n"
        f'      echo BLOCKING >> "{log}"; wait $pid\n'
        "    fi\n"
        '    dir=$(dirname "$res"); mkdir -p "$dir"\n'
        '    printf "%s" "$REVIEW_JUNIT" > "$res"\n'
        '    printf "%s" "$REVIEW_ENVIRONMENT" > "$dir/environment.json"\n'
        '    [ -z "$REVIEW_LIVE_STATUS" ] || '
        'printf \'{"status": "%s"}\' "$REVIEW_LIVE_STATUS" > "$dir/summary.json"\n'
        '    exit "$REVIEW_LIVE_EXIT" ;;\n'
        '  *" -m pytest "*)\n'
        '    for a in "$@"; do case "$a" in --junitxml=*) '
        'printf "%s" "$REVIEW_JUNIT" > "${a#--junitxml=}" ;; esac; done ;;\n'
        "esac\n"
        "exit 0\n"
    )
    stub.chmod(stub.stat().st_mode | stat.S_IXUSR)
    return stub, log


PASSING_JUNIT = '<testsuite><testcase classname="t" name="ok"/></testsuite>'
VERIFY_ENV = {
    "REVIEW_JUNIT": PASSING_JUNIT,
    "REVIEW_ENVIRONMENT": '{"lane": "integration"}',
    "REVIEW_LIVE_STATUS": "PASS",
    "REVIEW_LIVE_EXIT": "0",
}


def _verify(
    tmp_path: Path, *args: str, env: dict[str, str] | None = None
) -> tuple[subprocess.CompletedProcess[str], list[str]]:
    stub, log = _verify_stub(tmp_path)
    result = _make(
        *args,
        "verify",
        f"PYTHON={stub}",
        *_verify_results(tmp_path),
        env={**VERIFY_ENV, **(env or {})},
    )
    return result, log.read_text().splitlines()


def _verify_summary(tmp_path: Path) -> tuple[str, dict[str, str]]:
    summary = json.loads((tmp_path / "verify" / "summary.json").read_text())
    return summary["status"], {phase["name"]: phase["status"] for phase in summary["phases"]}


def test_verify_execs_the_runner_and_a_dry_run_writes_nothing(tmp_path: Path) -> None:
    # Not prerequisites, so `make -j` cannot start a later phase early.
    assert re.search(r"^verify: ## ", MAKEFILE, re.MULTILINE)
    result = _make("verify", *_verify_results(tmp_path), dry_run=True)
    recipe = result.stdout.replace("\\\n", " ")
    assert recipe.startswith("exec python3 scripts/run_verify.py --make "), recipe
    assert f'--integration-junit "{tmp_path}/integration/junit.xml"' in recipe
    assert "run_integration" not in recipe and " -m pytest" not in recipe
    assert not list(tmp_path.iterdir())


def test_verify_runs_check_then_offline_tests_then_the_live_lane(tmp_path: Path) -> None:
    result, calls = _verify(tmp_path)
    assert result.returncode == 0, result.stdout + result.stderr
    assert calls[0].startswith("scripts/run_verify.py --make ")
    live = [c for c in calls if c.startswith("scripts/run_integration.py")]
    reports = [c for c in calls if c.startswith("scripts/report_results.py")]
    tests = [c for c in calls if c.startswith("-m pytest")]
    static = [c for c in calls[1:] if c not in {*live, *reports, *tests}]
    _assert_check_calls(static)
    assert len(tests) == 1 and "-m not integration" in tests[0]
    # The offline result summaries (#1024) follow pytest, and the managed live lane
    # (#1022) runs last, only after the offline tests.
    assert len(reports) == 2 and len(live) == 1
    assert calls[-4:] == [tests[0], *reports, live[0]]
    assert "--lane normal" in live[0]
    # The aggregate (#1053) records every phase and copies the lane results.
    assert _verify_summary(tmp_path) == (
        "PASS",
        {"check": "PASS", "test": "PASS", "integration": "PASS"},
    )
    verify = tmp_path / "verify"
    assert (verify / "junit" / "test.xml").read_text() == PASSING_JUNIT
    assert (verify / "junit" / "integration.xml").read_text() == PASSING_JUNIT
    assert (verify / "environment.json").read_text() == VERIFY_ENV["REVIEW_ENVIRONMENT"]
    summary = json.loads((verify / "summary.json").read_text())
    line = result.stdout.splitlines()[-1]
    assert line == f"{rr.console_line(summary)} -> {verify / 'summary.json'}"
    assert line.startswith("verify: PASS check=PASS test=PASS integration=PASS -> ")


@pytest.mark.parametrize("jobs", [[], ["-j3"]], ids=["serial", "parallel"])
@pytest.mark.parametrize(
    ("fail", "phases"),
    [
        ("mypy", {"check": "FAIL", "test": "NOT_RUN", "integration": "NOT_RUN"}),
        ("pytest", {"check": "PASS", "test": "FAIL", "integration": "NOT_RUN"}),
    ],
    ids=["check", "test"],
)
def test_verify_stops_at_the_failing_phase_and_records_the_rest_as_not_run(
    tmp_path: Path, jobs: list[str], fail: str, phases: dict[str, str]
) -> None:
    result, calls = _verify(tmp_path, *jobs, env={"REVIEW_FAIL": fail})
    assert result.returncode != 0
    assert not any(c.startswith("scripts/run_integration.py") for c in calls)
    assert any(c.startswith("-m pytest") for c in calls) == (phases["test"] != "NOT_RUN")
    assert _verify_summary(tmp_path) == ("FAIL", phases)
    copies = {p.name for p in (tmp_path / "verify").rglob("*") if p.is_file()}
    # The failing pytest stub writes no JUnit file, so there is nothing to copy.
    assert copies == {"summary.json"}
    assert "verify: FAIL " in result.stdout


@pytest.mark.parametrize(
    ("live_status", "live_exit", "status"),
    [("", "0", "INCOMPLETE"), ("INCOMPLETE", "0", "INCOMPLETE"), ("INCOMPLETE", "1", "FAIL")],
    ids=["no-summary", "incomplete", "all-skipped"],
)
def test_verify_never_passes_without_a_passing_live_summary(
    tmp_path: Path, live_status: str, live_exit: str, status: str
) -> None:
    result, _ = _verify(
        tmp_path, env={"REVIEW_LIVE_STATUS": live_status, "REVIEW_LIVE_EXIT": live_exit}
    )
    assert result.returncode != 0
    assert _verify_summary(tmp_path) == (
        status,
        {"check": "PASS", "test": "PASS", "integration": status},
    )


def test_verify_masks_configured_passwords_in_the_copies(tmp_path: Path) -> None:
    secret = "s3cr&t pw"
    url = "cubrid://dba:url%20pw@localhost:33000/testdb"
    junit = (
        '<testsuite><testcase classname="t" name="ok"><system-out>'
        f"{secret.replace('&', '&amp;')} url%20pw url pw {url}</system-out></testcase></testsuite>"
    )
    env = {
        "CUBRID_TEST_PASSWORD": secret,
        "CUBRID_TEST_URL": url,
        "REVIEW_JUNIT": junit,
        "REVIEW_ENVIRONMENT": json.dumps({"command": f"x {secret} {url}"}),
    }
    result, _ = _verify(tmp_path, env=env)
    assert result.returncode == 0, result.stdout + result.stderr
    for path in (tmp_path / "verify").rglob("*"):
        if path.is_file():
            text = path.read_text()
            for form in ("s3cr", "url%20pw", "url pw"):
                assert form not in text, (path, text)
    assert "***" in (tmp_path / "verify" / "junit" / "integration.xml").read_text()
    assert "***" in (tmp_path / "verify" / "environment.json").read_text()


def test_verify_forwards_sigterm_to_the_live_lane_and_still_writes_the_summary(
    tmp_path: Path,
) -> None:
    # `timeout make verify` and `kill <make pid>` signal only make, which passes SIGTERM to
    # the runner; the runner must stop the live lane (which then cleans up), not orphan it.
    stub, log = _verify_stub(tmp_path)
    base = {k: v for k, v in os.environ.items() if k not in {"PYTHON", *TOOLS, *MAKE_VARIABLES}}
    process = subprocess.Popen(
        ["make", "-s", "-C", str(ROOT), "verify", f"PYTHON={stub}", *_verify_results(tmp_path)],
        env={**base, **VERIFY_ENV, "REVIEW_BLOCK": "1"},
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        deadline = time.monotonic() + 60
        while "BLOCKING" not in (log.read_text() if log.exists() else ""):
            assert process.poll() is None and time.monotonic() < deadline, "live lane never ran"
            time.sleep(0.05)
        process.send_signal(signal.SIGTERM)
        stdout, stderr = process.communicate(timeout=60)
    finally:
        if process.poll() is None:
            process.kill()
            process.communicate()
    assert process.returncode != 0
    assert "TERMINATED" in log.read_text(), stdout + stderr
    assert "Received SIGTERM; stopping make verify" in stderr
    assert _verify_summary(tmp_path) == (
        "FAIL",
        {"check": "PASS", "test": "PASS", "integration": "FAIL"},
    )
    assert "verify: FAIL check=PASS test=PASS integration=FAIL" in stdout


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


@pytest.mark.parametrize("signal_name", ["SIGINT", "SIGTERM", "SIGHUP"])
def test_run_verify_forwards_the_first_signal_as_sigterm_and_exits_128_plus_n(
    tmp_path: Path, signal_name: str
) -> None:
    """GNU make passes only SIGTERM on to its recipe, so every signal is forwarded as SIGTERM."""
    log = tmp_path / "make.log"
    make = tmp_path / "make"
    make.write_text(
        "#!/bin/sh\n"
        f'echo "$2" >> "{log}"\n'
        'if [ "$2" = integration ]; then\n'
        "  sleep 30 & pid=$!\n"
        f"  trap 'kill $pid; echo TERMINATED >> \"{log}\"; exit 143' TERM\n"
        f'  echo BLOCKING >> "{log}"; wait $pid\n'
        "fi\n"
        "exit 0\n"
    )
    make.chmod(make.stat().st_mode | stat.S_IXUSR)

    def _default_signals() -> None:
        # Background jobs of a non-interactive shell start with SIGINT ignored.
        for name in ("SIGINT", "SIGTERM", "SIGHUP"):
            signal.signal(getattr(signal, name), signal.SIG_DFL)

    process = subprocess.Popen(
        [
            sys.executable,
            str(ROOT / "scripts" / "run_verify.py"),
            "--make",
            str(make),
            "--results-dir",
            str(tmp_path / "verify"),
            "--test-results",
            str(tmp_path / "test"),
            "--integration-junit",
            str(tmp_path / "integration" / "junit.xml"),
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        preexec_fn=_default_signals,
    )
    try:
        deadline = time.monotonic() + 60
        while "BLOCKING" not in (log.read_text() if log.exists() else ""):
            assert process.poll() is None and time.monotonic() < deadline, "live lane never ran"
            time.sleep(0.05)
        signum = getattr(signal, signal_name)
        process.send_signal(signum)
        stdout, stderr = process.communicate(timeout=60)
    finally:
        if process.poll() is None:
            process.kill()
            process.communicate()
    assert process.returncode == 128 + signum, stdout + stderr
    assert log.read_text().splitlines() == [
        "check",
        "test",
        "integration",
        "BLOCKING",
        "TERMINATED",
    ]
    assert f"Received {signal_name}; stopping make verify" in stderr
    status, phases = _verify_summary(tmp_path)
    assert status == "FAIL"
    # The lane summaries are missing, so the passing phases are INCOMPLETE.
    assert phases == {"check": "PASS", "test": "INCOMPLETE", "integration": "FAIL"}


def test_run_verify_keeps_a_signal_received_while_the_summary_is_written(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Copilot review of #1054: a signal during reporting still exits 128 + N."""
    import types

    from scripts import run_verify

    make = tmp_path / "make"
    make.write_text("#!/bin/sh\nexit 0\n")
    make.chmod(make.stat().st_mode | stat.S_IXUSR)

    def _aggregate(*args: object) -> dict[str, str]:
        run_verify._on_signal(signal.SIGTERM, None)
        return {"status": "PASS"}

    monkeypatch.setattr(
        run_verify, "_reporter", lambda: types.SimpleNamespace(aggregate=_aggregate)
    )
    monkeypatch.setattr(run_verify, "HANDLED_SIGNALS", ())
    monkeypatch.setattr(run_verify._State, "received", None)
    argv = ["--make", str(make), "--results-dir", str(tmp_path / "verify")]
    argv += ["--test-results", str(tmp_path / "test")]
    argv += ["--integration-junit", str(tmp_path / "integration" / "junit.xml")]
    assert run_verify.main(argv) == 128 + signal.SIGTERM
