"""Stub-based regressions for ``make integration`` / ``make integration-tls`` teardown.

Both recipes are one ``scripts/run_integration.py`` call (#1007). These tests run the
real Makefile and the real runner with command stubs for ``docker``, pytest, the
readiness probe and the lane audit, so they need neither Docker nor a CUBRID. They
cover success, step failures, cleanup failure, SIGINT/SIGTERM and exit-status
propagation. Every ``docker compose`` call must carry the run-owned project name, and
cleanup removes (``down -v``) only that project, never the ``make docker-up`` one
(#1021). They also pin the Compose service and the manual
``docker-up``/``docker-down``/``docker-clean`` lifecycle (#1006).
"""

from __future__ import annotations

import os
import shlex
import stat
import subprocess
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.skipif(
    os.name != "posix", reason="Command stubs and signal delivery require a POSIX shell"
)

ROOT = Path(__file__).resolve().parents[1]
MAKEFILE = ROOT / "Makefile"
TARGETS = ["integration", "integration-tls"]
CONTAINER = "0123456789ab"


def _executable(path: Path, text: str) -> Path:
    path.write_text(text)
    path.chmod(path.stat().st_mode | stat.S_IEXEC)
    return path


def _stubs(tmp_path: Path, readiness: str | None = None) -> dict[str, str]:
    """Write the command stubs and return the environment that puts them first.

    ``docker`` logs every call, prints a container ID for ``compose ps -q cubrid`` and
    reports ``$REVIEW_EXISTING`` as a container of the project (ownership check).
    The ``PYTHON`` stub runs the real runner with this interpreter, logs the readiness
    probe's endpoint and the audit call, and exits with the configured statuses.
    """
    _executable(
        tmp_path / "docker",
        "#!/bin/sh\n"
        'printf "%s\\n" "$*" >> "$REVIEW_LOG"\n'
        'case " $* " in\n'
        '  *" compose "*" down "*) exit "$REVIEW_DOWN_EXIT" ;;\n'
        '  *" compose "*" up "*) exit "$REVIEW_UP_EXIT" ;;\n'
        f'  *" compose "*" ps -q cubrid "*) echo {CONTAINER} ;;\n'
        '  *" ps -aq "*) [ -z "$REVIEW_EXISTING" ] || echo "$REVIEW_EXISTING" ;;\n'
        "esac\n"
        "exit 0\n",
    )
    _executable(tmp_path / "sleep", "#!/bin/sh\nexit 0\n")
    _executable(
        tmp_path / "pytest-stub",
        '#!/bin/sh\nprintf "%s\\n" "$*" >> "$REVIEW_PYTEST_LOG"\nexit "$REVIEW_PYTEST_EXIT"\n',
    )
    readiness = readiness or (
        'printf "%s|%s|%s|%s|%s\\n" "$CUBRID_TEST_DOCKER_CONTAINER" "$CUBRID_TEST_URL" '
        '"$CUBRID_TEST_HOST" "$CUBRID_TEST_PORT" "$*" >> "$REVIEW_READINESS_LOG"\n'
        'exit "$REVIEW_READINESS_EXIT"\n'
    )
    _executable(
        tmp_path / "python-stub",
        "#!/bin/sh\n"
        'case "$1" in\n'
        f'  *run_integration.py) exec {shlex.quote(sys.executable)} "$@" ;;\n'
        '  *check_integration_lanes.py) printf "%s\\n" "$*" >> "$REVIEW_AUDIT_LOG"; '
        'exit "$REVIEW_AUDIT_EXIT" ;;\n'
        "  *wait_for_cubrid.py) ;;\n"
        "  *) exit 0 ;;\n"
        "esac\n" + readiness,
    )
    env = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith(("CUBRID_TEST_", "REVIEW_", "INTEGRATION_"))
        and key != "COMPOSE_PROJECT_NAME"
    }
    env.update(
        PATH=str(tmp_path) + os.pathsep + env.get("PATH", ""),
        REVIEW_LOG=str(tmp_path / "docker.log"),
        REVIEW_PYTEST_LOG=str(tmp_path / "pytest.log"),
        REVIEW_READINESS_LOG=str(tmp_path / "readiness.log"),
        REVIEW_AUDIT_LOG=str(tmp_path / "audit.log"),
        REVIEW_UP_EXIT="0",
        REVIEW_DOWN_EXIT="0",
        REVIEW_PYTEST_EXIT="0",
        REVIEW_READINESS_EXIT="0",
        REVIEW_AUDIT_EXIT="0",
    )
    return env


def _make_args(tmp_path: Path, target: str) -> list[str]:
    return [
        "make",
        "-C",
        str(ROOT),
        target,
        f"PYTEST={tmp_path / 'pytest-stub'}",
        f"PYTHON={tmp_path / 'python-stub'}",
        f"INTEGRATION_RESULTS={tmp_path / 'results.xml'}",
        "CUBRID_TEST_PORT=33522",
    ]


def _ownership_probes(project: str) -> list[str]:
    label = f"label=com.docker.compose.project={project}"
    return [
        f"ps -aq --filter {label}",
        f"volume ls -q --filter {label}",
        f"network ls -q --filter {label}",
        "volume ls -q",
    ]


def _project(result: subprocess.CompletedProcess[str]) -> str:
    """Return the Compose project the run announced."""
    for line in result.stdout.splitlines():
        if line.startswith("Using run-owned Compose project '"):
            return line.split("'")[1]
    raise AssertionError(result.stdout + result.stderr)


def _read(path: Path) -> str:
    return path.read_text() if path.exists() else ""


def _run_make(
    tmp_path: Path,
    target: str,
    *,
    pytest_exit: int = 0,
    down_exit: int = 0,
    readiness_exit: int = 0,
    audit_exit: int = 0,
    up_exit: int = 0,
    extra_env: dict[str, str] | None = None,
    make_args: list[str] | None = None,
) -> subprocess.CompletedProcess[str]:
    env = _stubs(tmp_path)
    env.update(
        REVIEW_PYTEST_EXIT=str(pytest_exit),
        REVIEW_DOWN_EXIT=str(down_exit),
        REVIEW_READINESS_EXIT=str(readiness_exit),
        REVIEW_AUDIT_EXIT=str(audit_exit),
        REVIEW_UP_EXIT=str(up_exit),
        **(extra_env or {}),
    )
    result = subprocess.run(
        [*_make_args(tmp_path, target), *(make_args or [])],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    setattr(result, "log_text", _read(tmp_path / "docker.log"))
    return result


@pytest.mark.parametrize("target", TARGETS)
def test_recipe_is_a_single_runner_invocation(tmp_path: Path, target: str) -> None:
    recipe = subprocess.run(
        ["make", "-n", "-C", str(ROOT), target],
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    commands = [line for line in recipe.splitlines() if line.strip() and "Entering" not in line]
    commands = [line for line in commands if "Leaving" not in line]
    assert len(commands) == 1, recipe
    assert "scripts/run_integration.py" in commands[0]
    assert "docker" not in commands[0] and "trap" not in commands[0]


@pytest.mark.parametrize("target", TARGETS)
def test_cleanup_runs_after_pytest_failure(tmp_path: Path, target: str) -> None:
    result = _run_make(tmp_path, target, pytest_exit=1)
    assert result.returncode != 0
    assert "Error 1" in result.stderr
    assert " up -d" in result.log_text
    assert " down -v" in result.log_text
    # A failing test run is not audited; its own status is reported.
    assert _read(tmp_path / "audit.log") == ""


@pytest.mark.parametrize("target", TARGETS)
def test_cleanup_runs_after_pytest_success(tmp_path: Path, target: str) -> None:
    result = _run_make(tmp_path, target, pytest_exit=0)
    assert result.returncode == 0, result.stderr
    project = _project(result)
    assert result.log_text.splitlines() == [
        *_ownership_probes(project),
        f"compose -p {project} up -d",
        f"compose -p {project} ps -q cubrid",
        f"compose -p {project} down -v",
    ]
    assert _read(tmp_path / "audit.log").split() == [
        "scripts/check_integration_lanes.py",
        "--results",
        str(tmp_path / "results.xml"),
    ]


@pytest.mark.parametrize("target", TARGETS)
def test_every_test_skipped_fails_after_cleanup(tmp_path: Path, target: str) -> None:
    """The lane audit fails a run in which every selected test skipped."""
    result = _run_make(tmp_path, target, audit_exit=4)
    assert result.returncode != 0
    assert "Error 4" in result.stderr
    assert " down -v" in result.log_text


@pytest.mark.parametrize("target", TARGETS)
def test_compose_up_failure_still_cleans_up(tmp_path: Path, target: str) -> None:
    result = _run_make(tmp_path, target, up_exit=6)
    assert result.returncode != 0
    assert "Error 6" in result.stderr
    assert " down -v" in result.log_text
    assert _read(tmp_path / "readiness.log") == ""
    assert _read(tmp_path / "pytest.log") == ""


@pytest.mark.parametrize("target", TARGETS)
def test_cleanup_failure_does_not_hide_pytest_failure(tmp_path: Path, target: str) -> None:
    result = _run_make(tmp_path, target, pytest_exit=7, down_exit=1)
    assert result.returncode != 0
    assert " down -v" in result.log_text
    assert "Docker cleanup failed" in result.stderr + result.stdout
    assert "Error 7" in result.stderr


@pytest.mark.parametrize("target", TARGETS)
def test_cleanup_failure_fails_successful_run(tmp_path: Path, target: str) -> None:
    result = _run_make(tmp_path, target, pytest_exit=0, down_exit=3)
    assert result.returncode != 0
    assert "Error 3" in result.stderr
    assert " down -v" in result.log_text
    assert "Docker cleanup failed" in result.stderr + result.stdout


@pytest.mark.parametrize("target", TARGETS)
def test_readiness_failure_still_cleans_up(tmp_path: Path, target: str) -> None:
    result = _run_make(tmp_path, target, readiness_exit=9)
    assert result.returncode != 0
    assert "Error 9" in result.stderr
    assert " down -v" in result.log_text
    assert _read(tmp_path / "pytest.log") == ""


@pytest.mark.parametrize("target", TARGETS)
def test_cleanup_removes_the_run_owned_volumes(tmp_path: Path, target: str) -> None:
    result = _run_make(tmp_path, target)
    assert result.returncode == 0
    assert " down -v" in result.log_text


def _compose_calls(result: subprocess.CompletedProcess[str]) -> list[str]:
    return [line for line in result.log_text.splitlines() if line.startswith("compose ")]


@pytest.mark.parametrize("target", TARGETS)
def test_every_compose_call_carries_a_run_unique_project(tmp_path: Path, target: str) -> None:
    projects = []
    for name in ("a", "b"):
        (tmp_path / name).mkdir()
        result = _run_make(tmp_path / name, target)
        assert result.returncode == 0, result.stderr
        project = _project(result)
        assert project.startswith("pycubrid-it-")
        calls = _compose_calls(result)
        assert calls and all(call.startswith(f"compose -p {project} ") for call in calls)
        projects.append(project)
    assert projects[0] != projects[1]


@pytest.mark.parametrize("target", TARGETS)
def test_project_is_overridable_and_ignores_compose_project_name(
    tmp_path: Path, target: str
) -> None:
    env = {"COMPOSE_PROJECT_NAME": "manual-lab"}
    result = _run_make(tmp_path, target, extra_env=env, make_args=["INTEGRATION_PROJECT=it-mine"])
    assert result.returncode == 0, result.stderr
    assert _project(result) == "it-mine"
    calls = _compose_calls(result)
    assert len(calls) == 3
    assert all(call.startswith("compose -p it-mine ") for call in calls)
    assert "manual-lab" not in result.log_text


@pytest.mark.parametrize("target", TARGETS)
def test_cleanup_touches_only_the_run_owned_project(tmp_path: Path, target: str) -> None:
    result = _run_make(tmp_path, target, pytest_exit=1, make_args=["INTEGRATION_PROJECT=it-mine"])
    assert result.returncode != 0
    downs = [line for line in result.log_text.splitlines() if " down" in line]
    assert downs == ["compose -p it-mine down -v"]
    # Nothing else removes resources, and no call lacks the project.
    assert not any(" rm" in line or " prune" in line for line in result.log_text.splitlines())


@pytest.mark.parametrize("target", TARGETS)
def test_existing_project_is_refused_and_left_alone(tmp_path: Path, target: str) -> None:
    result = _run_make(
        tmp_path,
        target,
        extra_env={"REVIEW_EXISTING": CONTAINER},
        make_args=["INTEGRATION_PROJECT=manual"],
    )
    assert result.returncode != 0
    assert "Refusing to run" in result.stderr
    assert _compose_calls(result) == []
    assert _read(tmp_path / "readiness.log") == ""
    assert _read(tmp_path / "pytest.log") == ""


@pytest.mark.parametrize("target", TARGETS)
def test_invalid_project_name_is_rejected_before_docker(tmp_path: Path, target: str) -> None:
    result = _run_make(tmp_path, target, make_args=["INTEGRATION_PROJECT=Bad Name"])
    assert result.returncode != 0
    assert "INTEGRATION_PROJECT must match" in result.stderr
    assert result.log_text == ""


@pytest.mark.parametrize(
    ("target", "selection"),
    [
        ("integration", ["tests/", "-m", "integration and not tls", "-v"]),
        ("integration-tls", ["tests/test_aio_ssl_integration.py", "-v"]),
    ],
)
def test_each_lane_runs_its_unchanged_test_selection(
    tmp_path: Path, target: str, selection: list[str]
) -> None:
    result = _run_make(tmp_path, target)
    assert result.returncode == 0, result.stderr
    recorded = _read(tmp_path / "pytest.log").rstrip("\n")
    assert recorded == " ".join([*selection, f"--junitxml={tmp_path / 'results.xml'}"])


@pytest.mark.parametrize("target", TARGETS)
def test_readiness_probe_gets_the_compose_container(tmp_path: Path, target: str) -> None:
    # A stray endpoint export must not redirect the run away from the compose service.
    result = _run_make(
        tmp_path,
        target,
        extra_env={"CUBRID_TEST_HOST": "elsewhere", "CUBRID_TEST_URL": "cubrid://x@elsewhere:1/x"},
    )
    assert result.returncode == 0, result.stderr
    assert _read(tmp_path / "readiness.log").splitlines() == [
        f"{CONTAINER}|cubrid://dba@localhost:33522/testdb|localhost|33522|"
        "scripts/wait_for_cubrid.py 36 5"
    ]


@pytest.mark.parametrize("target", TARGETS)
@pytest.mark.parametrize("signal_name", ["SIGINT", "SIGTERM"])
def test_signal_cleanup_removes_the_run_owned_project(
    tmp_path: Path, target: str, signal_name: str
) -> None:
    import signal
    import time

    # Block inside the readiness probe only; the runner is the probe's parent.
    ready = tmp_path / "ready"
    env = _stubs(
        tmp_path,
        readiness=(
            'printf "%s %s" "$$" "$PPID" > "$REVIEW_READY.tmp"\n'
            'mv "$REVIEW_READY.tmp" "$REVIEW_READY"\n'
            "while :; do sleep 0.02; done\n"
        ),
    )
    env["REVIEW_READY"] = str(ready)

    def _default_signals() -> None:
        # A non-interactive shell starts `cmd &` jobs with SIGINT ignored, and the
        # runner keeps an ignored signal ignored, like a shell trap. Reset the
        # dispositions so the runner's handlers are what the test exercises, however
        # pytest itself was launched (#775).
        signal.signal(signal.SIGINT, signal.SIG_DFL)
        signal.signal(signal.SIGTERM, signal.SIG_DFL)

    process = subprocess.Popen(
        _make_args(tmp_path, target),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        start_new_session=True,
        preexec_fn=_default_signals,
    )
    # Upper bounds only: both waits end as soon as the stub or make finishes (well
    # under a second when idle). They are generous so CPU contention from parallel
    # suites cannot turn a slow fork/exec chain into a false failure.
    timeout = 60
    try:
        deadline = time.monotonic() + timeout
        while not ready.exists() and time.monotonic() < deadline:
            time.sleep(0.01)
        assert ready.exists(), "readiness stub did not start"
        child, runner = map(int, ready.read_text().split())
        os.kill(runner, getattr(signal, signal_name))
        stdout, stderr = process.communicate(timeout=timeout)
        assert process.returncode != 0
        assert "Error 130" in stderr, stdout + stderr
        assert f"Interrupted by {signal_name}" in stderr
        log = _read(tmp_path / "docker.log")
        assert " down -v" in log
        assert _read(tmp_path / "pytest.log") == ""
        # The runner stopped the step it was waiting for.
        with pytest.raises(ProcessLookupError):
            os.kill(child, 0)
    finally:
        if process.poll() is None:
            os.killpg(process.pid, signal.SIGKILL)
            process.communicate()


def test_integration_local_without_url_does_not_call_docker(tmp_path: Path) -> None:
    log = tmp_path / "docker.log"
    docker = tmp_path / "docker"
    docker.write_text('#!/bin/sh\nprintf "%s\\n" "$*" >> "$REVIEW_LOG"\nexit 0\n')
    docker.chmod(docker.stat().st_mode | stat.S_IEXEC)
    stub_pytest = tmp_path / "pytest-stub"
    stub_pytest.write_text("#!/bin/sh\nexit 0\n")
    stub_pytest.chmod(stub_pytest.stat().st_mode | stat.S_IEXEC)
    env = dict(os.environ)
    env["PATH"] = str(tmp_path) + os.pathsep + env.get("PATH", "")
    env["REVIEW_LOG"] = str(log)
    env.pop("CUBRID_TEST_URL", None)
    env.pop("CUBRID_TEST_HOST", None)
    result = subprocess.run(
        ["make", "-f", str(MAKEFILE), "integration-local", f"PYTEST={stub_pytest}"],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
    )
    assert result.returncode != 0
    assert not log.exists()


def test_integration_local_with_url_does_not_call_docker(tmp_path: Path) -> None:
    log = tmp_path / "docker.log"
    docker = tmp_path / "docker"
    docker.write_text('#!/bin/sh\nprintf "%s\\n" "$*" >> "$REVIEW_LOG"\nexit 0\n')
    docker.chmod(docker.stat().st_mode | stat.S_IEXEC)
    stub_pytest = tmp_path / "pytest-stub"
    stub_pytest.write_text("#!/bin/sh\nexit 0\n")
    stub_pytest.chmod(stub_pytest.stat().st_mode | stat.S_IEXEC)
    env = dict(os.environ)
    env["PATH"] = str(tmp_path) + os.pathsep + env.get("PATH", "")
    env["REVIEW_LOG"] = str(log)
    env["CUBRID_TEST_URL"] = "cubrid://dba@127.0.0.1:33000/testdb"
    result = subprocess.run(
        ["make", "-f", str(MAKEFILE), "integration-local", f"PYTEST={stub_pytest}"],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0
    assert not log.exists()


def test_compose_registers_the_database_for_a_stable_host() -> None:
    """The image records CUBRID_DB_HOST (else the container ID) on the volume (#1000)."""
    import yaml

    compose = yaml.safe_load((ROOT / "docker-compose.yml").read_text())
    environment = compose["services"]["cubrid"]["environment"]
    assert environment["CUBRID_DB"] == "testdb"
    assert environment["CUBRID_DB_HOST"] == "localhost"


def test_compose_keeps_every_image_database_directory_in_a_named_volume() -> None:
    """Images up to 11.3 keep databases in /var/lib/cubrid, 11.4 elsewhere (#1002)."""
    import yaml

    compose = yaml.safe_load((ROOT / "docker-compose.yml").read_text())
    mounts = dict(volume.split(":", 1)[::-1] for volume in compose["services"]["cubrid"]["volumes"])
    assert mounts == {
        "/var/lib/cubrid": "cubrid-data",
        "/home/cubrid/CUBRID/databases": "cubrid-databases",
    }
    assert set(compose["volumes"]) == {"cubrid-data", "cubrid-databases"}


def test_compose_service_contract() -> None:
    """One ``cubrid`` service, overridable image and port, loopback-only broker (#1006)."""
    import yaml

    compose = yaml.safe_load((ROOT / "docker-compose.yml").read_text())
    assert set(compose["services"]) == {"cubrid"}
    service = compose["services"]["cubrid"]
    assert service["image"] == "cubrid/cubrid:${CUBRID_VERSION:-11.2}"
    assert service["ports"] == ["127.0.0.1:${CUBRID_TEST_PORT:-33000}:33000"]
    # A fixed name would stop two Compose projects from running side by side.
    assert "container_name" not in service
    assert service["healthcheck"]["test"] == [
        "CMD",
        "csql",
        "-u",
        "dba",
        "testdb",
        "-c",
        "SELECT 1",
    ]


@pytest.mark.parametrize(
    ("target", "command"),
    [
        ("docker-up", "compose up -d"),
        ("docker-down", "compose down"),
        ("docker-clean", "compose down -v"),
    ],
)
def test_manual_docker_lifecycle(tmp_path: Path, target: str, command: str) -> None:
    """docker-down keeps the data volumes; only docker-clean deletes them (#1006)."""
    log = tmp_path / "docker.log"
    _executable(
        tmp_path / "docker",
        '#!/bin/sh\nprintf "%s|%s\\n" "$CUBRID_TEST_PORT" "$*" >> "$REVIEW_LOG"\nexit 0\n',
    )
    # Drop inherited ports and outer-make state: `make test CUBRID_TEST_PORT=...` would
    # otherwise reach the nested make through MAKEFLAGS and override the default.
    inherited = ("MAKEFLAGS", "MFLAGS", "MAKEOVERRIDES", "MAKELEVEL")
    env = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith("CUBRID_TEST_") and key not in inherited
    }
    env.update(PATH=str(tmp_path) + os.pathsep + env.get("PATH", ""), REVIEW_LOG=str(log))

    for port_args, port in (([], "33000"), (["CUBRID_TEST_PORT=33522"], "33522")):
        log.unlink(missing_ok=True)
        result = subprocess.run(
            ["make", "-s", "-C", str(ROOT), target, *port_args],
            env=env,
            capture_output=True,
            text=True,
            check=False,
        )
        assert result.returncode == 0, result.stdout + result.stderr
        assert log.read_text().splitlines() == [f"{port}|{command}"]


def test_fix_db_host_rewrites_only_the_testdb_host(tmp_path: Path) -> None:
    """Run the target's in-container script against a local databases.txt (#1000)."""
    databases = tmp_path / "databases"
    databases.mkdir()
    listing = databases / "databases.txt"
    listing.write_text(
        "#db-name\tvol-path\t\tdb-host\t\tlog-path\t\tlob-base-path\n"
        "testdb\t\t/var/lib/cubrid/testdb\tee2bfa1105f9\t/var/lib/cubrid/testdb\t"
        "file:/var/lib/cubrid/testdb/lob\n"
        "otherdb\t/var/lib/cubrid/otherdb\tee2bfa1105f9\t/var/lib/cubrid/otherdb\t"
        "file:/var/lib/cubrid/otherdb/lob\n"
    )
    log = tmp_path / "docker.log"
    docker = tmp_path / "docker"
    # Log the call, then run the `sh -c` script locally as `docker compose exec` would.
    docker.write_text(
        "#!/bin/sh\n"
        'printf "%s\\n" "$*" >> "$REVIEW_LOG"\n'
        'while [ "$#" -gt 0 ] && [ "$1" != "-c" ]; do shift; done\n'
        'exec sh -c "$2"\n'
    )
    docker.chmod(docker.stat().st_mode | stat.S_IEXEC)
    env = dict(os.environ)
    env["PATH"] = str(tmp_path) + os.pathsep + env.get("PATH", "")
    env["REVIEW_LOG"] = str(log)
    env["CUBRID_DATABASES"] = str(databases)
    env["TMPDIR"] = str(tmp_path)

    listing.chmod(0o640)
    os.utime(listing, (1_000_000_000, 1_000_000_000))
    inode = listing.stat().st_ino

    def fix() -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            ["make", "-f", str(MAKEFILE), "docker-fix-db-host"],
            cwd=tmp_path,
            env=env,
            capture_output=True,
            text=True,
        )

    result = fix()

    assert result.returncode == 0, result.stderr
    # Rewritten in place, so the owner and mode the server relies on are kept.
    assert listing.stat().st_ino == inode
    assert stat.S_IMODE(listing.stat().st_mode) == 0o640
    # cp -p: the backup keeps the original's mode and timestamp.
    backup = databases / "databases.txt.before-fix-db-host"
    assert stat.S_IMODE(backup.stat().st_mode) == 0o640
    assert backup.stat().st_mtime == 1_000_000_000
    assert log.read_text().startswith("compose exec -T -u root cubrid sh -c ")
    original = (databases / "databases.txt.before-fix-db-host").read_text()
    assert original.splitlines()[1].split()[2] == "ee2bfa1105f9"
    lines = listing.read_text().splitlines()
    assert lines[0] == "#db-name\tvol-path\t\tdb-host\t\tlog-path\t\tlob-base-path"
    assert lines[1].split() == [
        "testdb",
        "/var/lib/cubrid/testdb",
        "localhost",
        "/var/lib/cubrid/testdb",
        "file:/var/lib/cubrid/testdb/lob",
    ]
    assert lines[2].split()[2] == "ee2bfa1105f9"
    assert "localhost" in result.stdout
    assert sorted(path.name for path in tmp_path.iterdir()) == ["databases", "docker", "docker.log"]
    assert sorted(path.name for path in databases.iterdir()) == [
        "databases.txt",
        "databases.txt.before-fix-db-host",
    ]
    # A second run keeps the first backup, the only record of the stale host.
    assert fix().returncode == 0
    assert backup.read_text() == original


@pytest.mark.parametrize("target", TARGETS)
def test_second_run_of_the_same_project_is_refused_without_docker(
    tmp_path: Path, target: str
) -> None:
    """A concurrent run holding the project lock blocks a same-name run (#1021)."""
    import fcntl
    import tempfile

    project = f"it-lock-{os.getpid()}"
    lock = Path(tempfile.gettempdir()) / f"pycubrid-integration-{project}.lock"
    with open(lock, "w") as held:
        fcntl.flock(held, fcntl.LOCK_EX | fcntl.LOCK_NB)
        result = _run_make(tmp_path, target, make_args=[f"INTEGRATION_PROJECT={project}"])
    lock.unlink(missing_ok=True)
    assert result.returncode != 0
    assert f"another run owns project '{project}'" in result.stderr
    assert result.log_text == ""
    assert _read(tmp_path / "pytest.log") == ""
