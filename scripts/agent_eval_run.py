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

``collect TASK_DIR OUT_DIR [--repo REPO] [--report REPORT] [--meta META]``
    Takes the attempt's uncommitted changes as ``patch.diff``, builds the base
    tree (base commit plus the patch's ``tests/`` files) and the candidate tree
    (base commit plus the whole patch) from ``git archive``, runs the task's
    validation commands, regression tests and acceptance scripts there in the
    sandbox described below, writes ``evidence.json`` and
    redacted logs, grades the candidate with ``scripts/agent_eval.py`` and
    writes ``run.json``. It exits 0 for a passing verdict, 1 for a failing one
    and 2 for unusable input. The task and the repository come from the
    caller, never from OUT_DIR; it assumes no attempt process is still running.

``summarize OUT_DIR...``
    Reports repeated attempts as distributions of verdicts and failed checks
    per task, never as one success rate.

The runner never commits, pushes, merges or publishes anything. Each check
runs in a sandbox: new user, mount, PID and network namespaces
(``unshare``) with a private root that holds only the system directories and
the interpreter read-only, a fresh ``/tmp``, a minimal ``/dev`` and the tree
under test as the only writable path. The caller's home directory, host
sockets, the other tree and the runner's own artifacts are not visible, there
is no network, and the environment is allowlisted. Where the sandbox cannot
be built, ``collect`` refuses to run unless ``--no-sandbox`` is given, which
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
import signal
import stat
import subprocess
import sys
import tarfile
import tempfile
import time
from datetime import UTC, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import agent_eval  # noqa: E402 - the grader next to this script

InputError = agent_eval.InputError
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
UNSHARE = ("unshare", "--net", "--mount", "--map-root-user", "--pid", "--fork", "--kill-child")
SYSTEM_DIRS = ("/usr", "/bin", "/sbin", "/lib", "/lib32", "/lib64", "/libx32", "/etc")
# Runs as root of the new user namespace: builds a private root from the
# read-only system directories, the read-only paths given before "--", a
# fresh /tmp, a minimal /dev and the writable tree, then runs the command
# there. Any failed mount stops it before the command runs.
SANDBOX = r"""
set -eu
root=$1; tree=$2; nproc=$3; shift 3
user_path=$PATH; PATH=/usr/sbin:/sbin:/usr/bin:/bin
mount -t tmpfs -o mode=0755,size=16m none "$root"
for p in SYSTEM_DIRS; do
  if [ -L "$p" ]; then ln -s "$(readlink "$p")" "$root$p"
  elif [ -d "$p" ]; then
    mkdir -p "$root$p"; mount --bind "$p" "$root$p"; mount -o remount,bind,ro "$root$p"
  fi
done
mkdir -p "$root/dev" "$root/proc" "$root/tmp"
mount -t tmpfs -o mode=0755,size=1m none "$root/dev"
for d in null zero full random urandom; do
  touch "$root/dev/$d"; mount --bind "/dev/$d" "$root/dev/$d"
done
ln -s /proc/self/fd "$root/dev/fd"
mount -t proc proc "$root/proc"
mount -t tmpfs -o mode=1777,size=512m none "$root/tmp"
while [ "$1" != "--" ]; do
  case "$1" in
    link:*)  # a symlink of the interpreter chain outside the bound directories
      spec=${1#link:}; path=${spec%%=*}
      mkdir -p "$root$(dirname "$path")"; ln -s "${spec#*=}" "$root$path";;
    *) mkdir -p "$root$1"; mount --bind "$1" "$root$1"; mount -o remount,bind,ro "$root$1";;
  esac
  shift
done
shift
mkdir -p "$root$tree"; mount --bind "$tree" "$root$tree"
# Limit processes, address space and file size, then drop every capability
# of the namespace root and forbid regaining any before the check starts, so
# it can neither chroot out nor remount.
exec chroot "$root" /usr/bin/prlimit --nproc="$nproc" --as=8589934592 --fsize=2147483648 -- \
  /usr/bin/setpriv --bounding-set=-all --inh-caps=-all --ambient-caps=-all --no-new-privs -- \
  /usr/bin/env PATH="$user_path" /bin/sh -c 'cd "$0" && exec "$@"' "$tree" "$@"
""".replace("SYSTEM_DIRS", " ".join(SYSTEM_DIRS))


# Host-side Git ignores the caller's global and system configuration and
# attributes, so an attempt's .gitattributes cannot select a filter driver
# that the caller configured (such as Git LFS).
GIT_ENV = {"GIT_CONFIG_GLOBAL": os.devnull, "GIT_CONFIG_NOSYSTEM": "1", "GIT_ATTR_NOSYSTEM": "1"}


def _git(*args: str, cwd: Path | None = None) -> str:
    result = subprocess.run(
        ["git", *args],
        cwd=cwd,
        env={**os.environ, **GIT_ENV},
        capture_output=True,
        text=True,
        check=False,
    )
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
        "repo": str(repo.resolve()),
        "prepared_at": _now(),
        "prepared_epoch": time.time(),
    }
    (out_dir / "attempt.json").write_text(json.dumps(attempt, indent=2) + "\n", encoding="utf-8")
    # The runner's own record of when the attempt started, outside OUT_DIR,
    # which the attempt can write (#985).
    record = _prepare_record(out_dir)
    _state_directory(record.parent)
    data = {"task_digest": attempt["task_digest"], "prepared_epoch": attempt["prepared_epoch"]}
    _write_record(record, (json.dumps(data) + "\n").encode())
    return worktree


def _state_directory(path: Path) -> None:
    """Create the runner's state directory, or check and tighten an existing one."""
    path.mkdir(parents=True, exist_ok=True, mode=0o700)
    info = os.lstat(path)
    if not stat.S_ISDIR(info.st_mode):
        raise InputError(f"{path}: the runner state directory is not a directory")
    if info.st_uid != os.getuid():
        raise InputError(f"{path}: the runner state directory belongs to another user")
    if stat.S_IMODE(info.st_mode) != stat.S_IRWXU:
        # Owner-only and complete (0700): neither more for others nor less
        # for the runner, which must write the record. Checked above: a
        # directory, not a link.
        os.chmod(path, stat.S_IRWXU)


def _write_record(record: Path, data: bytes) -> None:
    """Create the start record; never follow or write through an existing entry.

    OUT_DIR was new or empty, so an entry already at this path is a stale
    record of an earlier attempt there, or something planted. It is removed
    (a symlink itself, never its target) and the record created afresh.
    """
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW
    try:
        descriptor = os.open(record, flags, 0o600)
    except FileExistsError:
        if stat.S_ISDIR(os.lstat(record).st_mode):
            raise InputError(f"{record}: a directory is in the way of the start record") from None
        os.unlink(record)
        try:
            descriptor = os.open(record, flags, 0o600)
        except FileExistsError as exc:
            raise InputError(f"{record}: replaced again while being written") from exc
    with os.fdopen(descriptor, "wb") as handle:
        handle.write(data)


def _prepare_record(out_dir: Path) -> Path:
    """Where ``prepare`` records the start of the attempt in ``out_dir``."""
    base = os.environ.get("AGENT_EVAL_STATE_DIR") or os.environ.get("XDG_STATE_HOME")
    state = Path(base) if base else Path.home() / ".local" / "state"
    if not os.environ.get("AGENT_EVAL_STATE_DIR"):
        state = state / "pycubrid-agent-eval"
    key = hashlib.sha256(str(out_dir.resolve()).encode()).hexdigest()
    return state / f"{key}.json"


def _epoch(value: object, now: float) -> float | None:
    """A start time that is a finite number not later than now, else ``None``."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    # Compare first: an int too large for a float, NaN and infinity all fail
    # here without raising.
    if not 0 < value <= now:
        return None
    return float(value)


def _started_at(
    out_dir: Path, attempt: dict, task_digest: str, now: float
) -> tuple[float | None, str]:
    """When the attempt started, and where that came from.

    The runner's own record wins. Without it (prepared elsewhere), the
    attempt-writable ``attempt.json`` value is used only if it is a valid time,
    and the source says so; otherwise the duration is unknown.
    """
    try:
        record = json.loads(_read_regular(_prepare_record(out_dir), limit=4096))
    except (InputError, ValueError):
        record = None  # missing, a symlink, not a regular file, too large or not JSON
    if isinstance(record, dict) and record.get("task_digest") == task_digest:
        started = _epoch(record.get("prepared_epoch"), now)
        if started is not None:
            return started, "runner"
    started = _epoch(attempt.get("prepared_epoch"), now)
    return (started, "attempt.json") if started is not None else (None, UNKNOWN)


def _patch(worktree_fd: int, repo: Path, base_sha: str, scratch: Path) -> str:
    """The attempt's changes against the base, new files included, as ``git diff`` text.

    Git never runs in the attempt's checkout, whose configuration the attempt
    controls (``core.fsmonitor``, filters, external diff and textconv
    drivers). Its files, without ``.git``, are copied from the held directory
    descriptor over a fresh depth-1 clone of the base, whose configuration is
    the runner's, and diffed there. Only regular files and symlinks (as
    links) are copied; a FIFO, socket or device is unusable input.
    """
    _git("init", "-q", str(scratch))
    _git("fetch", "-q", "--depth", "1", repo.resolve().as_uri(), base_sha, cwd=scratch)
    _git("checkout", "-q", "--detach", base_sha, cwd=scratch)
    for entry in scratch.iterdir():
        if entry.name != ".git":
            shutil.rmtree(entry) if entry.is_dir() and not entry.is_symlink() else entry.unlink()

    try:
        _copy_held(worktree_fd, scratch)
    except OSError as exc:
        raise InputError(f"the checkout cannot be copied: {exc}") from exc
    _git("add", "-A", cwd=scratch)
    return _git(
        "diff",
        "--cached",
        "--no-color",
        "--no-renames",
        "--no-ext-diff",
        "--no-textconv",
        base_sha,
        cwd=scratch,
    )


def _copy_held(worktree_fd: int, destination: Path) -> None:
    """Copy the held checkout, without ``.git``, using descriptors only.

    ``os.fwalk`` opens each directory relative to its parent's descriptor and
    skips one that was swapped for something else, and every file is opened
    relative to its directory's descriptor without following a link, so a
    path replaced during the copy can never pull in content from outside the
    checkout. Symlinks are kept as links; any other non-regular file is
    unusable input.
    """

    def refuse(error: OSError) -> None:
        raise error  # a directory that cannot be read would drop its changes silently

    for directory, subdirectories, files, directory_fd in os.fwalk(
        ".", dir_fd=worktree_fd, follow_symlinks=False, onerror=refuse
    ):
        target = destination / directory
        target.mkdir(exist_ok=True)
        # fwalk lists a symlink to a directory among the subdirectories without
        # descending into it; copy it as a link and keep it out of the walk.
        walk = []
        for name in subdirectories:
            if name == ".git":
                continue
            if stat.S_ISLNK(os.stat(name, dir_fd=directory_fd, follow_symlinks=False).st_mode):
                os.symlink(os.readlink(name, dir_fd=directory_fd), target / name)
            else:
                walk.append(name)
        subdirectories[:] = walk
        for name in files:
            if name == ".git":
                continue
            info = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
            if stat.S_ISLNK(info.st_mode):
                os.symlink(os.readlink(name, dir_fd=directory_fd), target / name)
                continue
            source = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory_fd)
            try:
                held = os.fstat(source)
                if not stat.S_ISREG(held.st_mode):
                    raise InputError(
                        f"the checkout holds a file that is not a regular file: {directory}/{name}"
                    )
                with (
                    os.fdopen(os.dup(source), "rb") as reader,
                    open(target / name, "xb") as writer,
                ):
                    shutil.copyfileobj(reader, writer)
                os.chmod(target / name, stat.S_IMODE(held.st_mode))
            finally:
                os.close(source)


def _sections(patch: str, keep: str) -> str:
    """The file sections of ``patch`` whose path starts with ``keep``."""
    parts = re.split(r"(?m)^(?=diff --git )", patch)
    return "".join(part for part in parts if part.startswith(f"diff --git a/{keep}"))


def _tree(worktree: Path, base_sha: str, dest: Path, patch: str) -> None:
    archive = subprocess.run(
        ["git", "archive", "--format=tar", base_sha],
        cwd=worktree,
        env={**os.environ, **GIT_ENV},
        capture_output=True,
        check=True,
    ).stdout
    dest.mkdir(parents=True)
    with tarfile.open(fileobj=io.BytesIO(archive)) as tar:
        members = tar.getmembers()
        for member in members:
            # The archive is the attempt checkout's base commit; refuse any
            # entry that would land outside dest anyway.
            name = Path(member.name)
            if name.is_absolute() or ".." in name.parts or member.islnk() or member.isdev():
                raise InputError(f"unsafe path in the base commit archive: {member.name}")
            if member.issym() and (
                Path(member.linkname).is_absolute() or ".." in Path(member.linkname).parts
            ):
                raise InputError(f"unsafe link in the base commit archive: {member.name}")
        if hasattr(tarfile, "data_filter"):
            tar.extractall(dest, members=members, filter="data")
        else:  # pragma: no cover - Python 3.11 before 3.11.4
            tar.extractall(dest, members=members)  # noqa: S202
    if patch:
        # The tree is not a repository: without the ceiling, git apply would
        # find an enclosing one and skip paths outside its subdirectory.
        env = {**os.environ, **GIT_ENV, "GIT_CEILING_DIRECTORIES": str(dest.parent.resolve())}
        applied = subprocess.run(
            ["git", "apply", "--whitespace=nowarn", "-"],
            cwd=dest,
            env=env,
            input=patch.encode(),
            capture_output=True,
            check=False,
        )
        if applied.returncode != 0:
            raise InputError(f"the patch does not apply to {dest.name}: {applied.stderr.decode()}")


def _environment(home: Path, tree: Path, python: Path, sandboxed: bool) -> dict[str, str]:
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
            "HOME": "/tmp" if sandboxed else str(home),
            "PYTHONPATH": str(tree),
            "PYTHONDONTWRITEBYTECODE": "1",
            "PIP_NO_INDEX": "1",
            "http_proxy": "http://127.0.0.1:9",
            "https_proxy": "http://127.0.0.1:9",
        }
    )
    return env


def _readonly_paths(python: Path, task_dir: Path, home: Path) -> list[str]:
    """What a sandboxed check reads beyond the system directories.

    The interpreter's prefixes, the task directory and the wrapper directory
    are bound read-only; each symlink on the way from ``python`` to the real
    interpreter outside them is recreated (``link:PATH=TARGET``) rather than
    bound, so a virtual environment still finds its base interpreter.
    """
    probe = (
        "import os, sys; print(sys.prefix); print(sys.base_prefix); "
        "print(os.path.realpath(sys.executable))"
    )
    found = subprocess.run(
        [str(python), "-c", probe], capture_output=True, text=True, check=False
    ).stdout.split("\n")
    interpreter = [*found[:2], str(Path(found[2]).parent.parent)] if len(found) > 2 else []
    for path in interpreter:
        _check_interpreter_path(Path(path))
    paths = [*interpreter, str(task_dir.resolve()), str((home / "bin").resolve())]
    system = tuple(f"{d}/" for d in SYSTEM_DIRS)
    unique: list[str] = []
    for path in paths:
        if path and not (path + "/").startswith(system) and path not in unique:
            unique.append(path)
    covered = (*system, *(f"{path}/" for path in unique))
    # Resolve python one component at a time, so directory symlinks count too.
    parts, current = list(Path(python).parts[1:]), Path("/")
    for _ in range(256):
        if not parts:
            break
        step = current / parts.pop(0)
        if not step.is_symlink():
            current = step
            continue
        target = os.readlink(step)
        if not str(step).startswith(covered):
            unique.append(f"link:{step}={target}")
        parts = [*Path(os.path.normpath(current / target)).parts[1:], *parts]
        current = Path("/")
    return unique


# Interpreter installs under the home directory that hold only Python.
KNOWN_INSTALLS = (
    ".local/share/uv/python",
    ".pyenv/versions",
    ".local/share/mise/installs",
    ".asdf/installs",
    ".rye/py",
    ".local/share/hatch/pythons",
)


def _check_interpreter_path(path: Path) -> None:
    """Refuse to bind an interpreter prefix that would expose the home directory."""
    home, path = Path.home().resolve(), path.resolve()
    if path == home or path in home.parents:
        raise InputError(f"the interpreter prefix {path} contains the home directory {home}")
    if home not in path.parents:
        return
    known = any(path == home / d or home / d in path.parents for d in KNOWN_INSTALLS)
    if not known and not (path / "pyvenv.cfg").is_file():
        raise InputError(
            f"the interpreter prefix {path} is inside the home directory and is neither a "
            "virtual environment nor a known interpreter install; pass --python from another "
            "location"
        )


def _command(argv: list[str], tree: Path, sandbox: tuple[Path, list[str]] | None) -> list[str]:
    if sandbox is None:
        return argv
    root, readonly = sandbox
    nproc = str(_user_processes() + 1024)  # RLIMIT_NPROC counts the caller's processes too
    return [
        *UNSHARE,
        "sh",
        "-c",
        SANDBOX,
        "sandbox",
        str(root),
        str(tree),
        nproc,
        *readonly,
        "--",
        *argv,
    ]


def _user_processes() -> int:
    uid, count = str(os.getuid()), 0
    for status in Path("/proc").glob("[0-9]*/status"):
        try:
            text = status.read_text()
        except OSError:
            continue
        if any(line.split()[1:2] == [uid] for line in text.splitlines() if line.startswith("Uid:")):
            count += 1
    return count


def _execute(argv: list[str], cwd: Path, env: dict[str, str], timeout: int) -> tuple[int, str]:
    """Run one check in its own process group; on timeout, kill the whole group."""
    try:
        process = subprocess.Popen(
            argv,
            cwd=cwd,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
    except OSError as exc:
        return 127, f"{exc}\n"
    try:
        output, _ = process.communicate(timeout=timeout)
        return process.returncode, output.decode(errors="replace")
    except subprocess.TimeoutExpired:
        os.killpg(process.pid, signal.SIGKILL)
        try:
            output, _ = process.communicate(timeout=10)
        except subprocess.TimeoutExpired:
            # Unsandboxed, a process that left the group (setsid) can keep the
            # output pipe open; stop reading instead of waiting for it.
            if process.stdout is not None:
                process.stdout.close()
            process.wait()
            output = b""
        return 124, output.decode(errors="replace") + f"\n[timeout {timeout}s]\n"


def _sandbox(scratch: Path, readonly: list[str], disabled: bool) -> tuple[Path, list[str]] | None:
    """The sandbox root and read-only paths, or ``None`` with ``--no-sandbox``."""
    if disabled:
        return None
    root = scratch.resolve() / "sandbox-root"
    probe_tree = scratch.resolve() / "sandbox-probe"
    root.mkdir()
    probe_tree.mkdir()
    code, output = _execute(
        _command(["true"], probe_tree, (root, readonly)), probe_tree, {"PATH": "/usr/bin:/bin"}, 60
    )
    probe_tree.rmdir()
    if code != 0:
        raise InputError(
            "the sandbox (unshare user, mount, PID and network namespaces) cannot be built "
            f"here: {output.strip()}; pass --no-sandbox to run without it, which run.json records"
        )
    return root, readonly


READ_LIMIT = 16 * 1024 * 1024
DIRECTORY = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW


def _read_regular(path: str | Path, dir_fd: int | None = None, limit: int = READ_LIMIT) -> bytes:
    """Read a regular file without following a final symlink or blocking on a FIFO."""
    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=dir_fd)
    except OSError as exc:
        raise InputError(f"{path}: cannot be opened ({exc.strerror})") from exc
    try:
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode):
            raise InputError(f"{path}: not a regular file")
        chunks, size = [], 0
        while chunk := os.read(descriptor, 65536):
            size += len(chunk)
            if size > limit:
                raise InputError(f"{path}: larger than {limit} bytes")
            chunks.append(chunk)
        return b"".join(chunks)
    finally:
        os.close(descriptor)


def _open_directory(path: str | Path, dir_fd: int | None = None) -> int:
    try:
        return os.open(path, DIRECTORY, dir_fd=dir_fd)
    except OSError as exc:
        raise InputError(f"{path}: not a directory, or a symlink ({exc.strerror})") from exc


def _write_at(dir_fd: int, name: str, data: bytes) -> None:
    """Create ``name`` in the held directory; never follow or replace an entry."""
    try:
        descriptor = os.open(
            name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=dir_fd
        )
    except FileExistsError as exc:
        raise InputError(f"{name}: already exists; collect already ran for this attempt") from exc
    with os.fdopen(descriptor, "wb") as handle:
        handle.write(data)


def _publish(out_fd: int, name: str, files: dict[str, bytes]) -> None:
    """Create the directory ``name`` in OUT_DIR and write ``files`` into it by descriptor."""
    try:
        os.mkdir(name, 0o700, dir_fd=out_fd)
    except FileExistsError as exc:
        raise InputError(f"{name}: already exists; collect already ran for this attempt") from exc
    directory = _open_directory(name, dir_fd=out_fd)
    try:
        for file_name, data in files.items():
            _write_at(directory, file_name, data)
    finally:
        os.close(directory)


def collect(
    task_dir: Path,
    out_dir: Path,
    repo: Path = ROOT,
    report_path: Path | None = None,
    meta_path: Path | None = None,
    python: Path = Path(sys.executable),
    timeout: int = 900,
    no_sandbox: bool = False,
    keep_trees: bool = False,
) -> dict:
    """Run the attempt's checks and grade it; return ``run.json``.

    The task and the source repository come from the caller, never from
    OUT_DIR, which the attempt can write: ``attempt.json`` only has to agree
    with them. Every output is built in a private directory, graded there and
    then written into OUT_DIR through held directory descriptors.
    """
    started = time.time()
    python = python.absolute()  # absolute, not resolved: a venv keeps its path
    task_dir = task_dir.resolve()  # acceptance scripts run from inside the trees
    task = agent_eval.load_task(task_dir)
    task_digest = _digest(task_dir / "task.toml")
    base_sha = task["base_sha"]
    meta = load_meta(meta_path)
    out_fd = _open_directory(out_dir.absolute())
    try:
        for name in ("candidate", "logs", "grade.json", "run.json"):
            try:
                os.lstat(name, dir_fd=out_fd)
            except FileNotFoundError:
                continue
            raise InputError(f"{out_dir / name}: already exists; collect already ran")
        attempt_bytes = _read_regular("attempt.json", dir_fd=out_fd)
        try:
            attempt = json.loads(attempt_bytes)
        except ValueError as exc:
            raise InputError(f"{out_dir}/attempt.json: {exc}") from exc
        if (
            not isinstance(attempt, dict)
            or attempt.get("format") != agent_eval.FORMAT
            or attempt.get("task_digest") != task_digest
            or attempt.get("base_sha") != base_sha
        ):
            raise InputError(f"{out_dir}/attempt.json: not made by prepare for this task")
        if report_path is None:
            report_bytes = _read_regular("report.json", dir_fd=out_fd)
        else:
            report_bytes = _read_regular(report_path)
        worktree_fd = _open_directory("worktree", dir_fd=out_fd)
        try:
            git_fd = _open_directory(".git", dir_fd=worktree_fd)
            try:
                head = _read_regular("HEAD", dir_fd=git_fd, limit=4096).decode(errors="replace")
            finally:
                os.close(git_fd)
            return _collect(
                task,
                task_dir,
                task_digest,
                out_dir,
                out_fd,
                worktree_fd,
                repo,
                attempt,
                attempt_bytes,
                report_bytes,
                head.strip(),
                meta,
                python,
                timeout,
                no_sandbox,
                keep_trees,
                started,
            )
        finally:
            os.close(worktree_fd)
    finally:
        os.close(out_fd)


def _collect(
    task: dict,
    task_dir: Path,
    task_digest: str,
    out_dir: Path,
    out_fd: int,
    worktree_fd: int,
    repo: Path,
    attempt: dict,
    attempt_bytes: bytes,
    report_bytes: bytes,
    head: str,
    meta: dict,
    python: Path,
    timeout: int,
    no_sandbox: bool,
    keep_trees: bool,
    started: float,
) -> dict:
    base_sha = task["base_sha"]
    # Every scratch path lives in a new private directory; nothing in OUT_DIR
    # is written until the grade is done.
    scratch = Path(tempfile.mkdtemp(prefix="agent-eval-"))
    trees, home, clone = scratch / "trees", scratch / "home", scratch / "patch-repo"
    candidate = scratch / "candidate"
    try:
        candidate.mkdir()
        (candidate / "report.json").write_bytes(report_bytes)
        agent_eval.load_report(candidate / "report.json")  # unusable reports stop here
        _environment(home, trees, python, sandboxed=False)  # writes the python wrappers
        sandbox = _sandbox(scratch, _readonly_paths(python, task_dir, home), no_sandbox)
        held = os.fstat(worktree_fd)
        patch = _patch(worktree_fd, repo, base_sha, clone)
        try:
            now = os.lstat("worktree", dir_fd=out_fd)
        except OSError:
            now = None
        if now is None or (now.st_dev, now.st_ino) != (held.st_dev, held.st_ino):
            raise InputError(f"{out_dir}/worktree was replaced while it was being copied")
        (candidate / "patch.diff").write_text(patch, encoding="utf-8")
        _tree(clone, base_sha, trees / "base", _sections(patch, "tests/"))
        _tree(clone, base_sha, trees / "candidate", patch)
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

        evidence, timings, logs = [], [], {}
        checked: set[str] = set()
        env: dict[str, str] = {}
        for number, (tree_name, command, argv) in enumerate(plan, start=1):
            tree = (trees / tree_name).resolve()
            env = _environment(home, tree, python, sandboxed=sandbox is not None)
            if tree_name not in checked:
                _check_import(tree, env, sandbox, timeout)
                checked.add(tree_name)
            began = time.time()
            code, output = _execute(_command(argv, tree, sandbox), tree, env, timeout)
            name = f"{number:02d}-{tree_name}.log"
            text = redact(f"$ {command}\n{output}\n[exit {code}]\n")
            logs[name] = text.replace(str(Path.home()), "~").encode()
            evidence.append({"command": command, "tree": tree_name, "exit_code": code})
            timings.append({"log": name, "seconds": round(time.time() - began, 1)})
        (candidate / "evidence.json").write_text(
            json.dumps(evidence, indent=2) + "\n", encoding="utf-8"
        )
        grade = agent_eval.grade(task_dir, candidate)
        graded = {name: (candidate / name).read_bytes() for name in sorted(os.listdir(candidate))}
    except BaseException:
        shutil.rmtree(scratch, ignore_errors=True)
        raise
    for path in (home, clone, scratch / "sandbox-root", candidate):
        shutil.rmtree(path, ignore_errors=True)  # runner-owned scratch, not artifacts
    if not keep_trees:
        shutil.rmtree(scratch, ignore_errors=True)

    grade_text = agent_eval.dumps(grade)
    began, duration_source = _started_at(out_dir, attempt, task_digest, started)
    minutes: float | str = UNKNOWN if began is None else round((started - began) / 60, 1)
    max_minutes = task.get("limits", {}).get("max_minutes")
    if max_minutes is None:
        over: bool | str = False
    elif isinstance(minutes, str):
        over = UNKNOWN  # never hidden as False
    else:
        over = minutes > max_minutes
    checks = grade["checks"]
    result = {
        "format": RUNNER_VERSION,
        "task": task["id"],
        "task_digest": task_digest,
        "base_sha": base_sha,
        **meta,
        "prompt_id": meta["prompt_id"] if meta["prompt_id"] != UNKNOWN else task["id"],
        "prepared_at": str(attempt.get("prepared_at", UNKNOWN)),
        "collected_at": _now(),
        "attempt_minutes": minutes,
        "over_max_minutes": over,
        "duration_source": duration_source,
        "sandbox": "none" if sandbox is None else "namespaces",
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
    # The digests of exactly the bytes the runner read, graded and published.
    published = {
        "attempt.json": attempt_bytes,
        "report.json": report_bytes,
        "grade.json": grade_text.encode(),
        **{f"candidate/{name}": data for name, data in graded.items()},
        **{f"logs/{name}": data for name, data in logs.items()},
    }
    result["artifacts"] = {
        name: hashlib.sha256(data).hexdigest() for name, data in sorted(published.items())
    }
    if keep_trees:
        result["kept_trees"] = str(trees)
    _publish(out_fd, "candidate", graded)
    _publish(out_fd, "logs", logs)
    _write_at(out_fd, "grade.json", grade_text.encode())
    _write_at(out_fd, "run.json", (json.dumps(result, indent=2, sort_keys=True) + "\n").encode())
    _prepare_record(out_dir).unlink(missing_ok=True)
    return result


def _check_import(
    tree: Path, env: dict[str, str], sandbox: tuple[Path, list[str]] | None, timeout: int
) -> None:
    """Fail when the tree's own ``pycubrid`` would be shadowed by an installed one."""
    if not (tree / "pycubrid" / "__init__.py").is_file():
        return
    argv = ["python", "-c", "import pycubrid; print(pycubrid.__file__)"]
    code, output = _execute(_command(argv, tree, sandbox), tree, env, timeout)
    if code == 124:
        raise InputError(f"importing pycubrid from {tree} took longer than {timeout}s")
    found = output.strip().splitlines()[-1] if output.strip() else ""
    if code != 0 or not found or tree not in Path(found).resolve().parents:
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
    col.add_argument("task_dir", type=Path)
    col.add_argument("out_dir", type=Path)
    col.add_argument("--repo", type=Path, default=ROOT, help="the source repository")
    col.add_argument("--report", type=Path, help="default: OUT_DIR/report.json")
    col.add_argument("--meta", type=Path)
    col.add_argument("--python", type=Path, default=Path(sys.executable))
    col.add_argument("--timeout", type=int, default=900, help="seconds per command")
    col.add_argument("--no-sandbox", action="store_true", help="run checks unsandboxed")
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
            args.task_dir,
            args.out_dir,
            args.repo,
            args.report,
            args.meta,
            args.python,
            args.timeout,
            args.no_sandbox,
            args.keep_trees,
        )
    except InputError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    print(result["verdict"])
    return 0 if result["verdict"] == "pass" else 1


if __name__ == "__main__":
    sys.exit(main())
