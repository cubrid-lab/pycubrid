"""Every live CI job runs the Make/Compose contract or documents why not (#1023).

A job "talks to a live CUBRID" when it declares a ``cubrid/cubrid`` service container
or a step starts one with ``docker run``. Such a job either executes ``make integration``
or ``make integration-tls`` (optionally with ``VAR=value`` arguments) or carries a ``# Not using make integration because <reason>`` comment in its workflow
block and a row in the docs/DEVELOPMENT.md (and docs/ko/DEVELOPMENT.md) table.
"""

from __future__ import annotations

import re
import shlex
from pathlib import Path
from typing import Any

import pytest
import yaml

pytestmark = pytest.mark.repo_tooling

ROOT = Path(__file__).resolve().parents[1]
WORKFLOWS = ROOT / ".github/workflows"
DOCS = [ROOT / "docs/DEVELOPMENT.md", ROOT / "docs/ko/DEVELOPMENT.md"]
REASON = re.compile(r"^    # Not using make integration because \S", re.MULTILINE)
TARGETS = {"integration", "integration-tls"}
ASSIGNMENT = re.compile(r"[A-Za-z_][A-Za-z0-9_]*=")
SEPARATORS = {";", "&&", "||", "|", "&", "(", ")"}
ROW = re.compile(r"^\| `([\w.-]+\.yml)` \| `([\w-]+)` \|", re.MULTILINE)


def _is_live(job: dict[str, Any]) -> bool:
    services = job.get("services") or {}
    if any("cubrid/cubrid" in str(s.get("image", "")) for s in services.values()):
        return True
    return any("cubrid/cubrid" in str(s.get("run", "")) for s in job.get("steps") or [])


def _job_block(text: str, job: str) -> str:
    """Return the raw lines of one job, comments included, up to the next job key."""
    start = re.search(rf"^  {re.escape(job)}:\s*$", text, re.MULTILINE)
    assert start, job
    end = re.search(r"^ {0,2}\S", text[start.end() :], re.MULTILINE)
    return text[start.end() : start.end() + end.start()] if end else text[start.end() :]


def _live_jobs() -> list[tuple[str, str, dict[str, Any], str]]:
    found = []
    for path in sorted(WORKFLOWS.glob("*.yml")):
        text = path.read_text()
        for name, job in (yaml.safe_load(text).get("jobs") or {}).items():
            if _is_live(job):
                found.append((path.name, name, job, _job_block(text, name)))
    return found


def _invokes_make_target(script: str) -> bool:
    """Return True when a shell command of ``script`` is ``make integration[-tls]``.

    Only the command word counts: ``echo "make integration"``, a ``#`` comment and
    other targets such as ``make integration-local`` do not. Leading and trailing
    ``VAR=value`` words are allowed; any other argument or flag is not.
    """
    for line in script.replace("\\\n", " ").splitlines():
        lexer = shlex.shlex(line, posix=True, punctuation_chars=True)
        lexer.whitespace_split = True
        try:
            tokens = list(lexer)
        except ValueError:  # unbalanced quotes: not a command this check can trust
            continue
        command: list[str] = []
        for token in [*tokens, ";"]:
            if token not in SEPARATORS:
                command.append(token)
                continue
            words = [w for w in command if not ASSIGNMENT.match(w)]
            while command and ASSIGNMENT.match(command[0]):
                command.pop(0)
            if command[:1] == ["make"] and len(words) == 2 and words[1] in TARGETS:
                return True
            command = []
    return False


def _runs_make(job: dict[str, Any]) -> bool:
    return any(_invokes_make_target(str(s.get("run", ""))) for s in job.get("steps") or [])


@pytest.mark.parametrize(
    "script",
    [
        "make integration",
        "make integration-tls",
        "make integration CUBRID_TEST_PORT=33522 INTEGRATION_RESULTS=out/junit.xml",
        "CUBRID_VERSION=10.2 make integration",
        "set -e\nmake integration-tls && echo done",
        "make \\\n  integration CUBRID_TEST_PORT=33522",
    ],
)
def test_make_integration_invocations_are_recognised(script: str) -> None:
    assert _invokes_make_target(script)


@pytest.mark.parametrize(
    "script",
    [
        "make integration-local",
        "make integration-tlsx",
        'echo "make integration"',
        "echo make integration",
        "# make integration",
        "python -m pytest  # replaces make integration",
        "make -n integration",
        "make integration test",
        "make lint",
        "",
    ],
)
def test_other_commands_are_not_make_integration(script: str) -> None:
    assert not _invokes_make_target(script)


def test_live_jobs_are_detected() -> None:
    names = {(wf, name) for wf, name, _, _ in _live_jobs()}
    assert ("ci.yml", "integration-tests") in names
    assert ("integration-full.yml", "integration-tls") in names  # docker run, no services


@pytest.mark.parametrize(
    ("wf", "name", "job", "block"),
    [pytest.param(*j, id=f"{j[0]}:{j[1]}") for j in _live_jobs()],
)
def test_live_job_runs_make_integration_or_documents_why_not(
    wf: str, name: str, job: dict[str, Any], block: str
) -> None:
    assert _runs_make(job) or REASON.search(block), (
        f"{wf}:{name} talks to a live CUBRID; run `make integration` or add a "
        "'# Not using make integration because ...' comment and a docs/DEVELOPMENT.md row"
    )


@pytest.mark.parametrize("doc", DOCS, ids=lambda p: str(p.relative_to(ROOT)))
def test_documented_jobs_match_the_live_jobs_that_keep_their_own_steps(doc: Path) -> None:
    kept = {(wf, name) for wf, name, job, _ in _live_jobs() if not _runs_make(job)}
    rows = ROW.findall(doc.read_text())
    assert len(rows) == len(set(rows)), "duplicate rows"
    assert set(rows) == kept
