"""Every live CI job runs the Make/Compose contract or documents why not (#1023).

A job "talks to a live CUBRID" when it declares a ``cubrid/cubrid`` service container
or a step starts one with ``docker run``. Such a job either runs ``make integration*``
or carries a ``# Not using make integration because <reason>`` comment in its workflow
block and a row in the docs/DEVELOPMENT.md (and docs/ko/DEVELOPMENT.md) table.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import pytest
import yaml

pytestmark = pytest.mark.repo_tooling

ROOT = Path(__file__).resolve().parents[1]
WORKFLOWS = ROOT / ".github/workflows"
DOCS = [ROOT / "docs/DEVELOPMENT.md", ROOT / "docs/ko/DEVELOPMENT.md"]
REASON = re.compile(r"^    # Not using make integration because \S", re.MULTILINE)
MAKE = re.compile(r"\bmake integration(-tls)?\b")
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


def _runs_make(job: dict[str, Any]) -> bool:
    return any(MAKE.search(str(s.get("run", ""))) for s in job.get("steps") or [])


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
