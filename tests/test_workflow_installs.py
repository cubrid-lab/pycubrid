"""CI and full-matrix jobs install dependencies with a pinned uv (#759).

Resolution is unchanged from pip (same pyproject constraints); uv only replaces the
installer. The packaging smoke test keeps plain ``pip`` on purpose: it proves the
built wheel and sdist install with the tool end users run.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
pytestmark = pytest.mark.repo_tooling

UV_ACTION = "astral-sh/setup-uv@c18668ad3cf93ea998bef934396af7bb5c839dc7"
UV_VERSION = "0.12.17"
# The release gate keeps setup-uv's event guard (`auto`); routine CI always caches.
WORKFLOWS = {"ci.yml": True, "integration-full.yml": "auto"}
EXPECTED_JOBS = {"ci.yml": 11, "integration-full.yml": 10}
PIP_INSTALL = re.compile(r"\bpip3?\s+install\b")
PIP_SMOKE_ALLOWED = ("/tmp/test-wheel/bin/pip install", "/tmp/test-sdist/bin/pip install")


def _python_jobs() -> list:
    params = []
    for wf in WORKFLOWS:
        jobs = yaml.safe_load((ROOT / ".github/workflows" / wf).read_text())["jobs"]
        for name, job in jobs.items():
            steps = job.get("steps", [])
            if any("actions/setup-python@" in str(s.get("uses", "")) for s in steps):
                params.append(pytest.param(wf, name, steps, id=f"{wf}:{name}"))
    return params


PYTHON_JOBS = _python_jobs()


def test_python_jobs_are_found() -> None:
    for wf, expected in EXPECTED_JOBS.items():
        assert sum(1 for p in PYTHON_JOBS if p.values[0] == wf) == expected, wf


@pytest.mark.parametrize(("wf", "name", "steps"), PYTHON_JOBS)
def test_job_sets_up_pinned_uv_with_cache(wf: str, name: str, steps: list) -> None:
    uv = [s for s in steps if str(s.get("uses", "")).startswith("astral-sh/setup-uv@")]
    assert len(uv) == 1, f"{wf}:{name} needs exactly one setup-uv step"
    assert uv[0]["uses"] == UV_ACTION
    assert uv[0]["with"]["version"] == UV_VERSION
    assert uv[0]["with"]["enable-cache"] == WORKFLOWS[wf]
    assert uv[0]["with"]["cache-dependency-glob"] == "pyproject.toml"
    # One cache per job (different extras). setup-uv follows setup-python and both
    # share one python-version spec, so UV_PYTHON resolves to setup-python's interpreter.
    assert uv[0]["with"]["cache-suffix"] == "${{ github.job }}"
    python = next(s for s in steps if "actions/setup-python@" in str(s.get("uses", "")))
    assert steps.index(python) < steps.index(uv[0]), f"{wf}:{name} set up Python first"
    # The cache key must carry the job's Python spec (e.g. "3.12"), not the patch
    # release `uv python find` reports, which drifts across runner images.
    assert uv[0]["with"]["python-version"] == python["with"]["python-version"]
    assert "cache" not in python.get("with", {}), "pip cache is unused once uv installs"


@pytest.mark.parametrize(("wf", "name", "steps"), PYTHON_JOBS)
def test_job_installs_with_uv_and_logs_versions(wf: str, name: str, steps: list) -> None:
    runs = [s["run"] for s in steps if "run" in s]
    lines = [line.strip() for run in runs for line in run.splitlines()]
    assert any(line.startswith("uv pip install --system ") for line in lines), name
    assert "uv pip freeze --system" in lines, f"{wf}:{name} must log resolved versions"
    for line in lines:
        if PIP_INSTALL.search(line) and not line.startswith("uv pip "):
            assert line.startswith(PIP_SMOKE_ALLOWED), f"{wf}:{name} still uses pip: {line}"
