"""CI and full-matrix jobs install dependencies with a pinned uv (#759).

Resolution is unchanged from pip (same pyproject constraints); uv only replaces the
installer. The packaging smoke test keeps plain ``pip`` on purpose: it proves the
built wheel and sdist install with the tool end users run.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
pytestmark = pytest.mark.repo_tooling

UV_ACTION = "astral-sh/setup-uv@c18668ad3cf93ea998bef934396af7bb5c839dc7"
UV_VERSION = "0.12.17"
WORKFLOWS = ("ci.yml", "integration-full.yml")
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
    assert len(PYTHON_JOBS) >= 10


@pytest.mark.parametrize(("wf", "name", "steps"), PYTHON_JOBS)
def test_job_sets_up_pinned_uv_with_cache(wf: str, name: str, steps: list) -> None:
    uv = [s for s in steps if str(s.get("uses", "")).startswith("astral-sh/setup-uv@")]
    assert len(uv) == 1, f"{wf}:{name} needs exactly one setup-uv step"
    assert uv[0]["uses"] == UV_ACTION
    assert uv[0]["with"]["version"] == UV_VERSION
    assert uv[0]["with"]["enable-cache"] is True
    assert uv[0]["with"]["cache-dependency-glob"] == "pyproject.toml"
    python = next(s for s in steps if "actions/setup-python@" in str(s.get("uses", "")))
    assert "cache" not in python.get("with", {}), "pip cache is unused once uv installs"


@pytest.mark.parametrize(("wf", "name", "steps"), PYTHON_JOBS)
def test_job_installs_with_uv_and_logs_versions(wf: str, name: str, steps: list) -> None:
    runs = [s["run"] for s in steps if "run" in s]
    lines = [line.strip() for run in runs for line in run.splitlines()]
    assert any(line.startswith("uv pip install --system ") for line in lines), name
    assert "uv pip freeze --system" in lines, f"{wf}:{name} must log resolved versions"
    for line in lines:
        if "pip install" in line and not line.startswith("uv pip install --system "):
            assert line.startswith(PIP_SMOKE_ALLOWED), f"{wf}:{name} still uses pip: {line}"
