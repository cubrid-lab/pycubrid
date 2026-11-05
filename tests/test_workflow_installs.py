"""CI installs locked third-party wheels separately from trusted first-party builds."""

from __future__ import annotations

import hashlib
import re
import shutil
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
pytestmark = pytest.mark.repo_tooling

UV_ACTION = "astral-sh/setup-uv@07a047e608db84b3d8c06cd9a7c7dcc9c355c18e"
UV_VERSION = "0.12.17"
# The release gate keeps setup-uv's event guard (`auto`); routine CI always caches.
WORKFLOWS = {"ci.yml": True, "integration-full.yml": "auto"}
EXPECTED_JOBS = {"ci.yml": 12, "integration-full.yml": 10}
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
    assert set(uv[0]["with"]["cache-dependency-glob"].splitlines()) == {
        "pyproject.toml",
        "uv.lock",
        ".github/ci-wheels/*.whl",
    }
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
    exports = [line for line in lines if line.startswith("uv export ")]
    assert len(exports) == 1, f"{wf}:{name} needs one locked group export"
    assert all(flag in exports[0] for flag in ("--locked", "--no-build", "--no-emit-project"))
    assert "--only-group" in exports[0] or "--no-default-groups --group" in exports[0]
    for line in lines:
        if line.startswith("uv pip install "):
            if line.endswith("-e ."):
                assert "--no-index --no-deps --no-build-isolation" in line
            else:
                assert "--require-hashes --only-binary :all:" in line
        if PIP_INSTALL.search(line) and not line.startswith("uv pip "):
            assert line.startswith(PIP_SMOKE_ALLOWED), f"{wf}:{name} still uses pip: {line}"
            assert "--no-index --no-deps" in line
            if "test-sdist" in line:
                assert "--no-build-isolation" in line


def test_package_build_uses_the_locked_backend() -> None:
    text = (ROOT / ".github/workflows/ci.yml").read_text()
    assert "run: python -m build --no-isolation" in text


@pytest.fixture
def source_only_backend(tmp_path: Path) -> tuple[Path, Path]:
    source = tmp_path / "source"
    source.mkdir()
    marker = tmp_path / "backend-ran"
    (source / "pyproject.toml").write_text(
        '[build-system]\nrequires = []\nbuild-backend = "backend"\nbackend-path = ["."]\n'
    )
    (source / "backend.py").write_text(
        f"from pathlib import Path\nPath({str(marker)!r}).touch()\nraise RuntimeError('inert control')\n"
    )
    archive = Path(shutil.make_archive(str(tmp_path / "source-only-1.0"), "gztar", source))
    return archive, marker


@pytest.mark.skipif(shutil.which("uv") is None, reason="uv is required for installer controls")
def test_binary_only_install_rejects_source_before_running_its_backend(
    tmp_path: Path, source_only_backend: tuple[Path, Path]
) -> None:
    archive, marker = source_only_backend
    requirements = tmp_path / "requirements.txt"
    requirements.write_text(
        f"source-only==1.0 --hash=sha256:{hashlib.sha256(archive.read_bytes()).hexdigest()}\n"
    )
    common = [
        "uv",
        "pip",
        "install",
        "--python",
        sys.executable,
        "--no-cache",
        "--dry-run",
        "--no-index",
        "--find-links",
        str(tmp_path),
        "--no-build-isolation",
    ]
    # The original source-capable path must exercise the inert backend.
    old = subprocess.run([*common, "source-only==1.0"], capture_output=True, text=True, timeout=30)
    assert old.returncode != 0 and marker.exists(), old.stderr
    marker.unlink()
    hardened = subprocess.run(
        [*common, "--require-hashes", "--only-binary", ":all:", "-r", str(requirements)],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert hardened.returncode != 0, hardened.stdout
    assert not marker.exists(), hardened.stderr


@pytest.mark.skipif(shutil.which("uv") is None, reason="uv is required for export controls")
def test_locked_export_rejects_source_metadata_builds(
    tmp_path: Path, source_only_backend: tuple[Path, Path]
) -> None:
    archive, marker = source_only_backend
    project = tmp_path / "project"
    project.mkdir()
    manifest = project / "pyproject.toml"
    manifest.write_text(
        '[project]\nname="export-control"\nversion="1.0"\nrequires-python=">=3.11"\n'
    )
    locked = subprocess.run(
        ["uv", "lock", "--offline", "--python", sys.executable, "--directory", str(project)],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert locked.returncode == 0, locked.stderr
    previous_lock = (project / "uv.lock").read_bytes()
    manifest.write_text(
        manifest.read_text()
        + f'\n[dependency-groups]\ncontrol=["source-only @ {archive.as_uri()}"]\n'
    )
    common = [
        "uv",
        "export",
        "--locked",
        "--offline",
        "--no-cache",
        "--python",
        sys.executable,
        "--no-default-groups",
        "--group",
        "control",
        "--directory",
        str(project),
    ]
    old = subprocess.run(common, capture_output=True, text=True, timeout=30)
    assert old.returncode != 0 and marker.exists(), old.stderr
    assert (project / "uv.lock").read_bytes() == previous_lock
    marker.unlink()
    hardened = subprocess.run([*common, "--no-build"], capture_output=True, text=True, timeout=30)
    assert hardened.returncode != 0, hardened.stdout
    assert not marker.exists(), hardened.stderr
    assert (project / "uv.lock").read_bytes() == previous_lock


@pytest.mark.skipif(shutil.which("uv") is None, reason="uv is required for lock controls")
def test_locked_export_rejects_manifest_drift(tmp_path: Path) -> None:
    manifest = tmp_path / "pyproject.toml"
    manifest.write_text('[project]\nname="lock-control"\nversion="1.0"\nrequires-python=">=3.11"\n')
    locked = subprocess.run(
        ["uv", "lock", "--offline", "--python", sys.executable, "--directory", str(tmp_path)],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert locked.returncode == 0, locked.stderr
    previous_lock = (tmp_path / "uv.lock").read_bytes()
    manifest.write_text(manifest.read_text().replace('version="1.0"', 'version="1.1"'))
    export = subprocess.run(
        ["uv", "export", "--locked", "--offline", "--directory", str(tmp_path)],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert export.returncode != 0 and "needs to be updated" in export.stderr, export.stderr
    assert (tmp_path / "uv.lock").read_bytes() == previous_lock
    unlocked = subprocess.run(
        ["uv", "export", "--offline", "--directory", str(tmp_path)],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert unlocked.returncode == 0, unlocked.stderr
    assert (tmp_path / "uv.lock").read_bytes() != previous_lock


@pytest.mark.skipif(shutil.which("uv") is None, reason="uv is required for hash controls")
def test_hash_checked_install_rejects_tampered_wheel(tmp_path: Path) -> None:
    original = ROOT / ".github/ci-wheels/pyyaml-6.0.3-1-py3-none-any.whl"
    wheel = tmp_path / original.name
    shutil.copyfile(original, wheel)
    requirements = tmp_path / "requirements.txt"
    requirements.write_text(
        f"{wheel.as_uri()} --hash=sha256:{hashlib.sha256(original.read_bytes()).hexdigest()}\n"
    )
    common = [
        "uv",
        "pip",
        "install",
        "--python",
        sys.executable,
        "--no-cache",
        "--no-index",
        "--only-binary",
        ":all:",
        "--require-hashes",
        "-r",
        str(requirements),
    ]
    valid = subprocess.run(
        [*common, "--target", str(tmp_path / "valid")], capture_output=True, text=True, timeout=30
    )
    assert valid.returncode == 0, valid.stderr
    wheel.write_bytes(wheel.read_bytes() + b"tampered-control")
    tampered = subprocess.run(
        [*common, "--target", str(tmp_path / "tampered")],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert tampered.returncode != 0 and "hash" in tampered.stderr.lower(), tampered.stderr
    assert not (tmp_path / "tampered/yaml").exists()
