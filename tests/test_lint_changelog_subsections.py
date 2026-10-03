"""Exercise the real CLI against independent changelog fixtures."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize("release", ["Unreleased", "1.0.0"])
@pytest.mark.parametrize("heading", ["Fixed", "Documentation"])
def test_duplicate_subsection_is_rejected(tmp_path: Path, release: str, heading: str) -> None:
    scripts = tmp_path / "scripts"
    scripts.mkdir()
    script = scripts / "lint_changelog.py"
    script.write_text((ROOT / "scripts/lint_changelog.py").read_text())
    prefix = "## [Unreleased]\n" if release != "Unreleased" else ""
    (tmp_path / "CHANGELOG.md").write_text(
        prefix + f"## [{release}]\n### {heading}\n- First\n### {heading}\n- Second\n"
    )
    result = subprocess.run([sys.executable, str(script)], capture_output=True, text=True)
    assert result.returncode == 1
    assert f"Duplicate subsection '### {heading}' in [{release}]" in result.stderr


def test_same_subsection_across_releases_is_valid(tmp_path: Path) -> None:
    scripts = tmp_path / "scripts"
    scripts.mkdir()
    script = scripts / "lint_changelog.py"
    script.write_text((ROOT / "scripts/lint_changelog.py").read_text())
    (tmp_path / "CHANGELOG.md").write_text(
        "## [Unreleased]\n### Fixed\n- New\n## [1.0.0]\n### Fixed\n- Old\n"
    )
    result = subprocess.run([sys.executable, str(script)], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
