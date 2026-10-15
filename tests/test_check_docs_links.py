"""Deterministic documentation checks: local links, anchors, fences, commands (#927)."""

from __future__ import annotations

import importlib.util
import socket
import subprocess
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.repo_tooling

ROOT = Path(__file__).resolve().parents[1]
_SPEC = importlib.util.spec_from_file_location(
    "check_docs_links", ROOT / "scripts/check_docs_links.py"
)
assert _SPEC is not None and _SPEC.loader is not None
check = importlib.util.module_from_spec(_SPEC)
sys.modules["check_docs_links"] = check
_SPEC.loader.exec_module(check)

# check_docs_links imports the shared parser; reuse that module.
_markdown = sys.modules["_markdown"]
github_slug, make_goals, parse = _markdown.github_slug, _markdown.make_goals, _markdown.parse

TARGET = """# Target

## Connection options {#options}

<a id="legacy-anchor"></a>

## 한글 제목

## Same

## Same
"""


def _tree(tmp_path: Path, page: str, extra: dict[str, str] | None = None) -> Path:
    (tmp_path / "docs").mkdir()
    (tmp_path / "scripts").mkdir()
    (tmp_path / "scripts/tool.py").write_text("", encoding="utf-8")
    (tmp_path / "Makefile").write_text("test:\n\tpytest\nlint:\n\truff\n", encoding="utf-8")
    (tmp_path / "docs/TARGET.md").write_text(TARGET, encoding="utf-8")
    (tmp_path / "docs/PAGE.md").write_text(page, encoding="utf-8")
    for name, text in (extra or {}).items():
        path = tmp_path / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
    return tmp_path


def test_repository_documentation_has_no_deterministic_defects() -> None:
    assert check.run(ROOT) == []


def test_valid_relative_links_and_anchors_pass(tmp_path: Path) -> None:
    page = (
        "# Page\n\n[t](TARGET.md), [o](TARGET.md#options), [a](TARGET.md#legacy-anchor),\n"
        "[k](TARGET.md#한글-제목), [d](TARGET.md#same-1), [s](TARGET.md#same_1),\n"
        '[self](#page), [dir](../docs/), [img](TARGET.md "title") and ![i](TARGET.md).\n'
    )
    assert check.run(_tree(tmp_path, page)) == []


def test_external_links_are_not_fetched(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    def no_network(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("the documentation check must not open a connection")

    monkeypatch.setattr(socket, "create_connection", no_network)
    monkeypatch.setattr(socket.socket, "connect", no_network)
    page = "[x](https://unreachable.invalid/page) [m](mailto:a@b.invalid) [h](http://x.invalid)\n"
    assert check.run(_tree(tmp_path, page)) == []


def test_broken_relative_link_fails_with_location_and_suggestion(tmp_path: Path) -> None:
    root = _tree(tmp_path, "# Page\n\nSee [the target](TARGTE.md).\n")
    assert check.run(root) == [
        "docs/PAGE.md:3: link target 'TARGTE.md' does not exist; did you mean 'TARGET.md'?"
    ]


def test_deleted_reference_fails(tmp_path: Path) -> None:
    root = _tree(tmp_path, "# Page\n\nSee [the target](TARGET.md#options).\n")
    assert check.run(root) == []
    (root / "docs/TARGET.md").unlink()
    assert check.run(root) == ["docs/PAGE.md:3: link target 'TARGET.md#options' does not exist"]


def test_invalid_anchor_fails_with_suggestion(tmp_path: Path) -> None:
    root = _tree(tmp_path, "# Page\n\nSee [options](TARGET.md#option) and [x](#nowhere).\n")
    assert check.run(root) == [
        "docs/PAGE.md:3: anchor #nowhere is not a heading or anchor of docs/PAGE.md",
        "docs/PAGE.md:3: anchor #option is not a heading or anchor of docs/TARGET.md; "
        "did you mean 'options'?",
    ]


def test_absolute_and_escaping_paths_fail(tmp_path: Path) -> None:
    root = _tree(tmp_path, "[a](/docs/TARGET.md) and [b](../../outside.md)\n")
    assert check.run(root) == [
        "docs/PAGE.md:1: link target '../../outside.md' points outside the repository",
        "docs/PAGE.md:1: link target '/docs/TARGET.md' is absolute; use a path relative to "
        "the file",
    ]


def test_reference_links_and_definitions(tmp_path: Path) -> None:
    page = (
        "# Page\n\nSee [ok][t], [bad][gone] and [x][missing].\n\n"
        "[t]: TARGET.md#options\n[gone]: GONE.md\n[unused]: ALSO_GONE.md\n"
    )
    assert check.run(_tree(tmp_path, page)) == [
        "docs/PAGE.md:3: link target 'GONE.md' does not exist",
        "docs/PAGE.md:3: reference [missing] has no definition",
        "docs/PAGE.md:6: link target 'GONE.md' does not exist",
        "docs/PAGE.md:7: link target 'ALSO_GONE.md' does not exist",
    ]


def test_links_in_code_are_examples(tmp_path: Path) -> None:
    page = (
        "`[a](GONE.md)` and ``[b](GONE.md) with ` inside``\n\n"
        "```md\n[c](GONE.md)\n```\n\n~~~\n[d](GONE.md)\n~~~\n\n"
        "````\n```\n[e](GONE.md)\n```\n````\n"
    )
    assert check.run(_tree(tmp_path, page)) == []


@pytest.mark.parametrize(
    ("page", "line"),
    [("# Page\n\n```bash\nmake test\n", 3), ("~~~\nx\n```\n", 1), ("````\nx\n```\n", 1)],
)
def test_unclosed_fence_fails(tmp_path: Path, page: str, line: int) -> None:
    assert check.run(_tree(tmp_path, page)) == [f"docs/PAGE.md:{line}: code fence is never closed"]


def test_closed_fences_pass(tmp_path: Path) -> None:
    page = "```\nx\n```\n\n~~~~\n~~~\n~~~~\n\n```\nlast\n```\n"
    assert check.run(_tree(tmp_path, page)) == []


def test_commands_are_checked_only_in_code(tmp_path: Path) -> None:
    page = (
        "Make sure to make good use of it.\n\n"
        "Run `make test lnit` then:\n\n```bash\nmake -j2 lint && make tset\n"
        "python scripts/tool.py && python scripts/gone.py\n```\n"
    )
    assert check.run(_tree(tmp_path, page)) == [
        "docs/PAGE.md:3: `make lnit` is not a Makefile target; did you mean 'lint'?",
        "docs/PAGE.md:6: `make tset` is not a Makefile target; did you mean 'test'?",
        "docs/PAGE.md:7: scripts/gone.py does not exist",
    ]


def test_release_history_keeps_old_commands(tmp_path: Path) -> None:
    history = "# Changelog\n\n- Removed `make release` and scripts/check_version.py.\n"
    root = _tree(tmp_path, "# Page\n", {"CHANGELOG.md": history})
    assert check.run(root) == []
    (root / "CHANGELOG.md").write_text(history + "\nSee [gone](GONE.md).\n", encoding="utf-8")
    assert check.run(root) == ["CHANGELOG.md:5: link target 'GONE.md' does not exist"]


def test_suppression_comment_on_the_line_or_the_line_before(tmp_path: Path) -> None:
    page = (
        "<!-- docs-check: ignore -->\n[a](GONE.md)\n\n[b](GONE.md) <!-- docs-check: ignore -->\n"
        "\n[c](GONE.md)\n"
    )
    assert check.run(_tree(tmp_path, page)) == [
        "docs/PAGE.md:6: link target 'GONE.md' does not exist"
    ]


def test_only_tracked_files_are_checked_in_git(tmp_path: Path) -> None:
    root = _tree(tmp_path, "# Page\n", {"untracked/NOTES.md": "[x](GONE.md)\n"})
    subprocess.run(["git", "-C", str(root), "init", "-q"], check=True)
    subprocess.run(["git", "-C", str(root), "add", "docs", "Makefile", "scripts"], check=True)
    assert check.run(root) == []
    assert [p.name for p in check.markdown_files(root)] == ["PAGE.md", "TARGET.md"]


def test_github_slug_matches_github_rendering() -> None:
    assert github_slug("Development Workflow (cubrid-lab org standard)") == (
        "development-workflow-cubrid-lab-org-standard"
    )
    assert github_slug("`ping(reconnect=True)` and #485") == "pingreconnecttrue-and-485"
    assert github_slug("한글 제목 (Korean)") == "한글-제목-korean"


def test_make_goals() -> None:
    assert make_goals("make test") == ["test"]
    assert make_goals("make -j2 -C sub lint VAR=1 test # comment") == ["lint", "test"]
    assert make_goals("cd x && make integration CUBRID_TEST_PORT=1 | tee log") == ["integration"]
    assert make_goals("make sure") == ["sure"]
    assert make_goals("cmake build") == []


def test_parse_reports_heading_anchors(tmp_path: Path) -> None:
    path = tmp_path / "t.md"
    path.write_text(TARGET, encoding="utf-8")
    assert parse(path).anchors == {
        "target",
        "options",
        "connection-options",
        "legacy-anchor",
        "한글-제목",
        "same",
        "same-1",
        "same_1",
    }


def test_cli(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    root = _tree(tmp_path, "# Page\n\n[x](GONE.md)\n")
    assert check.main(["--root", str(root)]) == 1
    out = capsys.readouterr().out
    assert "docs/PAGE.md:3: link target 'GONE.md' does not exist" in out
    assert "<!-- docs-check: ignore -->" in out
    (root / "docs/PAGE.md").write_text("# Page\n", encoding="utf-8")
    assert check.main(["--root", str(root), str(root / "docs/PAGE.md")]) == 0
    assert "OK: 1 Markdown files" in capsys.readouterr().out
