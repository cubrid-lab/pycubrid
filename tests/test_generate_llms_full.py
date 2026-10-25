from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
_SPEC = importlib.util.spec_from_file_location(
    "_pycubrid_generate_llms_full", ROOT / "scripts" / "generate_llms_full.py"
)
assert _SPEC is not None and _SPEC.loader is not None
generate_llms_full = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(generate_llms_full)


def _configure_fixture(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, required: list[str]
) -> tuple[Path, Path, Path]:
    docs = tmp_path / "docs"
    docs.mkdir()
    output = docs / "llms-full.txt"
    index = docs / "llms.txt"
    root_index = tmp_path / "llms.txt"
    monkeypatch.setattr(generate_llms_full, "DOCS_DIR", docs)
    monkeypatch.setattr(generate_llms_full, "OUTPUT", output)
    monkeypatch.setattr(generate_llms_full, "INDEX", index)
    monkeypatch.setattr(generate_llms_full, "ROOT_INDEX", root_index)
    monkeypatch.setattr(generate_llms_full, "DOC_FILES", required)
    return output, index, root_index


def test_missing_required_source_fails_before_writing_artifacts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    output, index, root_index = _configure_fixture(
        tmp_path, monkeypatch, ["present.md", "missing.md"]
    )
    (tmp_path / "docs" / "present.md").write_text("present\n", encoding="utf-8")
    index.write_text("new index\n", encoding="utf-8")
    output.write_text("existing full artifact\n", encoding="utf-8")
    root_index.write_text("existing root index\n", encoding="utf-8")

    with pytest.raises(
        FileNotFoundError,
        match=r"required llms-full source page\(s\) missing: missing\.md",
    ):
        generate_llms_full.generate()

    assert output.read_text(encoding="utf-8") == "existing full artifact\n"
    assert root_index.read_text(encoding="utf-8") == "existing root index\n"


def test_complete_sources_preserve_canonical_then_sorted_additional_order(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    output, index, root_index = _configure_fixture(tmp_path, monkeypatch, ["second.md", "first.md"])
    docs = tmp_path / "docs"
    for name in ("second.md", "first.md", "z-extra.md", "a-extra.md"):
        (docs / name).write_text(f"content from {name}\n", encoding="utf-8")
    (docs / "PRD.md").write_text("excluded\n", encoding="utf-8")
    (docs / "README.ko.md").write_text("translated\n", encoding="utf-8")
    index.write_bytes(b"canonical index\n")

    generate_llms_full.generate()

    divider = "=" * 60
    names = ("second.md", "first.md", "a-extra.md", "z-extra.md")
    expected = "\n\n".join(
        f"{divider}\nFile: {name}\n{divider}\n\ncontent from {name}" for name in names
    )
    assert output.read_text(encoding="utf-8") == expected + "\n"
    assert root_index.read_bytes() == b"canonical index\n"
    messages = capsys.readouterr().out
    assert f"Generated {output}" in messages
    assert f"Synced {root_index} from docs/llms.txt" in messages
