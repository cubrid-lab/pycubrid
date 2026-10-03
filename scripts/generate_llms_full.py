"""Generate the LLM-facing documentation artifacts.

* ``docs/llms-full.txt`` is the documentation Markdown files concatenated.
* ``llms.txt`` (repository root) is a byte-identical copy of the canonical
  ``docs/llms.txt`` index, which the docs site also serves as ``/llms.txt``.
  Edit ``docs/llms.txt`` only; CI fails if either artifact is stale.
"""

from __future__ import annotations

import pathlib

DOCS_DIR = pathlib.Path(__file__).resolve().parent.parent / "docs"
OUTPUT = DOCS_DIR / "llms-full.txt"
INDEX = DOCS_DIR / "llms.txt"
ROOT_INDEX = DOCS_DIR.parent / "llms.txt"

DOC_FILES = [
    "index.md",
    "quickstart.md",
    "CONNECTION.md",
    "API_REFERENCE.md",
    "TYPES.md",
    "EXAMPLES.md",
    "PROTOCOL.md",
    "ARCHITECTURE.md",
    "PERFORMANCE.md",
    "TROUBLESHOOTING.md",
    "faq.md",
    "SUPPORT_MATRIX.md",
    "DEVELOPMENT.md",
]

EXCLUDED_FILES = {
    "agent-playbook.md",
    "PRD.md",
}


def _iter_additional_docs() -> list[str]:
    seen = set(DOC_FILES)
    extra: list[str] = []
    for path in sorted(DOCS_DIR.glob("*.md")):
        name = path.name
        if name in seen:
            continue
        if name in EXCLUDED_FILES:
            continue
        if name.startswith("README.") and name.endswith(".md"):
            continue
        extra.append(name)
    return extra


def generate() -> None:
    parts: list[str] = []

    for name in [*DOC_FILES, *_iter_additional_docs()]:
        path = DOCS_DIR / name
        if not path.exists():
            continue
        content = path.read_text(encoding="utf-8").strip()
        parts.append(f"{'=' * 60}\nFile: {name}\n{'=' * 60}\n\n{content}")

    _ = OUTPUT.write_text("\n\n".join(parts) + "\n", encoding="utf-8")
    print(f"Generated {OUTPUT} ({OUTPUT.stat().st_size:,} bytes)")

    _ = ROOT_INDEX.write_bytes(INDEX.read_bytes())
    print(f"Synced {ROOT_INDEX} from docs/llms.txt")


if __name__ == "__main__":
    generate()
