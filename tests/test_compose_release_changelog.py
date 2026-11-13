"""Publisher-format and regeneration regressions; no publication or database."""

from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.repo_tooling
SPEC = importlib.util.spec_from_file_location(
    "compose_release_changelog",
    Path(__file__).resolve().parents[1] / "scripts/compose_release_changelog.py",
)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)
compose = MODULE.compose
BASE = "# Changelog\n\n## [Unreleased]\n\n### Upgrade notes\n\nKeep this **exactly**.\n\n## [1.8.0] - 2026-09-01\n\nOld history.\n"
NOTES = "# Changelog\n\n## [1.9.0](https://github.com/example/compare/v1.8.0...v1.9.0) (2026-10-03)\n\n### Added\n\n* new API\n"


def test_preserves_curated_notes_history_and_canonical_header() -> None:
    actual = compose(BASE, NOTES, "1.9.0")
    assert actual.startswith("# Changelog\n\n## [Unreleased]\n\n## [1.9.0] - 2026-10-03\n")
    assert "### Upgrade notes\n\nKeep this **exactly**." in actual
    assert "### Added\n\n* new API" in actual
    assert "Conventional commits" not in actual and "####" not in actual
    assert actual[actual.index("## [1.8.0]") :] == BASE[BASE.index("## [1.8.0]") :]
    assert compose(BASE, NOTES, "1.9.0") == actual


def test_regeneration_updates_generated_notes_without_losing_curated_notes() -> None:
    updated = compose(BASE, NOTES.replace("new API", "new API and fix"), "1.9.0")
    assert "Keep this **exactly**." in updated
    assert "new API and fix" in updated
    assert updated.count("### Added") == 1


CURATED = (
    "# Changelog\n\n## [Unreleased]\n\n"
    "### Upgrade notes\n\n- Curated upgrade.\n\n"
    "### Fixed\n\n- Curated fix.\n  Continued line.\n\n"
    "### Documentation\n\n- Curated docs.\n\n"
    "### CI\n\n- Curated CI.\n\n"
    "## [1.8.0] - 2026-09-01\n\n### Conventional commits\n\nOld history.\n"
)
# release-please 17.6.0 output with the repo's changelog-sections: config order,
# not the standard order, plus its breaking-change heading.
GENERATED = (
    "# Changelog\n\n## [1.9.0](https://github.com/example/compare/v1.8.0...v1.9.0) (2026-10-03)"
    "\n\n\n### ⚠ BREAKING CHANGES\n\n* drop old API\n\n"
    "### Added\n\n* new API ([aaaaaaa](https://example/commit/a))\n\n\n"
    "### Fixed\n\n* generated fix\n\n\n"
    "### Performance\n\n* faster\n\n\n"
    "### Changed\n\n* reverted thing\n\n\n"
    "### Documentation\n\n* generated docs\n"
)


def test_merges_curated_and_generated_under_standard_headings_in_order() -> None:
    actual = compose(CURATED, GENERATED, "1.9.0")
    candidate = actual[actual.index("## [1.9.0]") : actual.index("## [1.8.0]")]
    assert candidate == (
        "## [1.9.0] - 2026-10-03\n\n"
        "### Upgrade notes\n\n- Curated upgrade.\n\n* drop old API\n\n"
        "### Added\n\n* new API ([aaaaaaa](https://example/commit/a))\n\n"
        "### Changed\n\n* reverted thing\n\n"
        "### Fixed\n\n- Curated fix.\n  Continued line.\n\n* generated fix\n\n"
        "### Performance\n\n* faster\n\n"
        "### Documentation\n\n- Curated docs.\n\n* generated docs\n\n"
        "### CI\n\n- Curated CI.\n\n"
    )
    # Pre-cutoff history keeps its historical heading, byte for byte.
    assert actual[actual.index("## [1.8.0]") :] == CURATED[CURATED.index("## [1.8.0]") :]


def test_composed_candidate_passes_the_changelog_lint(tmp_path: Path) -> None:
    import shutil
    import subprocess
    import sys

    root = Path(__file__).resolve().parents[1]
    (tmp_path / "scripts").mkdir()
    shutil.copy(root / "scripts/lint_changelog.py", tmp_path / "scripts")
    candidate = compose(CURATED, GENERATED, "1.9.0")
    # Move the fixture past the lint cutoff so the candidate is checked.
    candidate = candidate.replace("[1.9.0]", "[9.9.0]").replace("[1.8.0]", "[1.0.0]")
    (tmp_path / "CHANGELOG.md").write_text(candidate, encoding="utf-8")
    result = subprocess.run(
        [sys.executable, str(tmp_path / "scripts/lint_changelog.py")],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize(
    "base,generated",
    [
        (CURATED.replace("### CI", "### Docs"), GENERATED),
        (CURATED, GENERATED.replace("### Performance", "### Performance Improvements")),
        (CURATED, GENERATED.replace("\n\n\n### ⚠", "\n\nstray text\n\n### ⚠")),
    ],
)
def test_rejects_nonstandard_headings_or_stray_text(base: str, generated: str) -> None:
    with pytest.raises(ValueError):
        compose(base, generated, "1.9.0")


@pytest.mark.parametrize(
    "generated",
    [
        NOTES.replace("1.9.0", "2.0.0"),
        NOTES.replace("2026-10-03", "2026-02-31"),
        NOTES + NOTES,
        NOTES + "\n## unexpected\n",
        NOTES[: NOTES.index("### Added")],
    ],
)
def test_rejects_unsupported_candidate_without_writing(generated: str) -> None:
    with pytest.raises(ValueError):
        compose(BASE, generated, "1.9.0")


@pytest.mark.parametrize(
    "base,version",
    [
        (BASE.replace("Unreleased", "1.9.0"), "1.9.0"),
        (BASE + "\n## [Unreleased]\n", "1.9.0"),
        (BASE, "1.8.0"),
        (BASE, "1.7.0"),
        (BASE, "1.9.0rc1"),
    ],
)
def test_rejects_invalid_base_or_version(base: str, version: str) -> None:
    with pytest.raises(ValueError):
        compose(base, NOTES, version)


def test_curated_preamble_before_the_first_heading_is_kept_verbatim() -> None:
    preamble = "Curated lead paragraph with **markup**.\n  Indented continuation."
    base = CURATED.replace("## [Unreleased]\n\n", f"## [Unreleased]\n\n{preamble}\n\n", 1)
    actual = compose(base, GENERATED, "1.9.0")
    assert f"## [1.9.0] - 2026-10-03\n\n{preamble}\n\n### Upgrade notes\n" in actual


def test_empty_curated_section_is_dropped() -> None:
    base = BASE.replace("### Upgrade notes", "### Added\n\n### Upgrade notes")
    generated = NOTES.replace("### Added", "### Fixed")
    actual = compose(base, generated, "1.9.0")
    assert "### Added" not in actual
    assert "### Upgrade notes\n\nKeep this **exactly**.\n\n### Fixed\n\n* new API" in actual


FENCED_CURATED = (
    "# Changelog\n\n## [Unreleased]\n\n### Added\n\n- Example:\n\n"
    "```markdown\n### Not a heading\n```\n\n"
    "## [1.8.0] - 2026-09-01\n\nOld history.\n"
)


def test_fenced_heading_in_curated_entry_is_content() -> None:
    actual = compose(FENCED_CURATED, NOTES, "1.9.0")
    assert "```markdown\n### Not a heading\n```" in actual
    assert actual.count("### Added") == 1
    assert (
        actual[actual.index("## [1.8.0]") :] == FENCED_CURATED[FENCED_CURATED.index("## [1.8.0]") :]
    )


def test_fenced_heading_in_generated_entry_is_content() -> None:
    generated = NOTES + "\n```\n### Not a heading\n## also not\n```\n"
    actual = compose(BASE, generated, "1.9.0")
    assert "```\n### Not a heading\n## also not\n```" in actual


def test_fenced_curated_entry_passes_the_changelog_lint(tmp_path: Path) -> None:
    actual = compose(FENCED_CURATED, NOTES, "1.9.0")
    (tmp_path / "scripts").mkdir()
    script = tmp_path / "scripts/lint_changelog.py"
    script.write_text((Path(MODULE.__file__).parent / "lint_changelog.py").read_text())
    (tmp_path / "CHANGELOG.md").write_text(actual)
    result = subprocess.run([sys.executable, str(script)], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize(
    "base,generated,message",
    [
        (BASE.replace("Keep this", "```\n### Fixed\nKeep this"), NOTES, "release header inside"),
        ("# Changelog\n\n## [Unreleased]\n\n```\n### Fixed\nx\n", NOTES, "unclosed code fence"),
        (BASE, NOTES + "\n```\n### Fixed\n", "unclosed code fence"),
    ],
)
def test_unclosed_code_fence_fails_closed(base: str, generated: str, message: str) -> None:
    with pytest.raises(ValueError, match=message):
        compose(base, generated, "1.9.0")


@pytest.mark.parametrize(
    "base,generated",
    [
        (FENCED_CURATED.replace("### Not a heading\n", "## [9.9.9] - 2000-01-01\n"), NOTES),
        (BASE, NOTES + "\n```\n## [1.9.0](x) (2026-01-01)\n```\n"),
        (BASE, NOTES.replace("### Added", "```\n## [1.9.0](x) (2026-01-01)\n### Added")),
    ],
)
def test_fenced_release_header_fails_closed(base: str, generated: str) -> None:
    with pytest.raises(ValueError, match="release header inside an open code fence"):
        compose(base, generated, "1.9.0")


def test_generated_issue_references_render_as_refs_not_closes() -> None:
    # release-please renders a "Refs #479" footer as ", closes [#479](...)"
    # (cubrid-lab/sqlalchemy-cubrid#770).
    entry = (
        "* pin note ([#801](https://example/issues/801)) ([ea9392e](https://example/commit/e))"
        ", closes [cubrid-lab/pycubrid#801](https://example/pycubrid/issues/801)"
        " [#479](https://example/issues/479)"
    )
    curated = (
        "- Curated ([abc1234](https://example/commit/c)), closes [#9](https://example/issues/9)"
    )
    history = "* old ([def5678](https://example/commit/d)), closes [#7](https://example/issues/7)"
    subject = "* note (x), closes [y] stays"
    fenced = "```\n* call ([abc1234](https://example/commit/f)), closes [literal]\n```"
    base = BASE.replace("Keep this **exactly**.", curated).replace("Old history.", history)
    generated = NOTES.replace("* new API", f"{entry}\n{subject}\n\n{fenced}") + (
        "\n## [1.8.0](https://example/compare) (2026-09-01)\n\n### Fixed\n\n" + history + "\n"
    )
    actual = compose(base, generated, "1.9.0")
    assert (
        "([ea9392e](https://example/commit/e)), refs "
        "[cubrid-lab/pycubrid#801](https://example/pycubrid/issues/801)"
        " [#479](https://example/issues/479)" in actual
    )
    candidate = actual[actual.index("## [1.9.0]") : actual.index("## [1.8.0]")]
    assert curated in candidate and subject in candidate and fenced in candidate
    assert candidate.count(", closes [") == 3  # curated line, subject and fenced example
    assert actual[actual.index("## [1.8.0]") :] == base[base.index("## [1.8.0]") :]
    assert compose(base, generated, "1.9.0") == actual


RELEASE_PR_BODY = (
    ":robot: I have created a release *beep* *boop*\n---\n\n\n"
    "## [1.9.0](https://example/compare/v1.8.0...v1.9.0) (2026-10-03)\n\n\n### Documentation\n\n"
    "* pin note ([#801](https://example/issues/801)) ([ea9392e](https://example/commit/e))"
    ", closes [#479](https://example/issues/479)\n"
    "* plain note ([#802](https://example/issues/802)) ([fb12345](https://example/commit/f))\n"
    "\n```\n* call ([abc1234](https://example/commit/a)), closes [literal]\n```\n\n---\n"
    "This PR was generated with [Release Please](https://github.com/googleapis/release-please).\n"
)


def release_pr(body: str | None, ref: str = "release-please--branches--main--components--x"):
    return {"number": 7, "head": {"ref": ref}, "body": body}


def test_release_pr_body_references_render_as_refs() -> None:
    rewritten = MODULE.rewrite_pr_body(release_pr(RELEASE_PR_BODY))
    assert rewritten == RELEASE_PR_BODY.replace(
        "([ea9392e](https://example/commit/e)), closes [#479]",
        "([ea9392e](https://example/commit/e)), refs [#479]",
    )
    assert "), closes [literal]" in rewritten  # fenced content stays
    crlf = RELEASE_PR_BODY.replace("\n", "\r\n")
    assert MODULE.rewrite_pr_body(release_pr(crlf)) == rewritten.replace("\n", "\r\n")
    assert MODULE.rewrite_pr_body(release_pr(rewritten)) is None
    assert MODULE.rewrite_pr_body(release_pr(None)) is None


def test_release_pr_body_rewrite_refuses_other_pull_requests() -> None:
    with pytest.raises(ValueError, match="not a release-please release PR"):
        MODULE.rewrite_pr_body(release_pr(RELEASE_PR_BODY, ref="feature/x"))
    with pytest.raises(ValueError, match="not a release-please release PR"):
        MODULE.rewrite_pr_body({"body": RELEASE_PR_BODY})
    with pytest.raises(ValueError, match="not a release-please release PR"):
        MODULE.rewrite_pr_body({"head": "release-please--branches--main", "body": ""})


@pytest.mark.parametrize(
    "fence",
    [("```", "```"), ("~~~", "~~~"), ("   ```", "```"), ("````", "````"), ("```", "~~~\n```")],
)
def test_reference_rewrite_skips_every_github_fence_form(fence: tuple[str, str]) -> None:
    opening, closing = fence
    example = "* call ([abc1234](https://example/commit/a)), closes [literal]"
    entry = "* entry ([fb12345](https://example/commit/f)), closes [#1](https://example/issues/1)"
    body = f"{opening}\n{example}\n{closing}\n{entry}\n"
    rewritten = MODULE.rewrite_pr_body(release_pr(body))
    assert rewritten == body.replace("/f)), closes [#1]", "/f)), refs [#1]")


def test_release_pr_body_cli_writes_a_patch_payload_only_on_change(tmp_path: Path) -> None:
    pr_json = tmp_path / "pr.json"
    out = tmp_path / "body.json"
    pr_json.write_text(json.dumps(release_pr(RELEASE_PR_BODY)), encoding="utf-8")
    assert MODULE.main(["--pr-body", str(pr_json), "--output", str(out)]) == 0
    payload = json.loads(out.read_text(encoding="utf-8"))
    assert list(payload) == ["body"] and "), refs [#479]" in payload["body"]
    pr_json.write_text(json.dumps(release_pr(payload["body"])), encoding="utf-8")
    assert MODULE.main(["--pr-body", str(pr_json), "--output", str(out)]) == 0
    assert not out.exists()
    pr_json.write_text(json.dumps(release_pr(RELEASE_PR_BODY, ref="main")), encoding="utf-8")
    assert MODULE.main(["--pr-body", str(pr_json), "--output", str(out)]) == 1
    for broken in ("not json", "[1]"):
        pr_json.write_text(broken, encoding="utf-8")
        assert MODULE.main(["--pr-body", str(pr_json), "--output", str(out)]) == 1
    with pytest.raises(SystemExit):
        MODULE.main(["--pr-body", "", "--output", str(out)])
    with pytest.raises(SystemExit):
        MODULE.main(["--pr-body", str(pr_json), "--output", str(out), "--generated-changelog", "x"])
    with pytest.raises(SystemExit):
        MODULE.main(["--pr-body", str(pr_json)])
    with pytest.raises(SystemExit):
        MODULE.main(["--pr-body", str(pr_json), "--output", str(out), "--version", "1.9.0"])


# One-to-one correlation is deliberate: never merge based only on fuzzy prose.
REPO = "https://github.com/cubrid-lab/pycubrid"
ISSUE_1083 = f"[#1083]({REPO}/issues/1083)"
PR_1141 = f"[PR #1141]({REPO}/pull/1141)"
GENERATED_PR_1141 = (
    f"* **compat:** warn against rewrapping a LOB handle across connections "
    f"([#1141]({REPO}/issues/1141)) "
    f"([5c7f769]({REPO}/commit/5c7f7698d3179b2106d84d940e8d0cd165be2caf)), "
    f"refs [#1130]({REPO}/issues/1130)"
)


def test_removes_single_pr_correlated_duplicate_and_keeps_human_credit() -> None:
    base = BASE.replace(
        "Keep this **exactly**.",
        f"- **LOB safety** — Do not rewrap a LOB across connections ({PR_1141}) by @alice.",
    )
    generated = NOTES.replace("* new API", GENERATED_PR_1141)
    candidate = compose(base, generated, "1.9.0")
    section = candidate.split("## [1.9.0]", 1)[1].split("## [1.8.0]", 1)[0]
    assert section.count("**LOB safety**") == 1
    assert PR_1141 in section and "by @alice" in section
    assert "warn against rewrapping" not in section
    assert section.count("### Upgrade notes") == 1
    assert "### Added" not in section


def test_issue_only_correlation_requires_unique_reference_and_shared_meaning() -> None:
    curated = (
        "### Documentation\n\n"
        f"- **Safer Docker setup** — Wait for a real Docker login ({ISSUE_1083}) by @alice.\n"
    )
    base = BASE.replace("### Upgrade notes\n\nKeep this **exactly**.", curated)
    generated_entry = (
        "* **troubleshooting:** wait for a real Docker login and bind locally "
        f"([deba70f]({REPO}/commit/deba70fac7041b336e83f2a667727e0000f815fb)), "
        f"refs {ISSUE_1083}"
    )
    generated = NOTES.replace("* new API", generated_entry)
    result = compose(base, generated, "1.9.0")
    version = result.split("## [1.9.0]", 1)[1].split("## [1.8.0]", 1)[0]
    assert "Safer Docker setup" in version
    assert "troubleshooting" not in version
    assert "by @alice" in version


def test_issue_reference_with_unrelated_subject_is_not_silently_dropped() -> None:
    curated = f"### Documentation\n\n- **Docker login** — Fix Docker readiness ({ISSUE_1083}).\n"
    base = BASE.replace("### Upgrade notes\n\nKeep this **exactly**.", curated)
    generated_entry = f"* **Database:** change number conversion, refs {ISSUE_1083}"
    output = compose(base, NOTES.replace("* new API", generated_entry), "1.9.0")
    assert "change number conversion" in output


def test_matching_issue_is_ambiguous_when_two_generated_entries_refer_to_it() -> None:
    curated = f"### Documentation\n\n- **Docker login** — Fix Docker readiness ({ISSUE_1083}).\n"
    base = BASE.replace("### Upgrade notes\n\nKeep this **exactly**.", curated)
    both = (
        f"* **Docker:** fix Docker login, refs {ISSUE_1083}\n"
        f"* **Docker:** clarify Docker port binding, refs {ISSUE_1083}"
    )
    output = compose(base, NOTES.replace("* new API", both), "1.9.0")
    assert "fix Docker login" in output and "clarify Docker port binding" in output


def test_same_pr_with_two_generated_entries_is_not_deleted() -> None:
    curated = f"### Documentation\n\n- **Driver setup** — Source build setup ({PR_1141}).\n"
    base = BASE.replace("### Upgrade notes\n\nKeep this **exactly**.", curated)
    generated = NOTES.replace(
        "* new API",
        GENERATED_PR_1141 + "\n" + GENERATED_PR_1141.replace("LOB handle", "unrelated API"),
    )
    output = compose(base, generated, "1.9.0")
    assert "warn against rewrapping" in output
    assert "unrelated API" in output


def test_deduplication_is_repo_local_and_preserves_unrelated_entries() -> None:
    base = BASE.replace(
        "Keep this **exactly**.",
        f"- **LOB handling across connections** — Use a safe LOB path ({PR_1141}) by @alice.",
    )
    duplicate = GENERATED_PR_1141
    foreign = (
        "* **driver:** unrelated new API "
        "([#1141](https://github.com/other/repository/issues/1141)) "
        f"([234aaaa]({REPO}/commit/234aaaa0000000000000000000000000000000000))"
    )
    result = compose(base, NOTES.replace("* new API", duplicate + "\n" + foreign), "1.9.0")
    version = result.split("## [1.9.0]", 1)[1].split("## [1.8.0]", 1)[0]
    assert "warn against rewrapping" not in version
    assert "unrelated new API" in version
    assert "by @alice" in version
    assert "### Added" in version


def test_no_overlap_preserves_generated_section_byte_for_byte() -> None:
    assert "### Added" in compose(BASE, NOTES, "1.9.0")
    version = compose(BASE, NOTES, "1.9.0")
    assert "* new API\n" in version


def test_deduplication_does_not_rewrite_history() -> None:
    base = BASE.replace(
        "Keep this **exactly**.",
        f"- **LOB safety** — Do not rewrap a LOB ({PR_1141}).",
    )
    result = compose(base, NOTES.replace("* new API", GENERATED_PR_1141), "1.9.0")
    assert result[result.index("## [1.8.0]") :] == base[base.index("## [1.8.0]") :]


def test_matching_pr_with_different_behavior_is_not_deduplicated() -> None:
    base = BASE.replace(
        "Keep this **exactly**.",
        f"- **Query speed** — Accelerated integer execution ({PR_1141}) by @alice.",
    )
    generated = NOTES.replace("* new API", GENERATED_PR_1141)
    composed = compose(base, generated, "1.9.0")
    assert "Accelerated integer execution" in composed
    assert "warn against rewrapping" in composed


def test_generated_explanatory_paragraph_is_preserved_when_entry_matches() -> None:
    base = BASE.replace(
        "Keep this **exactly**.",
        f"- **LOB safety** — Do not rewrap a LOB across connections ({PR_1141}).",
    )
    generated = NOTES.replace(
        "* new API",
        "Important independent upgrade context.\n\n" + GENERATED_PR_1141,
    )
    composed = compose(base, generated, "1.9.0")
    assert "Important independent upgrade context." in composed
    assert "warn against rewrapping" in composed


def test_fenced_pr_link_is_not_release_provenance() -> None:
    fenced = (
        "### Documentation\n\n"
        "- **LOB safety example** — Explain LOB handles across connections.\n\n"
        "  ~~~markdown\n"
        f"  {PR_1141}\n"
        "  ~~~\n"
    )
    base = BASE.replace("### Upgrade notes\n\nKeep this **exactly**.", fenced)
    composed = compose(base, NOTES.replace("* new API", GENERATED_PR_1141), "1.9.0")
    assert "warn against rewrapping" in composed
    assert f"  {PR_1141}" in composed


def test_issue_correlation_does_not_hide_a_second_pr_using_the_same_issue() -> None:
    curated = (
        "### Documentation\n\n"
        f"- **Safer Docker setup** — Wait for a real Docker login ({ISSUE_1083}).\n"
    )
    base = BASE.replace("### Upgrade notes\n\nKeep this **exactly**.", curated)
    issue_only = (
        "* **Docker:** wait for a real Docker login "
        f"([deba70f]({REPO}/commit/deba70fac7041b336e83f2a667727e0000f815fb)), "
        f"refs {ISSUE_1083}"
    )
    another_pr = (
        "* **Docker:** clarify the same issue separately "
        f"([#500]({REPO}/issues/500)) "
        f"([aaaabbb]({REPO}/commit/aaaabbb0000000000000000000000000000000000)), "
        f"refs {ISSUE_1083}"
    )
    composed = compose(base, NOTES.replace("* new API", issue_only + "\n" + another_pr), "1.9.0")
    assert "wait for a real Docker login" in composed
    assert another_pr in composed


def test_issue_fallback_requires_local_commit_and_no_primary_pr() -> None:
    curated = (
        "### Documentation\n\n"
        f"- **Safer Docker setup** — Wait for a real Docker login ({ISSUE_1083}).\n"
    )
    base = BASE.replace("### Upgrade notes\n\nKeep this **exactly**.", curated)
    missing_commit = f"* **Docker:** wait for a real Docker login, refs {ISSUE_1083}"
    foreign_primary = (
        "* **Docker:** wait for a real Docker login "
        "([#800](https://github.com/other/repository/issues/800)) "
        f"([deba70f]({REPO}/commit/deba70fac7041b336e83f2a667727e0000f815fb)), "
        f"refs {ISSUE_1083}"
    )
    for candidate in (missing_commit, foreign_primary):
        composed = compose(base, NOTES.replace("* new API", candidate), "1.9.0")
        assert candidate in composed


def test_curated_fenced_issue_link_does_not_make_generated_issue_entry_disappear() -> None:
    curated = (
        "### Documentation\n\n"
        "- **Docker example** — Demonstrate waiting for a real Docker login.\n\n"
        "  ~~~markdown\n"
        f"  refs {ISSUE_1083}\n"
        "  ~~~\n"
    )
    base = BASE.replace("### Upgrade notes\n\nKeep this **exactly**.", curated)
    generated = (
        "* **Docker:** wait for a real Docker login "
        f"([deba70f]({REPO}/commit/deba70fac7041b336e83f2a667727e0000f815fb)), "
        f"refs {ISSUE_1083}"
    )
    composed = compose(base, NOTES.replace("* new API", generated), "1.9.0")
    assert generated in composed


def test_tilde_fenced_example_bullet_is_not_treated_as_a_generated_entry() -> None:
    base = BASE.replace(
        "Keep this **exactly**.",
        f"- **LOB handling across connections** — Use a safe LOB path ({PR_1141}).",
    )
    generated = NOTES.replace(
        "* new API",
        "* **Markdown examples** — Add an example.\n\n"
        f"~~~markdown\n{GENERATED_PR_1141}\n~~~\n\n"
        "* **Distinct change** — Keep this change.",
    )
    result = compose(base, generated, "1.9.0")
    assert "Markdown examples" in result
    assert "warn against rewrapping" in result
    assert "Distinct change" in result


def test_removing_a_duplicate_keeps_the_remaining_list_tight() -> None:
    base = BASE.replace(
        "Keep this **exactly**.",
        f"- **LOB safety** — Do not rewrap a LOB across connections ({PR_1141}) by @alice.",
    )
    kept = [
        f"* first unrelated note ([aaaaaaa]({REPO}/commit/aaaaaaa000000000000000000000000000000000))",
        f"* second unrelated note ([bbbbbbb]({REPO}/commit/bbbbbbb000000000000000000000000000000000))",
    ]
    generated = NOTES.replace("* new API", "\n".join([kept[0], GENERATED_PR_1141, kept[1]]))
    version = compose(base, generated, "1.9.0").split("## [1.9.0]", 1)[1]
    assert "warn against rewrapping" not in version
    assert "\n".join(kept) + "\n" in version


# release-please lists a squash commit twice when its body carries a second
# conventional-commit line: once bare, once with the pull request reference.
SHA_A = "6c2cba8fe7c4742b96f38151a59a85077a156655"
SHA_B = "1c32deca2089606fc8d5aab3b5b046894ed093a0"


def _commit(sha: str) -> str:
    return f"([{sha[:7]}]({REPO}/commit/{sha}))"


def _with_pr(text: str, sha: str, number: int = 1230) -> str:
    return f"* {text} ([#{number}]({REPO}/issues/{number})) {_commit(sha)}"


def _compose_generated(*entries: str, section: str = "Documentation") -> str:
    generated = f"# Changelog\n\n## [1.9.0](https://example.com) (2026-10-03)\n\n### {section}\n\n"
    return compose(BASE, generated + "\n".join(entries) + "\n", "1.9.0")


def test_bare_bullet_repeating_a_pr_bullet_commit_is_collapsed() -> None:
    bare = f"* clarify issue handoff and work states {_commit(SHA_A)}"
    output = _compose_generated(bare, _with_pr("define issue handoff states", SHA_A))
    assert "clarify issue handoff" not in output
    assert "define issue handoff states" in output
    assert "### Documentation\n\n* define" in output


def test_collapse_keeps_other_entries_and_list_tightness() -> None:
    bare = f"* clarify handoff {_commit(SHA_A)}"
    other = f"* unrelated change {_commit(SHA_B)}"
    output = _compose_generated(bare, other, _with_pr("define handoff", SHA_A))
    assert "clarify handoff" not in output
    assert f"{other}\n* define handoff" in output


def test_same_commit_with_pr_references_on_both_bullets_is_kept() -> None:
    output = _compose_generated(
        _with_pr("first wording", SHA_A, 1230), _with_pr("second wording", SHA_A, 1231)
    )
    assert "first wording" in output and "second wording" in output


def test_two_bare_bullets_for_one_commit_are_kept() -> None:
    output = _compose_generated(
        f"* first wording {_commit(SHA_A)}", f"* second wording {_commit(SHA_A)}"
    )
    assert "first wording" in output and "second wording" in output


def test_bare_bullet_with_a_different_commit_is_kept() -> None:
    output = _compose_generated(
        f"* bare wording {_commit(SHA_B)}", _with_pr("define handoff", SHA_A)
    )
    assert "bare wording" in output and "define handoff" in output


def test_bare_bullet_with_an_issue_reference_is_kept() -> None:
    bare = f"* bare wording {_commit(SHA_A)}, refs [#1130]({REPO}/issues/1130)"
    output = _compose_generated(bare, _with_pr("define handoff", SHA_A))
    assert "bare wording" in output and "define handoff" in output


def test_bare_bullet_naming_two_commits_is_kept() -> None:
    bare = f"* bare wording {_commit(SHA_A)} {_commit(SHA_B)}"
    output = _compose_generated(bare, _with_pr("define handoff", SHA_A))
    assert "bare wording" in output and "define handoff" in output


def test_bare_bullet_is_collapsed_across_sections_of_one_release() -> None:
    # The comparison covers the whole release, not one section: one commit is one
    # change wherever release-please files its two lines.
    generated = (
        "# Changelog\n\n## [1.9.0](https://example.com) (2026-10-03)\n\n"
        f"### Documentation\n\n* bare wording {_commit(SHA_A)}\n\n"
        f"### CI\n\n{_with_pr('define handoff', SHA_A)}\n"
    )
    output = compose(BASE, generated, "1.9.0")
    assert "bare wording" not in output and "define handoff" in output
    assert "### Documentation" not in output


def test_bare_bullet_with_a_section_introduction_is_kept() -> None:
    generated = (
        "# Changelog\n\n## [1.9.0](https://example.com) (2026-10-03)\n\n"
        f"### Documentation\n\nIntro prose.\n\n* bare wording {_commit(SHA_A)}\n\n"
        f"### CI\n\n{_with_pr('define handoff', SHA_A)}\n"
    )
    output = compose(BASE, generated, "1.9.0")
    assert "Intro prose." in output and "bare wording" in output
