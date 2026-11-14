"""Offline safety tests for the one-off historical GitHub Release body sync."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

from scripts import sync_published_release_notes_once as sync

from scripts.sync_published_release_notes_once import (
    CHANGELOG,
    MANIFEST,
    build_plan,
    canonical_notes,
    digest,
    immutable_metadata,
    published_history_digest,
    source_blob_sha,
)


SOURCE = """# Changelog

## [Unreleased]

## [1.2.0] - 2026-11-14

A maintenance release.

### Fixed

- **Fix** — Corrected a query.

## [1.1.0] - 2026-11-01

The first released version.

### Added

- **Driver** — Added the database driver.
"""


def release(tag: str, body: str, release_id: int) -> dict:
    return {
        "id": release_id,
        "tag_name": tag,
        "body": body,
        "name": tag,
        "updated_at": "2026-11-14T01:00:00Z",
        "created_at": "2026-10-30T01:00:00Z",
        "published_at": "2026-11-01T01:00:00Z",
        "target_commitish": "main",
        "draft": False,
        "prerelease": False,
        "assets": [],
    }


def fixture() -> tuple[dict, list[dict]]:
    prior = release("v1.1.0", canonical_notes(SOURCE, "1.1.0"), 11)
    changed = release("v1.2.0", "An outdated public body.\n", 12)
    manifest = {
        "repository": "cubrid-lab/pycubrid",
        "expected_release_count": 2,
        "versions": {
            "v1.2.0": {
                "release_id": 12,
                "updated_at": changed["updated_at"],
                "old_body_sha256": digest(changed["body"]),
                "new_body_sha256": digest(canonical_notes(SOURCE, "1.2.0")),
                "created_at": changed["created_at"],
                "published_at": changed["published_at"],
                "target_commitish": "main",
                "release_name": "v1.2.0",
                "draft": False,
                "prerelease": False,
            }
        },
    }
    return manifest, [changed, prior]


def test_extract_has_one_real_comparison_link_and_no_first_release_link() -> None:
    assert canonical_notes(SOURCE, "1.2.0").count("**Full Changelog**") == 1
    assert "/compare/v1.1.0...v1.2.0" in canonical_notes(SOURCE, "1.2.0")
    assert "**Full Changelog**" not in canonical_notes(SOURCE, "1.1.0")


def test_preflight_only_targets_the_pinned_body() -> None:
    manifest, releases = fixture()
    expected, pending = build_plan(manifest, SOURCE, releases)
    assert pending == ["v1.2.0"]
    assert expected["v1.1.0"] == releases[1]["body"]


def test_preflight_rejects_an_unreviewed_body_edit() -> None:
    manifest, releases = fixture()
    releases[0]["body"] = "Something else changed."
    with pytest.raises(ValueError, match="Unreviewed original body change"):
        build_plan(manifest, SOURCE, releases)


def test_preflight_rejects_changed_metadata_or_time() -> None:
    manifest, releases = fixture()
    releases[0]["published_at"] = "2026-11-12T01:00:00Z"
    with pytest.raises(ValueError, match="metadata changed"):
        build_plan(manifest, SOURCE, releases)


def test_preflight_rejects_extra_public_releases() -> None:
    manifest, releases = fixture()
    releases.append(release("v9.9.9", "Unexpected body", 99))
    with pytest.raises(ValueError, match="Release inventory changed"):
        build_plan(manifest, SOURCE, releases)


def test_preflight_accepts_a_previously_completed_target() -> None:
    manifest, releases = fixture()
    releases[0]["body"] = canonical_notes(SOURCE, "1.2.0")
    expected, pending = build_plan(manifest, SOURCE, releases)
    assert pending == []
    assert releases[0]["body"] == expected["v1.2.0"]


def test_metadata_signature_ignores_update_timestamp_and_asset_downloads() -> None:
    original = release("v1.2.0", "Before", 12)
    changed = release("v1.2.0", "After", 12)
    original["assets"] = [{"id": 2, "name": "archive", "size": 10, "download_count": 1}]
    changed["assets"] = [{"id": 2, "name": "archive", "size": 10, "download_count": 3}]
    changed["updated_at"] = "2026-11-15T01:00:00Z"
    assert immutable_metadata(original) == immutable_metadata(changed)
    changed["assets"][0]["size"] = 11
    assert immutable_metadata(original) != immutable_metadata(changed)


def test_all_pinned_sha256_values_match_the_actual_reviewed_changelog() -> None:
    manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
    assert len(source_blob_sha()) == 40
    source = CHANGELOG.read_text(encoding="utf-8")
    assert published_history_digest(source) == manifest["source_history_sha256"]
    for tag, baseline in manifest["versions"].items():
        assert digest(canonical_notes(source, tag[1:])) == baseline["new_body_sha256"]
    assert len(manifest["versions"]) == 11


def test_invalid_version_or_missing_notes_is_rejected() -> None:
    with pytest.raises(ValueError, match="exactly one"):
        canonical_notes(SOURCE, "9.9.9")


def test_apply_patches_only_pinned_body_and_preserves_a_recoverable_backup(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    manifest, initial = fixture()
    manifest["source_changelog_blob_sha"] = "a" * 40
    manifest["source_history_sha256"] = sync.published_history_digest(SOURCE)
    manifest["expected_latest_tag"] = "v1.2.0"
    manifest_file = tmp_path / "manifest.json"
    manifest_file.write_text(json.dumps(manifest), encoding="utf-8")
    changelog_file = tmp_path / "CHANGELOG.md"
    changelog_file.write_text(SOURCE, encoding="utf-8")
    by_id = {item["id"]: item.copy() for item in initial}
    patched: list[tuple[int, dict]] = []

    def fake_api(path: str, token: str, method: str = "GET", payload: dict | None = None):
        assert token == "test-token"
        if path.endswith("/releases?per_page=100"):
            return [item.copy() for item in by_id.values()]
        if path.endswith("/releases/latest"):
            return {"tag_name": "v1.2.0"}
        release_id = int(path.rsplit("/", 1)[-1])
        if method == "PATCH":
            assert release_id == 12
            assert payload == {
                "body": canonical_notes(SOURCE, "1.2.0"),
                "make_latest": "false",
            }
            patched.append((release_id, payload))
            by_id[release_id]["body"] = payload["body"]
            by_id[release_id]["updated_at"] = "2026-11-14T03:00:00Z"
        return by_id[release_id].copy()

    monkeypatch.setattr(sync, "api_json", fake_api)
    monkeypatch.setattr(sync, "MANIFEST", manifest_file)
    monkeypatch.setattr(sync, "CHANGELOG", changelog_file)
    monkeypatch.setattr(sync, "source_blob_sha", lambda: "a" * 40)
    monkeypatch.setenv("GH_TOKEN", "test-token")
    monkeypatch.setenv("GITHUB_ACTIONS", "true")
    monkeypatch.setenv("GITHUB_REPOSITORY", "cubrid-lab/pycubrid")
    monkeypatch.setenv("GITHUB_REF", "refs/heads/main")
    monkeypatch.setenv("GITHUB_EVENT_NAME", "workflow_dispatch")
    monkeypatch.setenv("RUNNER_TEMP", str(tmp_path))
    monkeypatch.setattr(sys, "argv", ["sync", "--mode", "apply"])

    assert sync.main() == 0
    assert len(patched) == 1
    evidence = tmp_path / "pycubrid-release-sync-20261114"
    backup = json.loads((evidence / "prewrite_backup.json").read_text())
    report = json.loads((evidence / "report.json").read_text())
    assert len(backup["releases"]) == 2
    assert report["updated"] == ["v1.2.0"]
    assert report["result"] == "35/35 verified"

    # A subsequent application is safe and must not PATCH the already-synced body.
    assert sync.main() == 0
    assert len(patched) == 1


def test_apply_refuses_a_concurrent_edit_before_patch(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    manifest, initial = fixture()
    manifest["source_history_sha256"] = sync.published_history_digest(SOURCE)
    manifest["source_changelog_blob_sha"] = "a" * 40
    manifest["expected_latest_tag"] = "v1.2.0"
    manifest_file = tmp_path / "manifest.json"
    manifest_file.write_text(json.dumps(manifest), encoding="utf-8")
    changelog_file = tmp_path / "CHANGELOG.md"
    changelog_file.write_text(SOURCE, encoding="utf-8")
    calls: list[str] = []

    def fake_api(path: str, token: str, method: str = "GET", payload: dict | None = None):
        if path.endswith("/releases?per_page=100"):
            return [item.copy() for item in initial]
        if path.endswith("/releases/latest"):
            return {"tag_name": "v1.2.0"}
        calls.append(method)
        if method == "PATCH":
            pytest.fail("Concurrent edit must prevent PATCH")
        return {**initial[0], "body": "A concurrent editor changed the body."}

    monkeypatch.setattr(sync, "api_json", fake_api)
    monkeypatch.setattr(sync, "MANIFEST", manifest_file)
    monkeypatch.setattr(sync, "CHANGELOG", changelog_file)
    monkeypatch.setattr(sync, "source_blob_sha", lambda: "a" * 40)
    for key, value in {
        "GH_TOKEN": "test-token",
        "GITHUB_ACTIONS": "true",
        "GITHUB_REPOSITORY": "cubrid-lab/pycubrid",
        "GITHUB_REF": "refs/heads/main",
        "GITHUB_EVENT_NAME": "push",
        "RUNNER_TEMP": str(tmp_path),
    }.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setattr(sys, "argv", ["sync", "--mode", "apply"])

    with pytest.raises(ValueError, match="Concurrent edit detected"):
        sync.main()
    assert calls == ["GET"]
    assert (tmp_path / "pycubrid-release-sync-20261114/prewrite_backup.json").exists()
