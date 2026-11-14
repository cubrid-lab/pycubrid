"""Offline safety tests for the one-off historical GitHub Release body sync."""

from __future__ import annotations

import json

import pytest

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
