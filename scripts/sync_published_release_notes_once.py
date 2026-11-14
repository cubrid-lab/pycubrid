#!/usr/bin/env python3
"""One-off, fail-closed synchronization of reviewed pycubrid Release descriptions.

Only the 11 pinned historical Release bodies may be changed. Tags, assets,
packages, publication dates and the latest Release must stay untouched.
After the reviewed PR merges, the one-off workflow applies this script and
uploads a pre-write snapshot plus a result report.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))
from extract_release_notes import COMPARE_LINK, full_changelog_link  # noqa: E402

REPOSITORY = "cubrid-lab/pycubrid"
API = "https://api.github.com"
MANIFEST = SCRIPT_DIR / "release_notes_sync_20261114.json"
CHANGELOG = SCRIPT_DIR.parent / "CHANGELOG.md"
VERSION_HEADING = re.compile(r"^## \[(\d+\.\d+\.\d+)\] - \d{4}-\d{2}-\d{2}[ \t]*$", re.M)


def digest(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def canonical_notes(changelog: str, version: str) -> str:
    """Match scripts/extract_release_notes.py without writing RELEASE_NOTES.md."""
    header = re.compile(
        rf"^## \[{re.escape(version)}\] - \d{{4}}-\d{{2}}-\d{{2}}\s*$",
        re.MULTILINE,
    )
    found = list(header.finditer(changelog))
    if len(found) != 1:
        raise ValueError(f"Expected exactly one dated CHANGELOG heading for {version}")
    start = found[0].end()
    next_header = re.search(r"^## \[", changelog[start:], re.MULTILINE)
    end = start + next_header.start() if next_header else len(changelog)
    body = changelog[start:end].strip()
    if not body:
        raise ValueError(f"Empty release notes for {version}")
    notes = body + "\n"
    compare = full_changelog_link(f"v{version}", changelog[end:])
    if compare and not COMPARE_LINK.search(body):
        notes += "\n" + compare + "\n"
    return notes


def source_blob_sha() -> str:
    result = subprocess.run(
        ["git", "hash-object", "CHANGELOG.md"],
        cwd=CHANGELOG.parent,
        capture_output=True,
        text=True,
        check=True,
    )
    return result.stdout.strip()


def api_json(path: str, token: str, method: str = "GET", payload: dict | None = None) -> Any:
    data = json.dumps(payload).encode("utf-8") if payload is not None else None
    headers = {
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": "pycubrid-one-off-release-note-sync",
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"
    if data is not None:
        headers["Content-Type"] = "application/json"
    request = Request(API + path, data=data, headers=headers, method=method)
    try:
        with urlopen(request, timeout=30) as response:
            return json.load(response)
    except (HTTPError, URLError) as exc:
        # Do not print request headers, token or potentially sensitive response bodies.
        raise RuntimeError(f"GitHub {method} {path} failed: {exc}") from exc


def immutable_metadata(release: dict) -> dict:
    """Only metadata whose values must survive a body-only edit."""
    fields = ("id", "tag_name", "name", "draft", "prerelease",
              "created_at", "published_at", "target_commitish")
    metadata = {field: release.get(field) for field in fields}
    metadata["assets"] = sorted(
        (
            {key: asset.get(key) for key in (
                "id", "name", "size", "digest", "created_at", "updated_at"
            )}
            for asset in release.get("assets", [])
        ),
        key=lambda asset: asset["id"],
    )
    return metadata


def build_plan(manifest: dict, changelog: str, releases: list[dict]) -> tuple[dict, list[str]]:
    """Validate every existing Release before allowing even one PATCH."""
    if manifest["repository"] != REPOSITORY:
        raise ValueError("Unexpected repository")
    if len(releases) != manifest["expected_release_count"]:
        raise ValueError("The public Release inventory changed; re-review required")
    by_tag = {release["tag_name"]: release for release in releases}
    if len(by_tag) != len(releases):
        raise ValueError("Duplicate Release tags")
    versions = VERSION_HEADING.findall(changelog)
    expected_tags = {f"v{version}" for version in versions}
    if len(versions) != manifest["expected_release_count"] or set(by_tag) != expected_tags:
        raise ValueError("GitHub Releases and dated CHANGELOG headings differ")

    targets = manifest["versions"]
    expected = {}
    pending = []
    for tag, release in by_tag.items():
        if release.get("draft") or release.get("prerelease") or release.get("name") != tag:
            raise ValueError(f"Unexpected release title or publication status: {tag}")
        body = canonical_notes(changelog, tag[1:])
        expected[tag] = body
        baseline = targets.get(tag)
        if baseline:
            if digest(body) != baseline["new_body_sha256"]:
                raise ValueError(f"Reviewed source content changed for {tag}")
            for field, key in (("id", "release_id"), ("name", "release_name"),
                               ("created_at", "created_at"),
                               ("published_at", "published_at"),
                               ("target_commitish", "target_commitish"),
                               ("draft", "draft"), ("prerelease", "prerelease")):
                if release.get(field) != baseline[key]:
                    raise ValueError(f"Published release metadata changed for {tag}: {field}")
        if release.get("body") == body:
            continue
        if not baseline:
            raise ValueError(f"Unreviewed public body drift in {tag}")
        if digest(release.get("body") or "") != baseline["old_body_sha256"]:
            raise ValueError(f"Unreviewed original body change in {tag}")
        if release.get("updated_at") != baseline["updated_at"]:
            raise ValueError(f"Unreviewed Release update timestamp in {tag}")
        pending.append(tag)
    if not set(pending).issubset(targets):
        raise ValueError("Unreviewed release would be modified")
    return expected, pending


def save_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def main() -> int:
    args = argparse.ArgumentParser(description=__doc__)
    args.add_argument("--mode", choices=("audit", "apply"), default="audit")
    opts = args.parse_args()

    manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
    if source_blob_sha() != manifest["source_changelog_blob_sha"]:
        raise ValueError("CHANGELOG.md changed since review; no updates allowed")
    changelog = CHANGELOG.read_text(encoding="utf-8")
    token = os.environ.get("GH_TOKEN") or os.environ.get("GITHUB_TOKEN", "")
    if opts.mode == "apply":
        if not token:
            raise ValueError("A GitHub Actions token is required for apply")
        if not (
            os.environ.get("GITHUB_ACTIONS") == "true"
            and os.environ.get("GITHUB_REPOSITORY") == REPOSITORY
            and os.environ.get("GITHUB_REF") == "refs/heads/main"
            and os.environ.get("GITHUB_EVENT_NAME") in ("push", "workflow_dispatch")
        ):
            raise ValueError("Apply requires the reviewed workflow on main")

    output_dir = Path(os.environ.get("RUNNER_TEMP", "/tmp")) / "pycubrid-release-sync-20261114"
    path = f"/repos/{REPOSITORY}"
    releases = api_json(path + "/releases?per_page=100", token)
    latest = api_json(path + "/releases/latest", token)
    if latest["tag_name"] != manifest["expected_latest_tag"]:
        raise ValueError("Latest published Release changed; re-review required")
    expected, pending = build_plan(manifest, changelog, releases)
    backup = {
        "source_changelog_blob_sha": manifest["source_changelog_blob_sha"],
        "release_count": len(releases),
        "latest_tag": latest["tag_name"],
        "releases": releases,
    }
    save_json(output_dir / "prewrite_backup.json", backup)
    report = {
        "mode": opts.mode,
        "source_changelog_blob_sha": manifest["source_changelog_blob_sha"],
        "pending_at_preflight": pending,
        "updated": [],
        "skipped": [],
        "result": "preflight-only" if opts.mode == "audit" else "applying",
    }
    save_json(output_dir / "report.json", report)
    print(f"Preflight: {len(releases)} releases; {len(pending)} pinned updates")
    if opts.mode == "audit":
        return 0

    original = {release["tag_name"]: release for release in releases}
    # Preserve historical order: apply only to explicitly pinned versions.
    for tag in manifest["versions"]:
        baseline = manifest["versions"][tag]
        previous = original[tag]
        if tag not in pending:
            report["skipped"].append(tag)
            save_json(output_dir / "report.json", report)
            continue
        release_id = baseline["release_id"]
        release_path = f"{path}/releases/{release_id}"
        # Re-fetch immediately before the unconditional PATCH. Never overwrite
        # another editor's body or an altered release metadata snapshot.
        current = api_json(release_path, token)
        if current.get("body") != previous.get("body") or (
            current.get("updated_at") != previous.get("updated_at")
            or immutable_metadata(current) != immutable_metadata(previous)
        ):
            raise ValueError(f"Concurrent edit detected before PATCH for {tag}")
        # Explicitly keep historical releases from becoming the Latest Release.
        api_json(release_path, token, "PATCH", {
            "body": expected[tag],
            "make_latest": "false",
        })
        after = api_json(release_path, token)
        if after.get("body") != expected[tag]:
            raise ValueError(f"Public body verification failed after PATCH for {tag}")
        if immutable_metadata(after) != immutable_metadata(previous):
            raise ValueError(f"Release identity, dates or assets changed for {tag}")
        report["updated"].append(tag)
        save_json(output_dir / "report.json", report)
        print(f"Verified {tag}")

    after_releases = api_json(path + "/releases?per_page=100", token)
    if len(after_releases) != len(releases):
        raise ValueError("Release count changed during publication")
    after_by_tag = {release["tag_name"]: release for release in after_releases}
    if set(after_by_tag) != set(original):
        raise ValueError("Release tags changed during publication")
    for tag, public in after_by_tag.items():
        if public.get("body") != expected[tag]:
            raise ValueError(f"Final public body mismatch for {tag}")
        if immutable_metadata(public) != immutable_metadata(original[tag]):
            raise ValueError(f"Final metadata or asset mismatch for {tag}")
    final_latest = api_json(path + "/releases/latest", token)
    if final_latest["tag_name"] != manifest["expected_latest_tag"]:
        raise ValueError("Latest Release changed unexpectedly")
    report["result"] = "35/35 verified"
    save_json(output_dir / "report.json", report)
    print("Success: 35/35 published Release bodies match reviewed CHANGELOG.md")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (ValueError, RuntimeError, OSError, subprocess.CalledProcessError) as exc:
        print(f"Release-note sync aborted: {exc}", file=sys.stderr)
        sys.exit(1)
