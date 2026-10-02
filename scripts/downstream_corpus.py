"""Verify the installed driver and fail-closed nightly downstream JUnit evidence.

This is deliberately independent of the driver's integration-lane inventory:
downstream repositories have their own test names and skip contracts.
"""

from __future__ import annotations

import argparse
import importlib
import importlib.metadata as metadata
import json
import os
import platform
import re
import sys
from pathlib import Path
from typing import Any

# Only local pytest JUnit files are parsed; DTD and entity declarations are rejected.
from xml.etree import ElementTree  # nosec B405


ORIGIN = "https://github.com/cubrid-lab/pycubrid.git"
WORKLOADS = {
    "sqlalchemy": ("dogfood", "pool"),
    "mcp": ("mcp",),
    "cookbook": ("ai", "worker"),
}
VERSION_PACKAGES = {
    "sqlalchemy": ("sqlalchemy", "sqlalchemy-cubrid", "pytest"),
    "mcp": ("cubrid-mcp-server", "fastmcp", "pytest"),
    "cookbook": ("sqlalchemy-cubrid", "cubrid-mcp-server", "celery", "pytest"),
}
MCP_EMPTY_SCHEMA_SKIPS = {"no user tables", "no user tables in database"}
SHA_RE = re.compile(r"[0-9a-f]{40}\Z")


def verify_driver_origin(
    expected_sha: str,
    *,
    distribution: Any | None = None,
    import_file: Path | None = None,
) -> dict[str, str]:
    """Require a VCS install of precisely this workflow's commit, not a release.

    Comparing both distribution metadata and the imported module path catches a
    same-version PyPI/local replacement and source-checkout import shadowing.
    """
    if SHA_RE.fullmatch(expected_sha) is None:
        raise ValueError("driver SHA must be a full lowercase Git commit")
    dist = distribution if distribution is not None else metadata.distribution("pycubrid")
    direct_url = dist.read_text("direct_url.json")
    if not direct_url:
        raise ValueError("pycubrid is not a direct VCS install")
    try:
        record = json.loads(direct_url)
    except json.JSONDecodeError as exc:
        raise ValueError("pycubrid direct_url.json is malformed") from exc
    if not isinstance(record, dict) or record.get("url") != ORIGIN:
        raise ValueError("pycubrid came from an unexpected repository")
    vcs_info = record.get("vcs_info")
    if not isinstance(vcs_info, dict) or vcs_info.get("vcs") != "git":
        raise ValueError("pycubrid is not a Git VCS install")
    if vcs_info.get("requested_revision") != expected_sha:
        raise ValueError("pycubrid was not requested at the workflow commit")
    if vcs_info.get("commit_id") != expected_sha:
        raise ValueError("pycubrid installed a different commit")
    if record.get("dir_info") is not None or record.get("archive_info") is not None:
        raise ValueError("pycubrid came from a local or archive install")

    imported = import_file
    if imported is None:
        module_file = importlib.import_module("pycubrid").__file__
        if module_file is None:
            raise ValueError("imported pycubrid has no package file")
        imported = Path(module_file)
    installed_file = Path(str(dist.locate_file("pycubrid/__init__.py"))).resolve()
    if imported.resolve() != installed_file:
        raise ValueError("imported pycubrid is not the installed VCS distribution")
    return {
        "commit": expected_sha,
        "origin": ORIGIN,
        "version": str(dist.version),
        "import_file": str(installed_file),
    }


def probe_server() -> str:
    """Read the actual server version through the verified installed driver."""
    pycubrid = importlib.import_module("pycubrid")
    connection = pycubrid.connect(
        host=os.environ.get("CUBRID_TEST_HOST", "localhost"),
        port=int(os.environ.get("CUBRID_TEST_PORT", "33000")),
        database=os.environ.get("CUBRID_TEST_DB", "testdb"),
        user=os.environ.get("CUBRID_TEST_USER", "dba"),
        password=os.environ.get("CUBRID_TEST_PASSWORD", ""),
        connect_timeout=5,
        read_timeout=5,
    )
    try:
        cursor = connection.cursor()
        try:
            cursor.execute("SELECT 1")
            if cursor.fetchone() != (1,):
                raise ValueError("CUBRID SELECT 1 returned an unexpected row")
        finally:
            cursor.close()
        return str(connection.get_server_version()).replace("\n", " ").replace("\r", " ")
    finally:
        connection.close()


def _junit_counts(path: Path, *, allow_mcp_empty_schema: bool) -> dict[str, Any]:
    payload = path.read_text(encoding="utf-8-sig")
    upper = payload.upper()
    if "\x00" in payload or "<!DOCTYPE" in upper or "<!ENTITY" in upper:
        raise ValueError(f"{path.name}: DTD/entities or NUL are forbidden")
    root = ElementTree.fromstring(payload)  # nosec B314
    cases = list(root.iter("testcase"))
    skipped: list[str] = []
    violations: list[str] = []
    failures = errors = 0
    for case in cases:
        failures += case.find("failure") is not None
        errors += case.find("error") is not None
        skip = case.find("skipped")
        if skip is not None:
            reason = (skip.get("message", "") or skip.text or "").strip()
            skipped.append(reason)
            if not allow_mcp_empty_schema or reason not in MCP_EMPTY_SCHEMA_SKIPS:
                violations.append(f"unexpected skip: {reason!r}")
    passed = len(cases) - len(skipped) - failures - errors
    if not cases:
        violations.append("no test cases ran")
    elif passed < 1:
        violations.append("no test cases passed")
    if failures or errors:
        violations.append(f"{failures} failed, {errors} errors")
    return {
        "cases": len(cases),
        "passed": passed,
        "skipped": len(skipped),
        "failures": failures,
        "errors": errors,
        "skip_reasons": skipped,
        "violations": violations,
    }


def collect_reports(lane: str, report_dir: Path) -> dict[str, Any]:
    """Require positive passes for *each* selected workload, not lane total."""
    if lane not in WORKLOADS:
        raise ValueError(f"unknown downstream lane: {lane}")
    workloads: dict[str, dict[str, Any]] = {}
    reasons: list[str] = []
    for name in WORKLOADS[lane]:
        path = report_dir / f"{name}.xml"
        try:
            counts = _junit_counts(path, allow_mcp_empty_schema=lane == "mcp")
            workloads[name] = counts
            reasons.extend(f"{name}: {violation}" for violation in counts["violations"])
        except (OSError, ValueError, ElementTree.ParseError) as exc:
            reasons.append(f"{name}: {exc}")
    return {
        "lane": lane,
        "status": "success" if not reasons else "failure",
        "workloads": workloads,
        "reasons": reasons,
    }


def _render_summary(evidence: dict[str, Any]) -> str:
    lines = [
        f"## Downstream corpus: {evidence['lane']}",
        "",
        "| Field | Value |",
        "| --- | --- |",
    ]
    for key in (
        "status",
        "driver_expected_sha",
        "driver_installed_sha",
        "driver_origin",
        "driver_version",
        "downstream_sha",
        "server_version",
        "python_version",
    ):
        lines.append(f"| {key} | {evidence.get(key) or 'unavailable'} |")
    versions = evidence.get("package_versions", {})
    lines.append(
        "| package_versions | " + ", ".join(f"{k}={v}" for k, v in versions.items()) + " |"
    )
    lines.extend(
        [
            "",
            "| Workload | Cases | Passed | Skipped | Failures | Errors |",
            "| --- | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for name in WORKLOADS[evidence["lane"]]:
        counts = evidence["workloads"].get(name, {})
        lines.append(
            f"| {name} | {counts.get('cases', 'missing')} | {counts.get('passed', 0)} | "
            f"{counts.get('skipped', 0)} | {counts.get('failures', 0)} | {counts.get('errors', 0)} |"
        )
    if evidence["reasons"]:
        lines.extend(["", "Failures: " + "; ".join(evidence["reasons"])])
    for name, counts in evidence["workloads"].items():
        if counts["skip_reasons"]:
            lines.append(f"{name} skips: {counts['skip_reasons']}")
    return "\n".join(lines) + "\n"


def _report(args: argparse.Namespace) -> int:
    result = collect_reports(args.lane, args.report_dir)
    reasons = result["reasons"]
    driver: dict[str, str] = {}
    try:
        loaded = json.loads(args.driver_evidence.read_text(encoding="utf-8"))
        if not isinstance(loaded, dict):
            raise ValueError("driver evidence is not an object")
        driver = loaded
    except (OSError, ValueError) as exc:
        reasons.append(f"driver evidence unavailable: {exc}")
    if driver.get("commit") != args.expected_sha:
        reasons.append("installed driver commit does not match workflow SHA")
    if driver.get("origin") != ORIGIN or not driver.get("server_version"):
        reasons.append("driver origin or actual CUBRID server version is unverified")
    if not args.downstream_sha or SHA_RE.fullmatch(args.downstream_sha) is None:
        reasons.append("downstream checkout SHA is missing or invalid")
    if args.job_status != "success":
        reasons.append(f"upstream job status: {args.job_status}")
    if args.test_outcome != "success":
        reasons.append(f"selected test step outcome: {args.test_outcome or 'missing'}")
    versions: dict[str, str] = {}
    for name in VERSION_PACKAGES[args.lane]:
        try:
            versions[name] = metadata.version(name)
        except metadata.PackageNotFoundError:
            versions[name] = "unavailable"
            if args.job_status == "success":
                reasons.append(f"required package is not installed: {name}")
    result.update(
        {
            "status": "failure" if reasons else "success",
            "driver_expected_sha": args.expected_sha,
            "driver_installed_sha": driver.get("commit"),
            "driver_origin": driver.get("origin"),
            "driver_version": driver.get("version"),
            "downstream_sha": args.downstream_sha,
            "server_version": driver.get("server_version"),
            "python_version": platform.python_version(),
            "package_versions": versions,
        }
    )
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    markdown = _render_summary(result)
    args.markdown.write_text(markdown, encoding="utf-8")
    if args.step_summary:
        with args.step_summary.open("a", encoding="utf-8") as handle:
            handle.write(markdown)
    print(markdown)
    return 0 if result["status"] == "success" else 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    verify = commands.add_parser("verify-driver")
    verify.add_argument("--expected-sha", required=True)
    verify.add_argument("--out", type=Path, required=True)
    report = commands.add_parser("report")
    report.add_argument("--lane", choices=tuple(WORKLOADS), required=True)
    report.add_argument("--report-dir", type=Path, required=True)
    report.add_argument("--driver-evidence", type=Path, required=True)
    report.add_argument("--expected-sha", required=True)
    report.add_argument("--downstream-sha", default="")
    report.add_argument("--job-status", required=True)
    report.add_argument("--test-outcome", required=True)
    report.add_argument("--out", type=Path, required=True)
    report.add_argument("--markdown", type=Path, required=True)
    report.add_argument("--step-summary", type=Path)
    args = parser.parse_args(argv)
    if args.command == "verify-driver":
        try:
            evidence = verify_driver_origin(args.expected_sha)
            evidence["server_version"] = probe_server()
            args.out.parent.mkdir(parents=True, exist_ok=True)
            args.out.write_text(json.dumps(evidence, indent=2) + "\n", encoding="utf-8")
            print(json.dumps(evidence, sort_keys=True))
            return 0
        except (OSError, ValueError, metadata.PackageNotFoundError) as exc:
            print(f"Downstream driver verification failed: {exc}", file=sys.stderr)
            return 1
    return _report(args)


if __name__ == "__main__":
    raise SystemExit(main())
