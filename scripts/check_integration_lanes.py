"""Audit collected integration markers, workflow selectors, and reported skips.

No test-count constants: pytest collection is the inventory. Missing dependencies
for the optional native-driver comparison and platforms without /proc are explicit
exceptions; missing broker/TLS configuration or unknown skip reasons fail CI.
"""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any
from xml.etree import ElementTree  # nosec B405 - local JUnit only; DTD/entities rejected below

import pytest

ROOT = Path(__file__).resolve().parents[1]
SELECTORS = {
    "normal": "integration and not slow and not tls",
    "slow": "integration and slow and not tls",
    "tls": "integration and tls",
    # Cross-version differential (#351): every CUBRID of the matrix at once.
    "version": "integration and version_matrix",
}


def _module_of(identity: str) -> str:
    """Return the test module name of a collection node id or JUnit identity.

    Accepts ``tests/test_x.py::test`` (collection) and ``tests.test_x::test``
    (JUnit ``classname::name``, possibly with a class after the module).
    """
    head = identity.split("::", 1)[0]
    if head.endswith(".py"):
        return head.rsplit("/", 1)[-1][: -len(".py")]
    for part in head.split("."):
        if part.startswith("test_"):
            return part
    return head


def skip_category(identity: str, reason: str) -> str:
    if (
        "test_cubriddb_differential" in identity
        and "official CUBRIDdb C-extension not installed" in reason
    ):
        return "optional-native-driver"
    if (
        any(
            name in identity
            for name in ("test_resource_leaks", "test_soak", "test_tls_matrix_integration")
        )
        and "cannot count file descriptors on this platform" in reason
    ):
        return "platform-without-proc"
    if (
        _module_of(identity) == "test_integration_charset"
        and "requires an EUC-KR database (integration-charset lane)" in reason
    ):
        return "charset-lane-only"
    if (
        _module_of(identity) == "test_version_differential"
        and "CUBRID_VERSION_MATRIX not set" in reason
    ):
        return "version-lane-only"
    raise ValueError(f"unclassified integration skip: {identity}: {reason}")


def verify_workflows(root: Path = ROOT) -> None:
    expected = {
        "ci.yml": {"normal", "tls"},
        "integration-full.yml": {"normal", "tls", "version"},
        "bug-hunt.yml": {"normal", "slow"},
    }
    for filename, lanes in expected.items():
        content = (root / ".github" / "workflows" / filename).read_text()
        selectors = re.findall(r'^\s+python -m pytest tests/ -m "([^"]+)"', content, re.MULTILINE)
        for lane in lanes:
            if SELECTORS[lane] not in selectors:
                raise ValueError(f"{filename} has no executable {lane} marker selection")
    # The EUC-KR charset lane (#86) selects its module by path, not by marker.
    ci = (root / ".github" / "workflows" / "ci.yml").read_text()
    if not re.search(r"^\s+python -m pytest tests/test_integration_charset\.py ", ci, re.MULTILINE):
        raise ValueError("ci.yml has no executable EUC-KR charset lane")


class Inventory:
    def __init__(self) -> None:
        self.lanes: dict[str, list[str]] = {lane: [] for lane in SELECTORS}
        self.collection_skips: list[dict[str, str]] = []
        self.invalid: list[str] = []

    def pytest_collection_modifyitems(self, items: list[pytest.Item]) -> None:
        for item in items:
            integration = item.get_closest_marker("integration") is not None
            slow = item.get_closest_marker("slow") is not None
            tls = item.get_closest_marker("tls") is not None
            version = item.get_closest_marker("version_matrix") is not None
            if not integration:
                if slow or tls or version:
                    self.invalid.append(
                        f"{item.nodeid}: slow/tls/version_matrix requires integration"
                    )
                continue
            lane = "tls" if tls else "slow" if slow else "normal"
            self.lanes[lane].append(item.nodeid)
            if version:
                # Also selected (and skipped as version-lane-only) by the normal lane.
                self.lanes["version"].append(item.nodeid)

    def pytest_collectreport(self, report: pytest.CollectReport) -> None:
        if report.skipped:
            reason = str(report.longrepr)
            try:
                category = skip_category(report.nodeid, reason)
            except ValueError as exc:
                self.invalid.append(str(exc))
            else:
                self.collection_skips.append({"node": report.nodeid, "category": category})


def verify_results(path: Path) -> dict[str, Any]:
    payload = path.read_text(encoding="utf-8-sig")
    if "\x00" in payload or "<!DOCTYPE" in payload.upper() or "<!ENTITY" in payload.upper():
        raise ValueError(f"{path}: DTD/entity declarations are forbidden in JUnit reports")
    root = ElementTree.fromstring(payload)  # nosec B314 - UTF-8 only; DTD/entity declarations rejected
    cases = list(root.iter("testcase"))
    if not cases:
        raise ValueError(f"{path}: no test cases ran")
    skips = []
    for case in cases:
        skipped = case.find("skipped")
        if skipped is not None:
            identity = case.get("classname", "") + "::" + case.get("name", "")
            reason = skipped.get("message", "") + " " + (skipped.text or "")
            skips.append({"node": identity, "category": skip_category(identity, reason)})
        if case.find("failure") is not None or case.find("error") is not None:
            raise ValueError(f"{path}: failed tests cannot pass the lane audit")
    if len(skips) == len(cases):
        raise ValueError(f"{path}: all selected tests skipped")
    return {"report": str(path), "tests": len(cases), "classified_skips": skips}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--results", type=Path)
    args = parser.parse_args()
    try:
        if args.results is not None:
            print(json.dumps(verify_results(args.results), sort_keys=True))
            return 0
        verify_workflows()
        inventory = Inventory()
        status = pytest.main(
            [
                str(ROOT / "tests"),
                "--collect-only",
                "-p",
                "no:terminal",
                "-p",
                "no:cacheprovider",
                "-o",
                "addopts=",
            ],
            plugins=[inventory],
        )
        if status != 0:
            return int(status)
        if inventory.invalid:
            raise ValueError("; ".join(inventory.invalid))
        if any(not nodes for nodes in inventory.lanes.values()):
            raise ValueError("normal, slow, TLS, and version lanes must each have collected tests")
        print(
            json.dumps(
                {
                    "integration_total": sum(len(nodes) for nodes in inventory.lanes.values()),
                    "counts": {lane: len(nodes) for lane, nodes in inventory.lanes.items()},
                    "modules": {
                        lane: sorted({node.split("::", 1)[0] for node in nodes})
                        for lane, nodes in inventory.lanes.items()
                    },
                    "collection_skips": inventory.collection_skips,
                },
                sort_keys=True,
            )
        )
        return 0
    except (ValueError, OSError, ElementTree.ParseError) as exc:
        print(f"Integration lane audit failed: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
