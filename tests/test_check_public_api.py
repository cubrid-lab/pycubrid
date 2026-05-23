from __future__ import annotations

import functools
import importlib.util
import sys
from pathlib import Path

import pytest

SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "check_public_api.py"


@pytest.fixture(scope="module")
def api_module():
    spec = importlib.util.spec_from_file_location("_check_public_api_module", SCRIPT_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules["_check_public_api_module"] = module
    spec.loader.exec_module(module)
    return module


def _own_class(cls: type) -> type:
    """Re-tag a synthetic class so the ownership filter treats it as pycubrid code.

    The extractor intentionally skips members defined outside the ``pycubrid``
    package so that built-in dunders do not pollute the baseline. Synthetic
    classes used in unit tests must opt into the filter to exercise the real
    extraction path; otherwise the assertions pass for the wrong reason
    (every member would be filtered out before classification).
    """
    cls.__module__ = "pycubrid._test_synthetic"
    return cls


def test_classmethod_is_bucketed_as_method(api_module):
    @_own_class
    class Sample:
        @classmethod
        def factory(cls, x: int) -> "Sample":
            return cls()

    described = api_module._describe_class(Sample)

    assert "factory" in described["methods"], (
        "classmethod must be classified as a method, not silently dropped or "
        "bucketed as class_var (Oracle Phase 4 blocker B.4)"
    )
    assert "factory" not in described["class_vars"]
    sig = described["methods"]["factory"]["signature"]
    param_names = [p["name"] for p in sig["params"]]
    assert "x" in param_names


def test_staticmethod_is_bucketed_as_method(api_module):
    @_own_class
    class Sample:
        @staticmethod
        def helper(x: int) -> int:
            return x

    described = api_module._describe_class(Sample)

    assert "helper" in described["methods"]
    assert "helper" not in described["class_vars"]


def test_cached_property_is_bucketed_as_property(api_module):
    @_own_class
    class Sample:
        @functools.cached_property
        def computed(self) -> int:
            return 42

    described = api_module._describe_class(Sample)

    assert "computed" in described["properties"]
    assert "computed" not in described["methods"]
    assert "computed" not in described["class_vars"]


def test_regular_property_is_bucketed_as_property(api_module):
    @_own_class
    class Sample:
        @property
        def value(self) -> int:
            return 0

    described = api_module._describe_class(Sample)
    assert "value" in described["properties"]


def test_inherited_baseexception_members_are_excluded(api_module):
    """Cross-Python stability: BaseException members must not leak in.

    Regression test for Oracle Phase 4 blocker B.3 — ``BaseException.add_note``
    (added in CPython 3.11) caused the baseline to drift between supported
    Python versions when inherited members were not filtered out.
    """

    @_own_class
    class CustomError(Exception):
        def __init__(self, msg: str) -> None:
            super().__init__(msg)

    described = api_module._describe_class(CustomError)

    inherited_leak = {
        name for name in described["methods"] if name in {"add_note", "with_traceback"}
    }
    assert inherited_leak == set(), (
        f"Inherited BaseException members leaked into surface: {inherited_leak}"
    )

    assert "args" not in described["class_vars"]
    assert "args" not in described["properties"]
    assert "__init__" in described["methods"]


def test_non_pycubrid_member_is_filtered(api_module):
    class Sample:
        def helper(self) -> int:
            return 0

    described = api_module._describe_class(Sample)
    assert "helper" not in described["methods"], (
        "Members defined outside the pycubrid package must be filtered (Oracle Phase 4 blocker B.3)"
    )


def test_scalar_module_attribute_value_is_captured(api_module):
    described = api_module._describe_attribute("qmark")
    assert described == {"kind": "attribute", "type": "str", "value": "qmark"}


def test_int_attribute_value_is_captured(api_module):
    described = api_module._describe_attribute(1)
    assert described["value"] == 1


def test_bool_attribute_value_is_captured(api_module):
    described = api_module._describe_attribute(True)
    assert described["value"] is True


def test_non_scalar_attribute_value_is_not_captured(api_module):
    described = api_module._describe_attribute(object())
    assert "value" not in described


def test_full_surface_passes_against_committed_baseline(api_module):
    """Smoke test: the script must accept its own committed baseline.

    Belongs in the unit suite (not just CI) so that any local regeneration
    that produces a non-canonical baseline is caught before push.
    """
    current = api_module.extract_full_surface()
    baseline_path = api_module.BASELINE_PATH
    if not baseline_path.exists():
        pytest.skip("baseline not present in this checkout")

    import json

    baseline = json.loads(baseline_path.read_text(encoding="utf-8"))

    diffs = api_module._diff_dicts(baseline, current)
    assert diffs == [], "Committed baseline diverges from current surface:\n  " + "\n  ".join(diffs)
