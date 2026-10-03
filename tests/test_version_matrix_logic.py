"""Offline tests for the version-differential comparison core (issue #351)."""

from __future__ import annotations

import datetime
from decimal import Decimal

import pytest

from tests.helpers.version_matrix import (
    ALLOWLIST,
    SUPPORTED_VERSIONS,
    Endpoint,
    VersionDifference,
    classify,
    differing_fields,
    format_report,
    normalize_value,
    parse_matrix,
)

ALL = ("10.2", "11.0", "11.2", "11.4")
Obs = dict[str, list[dict[str, object]]]

ERROR: dict[str, object] = {"error": ("ProgrammingError", -494, "42000")}
OK_ROW: dict[str, object] = {"rowcount": 1, "lastrowid": None}


def _same(step: dict[str, object]) -> Obs:
    return {v: [dict(step)] for v in ALL}


def test_parse_matrix_orders_by_version() -> None:
    endpoints = parse_matrix("11.4=h:4, 10.2=h:2,11.0=host.example:33110")
    assert endpoints == [
        Endpoint("10.2", "h", 2),
        Endpoint("11.0", "host.example", 33110),
        Endpoint("11.4", "h", 4),
    ]


@pytest.mark.parametrize(
    ("raw", "message"),
    [
        ("10.2=h:1", "at least two"),
        ("10.2=h:1,10.2=h:2", "listed twice"),
        ("9.3=h:1,10.2=h:2", "unsupported"),
        ("10.2=h,11.4=h:2", "VERSION=HOST:PORT"),
        ("10.2:h:1,11.4=h:2", "VERSION=HOST:PORT"),
        ("10.2=h:x,11.4=h:2", "VERSION=HOST:PORT"),
    ],
)
def test_parse_matrix_rejects_malformed_configuration(raw: str, message: str) -> None:
    with pytest.raises(ValueError, match=message):
        parse_matrix(raw)


def test_normalize_value_keeps_type_and_exact_value() -> None:
    assert normalize_value(0.1) == ("float", "0.1")
    assert normalize_value(Decimal("1.50")) == ("Decimal", "1.50")
    assert normalize_value(Decimal("1.5")) != normalize_value(Decimal("1.50"))
    assert normalize_value(1) != normalize_value(True)
    assert normalize_value(b"\x01") == ("bytes", "01")
    assert normalize_value(None) == ("NoneType", None)
    assert normalize_value(datetime.date(2024, 1, 2)) == ("date", "2024-01-02")


def test_normalize_value_distinguishes_time_zones() -> None:
    utc = datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc)
    kst = utc.astimezone(datetime.timezone(datetime.timedelta(hours=9)))
    assert utc == kst
    assert normalize_value(utc) != normalize_value(kst)


def test_normalize_value_sets_are_order_independent() -> None:
    assert normalize_value(frozenset({3, 1})) == normalize_value(frozenset({1, 3}))
    assert normalize_value([1, 3]) != normalize_value([3, 1])


def test_agreement_has_no_problems() -> None:
    used, problems = classify(frozenset(), _same(OK_ROW))
    assert (used, problems) == ([], [])


def test_unexplained_divergence_is_reported() -> None:
    observations = _same(OK_ROW)
    observations["11.4"] = [dict(ERROR)]
    used, problems = classify(frozenset(), observations)
    assert used == []
    assert any("'error'" in p and "10.2/11.0/11.2 vs 11.4" in p for p in problems)


def test_tagged_divergence_matching_an_entry_is_explained() -> None:
    observations = _same(ERROR)
    observations["10.2"] = [{"rowcount": 1, "lastrowid": None}]
    used, problems = classify(frozenset({"string-overflow"}), observations)
    assert problems == []
    assert [entry.tag for entry in used] == ["string-overflow"]


def test_untagged_workload_cannot_use_an_entry() -> None:
    observations = _same(ERROR)
    observations["10.2"] = [dict(OK_ROW)]
    _used, problems = classify(frozenset({"char-concat"}), observations)
    assert problems  # char-concat only allows description differences


def test_wrong_version_split_is_not_explained() -> None:
    observations = _same(ERROR)
    observations["11.4"] = [dict(OK_ROW)]  # entry says 10.2 differs, not 11.4
    _used, problems = classify(frozenset({"string-overflow"}), observations)
    assert problems


def test_three_way_split_is_not_explained() -> None:
    observations = _same(OK_ROW)
    observations["10.2"] = [dict(ERROR)]
    observations["11.4"] = [{"error": ("DataError", None, "22000")}]
    _used, problems = classify(frozenset({"string-overflow"}), observations)
    assert problems


def test_three_way_split_is_explained_when_each_outlier_group_has_an_entry() -> None:
    old = VersionDifference("old", frozenset({"10.2"}), frozenset({"rows"}), "r", "l")
    mid = VersionDifference("mid", frozenset({"11.0"}), frozenset({"rows"}), "r", "l")
    observations: Obs = {
        "10.2": [{"rows": (1,)}],
        "11.0": [{"rows": (None,)}],
        "11.2": [{"rows": (0,)}],
        "11.4": [{"rows": (0,)}],
    }
    used, problems = classify(frozenset({"old", "mid"}), observations, allowlist=[old, mid])
    assert problems == []
    assert sorted(entry.tag for entry in used) == ["mid", "old"]


def test_three_way_split_needs_every_outlier_group_explained() -> None:
    old = VersionDifference("old", frozenset({"10.2"}), frozenset({"rows"}), "r", "l")
    mid = VersionDifference("mid", frozenset({"11.0"}), frozenset({"rows"}), "r", "l")
    observations: Obs = {
        "10.2": [{"rows": (1,)}],
        "11.0": [{"rows": (None,)}],
        "11.2": [{"rows": (0,)}],
        "11.4": [{"rows": (2,)}],
    }
    _used, problems = classify(frozenset({"old", "mid"}), observations, allowlist=[old, mid])
    assert problems


def test_outlier_group_may_be_part_of_a_wider_entry_masked_by_another() -> None:
    # 10.2 fails for one documented reason; 11.0 still shows a wider (10.2 +
    # 11.0) documented difference that 10.2's failure hides.
    only_old = VersionDifference("old", frozenset({"10.2"}), frozenset({"rows"}), "r", "l")
    wide = VersionDifference("wide", frozenset({"10.2", "11.0"}), frozenset({"rows"}), "r", "l")
    observations: Obs = {
        "10.2": [{"rows": ("error",)}],
        "11.0": [{"rows": ("int",)}],
        "11.2": [{"rows": ("bigint",)}],
        "11.4": [{"rows": ("bigint",)}],
    }
    used, problems = classify(frozenset({"old", "wide"}), observations, allowlist=[only_old, wide])
    assert problems == []
    assert sorted(entry.tag for entry in used) == ["old", "wide"]


def test_wider_entry_needs_all_its_versions_to_differ() -> None:
    wide = VersionDifference("wide", frozenset({"10.2", "11.0"}), frozenset({"rows"}), "r", "l")
    observations = _same({"rows": (0,)})
    observations["10.2"] = [{"rows": (1,)}]  # 11.0 agrees with 11.2/11.4
    _used, problems = classify(frozenset({"wide"}), observations, allowlist=[wide])
    assert problems


def test_step_tags_explain_only_their_own_statement() -> None:
    entry = VersionDifference("t", frozenset({"10.2"}), frozenset({"rows"}), "r", "l")
    observations: Obs = {v: [{"rows": (0,)}, {"rows": (0,)}] for v in ALL}
    observations["10.2"] = [{"rows": (1,)}, {"rows": (1,)}]
    used, problems = classify([frozenset({"t"}), frozenset()], observations, allowlist=[entry])
    assert used == [entry]
    assert problems == ["step 1 field 'rows' differs: 10.2 vs 11.0/11.2/11.4"]


def test_report_only_keys_are_not_compared() -> None:
    observations = _same(ERROR)
    observations["11.4"] = [dict(ERROR, _message="differs per version")]
    assert differing_fields(observations) == []


def test_entry_restricted_to_configured_versions() -> None:
    entry = VersionDifference("t", frozenset({"11.2", "11.4"}), frozenset({"rows"}), "r", "l")
    observations: Obs = {"10.2": [{"rows": (1,)}], "11.4": [{"rows": (0,)}]}
    used, problems = classify(frozenset({"t"}), observations, allowlist=[entry])
    assert problems == [] and used == [entry]


def test_missing_step_is_a_divergence() -> None:
    observations = _same(OK_ROW)
    observations["11.0"] = []
    diffs = differing_fields(observations)
    assert diffs and all(index == 0 for index, _field, _groups in diffs)


def test_report_names_statements_versions_and_remedy() -> None:
    observations = _same(OK_ROW)
    observations["11.4"] = [dict(ERROR)]
    _used, problems = classify(frozenset(), observations)
    report = format_report(["INSERT ..."], frozenset(), observations, problems)
    assert "INSERT ..." in report and "11.4" in report and "VersionDifference" in report


@pytest.mark.parametrize("entry", ALLOWLIST, ids=[entry.tag for entry in ALLOWLIST])
def test_allowlist_entries_are_documented(entry: VersionDifference) -> None:
    assert entry.reason.strip()
    assert entry.link.startswith("https://")
    assert entry.versions and entry.versions < frozenset(SUPPORTED_VERSIONS)
    assert entry.fields <= {"error", "rowcount", "lastrowid", "description", "rows"}


def test_allowlist_tags_are_unique() -> None:
    tags = [entry.tag for entry in ALLOWLIST]
    assert len(tags) == len(set(tags))


def test_every_allowlist_entry_has_a_live_probe() -> None:
    from tests.test_version_differential import PROBES

    assert sorted(PROBES) == sorted(entry.tag for entry in ALLOWLIST)
