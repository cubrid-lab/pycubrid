"""Cross-version comparison core for the CUBRID version differential (issue #351).

The live suite (``tests/test_version_differential.py``) runs one generated
workload against every CUBRID server in ``CUBRID_VERSION_MATRIX`` and records,
per statement, what the driver exposes: the error class/``errno``/``sqlstate``,
``rowcount``, ``lastrowid``, ``description`` and the fetched rows (Python type
and value). This module holds the parts that need no server, so they are unit
tested offline (``tests/test_version_matrix_logic.py``):

* :func:`parse_matrix` reads the endpoint list from the environment;
* :func:`normalize_value` turns fetched values into comparable tokens;
* :func:`classify` decides whether a divergence is explained by exactly one
  entry of :data:`ALLOWLIST` and returns a readable report otherwise.

A divergence is explained only when, for every differing statement field, one
group of versions is the reference behavior and every other group is covered
by a distinct entry whose ``tag`` the generator put on the workload, whose
``fields`` include that field, and whose ``versions`` all differ from the
reference (see :func:`_explain`). Anything else fails the test.
"""

from __future__ import annotations

import datetime
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal

ENV_VAR = "CUBRID_VERSION_MATRIX"
SUPPORTED_VERSIONS: tuple[str, ...] = ("10.2", "11.0", "11.2", "11.4")

# One statement's observation: field name -> comparable value.
Step = Mapping[str, object]
Observation = Sequence[Step]
# Allowlist tags: one set for every statement, or one set per statement so a
# documented difference in one statement cannot hide a regression in another.
Tags = frozenset[str] | Sequence[frozenset[str]]

FIELDS = frozenset({"error", "rowcount", "lastrowid", "description", "rows"})


@dataclass(frozen=True)
class Endpoint:
    """One CUBRID server of the matrix."""

    version: str
    host: str
    port: int


def parse_matrix(raw: str) -> list[Endpoint]:
    """Parse ``"10.2=host:port,11.4=host:port"`` into endpoints.

    Raises ``ValueError`` for anything malformed: a configured-but-broken
    matrix must fail the lane, never silently shrink it.
    """
    endpoints: list[Endpoint] = []
    seen: set[str] = set()
    for item in raw.split(","):
        item = item.strip()
        if not item:
            continue
        version, sep, address = item.partition("=")
        host, colon, port = address.rpartition(":")
        if not sep or not colon or not host or not port.isdigit():
            raise ValueError(f"{ENV_VAR}: expected VERSION=HOST:PORT, got {item!r}")
        version = version.strip()
        if version not in SUPPORTED_VERSIONS:
            raise ValueError(f"{ENV_VAR}: unsupported CUBRID version {version!r}")
        if version in seen:
            raise ValueError(f"{ENV_VAR}: version {version} listed twice")
        seen.add(version)
        endpoints.append(Endpoint(version, host.strip(), int(port)))
    if len(endpoints) < 2:
        raise ValueError(f"{ENV_VAR}: a differential needs at least two versions")
    return sorted(endpoints, key=lambda e: SUPPORTED_VERSIONS.index(e.version))


def normalize_value(value: object) -> tuple[str, object]:
    """Return ``(python type name, comparable token)`` for a fetched value.

    Floats compare by ``repr`` (exact bits), decimals by their string (scale
    matters), temporal values by ISO text plus UTC offset, and collections
    element-wise; everything else by ``repr``.
    """
    kind = type(value).__name__
    if value is None:
        return (kind, None)
    if isinstance(value, float):
        return (kind, repr(value))
    if isinstance(value, Decimal):
        return (kind, str(value))
    if isinstance(value, (datetime.datetime, datetime.time)):
        tz = value.tzinfo
        return (kind, (value.isoformat(), repr(tz), str(value.utcoffset())))
    if isinstance(value, datetime.date):
        return (kind, value.isoformat())
    if isinstance(value, (bytes, bytearray)):
        return (kind, bytes(value).hex())
    if isinstance(value, (frozenset, set)):
        return (kind, tuple(sorted((normalize_value(v) for v in value), key=repr)))
    if isinstance(value, (list, tuple)):
        return (kind, tuple(normalize_value(v) for v in value))
    return (kind, repr(value))


@dataclass(frozen=True)
class VersionDifference:
    """A documented, intentional behavior difference between CUBRID versions."""

    tag: str  # set by the generator on every workload that can hit it
    versions: frozenset[str]  # the versions that behave differently from the rest
    fields: frozenset[str]  # statement fields allowed to differ
    reason: str
    link: str


ALLOWLIST: tuple[VersionDifference, ...] = (
    VersionDifference(
        tag="string-overflow",
        versions=frozenset({"10.2"}),
        fields=frozenset({"error", "rowcount", "lastrowid", "description", "rows"}),
        reason=(
            "Inserting or updating a string longer than its CHAR(n)/VARCHAR(n) column, "
            "or a bit string longer than its BIT VARYING(n) column: 10.2 silently "
            "truncates it; 11.0 added allow_truncated_string (default no), so 11.0+ "
            "raise ProgrammingError errno -494 SQLSTATE 42000 instead. When the 10.2 "
            "cut splits a multi-byte UTF-8 character, reading the column back raises "
            "DataError (invalid UTF-8 from the server, by design since #499)."
        ),
        link="https://www.cubrid.org/manual/en/11.0/admin/config.html",
    ),
    VersionDifference(
        tag="regexp-function",
        versions=frozenset({"10.2"}),
        fields=frozenset({"error", "rowcount", "lastrowid", "description", "rows"}),
        reason=(
            "REGEXP_LIKE/REGEXP_COUNT/REGEXP_INSTR/REGEXP_REPLACE/REGEXP_SUBSTR were "
            "added in 11.0; 10.2 raises ProgrammingError errno -494 (undefined function)."
        ),
        link="https://github.com/CUBRID/cubrid/pull/2203",
    ),
    VersionDifference(
        tag="non-logical-condition",
        versions=frozenset({"11.2", "11.4"}),
        fields=frozenset({"error", "rowcount", "lastrowid", "description", "rows"}),
        reason=(
            "A condition that is a bare constant or column (IF(1, a, b), WHERE 1): "
            "10.2/11.0 rewrite it as '<> 0'; 11.2+ require a logical expression and "
            "raise ProgrammingError errno -493 SQLSTATE 42000 (CBRD-24083)."
        ),
        link="https://github.com/CUBRID/cubrid/pull/3119",
    ),
    VersionDifference(
        tag="char-concat",
        versions=frozenset({"10.2"}),
        fields=frozenset({"description"}),
        reason=(
            "Concatenating fixed-length strings ('a' || 'b', CONCAT, CONCAT_WS, '+'): "
            "10.2 types the result CHAR/NCHAR (type_code 1/3, precision -1); 11.0+ type "
            "it VARCHAR/NCHAR VARYING (type_code 2/4). Values are identical. Part of the "
            "11.0 string-comparison rework (CBRD-23731), which fixed the concatenation "
            "result type."
        ),
        link="https://github.com/CUBRID/cubrid/pull/2488",
    ),
    VersionDifference(
        tag="varchar-trailing-space",
        versions=frozenset({"10.2"}),
        fields=frozenset({"rows"}),
        reason=(
            "Comparing variable-length strings that differ only in trailing spaces "
            "(CAST('a' AS VARCHAR) = 'a '): 10.2 ignores trailing spaces (true); 11.0+ "
            "compare them (false). Fixed-length CHAR comparisons still pad (CBRD-23731)."
        ),
        link="https://github.com/CUBRID/cubrid/pull/2421",
    ),
    VersionDifference(
        tag="truncate-referenced-parent",
        versions=frozenset({"10.2", "11.0"}),
        fields=frozenset({"error"}),
        reason=(
            "TRUNCATE of a parent table that a foreign key still references: 10.2/11.0 "
            "report errno -924 (ER_FK_RESTRICT); 11.2 reworked TRUNCATE for FKs and "
            "reports -1284 (ER_TRUNCATE_PK_REFERRED). Both map to IntegrityError/23000."
        ),
        link="https://www.cubrid.org/manual/en/11.2/release_note/release_note_latest_ver.html",
    ),
    VersionDifference(
        tag="count-bigint",
        versions=frozenset({"10.2", "11.0"}),
        fields=frozenset({"description"}),
        reason=(
            "COUNT(*) / COUNT(expr) result type: 10.2/11.0 return INTEGER (type_code 8, "
            "precision 10); 11.2 changed it to BIGINT (type_code 21, precision 19) "
            "(CBRD-23903). Values are identical Python ints."
        ),
        link="https://github.com/CUBRID/cubrid/pull/2876",
    ),
    VersionDifference(
        tag="int-numeric-precision",
        versions=frozenset({"11.2"}),
        fields=FIELDS,
        reason=(
            "Arithmetic mixing an integer and a NUMERIC operand ((5) * (0.100), 7 / 2.0): "
            "11.2 coerces SMALLINT/INTEGER/BIGINT to NUMERIC with one fixed precision, so "
            "the result column reports a different precision (e.g. 19 instead of 14), and "
            "a BIGINT of 17+ digits overflows it: 10000000000000000 + 0.000 raises "
            "DatabaseError errno -427 (numeric data overflow) on 11.2 only. 11.3 restored "
            "per-type precision (CBRD-24815), so 10.2/11.0/11.4 agree."
        ),
        link="https://github.com/CUBRID/cubrid/pull/4375",
    ),
    VersionDifference(
        tag="regexp-operator-multibyte",
        versions=frozenset({"10.2"}),
        fields=frozenset({"rows"}),
        reason=(
            "The REGEXP/RLIKE operator on a subject containing 3-byte UTF-8 characters "
            "(e.g. Hangul): 10.2 matches it ('a한' REGEXP '^a' is 1); since the 11.0 "
            "unicode-aware regex rework (CBRD-22837) such subjects never match (0) on "
            "11.0-11.4. Server-side; csql returns the same values."
        ),
        link="https://github.com/CUBRID/cubrid/pull/1671",
    ),
    VersionDifference(
        tag="regexp-function-multibyte",
        versions=frozenset({"11.0"}),
        fields=frozenset({"description", "rows"}),
        reason=(
            "REGEXP_LIKE/REGEXP_COUNT/... on a subject containing 3-byte UTF-8 "
            "characters: 11.0 (std::regex backend) returns NULL typed NULL "
            "(type_code 0); 11.2 backported the RE2 backend (CBRD-24563), and 11.2/11.4 "
            "return INTEGER 0. 10.2 has no REGEXP_* functions (regexp-function)."
        ),
        link="https://github.com/CUBRID/cubrid/pull/4057",
    ),
)


def _partition(values: Mapping[str, object]) -> list[frozenset[str]]:
    groups: dict[str, set[str]] = {}
    for version, value in values.items():
        groups.setdefault(repr(value), set()).add(version)
    return [frozenset(g) for g in groups.values()]


def _step_field(step: Step | None, field: str) -> object:
    if step is None:
        return "<missing step>"
    return step.get(field, "<absent>")


def differing_fields(
    observations: Mapping[str, Observation],
) -> list[tuple[int, str, list[frozenset[str]]]]:
    """Return ``(step index, field, version partition)`` for every difference."""
    diffs: list[tuple[int, str, list[frozenset[str]]]] = []
    length = max(len(obs) for obs in observations.values())
    for index in range(length):
        steps = {v: (obs[index] if index < len(obs) else None) for v, obs in observations.items()}
        keys = FIELDS.union(*(set(s) for s in steps.values() if s is not None))
        # "_"-prefixed keys (e.g. the error message) are report-only context.
        names = sorted(k for k in keys if not k.startswith("_"))
        for field in names:
            values = {v: _step_field(s, field) for v, s in steps.items()}
            groups = _partition(values)
            if len(groups) > 1:
                diffs.append((index, field, groups))
    return diffs


def _explain(
    groups: Sequence[frozenset[str]],
    candidates: Sequence[VersionDifference],
    present: frozenset[str],
) -> list[VersionDifference] | None:
    """Return the entries that explain a version partition, or ``None``.

    One group is the reference behavior; every other group needs its own
    candidate entry that (a) lists all of that group's versions and (b) only
    lists versions that do differ from the reference. Two groups therefore
    need an entry naming exactly the outlier versions. A three-way split
    needs two entries, e.g. 10.2 lacks a function while 11.0 has a different
    bug in it, or 10.2 fails outright and so hides a 10.2+11.0 difference
    that 11.0 still shows.
    """
    for reference in groups:
        outliers = present - reference
        others = [g for g in groups if g != reference]
        options = [
            [e for e in candidates if g <= e.versions and e.versions & present <= outliers]
            for g in others
        ]
        chosen = _assign(options, [])
        if chosen is not None:
            return chosen
    return None


def _assign(
    options: Sequence[Sequence[VersionDifference]], taken: list[VersionDifference]
) -> list[VersionDifference] | None:
    """Pick one distinct entry per group (tiny backtracking search)."""
    if not options:
        return list(taken)
    for entry in options[0]:
        if entry not in taken:
            found = _assign(options[1:], [*taken, entry])
            if found is not None:
                return found
    return None


def step_tags(tags: Tags, index: int) -> frozenset[str]:
    """Return the tags that apply to statement ``index``."""
    if isinstance(tags, frozenset):
        return tags
    return tags[index] if index < len(tags) else frozenset()


def classify(
    tags: Tags,
    observations: Mapping[str, Observation],
    allowlist: Sequence[VersionDifference] = ALLOWLIST,
) -> tuple[list[VersionDifference], list[str]]:
    """Explain every divergence or report it.

    Returns ``(entries used, unexplained problems)``. The test fails when the
    second list is non-empty.
    """
    present = frozenset(observations)
    used: list[VersionDifference] = []
    problems: list[str] = []
    for index, field, groups in differing_fields(observations):
        applicable = step_tags(tags, index)
        candidates = [e for e in allowlist if e.tag in applicable and field in e.fields]
        explained = _explain(groups, candidates, present)
        if explained is None:
            split = " vs ".join("/".join(sorted(g)) for g in groups)
            problems.append(f"step {index} field {field!r} differs: {split}")
            continue
        used.extend(e for e in explained if e not in used)
    return used, problems


def format_report(
    statements: Sequence[str],
    tags: Tags,
    observations: Mapping[str, Observation],
    problems: Sequence[str],
) -> str:
    """Human-readable divergence report for an assertion message."""
    lines = ["Unexplained CUBRID version divergence:"]
    lines += [f"  - {p}" for p in problems]
    for index, sql in enumerate(statements):
        lines.append(f"[{index}] {sql}  tags={sorted(step_tags(tags, index))}")
        for version, obs in observations.items():
            step = obs[index] if index < len(obs) else None
            lines.append(f"      {version}: {dict(step) if step is not None else None}")
    lines.append(
        "Fix the driver, file a bug, or add a VersionDifference with a reason and "
        "link to tests/helpers/version_matrix.py (see CONTRIBUTING.md)."
    )
    return "\n".join(lines)
