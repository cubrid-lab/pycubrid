"""Property-driven behavior differential across CUBRID 10.2-11.4 (issue #351).

The same Hypothesis-generated values and statements run against every server
listed in ``CUBRID_VERSION_MATRIX`` (``"10.2=host:port,11.0=host:port,..."``).
For each statement the suite records what an application can observe through
pycubrid: error class, ``errno`` and ``sqlstate``; ``rowcount``;
``lastrowid``; ``cursor.description`` (type code, precision, scale,
nullability); and every fetched value's Python type and value. Then:

* all versions must agree, unless the statement carries the tags of
  documented :data:`~tests.helpers.version_matrix.ALLOWLIST` entries (reason +
  link) that explain exactly that split of versions and those fields;
* a DB-API error must never leave the session unusable, and anything that is
  not a ``pycubrid.Error`` fails the test as a raw exception leak;
* every allowlist entry is pinned by a deterministic probe, so an entry that
  stops reproducing (a stale expectation) fails too.

Workloads: bound-parameter round-trips per column type, generated scalar
expressions (numeric, string, temporal, JSON, collection, conditional),
condition forms, DML sequences (rowcount/lastrowid), error-producing
statements, and generated table schemas (``description`` metadata).

Runs only in the multi-version lane (``integration and version_matrix``, the
``version-differential`` job of ``integration-full.yml``); elsewhere it skips
with a classified reason. Exploration width follows ``HYPOTHESIS_PROFILE``
and failing examples print a ``@reproduce_failure`` blob (#359).
"""

from __future__ import annotations

import datetime
import json
import os
import uuid
from collections.abc import Iterator, Sequence
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

import pytest
from hypothesis import HealthCheck, given, settings, strategies as st

import pycubrid
from pycubrid.exceptions import Error as DBAPIError

from ._parity_helpers import TEST_DB, TEST_PASSWORD, TEST_USER
from .helpers.version_matrix import (
    ALLOWLIST,
    ENV_VAR,
    Endpoint,
    VersionDifference,
    classify,
    format_report,
    normalize_value,
    parse_matrix,
)

MATRIX_RAW = os.environ.get(ENV_VAR, "")

pytestmark = [
    pytest.mark.integration,
    pytest.mark.version_matrix,
    pytest.mark.skipif(
        not MATRIX_RAW,
        reason=f"{ENV_VAR} not set: the version differential runs only in the multi-version lane",
    ),
]

LIVE = settings(
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.data_too_large],
)
# Workloads that write to tables commit on every statement on four servers
# and cost ~20x a SELECT; cap them on nightly so the lane fits its timeout.
# Pure SELECT workloads (expressions, conditions) use the full profile.
WRITE_BUDGET = min(settings().max_examples, 200)
# Schema workloads create a table per example.
SCHEMA_BUDGET = min(settings().max_examples, 150)

RUN_ID = uuid.uuid4().hex[:6]


# ---------------------------------------------------------------------------
# Statements and servers
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Stmt:
    """One statement; ``{t}``-style placeholders name scratch tables."""

    sql: str
    params: tuple[object, ...] | list[tuple[object, ...]] | None = None
    many: bool = False
    # Allowlist tags for this statement only, so a documented difference in
    # one statement cannot hide a regression in another. A statement that
    # reads what an earlier tagged statement wrote carries the tag too.
    tags: frozenset[str] = frozenset()


@dataclass
class Workload:
    statements: list[Stmt]
    setup: list[str] = field(default_factory=list)  # run unobserved first

    @property
    def tags(self) -> list[frozenset[str]]:
        return [stmt.tags for stmt in self.statements]


class Server:
    """One CUBRID server of the matrix with its scratch tables."""

    def __init__(self, endpoint: Endpoint) -> None:
        self.endpoint = endpoint
        self.version = endpoint.version
        self.conn = pycubrid.connect(
            host=endpoint.host,
            port=endpoint.port,
            database=TEST_DB,
            user=TEST_USER,
            password=TEST_PASSWORD,
            decode_collections=True,
        )
        self.conn.autocommit = True
        reported = self.conn.get_server_version()
        if not reported.startswith(endpoint.version + "."):
            raise AssertionError(
                f"{ENV_VAR} lists {endpoint.version} at {endpoint.host}:{endpoint.port}, "
                f"but that server reports {reported}"
            )
        self.server_version = reported
        self.created: list[str] = []

    def raw(self, sql: str) -> None:
        cur = self.conn.cursor()
        try:
            cur.execute(sql)
        finally:
            cur.close()

    def ensure_table(self, name: str, ddl: str) -> None:
        if name not in self.created:
            self.raw("DROP TABLE IF EXISTS %s" % name)
            self.raw(ddl % name)
            self.created.append(name)

    def run(self, workload: Workload) -> list[dict[str, object]]:
        for sql in workload.setup:
            self.raw(sql)
        return [self._observe(stmt) for stmt in workload.statements]

    def _observe(self, stmt: Stmt) -> dict[str, object]:
        cur = self.conn.cursor()
        try:
            try:
                if stmt.many:
                    assert isinstance(stmt.params, list)
                    cur.executemany(stmt.sql, stmt.params)
                elif stmt.params is None:
                    cur.execute(stmt.sql)
                else:
                    cur.execute(stmt.sql, stmt.params)
                step: dict[str, object] = {"rowcount": cur.rowcount, "lastrowid": cur.lastrowid}
                if cur.description is not None:
                    step["description"] = tuple(tuple(col) for col in cur.description)
                    rows = cur.fetchall()
                    step["rows"] = tuple(tuple(normalize_value(v) for v in row) for row in rows)
                return step
            except DBAPIError as exc:
                self._assert_session_survives(stmt, exc)
                return {
                    "error": (
                        type(exc).__name__,
                        getattr(exc, "errno", None),
                        getattr(exc, "sqlstate", None),
                    ),
                    # Messages embed the SQL text and vary by version; report only.
                    "_message": str(exc)[:200],
                }
        finally:
            try:
                cur.close()
            except DBAPIError:
                pass  # the survival probe below already reports a dead session

    def _assert_session_survives(self, stmt: Stmt, exc: DBAPIError) -> None:
        try:
            probe = self.conn.cursor()
            probe.execute("SELECT 1")
            alive = probe.fetchone() == (1,)
            probe.close()
        except DBAPIError:
            alive = False
        assert alive, (
            f"CUBRID {self.server_version}: a statement-level {type(exc).__name__} "
            f"({exc}) left the session unusable. Statement: {stmt.sql!r} {stmt.params!r}"
        )

    def close(self) -> None:
        for name in reversed(self.created):
            try:
                self.raw("DROP TABLE IF EXISTS %s" % name)
            except DBAPIError:
                pass  # best-effort teardown
        self.conn.close()


@pytest.fixture(scope="module")
def servers() -> Iterator[list[Server]]:
    opened: list[Server] = []
    try:
        for endpoint in parse_matrix(MATRIX_RAW):
            opened.append(Server(endpoint))
        yield opened
    finally:
        for server in opened:
            server.close()


def compare(servers: Sequence[Server], workload: Workload) -> None:
    observations = {s.version: s.run(workload) for s in servers}
    _used, problems = classify(workload.tags, observations)
    if problems:
        statements = [f"{st.sql} {st.params!r}" for st in workload.statements]
        raise AssertionError(format_report(statements, workload.tags, observations, problems))


# ---------------------------------------------------------------------------
# Value strategies
# ---------------------------------------------------------------------------

# No NUL/Ctrl-Z (driver rejects them by contract), no surrogates, no
# backslash (escape-mode negotiation is covered elsewhere).
TEXT_ALPHABET = st.characters(blacklist_categories=["Cs"], blacklist_characters="\x00\x1a\\")
texts = st.text(alphabet=TEXT_ALPHABET, max_size=24)
# Identifier-ish and space-free text for expressions: trailing spaces are a
# documented difference exercised by its own tagged workload.
words = st.text(alphabet="abcXYZ019_-.%é한", min_size=0, max_size=8)


def sql_str(text: str) -> str:
    return "'" + text.replace("'", "''") + "'"


json_leaf = st.one_of(
    st.none(),
    st.booleans(),
    st.integers(-(2**31), 2**31 - 1),
    st.floats(allow_nan=False, allow_infinity=False, width=32),
    st.text(alphabet="abc xyz'\"é", max_size=6),
)
json_docs = st.recursive(
    json_leaf,
    lambda inner: st.one_of(
        st.lists(inner, max_size=3),
        st.dictionaries(st.text(alphabet="abk", min_size=1, max_size=3), inner, max_size=3),
    ),
    max_leaves=8,
)


@dataclass(frozen=True)
class ColumnValue:
    ddl: str
    value: object
    tags: frozenset[str] = frozenset()


@st.composite
def column_values(draw: st.DrawFn) -> ColumnValue:
    kind = draw(
        st.sampled_from(
            [
                "short",
                "int",
                "bigint",
                "numeric",
                "float",
                "double",
                "monetary",
                "varchar",
                "char",
                "string",
                "date",
                "time",
                "datetime",
                "timestamp",
                "bitvar",
                "enum",
                "json",
            ]
        )
    )
    if kind == "short":
        return ColumnValue("SHORT", draw(st.integers(-40000, 40000)))
    if kind == "int":
        return ColumnValue("INTEGER", draw(st.integers(-(2**31) - 10, 2**31 + 10)))
    if kind == "bigint":
        return ColumnValue("BIGINT", draw(st.integers(-(2**63), 2**63 - 1)))
    if kind == "numeric":
        precision = draw(st.integers(1, 38))
        scale = draw(st.integers(0, precision))
        places = draw(st.integers(0, min(scale + 2, 12)))
        digits = draw(st.integers(0, precision - scale + 1))
        bound = Decimal(10) ** digits - 1
        value = draw(st.decimals(min_value=-bound, max_value=bound, places=places, allow_nan=False))
        return ColumnValue("NUMERIC(%d,%d)" % (precision, scale), value)
    if kind == "float":
        return ColumnValue(
            "FLOAT", draw(st.floats(allow_nan=False, allow_infinity=False, width=32))
        )
    if kind == "double":
        return ColumnValue(
            "DOUBLE",
            draw(
                st.floats(
                    allow_nan=False, allow_infinity=False, min_value=-1e300, max_value=1e300
                ).filter(lambda x: x == 0.0 or abs(x) >= 2.3e-308)
            ),
        )
    if kind == "monetary":
        return ColumnValue(
            "MONETARY",
            draw(st.decimals(min_value=-(10**12), max_value=10**12, places=2, allow_nan=False)),
        )
    if kind in ("varchar", "char"):
        size = draw(st.integers(1, 20))
        text = draw(texts)
        tags = frozenset({"string-overflow"}) if len(text.encode("utf-8")) > size else frozenset()
        return ColumnValue("%s(%d)" % (kind.upper(), size), text, tags)
    if kind == "string":
        return ColumnValue("STRING", draw(texts))
    if kind == "date":
        return ColumnValue("DATE", draw(st.dates()))
    if kind == "time":
        return ColumnValue("TIME", draw(st.times().map(lambda t: t.replace(microsecond=0))))
    if kind == "datetime":
        return ColumnValue(
            "DATETIME",
            draw(st.datetimes().map(lambda d: d.replace(microsecond=d.microsecond // 1000 * 1000))),
        )
    if kind == "timestamp":
        return ColumnValue(
            "TIMESTAMP",
            draw(
                st.datetimes(
                    min_value=datetime.datetime(1969, 12, 30),
                    max_value=datetime.datetime(2038, 1, 20),
                ).map(lambda d: d.replace(microsecond=0))
            ),
        )
    if kind == "bitvar":
        size = draw(st.integers(1, 64))
        data = draw(st.binary(max_size=10))
        tags = frozenset({"string-overflow"}) if len(data) * 8 > size else frozenset()
        return ColumnValue("BIT VARYING(%d)" % size, data, tags)
    if kind == "enum":
        return ColumnValue("ENUM('a','b','c')", draw(st.sampled_from(["a", "b", "c", "d", ""])))
    doc = draw(json_docs)
    text = json.dumps(doc) if draw(st.booleans()) else draw(st.sampled_from(["{", "[1,", "nul"]))
    return ColumnValue("JSON", text)


# ---------------------------------------------------------------------------
# Expression grammar
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Expr:
    sql: str
    kind: str  # num | str | date | json | coll | bool
    tags: frozenset[str] = frozenset()


def _merge(*parts: Expr) -> frozenset[str]:
    return frozenset().union(*(p.tags for p in parts))


@st.composite
def num_leaf(draw: st.DrawFn) -> Expr:
    choice = draw(st.integers(0, 3))
    if choice == 0:
        value = draw(st.integers(-1000, 1000))
        return Expr("(%d)" % value, "num")
    if choice == 1:
        return Expr("(%d)" % draw(st.integers(-(2**62), 2**62)), "num")
    if choice == 2:
        dec = draw(st.decimals(min_value=-(10**6), max_value=10**6, places=3, allow_nan=False))
        return Expr("(%s)" % dec, "num", frozenset({"int-numeric-precision"}))
    x = draw(st.floats(allow_nan=False, allow_infinity=False, min_value=-1e12, max_value=1e12))
    return Expr("CAST(%s AS DOUBLE)" % format(x, ".17e"), "num")


@st.composite
def str_leaf(draw: st.DrawFn) -> Expr:
    return Expr(sql_str(draw(words)), "str")


@st.composite
def date_leaf(draw: st.DrawFn) -> Expr:
    choice = draw(st.integers(0, 4))
    if choice == 0:
        return Expr("DATE'%s'" % draw(st.dates()).isoformat(), "date")
    if choice == 1:
        dt = draw(st.datetimes())
        stamp = dt.replace(microsecond=dt.microsecond // 1000 * 1000).isoformat(" ", "milliseconds")
        return Expr("DATETIME'%s'" % stamp, "date")
    if choice == 2:
        return Expr("TIME'%s'" % draw(st.times()).strftime("%H:%M:%S"), "date")
    if choice == 3:
        offset = draw(st.sampled_from(["+00:00", "+09:00", "-05:30", "+14:00"]))
        dt = draw(
            st.datetimes(
                min_value=datetime.datetime(1971, 1, 1), max_value=datetime.datetime(2037, 1, 1)
            )
        )
        return Expr("DATETIMETZ'%s %s'" % (dt.isoformat(" ", "seconds"), offset), "date")
    # Zero dates are valid on the server and must raise DataError everywhere (#512).
    return Expr(
        draw(st.sampled_from(["DATE'0000-00-00'", "DATETIME'0000-00-00 00:00:00'"])), "date"
    )


@st.composite
def json_leaf_expr(draw: st.DrawFn) -> Expr:
    return Expr(sql_str(json.dumps(draw(json_docs))), "json")


def _has_wide_char(sql: str) -> bool:
    """True when the SQL text holds a character that is 3+ bytes in UTF-8."""
    return any(len(ch.encode("utf-8")) >= 3 for ch in sql)


def _unary_num(draw: st.DrawFn, a: Expr) -> Expr:
    fn = draw(
        st.sampled_from(
            ["ABS(%s)", "FLOOR(%s)", "CEIL(%s)", "SIGN(%s)", "ROUND(%s, 2)", "TRUNC(%s, 1)", "-%s"]
        )
    )
    return Expr(fn % a.sql, "num", a.tags)


@st.composite
def expressions(draw: st.DrawFn, depth: int = 2) -> Expr:
    if depth <= 0:
        return draw(st.one_of(num_leaf(), str_leaf(), date_leaf(), json_leaf_expr()))
    sub = expressions(depth=depth - 1)
    form = draw(st.integers(0, 13))
    if form == 0:  # arithmetic
        a, b = draw(num_expr(depth - 1)), draw(num_expr(depth - 1))
        op = draw(st.sampled_from(["+", "-", "*", "/"]))
        return Expr("(%s %s %s)" % (a.sql, op, b.sql), "num", _merge(a, b))
    if form == 1:
        a, b = draw(num_expr(depth - 1)), draw(num_expr(depth - 1))
        fn = draw(st.sampled_from(["MOD(%s, %s)", "GREATEST(%s, %s)", "LEAST(%s, %s)"]))
        return Expr(fn % (a.sql, b.sql), "num", _merge(a, b))
    if form == 2:
        return _unary_num(draw, draw(num_expr(depth - 1)))
    if form == 3:  # string functions
        s = draw(str_expr(depth - 1))
        fn = draw(
            st.sampled_from(
                [
                    "UPPER(%s)",
                    "LOWER(%s)",
                    "REVERSE(%s)",
                    "REPEAT(%s, 2)",
                    "LPAD(%s, 6, 'x')",
                    "SUBSTR(%s, 2, 3)",
                    "REPLACE(%s, 'a', 'Z')",
                    "LENGTH(%s)",
                    "CHAR_LENGTH(%s)",
                ]
            )
        )
        kind = "num" if "LENGTH" in fn else "str"
        return Expr(fn % s.sql, kind, s.tags)
    if form == 4:  # concatenation: documented CHAR -> VARCHAR metadata change
        a, b = draw(str_expr(depth - 1)), draw(str_expr(depth - 1))
        fn = draw(st.sampled_from(["(%s || %s)", "CONCAT(%s, %s)", "CONCAT_WS('-', %s, %s)"]))
        return Expr(fn % (a.sql, b.sql), "str", _merge(a, b) | {"char-concat"})
    if form == 5:  # casts
        a = draw(num_expr(depth - 1))
        target = draw(
            st.sampled_from(
                ["VARCHAR(64)", "INTEGER", "BIGINT", "NUMERIC(20,4)", "DOUBLE", "FLOAT"]
            )
        )
        tags = a.tags | {"int-numeric-precision"} if "NUMERIC" in target else a.tags
        return Expr("CAST(%s AS %s)" % (a.sql, target), "str" if "CHAR" in target else "num", tags)
    if form == 6:  # temporal functions
        d = draw(date_leaf())
        fn = draw(
            st.sampled_from(
                [
                    "ADD_MONTHS(%s, 1)",
                    "LAST_DAY(%s)",
                    "EXTRACT(YEAR FROM %s)",
                    "EXTRACT(DAY FROM %s)",
                    "DATEDIFF(%s, DATE'2000-01-01')",
                    "TO_CHAR(%s)",
                    "(%s + 1)",
                ]
            )
        )
        return Expr(fn % d.sql, "date", d.tags)
    if form == 7:  # JSON functions
        j = draw(json_leaf_expr())
        fn = draw(
            st.sampled_from(
                [
                    "JSON_TYPE(%s)",
                    "JSON_VALID(%s)",
                    "JSON_LENGTH(%s)",
                    "JSON_EXTRACT(%s, '$')",
                    "JSON_EXTRACT(%s, '$[0]')",
                    "JSON_EXTRACT(%s, '$.a')",
                    "JSON_KEYS(%s)",
                    "JSON_DEPTH(%s)",
                ]
            )
        )
        return Expr(fn % j.sql, "json", j.tags)
    if form == 8:  # JSON constructors
        items = draw(st.lists(st.one_of(num_leaf(), str_leaf()), min_size=0, max_size=3))
        if draw(st.booleans()):
            return Expr("JSON_ARRAY(%s)" % ", ".join(i.sql for i in items), "json", _merge(*items))
        pairs = ", ".join("'k%d', %s" % (i, e.sql) for i, e in enumerate(items))
        return Expr("JSON_OBJECT(%s)" % pairs, "json", _merge(*items))
    if form == 9:  # collections (decoded; decode_collections=True)
        elems = draw(st.lists(st.integers(-5, 5), max_size=4))
        ctor = draw(st.sampled_from(["SET", "MULTISET", "LIST"]))
        coll = "%s{%s}" % (ctor, ", ".join(map(str, elems)))
        if draw(st.booleans()):
            return Expr("CARDINALITY(%s)" % coll, "num")
        return Expr(coll, "coll")
    if form == 10:  # conditionals with logical conditions only
        cond = draw(logical_condition(depth - 1))
        a, b = draw(sub), draw(sub)
        fn = draw(st.sampled_from(["IF(%s, %s, %s)", "CASE WHEN %s THEN %s ELSE %s END"]))
        return Expr(fn % (cond.sql, a.sql, b.sql), a.kind, _merge(cond, a, b))
    if form == 11:
        a = draw(sub)
        fn = draw(st.sampled_from(["COALESCE(NULL, %s)", "NVL(NULL, %s)", "NULLIF(%s, NULL)"]))
        return Expr(fn % a.sql, a.kind, a.tags)
    if form == 12:  # regular expressions (functions added in 11.0)
        s = draw(str_expr(depth - 1))
        pattern = draw(st.sampled_from(["^a", "b$", "[0-9]+", "a.c", "(ab)*", "X|Y"]))
        wide = _has_wide_char(s.sql)
        if draw(st.booleans()):
            tags = s.tags | {"regexp-function"}
            if wide:
                tags |= {"regexp-function-multibyte"}
            return Expr("REGEXP_LIKE(%s, '%s')" % (s.sql, pattern), "num", tags)
        tags = s.tags | {"regexp-operator-multibyte"} if wide else s.tags
        return Expr("(%s REGEXP '%s')" % (s.sql, pattern), "num", tags)
    return draw(logical_condition(depth - 1))


@st.composite
def num_expr(draw: st.DrawFn, depth: int) -> Expr:
    if depth <= 0 or draw(st.booleans()):
        return draw(num_leaf())
    form = draw(st.integers(0, 1))
    a = draw(num_expr(depth - 1))
    if form == 0:
        return _unary_num(draw, a)
    b = draw(num_expr(depth - 1))
    return Expr(
        "(%s %s %s)" % (a.sql, draw(st.sampled_from(["+", "-", "*"])), b.sql), "num", _merge(a, b)
    )


@st.composite
def str_expr(draw: st.DrawFn, depth: int) -> Expr:
    if depth <= 0 or draw(st.booleans()):
        return draw(str_leaf())
    a = draw(str_expr(depth - 1))
    fn = draw(st.sampled_from(["UPPER(%s)", "LOWER(%s)", "REVERSE(%s)", "SUBSTR(%s, 1, 4)"]))
    return Expr(fn % a.sql, "str", a.tags)


@st.composite
def logical_condition(draw: st.DrawFn, depth: int) -> Expr:
    if draw(st.booleans()):
        a, b = draw(num_expr(depth)), draw(num_expr(depth))
    else:
        a, b = draw(str_expr(depth)), draw(str_expr(depth))
    op = draw(st.sampled_from(["=", "<>", "<", "<=", ">", ">="]))
    return Expr("(%s %s %s)" % (a.sql, op, b.sql), "bool", _merge(a, b))


# ---------------------------------------------------------------------------
# Workloads
# ---------------------------------------------------------------------------


def _value_table(servers: Sequence[Server], ddl_type: str) -> str:
    name = "vd_%s_%s" % (RUN_ID, uuid.uuid5(uuid.NAMESPACE_OID, ddl_type).hex[:10])
    for server in servers:
        server.ensure_table(name, "CREATE TABLE %s (v " + ddl_type + ")")
    return name


class TestVersionDifferential:
    @settings(LIVE, max_examples=WRITE_BUDGET)
    @given(case=column_values())
    def test_bound_value_roundtrip_agrees(self, servers: list[Server], case: ColumnValue) -> None:
        """Bound ``?`` values: insert, read back, update, echo -- all versions agree."""
        table = _value_table(servers, case.ddl)
        workload = Workload(
            setup=["DELETE FROM %s" % table],
            statements=[
                Stmt("INSERT INTO %s (v) VALUES (?)" % table, (case.value,), tags=case.tags),
                Stmt("SELECT v FROM %s" % table, tags=case.tags),
                Stmt("UPDATE %s SET v = ?" % table, (case.value,), tags=case.tags),
                # COUNT(*) is INTEGER on 10.2/11.0 and BIGINT on 11.2+.
                Stmt(
                    "SELECT v, COUNT(*) FROM %s GROUP BY v" % table,
                    tags=case.tags | {"count-bigint"},
                ),
                Stmt("SELECT ?", (case.value,)),
            ],
        )
        compare(servers, workload)

    @LIVE
    @given(expr=expressions())
    def test_scalar_expression_agrees(self, servers: list[Server], expr: Expr) -> None:
        """Generated numeric/string/temporal/JSON/collection/conditional expressions."""
        compare(servers, Workload([Stmt("SELECT %s" % expr.sql, tags=expr.tags)]))

    @LIVE
    @given(
        cond=st.one_of(
            logical_condition(depth=1),
            st.sampled_from(["1", "0", "(1 + 1)", "NULL", "'1'"]).map(
                lambda sql: Expr(sql, "num", frozenset({"non-logical-condition"}))
            ),
        ),
        form=st.sampled_from(
            [
                "SELECT IF(%s, 'y', 'n')",
                "SELECT COUNT(*) FROM db_root WHERE %s",
                "SELECT CASE WHEN %s THEN 1 ELSE 0 END",
            ]
        ),
    )
    def test_condition_forms_agree(self, servers: list[Server], cond: Expr, form: str) -> None:
        """Logical and bare-value conditions in IF / WHERE / CASE WHEN."""
        tags = cond.tags | {"count-bigint"} if "COUNT" in form else cond.tags
        compare(servers, Workload([Stmt(form % cond.sql, tags=tags)]))

    @settings(LIVE, max_examples=WRITE_BUDGET)
    @given(
        ops=st.lists(
            st.one_of(
                st.tuples(
                    st.just("insert_many"),
                    st.lists(
                        st.tuples(st.integers(0, 9), st.text(alphabet="abcxyz", max_size=8)),
                        min_size=1,
                        max_size=5,
                    ),
                ),
                st.tuples(
                    st.just("insert_one"), st.integers(0, 9), st.text(alphabet="abcxyz", max_size=8)
                ),
                st.tuples(
                    st.just("update"), st.text(alphabet="abcxyz", max_size=8), st.integers(0, 10)
                ),
                st.tuples(st.just("bump"), st.integers(-3, 3), st.integers(0, 9)),
                st.tuples(st.just("delete"), st.integers(0, 9)),
                st.tuples(st.just("upsert"), st.integers(1, 12), st.integers(0, 9)),
                st.tuples(st.just("not_null")),
                st.tuples(st.just("aggregate")),
                st.tuples(st.just("select"), st.integers(0, 9)),
            ),
            min_size=1,
            max_size=8,
        )
    )
    def test_dml_sequence_agrees(self, servers: list[Server], ops: list[tuple[Any, ...]]) -> None:
        """rowcount / lastrowid / integrity errors of generated DML sequences."""
        table = "vd_%s_dml" % RUN_ID
        for server in servers:
            server.ensure_table(
                table,
                "CREATE TABLE %s (id INT AUTO_INCREMENT PRIMARY KEY, k INT NOT NULL, s VARCHAR(16) UNIQUE)",
            )
        statements: list[Stmt] = []
        for op in ops:
            name = op[0]
            if name == "insert_many":
                rows = [tuple(row) for row in op[1]]
                statements.append(
                    Stmt("INSERT INTO %s (k, s) VALUES (?, ?)" % table, rows, many=True)
                )
            elif name == "insert_one":
                statements.append(
                    Stmt("INSERT INTO %s (k, s) VALUES (?, ?)" % table, (op[1], op[2]))
                )
            elif name == "update":
                statements.append(Stmt("UPDATE %s SET s = ? WHERE k < ?" % table, (op[1], op[2])))
            elif name == "bump":
                statements.append(
                    Stmt("UPDATE %s SET k = k + ? WHERE k = ?" % table, (op[1], op[2]))
                )
            elif name == "delete":
                statements.append(Stmt("DELETE FROM %s WHERE k = ?" % table, (op[1],)))
            elif name == "upsert":
                statements.append(
                    Stmt(
                        "INSERT INTO %s (id, k, s) VALUES (?, ?, NULL) ON DUPLICATE KEY UPDATE k = k + 1"
                        % table,
                        (op[1], op[2]),
                    )
                )
            elif name == "not_null":
                statements.append(Stmt("INSERT INTO %s (k, s) VALUES (NULL, 'n')" % table))
            elif name == "aggregate":
                statements.append(
                    Stmt(
                        "SELECT COUNT(*), SUM(k), MIN(s), MAX(id), AVG(k) FROM %s" % table,
                        tags=frozenset({"count-bigint"}),
                    )
                )
            else:
                statements.append(
                    Stmt("SELECT id, k, s FROM %s WHERE k >= ? ORDER BY id" % table, (op[1],))
                )
        statements.append(Stmt("SELECT id, k, s FROM %s ORDER BY id" % table))
        compare(servers, Workload(statements, setup=["TRUNCATE TABLE %s" % table]))

    @settings(LIVE, max_examples=WRITE_BUDGET)
    @given(
        errors=st.lists(
            st.one_of(
                st.text(alphabet="abcdefgh", min_size=1, max_size=6).map(
                    lambda n: ("SELECT * FROM no_such_%s" % n, frozenset())
                ),
                st.text(alphabet="abcdefgh", min_size=1, max_size=6).map(
                    lambda n: ("SELECT no_col_%s FROM {parent}" % n, frozenset())
                ),
                st.lists(
                    st.sampled_from(
                        [
                            "SELECT",
                            "FROM",
                            "(",
                            ")",
                            ",",
                            "1",
                            "'a'",
                            "*",
                            "ORDER",
                            "BY",
                            "INSERT",
                            "INTO",
                            "VALUES",
                        ]
                    ),
                    min_size=1,
                    max_size=6,
                ).map(lambda toks: (" ".join(toks), frozenset())),
                st.sampled_from(
                    [
                        ("SELECT 1/0", frozenset()),
                        ("SELECT 1.0/0", frozenset()),
                        ("SELECT CAST('x' AS INTEGER)", frozenset()),
                        ("SELECT CAST('2024-13-01' AS DATE)", frozenset()),
                        ("SELECT 2147483647 + 1", frozenset()),
                        ("SELECT 9223372036854775807 + 1", frozenset()),
                        ("SELECT TIMESTAMP'1960-01-01 00:00:00'", frozenset()),
                        ("SELECT DATE'0000-00-00'", frozenset()),
                        ("INSERT INTO {parent} VALUES (1)", frozenset()),
                        ("INSERT INTO {child} VALUES (9, 99)", frozenset()),
                        ("DELETE FROM {parent} WHERE id = 1", frozenset()),
                        ("UPDATE {parent} SET id = 7 WHERE id = 1", frozenset()),
                        ("TRUNCATE TABLE {parent}", frozenset({"truncate-referenced-parent"})),
                        ("INSERT INTO {parent} VALUES (NULL)", frozenset()),
                        ("INSERT INTO {parent} VALUES ('x')", frozenset()),
                        ("CALL no_such_procedure()", frozenset()),
                        ("DROP TABLE no_such_table_vd", frozenset()),
                    ]
                ),
            ),
            min_size=1,
            max_size=4,
        )
    )
    def test_error_contract_agrees(
        self, servers: list[Server], errors: list[tuple[str, frozenset[str]]]
    ) -> None:
        """Exception class, errno and SQLSTATE of generated failing statements."""
        parent, child = "vd_%s_parent" % RUN_ID, "vd_%s_child" % RUN_ID
        for server in servers:
            server.ensure_table(parent, "CREATE TABLE %s (id INT PRIMARY KEY)")
            server.ensure_table(
                child,
                "CREATE TABLE %%s (id INT, pid INT, FOREIGN KEY (pid) REFERENCES %s(id))" % parent,
            )
        statements = [
            Stmt(sql.format(parent=parent, child=child), tags=tags) for sql, tags in errors
        ]
        setup = [
            "DELETE FROM %s" % child,
            "DELETE FROM %s" % parent,
            "INSERT INTO %s VALUES (1), (2)" % parent,
            "INSERT INTO %s VALUES (1, 1)" % child,
        ]
        compare(servers, Workload(statements, setup))

    @settings(LIVE, max_examples=SCHEMA_BUDGET)
    @given(
        columns=st.lists(
            st.tuples(
                st.one_of(
                    st.sampled_from(
                        [
                            "SHORT",
                            "INTEGER",
                            "BIGINT",
                            "FLOAT",
                            "DOUBLE",
                            "MONETARY",
                            "STRING",
                            "DATE",
                            "TIME",
                            "DATETIME",
                            "TIMESTAMP",
                            "DATETIMETZ",
                            "DATETIMELTZ",
                            "TIMESTAMPTZ",
                            "TIMESTAMPLTZ",
                            "JSON",
                            "ENUM('a','bb')",
                            "SET(INTEGER)",
                            "MULTISET(VARCHAR(10))",
                            "LIST(DOUBLE)",
                            "BLOB",
                            "CLOB",
                        ]
                    ),
                    st.tuples(st.integers(1, 38), st.integers(0, 38))
                    .filter(lambda ps: ps[1] <= ps[0])
                    .map(lambda ps: "NUMERIC(%d,%d)" % ps),
                    st.tuples(
                        st.sampled_from(
                            ["CHAR", "VARCHAR", "NCHAR", "NCHAR VARYING", "BIT", "BIT VARYING"]
                        ),
                        st.integers(1, 300),
                    ).map(lambda tn: "%s(%d)" % tn),
                ),
                st.booleans(),
            ),
            min_size=1,
            max_size=6,
        )
    )
    def test_schema_metadata_agrees(
        self, servers: list[Server], columns: list[tuple[str, bool]]
    ) -> None:
        """``description`` (type code, precision, scale, null_ok) of generated schemas."""
        table = "vd_%s_%s" % (RUN_ID, uuid.uuid4().hex[:8])
        cols = ", ".join(
            "c%d %s%s" % (i, ddl, " NOT NULL" if not_null else "")
            for i, (ddl, not_null) in enumerate(columns)
        )
        names = ", ".join("c%d" % i for i in range(len(columns)))
        workload = Workload(
            [
                Stmt("CREATE TABLE %s (%s)" % (table, cols)),
                Stmt("SELECT * FROM %s" % table),
                Stmt("SELECT %s FROM %s WHERE 1 = 0" % (names, table)),
                Stmt("DROP TABLE %s" % table),
            ]
        )
        compare(servers, workload)


# ---------------------------------------------------------------------------
# Every documented difference must still reproduce (no stale allowlist entry)
# ---------------------------------------------------------------------------


PROBES: dict[str, tuple[list[str], list[Stmt]]] = {
    "string-overflow": (
        ["CREATE TABLE {t} (v VARCHAR(3))"],
        [Stmt("INSERT INTO {t} (v) VALUES (?)", ("abcdef",)), Stmt("SELECT v FROM {t}")],
    ),
    "regexp-function": ([], [Stmt("SELECT REGEXP_LIKE('abc', '^a')")]),
    "non-logical-condition": (
        [],
        [Stmt("SELECT IF(1, 'y', 'n')"), Stmt("SELECT COUNT(*) FROM db_root WHERE 1")],
    ),
    "char-concat": ([], [Stmt("SELECT 'a' || 'b'"), Stmt("SELECT CONCAT(N'a', N'b')")]),
    "varchar-trailing-space": (
        [],
        [Stmt("SELECT CAST('a' AS VARCHAR(5)) = CAST('a ' AS VARCHAR(5))")],
    ),
    "count-bigint": ([], [Stmt("SELECT COUNT(*) FROM db_root")]),
    "int-numeric-precision": (
        [],
        [
            Stmt("SELECT (5) * (0.100)"),
            Stmt("SELECT 7 / 2.0"),
            Stmt("SELECT 10000000000000000 + 0.000"),
        ],
    ),
    "regexp-operator-multibyte": ([], [Stmt("SELECT ('a\ud55c' REGEXP '^a')")]),
    "regexp-function-multibyte": ([], [Stmt("SELECT REGEXP_LIKE('x\ud55c', '^x')")]),
    "truncate-referenced-parent": (
        [
            "CREATE TABLE {t} (id INT PRIMARY KEY)",
            "CREATE TABLE {t}_c (id INT, pid INT, FOREIGN KEY (pid) REFERENCES {t}(id))",
            "INSERT INTO {t} VALUES (1)",
            "INSERT INTO {t}_c VALUES (1, 1)",
        ],
        [Stmt("TRUNCATE TABLE {t}")],
    ),
}

# Entries whose probe also hits another documented difference on other
# versions (REGEXP_* is undefined on 10.2, so a multibyte REGEXP_LIKE probe
# splits 10.2 | 11.0 | 11.2+).
PROBE_COMPANIONS: dict[str, frozenset[str]] = {
    "regexp-function-multibyte": frozenset({"regexp-function"}),
}


@pytest.mark.parametrize("entry", ALLOWLIST, ids=[entry.tag for entry in ALLOWLIST])
def test_documented_difference_still_reproduces(
    servers: list[Server], entry: VersionDifference
) -> None:
    setup, statements = PROBES[entry.tag]
    table = "vd_%s_probe_%s" % (RUN_ID, entry.tag.replace("-", "_"))
    for server in servers:
        server.raw("DROP TABLE IF EXISTS %s_c" % table)
        server.raw("DROP TABLE IF EXISTS %s" % table)
    try:
        tags = PROBE_COMPANIONS.get(entry.tag, frozenset()) | {entry.tag}
        workload = Workload(
            [Stmt(s.sql.format(t=table), s.params, s.many, tags) for s in statements],
            [sql.format(t=table) for sql in setup],
        )
        observations = {s.version: s.run(workload) for s in servers}
        entries = [e for e in ALLOWLIST if e.tag in tags]
        used, problems = classify(workload.tags, observations, allowlist=entries)
        assert not problems, format_report(
            [s.sql for s in workload.statements], workload.tags, observations, problems
        )
        present = frozenset(observations)
        affected = entry.versions & present
        if affected and affected != present:
            assert entry in used, (
                f"{entry.tag}: expected {sorted(affected)} to differ from the other versions, "
                f"but all configured versions agree -- the allowlist entry is stale "
                f"({entry.link})"
            )
    finally:
        for server in servers:
            server.raw("DROP TABLE IF EXISTS %s_c" % table)
            server.raw("DROP TABLE IF EXISTS %s" % table)
