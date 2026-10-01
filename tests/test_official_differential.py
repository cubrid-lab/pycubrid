"""Claims-driven differential against the pinned official driver (#446, from #344).

Every claim in ``tests/fixtures/official_differential_claims.json`` runs the same
SQL, values and transaction settings through pycubrid and the official
``CUBRIDdb``/``_cubrid`` driver on one live server. Observations are rendered as
type-tagged strings; a ``match`` claim requires identical observations and a
``deviation`` claim requires both sides to equal its pinned, reasoned
expectations, so any change on either side fails and is re-triaged.

The oracle is built by ``scripts/build_official_oracle.py``. Cases run only with
``PYCUBRID_OFFICIAL_ORACLE_REQUIRED=1`` (the ``official-differential`` lane or a
local reproduction); everywhere else they are collected but skipped, even when
some other ``CUBRIDdb`` build is importable. In required mode a missing driver or manifest, or an
extension whose SHA-256 differs from the manifest, then fails instead of
skipping. ``PYCUBRID_DIFFERENTIAL_EVIDENCE`` names a JSON Lines file receiving
one environment record and one record per case, which
``scripts/check_official_differential.py --evidence`` validates.
"""

from __future__ import annotations

import hashlib
import json
import os
import platform
import subprocess  # nosec B404 - fixed git argv for the evidence record
import uuid
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import pytest

import pycubrid
from pycubrid.compat import native

from ._cubrid_endpoint import TEST_DB, TEST_HOST, TEST_PASSWORD, TEST_PORT, TEST_USER

CLAIMS_PATH = Path(__file__).parent / "fixtures" / "official_differential_claims.json"
REQUIRED = os.environ.get("PYCUBRID_OFFICIAL_ORACLE_REQUIRED") == "1"
SKIP_REASON = "official CUBRIDdb oracle not installed (official-differential lane)"

try:
    import _cubrid
    import CUBRIDdb
except ImportError as exc:  # collected everywhere; skipped or failed by ``environment``
    _cubrid = CUBRIDdb = None
    IMPORT_ERROR: ImportError | None = exc
else:
    IMPORT_ERROR = None

pytestmark = [pytest.mark.integration, pytest.mark.official_differential, pytest.mark.no_escape_pin]

CLAIMS: list[dict[str, Any]] = json.loads(CLAIMS_PATH.read_text(encoding="utf-8"))["claims"]
URL = f"CUBRID:{TEST_HOST}:{TEST_PORT}:{TEST_DB}:::"


def render(value: object) -> str:
    """Render a value with its Python type so int(1) never equals bool(True)."""
    if isinstance(value, tuple):
        inner = ", ".join(render(item) for item in value)
        return f"({inner},)" if len(value) == 1 else f"({inner})"
    if isinstance(value, list):
        return "[" + ", ".join(render(item) for item in value) + "]"
    return f"{type(value).__name__}({value!r})"


def _ordinary() -> pycubrid.Connection:
    conn = pycubrid.connect(
        host=TEST_HOST, port=TEST_PORT, database=TEST_DB, user=TEST_USER, password=TEST_PASSWORD
    )
    conn.autocommit = True
    return conn


def _table(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


# -- wrapper surface: ordinary pycubrid DB-API versus CUBRIDdb ---------------------------


def _fetch_stored(ddl: str, literal: str) -> tuple[str, str]:
    table = _table("od")
    py = _ordinary()
    cdb = CUBRIDdb.connect(URL, TEST_USER, TEST_PASSWORD)
    pc = py.cursor()
    try:
        pc.execute(f"CREATE TABLE {table} (v {ddl})")
        pc.execute(f"INSERT INTO {table} VALUES ({literal})")
        pc.execute(f"SELECT v FROM {table}")
        py_row = pc.fetchone()
        cc = cdb.cursor()
        cc.execute(f"SELECT v FROM {table}")
        cdb_row = cc.fetchone()
        cc.close()
        return render(py_row[0]), render(cdb_row[0])
    finally:
        pc.execute(f"DROP TABLE IF EXISTS {table}")
        pc.close()
        py.close()
        cdb.close()


def _stored(ddl: str, literal: str) -> Callable[[], tuple[str, str]]:
    return lambda: _fetch_stored(ddl, literal)


SCALAR_SELECT = (
    "SELECT CAST(1 AS INTEGER) AS i, CAST(2 AS BIGINT) AS b, CAST(1.5 AS NUMERIC(10,2)) AS n,"
    " CAST(2.5 AS DOUBLE) AS d, CAST('ab' AS VARCHAR(20)) AS v, CAST('ab' AS CHAR(5)) AS c,"
    " DATE'2024-01-15' AS dt, CAST(NULL AS INTEGER) AS z"
)


def _scalar_select() -> tuple[tuple[Any, ...], tuple[Any, ...], tuple[Any, ...], tuple[Any, ...]]:
    py = _ordinary()
    cdb = CUBRIDdb.connect(URL, TEST_USER, TEST_PASSWORD)
    try:
        pc = py.cursor()
        pc.execute(SCALAR_SELECT)
        py_row, py_desc = pc.fetchone(), pc.description
        pc.close()
        cc = cdb.cursor()
        cc.execute(SCALAR_SELECT)
        cdb_row, cdb_desc = cc.fetchone(), cc.description
        cc.close()
        return py_row, cdb_row, tuple(py_desc), tuple(cdb_desc)
    finally:
        py.close()
        cdb.close()


def _select_row() -> tuple[str, str]:
    py_row, cdb_row, _, _ = _scalar_select()
    return render(py_row), render(cdb_row)


def _description_core() -> tuple[str, str]:
    _, _, py_desc, cdb_desc = _scalar_select()

    def core(desc: tuple[Any, ...]) -> list[tuple[Any, ...]]:
        return [(col[0], col[1], col[4], col[5]) for col in desc]

    return render(core(py_desc)), render(core(cdb_desc))


def _description_size_null_ok() -> tuple[str, str]:
    _, _, py_desc, cdb_desc = _scalar_select()

    def distinct(desc: tuple[Any, ...]) -> list[str]:
        return sorted({render((col[2], col[3], col[6])) for col in desc})

    return "[" + ", ".join(distinct(py_desc)) + "]", "[" + ", ".join(distinct(cdb_desc)) + "]"


# -- native surface: pycubrid.compat.native versus _cubrid -------------------------------


def _prepared_select(connect: Callable[..., Any], sql: str, values: tuple[object, ...]) -> str:
    conn = connect(URL, TEST_USER, TEST_PASSWORD)
    try:
        cur = conn.cursor()
        cur.prepare(sql)
        results = []
        for value in values:
            cur.bind_param(1, value)
            results.append((cur.execute(), cur.fetch_row(), cur.fetch_row()))
        cur.close()
        return render(results)
    finally:
        conn.close()


def _prepared(sql: str, values: tuple[object, ...]) -> Callable[[], tuple[str, str]]:
    return lambda: (
        _prepared_select(native.connect, sql, values),
        _prepared_select(_cubrid.connect, sql, values),
    )


def _prepared_insert(connect: Callable[..., Any]) -> str:
    table = _table("odi")
    verify = _ordinary()
    vc = verify.cursor()
    vc.execute(f"CREATE TABLE {table} (n INTEGER, s VARCHAR(20))")
    try:
        conn = connect(URL, TEST_USER, TEST_PASSWORD)
        try:
            cur = conn.cursor()
            cur.prepare(f"INSERT INTO {table} VALUES (?, ?)")
            counts = []
            for n, s in ((1, "one"), (2, "두")):
                cur.bind_param(1, n)
                cur.bind_param(2, s)
                counts.append(cur.execute())
            cur.close()
        finally:
            conn.close()
        vc.execute(f"SELECT n, s FROM {table} ORDER BY n")
        return render((counts, [tuple(row) for row in vc.fetchall()]))
    finally:
        vc.execute(f"DROP TABLE IF EXISTS {table}")
        vc.close()
        verify.close()


def _prepared_null() -> tuple[str, str]:
    def run(connect: Callable[..., Any]) -> str:
        conn = connect(URL, TEST_USER, TEST_PASSWORD)
        try:
            cur = conn.cursor()
            cur.prepare("SELECT CAST(? AS INTEGER)")
            try:
                cur.bind_param(1, None)
                return render((cur.execute(), cur.fetch_row()))
            except Exception as exc:  # the official extension raises SystemError
                return f"raises {type(exc).__name__}"
            finally:
                cur.close()
        finally:
            conn.close()

    return run(native.connect), run(_cubrid.connect)


CASES: dict[str, Callable[[], tuple[str, str]]] = {
    "fetch-integer": _stored("INTEGER", "42"),
    "fetch-bigint": _stored("BIGINT", "9223372036854775807"),
    "fetch-numeric": _stored("NUMERIC(10,2)", "12.34"),
    "fetch-double": _stored("DOUBLE", "3.14159"),
    "fetch-char": _stored("CHAR(5)", "'ab'"),
    "fetch-varchar": _stored("VARCHAR(20)", "'hello'"),
    "fetch-date": _stored("DATE", "DATE'2024-01-15'"),
    "fetch-time": _stored("TIME", "TIME'13:30:45'"),
    "fetch-datetime": _stored("DATETIME", "DATETIME'2024-01-15 13:30:45'"),
    "fetch-json": _stored("JSON", "'{\"a\": 1}'"),
    "fetch-monetary": _stored("MONETARY", "99.99"),
    "select-scalar-row": _select_row,
    "description-core-fields": _description_core,
    "description-size-and-null-ok": _description_size_null_ok,
    "prepared-int-reexecute": _prepared("SELECT CAST(? AS INTEGER)", (-(2**31), 0, 2**31 - 1, 0)),
    "prepared-string-reexecute": _prepared(
        "SELECT CAST(? AS VARCHAR(40))", ("a'\\한", "", "plain")
    ),
    "prepared-insert-two-binds": lambda: (
        _prepared_insert(native.connect),
        _prepared_insert(_cubrid.connect),
    ),
    "prepared-bind-null": _prepared_null,
}


def _write(record: dict[str, Any]) -> None:
    target = os.environ.get("PYCUBRID_DIFFERENTIAL_EVIDENCE")
    if target:
        with open(target, "a", encoding="utf-8") as handle:
            handle.write(json.dumps(record, sort_keys=True) + "\n")


def _pycubrid_commit() -> str:
    try:
        return subprocess.run(  # nosec B603 B607 - fixed git argv
            ["git", "rev-parse", "HEAD"],
            cwd=Path(__file__).parent,
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        return os.environ.get("GITHUB_SHA", "unknown")


@pytest.fixture(scope="module", autouse=True)
def environment() -> Iterator[dict[str, Any]]:
    """Record (and in the required lane verify) the oracle and server identities."""
    if not REQUIRED:
        # Only the pinned lane certifies claims; an arbitrary installed driver must not.
        pytest.skip(SKIP_REASON)
    if IMPORT_ERROR is not None:
        pytest.fail(f"official oracle required but not importable: {IMPORT_ERROR}")
    extension = Path(_cubrid.__file__)
    sha256 = hashlib.sha256(extension.read_bytes()).hexdigest()
    manifest: dict[str, Any] = {}
    manifest_path = os.environ.get("PYCUBRID_OFFICIAL_ORACLE_MANIFEST")
    if manifest_path:
        manifest = json.loads(Path(manifest_path).read_text(encoding="utf-8"))
    if REQUIRED:
        assert manifest, "PYCUBRID_OFFICIAL_ORACLE_MANIFEST is required in the official lane"
        assert manifest.get("extension_sha256") == sha256, (
            f"loaded {extension} does not match the built oracle manifest"
        )
    py = _ordinary()
    cdb = CUBRIDdb.connect(URL, TEST_USER, TEST_PASSWORD)
    try:
        record = {
            "record": "environment",
            "python": platform.python_version(),
            "pycubrid_commit": _pycubrid_commit(),
            "server_version": py.get_server_version(),
            "native_server_version": cdb.server_version(),
            "cubrid_python_commit": manifest.get("cubrid_python_commit"),
            "cci_commit": manifest.get("cci_commit"),
            "driver_version": manifest.get("driver_version"),
            "extension_sha256": sha256,
            "autocommit": {"wrapper_ordinary": True, "native": "driver default (True)"},
        }
    finally:
        py.close()
        cdb.close()
    _write(record)
    yield record


@pytest.mark.parametrize("claim", CLAIMS, ids=[claim["id"] for claim in CLAIMS])
def test_official_claim(claim: dict[str, Any], environment: dict[str, Any]) -> None:
    case = CASES.get(claim["id"])
    assert case is not None, f"claim {claim['id']} has no differential case"
    pycubrid_obs, native_obs = case()
    if claim["classification"] == "match":
        expected = {"pycubrid": native_obs, "native": native_obs}
    else:
        expected = claim["expected"]
    agrees = pycubrid_obs == expected["pycubrid"] and native_obs == expected["native"]
    if agrees:
        outcome = "match" if claim["classification"] == "match" else "classified-deviation"
    else:
        outcome = "mismatch"
    _write(
        {
            "record": "case",
            "claim": claim["id"],
            "classification": claim["classification"],
            "server_version": environment["server_version"],
            "pycubrid": pycubrid_obs,
            "native": native_obs,
            "outcome": outcome,
        }
    )
    assert agrees, (
        f"{claim['id']} ({claim['classification']}): pycubrid={pycubrid_obs} native={native_obs}"
        f" expected={expected}"
    )
