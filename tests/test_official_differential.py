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
import tempfile
import tracemalloc
import uuid
from collections.abc import Callable, Iterator
from contextlib import ExitStack, closing
from pathlib import Path
from typing import Any

import pytest

import pycubrid
from pycubrid.compat import cubriddb, native
from pycubrid.lob import Lob
from pycubrid.protocol import ExecutePacket, FetchPacket, PreparePacket

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
    if isinstance(value, (set, frozenset)):
        # Sorted, so a set's iteration order never decides agreement.
        inner = ", ".join(sorted(render(item) for item in value))
        return f"{type(value).__name__}({{{inner}}})"
    return f"{type(value).__name__}({value!r})"


def _ordinary() -> pycubrid.Connection:
    conn = pycubrid.connect(
        host=TEST_HOST, port=TEST_PORT, database=TEST_DB, user=TEST_USER, password=TEST_PASSWORD
    )
    conn.autocommit = True
    return conn


def _table(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


def _native_wrapper_utilities() -> tuple[str, str]:
    """Compare healthy utilities; retain distinct client identities separately."""
    manifest = json.loads(
        Path(os.environ["PYCUBRID_OFFICIAL_ORACLE_MANIFEST"]).read_text(encoding="utf-8")
    )
    identities: dict[str, Any] = {}

    def observe(
        native_connect: Callable[..., Any],
        wrapper_connect: Callable[..., Any],
        label: str,
        expected_client: str,
    ) -> str:
        observations = []
        for surface, connect in (("native", native_connect), ("wrapper", wrapper_connect)):
            with closing(connect(URL, TEST_USER, TEST_PASSWORD)) as conn:
                if surface == "native":
                    before = conn.client_version()
                for manual in (False, True):
                    if manual:
                        conn.set_autocommit(False)
                    version = conn.server_version()
                    indicator = conn.ping()
                    assert type(version) is str and version
                    assert type(indicator) is int and indicator == 1
                    observations.append((surface, not manual, version, indicator))
            if surface == "native":
                after = conn.client_version()
                identities[label] = {
                    "before_close": before,
                    "after_close": after,
                    "expected_own_identity": expected_client,
                    "is_str": type(before) is str and type(after) is str,
                    "stable_after_close": before == after,
                    "matches_own_identity": before == expected_client,
                }
        return render(tuple(observations))

    candidate = observe(native.connect, cubriddb.Connect, "pycubrid", pycubrid.__version__)
    official = observe(_cubrid.connect, CUBRIDdb.connect, "native", manifest["driver_version"])
    _write(
        {
            "record": "utility-client-identities",
            "claim": "native-wrapper-utilities",
            "pycubrid_commit": _pycubrid_commit(),
            "identities": identities,
            "scope": "own build identity/stability, not matching IDs or a four-component regex",
        }
    )
    for identity in identities.values():
        assert identity["is_str"]
        assert identity["stable_after_close"]
        assert identity["matches_own_identity"]
    return candidate, official


def _wrapper_transaction_boundaries() -> tuple[str, str]:
    """Compare explicit boundaries with independent and same-actor visibility."""
    with closing(_ordinary()) as observer, closing(observer.cursor()) as setup:
        created = False
        try:
            setup.execute(
                "SELECT class_name FROM db_class WHERE class_name=?", ("odtx662_fixture",)
            )
            existing = setup.fetchall()
            assert not existing, "refuse preexisting wrapper-transaction fixture"
            setup.execute("CREATE TABLE odtx662_fixture (id INTEGER PRIMARY KEY)")
            created = True

            def observer_rows() -> list[Any]:
                setup.execute("SELECT id FROM odtx662_fixture ORDER BY id")
                return setup.fetchall()

            def observe(connect: Callable[..., Any]) -> str:
                with ExitStack() as resources:
                    conn = resources.enter_context(closing(connect(URL, TEST_USER, TEST_PASSWORD)))
                    conn.set_autocommit(False)
                    # Fresh DML cursors avoid the unrelated official reprepare/description bug.
                    first = resources.enter_context(closing(conn.cursor()))
                    first.execute("INSERT INTO odtx662_fixture (id) VALUES (?)", (1,))
                    before_commit = observer_rows()
                    commit_result = conn.commit()
                    after_commit = observer_rows()
                    second = resources.enter_context(closing(conn.cursor()))
                    second.execute("INSERT INTO odtx662_fixture (id) VALUES (?)", (2,))
                    pending = resources.enter_context(closing(conn.cursor()))
                    pending.execute("SELECT id FROM odtx662_fixture ORDER BY id")
                    actor_before_rollback = pending.fetchall()
                    before_rollback = observer_rows()
                    rollback_result = conn.rollback()
                    # New execution, not a read from the rollback-invalidated result.
                    after = resources.enter_context(closing(conn.cursor()))
                    after.execute("SELECT id FROM odtx662_fixture ORDER BY id")
                    actor_after_rollback = after.fetchall()
                    after_rollback = observer_rows()
                final_rows = observer_rows()  # All actor cursors/connection are now closed.
                assert commit_result is None
                assert rollback_result is None
                assert before_commit == []
                assert after_commit == [(1,)]
                assert actor_before_rollback == [(1,), (2,)]
                assert before_rollback == [(1,)]
                assert actor_after_rollback == [(1,)]
                assert after_rollback == final_rows == [(1,)]
                return render(
                    (
                        commit_result,
                        rollback_result,
                        before_commit,
                        after_commit,
                        actor_before_rollback,
                        before_rollback,
                        actor_after_rollback,
                        after_rollback,
                        final_rows,
                    )
                )

            candidate = observe(cubriddb.connect)
            setup.execute("DELETE FROM odtx662_fixture")
            reset_rows = observer_rows()
            assert reset_rows == []
            return candidate, observe(CUBRIDdb.connect)
        finally:
            if created:
                setup.execute("DROP TABLE odtx662_fixture")


def _wrapper_row_conversion() -> tuple[str, str]:
    table = _table("owr")
    observer = _ordinary()
    setup = observer.cursor()
    try:
        setup.execute(
            f"CREATE TABLE {table} (id INTEGER NOT NULL, txt VARCHAR(40), opt VARCHAR(40))"
        )
        setup.execute(f"INSERT INTO {table} VALUES (1, '한', NULL)")
        setup.execute(f"INSERT INTO {table} VALUES (2, '', '')")
        query = f'SELECT id AS "MiXeD", txt AS dup, opt AS dup FROM {table} ORDER BY id'

        def observe(connect: Callable[..., Any]) -> str:
            conn = connect(URL, TEST_USER, TEST_PASSWORD)
            try:
                tuples = conn.cursor()
                dicts = conn.cursor(dictCursor=True)
                converted = conn.cursor(dictCursor=True)
                try:
                    tuples.execute(query)
                    desc = tuples.description
                    tuple_rows = tuples.fetchall()
                    dicts.execute(query)
                    dict_rows = dicts.fetchall()
                    conn.set_fetch_value_converter(lambda row, metadata: (row, metadata))
                    converted.execute(query)
                    converted_row = converted.fetchone()
                    return render((desc, tuple_rows, dict_rows, converted_row))
                finally:
                    tuples.close()
                    dicts.close()
                    converted.close()
            finally:
                conn.close()

        return observe(cubriddb.connect), observe(CUBRIDdb.connect)
    finally:
        setup.execute(f"DROP TABLE IF EXISTS {table}")
        setup.close()
        observer.close()


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


_NOT_IMPORTED = object()


def _set_bind(
    connect: Callable[..., Any],
    ddl: str,
    data: Any,
    element_type: int,
    kind: int | None = None,
) -> str:
    """Bind one ``imports()`` value with ``bind_set`` and read it back via pycubrid."""
    table = _table("ods")
    verify = pycubrid.connect(
        host=TEST_HOST,
        port=TEST_PORT,
        database=TEST_DB,
        user=TEST_USER,
        password=TEST_PASSWORD,
        autocommit=True,
        decode_collections=True,
    )
    vc = verify.cursor()
    vc.execute(f"CREATE TABLE {table} (c {ddl})")
    try:
        conn = connect(URL, TEST_USER, TEST_PASSWORD)
        try:
            cur = conn.cursor()
            try:
                cur.prepare(f"INSERT INTO {table} VALUES (?)")
                s = conn.set()
                if data is not _NOT_IMPORTED:
                    if kind is None:
                        s.imports(data, element_type)
                    else:
                        s.imports(data, element_type, kind=kind)
                cur.bind_set(1, s)
                count = cur.execute()
            except Exception as exc:  # the official extension raises InterfaceError
                return f"raises {type(exc).__name__}"
            finally:
                cur.close()
        finally:
            conn.close()
        vc.execute(f"SELECT c FROM {table}")
        return render((count, [tuple(row) for row in vc.fetchall()]))
    finally:
        vc.execute(f"DROP TABLE IF EXISTS {table}")
        vc.close()
        verify.close()


def _set_case(
    ddl: str, data: Any, element_type: int, kind: int | None = None
) -> Callable[[], tuple[str, str]]:
    """``kind`` goes to pycubrid only; the official ``imports`` has no such option."""
    return lambda: (
        _set_bind(native.connect, ddl, data, element_type, kind),
        _set_bind(_cubrid.connect, ddl, data, element_type),
    )


def _set_wrong_objects() -> tuple[str, str]:
    def run(connect: Callable[..., Any]) -> str:
        conn = connect(URL, TEST_USER, TEST_PASSWORD)
        outcomes = []
        try:
            cur = conn.cursor()
            cur.prepare("SELECT ?")
            for call in (
                lambda: cur.bind_set(1, ("1",)),
                lambda: conn.set().imports(["1"], FIELD_INT),
            ):
                try:
                    call()
                    outcomes.append("returns")
                except Exception as exc:  # compared by class name only
                    outcomes.append(f"raises {type(exc).__name__}")
            cur.close()
            return render(outcomes)
        finally:
            conn.close()

    return run(native.connect), run(_cubrid.connect)


def _set_error_classes() -> tuple[str, str]:
    """Client-side and server-side failure classes of the collection calls."""

    def run(module: Any) -> str:
        table = _table("ode")
        verify = _ordinary()
        vc = verify.cursor()
        vc.execute(f"CREATE TABLE {table} (c SET(INTEGER))")
        try:
            conn = module.connect(URL, TEST_USER, TEST_PASSWORD)
            try:
                cur = conn.cursor()
                cur.prepare(f"INSERT INTO {table} VALUES (?)")
                good = conn.set()
                good.imports(("1",), FIELD_INT)
                bad = conn.set()
                bad.imports(("x",), FIELD_INT)

                def server_reject() -> None:
                    cur.bind_set(1, bad)
                    cur.execute()

                outcomes = []
                for call in (
                    lambda: conn.set().imports((1.5,), FIELD_INT),
                    lambda: conn.set().imports((b"1",), FIELD_INT),
                    lambda: cur.bind_set(0, good),
                    lambda: cur.bind_set(5, good),
                    lambda: module.set(object()),
                    server_reject,
                ):
                    try:
                        call()
                        outcomes.append("returns")
                    except Exception as exc:  # compared by class name only
                        outcomes.append(f"raises {type(exc).__name__}")
                cur.close()
                return render(outcomes)
            finally:
                conn.close()
        finally:
            vc.execute(f"DROP TABLE IF EXISTS {table}")
            vc.close()
            verify.close()

    return run(native), run(_cubrid)


# -- native LOB handles: lob() / fetch_lob() / bind_lob() (#441) ----------------------

LOB_BLOB = bytes(range(256)) * 400  # 102400 bytes, above the ~80 KB LOB_READ cap
LOB_CLOB = "한글 CLOB ✓ " * 7000  # about 105 KB of UTF-8 text


def _lob_stored(observer: pycubrid.Connection, table: str, column: str) -> list[Any]:
    """Each stored LOB as (length, content or its SHA-256) read via an ordinary ``Lob``."""
    vc = observer.cursor()
    try:
        vc.execute(f"SELECT {column} FROM {table} ORDER BY id")
        cells = [row[0] for row in vc.fetchall()]
    finally:
        vc.close()
    stored: list[Any] = []
    for cell in cells:
        if cell is None:
            stored.append(None)
            continue
        lob = Lob(observer, cell["lob_type"], cell["packed_lob_handle"])
        content = lob.read(cell["lob_length"] + 16)
        if len(content) > 64:  # keep large values out of the evidence records
            content_text: object = "sha256:" + hashlib.sha256(content).hexdigest()
        else:
            content_text = content
        stored.append((cell["lob_length"], content_text))
    return stored


def _lob_run(
    module: Any,
    rows: tuple[tuple[Any, ...], ...],
    body: Callable[[Any, Any, str, str], object],
    dst_column: str,
) -> str:
    """Create a source table with ``rows`` and an empty copy target, run ``body``.

    ``body(module, conn, src, dst)`` returns its observations; the rendered
    result also carries the ``dst_column`` values stored in the target.
    """
    src, dst = _table("odl"), _table("odl")
    verify = _ordinary()
    vc = verify.cursor()
    try:
        for name in (src, dst):
            vc.execute(f"CREATE TABLE {name} (id INT, b BLOB, c CLOB)")
        for row in rows:
            vc.execute(f"INSERT INTO {src} VALUES (?, ?, ?)", row)
        conn = module.connect(URL, TEST_USER, TEST_PASSWORD)
        try:
            observed = body(module, conn, src, dst)
        finally:
            conn.close()
        return render((observed, _lob_stored(verify, dst, dst_column)))
    finally:
        for name in (src, dst):
            vc.execute(f"DROP TABLE IF EXISTS {name}")
        vc.close()
        verify.close()


def _outcome(call: Callable[[], object]) -> object:
    try:
        return call()
    except Exception as exc:  # compared by class name only
        return f"raises {type(exc).__name__}"


def _lob_copy(select: str, col: int, dst_column: str) -> Callable[[], tuple[str, str]]:
    """Fetch the handle at ``col`` of ``SELECT {select}`` and bind it into ``dst_column``."""

    def body(_module: Any, conn: Any, src: str, dst: str) -> object:
        cur = conn.cursor()
        cur.prepare(f"SELECT {select} FROM {src}")
        cur.execute()
        lob = conn.lob()
        fetched = cur.fetch_lob(col, lob)
        ins = conn.cursor()
        ins.prepare(f"INSERT INTO {dst} (id, {dst_column}) VALUES (1, ?)")
        ins.bind_lob(1, lob)
        count = ins.execute()
        lob.close()
        cur.close()
        ins.close()
        return (fetched, count)

    rows = ((1, LOB_BLOB, LOB_CLOB),)
    return lambda: (
        _lob_run(native, rows, body, dst_column),
        _lob_run(_cubrid, rows, body, dst_column),
    )


def _lob_fetch_end() -> tuple[str, str]:
    def body(_module: Any, conn: Any, src: str, dst: str) -> object:
        cur = conn.cursor()
        cur.prepare(f"SELECT b FROM {src}")
        cur.execute()
        lob = conn.lob()
        first = cur.fetch_lob(1, lob)
        at_end = cur.fetch_lob(1, lob)  # returns None and keeps the handle
        ins = conn.cursor()
        ins.prepare(f"INSERT INTO {dst} (id, b) VALUES (1, ?)")
        ins.bind_lob(1, lob)
        count = ins.execute()
        cur.close()
        ins.close()
        return (first, at_end, count)

    rows = ((1, b"only row", None),)
    return _lob_run(native, rows, body, "b"), _lob_run(_cubrid, rows, body, "b")


def _lob_fetch_null_cell() -> tuple[str, str]:
    def body(_module: Any, conn: Any, src: str, _dst: str) -> object:
        cur = conn.cursor()
        cur.prepare(f"SELECT b, id FROM {src} ORDER BY id")
        cur.execute()
        fetched = cur.fetch_lob(1, conn.lob())
        # The NULL row was consumed: the next row is id 2.
        following = cur.fetch_row()[1]
        cur.close()
        return (fetched, following)

    rows = ((1, None, None), (2, b"x", None))
    return _lob_run(native, rows, body, "b"), _lob_run(_cubrid, rows, body, "b")


def _lob_wrong_type() -> tuple[str, str]:
    def body(module: Any, conn: Any, src: str, dst: str) -> object:
        cur = conn.cursor()
        cur.prepare(f"SELECT b FROM {src}")
        cur.execute()
        ins = conn.cursor()
        ins.prepare(f"INSERT INTO {dst} (id, b) VALUES (1, ?)")
        outcomes = [
            _outcome(lambda: ins.bind_lob(1, b"x")),
            _outcome(lambda: ins.bind_lob(1, "x")),
            _outcome(lambda: ins.bind_lob(1, None)),
            _outcome(lambda: cur.fetch_lob(1, object())),
            _outcome(lambda: module.lob(object())),
            _outcome(lambda: ins.bind_lob("1", b"x")),  # the index is parsed first
        ]
        cur.close()
        ins.close()
        return outcomes

    rows = ((1, b"x", None),)
    return _lob_run(native, rows, body, "b"), _lob_run(_cubrid, rows, body, "b")


def _lob_without_value() -> tuple[str, str]:
    def body(_module: Any, conn: Any, src: str, dst: str) -> object:
        cur = conn.cursor()
        cur.prepare(f"SELECT b FROM {src}")
        cur.execute()
        closed = conn.lob()
        cur.fetch_lob(1, closed)
        closed.close()
        ins = conn.cursor()
        outcomes = []
        for row_id, lob in ((1, conn.lob()), (2, closed)):
            ins.prepare(f"INSERT INTO {dst} (id, b) VALUES ({row_id}, ?)")

            def bind_and_execute(target: Any = lob) -> object:
                ins.bind_lob(1, target)
                return ins.execute()

            outcomes.append(_outcome(bind_and_execute))
        cur.close()
        ins.close()
        return outcomes

    rows = ((1, b"x", None),)
    return _lob_run(native, rows, body, "b"), _lob_run(_cubrid, rows, body, "b")


def _lob_cross_connection() -> tuple[str, str]:
    def body(module: Any, conn: Any, src: str, dst: str) -> object:
        cur = conn.cursor()
        cur.prepare(f"SELECT b FROM {src}")
        cur.execute()
        lob = conn.lob()
        cur.fetch_lob(1, lob)
        other = module.connect(URL, TEST_USER, TEST_PASSWORD)
        try:
            ins = other.cursor()
            ins.prepare(f"INSERT INTO {dst} (id, b) VALUES (1, ?)")

            def bind_and_execute() -> object:
                ins.bind_lob(1, lob)
                return ins.execute()

            outcome = _outcome(bind_and_execute)
            ins.close()
        finally:
            other.close()
        cur.close()
        return outcome

    rows = ((1, b"committed", None),)
    return _lob_run(native, rows, body, "b"), _lob_run(_cubrid, rows, body, "b")


def _lob_fetch_into_closed_or_foreign() -> tuple[str, str]:
    def body(module: Any, conn: Any, src: str, dst: str) -> object:
        cur = conn.cursor()
        cur.prepare(f"SELECT b FROM {src} ORDER BY id")
        cur.execute()
        closed = conn.lob()
        cur.fetch_lob(1, closed)
        closed.close()
        other = module.connect(URL, TEST_USER, TEST_PASSWORD)
        try:
            foreign = other.lob()
            outcomes = [
                _outcome(lambda: cur.fetch_lob(1, closed)),  # row 2
                _outcome(lambda: cur.fetch_lob(1, foreign)),  # row 3
            ]
            ins = conn.cursor()
            for row_id, lob in ((1, closed), (2, foreign)):
                ins.prepare(f"INSERT INTO {dst} (id, b) VALUES ({row_id}, ?)")

                def bind_and_execute(target: Any = lob) -> object:
                    ins.bind_lob(1, target)
                    return ins.execute()

                outcomes.append(_outcome(bind_and_execute))
            ins.close()
        finally:
            other.close()
        cur.close()
        return outcomes

    rows = ((1, b"one", None), (2, b"two", None), (3, b"three", None))
    return _lob_run(native, rows, body, "b"), _lob_run(_cubrid, rows, body, "b")


def _lob_fetch_end_before_checks() -> tuple[str, str]:
    def body(_module: Any, conn: Any, src: str, _dst: str) -> object:
        cur = conn.cursor()
        cur.prepare(f"SELECT id, b FROM {src}")
        cur.execute()
        cur.fetch_row()
        closed = conn.lob()
        closed.close()
        outcomes = [
            _outcome(lambda: cur.fetch_lob(1, conn.lob())),  # INTEGER column
            _outcome(lambda: cur.fetch_lob(0, conn.lob())),
            _outcome(lambda: cur.fetch_lob(9, conn.lob())),
            _outcome(lambda: cur.fetch_lob(2, closed)),
            _outcome(lambda: cur.fetch_lob("2", conn.lob())),  # argument parsing first
        ]
        cur.close()
        return outcomes

    rows = ((1, b"x", None),)
    return _lob_run(native, rows, body, "b"), _lob_run(_cubrid, rows, body, "b")


def _lob_bind_after_source_close() -> tuple[str, str]:
    def body(module: Any, conn: Any, src: str, dst: str) -> object:
        source = module.connect(URL, TEST_USER, TEST_PASSWORD)
        cur = source.cursor()
        cur.prepare(f"SELECT b FROM {src}")
        cur.execute()
        lob = source.lob()
        cur.fetch_lob(1, lob)
        cur.close()
        source.close()
        ins = conn.cursor()
        ins.prepare(f"INSERT INTO {dst} (id, b) VALUES (1, ?)")

        def bind_and_execute() -> object:
            ins.bind_lob(1, lob)
            return ins.execute()

        outcome = _outcome(bind_and_execute)
        ins.close()
        return outcome

    rows = ((1, b"committed", None),)
    return _lob_run(native, rows, body, "b"), _lob_run(_cubrid, rows, body, "b")


def _lob_error_classes() -> tuple[str, str]:
    def body(_module: Any, conn: Any, src: str, dst: str) -> object:
        cur = conn.cursor()
        cur.prepare(f"SELECT id, b FROM {src}")
        cur.execute()
        lob = conn.lob()
        non_lob = _outcome(lambda: cur.fetch_lob(1, lob))
        # The rejected call did not consume row 1.
        following = cur.fetch_row()[0]
        cur.execute()
        cur.fetch_lob(2, lob)
        ins = conn.cursor()
        ins.prepare(f"INSERT INTO {dst} (id, b) VALUES (1, ?)")
        outcomes = [
            non_lob,
            following,
            _outcome(lambda: ins.bind_lob(0, lob)),
            _outcome(lambda: ins.bind_lob(2, lob)),
        ]
        cur.close()
        ins.close()
        return outcomes

    rows = ((1, b"x", None),)
    return _lob_run(native, rows, body, "b"), _lob_run(_cubrid, rows, body, "b")


def _lob_stream(payload: str | bytes, kind: str) -> tuple[str, str]:
    """Compare only in-range byte-position operations safe in the C extension."""

    def observe(module: Any) -> str:
        conn = module.connect(URL, TEST_USER, TEST_PASSWORD)
        lob = conn.lob()
        try:
            written = lob.write(payload, kind)
            end = lob.seek(0, module.SEEK_CUR)
            start = lob.seek(0, module.SEEK_SET)
            first = lob.read(1)  # ASCII prefix, never split a UTF-8 character
            after_first = lob.seek(0, module.SEEK_CUR)
            rest = lob.read()  # strictly before EOF
            last_pos = lob.seek(1, module.SEEK_END)
            last = lob.read(1)
            return render((written, end, start, first, after_first, rest, last_pos, last))
        finally:
            lob.close()
            conn.close()

    return observe(native), observe(_cubrid)


def _native_settings_cache_and_setters() -> tuple[str, str]:
    """Compare safe cached members and valid effective setter values (#467)."""

    def observe(module: Any) -> str:
        conn = module.connect(URL, TEST_USER, TEST_PASSWORD)
        try:
            initial = (
                conn.autocommit,
                conn.isolation_level,
                conn.lock_timeout,
                conn.max_string_len,
            )
            names = ("autocommit", "isolation_level", "lock_timeout", "max_string_len")
            markers = [object() for _ in names]
            for name, marker in zip(names, markers):
                setattr(conn, name, marker)
            identities = tuple(
                getattr(conn, name) is marker for name, marker in zip(names, markers)
            )
            auto_result = conn.set_autocommit(False)
            auto_after = conn.autocommit
            iso_result = conn.set_isolation_level(4)
            iso_after_four = conn.isolation_level
            conn.set_isolation_level(5)
            iso_after_five = conn.isolation_level
            conn.set_isolation_level(6)
            iso_after_six = conn.isolation_level
            conn.set_autocommit(True)
            return render(
                (
                    initial,
                    identities,
                    auto_result,
                    auto_after,
                    iso_result,
                    iso_after_four,
                    iso_after_five,
                    iso_after_six,
                    conn.autocommit,
                )
            )
        finally:
            conn.close()

    return observe(native), observe(_cubrid)


def _result_info_outcome(call: Callable[[], object]) -> object:
    """Compare only result_info errors, without changing older claim observations."""
    try:
        return call()
    except Exception as exc:  # numeric code is .code here, not pycubrid's errno
        code = getattr(exc, "code", None)
        if type(code) is not int:
            code = exc.args[0] if exc.args and type(exc.args[0]) is int else None
        return "raises", type(exc).__name__, code


def _native_result_info_columns() -> tuple[str, str]:
    """Read full metadata without fetching NUMERIC/collection/JSON values."""
    observer = _ordinary()
    setup = observer.cursor()
    parent_created = table_created = False
    try:
        setup.execute(
            "SELECT class_name FROM db_class WHERE class_name IN ('odri445_parent', 'odri445_meta')"
        )
        assert not setup.fetchall(), "result_info fixture tables already exist"
        setup.execute("CREATE TABLE odri445_parent (id INTEGER PRIMARY KEY)")
        parent_created = True
        setup.execute(
            "CREATE TABLE odri445_meta ("
            "id INTEGER AUTO_INCREMENT PRIMARY KEY, "
            "txt VARCHAR(40) DEFAULT '한' NOT NULL, num NUMERIC(10,2) DEFAULT 1.25, "
            "unique_txt VARCHAR(20) UNIQUE, parent_id INTEGER, "
            "rev_txt VARCHAR(20), rev_unique_txt VARCHAR(20), shared_num INTEGER SHARED 7, "
            "set_val SET(INTEGER), multi_val MULTISET(INTEGER), "
            "seq_val SEQUENCE(INTEGER), json_val JSON, "
            "CONSTRAINT fk_odri445 FOREIGN KEY (parent_id) REFERENCES odri445_parent(id))"
        )
        table_created = True
        setup.execute("CREATE REVERSE INDEX odri445_rev ON odri445_meta (rev_txt)")
        setup.execute(
            "CREATE REVERSE UNIQUE INDEX odri445_rev_unique ON odri445_meta (rev_unique_txt)"
        )

        def observe(module: Any) -> str:
            conn = module.connect(URL, TEST_USER, TEST_PASSWORD)
            cur = conn.cursor()
            try:
                cur.prepare("SELECT * FROM odri445_meta WHERE 1=0")
                assert cur.execute() == 0
                all_columns = cur.result_info()
                assert len(all_columns) == 12
                assert all(len(column) == 15 for column in all_columns)
                assert all(type(column[0]) is int for column in all_columns)
                for index, flag in (
                    (0, 8),
                    (0, 10),
                    (1, 1),
                    (3, 9),
                    (4, 11),
                    (5, 12),
                    (6, 13),
                    (7, 14),
                ):
                    assert all_columns[index][flag] == 1
                selected = cur.result_info(1)
                assert selected == (all_columns[0],)
                assert len(selected) == 1 and selected[0][4] == "id"
                cur.prepare("SELECT id, txt FROM odri445_meta WHERE 1=0")
                cur.execute()
                basic = cur.result_info()
                # Assess the original result_info scenario's four assertions.
                assert len(basic) == 2 and basic[0][10] == 1
                cur.prepare(
                    'SELECT id AS "MiXeD", txt AS "한글", CAST(1 AS INTEGER) AS expr '
                    "FROM odri445_meta WHERE 1=0"
                )
                cur.execute()
                aliases = cur.result_info()
                cur.prepare("UPDATE odri445_meta SET txt=txt WHERE 1=0")
                cur.execute()
                dml = tuple(
                    _result_info_outcome(lambda n=n: cur.result_info(n))
                    for n in (0, -1, -(2**31), 2**31 - 1, None, 2**31)
                )
                return render((all_columns, selected, basic, aliases, dml))
            finally:
                try:
                    cur.close()
                finally:
                    conn.close()

        return observe(native), observe(_cubrid)
    finally:
        try:
            if table_created:
                setup.execute("DROP TABLE odri445_meta")
            if parent_created:
                setup.execute("DROP TABLE odri445_parent")
        finally:
            try:
                setup.close()
            finally:
                observer.close()


def _native_result_info_selectors() -> tuple[str, str]:
    class IndexOne:
        def __index__(self) -> int:
            return 1

    class IntOnly:
        def __int__(self) -> int:
            return 1

    def observe(module: Any) -> str:
        conn = module.connect(URL, TEST_USER, TEST_PASSWORD)
        cur = conn.cursor()
        cursor_closed = False
        try:
            fresh = _result_info_outcome(cur.result_info)
            cur.prepare("SELECT 1 AS id FROM db_root WHERE 1=0")
            prepared = _result_info_outcome(cur.result_info)
            cur.execute()
            selected = tuple(cur.result_info(n) for n in (False, True, IndexOne()))
            errors = tuple(
                _result_info_outcome(lambda n=n: cur.result_info(n))
                for n in (-1, 2, None, "1", 1.5, IntOnly(), 2**31, -(2**31) - 1)
            )
            arity = _result_info_outcome(lambda: cur.result_info(0, 1))
            keyword = _result_info_outcome(lambda: cur.result_info(n=0))
            cur.close()
            cursor_closed = True
            closed = (
                _result_info_outcome(cur.result_info),
                _result_info_outcome(lambda: cur.result_info(None)),
                _result_info_outcome(lambda: cur.result_info(0, 1)),
                _result_info_outcome(lambda: cur.result_info(n=0)),
            )
            return render((fresh, prepared, selected, errors, arity, keyword, closed))
        finally:
            # Native close is terminal and its second close raises; avoid it.
            try:
                if not cursor_closed:
                    cur.close()
            finally:
                conn.close()

    return observe(native), observe(_cubrid)


def _native_result_info_position_and_boundaries() -> tuple[str, str]:
    def observe(module: Any) -> str:
        conn = module.connect(URL, TEST_USER, TEST_PASSWORD)
        cur = conn.cursor()
        try:
            conn.set_autocommit(False)
            cur.prepare(
                "SELECT 1 AS id, 'one' AS txt FROM db_root "
                "UNION ALL SELECT 2 AS id, 'two' AS txt FROM db_root ORDER BY id"
            )
            cur.execute()
            before = cur.result_info()
            first = cur.fetch_row()
            middle = cur.result_info(2)
            second = cur.fetch_row()
            eof = cur.fetch_row()
            after = cur.result_info()
            conn.commit()
            committed = cur.result_info()
            conn.rollback()
            rolled_back = cur.result_info()
            return render((before, first, middle, second, eof, after, committed, rolled_back))
        finally:
            try:
                cur.close()
            finally:
                conn.close()

    return observe(native), observe(_cubrid)


def _native_result_info_error_args() -> tuple[str, str]:
    def observe(module: Any) -> str:
        conn = module.connect(URL, TEST_USER, TEST_PASSWORD)
        cur = conn.cursor()
        try:
            cur.close()
            try:
                cur.result_info()
            except Exception as exc:  # retain the explicit exception-args deviation
                return render((len(exc.args), tuple(type(arg).__name__ for arg in exc.args)))
            raise AssertionError("closed result_info did not fail")
        finally:
            conn.close()

    return observe(native), observe(_cubrid)


_POSITION_QUERY = "SELECT id, txt FROM odnav444_fixture WHERE id <= ? ORDER BY id"


def _positioning_fixture(observe: Callable[[Any], str]) -> tuple[str, str]:
    """Own a fixed scalar fixture only after checking that it does not exist."""
    observer = _ordinary()  # Explicit ordinary autocommit=True.
    setup = observer.cursor()
    created = False
    try:
        setup.execute("SELECT class_name FROM db_class WHERE class_name=?", ("odnav444_fixture",))
        assert not setup.fetchall(), "positioning fixture already exists"
        setup.execute("CREATE TABLE odnav444_fixture (id INTEGER PRIMARY KEY, txt VARCHAR(20))")
        created = True
        setup.execute(
            "INSERT INTO odnav444_fixture VALUES " + ", ".join(["(?, ?)"] * 1537),
            tuple(value for row in range(1, 1538) for value in (row, str(row))),
        )
        assert setup.rowcount == 1537
        return observe(native), observe(_cubrid)
    finally:
        try:
            if created:
                setup.execute("DROP TABLE odnav444_fixture")
        finally:
            try:
                setup.close()
            finally:
                observer.close()


def _position_outcome(call: Callable[[], object]) -> object:
    try:
        return call()
    except Exception as exc:  # Only these new client-operation observations.
        code = getattr(exc, "code", None)
        if type(code) is not int:
            code = exc.args[0] if exc.args and type(exc.args[0]) is int else None
        return "raises", type(exc).__name__, code


def _position_stream_metrics(conn: Any) -> list[dict[str, int]]:
    """Candidate-only streaming: no result list, full trace or C-buffer guess."""
    assert not tracemalloc.is_tracing(), "stream metric requires an isolated tracer"
    cur = conn.cursor()
    samples = []
    try:
        for total in (257, 1537):
            cur.prepare(_POSITION_QUERY)
            cur.bind_param(1, total)
            count = checksum = peak_page = 0
            tracemalloc.start()
            try:
                assert cur.execute() == total
                initial_page = peak_page = len(cur._rows)
                while (row := cur.fetch_row()) is not None:
                    count += 1
                    assert row == (count, str(count))
                    checksum += row[0]
                    peak_page = max(peak_page, len(cur._rows))
                peak_bytes = tracemalloc.get_traced_memory()[1]
            finally:
                tracemalloc.stop()
            assert count == total and checksum == total * (total + 1) // 2
            samples.append(
                {
                    "rows": total,
                    "initial_page_rows": initial_page,
                    "requested_fetch_size": conn._driver._fetch_size,
                    "max_page_rows": peak_page,
                    "peak_bytes": peak_bytes,
                }
            )
        assert samples[1]["max_page_rows"] < samples[1]["rows"], samples
        return samples
    finally:
        cur.close()


def _native_position_sequences() -> tuple[str, str]:
    def observe(module: Any) -> str:
        conn = module.connect(URL, TEST_USER, TEST_PASSWORD)
        conn.set_autocommit(False)  # Match the measured official-only baseline.
        cur = conn.cursor()
        trace: list[dict[str, int | str]] = []
        driver = conn._driver if module is native else None
        original_send = driver._send_and_receive if driver is not None else None
        generation = driver._physical_generation if driver is not None else None

        def completed(packet: Any, **kwargs: Any) -> Any:
            assert original_send is not None
            result = original_send(packet, **kwargs)
            if isinstance(packet, (PreparePacket, ExecutePacket, FetchPacket)):
                rows = packet.rows if isinstance(packet, (ExecutePacket, FetchPacket)) else []
                trace.append(
                    {
                        "request": type(packet).__name__,
                        "start": packet.current_tuple_count + 1
                        if isinstance(packet, FetchPacket)
                        else 1,
                        "count": len(rows),
                        "first_id": rows[0][0] if rows else 0,
                        "last_id": rows[-1][0] if rows else 0,
                        "handle": packet.query_handle,
                        "generation": driver._physical_generation,
                    }
                )
                if isinstance(packet, ExecutePacket):
                    trace[-1]["auto_commit"] = packet.auto_commit
                    trace[-1]["forward_only"] = packet.forward_only
            return result

        if driver is not None:
            driver._send_and_receive = completed
        try:
            cur.prepare(_POSITION_QUERY)
            cur.bind_param(1, 257)
            executed = cur.execute()
            initial = cur.row_tell()
            first = cur.fetch_row()
            after_first = cur.row_tell()
            cur.data_seek(3)
            absolute = cur.row_tell()
            assert absolute == 3  # e75ec36 tests3/test_cubrid.py:393
            cur.row_seek(-2)
            backward = cur.row_tell()
            assert backward == 1  # source:406
            cur.row_seek(4)
            forward = cur.row_tell()
            assert forward == 5  # source:409
            sought = cur.fetch_row()
            after_sought = cur.row_tell()
            cur.execute()
            reexecuted = cur.row_tell()
            again = cur.fetch_row()
            after_again = cur.row_tell()
            cur.prepare(_POSITION_QUERY)
            cur.bind_param(1, 257)
            cur.execute()
            for expected in range(1, 206):
                assert cur.fetch_row() == (expected, str(expected))
            after_205 = cur.row_tell()
            pages = []
            for position in (3, 220):
                before = len(trace)
                page_start = cur._fetched_count - len(cur._rows) + 1 if driver is not None else 0
                outside = driver is not None and not page_start <= position <= cur._fetched_count
                cur.data_seek(position)
                assert len(trace) == before
                if driver is not None:
                    _write(
                        {
                            "record": "candidate-metrics",
                            "claim": "native-position-sequences",
                            "scope": "supplemental pre-fetch diagnostic; not a comparison pass",
                            "target": position,
                            "retained_page_start": page_start,
                            "retained_page_end": cur._fetched_count,
                            "retained_page_rows": len(cur._rows),
                            "total_rows": cur._total_tuple_count,
                            "configured_fetch_size": driver._fetch_size,
                            "completed_requests": list(trace),
                        }
                    )
                row = cur.fetch_row()
                assert row == (position, str(position))
                if outside:
                    assert trace[-1]["request"] == "FetchPacket" and trace[-1]["start"] == position
                pages.append((row, cur.row_tell()))
            page_start = cur._fetched_count - len(cur._rows) + 1 if driver is not None else 0
            outside_106 = driver is not None and not page_start <= 106 <= cur._fetched_count
            cur.row_seek(-115)
            row_106 = cur.fetch_row()
            assert row_106 == (106, "106")
            if outside_106:
                assert trace[-1]["request"] == "FetchPacket" and trace[-1]["start"] == 106
            final_257 = cur.row_tell()

            # A separate larger result proves actual evictions, not assumed size100.
            cur.prepare(_POSITION_QUERY)
            cur.bind_param(1, 1537)
            cur.execute()
            initial_1537 = len(cur._rows) if driver is not None else 0
            assert initial_1537 < 1537
            consumed = 1205
            assert initial_1537 < consumed, (
                "larger result still fits before the chosen eviction point"
            )
            for expected in range(1, consumed + 1):
                assert cur.fetch_row() == (expected, str(expected))
            extended = []
            for position in (3, 1500):
                before = len(trace)
                page_start = cur._fetched_count - len(cur._rows) + 1 if driver is not None else 0
                outside = driver is not None and not page_start <= position <= cur._fetched_count
                cur.data_seek(position)
                assert len(trace) == before
                if driver is not None:
                    _write(
                        {
                            "record": "candidate-metrics",
                            "claim": "native-position-sequences",
                            "scope": "supplemental before extended FETCH; not a comparison pass",
                            "target": position,
                            "retained_page_start": page_start,
                            "retained_page_end": cur._fetched_count,
                            "retained_page_rows": len(cur._rows),
                            "total_rows": cur._total_tuple_count,
                            "configured_fetch_size": driver._fetch_size,
                            "completed_requests": list(trace),
                        }
                    )
                row = cur.fetch_row()
                assert row == (position, str(position))
                if outside:
                    assert trace[-1]["request"] == "FetchPacket" and trace[-1]["start"] == position
                extended.append((row, cur.row_tell()))
            page_start = cur._fetched_count - len(cur._rows) + 1 if driver is not None else 0
            outside_106 = driver is not None and not page_start <= 106 <= cur._fetched_count
            cur.row_seek(-1395)
            row_106_extended = cur.fetch_row()
            assert row_106_extended == (106, "106")
            if outside_106:
                assert trace[-1]["request"] == "FetchPacket" and trace[-1]["start"] == 106
            final_1537 = cur.row_tell()
            if driver is not None:
                assert driver._physical_generation == generation
                starts = [r["start"] for r in trace if r["request"] == "FetchPacket"]
                _write(
                    {
                        "record": "candidate-metrics",
                        "claim": "native-position-sequences",
                        "scope": "supplemental completed requests before trace assertions",
                        "configured_fetch_size": driver._fetch_size,
                        "retained_page_rows": len(cur._rows),
                        "completed_requests": trace,
                    }
                )
                assert all(position in starts for position in (3, 1500, 106)), trace
                assert sum(r["request"] == "ExecutePacket" for r in trace) == 4
                assert sum(r["request"] == "PreparePacket" for r in trace) == 3
                driver._send_and_receive = original_send
                _write(
                    {
                        "record": "candidate-metrics",
                        "claim": "native-position-sequences",
                        "scope": "supplemental candidate-only; C-extension buffers unexposed",
                        "completed_requests": trace,
                        "stream_samples": _position_stream_metrics(conn),
                    }
                )
            return render(
                (
                    executed,
                    initial,
                    first,
                    after_first,
                    absolute,
                    backward,
                    forward,
                    sought,
                    after_sought,
                    reexecuted,
                    again,
                    after_again,
                    after_205,
                    pages,
                    row_106,
                    final_257,
                    consumed,
                    extended,
                    row_106_extended,
                    final_1537,
                )
            )
        finally:
            if driver is not None:
                driver._send_and_receive = original_send
            try:
                cur.close()
            finally:
                conn.close()

    return _positioning_fixture(observe)


def _native_position_boundaries() -> tuple[str, str]:
    def observe(module: Any) -> str:
        conn = module.connect(URL, TEST_USER, TEST_PASSWORD)
        conn.set_autocommit(False)
        cur = conn.cursor()
        try:
            drains = []
            for absolute_first in (False, True):
                cur.prepare(_POSITION_QUERY)
                cur.bind_param(1, 257)
                cur.execute()
                if absolute_first:
                    cur.data_seek(1)
                for expected in range(1, 258):
                    assert cur.fetch_row() == (expected, str(expected))
                drains.append((cur.fetch_row(), _position_outcome(cur.row_tell), cur.fetch_row()))
            cur.data_seek(3)
            before_first = (
                _position_outcome(lambda: cur.row_seek(-3)),
                cur.row_tell(),
                cur.fetch_row(),
            )
            cur.data_seek(3)
            recovered = cur.fetch_row()
            cur.data_seek(256)
            after_end = (
                _position_outcome(lambda: cur.row_seek(2)),
                cur.row_tell(),
                cur.fetch_row(),
            )
            cur.data_seek(257)
            last = (cur.fetch_row(), _position_outcome(cur.row_tell))
            cur.prepare(_POSITION_QUERY)
            cur.bind_param(1, 0)
            cur.execute()
            empty = (cur.row_tell(), cur.fetch_row(), _position_outcome(lambda: cur.data_seek(1)))
            return render((drains, before_first, recovered, after_end, last, empty))
        finally:
            try:
                cur.close()
            finally:
                conn.close()

    return _positioning_fixture(observe)


def _wrapper_position_fetches() -> tuple[str, str]:
    def observe(module: Any) -> str:
        conn = module.connect(URL, TEST_USER, TEST_PASSWORD)
        conn.set_autocommit(False)
        cursors = []
        observations = []
        try:
            for dict_cursor in (False, True):
                cur = conn.cursor(dictCursor=dict_cursor)
                cursors.append(cur)
                cur.execute(_POSITION_QUERY, (257,))
                description = cur.description
                cur._cs.data_seek(3)
                first = cur.fetchone()
                cur._cs.row_seek(-2)
                many = cur.fetchmany(3)
                cur._cs.data_seek(220)
                rest = cur.fetchall()
                cur.arraysize = 2
                cur._cs.data_seek(50)
                default_many = cur.fetchmany()
                assert cur.description == description
                observations.append((first, many, rest, default_many, cur._cs.row_tell()))
            return render(observations)
        finally:
            try:
                for cur in cursors:
                    cur.close()
            finally:
                conn.close()

    # The source wrapper delegates to its existing native _cs; no forwards added.
    return _positioning_fixture(lambda module: observe(cubriddb if module is native else CUBRIDdb))


def _native_position_error_args() -> tuple[str, str]:
    def observe(module: Any) -> str:
        conn = module.connect(URL, TEST_USER, TEST_PASSWORD)
        cur = conn.cursor()
        try:
            cur.close()
            outcomes = []
            for call in (lambda: cur.data_seek(1), lambda: cur.row_seek(1), cur.row_tell):
                try:
                    call()
                except Exception as exc:
                    outcomes.append((len(exc.args), tuple(type(arg).__name__ for arg in exc.args)))
                else:
                    raise AssertionError("closed positioning operation did not fail")
            return render(outcomes)
        finally:
            conn.close()

    return observe(native), observe(_cubrid)


def _lob_file_roundtrip(kind: str, payload: bytes, *, replacement: bool = False) -> tuple[str, str]:
    """Generate owned files and verify a real stored-column byte roundtrip."""
    observer = _ordinary()
    setup = observer.cursor()
    created = False
    try:
        setup.execute("SELECT class_name FROM db_class WHERE class_name=?", ("odfile443_fixture",))
        assert not setup.fetchall(), "refuse preexisting file-roundtrip fixture"
        setup.execute("CREATE TABLE odfile443_fixture (id INTEGER PRIMARY KEY, b BLOB, c CLOB)")
        created = True
        with tempfile.TemporaryDirectory(prefix="py443-") as directory:
            root = Path(directory)

            def observe(module: Any, row_id: int) -> str:
                source, destination = root / f"input-{row_id}.bin", root / f"output-{row_id}.bin"
                source.write_bytes(payload)
                destination.write_bytes(b"fixture-owned old output")
                conn = module.connect(URL, TEST_USER, TEST_PASSWORD)
                cur = conn.cursor()
                imported, fetched = conn.lob(), conn.lob()
                try:
                    if replacement:
                        imported.write(b"old value", kind)
                        imported.seek(3, module.SEEK_SET)
                    before_import = imported.seek(0, module.SEEK_CUR) if replacement else 0
                    # Omitted type proves BLOB default; CLOB uses its explicit type.
                    import_result = (
                        imported.imports(str(source))
                        if kind == "B"
                        else imported.imports(str(source), "C")
                    )
                    after_import = imported.seek(0, module.SEEK_CUR)
                    assert after_import == before_import
                    cur.prepare(
                        "INSERT INTO odfile443_fixture (id, b) VALUES (?, ?)"
                        if kind == "B"
                        else "INSERT INTO odfile443_fixture (id, c) VALUES (?, ?)"
                    )
                    cur.bind_param(1, row_id)
                    cur.bind_lob(2, imported)
                    inserted_count = cur.execute()
                    assert inserted_count == 1
                    # First bind consumed the created temporary locator; do not export it.
                    imported.close()
                    cur.prepare(
                        "SELECT b FROM odfile443_fixture WHERE id=?"
                        if kind == "B"
                        else "SELECT c FROM odfile443_fixture WHERE id=?"
                    )
                    cur.bind_param(1, row_id)
                    cur.execute()
                    cur.fetch_lob(1, fetched)  # Avoid the unrelated non-first-column C quirk.
                    fetched.seek(1, module.SEEK_SET)
                    before_export = fetched.seek(0, module.SEEK_CUR)
                    export_result = fetched.export(str(destination))
                    after_export = fetched.seek(0, module.SEEK_CUR)
                    assert after_export == before_export
                    with source.open("rb") as left, destination.open("rb") as right:
                        while True:
                            expected, actual = left.read(65536), right.read(65536)
                            assert (
                                expected == actual
                            )  # Explicit upstream file-equality assertion637.
                            if not expected:
                                break
                    exported = destination.read_bytes()
                    assert exported == payload
                    return render(
                        (
                            import_result,
                            export_result,
                            before_import,
                            after_import,
                            before_export,
                            after_export,
                            len(exported),
                            hashlib.sha256(exported).hexdigest(),
                        )
                    )
                finally:
                    try:
                        # Native close frees locally and is safe here even after prior close.
                        fetched.close()
                    finally:
                        try:
                            imported.close()
                        finally:
                            try:
                                cur.close()
                            finally:
                                conn.close()

            return observe(native, 1), observe(_cubrid, 2)
    finally:
        try:
            if created:
                setup.execute("DROP TABLE odfile443_fixture")
        finally:
            try:
                setup.close()
            finally:
                observer.close()


FIELD_INT, FIELD_STRING, FIELD_NUMERIC = 8, 2, 7  # CUBRIDdb.FIELD_TYPE values
KIND_MULTISET, KIND_SEQUENCE = 17, 18  # CUBRIDdb.FIELD_TYPE.MULTISET / .SEQUENCE


CASES: dict[str, Callable[[], tuple[str, str]]] = {
    "wrapper-row-conversion": _wrapper_row_conversion,
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
    "bind-set-int": _set_case("SET(INTEGER)", ("3", "1", "3", "2"), FIELD_INT),
    "bind-set-string": _set_case("SET(VARCHAR(20))", ("b", "a", "한", "b"), FIELD_STRING),
    "bind-set-elements-sent-as-string": _set_case("SET", ("1", "2"), FIELD_INT),
    "bind-set-empty": _set_case("SET(INTEGER)", (), FIELD_INT),
    "bind-set-not-imported": _set_case("SET(INTEGER)", _NOT_IMPORTED, FIELD_INT),
    "bind-set-wrong-object": _set_wrong_objects,
    "bind-multiset-duplicates": _set_case(
        "MULTISET(INTEGER)", ("3", "1", "3"), FIELD_INT, KIND_MULTISET
    ),
    "bind-sequence-order": _set_case(
        "SEQUENCE(INTEGER)", ("3", "1", "3", "2"), FIELD_INT, KIND_SEQUENCE
    ),
    "bind-set-null-text": _set_case("SET(VARCHAR(20))", ("NULL", "a"), FIELD_STRING),
    "bind-set-empty-string": _set_case("SET(VARCHAR(20))", ("", "a"), FIELD_STRING),
    "bind-set-python-int": _set_case("SET(INTEGER)", (1, 2), FIELD_INT),
    "bind-set-numeric-type": _set_case("SET(NUMERIC(5,2))", ("1.5", "2"), FIELD_NUMERIC),
    "bind-set-nul-truncation": _set_case("SET(VARCHAR(20))", ("a\x00b", "c"), FIELD_STRING),
    "bind-set-error-classes": _set_error_classes,
    "lob-fetch-bind-copy-blob": _lob_copy("b", 1, "b"),
    "lob-fetch-bind-copy-clob": _lob_copy("c", 1, "c"),
    "lob-fetch-non-first-column": _lob_copy("c, b", 2, "b"),
    "lob-fetch-non-first-column-int-first": _lob_copy("id, b", 2, "b"),
    "lob-fetch-non-first-column-blob-first": _lob_copy("b, c", 2, "c"),
    "lob-fetch-end-before-checks": _lob_fetch_end_before_checks,
    "lob-fetch-into-closed-or-foreign": _lob_fetch_into_closed_or_foreign,
    "lob-fetch-end": _lob_fetch_end,
    "lob-fetch-null-cell": _lob_fetch_null_cell,
    "lob-bind-wrong-type": _lob_wrong_type,
    "lob-bind-without-value": _lob_without_value,
    "lob-bind-cross-connection": _lob_cross_connection,
    "lob-bind-after-source-close": _lob_bind_after_source_close,
    "lob-error-classes": _lob_error_classes,
    "lob-stream-blob": lambda: _lob_stream(b"AhelloB", "B"),
    "lob-stream-clob": lambda: _lob_stream("A한éB", "C"),
    "native-cached-settings": _native_settings_cache_and_setters,
    "native-result-info-columns": _native_result_info_columns,
    "native-result-info-selectors": _native_result_info_selectors,
    "native-result-info-position-and-boundaries": _native_result_info_position_and_boundaries,
    "native-result-info-error-args": _native_result_info_error_args,
    "native-position-sequences": _native_position_sequences,
    "native-position-boundaries": _native_position_boundaries,
    "wrapper-position-fetches": _wrapper_position_fetches,
    "native-position-error-args": _native_position_error_args,
    "lob-file-blob-bytes": lambda: _lob_file_roundtrip("B", bytes(range(256))),
    "lob-file-blob-multi-chunk": lambda: _lob_file_roundtrip(
        "B", (bytes(range(256)) * 684)[:175000], replacement=True
    ),
    "lob-file-clob-utf8": lambda: _lob_file_roundtrip(
        "C", "A한éB".encode("utf-8") * 25000, replacement=True
    ),
    "wrapper-transaction-boundaries": _wrapper_transaction_boundaries,
    "native-wrapper-utilities": _native_wrapper_utilities,
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
            "autocommit": {
                "wrapper_ordinary": True,
                "native": "constructor default (True)",
                "scope": "constructor/setup defaults only; cases may explicitly override",
            },
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
    record: dict[str, Any] = {
        "record": "case",
        "claim": claim["id"],
        "classification": claim["classification"],
        "server_version": environment["server_version"],
        "pycubrid": pycubrid_obs,
        "native": native_obs,
        "outcome": outcome,
    }
    if claim["id"] == "native-wrapper-utilities":
        record["case_mode"] = {
            "autocommit": [True, False],
            "configured": "constructor default then explicit public manual setter on each actor",
            "scope": "healthy native/wrapper server text and query ping, not recovery or effects",
        }
    elif claim["id"] == "wrapper-transaction-boundaries":
        record["case_mode"] = {
            "autocommit": False,
            "configured": "public setter before INSERT on both wrappers",
            "observer_autocommit": True,
            "scope": "manual scalar row visibility, not result-lifetime or fault/recovery proof",
        }
    elif claim["id"] in {
        "native-position-sequences",
        "native-position-boundaries",
        "wrapper-position-fetches",
    }:
        record["case_mode"] = {
            "autocommit": False,
            "configured": "explicit setter before prepare/execute on both drivers/wrappers",
            "scope": "live same-owner SELECT; not default-mode lifetime proof",
        }
    elif claim["id"] == "native-position-error-args":
        record["case_mode"] = {
            "autocommit": "constructor default (True)",
            "query_executed": False,
            "scope": "closed client error args, not result-lifetime proof",
        }
    _write(record)
    assert agrees, (
        f"{claim['id']} ({claim['classification']}): pycubrid={pycubrid_obs} native={native_obs}"
        f" expected={expected}"
    )
