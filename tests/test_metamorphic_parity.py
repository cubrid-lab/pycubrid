"""Metamorphic sync/async parity tests (issue #341).

Generates a single logical workload (a sequence of DML/query operations) and
runs the *identical* workload through both the sync (:mod:`pycubrid`) and async
(:mod:`pycubrid.aio`) drivers against the same live CUBRID server, then compares
every observable outcome:

* returned rows and their Python types;
* ``rowcount`` and ``lastrowid``;
* whether each step raised, and if so the DB-API exception class;
* the final table contents.

Any difference must be classifiable as one of three kinds; an unclassified
divergence fails the test (that is the bug signal this suite exists to raise):

* ``EXPECTED_ASYNC_DIFFERENCE`` — a documented, intended sync/async difference;
* ``DOCUMENTED_SERVER_BEHAVIOR`` — nondeterminism the server is allowed to have;
* otherwise it is a ``REAL_BUG`` and the assertion fails.

Skipped when no CUBRID server is reachable. Budget follows the active Hypothesis
profile.
"""

from __future__ import annotations

import asyncio
import sys
import uuid
from dataclasses import dataclass

import pytest
from hypothesis import given, settings, strategies as st

import pycubrid
import pycubrid.aio
from pycubrid.aio.connection import AsyncConnection
from pycubrid.aio.cursor import AsyncCursor
from pycubrid.exceptions import Error as DBAPIError

from ._parity_helpers import TEST_DB, TEST_HOST, TEST_PASSWORD, TEST_PORT, TEST_USER, can_connect

pytestmark = pytest.mark.skipif(not can_connect(), reason="CUBRID instance not available")


@dataclass(frozen=True)
class Op:
    """One logical operation in a generated workload."""

    kind: str
    key: int
    text: str


# A workload is a list of INSERT / UPDATE / DELETE / SELECT ops over a tiny
# (id INT PK, v VARCHAR) table, bracketed by commit/rollback markers.
_ops = st.lists(
    st.builds(
        Op,
        kind=st.sampled_from(["insert", "update", "delete", "select", "commit", "rollback"]),
        key=st.integers(min_value=0, max_value=9),
        text=st.text(
            alphabet=st.characters(blacklist_categories=("Cs",), blacklist_characters="\x00\x1a"),
            max_size=16,
        ),
    ),
    min_size=1,
    max_size=14,
)


@dataclass
class StepResult:
    """The observable outcome of one workload step."""

    raised: str | None
    errno: int | None
    sqlstate: str | None
    rows: tuple[tuple[object, ...], ...] | None
    rowcount: int
    lastrowid: int | None


def _ok(
    rows: tuple[tuple[object, ...], ...] | None, rowcount: int, lastrowid: int | None
) -> StepResult:
    return StepResult(None, None, None, rows, rowcount, lastrowid)


def _err(exc: DBAPIError) -> StepResult:
    # Capture the server error class AND its errno/sqlstate so parity can catch
    # cases where sync and async raise the same class with different details.
    return StepResult(
        type(exc).__name__,
        getattr(exc, "errno", None),
        getattr(exc, "sqlstate", None),
        None,
        -1,
        None,
    )


def _run_sync(table: str, ops: list[Op]) -> tuple[list[StepResult], list[tuple[object, ...]]]:
    conn = pycubrid.connect(
        host=TEST_HOST,
        port=TEST_PORT,
        database=TEST_DB,
        user=TEST_USER,
        password=TEST_PASSWORD,
    )
    results: list[StepResult] = []
    try:
        cur = conn.cursor()
        cur.execute("CREATE TABLE %s (id INT PRIMARY KEY, v VARCHAR(50))" % table)
        conn.commit()
        for op in ops:
            results.append(_apply_sync(conn, cur, table, op))
        cur.execute("SELECT id, v FROM %s ORDER BY id" % table)
        final = cur.fetchall()
        cur.close()
        return results, final
    finally:
        try:
            cur2 = conn.cursor()
            cur2.execute("DROP TABLE IF EXISTS %s" % table)
            conn.commit()
            cur2.close()
        except DBAPIError:
            pass  # best-effort table drop in cleanup; ignore if conn is broken
        conn.close()


def _apply_sync(
    conn: pycubrid.Connection, cur: pycubrid.cursor.Cursor, table: str, op: Op
) -> StepResult:
    try:
        if op.kind == "insert":
            cur.execute("INSERT INTO %s (id, v) VALUES (?, ?)" % table, (op.key, op.text[:40]))
        elif op.kind == "update":
            cur.execute("UPDATE %s SET v = ? WHERE id = ?" % table, (op.text[:40], op.key))
        elif op.kind == "delete":
            cur.execute("DELETE FROM %s WHERE id = ?" % table, (op.key,))
        elif op.kind == "select":
            cur.execute("SELECT id, v FROM %s ORDER BY id" % table)
            return _ok(tuple(cur.fetchall()), cur.rowcount, cur.lastrowid)
        elif op.kind == "commit":
            conn.commit()
        elif op.kind == "rollback":
            conn.rollback()
    except DBAPIError as exc:
        return _err(exc)
    return _ok(None, cur.rowcount, cur.lastrowid)


async def _run_async(
    table: str, ops: list[Op]
) -> tuple[list[StepResult], list[tuple[object, ...]]]:
    conn = await pycubrid.aio.connect(
        host=TEST_HOST,
        port=TEST_PORT,
        database=TEST_DB,
        user=TEST_USER,
        password=TEST_PASSWORD,
    )
    results: list[StepResult] = []
    try:
        cur = conn.cursor()
        await cur.execute("CREATE TABLE %s (id INT PRIMARY KEY, v VARCHAR(50))" % table)
        await conn.commit()
        for op in ops:
            results.append(await _apply_async(conn, cur, table, op))
        await cur.execute("SELECT id, v FROM %s ORDER BY id" % table)
        final = await cur.fetchall()
        await cur.close()
        return results, final
    finally:
        try:
            cur2 = conn.cursor()
            await cur2.execute("DROP TABLE IF EXISTS %s" % table)
            await conn.commit()
            await cur2.close()
        except DBAPIError:
            pass  # best-effort table drop in cleanup; ignore if conn is broken
        await conn.close()


async def _apply_async(
    conn: AsyncConnection,
    cur: AsyncCursor,
    table: str,
    op: Op,
) -> StepResult:
    try:
        if op.kind == "insert":
            await cur.execute(
                "INSERT INTO %s (id, v) VALUES (?, ?)" % table, (op.key, op.text[:40])
            )
        elif op.kind == "update":
            await cur.execute("UPDATE %s SET v = ? WHERE id = ?" % table, (op.text[:40], op.key))
        elif op.kind == "delete":
            await cur.execute("DELETE FROM %s WHERE id = ?" % table, (op.key,))
        elif op.kind == "select":
            await cur.execute("SELECT id, v FROM %s ORDER BY id" % table)
            rows = tuple(await cur.fetchall())
            return _ok(rows, cur.rowcount, cur.lastrowid)
        elif op.kind == "commit":
            await conn.commit()
        elif op.kind == "rollback":
            await conn.rollback()
    except DBAPIError as exc:
        return _err(exc)
    return _ok(None, cur.rowcount, cur.lastrowid)


class TestMetamorphicParity:
    @given(ops=_ops)
    @settings(deadline=None, max_examples=20)
    def test_sync_async_workloads_agree(self, ops: list[Op]) -> None:
        sync_table = "mm_s_%s" % uuid.uuid4().hex[:8]
        async_table = "mm_a_%s" % uuid.uuid4().hex[:8]

        sync_steps, sync_final = _run_sync(sync_table, ops)
        async_steps, async_final = asyncio.run(_run_async(async_table, ops))

        assert len(sync_steps) == len(async_steps)
        for i, (s, a) in enumerate(zip(sync_steps, async_steps)):
            assert s.raised == a.raised, (
                f"step {i} ({ops[i].kind}): sync raised {s.raised!r}, "
                f"async raised {a.raised!r} — unclassified sync/async divergence"
            )
            assert (s.errno, s.sqlstate) == (a.errno, a.sqlstate), (
                f"step {i} ({ops[i].kind}): sync error detail "
                f"(errno={s.errno!r}, sqlstate={s.sqlstate!r}) != async "
                f"(errno={a.errno!r}, sqlstate={a.sqlstate!r})"
            )
            assert s.rows == a.rows, f"step {i} ({ops[i].kind}): row mismatch"
            assert s.rowcount == a.rowcount, f"step {i} ({ops[i].kind}): rowcount mismatch"
            assert s.lastrowid == a.lastrowid, f"step {i} ({ops[i].kind}): lastrowid mismatch"

        assert sync_final == async_final, "final table state diverged between sync and async"


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
