"""Scratch-table lifecycle shared by ``profile_execute.py`` and ``profile_fetch.py`` (#950).

Each run creates its own table ``_profile_tmp_<12 hex digits>`` with a plain
``CREATE TABLE`` (no ``IF NOT EXISTS``), so a name collision fails instead of
reusing or dropping a table the run does not own. Only a table this run created
is dropped. Teardown runs after success and after a workload failure; a
workload error stays the raised exception, with any cleanup failure named in a
note, and a cleanup failure after a successful workload raises
:class:`ScratchCleanupError`. Teardown never reconnects, not even through the
driver's implicit CHECK_CAS recovery: if the process dies or the connection is
lost, the cleanup failure is reported and the table can remain; drop it by hand
(``DROP TABLE _profile_tmp_<hex>``).
"""

from __future__ import annotations

import re
import uuid
from collections.abc import Callable, Mapping
from typing import Any

TABLE_PREFIX = "_profile_tmp_"
_TABLE_NAME = re.compile(r"_profile_tmp_[0-9a-f]{12}")
_COLUMNS = "(id INT AUTO_INCREMENT PRIMARY KEY, val VARCHAR(64))"


class ScratchCleanupError(RuntimeError):
    """The workload finished, but the run's own resources were not all released."""


def new_table_name() -> str:
    """Return a fresh per-run scratch table name."""
    return checked_table_name(TABLE_PREFIX + uuid.uuid4().hex[:12])


def checked_table_name(name: str) -> str:
    """Return *name* if it is a scratch table name this module generates."""
    if not _TABLE_NAME.fullmatch(name):
        raise ValueError(f"not a profiler scratch table name: {name!r}")
    return name


def table_sql(template: str, table: str) -> str:
    """Fill ``{table}`` in an internal SQL template with a checked scratch name.

    Only the generated identifier is substituted; values are still bound as
    ``?`` parameters by the callers.
    """
    return template.replace("{table}", checked_table_name(table))


def run_with_scratch_table(
    conn: Any,
    workload: Callable[[Any, Mapping[str, str]], None],
    templates: Mapping[str, str],
) -> None:
    """Run *workload* against a table owned by this run, then release everything.

    *conn* is owned by this call and closed on every exit. *workload* receives
    the cursor and *templates* rendered for the run's table.
    """
    table = new_table_name()
    statements = {key: table_sql(sql, table) for key, sql in templates.items()}
    create_sql = table_sql("CREATE TABLE {table} " + _COLUMNS, table)
    drop_sql = table_sql("DROP TABLE IF EXISTS {table}", table)
    cursor = None
    owned = False
    try:
        cursor = conn.cursor()
        cursor.execute(create_sql)
        owned = True  # only a CREATE that succeeded makes the table ours
        conn.commit()
        workload(cursor, statements)
    except BaseException as exc:
        for failure in _release(conn, cursor, drop_sql if owned else None, failed=True):
            exc.add_note(f"profiler cleanup also failed: {failure}")
        raise
    failures = _release(conn, cursor, drop_sql if owned else None, failed=False)
    if failures:
        raise ScratchCleanupError("profiling finished, but cleanup failed: " + "; ".join(failures))


def _release(conn: Any, cursor: Any, drop_sql: str | None, *, failed: bool) -> list[str]:
    """Best-effort teardown; return a description of each step that failed."""
    failures: list[str] = []

    def attempt(step: str, action: Callable[[], object]) -> None:
        try:
            action()
        except Exception as exc:  # noqa: BLE001 - reported to the caller
            failures.append(f"{step}: {type(exc).__name__}: {exc}")

    # Teardown never reconnects (#950): a CAS session lost after the last
    # reply must fail the cleanup, not run it on a replacement session.
    # ``Connection.close()`` uses the same guard.
    suspends = hasattr(conn, "_implicit_reconnect_suspended")
    if suspends:
        conn._implicit_reconnect_suspended += 1
    try:
        if cursor is not None:
            if failed:
                attempt("rollback", conn.rollback)  # end the failed workload transaction
            if drop_sql is not None:

                def drop() -> None:
                    cursor.execute(drop_sql)
                    conn.commit()

                attempt(f"drop {drop_sql.rsplit(' ', 1)[-1]}", drop)
            attempt("cursor close", cursor.close)
    finally:
        if suspends:
            conn._implicit_reconnect_suspended -= 1
    attempt("connection close", conn.close)
    return failures
