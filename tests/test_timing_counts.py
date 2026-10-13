"""TimingStats counts the same operations on both drivers, as documented (#899).

- The ``no_backslash_escapes`` probe of a new session is a driver request, not
  an application execute, so it is not counted.
- One successful ``executemany()`` call is one execute, whatever its path
  (``EXECUTE_BATCH``, ``prepared=True`` or the per-row loop).
- ``fetch_count`` counts the ``FETCH`` requests sent to the server; rows that
  arrived with the execute reply cost none.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pytest

from pycubrid.exceptions import ProgrammingError

from .helpers.replay_broker import run_replay_broker
from .test_batch_row_errors import _BatchScript
from .test_deferred_close import _Driver
from .test_replay_parity import _ints

# Run the real escape probe against the replay broker instead of the conftest pin.
pytestmark = pytest.mark.no_escape_pin

ASYNC = pytest.mark.parametrize("asynchronous", [False, True], ids=["sync", "async"])
SELECT = "SELECT n FROM t"
THREE_ROWS_ONE_INLINE = {SELECT: (_ints("three", 1, 2, 3), 1)}


def _counts(driver: _Driver) -> tuple[int, int, int]:
    stats = driver.conn.timing_stats
    return stats.connect_count, stats.execute_count, stats.fetch_count


def _run(
    asynchronous: bool,
    steps: Callable[[_Driver], Any],
    *,
    probe: bool = False,
    fetch_size: int = 100,
) -> Any:
    options: dict[str, Any] = {"enable_timing": True, "fetch_size": fetch_size}
    if probe:
        options["no_backslash_escapes"] = None  # detect it: run the escape probe
    with run_replay_broker(_BatchScript({}), results=THREE_ROWS_ONE_INLINE) as broker:
        driver = _Driver(asynchronous, broker.port, False, options)
        try:
            return steps(driver)
        finally:
            driver.teardown()


@ASYNC
def test_escape_probe_is_not_counted(asynchronous: bool) -> None:
    def steps(d: _Driver) -> Any:
        assert d.conn._no_backslash_escapes_explicit is False  # the probe ran
        after_connect = _counts(d)
        cursor = d.cursor()
        d.run(cursor.execute("SELECT 1"))
        d.run(cursor.fetchall())
        return after_connect, _counts(d)

    after_connect, after_select = _run(asynchronous, steps, probe=True)
    assert after_connect == (1, 0, 0)
    # The documented "connect + SELECT 1 + fetchall()" example.
    assert after_select == (1, 1, 0)


@ASYNC
@pytest.mark.parametrize(
    ("sql", "rows", "prepared"),
    [
        ("INSERT INTO t (a) VALUES (?)", [(1,), (2,), (3,)], False),
        ("INSERT INTO t (a, b) VALUES (?, ?)", [(1, "a"), (2, "b")], True),
        ("SELECT ? FROM db_root", [(1,), (2,)], False),
        ("INSERT INTO t (a) VALUES (?)", [], False),
    ],
    ids=["batch", "prepared", "loop", "empty"],
)
def test_executemany_counts_one_execute(
    asynchronous: bool, sql: str, rows: list[tuple[Any, ...]], prepared: bool
) -> None:
    def steps(d: _Driver) -> Any:
        stats = d.conn.timing_stats
        stats.reset()
        cursor = d.cursor()
        d.run(cursor.executemany(sql, rows, prepared=prepared))
        return stats.execute_count, stats.execute_total_ns

    count, total_ns = _run(asynchronous, steps)
    assert count == 1
    assert total_ns > 0


@ASYNC
def test_failed_executemany_is_not_counted(asynchronous: bool) -> None:
    def steps(d: _Driver) -> Any:
        stats = d.conn.timing_stats
        stats.reset()
        cursor = d.cursor()
        with pytest.raises(ProgrammingError, match="prepared must be True or False"):
            d.run(cursor.executemany("INSERT INTO t (a) VALUES (?)", [(1,)], prepared="yes"))
        # The cursor still counts later calls after the failure.
        d.run(cursor.executemany("INSERT INTO t (a) VALUES (?)", [(1,)]))
        return stats.execute_count

    assert _run(asynchronous, steps) == 1


@ASYNC
def test_fetch_count_is_server_fetch_requests(asynchronous: bool) -> None:
    def steps(d: _Driver) -> Any:
        stats = d.conn.timing_stats
        stats.reset()
        cursor = d.cursor()
        d.run(cursor.execute(SELECT))
        rows = d.run(cursor.fetchall())
        return rows, stats.execute_count, stats.fetch_count

    # One row inline, then two FETCH pages of one row each.
    rows, executes, fetches = _run(asynchronous, steps, fetch_size=1)
    assert [r[0] for r in rows] == [1, 2, 3]
    assert executes == 1
    assert fetches == 2


@pytest.mark.parametrize("probe", [False, True], ids=["pinned", "probed"])
def test_sync_and_async_counts_match(probe: bool) -> None:
    def steps(d: _Driver) -> Any:
        cursor = d.cursor()
        d.run(cursor.execute(SELECT))
        d.run(cursor.fetchall())
        d.run(cursor.executemany("INSERT INTO t (a) VALUES (?)", [(1,), (2,)]))
        d.run(cursor.executemany("INSERT INTO t (a, b) VALUES (?, ?)", [(1, "a")], prepared=True))
        d.run(cursor.executemany("SELECT ? FROM db_root", [(1,), (2,)]))
        return _counts(d)

    sync = _run(False, steps, probe=probe, fetch_size=1)
    asynchronous = _run(True, steps, probe=probe, fetch_size=1)
    assert sync == asynchronous == (1, 4, 2)
