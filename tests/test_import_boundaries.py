"""Fresh import orders and preserved cursor/exception boundaries (#561)."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

from pycubrid._cursor_common import _raise_batch_error
from pycubrid.exceptions import DatabaseError, ProgrammingError


@pytest.mark.parametrize(
    "first",
    [
        "pycubrid",
        "pycubrid.connection",
        "pycubrid.cursor",
        "pycubrid.aio",
        "pycubrid.aio.connection",
        "pycubrid.aio.cursor",
        "pycubrid._cursor_common",
        "pycubrid.exceptions",
    ],
)
def test_import_order_preserves_real_cursor_factories_and_aliases(first: str) -> None:
    code = """
import importlib
import sys
import weakref
from pathlib import Path
importlib.import_module(sys.argv[1])
import pycubrid
from pycubrid import _connection_common, _cursor_common, connection, cursor
from pycubrid.aio.connection import AsyncConnection
from pycubrid.aio.cursor import AsyncCursor
from pycubrid.exceptions import InterfaceError
from pycubrid.timing import TimingStats

assert Path(pycubrid.__file__).resolve().parent.parent == Path.cwd()
assert connection._resolve_ssl_context is _connection_common.resolve_ssl_context
assert _connection_common._resolve_ssl_context is _connection_common.resolve_ssl_context
assert cursor._DML_BATCH_VERBS is _cursor_common.DML_BATCH_VERBS
assert cursor._extract_first_keyword is _cursor_common.extract_first_keyword
assert cursor._split_on_placeholders is _cursor_common.split_on_placeholders
for factory, expected in ((connection.Connection, cursor.Cursor), (AsyncConnection, AsyncCursor)):
    conn = factory.__new__(factory)
    conn._connected = True
    conn._cursors = weakref.WeakSet()
    conn._fetch_size = 37
    conn._timing = TimingStats()
    cur = conn.cursor()
    assert type(cur) is expected
    assert cur._connection is conn and cur in conn._cursors
    assert cur._fetch_size == 37 and cur._timing is conn._timing
    conn._connected = False
    try:
        conn.cursor()
    except InterfaceError as error:
        assert str(error) == 'connection is closed'
    else:
        raise AssertionError('closed connection constructed a cursor')
"""
    result = subprocess.run(
        [sys.executable, "-c", code, first],
        cwd=Path(__file__).resolve().parents[1],
        capture_output=True,
        text=True,
        check=False,
        timeout=10,
    )
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.parametrize(
    "code, expected, sqlstate",
    [(-493, ProgrammingError, "42000"), (-9999, DatabaseError, "HY000")],
)
def test_shared_batch_error_preserves_known_and_unknown_metadata(
    code: int, expected: type[DatabaseError], sqlstate: str
) -> None:
    with pytest.raises(expected) as caught:
        _raise_batch_error({"code": code, "message": "batch failure"})
    assert type(caught.value) is expected
    assert caught.value.msg == "batch failure"
    assert caught.value.code == caught.value.errno == code
    assert caught.value.sqlstate == sqlstate
