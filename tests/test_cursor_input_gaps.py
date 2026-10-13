"""Input handling gaps found in review (#898).

- A UTC offset that is not a whole number of minutes raises ``DataError``
  instead of being truncated to ``+HH:MM``, in the literal and prepared paths.
- A verb followed directly by a comment (``INSERT/*x*/INTO``) is detected as
  the verb, so ``executemany()`` batches it and ``prepared=True`` accepts it.
- ``Lob.write()`` accepts only bytes-like data and skips I/O for any empty value.
"""

from __future__ import annotations

import array
import datetime
from typing import Any
from unittest.mock import MagicMock

import pytest

from pycubrid._cursor_common import (
    check_prepared_batch_operation,
    extract_first_keyword,
    format_parameter,
)
from pycubrid._prepared_binding import _encode_prepared_scalar
from pycubrid.constants import CUBRIDDataType
from pycubrid.exceptions import DataError
from pycubrid.lob import Lob
from pycubrid.protocol import LOBWritePacket

from .test_batch_row_errors import _BatchScript
from .test_deferred_close import _Driver
from .test_execute_array import _functions, _run

ASYNC = pytest.mark.parametrize("asynchronous", [False, True], ids=["sync", "async"])


def _aware(offset: datetime.timedelta) -> datetime.datetime:
    return datetime.datetime(2026, 1, 15, 10, 30, tzinfo=datetime.timezone(offset))


SUB_MINUTE = [
    datetime.timedelta(hours=5, minutes=30, seconds=45),
    datetime.timedelta(seconds=-1),
    datetime.timedelta(microseconds=-1),
    datetime.timedelta(microseconds=1),
    -datetime.timedelta(hours=23, minutes=59, seconds=59, microseconds=999999),
]
SUB_MINUTE_IDS = ["+05:30:45", "-1s", "-1us", "+1us", "-23:59:59.999999"]


# --- sub-minute UTC offsets ----------------------------------------------------


@pytest.mark.parametrize("offset", SUB_MINUTE, ids=SUB_MINUTE_IDS)
@pytest.mark.parametrize("no_backslash_escapes", [True, False])
def test_literal_rejects_sub_minute_offset(
    offset: datetime.timedelta, no_backslash_escapes: bool
) -> None:
    with pytest.raises(DataError, match="whole number of minutes"):
        format_parameter(_aware(offset), no_backslash_escapes=no_backslash_escapes)


@pytest.mark.parametrize("offset", SUB_MINUTE, ids=SUB_MINUTE_IDS)
def test_prepared_datetimetz_rejects_sub_minute_offset(offset: datetime.timedelta) -> None:
    with pytest.raises(DataError, match="whole number of minutes"):
        _encode_prepared_scalar(_aware(offset))


@pytest.mark.parametrize(
    ("offset", "text"),
    [
        (datetime.timedelta(0), "+00:00"),
        (datetime.timedelta(hours=5, minutes=30), "+05:30"),
        (-datetime.timedelta(hours=3, minutes=30), "-03:30"),
        (datetime.timedelta(hours=23, minutes=59), "+23:59"),
        (-datetime.timedelta(hours=23, minutes=59), "-23:59"),
    ],
)
def test_whole_minute_offsets_are_unchanged(offset: datetime.timedelta, text: str) -> None:
    literal = format_parameter(_aware(offset), no_backslash_escapes=True)
    assert literal == f"DATETIMETZ'2026-01-15 10:30:00.000 {text}'"


@ASYNC
def test_cursor_rejects_sub_minute_offset_before_sending(asynchronous: bool) -> None:
    value = _aware(datetime.timedelta(hours=5, minutes=30, seconds=45))

    def steps(d: _Driver) -> Any:
        cursor = d.cursor()
        with pytest.raises(DataError, match="whole number of minutes"):
            d.run(cursor.execute("INSERT INTO t (a) VALUES (?)", (value,)))

    requests, _ = _run(asynchronous, steps)
    assert _functions(requests) == []


# --- verb followed by a comment ------------------------------------------------


@pytest.mark.parametrize(
    ("sql", "keyword"),
    [
        ("INSERT/*x*/INTO t VALUES (1)", "INSERT"),
        ("insert--c\nINTO t VALUES (1)", "INSERT"),
        ("UPDATE//c\nt SET a = 1", "UPDATE"),
        ("/* hint */ DELETE/*x*/FROM t", "DELETE"),
        ("MERGE/**/INTO t USING s ON (1=1)", "MERGE"),
        ("INSERT INTO t VALUES (1)", "INSERT"),
        ("SELECT 1", "SELECT"),
        ("  -- only a comment", ""),
    ],
)
def test_first_keyword_stops_at_a_comment(sql: str, keyword: str) -> None:
    assert extract_first_keyword(sql) == keyword


def test_prepared_batch_accepts_verb_followed_by_comment() -> None:
    sql = "INSERT/*x*/INTO t (a, b) VALUES (?, ?)"
    assert check_prepared_batch_operation(sql) == sql


@ASYNC
def test_prepared_executemany_runs_verb_followed_by_comment(asynchronous: bool) -> None:
    sql = "INSERT/*x*/INTO t (a, b) VALUES (?, ?)"

    def steps(d: _Driver) -> Any:
        cursor = d.cursor()
        d.run(cursor.executemany(sql, [(1, "a"), (2, "b")], prepared=True))
        return cursor.rowcount

    requests, rowcount = _run(asynchronous, steps)
    assert _functions(requests) == ["PREPARE", "EXECUTE_ARRAY"]
    assert rowcount == 2


@ASYNC
def test_executemany_batches_verb_followed_by_comment(asynchronous: bool) -> None:
    sql = "INSERT/*x*/INTO t (a) VALUES (?)"

    def steps(d: _Driver) -> Any:
        cursor = d.cursor()
        d.run(cursor.executemany(sql, [(1,), (2,), (3,)]))
        return cursor.rowcount

    requests, rowcount = _run(asynchronous, steps, script=_BatchScript({}))
    assert _functions(requests) == ["EXECUTE_BATCH"]
    assert rowcount == 3


# --- Lob.write() input ---------------------------------------------------------


@pytest.fixture
def connection() -> MagicMock:
    conn = MagicMock()

    def send(packet: LOBWritePacket) -> None:
        packet.write(b"\x00" * 4)  # real argument serialization
        packet.bytes_written = len(packet.data)

    conn._send_and_receive = MagicMock(side_effect=send)
    return conn


def test_lob_write_counts_bytes_of_a_multibyte_memoryview(connection: MagicMock) -> None:
    lob = Lob(connection, CUBRIDDataType.BLOB, b"handle")
    data = memoryview(array.array("i", [1, 2]))
    written = lob.write(data)
    assert written == data.nbytes
    packet = connection._send_and_receive.call_args.args[0]
    assert packet.data == data.tobytes()


def test_lob_write_does_not_call_a_subclass_bytes_method(connection: MagicMock) -> None:
    class Sneaky(bytearray):
        def __bytes__(self) -> bytes:
            raise AssertionError("__bytes__ must not be called")

    lob = Lob(connection, CUBRIDDataType.BLOB, b"handle")
    written = lob.write(Sneaky(b"abc"))
    assert written == 3
    assert connection._send_and_receive.call_args.args[0].data == b"abc"
