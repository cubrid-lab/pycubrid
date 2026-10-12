"""Per-row outcome of a failed ``executemany()`` batch (#841).

The server runs every statement of a batch and reports one result per
statement, in order. The raised exception keeps its class, message, code,
errno and sqlstate, and gains ``batch_index``, ``batch_errors`` and
``batch_rowcounts``; ``rowcount`` stays ``-1``. The driver tests run the sync
and async cursors against the replay broker for the literal batch
(``EXECUTE_BATCH``) and the prepared batch (``EXECUTE_ARRAY``).
"""

from __future__ import annotations

import copy
import gc
import pickle
import weakref
from typing import Any

import pytest

from pycubrid._cursor_common import _batch_exception, _batch_failure
from pycubrid.constants import CUBRIDStatementType
from pycubrid.exceptions import (
    DatabaseError,
    DataError,
    Error,
    IntegrityError,
    InterfaceError,
    OperationalError,
    ProgrammingError,
)
from pycubrid.protocol import BatchExecutePacket

from .helpers.replay_broker import IN_TRAN, Reply, Request, Session, cas_info
from .test_deferred_close import _Driver
from .test_execute_array import INSERT, _CasScript, _error_row, _int, _ok_row, _run

ASYNC = pytest.mark.parametrize("asynchronous", [False, True], ids=["sync", "async"])
CAS_INFO = b"\x00\xff\xff\xff"
_STMT = bytes((CUBRIDStatementType.INSERT,))


def _batch_reply(*rows: bytes, executed: int | None = None, shard: bool = True) -> bytes:
    body = CAS_INFO + _int(0) + _int(len(rows) if executed is None else executed)
    body += b"".join(rows)
    if shard:
        body += _int(0)
    return body


def _batch_ok(count: int) -> bytes:
    return _STMT + _ok_row(count)


def _batch_error(code: int, message: str, *, v3: bool = True) -> bytes:
    return _STMT + _error_row(code, message, v3=v3)


class _BatchScript(_CasScript):
    """Adds per-statement failures to the ``EXECUTE_BATCH`` reply."""

    def __init__(self, batch_failures: dict[int, int], **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.batch_failures = batch_failures
        self.batch_reply: Reply | None = None

    def __call__(self, request: Request, session: Session) -> Reply | None:
        if request.function != "EXECUTE_BATCH":
            return super().__call__(request, session)
        if self.batch_reply is not None:
            return self.batch_reply
        # args: autocommit, timeout, then one SQL text per statement
        rows = []
        for index in range(len(request.args) - 2):
            code = self.batch_failures.get(index)
            rows.append(_batch_ok(1) if code is None else _batch_error(code, f"row {index}"))
        session.status = IN_TRAN
        return Reply(cas_info(IN_TRAN) + _int(0) + _int(len(rows)) + b"".join(rows) + _int(0))


# ---------------------------------------------------------------------------
# BatchExecutePacket reply
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("protocol_version", [2, 8])
def test_batch_reply_keeps_statement_order(protocol_version: int) -> None:
    v3 = protocol_version > 2
    packet = BatchExecutePacket(["s"] * 4, protocol_version=protocol_version)
    packet.parse(
        _batch_reply(
            _batch_ok(1),
            _batch_error(-670, "dup", v3=v3),
            _batch_ok(2),
            _batch_error(-494, "bad", v3=v3),
            shard=protocol_version > 4,
        )
    )
    insert = CUBRIDStatementType.INSERT
    assert packet.results == [(insert, 1), (insert, 2)]
    assert packet.errors == [
        {"code": -670, "message": "dup", "index": 1},
        {"code": -494, "message": "bad", "index": 3},
    ]
    assert packet.row_results == ((1, 0, ""), (-1, -670, "dup"), (2, 0, ""), (-1, -494, "bad"))


def test_batch_reply_starts_empty() -> None:
    assert BatchExecutePacket(["s"]).row_results == ()


@pytest.mark.parametrize("executed", [-1, 3])
def test_batch_reply_rejects_an_impossible_statement_count(executed: int) -> None:
    packet = BatchExecutePacket(["s"] * 2)
    with pytest.raises(ValueError, match="invalid batch execute statement count"):
        packet.parse(_batch_reply(_batch_ok(1), _batch_ok(1), executed=executed))


def test_batch_reply_may_stop_after_an_interrupted_statement() -> None:
    # CAS ux_execute_batch rewrites the count and stops after ER_INTERRUPTED.
    packet = BatchExecutePacket(["s"] * 4)
    packet.parse(_batch_reply(_batch_error(-670, "dup"), _batch_ok(1), _batch_error(-4, "stop")))
    assert packet.row_results == ((-1, -670, "dup"), (1, 0, ""), (-1, -4, "stop"))
    assert [error["index"] for error in packet.errors] == [0, 2]


@pytest.mark.parametrize(
    "rows",
    [(), (_batch_ok(1),), (_batch_error(-670, "dup"),)],
    ids=["empty", "success", "other-error"],
)
def test_batch_reply_rejects_any_other_short_reply(rows: tuple[bytes, ...]) -> None:
    packet = BatchExecutePacket(["s"] * 2)
    with pytest.raises(ValueError, match="fewer statements than sent"):
        packet.parse(_batch_reply(*rows))


# ---------------------------------------------------------------------------
# Exception attributes
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "exc",
    [
        DatabaseError(),
        IntegrityError("dup", code=-670, errno=-670, sqlstate="23000"),
        OperationalError("lost"),
        _batch_exception({"code": -670, "message": "dup"}),
    ],
)
def test_other_exceptions_carry_no_batch_details(exc: DatabaseError) -> None:
    assert exc.batch_index is None
    assert exc.batch_errors is None
    assert exc.batch_rowcounts is None


def test_interface_error_is_not_a_batch_error() -> None:
    assert not hasattr(InterfaceError("x"), "batch_errors")
    assert not hasattr(Error("x"), "batch_errors")


ROWS = ((1, 0, ""), (-1, -670, "dup"), (1, 0, ""), (-1, -494, "bad"))
ERRORS = [
    {"code": -670, "message": "dup", "index": 1},
    {"code": -494, "message": "bad", "index": 3},
]


def test_raised_error_matches_the_single_error_mapping() -> None:
    exc = _batch_failure(ROWS, ERRORS, ERRORS[0])
    plain = _batch_exception({"code": -670, "message": "dup"})
    assert type(exc) is type(plain) is IntegrityError
    assert (exc.msg, exc.code, exc.errno, exc.sqlstate) == (
        plain.msg,
        plain.code,
        plain.errno,
        plain.sqlstate,
    )
    assert str(exc) == str(plain)
    assert repr(exc) == repr(plain)
    assert exc.args == plain.args


def test_raised_error_lists_every_failed_row() -> None:
    exc = _batch_failure(ROWS, ERRORS, ERRORS[0])
    assert exc.batch_index == 1
    assert exc.batch_rowcounts == (1, -1, 1, -1)
    assert exc.batch_errors is not None
    assert [index for index, _error in exc.batch_errors] == [1, 3]
    first, second = (error for _index, error in exc.batch_errors)
    assert type(first) is IntegrityError and first.errno == -670 and first.sqlstate == "23000"
    assert type(second) is ProgrammingError and second.errno == -494
    assert second.sqlstate == "42000"
    # Each entry is its own exception: it names its row and has no nested details.
    assert (first.batch_index, second.batch_index) == (1, 3)
    assert first.batch_errors is None and first.batch_rowcounts is None
    assert all(error is not exc for _index, error in exc.batch_errors)


def test_raised_error_is_freed_without_the_cycle_collector() -> None:
    enabled = gc.isenabled()
    gc.disable()
    try:
        try:
            raise _batch_failure(ROWS, ERRORS, ERRORS[0])
        except DatabaseError as exc:
            assert exc.batch_errors is not None
            ref = weakref.ref(exc)
            entry = weakref.ref(exc.batch_errors[0][1])
        assert ref() is None
        assert entry() is None
    finally:
        if enabled:
            gc.enable()


def test_batch_details_survive_pickle_and_copy() -> None:
    exc = _batch_failure(ROWS, ERRORS, ERRORS[0])
    for clone in (pickle.loads(pickle.dumps(exc)), copy.copy(exc), copy.deepcopy(exc)):
        assert type(clone) is IntegrityError
        assert str(clone) == str(exc)
        assert clone.batch_index == 1
        assert clone.batch_rowcounts == (1, -1, 1, -1)
        assert clone.batch_errors is not None
        assert [(i, type(e), e.errno) for i, e in clone.batch_errors] == [
            (1, IntegrityError, -670),
            (3, ProgrammingError, -494),
        ]


# ---------------------------------------------------------------------------
# Cursors against the replay broker
# ---------------------------------------------------------------------------


@ASYNC
@pytest.mark.parametrize("prepared", [False, True], ids=["literal", "prepared"])
def test_partial_failure_reports_every_failed_row(asynchronous: bool, prepared: bool) -> None:
    failures = {1: -670, 3: -494}

    def steps(d: _Driver) -> Any:
        cursor = d.cursor()
        with pytest.raises(IntegrityError) as raised:
            d.run(cursor.executemany(INSERT, [(i, "v") for i in range(5)], prepared=prepared))
        assert cursor.rowcount == -1
        return raised.value

    script = _BatchScript(failures, failing_rows=failures)
    exc = _run(asynchronous, steps, script=script)[1]
    assert exc.errno == -670
    assert exc.sqlstate == "23000"
    assert exc.batch_index == 1
    assert [(i, type(e), e.errno) for i, e in exc.batch_errors] == [
        (1, IntegrityError, -670),
        (3, ProgrammingError, -494),
    ]
    assert exc.batch_rowcounts == (1, -1, 1, -1, 1)


@ASYNC
def test_executemany_batch_reports_every_failed_statement(asynchronous: bool) -> None:
    def steps(d: _Driver) -> Any:
        cursor = d.cursor()
        with pytest.raises(DataError) as raised:
            d.run(cursor.executemany_batch(["INSERT INTO t VALUES (1)"] * 3))
        assert cursor.rowcount == -1
        return raised.value

    # -8 (type conversion error) maps to DataError.
    exc = _run(asynchronous, steps, script=_BatchScript({0: -8, 2: -670}))[1]
    assert type(exc) is DataError
    assert exc.batch_index == 0
    assert [(i, type(e)) for i, e in exc.batch_errors] == [(0, DataError), (2, IntegrityError)]
    assert exc.batch_rowcounts == (-1, 1, -1)


@ASYNC
def test_successful_batch_is_unchanged(asynchronous: bool) -> None:
    def steps(d: _Driver) -> Any:
        cursor = d.cursor()
        results = d.run(cursor.executemany_batch(["INSERT INTO t VALUES (1)"] * 2))
        return results, cursor.rowcount

    results, rowcount = _run(asynchronous, steps, script=_BatchScript({}))[1]
    assert results == [(CUBRIDStatementType.INSERT, 1)] * 2
    assert rowcount == 2


@ASYNC
def test_interrupted_prepared_batch_names_the_interrupted_row(asynchronous: bool) -> None:
    def steps(d: _Driver) -> Any:
        cursor = d.cursor()
        with pytest.raises(DatabaseError) as raised:
            d.run(cursor.executemany(INSERT, [(i, "v") for i in range(5)], prepared=True))
        assert cursor.rowcount == -1
        return raised.value

    script = _CasScript(failing_rows={0: -670, 2: -4})
    exc = _run(asynchronous, steps, script=script)[1]
    assert exc.code == -4
    assert exc.batch_index == 2
    assert [(i, e.code) for i, e in exc.batch_errors] == [(0, -670), (2, -4)]
    # Three of five rows executed: rows 3 and 4 never ran.
    assert exc.batch_rowcounts == (-1, 1, -1)


@ASYNC
def test_interrupted_literal_batch_still_raises_the_first_failure(asynchronous: bool) -> None:
    def steps(d: _Driver) -> Any:
        cursor = d.cursor()
        with pytest.raises(IntegrityError) as raised:
            d.run(cursor.executemany(INSERT, [(i, "v") for i in range(5)]))
        assert cursor.rowcount == -1
        return raised.value

    script = _BatchScript({})
    script.batch_reply = Reply(
        cas_info(IN_TRAN)
        + _int(0)
        + _int(3)
        + _batch_error(-670, "dup")
        + _batch_ok(1)
        + _batch_error(-4, "interrupted")
        + _int(0)
    )
    exc = _run(asynchronous, steps, script=script)[1]
    # Unchanged literal behaviour: the first failing row is raised.
    assert exc.batch_index == 0
    assert [(i, e.code) for i, e in exc.batch_errors] == [(0, -670), (2, -4)]
    # Three of five statements executed: rows 3 and 4 never ran.
    assert exc.batch_rowcounts == (-1, 1, -1)


def test_unknown_code_falls_back_to_database_error() -> None:
    errors = [{"code": -99999, "message": "odd", "index": 0}]
    exc = _batch_failure(((-1, -99999, "odd"),), errors, errors[0])
    assert type(exc) is DatabaseError
    assert exc.batch_errors is not None
    assert type(exc.batch_errors[0][1]) is DatabaseError
    assert exc.sqlstate == "HY000"


@ASYNC
def test_impossible_statement_count_is_a_malformed_reply(asynchronous: bool) -> None:
    def steps(d: _Driver) -> Any:
        cursor = d.cursor()
        with pytest.raises(OperationalError):
            d.run(cursor.executemany_batch(["INSERT INTO t VALUES (1)"]))
        assert cursor.rowcount == -1

    script = _BatchScript({})
    script.batch_reply = Reply(cas_info(IN_TRAN) + _int(0) + _int(2) + _batch_ok(1) * 2 + _int(0))
    _run(asynchronous, steps, script=script)


def test_batch_reply_rejects_another_error_indicator_for_v3() -> None:
    # CAS ux_execute_batch writes DBMS_ERROR_INDICATOR (-2) before a V3+ error code.
    row = _STMT + _int(-1) + _int(-670) + _int(2) + b"x\x00"
    packet = BatchExecutePacket(["s"])
    with pytest.raises(ValueError, match="invalid batch execute error indicator"):
        packet.parse(_batch_reply(row))


@ASYNC
def test_interruption_on_the_last_row_is_still_raised(asynchronous: bool) -> None:
    # The reply covers every row, so only the last row's code shows the interruption.
    def steps(d: _Driver) -> Any:
        cursor = d.cursor()
        with pytest.raises(DatabaseError) as raised:
            d.run(cursor.executemany(INSERT, [(i, "v") for i in range(5)], prepared=True))
        assert cursor.rowcount == -1
        return raised.value

    exc = _run(asynchronous, steps, script=_CasScript(failing_rows={0: -670, 4: -4}))[1]
    assert exc.code == -4
    assert exc.batch_index == 4
    assert [(i, e.code) for i, e in exc.batch_errors] == [(0, -670), (4, -4)]
    assert exc.batch_rowcounts == (-1, 1, 1, 1, -1)


def test_execute_array_packet_reports_an_interrupted_reply() -> None:
    from pycubrid.protocol import ExecuteArrayPacket, _encode_prepared_scalar

    def packet() -> ExecuteArrayPacket:
        bound = tuple((_encode_prepared_scalar(i),) for i in range(2))
        return ExecuteArrayPacket(7, bound, auto_commit=False, bind_count=1)

    assert packet().interrupted is False
    full = packet()
    full.parse(_batch_reply(_ok_row(1), _error_row(-4, "stop")))
    assert full.interrupted is True
    failed = packet()
    failed.parse(_batch_reply(_error_row(-4, "stop"), _error_row(-670, "dup")))
    assert failed.interrupted is False
