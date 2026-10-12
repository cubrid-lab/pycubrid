"""Opt-in ``executemany(prepared=True)``: PREPARE + EXECUTE_ARRAY (#839).

The packet tests pin the FC21 request and reply layout of CAS
``fn_execute_array`` / ``ux_execute_array``. The driver tests run the sync and
async cursors against the replay broker, which answers ``PREPARE`` and
``EXECUTE_ARRAY`` as CAS does: every row is executed, a failing row only
reports its error, and an autocommitting pooling-off CAS frees its handles.
"""

from __future__ import annotations

import asyncio
import struct
import threading
import time
from collections.abc import Callable
from decimal import Decimal
from typing import Any

import pytest

from pycubrid._connection_common import ConnectionCommonMixin
from pycubrid._cursor_common import check_prepared_batch_operation
from pycubrid.constants import CASFunctionCode, CUBRIDDataType, CUBRIDStatementType
from pycubrid.cursor import Cursor
from pycubrid.exceptions import (
    DataError,
    IntegrityError,
    InterfaceError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
)
from pycubrid.lob import Lob
from pycubrid.protocol import (
    ExecuteArrayPacket,
    _encode_prepared_rows,
    _encode_prepared_scalar,
    _encode_prepared_value,
    _PreparedCollection,
    _PreparedLob,
)
from pycubrid.types import Multiset, Sequence, Set

from .helpers.replay_broker import (
    IN_TRAN,
    OUT_TRAN,
    Reply,
    Request,
    Result,
    Session,
    _single_int,
    cas_info,
    error_body,
    run_replay_broker,
)
from .test_deferred_close import _Driver

ASYNC = pytest.mark.parametrize("asynchronous", [False, True], ids=["sync", "async"])
CAS_INFO = b"\x00\xff\xff\xff"
INSERT = "INSERT INTO t (a, b) VALUES (?, ?)"
# A well-formed packed CLOB handle: db_type 34, size 5, locator with one NUL.
LOCATOR = b"ces_temp.0001\x00"
CLOB_HANDLE = struct.pack(">iqi", 34, 5, len(LOCATOR)) + LOCATOR


def _int(value: int) -> bytes:
    return struct.pack(">i", value)


def _arg(payload: bytes) -> bytes:
    return _int(len(payload)) + payload


def _ok_row(count: int) -> bytes:
    return _int(count) + b"\x00" * 8


def _error_row(code: int, message: str, *, v3: bool = True) -> bytes:
    text = message.encode() + b"\x00"
    head = _int(-2) + _int(code) if v3 else _int(code)  # DBMS_ERROR_INDICATOR
    return head + _int(len(text)) + text


def _reply(*rows: bytes, executed: int | None = None, shard: bool = True) -> bytes:
    body = CAS_INFO + _int(0) + _int(len(rows) if executed is None else executed)
    body += b"".join(rows)
    if shard:
        body += _int(0)
    return body


def _packet(rows: int = 2, protocol_version: int = 8) -> ExecuteArrayPacket:
    bound = tuple((_encode_prepared_scalar(i),) for i in range(rows))
    return ExecuteArrayPacket(
        7, bound, auto_commit=False, bind_count=1, protocol_version=protocol_version
    )


# ---------------------------------------------------------------------------
# ExecuteArrayPacket request
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("protocol_version", [3, 4, 8])
@pytest.mark.parametrize("auto_commit", [False, True])
def test_request_layout(protocol_version: int, auto_commit: bool) -> None:
    rows = (
        (_encode_prepared_scalar(1), _encode_prepared_scalar("a")),
        (_encode_prepared_scalar(2**40), _encode_prepared_scalar(None)),
    )
    packet = ExecuteArrayPacket(
        9, rows, auto_commit=auto_commit, bind_count=2, protocol_version=protocol_version
    )
    data = packet.write(CAS_INFO)
    payload = data[8:]
    expected = bytes((CASFunctionCode.EXECUTE_ARRAY,)) + _arg(_int(9))
    if protocol_version > 3:
        expected += _arg(_int(0))
    expected += _arg(b"\x01" if auto_commit else b"\x00")
    expected += _arg(bytes((CUBRIDDataType.INT,))) + _arg(_int(1))
    expected += _arg(bytes((CUBRIDDataType.CHAR,))) + _arg(b"a\x00")
    expected += _arg(bytes((CUBRIDDataType.BIGINT,))) + _arg(struct.pack(">q", 2**40))
    expected += _arg(bytes((CUBRIDDataType.NULL,))) + _arg(b"")
    assert payload == expected
    assert struct.unpack(">i", data[:4])[0] == len(payload)
    assert data[4:8] == CAS_INFO


def test_request_writes_collections_and_lob_handles() -> None:
    collection = _encode_prepared_value(Set([1, "x", None]))
    lob = _PreparedLob(CUBRIDDataType.CLOB, CLOB_HANDLE, object(), 1)
    packet = ExecuteArrayPacket(1, ((collection, lob),), auto_commit=False, bind_count=2)
    payload = packet.write(CAS_INFO)[8:]
    assert payload.endswith(
        _arg(bytes((CUBRIDDataType.SET,)))
        + _arg(collection.payload)
        + _arg(bytes((CUBRIDDataType.CLOB,)))
        + _arg(CLOB_HANDLE)
    )


@pytest.mark.parametrize(
    ("change", "message"),
    [
        ({"auto_commit": 1}, "autocommit must be a boolean"),
        ({"bind_count": True}, "bind count must be a positive integer"),
        ({"bind_count": 0}, "bind count must be a positive integer"),
        ({"bind_count": 2}, "does not match server bind count"),
        ({"rows": ()}, "at least one row"),
        ({"rows": ((object(),),)}, "invalid prepared parameter encoding"),
    ],
)
def test_request_validation(change: dict[str, Any], message: str) -> None:
    packet = _packet()
    for name, value in change.items():
        setattr(packet, name, value)
    with pytest.raises(ProgrammingError, match=message):
        packet.write(CAS_INFO)


def test_request_rejects_text_encoded_for_another_charset() -> None:
    rows = ((_encode_prepared_scalar("a", "euc-kr"),),)
    packet = ExecuteArrayPacket(1, rows, auto_commit=False, bind_count=1)
    with pytest.raises(ProgrammingError, match="different charset"):
        packet.write(CAS_INFO)


# ---------------------------------------------------------------------------
# ExecuteArrayPacket reply
# ---------------------------------------------------------------------------


def test_reply_keeps_per_row_results_in_order() -> None:
    packet = _packet(rows=3)
    packet.parse(_reply(_ok_row(1), _error_row(-670, "duplicate"), _ok_row(2)))
    assert packet.row_results == ((1, 0, ""), (-1, -670, "duplicate"), (2, 0, ""))
    assert packet.errors == [{"code": -670, "message": "duplicate", "index": 1}]


def test_reply_before_protocol_v3_carries_the_code_as_result() -> None:
    packet = _packet(rows=2, protocol_version=2)
    packet.parse(_reply(_error_row(-494, "coerce", v3=False), _ok_row(1), shard=False))
    assert packet.row_results == ((-1, -494, "coerce"), (1, 0, ""))


def test_reply_without_shard_id_before_protocol_v5() -> None:
    packet = _packet(rows=1, protocol_version=4)
    packet.parse(_reply(_ok_row(3), shard=False))
    assert packet.row_results == ((3, 0, ""),)


def test_interrupted_reply_may_stop_after_the_interrupted_row() -> None:
    # ux_execute_array breaks its loop only after ER_INTERRUPTED (-4).
    packet = _packet(rows=3)
    packet.parse(_reply(_ok_row(1), _error_row(-4, "interrupted")))
    assert packet.row_results == ((1, 0, ""), (-1, -4, "interrupted"))


@pytest.mark.parametrize(
    "body",
    [
        _reply(_ok_row(1)),  # fewer rows than sent, without an error
        _reply(executed=0),
        _reply(_error_row(-670, "duplicate")),  # stops early after a non-interrupt error
        _reply(_ok_row(1), _ok_row(1), _ok_row(1)),  # more rows than sent
        _reply(executed=-1),
        # V3+ failures carry DBMS_ERROR_INDICATOR (-2), not CAS_ERROR_INDICATOR (-1)
        _reply(_int(-1) + _int(-670) + _int(2) + b"x\x00", _ok_row(1)),
        CAS_INFO + _int(0) + _int(1) + _int(-1) + _int(-670) + _int(99) + b"x",
    ],
)
def test_malformed_reply_raises_value_error(body: bytes) -> None:
    with pytest.raises(ValueError):
        _packet(rows=2).parse(body)


def test_whole_request_error_raises_mapped_error() -> None:
    body = CAS_INFO + _int(-1) + _int(-1009) + b"no such handle\x00"
    with pytest.raises(Exception, match="no such handle"):
        _packet().parse(body)


def test_skip_after_reconnect_refuses_execute_array() -> None:
    with pytest.raises(OperationalError, match="prepared batch handle lost"):
        ConnectionCommonMixin._skip_request_after_reconnect(_packet())


# ---------------------------------------------------------------------------
# Value and row encoding
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("cls", "code"),
    [
        (Set, CUBRIDDataType.SET),
        (Multiset, CUBRIDDataType.SEQUENCE),  # the broker rejects MULTISET
        (Sequence, CUBRIDDataType.SEQUENCE),
    ],
)
def test_collections_are_string_elements(cls: Any, code: int) -> None:
    binding = _encode_prepared_value(cls([1, "a", None, 2**40]))
    assert isinstance(binding, _PreparedCollection)
    assert binding.type_code == code
    assert binding.element_type == CUBRIDDataType.STRING
    assert binding.elements == (b"1\x00", b"a\x00", None, b"1099511627776\x00")


def test_empty_collection_is_allowed() -> None:
    binding = _encode_prepared_value(Set([]))
    assert isinstance(binding, _PreparedCollection)
    assert binding.elements == ()


@pytest.mark.parametrize(
    ("value", "error", "message"),
    [
        (Set([Set([1])]), ProgrammingError, "nested collection"),
        (Set([True]), ProgrammingError, "unsupported prepared collection element"),
        (Set([1.5]), ProgrammingError, "unsupported prepared collection element"),
        (Set([10**38]), DataError, "more than 38 digits"),
        (Set(["a\x00"]), ProgrammingError, "contains NUL"),
        ([1, 2], ProgrammingError, "cannot bind a list"),
        ({1}, ProgrammingError, "cannot bind a list"),
        (object(), ProgrammingError, "unsupported prepared parameter type"),
    ],
)
def test_value_rejections(value: Any, error: type[Exception], message: str) -> None:
    with pytest.raises(error, match=message):
        _encode_prepared_value(value)


def test_forged_collection_is_rejected() -> None:
    forged = object.__new__(Set)
    with pytest.raises(ProgrammingError, match="missing its elements"):
        _encode_prepared_value(forged)


def test_collection_with_non_tuple_elements_is_rejected() -> None:
    forged = Set([1])
    object.__setattr__(forged, "_elements", [1])
    with pytest.raises(ProgrammingError, match="elements must be a tuple"):
        _encode_prepared_value(forged)


def test_scalars_use_the_840_encoder() -> None:
    assert _encode_prepared_value(Decimal("1.5")) == _encode_prepared_scalar(Decimal("1.5"))


def test_rows_allow_mixed_types_per_column() -> None:
    rows = _encode_prepared_rows([(1,), (2**40,), (Decimal("1.5"),), (None,)], "utf-8")
    assert [row[0].type_code for row in rows] == [
        CUBRIDDataType.INT,
        CUBRIDDataType.BIGINT,
        CUBRIDDataType.NUMERIC,
        CUBRIDDataType.NULL,
    ]


def test_rows_accept_an_empty_sequence() -> None:
    assert _encode_prepared_rows([], "utf-8") == ()


@pytest.mark.parametrize(
    ("rows", "error", "message"),
    [
        ("ab", ProgrammingError, "iterable of parameter sequences"),
        (5, ProgrammingError, "iterable of parameter sequences"),
        ([5], ProgrammingError, r"rows\[0\]: parameters must be a sequence"),
        (["ab"], ProgrammingError, r"rows\[0\]: parameters must be a sequence"),
        ([()], NotSupportedError, "at least one \\? parameter marker"),
        ([(1, 2), (1,)], ProgrammingError, r"rows\[1\]: wrong number of parameters"),
        ([(1, 2), (3, float("nan"))], ProgrammingError, r"rows\[1\]\[1\]: nan and inf"),
        ([(b"",)], NotSupportedError, r"rows\[0\]\[0\]: an empty bytes value"),
    ],
)
def test_row_rejections_name_the_position(rows: Any, error: type[Exception], message: str) -> None:
    with pytest.raises(error, match=message):
        _encode_prepared_rows(rows, "utf-8")


def test_row_errors_keep_their_code() -> None:
    def hook(value: Any) -> Any:
        raise DataError("boom", code=-5)

    with pytest.raises(DataError, match=r"rows\[0\]\[0\]: boom") as raised:
        _encode_prepared_rows([(1,)], "utf-8", hook)
    assert raised.value.code == -5


def test_rows_try_the_cursor_hook_first() -> None:
    marker = _encode_prepared_scalar(42)

    def hook(value: Any) -> Any:
        return marker if value == "lob" else None

    rows = _encode_prepared_rows([("lob", 1)], "utf-8", hook)
    assert rows == ((marker, _encode_prepared_scalar(1)),)


@pytest.mark.parametrize(
    ("operation", "error", "message"),
    [
        (b"INSERT", ProgrammingError, "string without NUL"),
        ("INSERT INTO t VALUES (?)\x00", ProgrammingError, "string without NUL"),
        ("SELECT ?", NotSupportedError, "only INSERT, UPDATE, DELETE and MERGE"),
        ("WITH x AS (SELECT 1) INSERT INTO t SELECT * FROM x", NotSupportedError, "only"),
        ("CALL p(?)", NotSupportedError, "only"),
    ],
)
def test_operation_rejections(operation: Any, error: type[Exception], message: str) -> None:
    with pytest.raises(error, match=message):
        check_prepared_batch_operation(operation)


def test_operation_subclass_is_copied() -> None:
    class Sneaky(str):
        def __str__(self) -> str:
            return "SELECT 1"

    sql = check_prepared_batch_operation(Sneaky("/* c */ merge INTO t USING db_root ON (1=1)"))
    assert type(sql) is str
    assert sql.startswith("/* c */ merge")


# ---------------------------------------------------------------------------
# Driver behaviour against the replay broker
# ---------------------------------------------------------------------------


def _prepare_body(status: int, handle: int, statement_type: int, bind_count: int) -> bytes:
    # handle, cache lifetime, statement type, bind count, updatable, column count
    return cas_info(status) + struct.pack(">iibibi", handle, 0, statement_type, bind_count, 0, 0)


def _array_rows(request: Request, bind_count: int) -> int:
    # args: handle, timeout, autocommit, then one (type, value) pair per value
    return (len(request.args) - 3) // 2 // bind_count


class _CasScript:
    """PREPARE and EXECUTE_ARRAY replies of a CUBRID CAS."""

    def __init__(
        self,
        *,
        bind_count: int = 2,
        statement_type: int = CUBRIDStatementType.INSERT,
        failing_rows: dict[int, int] | None = None,
        prepare_error: bool = False,
        array_error: bool = False,
        pooling: int = 0,
        array_reply: Reply | None = None,
        prepare_delay: float = 0.0,
    ) -> None:
        # Set by a test: the next CHECK_CAS finds the CAS gone (#485).
        self.close_on_probe = False
        self.array_reply = array_reply
        self.prepare_delay = prepare_delay
        self.bind_count = bind_count
        self.statement_type = statement_type
        self.failing_rows = failing_rows or {}
        self.prepare_error = prepare_error
        self.array_error = array_error
        self.pooling = pooling

    def __call__(self, request: Request, session: Session) -> Reply | None:
        if request.function == "CHECK_CAS" and self.close_on_probe:
            self.close_on_probe = False
            return Reply(close=True)
        if request.function == "PREPARE":
            # args: SQL, flag, autocommit, then deferred-close handle ids
            for arg in request.args[3:]:
                session.results.pop(struct.unpack(">i", arg)[0], None)
            time.sleep(self.prepare_delay)
            if self.prepare_error:
                return Reply(error_body(session.status, -493, "Syntax: unexpected end"))
            handle = session.allocate_handle()
            session.results[handle] = Result(_single_int("v", 1), 0)
            return Reply(
                _prepare_body(session.status, handle, self.statement_type, self.bind_count)
            )
        if request.function == "EXECUTE_BATCH":
            # args: autocommit, timeout, then one SQL text per statement
            count = len(request.args) - 2
            row = bytes((CUBRIDStatementType.INSERT,)) + _ok_row(1)
            return Reply(cas_info(IN_TRAN) + _int(0) + _int(count) + row * count + _int(0))
        if request.function != "EXECUTE_ARRAY":
            return None
        if self.array_error:
            return Reply(error_body(session.status, -1009, "invalid handle"))
        if self.array_reply is not None:
            return self.array_reply
        auto_commit = request.args[2] == b"\x01"
        rows = []
        for index in range(_array_rows(request, self.bind_count)):
            code = self.failing_rows.get(index)
            rows.append(_ok_row(1) if code is None else _error_row(code, "row failed"))
            if code == -4:
                break  # ux_execute_array stops after ER_INTERRUPTED
        if auto_commit:
            session.status = OUT_TRAN
            if not self.pooling:
                session.results.clear()  # CAS frees every handle at the commit
        else:
            session.status = IN_TRAN
        body = cas_info(session.status) + _int(0) + _int(len(rows)) + b"".join(rows) + _int(0)
        return Reply(body)


def _run(
    asynchronous: bool,
    steps: Callable[[_Driver], Any],
    *,
    script: _CasScript | None = None,
    autocommit: bool = False,
) -> tuple[list[Request], Any]:
    script = script or _CasScript()
    with run_replay_broker(script, statement_pooling=script.pooling) as broker:
        driver = _Driver(asynchronous, broker.port, autocommit)
        start = len(broker.requests)
        try:
            result = steps(driver)
        finally:
            requests = broker.requests[start:]
            driver.teardown()
    return requests, result


def _functions(requests: list[Request]) -> list[str]:
    return [r.function for r in requests if r.function not in ("CON_CLOSE", "CHECK_CAS")]


@ASYNC
@pytest.mark.parametrize("autocommit", [False, True])
def test_batch_sends_prepare_and_one_execute_array(asynchronous: bool, autocommit: bool) -> None:
    def steps(d: _Driver) -> Any:
        cursor = d.cursor()
        returned = d.run(
            cursor.executemany(INSERT, [(1, "a"), (2**40, None), ("x", 3)], prepared=True)
        )
        assert returned is cursor
        return cursor

    requests, cursor = _run(asynchronous, steps, autocommit=autocommit)
    assert _functions(requests) == ["PREPARE", "EXECUTE_ARRAY"]
    prepare, array = [r for r in requests if r.function in ("PREPARE", "EXECUTE_ARRAY")]
    assert prepare.args[0] == INSERT.encode() + b"\x00"
    assert prepare.args[2] == (b"\x01" if autocommit else b"\x00")
    assert array.args[2] == (b"\x01" if autocommit else b"\x00")
    assert _array_rows(array, 2) == 3
    assert cursor.rowcount == 3
    assert cursor.lastrowid is None
    assert cursor.description is None
    assert cursor._batch_row_results == ((1, 0, ""), (1, 0, ""), (1, 0, ""))


@ASYNC
def test_partial_failure_raises_first_error_and_keeps_row_order(asynchronous: bool) -> None:
    def steps(d: _Driver) -> Any:
        cursor = d.cursor()
        with pytest.raises(IntegrityError) as raised:
            d.run(cursor.executemany(INSERT, [(i, "v") for i in range(5)], prepared=True))
        assert raised.value.code == -670
        return cursor

    script = _CasScript(failing_rows={2: -670, 4: -494})
    _requests, cursor = _run(asynchronous, steps, script=script)
    assert cursor.rowcount == -1
    assert [code for _count, code, _message in cursor._batch_row_results] == [0, 0, -670, 0, -494]


@ASYNC
def test_default_path_is_unchanged(asynchronous: bool) -> None:
    def steps(d: _Driver) -> None:
        cursor = d.cursor()
        d.run(cursor.executemany(INSERT, [(1, "a")]))
        d.run(cursor.executemany(INSERT, [(1, "a")], prepared=False))

    requests, _ = _run(asynchronous, steps)
    assert _functions(requests) == ["EXECUTE_BATCH", "EXECUTE_BATCH"]


@ASYNC
@pytest.mark.parametrize(
    ("operation", "rows", "prepared", "error"),
    [
        (INSERT, [(1, "a")], 1, ProgrammingError),
        ("SELECT ?", [(1,)], True, NotSupportedError),
        ("SELECT ?", [], True, NotSupportedError),
        (INSERT, [(1, 1.0 * float("inf"))], True, ProgrammingError),
        (INSERT, [(1, [1])], True, ProgrammingError),
        ("INSERT INTO t VALUES (1)", [()], True, NotSupportedError),
        ("INSERT INTO t VALUES ('€', ?)", [(1,)], True, DataError),
    ],
)
def test_local_rejections_send_nothing(
    asynchronous: bool, operation: str, rows: Any, prepared: Any, error: type[Exception]
) -> None:
    def steps(d: _Driver) -> None:
        if operation.startswith("INSERT INTO t VALUES ('"):
            d.conn._encoding = "ascii"
        cursor = d.cursor()
        with pytest.raises(error):
            d.run(cursor.executemany(operation, rows, prepared=prepared))

    requests, _ = _run(asynchronous, steps)
    assert _functions(requests) == []


@ASYNC
def test_empty_batch_sends_nothing(asynchronous: bool) -> None:
    def steps(d: _Driver) -> Any:
        cursor = d.cursor()
        cursor._rowcount = 9
        d.run(cursor.executemany(INSERT, [], prepared=True))
        return cursor

    requests, cursor = _run(asynchronous, steps)
    assert _functions(requests) == []
    assert cursor.rowcount == 0
    assert cursor._batch_row_results is None


@ASYNC
@pytest.mark.parametrize(
    ("script", "error", "message"),
    [
        (_CasScript(bind_count=3), ProgrammingError, "wrong number of parameters"),
        (
            _CasScript(statement_type=CUBRIDStatementType.SELECT),
            NotSupportedError,
            "only INSERT",
        ),
        (_CasScript(statement_type=99), NotSupportedError, "only INSERT"),
        (
            _CasScript(statement_type=CUBRIDStatementType.CALL),
            NotSupportedError,
            "only INSERT",
        ),
        (_CasScript(array_error=True), Exception, "invalid handle"),
    ],
)
def test_handle_is_owned_after_a_failure_past_prepare(
    asynchronous: bool, script: _CasScript, error: type[Exception], message: str
) -> None:
    def steps(d: _Driver) -> Any:
        cursor = d.cursor()
        with pytest.raises(error, match=message):
            d.run(cursor.executemany(INSERT, [(1, "a")], prepared=True))
        handle = cursor._query_handle
        assert handle is not None
        assert cursor.rowcount == -1
        d.run(cursor.execute("SELECT 1"))
        return handle

    requests, handle = _run(asynchronous, steps, script=script)
    functions = _functions(requests)
    if script.array_error:
        assert functions[:2] == ["PREPARE", "EXECUTE_ARRAY"]
    else:
        assert functions[:1] == ["PREPARE"]
        assert "EXECUTE_ARRAY" not in functions
    # Manual-commit mode: the next statement closes the handle first.
    close = [r for r in requests if r.function == "CLOSE_REQ_HANDLE"]
    assert [r.int_arg(0) for r in close] == [handle]


@ASYNC
def test_prepare_error_owns_no_handle(asynchronous: bool) -> None:
    def steps(d: _Driver) -> Any:
        cursor = d.cursor()
        with pytest.raises(ProgrammingError, match="Syntax"):
            d.run(cursor.executemany(INSERT, [(1, "a")], prepared=True))
        return cursor

    requests, cursor = _run(asynchronous, steps, script=_CasScript(prepare_error=True))
    assert _functions(requests) == ["PREPARE"]
    assert cursor._query_handle is None
    assert cursor.rowcount == -1


@ASYNC
def test_pooling_off_autocommit_retires_the_handle(asynchronous: bool) -> None:
    def steps(d: _Driver) -> Any:
        cursor = d.cursor()
        d.run(cursor.executemany(INSERT, [(1, "a")], prepared=True))
        assert cursor._query_handle is None
        d.run(cursor.execute("SELECT 1"))

    requests, _ = _run(asynchronous, steps, autocommit=True)
    assert "CLOSE_REQ_HANDLE" not in _functions(requests)


@ASYNC
def test_pooling_on_autocommit_defers_the_close(asynchronous: bool) -> None:
    def steps(d: _Driver) -> Any:
        cursor = d.cursor()
        handles = []
        for _ in range(3):
            d.run(cursor.executemany(INSERT, [(1, "a")], prepared=True))
            handles.append(cursor._query_handle)
        d.run(cursor.execute("SELECT 1"))
        return handles

    requests, handles = _run(asynchronous, steps, script=_CasScript(pooling=1), autocommit=True)
    assert None not in handles
    assert "CLOSE_REQ_HANDLE" not in _functions(requests)
    # Each prepared batch's PREPARE carries the previous batch's handle, as FC41 does,
    # so repeated batches never queue more than one handle (#488).
    prepares = [r for r in requests if r.function == "PREPARE"]
    carried = [tuple(struct.unpack(">i", arg)[0] for arg in r.args[3:]) for r in prepares]
    assert carried == [(), (handles[0],), (handles[1],)]
    assert len(set(handles)) == 1  # the freed id is reused, as CAS allocates
    statement = [r for r in requests if r.function == "PREPARE_AND_EXECUTE"][-1]
    assert statement.deferred_closes == (handles[2],)


@ASYNC
def test_manual_commit_prepare_carries_no_handles(asynchronous: bool) -> None:
    def steps(d: _Driver) -> None:
        cursor = d.cursor()
        d.run(cursor.executemany(INSERT, [(1, "a")], prepared=True))
        d.run(cursor.executemany(INSERT, [(1, "a")], prepared=True))

    requests, _ = _run(asynchronous, steps, script=_CasScript(pooling=1))
    assert _functions(requests) == [
        "PREPARE",
        "EXECUTE_ARRAY",
        "CLOSE_REQ_HANDLE",
        "PREPARE",
        "EXECUTE_ARRAY",
    ]
    assert all(len(r.args) == 3 for r in requests if r.function == "PREPARE")


@ASYNC
def test_session_replaced_after_the_probe_sends_nothing(asynchronous: bool) -> None:
    def steps(d: _Driver) -> Any:
        cursor = d.cursor()
        original = d.conn._generation_for_binding

        if asynchronous:

            async def stale() -> int:
                return await original() - 1

        else:

            def stale() -> int:
                return original() - 1

        d.conn._generation_for_binding = stale
        with pytest.raises(OperationalError, match="CAS session replaced"):
            d.run(cursor.executemany(INSERT, [(1, "a")], prepared=True))
        return cursor

    requests, cursor = _run(asynchronous, steps)
    assert _functions(requests) == []
    assert cursor._query_handle is None
    assert cursor.rowcount == -1


@ASYNC
@pytest.mark.parametrize(
    "reply",
    [
        Reply(cas_info(IN_TRAN) + _int(0) + _int(5)),  # truncated
        Reply(close=True),  # transport closed before a reply
    ],
    ids=["malformed", "transport"],
)
def test_broken_array_reply_retires_the_session(asynchronous: bool, reply: Reply) -> None:
    def steps(d: _Driver) -> Any:
        cursor = d.cursor()
        with pytest.raises(OperationalError):
            d.run(cursor.executemany(INSERT, [(1, "a")], prepared=True))
        assert cursor._query_handle is None
        assert cursor.rowcount == -1
        assert cursor._batch_row_results is None
        return cursor

    _run(asynchronous, steps, script=_CasScript(array_reply=reply))


@ASYNC
@pytest.mark.parametrize(
    "statement_type",
    [
        CUBRIDStatementType.INSERT,
        CUBRIDStatementType.UPDATE,
        CUBRIDStatementType.DELETE,
        57,  # MERGE
    ],
)
def test_dml_statement_types_run(asynchronous: bool, statement_type: int) -> None:
    def steps(d: _Driver) -> Any:
        cursor = d.cursor()
        d.run(cursor.executemany(INSERT, [(1, "a")], prepared=True))
        return cursor.rowcount

    requests, rowcount = _run(asynchronous, steps, script=_CasScript(statement_type=statement_type))
    assert _functions(requests) == ["PREPARE", "EXECUTE_ARRAY"]
    assert rowcount == 1


@ASYNC
def test_interrupted_batch_raises_the_interruption(asynchronous: bool) -> None:
    def steps(d: _Driver) -> Any:
        cursor = d.cursor()
        with pytest.raises(Exception) as raised:
            d.run(cursor.executemany(INSERT, [(i, "v") for i in range(5)], prepared=True))
        assert getattr(raised.value, "code", None) == -4
        return cursor

    script = _CasScript(failing_rows={0: -670, 2: -4})
    _requests, cursor = _run(asynchronous, steps, script=script)
    assert cursor.rowcount == -1
    assert [code for _count, code, _m in cursor._batch_row_results] == [-670, 0, -4]


@ASYNC
def test_failed_probe_reconnects_before_prepare(asynchronous: bool) -> None:
    script = _CasScript()

    def steps(d: _Driver) -> Any:
        cursor = d.cursor()
        d.conn._cas_info = bytes((OUT_TRAN,)) + d.conn._cas_info[1:]
        d.conn._cas_reply_verified = False
        script.close_on_probe = True
        d.run(cursor.executemany(INSERT, [(1, "a")], prepared=True))
        assert not d.conn._deferred_closes
        return cursor.rowcount

    requests, rowcount = _run(asynchronous, steps, script=script)
    assert rowcount == 1
    sessions = {
        r.function: r.session for r in requests if r.function in ("PREPARE", "EXECUTE_ARRAY")
    }
    first = requests[0].session
    assert sessions["PREPARE"] == sessions["EXECUTE_ARRAY"] != first


@ASYNC
def test_replacement_between_probe_and_prepare_sends_nothing(asynchronous: bool) -> None:
    script = _CasScript()

    def steps(d: _Driver) -> Any:
        cursor = d.cursor()
        original = d.conn._generation_for_binding

        def lose_the_cas() -> None:
            # The CAS closes after an OUT_TRAN reply; the PREPARE preflight probes it.
            d.conn._cas_info = bytes((OUT_TRAN,)) + d.conn._cas_info[1:]
            d.conn._cas_reply_verified = False
            script.close_on_probe = True

        if asynchronous:

            async def probed() -> int:
                generation = await original()
                lose_the_cas()
                return generation

        else:

            def probed() -> int:
                generation = original()
                lose_the_cas()
                return generation

        d.conn._generation_for_binding = probed
        with pytest.raises(OperationalError, match="CAS session replaced"):
            d.run(cursor.executemany(INSERT, [(1, "a")], prepared=True))
        assert cursor._query_handle is None
        return cursor

    requests, _ = _run(asynchronous, steps, script=script)
    assert [r for r in requests if r.function in ("PREPARE", "EXECUTE_ARRAY")] == []


@ASYNC
def test_generator_rows_are_accepted(asynchronous: bool) -> None:
    def steps(d: _Driver) -> Any:
        cursor = d.cursor()
        d.run(cursor.executemany(INSERT, ((i, "v") for i in range(3)), prepared=True))
        return cursor.rowcount

    _requests, rowcount = _run(asynchronous, steps)
    assert rowcount == 3


@ASYNC
def test_open_schema_result_is_closed_before_an_autocommit_array(asynchronous: bool) -> None:
    def steps(d: _Driver) -> None:
        d.run(d.conn.get_schema_info(1, "t"))
        cursor = d.cursor()
        d.run(cursor.executemany(INSERT, [(1, "a")], prepared=True))
        assert d.conn._schema_results == {}

    requests, _ = _run(asynchronous, steps, autocommit=True)
    functions = _functions(requests)
    assert functions.index("CLOSE_REQ_HANDLE") < functions.index("PREPARE")


@ASYNC
def test_new_execute_clears_row_results(asynchronous: bool) -> None:
    def steps(d: _Driver) -> Any:
        cursor = d.cursor()
        d.run(cursor.executemany(INSERT, [(1, "a")], prepared=True))
        assert cursor._batch_row_results is not None
        d.run(cursor.execute("SELECT 1"))
        assert cursor._batch_row_results is None
        d.run(cursor.executemany(INSERT, [(1, "a")], prepared=True))
        d.run(cursor.executemany(INSERT, [(1, "a")]))
        assert cursor._batch_row_results is None
        d.run(cursor.executemany(INSERT, [(1, "a")], prepared=True))
        d.run(cursor.executemany(INSERT, []))
        assert cursor._batch_row_results is None

    _run(asynchronous, steps)


def test_async_prepare_and_array_hold_the_lock_together() -> None:
    def steps(d: _Driver) -> None:
        cursor = d.cursor()

        async def both() -> None:
            await asyncio.gather(
                cursor.executemany(INSERT, [(1, "a")], prepared=True),
                d.conn.commit(),
                d.conn.commit(),
            )

        d.run(both())

    requests, _ = _run(True, steps)
    functions = _functions(requests)
    assert functions[functions.index("PREPARE") + 1] == "EXECUTE_ARRAY"


def test_sync_prepare_and_array_hold_the_lock_together() -> None:
    def steps(d: _Driver) -> None:
        cursor = d.cursor()
        started = threading.Event()

        def commit_while_preparing() -> None:
            started.wait(5)
            d.conn.commit()

        thread = threading.Thread(target=commit_while_preparing)
        thread.start()
        original = d.conn._send_and_receive

        def send(packet: Any, **kwargs: Any) -> Any:
            if type(packet).__name__ == "PreparePacket":
                started.set()
            return original(packet, **kwargs)

        d.conn._send_and_receive = send
        cursor.executemany(INSERT, [(1, "a")], prepared=True)
        thread.join(5)

    requests, _ = _run(False, steps, script=_CasScript(prepare_delay=0.2))
    functions = _functions(requests)
    assert functions.index("EXECUTE_ARRAY") == functions.index("PREPARE") + 1
    assert "END_TRAN" in functions[functions.index("EXECUTE_ARRAY") :]


def test_async_cursor_rejects_lob_values() -> None:
    def steps(d: _Driver) -> None:
        cursor = d.cursor()
        sync_conn = type("C", (), {"_ensure_connected": lambda self: None})()
        sync_conn._send_and_receive = lambda packet: packet
        lob = Lob(sync_conn, CUBRIDDataType.CLOB, CLOB_HANDLE)
        with pytest.raises(NotSupportedError, match=r"rows\[0\]\[1\]: LOB values are not"):
            d.run(cursor.executemany(INSERT, [(1, lob)], prepared=True))

    requests, _ = _run(True, steps)
    assert _functions(requests) == []


# ---------------------------------------------------------------------------
# LOB values on the sync cursor
# ---------------------------------------------------------------------------


def test_lob_values_bind_with_autocommit_off() -> None:
    def steps(d: _Driver) -> Any:
        cursor = d.cursor()
        lob = Lob(d.conn, CUBRIDDataType.CLOB, CLOB_HANDLE)
        d.run(cursor.executemany(INSERT, [(1, lob), (2, lob)], prepared=True))
        return cursor.rowcount

    requests, rowcount = _run(False, steps)
    assert rowcount == 2
    array = [r for r in requests if r.function == "EXECUTE_ARRAY"][0]
    assert array.args[3:7] == (bytes((CUBRIDDataType.INT,)), _int(1), b"\x18", CLOB_HANDLE)


@pytest.mark.parametrize(
    "case", ["autocommit", "closed", "other", "stale", "forged", "untyped", "handle"]
)
def test_lob_rejections(case: str) -> None:
    def steps(d: _Driver) -> None:
        cursor = d.cursor()
        owner: Any = d.conn
        if case == "other":
            owner = type("C", (), {"_ensure_connected": lambda self: None})()
            owner._send_and_receive = lambda packet: packet
        lob = Lob(owner, CUBRIDDataType.CLOB, CLOB_HANDLE)
        if case == "forged":
            lob = Lob.__new__(Lob)  # never ran Lob.__init__()
        if case == "untyped":
            lob._generation = None
        if case == "handle":
            lob._lob_handle = None
        if case == "closed":
            lob.close()
        if case == "stale":
            lob._generation = d.conn._physical_generation + 1
        error, message = {
            "autocommit": (NotSupportedError, r"rows\[1\]\[1\]: LOB values require autocommit"),
            "closed": (InterfaceError, r"rows\[1\]\[1\]: LOB is closed"),
            "other": (InterfaceError, "another connection"),
            "stale": (InterfaceError, r"rows\[1\]\[1\]: LOB binding belongs to another physical"),
            "forged": (InterfaceError, r"rows\[1\]\[1\]: LOB is closed"),
            "untyped": (InterfaceError, r"rows\[1\]\[1\]: LOB binding belongs to another physical"),
            "handle": (InterfaceError, r"rows\[1\]\[1\]: invalid LOB handle"),
        }[case]
        with pytest.raises(error, match=message):
            cursor.executemany(INSERT, [(1, "a"), (2, lob)], prepared=True)

    requests, _ = _run(False, steps, autocommit=case == "autocommit")
    assert _functions(requests) == []


def test_lob_autocommit_is_checked_again_under_the_session_lock() -> None:
    # autocommit changed between encoding and the send (another thread).
    def steps(d: _Driver) -> None:
        cursor = d.cursor()
        lob = Lob(d.conn, CUBRIDDataType.CLOB, CLOB_HANDLE)
        original = d.conn._generation_for_binding

        def flip() -> int:
            generation = original()
            d.conn._autocommit = True
            return generation

        d.conn._generation_for_binding = flip
        with pytest.raises(NotSupportedError, match="autocommit off"):
            cursor.executemany(INSERT, [(1, lob)], prepared=True)

    requests, _ = _run(False, steps)
    assert "PREPARE" not in _functions(requests)


def test_cursor_type_is_exported() -> None:
    assert Cursor.executemany.__kwdefaults__ == {"prepared": False}
