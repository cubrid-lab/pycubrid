"""Pooling-off transaction replies retire IDs before CAS can reuse them (#584)."""

from __future__ import annotations

import struct
from typing import Any

import pytest

from pycubrid.constants import CUBRIDDataType as T
from pycubrid.constants import CUBRIDStatementType
from pycubrid.exceptions import DataError, DatabaseError, InterfaceError
from pycubrid.protocol import PrepareAndExecutePacket, PreparePacket, SetDbParameterPacket

from .helpers.cas_reply import (
    BatchStatement,
    Column,
    ResultSet,
    Value,
    batch_reply,
    int_,
    prepare_and_execute_reply,
    prepare_reply,
)
from .helpers.replay_broker import (
    IN_TRAN,
    OUT_TRAN,
    Reply,
    Request,
    Session,
    cas_info,
    error_body,
    ok_body,
    run_replay_broker,
    with_status,
)
from .test_deferred_close import _Driver

_SELECT = "SELECT v FROM paged"
_INSERT = "INSERT INTO t VALUES (7)"
_PAGED = ResultSet("paged", (Column("v", T.INT),), ((int_(1),), (int_(2),), (int_(3),)))
_INSERT_RESULT = ResultSet("insert", (), (), statement_type=CUBRIDStatementType.INSERT)
_RESULTS = {_SELECT: (_PAGED, 1), _INSERT: (_INSERT_RESULT, 0)}
ASYNC = pytest.mark.parametrize("asynchronous", [False, True], ids=["sync", "async"])


def _identity(request: Request, session: Session) -> Reply | None:
    if request.function == "GET_LAST_INSERT_ID":
        # This RPC changes CAS status back to IN_TRAN; retirement must have
        # processed the preceding FC41's OUT_TRAN, not the final execute status.
        session.status = IN_TRAN
        return Reply(cas_info(IN_TRAN) + struct.pack(">ii", 0, 3) + b"\x01" + b"7\x00")
    return None


def _driver(asynchronous: bool, broker: Any, autocommit: bool = True) -> _Driver:
    return _Driver(asynchronous, broker.port, autocommit)


@ASYNC
def test_old_cursor_cannot_close_a_reused_live_handle(asynchronous: bool) -> None:
    with run_replay_broker(_identity, results=_RESULTS, free_on_autocommit=True) as broker:
        d = _driver(asynchronous, broker)
        try:
            old, insert, fresh = d.cursor(), d.cursor(), d.cursor()
            d.execute(old, _SELECT)
            old_id = old._query_handle
            schema = d.run(d.conn.get_schema_info(1, "t"))
            generation = d.conn._physical_generation
            d.execute(insert, _INSERT)
            assert d.conn._cas_info[0] == IN_TRAN
            assert old._query_handle is None
            assert insert._query_handle is None
            assert d.conn._schema_results == {}
            assert d.conn._physical_generation == generation
            assert d.run(old.fetchone()) == (1,)
            with pytest.raises(InterfaceError, match="result set invalidated"):
                d.run(old.fetchone())
            d.execute(fresh, _SELECT)
            assert fresh._query_handle == old_id
            before = len(broker.requests)
            d.close(old)
            d.run(d.conn.close_schema_info(schema))
            assert len(broker.requests) == before
            assert d.run(fresh.fetchall()) == [(1,), (2,), (3,)]
        finally:
            d.teardown()


@ASYNC
def test_inline_completed_result_is_not_adopted(asynchronous: bool) -> None:
    with run_replay_broker(free_on_autocommit=True) as broker:
        d = _driver(asynchronous, broker)
        try:
            cursor = d.cursor()
            d.execute(cursor, "SELECT 1")
            assert cursor._query_handle is None
            assert d.run(cursor.fetchall()) == [(1,)]
            before = len(broker.requests)
            d.close(cursor)
            assert len(broker.requests) == before
        finally:
            d.teardown()


@ASYNC
def test_final_fetch_retires_other_results_and_schema(asynchronous: bool) -> None:
    with run_replay_broker(results=_RESULTS, free_on_autocommit=True) as broker:
        d = _driver(asynchronous, broker)
        try:
            first, other = d.cursor(), d.cursor()
            d.execute(first, _SELECT)
            d.execute(other, _SELECT)
            d.run(d.conn.get_schema_info(1, "t"))
            assert d.run(first.fetchall()) == [(1,), (2,), (3,)]
            assert first._query_handle is None
            assert other._query_handle is None
            assert d.conn._schema_results == {}
            assert d.run(other.fetchone()) == (1,)
            with pytest.raises(InterfaceError, match="result set invalidated"):
                d.run(other.fetchone())
        finally:
            d.teardown()


@ASYNC
@pytest.mark.parametrize("mode", ["manual", "pooling"])
def test_manual_and_pooling_on_results_keep_ownership(asynchronous: bool, mode: str) -> None:
    with run_replay_broker(
        _identity,
        results=_RESULTS,
        free_on_autocommit=True,
        statement_pooling=1 if mode == "pooling" else 0,
    ) as broker:
        d = _driver(asynchronous, broker, autocommit=mode != "manual")
        try:
            first, second = d.cursor(), d.cursor()
            d.execute(first, _SELECT)
            old_id = first._query_handle
            d.execute(second, _INSERT)
            assert first._query_handle == old_id
            assert d.run(first.fetchall()) == [(1,), (2,), (3,)]
            assert first._query_handle == old_id
        finally:
            d.teardown()


@ASYNC
@pytest.mark.parametrize("bad", ["value", "metadata"])
def test_completed_data_error_cannot_resurrect_a_freed_handle(
    asynchronous: bool,
    bad: str,
) -> None:
    zero_date = Value(T.DATE, lambda wire: wire.raw(b"\x00" * 6), None)
    rs = ResultSet("bad", (Column("v", T.DATE),), ((zero_date,),))

    def script(request: Request, session: Session) -> Reply | None:
        if request.sql == "SELECT bad":
            seed = prepare_and_execute_reply(rs)
            body = bytearray(seed.data)
            body[:4] = cas_info(OUT_TRAN)
            if bad == "metadata":
                body[seed.metadata_lengths[0] + 4] = 0xFF
            session.status = OUT_TRAN
            session.results.clear()
            return Reply(bytes(body))
        return None

    with run_replay_broker(script, free_on_autocommit=True) as broker:
        d = _driver(asynchronous, broker)
        try:
            cursor = d.cursor()
            with pytest.raises(DataError):
                d.execute(cursor, "SELECT bad")
            assert d.conn._connected is True
            assert cursor._query_handle is None
            assert d.select(cursor) == [(1,)]
        finally:
            d.teardown()


@ASYNC
def test_failed_autocommit_reply_retires_before_error_is_raised(asynchronous: bool) -> None:
    def script(request: Request, session: Session) -> Reply | None:
        if request.sql == "SELECT bad":
            session.status = OUT_TRAN
            session.results.clear()
            return Reply(error_body(OUT_TRAN, -10004, "invalid query handle"))
        return None

    with run_replay_broker(script, results=_RESULTS, free_on_autocommit=True) as broker:
        d = _driver(asynchronous, broker)
        try:
            old = d.cursor()
            d.execute(old, _SELECT)
            with pytest.raises(DatabaseError):
                d.execute(d.cursor(), "SELECT bad")
            assert old._query_handle is None
            assert d.conn._connected is True
        finally:
            d.teardown()


@ASYNC
def test_schema_fetch_does_not_end_an_autocommit_result(asynchronous: bool) -> None:
    with run_replay_broker(results=_RESULTS, free_on_autocommit=True) as broker:
        d = _driver(asynchronous, broker)
        try:
            cursor = d.cursor()
            d.execute(cursor, _SELECT)
            handle = cursor._query_handle
            schema = d.run(d.conn.get_schema_info(1, "t"))
            assert d.run(d.conn.fetch_schema_info(schema))
            assert cursor._query_handle == handle
            assert d.conn._connected is True
        finally:
            d.teardown()


@ASYNC
def test_autocommit_version_reply_retires_open_results(asynchronous: bool) -> None:
    with run_replay_broker(results=_RESULTS, free_on_autocommit=True) as broker:
        d = _driver(asynchronous, broker)
        try:
            cursor = d.cursor()
            d.execute(cursor, _SELECT)
            assert d.run(d.conn.get_server_version()) == "11.4.0.0000"
            assert cursor._query_handle is None
            assert d.conn._connected is True
        finally:
            d.teardown()


@ASYNC
@pytest.mark.parametrize("request_kind", ["ping", "close", "parameter", "batch", "schema"])
def test_out_tran_echoes_do_not_retire_unrelated_handles(
    asynchronous: bool,
    request_kind: str,
) -> None:
    holder: dict[str, Any] = {}

    def script(request: Request, session: Session) -> Reply | None:
        if request.function in ("CHECK_CAS", "SET_DB_PARAMETER", "CLOSE_REQ_HANDLE"):
            return Reply(ok_body(OUT_TRAN))
        if request.function == "EXECUTE_BATCH":
            result = batch_reply((BatchStatement(CUBRIDStatementType.INSERT, 1),)).data
            return Reply(with_status(result, OUT_TRAN))
        if request.function == "SCHEMA_INFO":
            result = holder["broker"].default_reply(request, session)
            assert result.body is not None
            return Reply(with_status(result.body, OUT_TRAN))
        return None

    with run_replay_broker(script, results=_RESULTS) as broker:
        holder["broker"] = broker
        d = _driver(asynchronous, broker)
        try:
            old = d.cursor()
            d.execute(old, _SELECT)
            handle = old._query_handle
            if request_kind == "ping":
                assert d.run(d.conn.ping(reconnect=False))
            elif request_kind == "close":
                second = d.cursor()
                d.execute(second, _SELECT)
                d.close(second)
            elif request_kind == "parameter":
                d.run(d.conn._send_and_receive(SetDbParameterPacket(1, 0)))
            elif request_kind == "batch":
                d.run(d.cursor().executemany(_INSERT, [()]))
            else:
                d.run(d.conn.get_schema_info(1, "t"))
            assert d.conn._cas_info[0] == OUT_TRAN
            assert old._query_handle == handle
            assert d.run(old.fetchall()) == [(1,), (2,), (3,)]
        finally:
            d.teardown()


@ASYNC
@pytest.mark.parametrize("mode", ["manual", "schema"])
def test_out_tran_fetch_excludes_manual_and_schema_handles(
    asynchronous: bool,
    mode: str,
) -> None:
    holder: dict[str, Any] = {}

    def script(request: Request, session: Session) -> Reply | None:
        if request.function == "FETCH":
            result = holder["broker"].default_reply(request, session)
            assert result.body is not None
            return Reply(with_status(result.body, OUT_TRAN))
        return None

    with run_replay_broker(script, results=_RESULTS) as broker:
        holder["broker"] = broker
        d = _driver(asynchronous, broker, autocommit=mode != "manual")
        try:
            old = d.cursor()
            d.execute(old, _SELECT)
            handle = old._query_handle
            if mode == "manual":
                assert d.run(old.fetchall()) == [(1,), (2,), (3,)]
            else:
                schema = d.run(d.conn.get_schema_info(1, "t"))
                assert d.run(d.conn.fetch_schema_info(schema))
            assert old._query_handle == handle
        finally:
            d.teardown()


@ASYNC
def test_proxy_and_unknown_pooling_do_not_assume_handle_retirement(asynchronous: bool) -> None:
    with run_replay_broker(results=_RESULTS) as broker:
        d = _driver(asynchronous, broker)
        try:
            old = d.cursor()
            d.execute(old, _SELECT)
            handle = old._query_handle
            packet = PrepareAndExecutePacket("SELECT 1", auto_commit=True)
            for pooling, db_type in ((0, 4), (None, 1)):
                d.conn._statement_pooling = pooling
                d.conn._broker_db_type = db_type
                d.conn._retire_pooling_off_reply_handles(packet, cas_info(OUT_TRAN))
                assert old._query_handle == handle
                assert packet._query_handle_retired is False
        finally:
            d.teardown()


@ASYNC
def test_reused_packet_resets_its_retired_result_marker(asynchronous: bool) -> None:
    with run_replay_broker(free_on_autocommit=True) as broker:
        d = _driver(asynchronous, broker)
        try:
            packet = PrepareAndExecutePacket("SELECT 1", auto_commit=True)
            d.run(d.conn._send_and_receive(packet))
            assert packet._query_handle_retired is True
            packet.auto_commit = False
            d.run(d.conn._send_and_receive(packet))
            assert packet._query_handle_retired is False
            assert packet.rows == [(1,)]
        finally:
            d.teardown()


@ASYNC
@pytest.mark.parametrize("failed", [False, True])
def test_only_failed_autocommit_prepare_proves_a_rollback(
    asynchronous: bool,
    failed: bool,
) -> None:
    def script(request: Request, session: Session) -> Reply | None:
        if request.function == "PREPARE":
            if failed:
                session.results.clear()
                return Reply(error_body(OUT_TRAN, -10004, "invalid query handle"))
            return Reply(with_status(prepare_reply(_PAGED).data, OUT_TRAN))
        return None

    with run_replay_broker(script, results=_RESULTS) as broker:
        d = _driver(asynchronous, broker)
        try:
            cursor = d.cursor()
            d.execute(cursor, _SELECT)
            handle = cursor._query_handle
            packet = PreparePacket("SELECT v FROM paged", auto_commit=True)
            if failed:
                with pytest.raises(DatabaseError):
                    d.run(d.conn._send_and_receive(packet))
                assert cursor._query_handle is None
            else:
                d.run(d.conn._send_and_receive(packet))
                assert cursor._query_handle == handle
            assert d.conn._connected is True
        finally:
            d.teardown()
