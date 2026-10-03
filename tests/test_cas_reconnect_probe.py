"""OUT_TRAN CHECK_CAS probe, verified reconnect and END_TRAN handle release (#485).

CAS_INFO status 0 (OUT_TRAN) keeps the physical session (#468), but the CAS may
close the socket right after replying at a transaction boundary (memory restart,
``cubrid broker reset``, CHANGE CLIENT). Before the next request the driver
probes with CHECK_CAS (JDBC ``checkReconnect`` parity) and only a failed probe
replaces the session: once per request, restoring driver-owned state and never
replaying SQL.
"""

from __future__ import annotations

import asyncio
import struct
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from pycubrid._connection_common import ConnectionCommonMixin
from pycubrid.aio.connection import AsyncConnection
from pycubrid.connection import Connection
from pycubrid.constants import CASFunctionCode
from pycubrid.exceptions import InterfaceError, OperationalError, ProgrammingError
from pycubrid.protocol import (
    BatchExecutePacket,
    CheckCasPacket,
    CloseDatabasePacket,
    CloseQueryPacket,
    CommitPacket,
    FetchPacket,
    GetLastInsertIdPacket,
    GetSchemaPacket,
    LOBReadPacket,
    LOBWritePacket,
    PrepareAndExecutePacket,
    RollbackPacket,
    SetDbParameterPacket,
)

from .test_network_edge_cases import (
    build_handshake_response,
    build_open_db_response,
    build_simple_ok_response,
    make_connected_connection,
    make_socket_from_chunks,
)

OUT_TRAN = b"\x00\x01\x02\x03"
IN_TRAN = b"\x01\x01\x02\x03"


def _frames(*frames: bytes) -> list[bytes]:
    chunks: list[bytes] = []
    for frame in frames:
        chunks += [frame[:4], frame[4:]]
    return chunks


def _function_codes(sock: MagicMock, start: int = 0) -> list[int]:
    return [call.args[0][8] for call in sock.sendall.call_args_list[start:]]


def _script(sock: MagicMock, chunks: list[bytes]) -> None:
    sock.recv_into.side_effect = make_socket_from_chunks(chunks).recv_into.side_effect


def _sync_out_tran() -> tuple[Connection, MagicMock]:
    conn, sock = make_connected_connection()
    conn._record_reply_cas_info(bytearray(OUT_TRAN))  # a fresh, unverified END_TRAN reply
    return conn, sock


def _replacement_socket(*frames: bytes) -> MagicMock:
    open_db = build_open_db_response()
    return make_socket_from_chunks(
        [build_handshake_response(), open_db[:4], open_db[4:]] + _frames(*frames)
    )


# -- shared decision helpers ---------------------------------------------------


def test_skip_request_after_reconnect_classifies_session_bound_requests() -> None:
    assert Connection._skip_request_after_reconnect(CloseQueryPacket(3)) is True
    assert Connection._skip_request_after_reconnect(CommitPacket()) is False
    assert Connection._skip_request_after_reconnect(CheckCasPacket()) is False
    with pytest.raises(OperationalError, match="result set lost due to broker reconnect"):
        Connection._skip_request_after_reconnect(FetchPacket(3, 0, 10))
    for packet in (LOBReadPacket(b"h", 0, 1), LOBWritePacket(b"h", 0, b"x")):
        with pytest.raises(OperationalError, match="LOB handle lost due to broker reconnect"):
            Connection._skip_request_after_reconnect(packet)


def test_lost_last_insert_id_is_logged_at_warning(caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level("WARNING", logger="pycubrid._connection_common"):
        with pytest.raises(OperationalError, match="last insert id lost"):
            Connection._skip_request_after_reconnect(GetLastInsertIdPacket())
    assert "lastrowid is unavailable" in caplog.text


# -- sync ----------------------------------------------------------------------


def test_sync_probe_success_keeps_session_and_sends_request() -> None:
    conn, sock = _sync_out_tran()
    ok = build_simple_ok_response(OUT_TRAN)
    _script(sock, _frames(ok, ok))
    start = sock.sendall.call_count
    generation = conn._physical_generation

    with patch("socket.create_connection") as create:
        conn._send_and_receive(CommitPacket())

    create.assert_not_called()
    assert conn._socket is sock
    assert conn._physical_generation == generation
    assert _function_codes(sock, start) == [CASFunctionCode.CHECK_CAS, CASFunctionCode.END_TRAN]


def test_sync_verified_or_in_tran_status_is_not_probed_again() -> None:
    conn, sock = _sync_out_tran()
    _script(sock, _frames(build_simple_ok_response(OUT_TRAN)))
    start = sock.sendall.call_count

    assert conn._check_reconnect() is False
    assert conn._check_reconnect() is False  # same CAS_INFO already verified live
    conn._record_reply_cas_info(bytearray(IN_TRAN))
    assert conn._check_reconnect() is False  # IN_TRAN never probes
    conn._record_reply_cas_info(bytearray(OUT_TRAN))
    assert conn._check_reconnect(allow_reconnect=False) is False

    assert _function_codes(sock, start) == [CASFunctionCode.CHECK_CAS]


def test_sync_fresh_session_and_healthy_ping_count_as_verified() -> None:
    conn, sock = make_connected_connection()
    assert conn._cas_reply_verified  # OPEN_DATABASE reply
    conn._record_reply_cas_info(bytearray(OUT_TRAN))
    _script(sock, _frames(build_simple_ok_response(OUT_TRAN)))
    start = sock.sendall.call_count

    assert conn.ping(reconnect=False) is True
    assert conn._check_reconnect() is False
    assert _function_codes(sock, start) == [CASFunctionCode.CHECK_CAS]


@pytest.mark.parametrize("failure", ["eof", "negative"])
def test_sync_probe_failure_reconnects_once_and_restores(failure: str) -> None:
    conn, old = _sync_out_tran()
    conn._autocommit = True
    conn._autocommit_explicitly_set = True
    if failure == "eof":
        _script(old, [])
    else:
        # CHECK_CAS answered with a negative code: the CAS lost its DB link.
        body = OUT_TRAN + struct.pack(">i", -1)
        _script(old, _frames(struct.pack(">i", len(body) - 4) + body))
    cursor = conn.cursor()
    cursor._query_handle = 5
    cursor._fetched_count = 1
    cursor._total_tuple_count = 3
    ok = build_simple_ok_response()
    new = _replacement_socket(ok, ok)
    generation = conn._physical_generation
    old_start = old.sendall.call_count

    with patch("socket.create_connection", return_value=new) as create:
        conn._send_and_receive(CommitPacket())

    create.assert_called_once()
    old.close.assert_called()
    assert conn._socket is new
    assert conn._physical_generation == generation + 1
    # The dead session only ever saw the probe: no SQL was sent to it or replayed.
    assert _function_codes(old, old_start) == [CASFunctionCode.CHECK_CAS]
    # Handshake and OPEN_DATABASE, then the autocommit restore, then the request.
    assert _function_codes(new, 2) == [CASFunctionCode.SET_DB_PARAMETER, CASFunctionCode.END_TRAN]
    assert cursor._query_handle is None
    assert cursor._invalidated_by_reconnect is True


def test_sync_reconnect_failure_raises_operational_error_once() -> None:
    conn, old = _sync_out_tran()
    _script(old, [])

    with patch("socket.create_connection", side_effect=OSError("refused")) as create:
        with pytest.raises(OperationalError, match="reconnecting failed"):
            conn._send_and_receive(CommitPacket())
        assert create.call_count == 1
        assert conn._connected is False
        with pytest.raises(InterfaceError, match="closed"):
            conn._send_and_receive(CommitPacket())
        assert create.call_count == 1


def test_sync_requests_bound_to_the_replaced_session_are_not_sent() -> None:
    conn, old = _sync_out_tran()
    _script(old, [])
    new = _replacement_socket()

    with patch("socket.create_connection", return_value=new):
        # CLOSE_REQ for a handle of the dead session: nothing left to close.
        packet = CloseQueryPacket(9)
        assert conn._send_and_receive(packet) is packet
    assert _function_codes(new, 2) == []

    conn2, old2 = _sync_out_tran()
    _script(old2, [])
    with patch("socket.create_connection", return_value=_replacement_socket()):
        with pytest.raises(OperationalError, match="result set lost"):
            conn2._send_and_receive(FetchPacket(9, 0, 10))


def test_sync_close_never_probes_or_reconnects() -> None:
    conn, sock = _sync_out_tran()
    cursor = conn.cursor()
    cursor._query_handle = 4
    _script(sock, _frames(build_simple_ok_response(OUT_TRAN), build_simple_ok_response(OUT_TRAN)))
    start = sock.sendall.call_count

    with patch("socket.create_connection") as create:
        conn.close()

    create.assert_not_called()
    assert _function_codes(sock, start) == [
        CASFunctionCode.CLOSE_REQ_HANDLE,
        CASFunctionCode.CON_CLOSE,
    ]
    assert conn._implicit_reconnect_suspended == 0


@pytest.mark.parametrize("boundary", ["commit", "rollback"])
def test_sync_boundary_closes_open_handles_first(boundary: str) -> None:
    conn, _ = make_connected_connection()
    open_cursor, closed_cursor = conn.cursor(), conn.cursor()
    open_cursor._query_handle = 7
    closed_cursor._query_handle = None
    sent: list[Any] = []
    conn._send_and_receive = MagicMock(side_effect=sent.append)  # type: ignore[method-assign]

    getattr(conn, boundary)()

    end_tran = CommitPacket if boundary == "commit" else RollbackPacket
    assert [type(packet) for packet in sent] == [CloseQueryPacket, end_tran]
    assert sent[0].query_handle == 7
    assert open_cursor._query_handle is None


def test_sync_native_close_error_is_ignored_before_commit() -> None:
    conn, _ = make_connected_connection()
    conn_cursor = conn.cursor()  # held: cursors are tracked weakly (#488)
    conn_cursor._query_handle = 7
    sent: list[Any] = []

    def send(packet: Any) -> None:
        sent.append(packet)
        if isinstance(packet, CloseQueryPacket):
            raise ProgrammingError("invalid handle", code=-1)

    conn._send_and_receive = MagicMock(side_effect=send)  # type: ignore[method-assign]
    conn.commit()
    assert [type(packet) for packet in sent] == [CloseQueryPacket, CommitPacket]


def test_sync_transport_failure_on_close_aborts_commit() -> None:
    conn, _ = make_connected_connection()
    conn_cursor = conn.cursor()  # held: cursors are tracked weakly (#488)
    conn_cursor._query_handle = 7
    sent: list[Any] = []

    def send(packet: Any) -> None:
        sent.append(packet)
        conn._connected = False
        raise OperationalError("socket communication failed")

    conn._send_and_receive = MagicMock(side_effect=send)  # type: ignore[method-assign]
    with pytest.raises(OperationalError, match="socket communication failed"):
        conn.commit()
    assert [type(packet) for packet in sent] == [CloseQueryPacket]


def test_sync_handle_close_stops_after_reconnect() -> None:
    conn, _ = make_connected_connection()
    first, second = conn.cursor(), conn.cursor()
    first._query_handle, second._query_handle = 1, 2
    sent: list[Any] = []

    def send(packet: Any) -> None:
        sent.append(packet)
        if isinstance(packet, CloseQueryPacket):
            # Emulate a probe-verified reconnect during the first CLOSE_REQ.
            conn._physical_generation += 1
            conn._invalidate_query_handles_for_reconnect()

    conn._send_and_receive = MagicMock(side_effect=send)  # type: ignore[method-assign]
    conn.commit()
    assert [type(packet) for packet in sent] == [CloseQueryPacket, CommitPacket]


# -- async ---------------------------------------------------------------------


class _FakeCas:
    """Scripted async CAS replies; ``dead`` fails the next request once."""

    def __init__(self, conn: AsyncConnection, *, escape_length: object = 2) -> None:
        self.conn = conn
        self.dead = False
        self.escape_length = escape_length
        self.reply = OUT_TRAN
        self.fail_on: set[int] = set()  # indices into ``sent`` that fail like EOF
        self.sent: list[tuple[int, Any]] = []

    async def __call__(self, packet: Any) -> Any:
        assert self.conn._lock.locked(), "every request, including reconnect setup, holds _lock"
        self.sent.append((self.conn._physical_generation, packet))
        if self.dead or len(self.sent) - 1 in self.fail_on:
            self.dead = False
            self.conn._drop_connection()
            raise OperationalError("connection lost during receive")
        if isinstance(packet, PrepareAndExecutePacket):
            packet.rows = [] if self.escape_length is None else [(self.escape_length,)]
            packet.query_handle = 5
        self.conn._record_reply_cas_info(bytearray(self.reply))
        return packet

    def kinds(self) -> list[tuple[int, type]]:
        return [(generation, type(packet)) for generation, packet in self.sent]


def _async_out_tran(**kwargs: Any) -> tuple[AsyncConnection, _FakeCas, AsyncMock]:
    kwargs.setdefault("no_backslash_escapes", None)
    conn = AsyncConnection("localhost", 33000, "testdb", "dba", "", **kwargs)
    conn._connected = True
    conn._physical_generation = 1
    if conn._no_backslash_escapes is None:
        conn._no_backslash_escapes = True
    conn._reader = MagicMock()
    conn._writer = MagicMock()
    conn._writer.wait_closed = AsyncMock()
    conn._record_reply_cas_info(bytearray(OUT_TRAN))
    cas = _FakeCas(conn)
    conn._do_send_and_receive = cas  # type: ignore[method-assign]

    writer = MagicMock()
    writer.wait_closed = AsyncMock()
    open_connection = AsyncMock(return_value=(MagicMock(), writer))

    async def handshake(reader: Any, stream_writer: Any) -> None:
        conn._reader, conn._writer = reader, stream_writer
        conn._record_reply_cas_info(bytearray(OUT_TRAN))

    conn._open_connection = open_connection  # type: ignore[method-assign]
    conn._do_connect_handshake = handshake  # type: ignore[method-assign]
    return conn, cas, open_connection


@pytest.mark.asyncio
async def test_async_probe_success_keeps_session_and_sends_request() -> None:
    conn, cas, open_connection = _async_out_tran()

    await conn._send_and_receive(CommitPacket())

    open_connection.assert_not_awaited()
    assert cas.kinds() == [(1, CheckCasPacket), (1, CommitPacket)]


@pytest.mark.asyncio
async def test_async_verified_or_in_tran_status_is_not_probed_again() -> None:
    conn, cas, _ = _async_out_tran()

    assert await conn._check_reconnect() is False
    assert await conn._check_reconnect() is False
    conn._record_reply_cas_info(bytearray(IN_TRAN))
    assert await conn._check_reconnect() is False
    conn._record_reply_cas_info(bytearray(OUT_TRAN))
    assert await conn._check_reconnect(allow_reconnect=False) is False
    assert cas.kinds() == [(1, CheckCasPacket)]


@pytest.mark.asyncio
async def test_async_healthy_ping_counts_as_verified() -> None:
    conn, cas, _ = _async_out_tran()
    assert await conn.ping(reconnect=False) is True
    assert await conn._check_reconnect() is False
    assert cas.kinds() == [(1, CheckCasPacket)]


@pytest.mark.asyncio
async def test_async_probe_failure_reconnects_once_and_restores_under_lock() -> None:
    conn, cas, open_connection = _async_out_tran()
    conn._autocommit = True
    conn._autocommit_explicitly_set = True
    cursor = conn.cursor()
    cursor._query_handle = 8
    cas.dead = True

    await conn._send_and_receive(CommitPacket())

    open_connection.assert_awaited_once()
    assert conn._physical_generation == 2
    assert conn._no_backslash_escapes is True  # re-probed on the new session (#471)
    assert cas.kinds() == [
        (1, CheckCasPacket),
        (2, PrepareAndExecutePacket),
        (2, CloseQueryPacket),
        (2, RollbackPacket),
        (2, SetDbParameterPacket),
        (2, CheckCasPacket),  # setup ended OUT_TRAN: verify before the request
        (2, CommitPacket),
    ]
    assert cas.sent[1][1].sql == "SELECT CHAR_LENGTH('\\\\')"
    assert cursor._query_handle is None
    assert cursor._invalidated_by_reconnect is True
    assert conn._implicit_reconnect_suspended == 0


@pytest.mark.asyncio
async def test_async_pending_constructor_autocommit_is_applied_on_reconnect() -> None:
    conn, cas, _ = _async_out_tran(no_backslash_escapes=True)
    conn._pending_autocommit = True
    cas.dead = True

    await conn._send_and_receive(CommitPacket())

    assert conn._autocommit is True
    assert conn._pending_autocommit is False
    assert cas.kinds()[1:] == [
        (2, SetDbParameterPacket),
        (2, CommitPacket),
        (2, CheckCasPacket),
        (2, CommitPacket),
    ]


@pytest.mark.asyncio
async def test_async_reconnect_failure_raises_operational_error_once() -> None:
    conn, cas, open_connection = _async_out_tran()
    open_connection.side_effect = OperationalError("could not connect")
    cas.dead = True

    with pytest.raises(OperationalError, match="reconnecting failed"):
        await conn._send_and_receive(CommitPacket())
    assert conn._connected is False
    with pytest.raises(InterfaceError, match="closed"):
        await conn._send_and_receive(CommitPacket())
    open_connection.assert_awaited_once()
    assert cas.kinds() == [(1, CheckCasPacket)]


@pytest.mark.asyncio
@pytest.mark.parametrize("length", [3, None, "error"])
async def test_async_failed_escape_probe_fails_the_reconnect(length: object) -> None:
    conn, cas, _ = _async_out_tran()
    cas.escape_length = length
    cas.dead = True
    if length == "error":

        async def failing(packet: Any) -> Any:
            if isinstance(packet, PrepareAndExecutePacket):
                raise ProgrammingError("probe failed", code=-1)
            return await cas(packet)

        conn._do_send_and_receive = failing  # type: ignore[method-assign]

    with pytest.raises(OperationalError, match="reconnecting failed") as info:
        await conn._send_and_receive(CommitPacket())
    assert "backslash-escape" in str(info.value.__cause__)
    assert conn._connected is False


@pytest.mark.asyncio
@pytest.mark.parametrize("new_length", [2, 1], ids=["same-mode", "changed-mode"])
async def test_async_sql_bound_before_its_preflight_reconnect_is_never_sent(
    new_length: int,
) -> None:
    """SQL rendered for generation 1 is rejected when its own probe replaces the CAS."""
    conn, cas, _ = _async_out_tran()
    cas.escape_length = new_length
    cas.dead = True
    packet = PrepareAndExecutePacket("SELECT 'a\\\\b'")

    with pytest.raises(OperationalError, match="parameter binding; retry operation"):
        await conn._send_and_receive(packet, expected_escape_generation=1)

    assert conn._physical_generation == 2
    assert all(sent_packet is not packet for _, sent_packet in cas.sent)
    assert conn._connected is True  # the healthy replacement stays usable

    # A fresh retry binds for generation 2 and is sent once.
    cursor = conn.cursor()
    await cursor.execute("SELECT ?", ("a\\b",))
    assert cas.sent[-1][0] == 2
    assert isinstance(cas.sent[-1][1], PrepareAndExecutePacket)


@pytest.mark.asyncio
async def test_async_cursor_probes_before_binding_so_first_statement_succeeds() -> None:
    conn, cas, open_connection = _async_out_tran()
    cursor = conn.cursor()
    cas.dead = True  # the CAS was recycled after the last OUT_TRAN reply

    await cursor.execute("SELECT ?", (1,))

    open_connection.assert_awaited_once()
    statements = [
        (generation, packet.sql)
        for generation, packet in cas.sent
        if isinstance(packet, PrepareAndExecutePacket) and "CHAR_LENGTH" not in packet.sql
    ]
    assert statements == [(2, "SELECT 1")]


@pytest.mark.asyncio
async def test_async_requests_bound_to_the_replaced_session_are_not_sent() -> None:
    conn, cas, _ = _async_out_tran()
    cas.dead = True
    packet = CloseQueryPacket(9)
    assert await conn._send_and_receive(packet) is packet
    assert all(sent is not packet for _, sent in cas.sent)

    conn2, cas2, _ = _async_out_tran()
    cas2.dead = True
    with pytest.raises(OperationalError, match="result set lost"):
        await conn2._send_and_receive(FetchPacket(9, 0, 10))


@pytest.mark.asyncio
async def test_async_close_never_probes_or_reconnects() -> None:
    conn, cas, open_connection = _async_out_tran()
    conn_cursor = conn.cursor()  # held: cursors are tracked weakly (#488)
    conn_cursor._query_handle = 4

    await conn.close()

    open_connection.assert_not_awaited()
    assert cas.kinds() == [(1, CloseQueryPacket), (1, CloseDatabasePacket)]
    assert conn._implicit_reconnect_suspended == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("boundary", ["commit", "rollback"])
async def test_async_boundary_closes_open_handles_first(boundary: str) -> None:
    conn, cas, _ = _async_out_tran()
    conn._record_reply_cas_info(bytearray(IN_TRAN))
    cas.reply = IN_TRAN
    open_cursor, closed_cursor = conn.cursor(), conn.cursor()
    open_cursor._query_handle = 7
    closed_cursor._query_handle = None

    await getattr(conn, boundary)()

    end_tran = CommitPacket if boundary == "commit" else RollbackPacket
    assert cas.kinds() == [(1, CloseQueryPacket), (1, end_tran)]
    assert cas.sent[0][1].query_handle == 7
    assert open_cursor._query_handle is None


@pytest.mark.asyncio
async def test_async_close_errors_before_commit() -> None:
    conn, cas, _ = _async_out_tran()
    conn._record_reply_cas_info(bytearray(IN_TRAN))
    conn_cursor = conn.cursor()  # held: cursors are tracked weakly (#488)
    conn_cursor._query_handle = 7

    async def native_error(packet: Any) -> Any:
        if isinstance(packet, CloseQueryPacket):
            cas.sent.append((1, packet))
            raise ProgrammingError("invalid handle", code=-1)
        return await cas(packet)

    conn._do_send_and_receive = native_error  # type: ignore[method-assign]
    await conn.commit()
    assert cas.kinds() == [(1, CloseQueryPacket), (1, CommitPacket)]

    conn2, cas2, _ = _async_out_tran()
    conn2._record_reply_cas_info(bytearray(IN_TRAN))
    conn2_cursor = conn2.cursor()  # held: cursors are tracked weakly (#488)
    conn2_cursor._query_handle = 7
    cas2.dead = True
    with pytest.raises(OperationalError, match="connection lost"):
        await conn2.commit()
    assert cas2.kinds() == [(1, CloseQueryPacket)]


@pytest.mark.asyncio
async def test_async_handle_close_stops_after_reconnect() -> None:
    conn, cas, _ = _async_out_tran()
    first, second = conn.cursor(), conn.cursor()
    first._query_handle, second._query_handle = 1, 2
    cas.dead = True  # the CLOSE_REQ probe finds the CAS gone

    await conn.commit()

    kinds = [kind for _, kind in cas.kinds()]
    assert kinds.count(CloseQueryPacket) == 1  # only the escape probe's own handle
    assert kinds[-1] is CommitPacket
    assert first._query_handle is None and second._query_handle is None


@pytest.mark.asyncio
async def test_async_cancelled_reconnect_retires_the_session() -> None:
    conn, cas, open_connection = _async_out_tran()
    open_connection.side_effect = asyncio.CancelledError()
    cas.dead = True

    with pytest.raises(asyncio.CancelledError):
        await conn._send_and_receive(CommitPacket())
    assert conn._connected is False
    assert conn._writer is None
    assert conn._implicit_reconnect_suspended == 0


# -- review follow-ups -----------------------------------------------------------


def test_sync_sql_bound_before_its_preflight_reconnect_is_never_sent() -> None:
    """SQL rendered for generation 1 is rejected when its own probe replaces the CAS."""
    conn, old = _sync_out_tran()
    _script(old, [])
    ok = build_simple_ok_response()
    new = _replacement_socket(ok, ok)
    generation = conn._physical_generation
    packet = PrepareAndExecutePacket("SELECT 'a\\\\b'")

    with patch("socket.create_connection", return_value=new):
        with pytest.raises(OperationalError, match="parameter binding; retry operation"):
            conn._send_and_receive(packet, bound_generation=generation)

    assert conn._physical_generation == generation + 1
    assert _function_codes(new, 2) == []  # nothing sent on the replacement
    assert conn._connected is True

    # A fresh retry is bound for the new generation and sent once.
    retry = CommitPacket()
    conn._send_and_receive(retry, bound_generation=conn._generation_for_binding())
    assert _function_codes(new, 2) == [CASFunctionCode.END_TRAN]


def test_sync_cursor_probes_before_binding_so_first_statement_succeeds() -> None:
    conn, old = _sync_out_tran()
    cursor = conn.cursor()
    _script(old, [])
    new = _replacement_socket()
    sent: list[Any] = []
    original = conn._send_and_receive_locked

    def record(packet: Any, **kwargs: Any) -> Any:
        if isinstance(packet, PrepareAndExecutePacket):
            sent.append((conn._physical_generation, packet.sql))
            _prepare_reply(packet)
            return packet
        return original(packet, **kwargs)

    conn._send_and_receive_locked = record  # type: ignore[method-assign]
    generation = conn._physical_generation
    with patch("socket.create_connection", return_value=new):
        cursor.execute("SELECT ?", (1,))

    assert sent == [(generation + 1, "SELECT 1")]


def test_sync_close_resets_suspension_after_base_exception() -> None:
    conn, _ = make_connected_connection()
    cursor = conn.cursor()
    cursor.close = MagicMock(side_effect=KeyboardInterrupt)  # type: ignore[method-assign]

    with pytest.raises(KeyboardInterrupt):
        conn.close()
    assert conn._implicit_reconnect_suspended == 0


def test_sync_interrupted_reconnect_retires_the_session() -> None:
    conn, old = _sync_out_tran()
    _script(old, [])

    with patch("socket.create_connection", side_effect=KeyboardInterrupt):
        with pytest.raises(KeyboardInterrupt):
            conn._send_and_receive(CommitPacket())
    assert conn._connected is False
    assert conn._socket is None
    assert conn._implicit_reconnect_suspended == 0


def _prepare_reply(packet: Any) -> None:
    packet.query_handle = 3
    packet.statement_type = 21  # SELECT
    packet.columns = []
    packet.total_tuple_count = 0
    packet.rows = []
    packet.result_infos = []


def test_sync_execute_clears_reconnect_flag_for_its_new_result() -> None:
    conn, _ = make_connected_connection()
    cursor = conn.cursor()

    def send(packet: Any) -> Any:
        if isinstance(packet, PrepareAndExecutePacket):
            conn._invalidate_query_handles_for_reconnect()  # probe reconnected first
            _prepare_reply(packet)
        return packet

    conn._send_and_receive = MagicMock(side_effect=send)  # type: ignore[method-assign]
    cursor.execute("SELECT 1")
    assert cursor._query_handle == 3
    assert cursor._invalidated_by_reconnect is False


@pytest.mark.asyncio
async def test_async_execute_clears_reconnect_flag_for_its_new_result() -> None:
    conn, _, _ = _async_out_tran()
    cursor = conn.cursor()

    async def send(packet: Any, **_: object) -> Any:
        if isinstance(packet, PrepareAndExecutePacket):
            conn._invalidate_query_handles_for_reconnect()
            _prepare_reply(packet)
        return packet

    conn._send_and_receive = send  # type: ignore[method-assign]
    await cursor.execute("SELECT 1")
    assert cursor._query_handle == 3
    assert cursor._invalidated_by_reconnect is False


@pytest.mark.asyncio
@pytest.mark.parametrize("by_reconnect", [False, True])
async def test_async_handle_released_while_waiting_is_never_sent(by_reconnect: bool) -> None:
    """Another task's boundary frees handle 5 while this FETCH waits for _lock."""
    conn, cas, _ = _async_out_tran()
    conn._record_reply_cas_info(bytearray(IN_TRAN))
    cas.reply = IN_TRAN
    cursor = conn.cursor()
    cursor._query_handle = 5

    await conn._lock.acquire()
    fetch = asyncio.create_task(conn._send_and_receive(FetchPacket(5, 0, 10), handle_owner=cursor))
    close = asyncio.create_task(conn._send_and_receive(CloseQueryPacket(5), handle_owner=cursor))
    await asyncio.sleep(0)
    if by_reconnect:
        conn._invalidate_query_handles_for_reconnect()
    else:
        conn._invalidate_query_handles()
    conn._lock.release()

    error = OperationalError if by_reconnect else InterfaceError
    with pytest.raises(error, match="re-execute the query"):
        _ = await fetch
    assert (await close).query_handle == 5
    assert cas.sent == []  # handle 5 may already name another result


# -- second review round -----------------------------------------------------------


@pytest.mark.asyncio
async def test_async_replacement_recycled_during_setup_fails_cleanly() -> None:
    """The escape probe's rollback can recycle the new CAS too (M3)."""
    conn, cas, open_connection = _async_out_tran()
    cas.dead = True  # the first probe fails
    cas.fail_on = {4}  # 0 probe, 1 escape SELECT, 2 CLOSE_REQ, 3 ROLLBACK, 4 re-probe
    request = CommitPacket()

    with pytest.raises(OperationalError, match="did not answer CHECK_CAS.*reconnecting failed"):
        await conn._send_and_receive(request)

    open_connection.assert_awaited_once()  # still one attempt per request
    assert cas.kinds()[-1] == (2, CheckCasPacket)
    assert all(packet is not request for _, packet in cas.sent)
    assert conn._connected is False


def test_sync_replacement_recycled_during_setup_fails_cleanly() -> None:
    conn, old = _sync_out_tran()
    conn._autocommit = True
    conn._autocommit_explicitly_set = True
    _script(old, [])
    # The restore reply is OUT_TRAN, then the replacement closes too.
    new = _replacement_socket(build_simple_ok_response(OUT_TRAN))

    with patch("socket.create_connection", return_value=new) as create:
        with pytest.raises(OperationalError, match="did not answer CHECK_CAS.*reconnecting failed"):
            conn._send_and_receive(CommitPacket())

    create.assert_called_once()
    assert _function_codes(new, 2) == [
        CASFunctionCode.SET_DB_PARAMETER,
        CASFunctionCode.CHECK_CAS,
    ]
    assert conn._connected is False


@pytest.mark.parametrize("boundary", ["commit", "rollback"])
def test_sync_boundary_probes_before_closing_schema_results(boundary: str) -> None:
    conn, _ = make_connected_connection()
    order: list[str] = []
    conn._check_reconnect = MagicMock(  # type: ignore[method-assign]
        side_effect=lambda **_: order.append("probe") or False
    )
    conn._close_schema_results = MagicMock(  # type: ignore[method-assign]
        side_effect=lambda: order.append("schema")
    )
    conn._send_and_receive = MagicMock()  # type: ignore[method-assign]

    getattr(conn, boundary)()
    assert order[:2] == ["probe", "schema"]


@pytest.mark.asyncio
@pytest.mark.parametrize("boundary", ["commit", "rollback"])
async def test_async_boundary_probes_before_closing_schema_results(boundary: str) -> None:
    conn, cas, _ = _async_out_tran()
    conn._close_schema_results_locked = AsyncMock()  # type: ignore[method-assign]
    cas.dead = True  # recycled after the last OUT_TRAN reply

    await getattr(conn, boundary)()

    assert conn._physical_generation == 2
    assert cas.kinds()[0] == (1, CheckCasPacket)
    assert cas.kinds()[-1][1] is (CommitPacket if boundary == "commit" else RollbackPacket)


@pytest.mark.asyncio
async def test_async_overlapping_suspension_scopes_nest() -> None:
    conn, _, _ = _async_out_tran()
    conn._implicit_reconnect_suspended += 1  # e.g. close() running in another task
    await conn.close()
    assert conn._implicit_reconnect_suspended == 1


@pytest.mark.parametrize("boundary", ["commit", "rollback"])
def test_sync_boundary_with_live_schema_result_survives_cas_recycle(boundary: str) -> None:
    """A recycled CAS is replaced before schema cleanup; no CLOSE_REQ hits the dead socket."""
    conn, old = _sync_out_tran()
    schema = GetSchemaPacket(schema_type=1)
    schema.query_handle, schema.tuple_count, schema.columns = 4, 1, []
    conn._register_schema_result(schema)
    _script(old, [])
    new = _replacement_socket(build_simple_ok_response())
    old_start = old.sendall.call_count

    with patch("socket.create_connection", return_value=new):
        getattr(conn, boundary)()

    assert _function_codes(old, old_start) == [CASFunctionCode.CHECK_CAS]
    assert _function_codes(new, 2) == [CASFunctionCode.END_TRAN]
    assert conn._schema_results == {}


@pytest.mark.asyncio
@pytest.mark.parametrize("boundary", ["commit", "rollback"])
async def test_async_boundary_with_live_schema_result_survives_cas_recycle(
    boundary: str,
) -> None:
    conn, cas, _ = _async_out_tran(no_backslash_escapes=True)
    schema = GetSchemaPacket(schema_type=1)
    schema.query_handle, schema.tuple_count, schema.columns = 4, 1, []
    conn._register_schema_result(schema)
    cas.dead = True

    await getattr(conn, boundary)()

    end_tran = CommitPacket if boundary == "commit" else RollbackPacket
    assert cas.kinds() == [(1, CheckCasPacket), (2, end_tran)]
    assert conn._schema_results == {}


# -- independent verification round --------------------------------------------------


def _intercept_statements(conn: Connection) -> list[tuple[int, str]]:
    """Answer PREPARE_AND_EXECUTE/EXECUTE_BATCH locally; record their generation."""
    sent: list[tuple[int, str]] = []
    original = conn._send_and_receive_locked

    def record(packet: Any, **kwargs: Any) -> Any:
        if isinstance(packet, PrepareAndExecutePacket):
            sent.append((conn._physical_generation, packet.sql))
            _prepare_reply(packet)
            return packet
        if isinstance(packet, BatchExecutePacket):
            sent.append((conn._physical_generation, packet.sql_list[0]))
            packet.results = [(20, 1)]
            return packet
        return original(packet, **kwargs)

    conn._send_and_receive_locked = record  # type: ignore[method-assign]
    return sent


def test_sync_executemany_closes_old_handle_before_binding() -> None:
    """A CAS recycled after the old handle's CLOSE_REQ must not reject the batch."""
    conn, old = _sync_out_tran()
    cursor = conn.cursor()
    cursor._query_handle = 7
    ok = build_simple_ok_response(OUT_TRAN)
    _script(old, _frames(ok, ok))  # probe, CLOSE_REQ; then EOF for the pre-bind probe
    sent = _intercept_statements(conn)
    generation = conn._physical_generation
    old_start = old.sendall.call_count

    with patch("socket.create_connection", return_value=_replacement_socket()):
        cursor.executemany("INSERT INTO t VALUES (?)", [("a\\b",)])

    assert _function_codes(old, old_start) == [
        CASFunctionCode.CHECK_CAS,
        CASFunctionCode.CLOSE_REQ_HANDLE,
        CASFunctionCode.CHECK_CAS,
    ]
    assert [g for g, _ in sent] == [generation + 1]


@pytest.mark.asyncio
async def test_async_executemany_closes_old_handle_before_binding() -> None:
    conn, cas, _ = _async_out_tran()
    cursor = conn.cursor()
    cursor._query_handle = 7
    cas.fail_on = {2}  # 0 probe, 1 CLOSE_REQ(7), 2 pre-bind probe fails

    await cursor.executemany("INSERT INTO t VALUES (?)", [("a\\b",)])

    batches = [(g, p) for g, p in cas.sent if isinstance(p, BatchExecutePacket)]
    assert [g for g, _ in batches] == [2]
    assert cas.kinds()[:3] == [(1, CheckCasPacket), (1, CloseQueryPacket), (1, CheckCasPacket)]


def _sync_autocommit_with_schema(conn: Connection) -> None:
    conn._autocommit = True
    schema = GetSchemaPacket(schema_type=1)
    schema.query_handle, schema.tuple_count, schema.columns = 4, 1, []
    conn._register_schema_result(schema)


@pytest.mark.parametrize("batch", [False, True], ids=["execute", "executemany"])
def test_sync_reconnect_between_bind_and_send_rejects_bound_sql(batch: bool) -> None:
    """Auto-committing SQL closes schema handles first; that CLOSE_REQ's OUT_TRAN
    reply is re-probed, and a recycled CAS replaces the session after binding."""
    conn, sock = make_connected_connection()  # IN_TRAN, verified: no pre-bind probe
    _sync_autocommit_with_schema(conn)
    cursor = conn.cursor()
    _script(sock, _frames(build_simple_ok_response(OUT_TRAN)))  # CLOSE_REQ; then EOF
    new = _replacement_socket()
    start = sock.sendall.call_count

    with patch("socket.create_connection", return_value=new):
        with pytest.raises(OperationalError, match="parameter binding; retry operation"):
            if batch:
                cursor.executemany("INSERT INTO t VALUES (?)", [("a\\b",)])
            else:
                cursor.execute("SELECT ?", ("a\\b",))

    assert _function_codes(sock, start) == [
        CASFunctionCode.CLOSE_REQ_HANDLE,
        CASFunctionCode.CHECK_CAS,
    ]
    assert _function_codes(new, 2) == []  # the bound SQL never reaches the new session
    assert conn._connected is True


def test_sync_stale_bound_sql_is_rejected_before_any_probe() -> None:
    conn, sock = _sync_out_tran()  # OUT_TRAN, unverified: a probe would be due
    start = sock.sendall.call_count

    with pytest.raises(OperationalError, match="parameter binding; retry operation"):
        conn._send_and_receive(
            PrepareAndExecutePacket("SELECT 'a\\\\b'"),
            bound_generation=conn._physical_generation - 1,
        )
    assert sock.sendall.call_count == start


@pytest.mark.asyncio
async def test_async_reconnect_between_bind_and_send_rejects_bound_sql() -> None:
    conn, cas, _ = _async_out_tran()
    conn._record_reply_cas_info(bytearray(IN_TRAN))  # no pre-bind probe
    conn._autocommit = True
    schema = GetSchemaPacket(schema_type=1)
    schema.query_handle, schema.tuple_count, schema.columns = 4, 1, []
    conn._register_schema_result(schema)
    cas.fail_on = {1}  # 0 schema CLOSE_REQ (OUT_TRAN reply), 1 re-probe fails
    cursor = conn.cursor()

    with pytest.raises(OperationalError, match="parameter binding; retry operation"):
        await cursor.execute("SELECT ?", ("a\\b",))

    statements = [
        p.sql
        for _, p in cas.sent
        if isinstance(p, PrepareAndExecutePacket) and "CHAR_LENGTH" not in p.sql
    ]
    assert statements == []
    assert conn._physical_generation == 2


# -- autocommit setter keeps SET_DB_PARAMETER and COMMIT on one session (#551) --


def _native_error_response(cas_info: bytes = IN_TRAN) -> bytes:
    body = cas_info + struct.pack(">ii", -1, -1) + b"denied\x00"
    return struct.pack(">i", len(body) - 4) + body


def test_sync_setter_recycle_before_commit_restores_new_value_on_replacement() -> None:
    conn, old = make_connected_connection()
    # SET_DB_PARAMETER is answered OUT_TRAN, then the CAS closes the socket.
    _script(old, _frames(build_simple_ok_response(OUT_TRAN)))
    ok = build_simple_ok_response()
    new = _replacement_socket(ok, ok)
    old_start = old.sendall.call_count

    with patch("socket.create_connection", return_value=new) as create:
        conn.autocommit = True

    create.assert_called_once()
    assert conn.autocommit is True
    assert _function_codes(old, old_start) == [
        CASFunctionCode.SET_DB_PARAMETER,
        CASFunctionCode.CHECK_CAS,
    ]
    # The replacement receives the new value before the COMMIT.
    assert _function_codes(new, 2) == [CASFunctionCode.SET_DB_PARAMETER, CASFunctionCode.END_TRAN]
    assert new.sendall.call_args_list[2].args[0][-4:] == struct.pack(">i", 1)


def test_sync_setter_replaces_the_session_at_most_once() -> None:
    conn, old = _sync_out_tran()
    _script(old, [])  # the SET_DB_PARAMETER probe finds the CAS gone
    # The replacement answers SET_DB_PARAMETER OUT_TRAN and is recycled too.
    new = _replacement_socket(build_simple_ok_response(OUT_TRAN))

    with patch("socket.create_connection", return_value=new) as create:
        with pytest.raises(OperationalError, match="autocommit change"):
            conn.autocommit = True

    create.assert_called_once()
    assert _function_codes(new, 2) == [CASFunctionCode.SET_DB_PARAMETER, CASFunctionCode.END_TRAN]
    assert conn._connected is False
    assert conn._autocommit is False
    assert conn._autocommit_explicitly_set is False


def test_sync_setter_commit_error_retires_session_and_keeps_previous_value() -> None:
    conn, sock = make_connected_connection()
    conn._autocommit_explicitly_set = True  # an earlier explicit False
    _script(sock, _frames(build_simple_ok_response(), _native_error_response()))

    with pytest.raises(OperationalError, match="autocommit change") as info:
        conn.autocommit = True

    assert type(info.value.__cause__).__name__ == "DatabaseError"
    assert conn._connected is False
    assert conn._socket is None
    assert (conn._autocommit, conn._autocommit_explicitly_set) == (False, True)


def test_sync_setter_native_set_error_keeps_session_and_value() -> None:
    conn, sock = make_connected_connection()
    _script(sock, _frames(_native_error_response()))
    start = sock.sendall.call_count

    with pytest.raises(Exception) as info:
        conn.autocommit = True

    assert not isinstance(info.value, OperationalError)
    assert _function_codes(sock, start) == [CASFunctionCode.SET_DB_PARAMETER]
    assert conn._connected is True
    assert conn._autocommit is False
    assert conn._autocommit_explicitly_set is False


def test_sync_setter_interrupted_commit_retires_session_and_keeps_previous_value() -> None:
    conn, sock = make_connected_connection()
    _script(sock, _frames(build_simple_ok_response()))
    original = conn._send_and_receive_locked

    def interrupt_commit(packet: Any, **kwargs: Any) -> Any:
        if isinstance(packet, CommitPacket):
            raise KeyboardInterrupt
        return original(packet, **kwargs)

    with patch.object(conn, "_send_and_receive_locked", interrupt_commit):
        with pytest.raises(KeyboardInterrupt):
            conn.autocommit = True

    assert conn._connected is False
    assert conn._autocommit is False
    assert conn._autocommit_explicitly_set is False


@pytest.mark.asyncio
async def test_async_setter_recycle_before_commit_restores_new_value_on_replacement() -> None:
    conn, cas, open_connection = _async_out_tran(no_backslash_escapes=True)
    cas.fail_on = {2}  # the COMMIT's CHECK_CAS finds the CAS recycled

    await conn.set_autocommit(True)

    open_connection.assert_awaited_once()
    assert conn.autocommit is True
    assert cas.kinds() == [
        (1, CheckCasPacket),
        (1, SetDbParameterPacket),
        (1, CheckCasPacket),
        (2, SetDbParameterPacket),  # restored before the COMMIT
        (2, CheckCasPacket),
        (2, CommitPacket),
    ]
    assert cas.sent[3][1].value == 1


@pytest.mark.asyncio
async def test_async_setter_replaces_the_session_at_most_once() -> None:
    conn, cas, open_connection = _async_out_tran(no_backslash_escapes=True)
    # The SET_DB_PARAMETER probe fails, then the replacement's COMMIT fails too.
    cas.fail_on = {0, 2}

    with pytest.raises(OperationalError, match="autocommit change"):
        await conn.set_autocommit(True)

    open_connection.assert_awaited_once()
    # The COMMIT follows an OUT_TRAN reply but is not probed: no second reconnect.
    assert cas.kinds() == [(1, CheckCasPacket), (2, SetDbParameterPacket), (2, CommitPacket)]
    assert conn._connected is False
    assert conn._autocommit is False
    assert conn._autocommit_explicitly_set is False


@pytest.mark.asyncio
async def test_async_setter_cancelled_commit_retires_session_and_keeps_previous_value() -> None:
    conn, cas, _ = _async_out_tran(no_backslash_escapes=True)
    conn._autocommit_explicitly_set = True

    async def cancel_commit(packet: Any) -> Any:
        if isinstance(packet, CommitPacket):
            raise asyncio.CancelledError
        return await cas(packet)

    with patch.object(conn, "_do_send_and_receive", cancel_commit):
        with pytest.raises(asyncio.CancelledError):
            await conn.set_autocommit(True)

    assert conn._connected is False
    assert (conn._autocommit, conn._autocommit_explicitly_set) == (False, True)


# -- explicit per-reply verification (#525) ------------------------------------


def test_reply_verification_is_explicit_per_reply_state() -> None:
    conn = ConnectionCommonMixin()
    conn._init_common_state(host="localhost", port=33000, database="d", user="u", password="")
    assert conn._cas_status_unverified()  # no reply proved anything yet

    conn._record_reply_cas_info(bytearray(OUT_TRAN))
    assert conn._cas_status_unverified()
    conn._mark_cas_reply_verified()
    assert not conn._cas_status_unverified()

    # The next reply needs its own proof even with the same object or bytes:
    # the CAS may close the socket after any OUT_TRAN reply.
    verified = conn._cas_info
    conn._record_reply_cas_info(verified)
    assert conn._cas_status_unverified()
    conn._mark_cas_reply_verified()
    conn._record_reply_cas_info(bytes(verified))
    assert conn._cas_status_unverified()

    conn._record_reply_cas_info(bytearray(IN_TRAN))
    assert not conn._cas_status_unverified()  # IN_TRAN is never probed


@pytest.mark.parametrize("same_object", [True, False])
def test_sync_each_out_tran_reply_is_probed_even_with_identical_cas_info(
    same_object: bool,
) -> None:
    conn, sock = _sync_out_tran()
    ok = build_simple_ok_response(OUT_TRAN)
    _script(sock, _frames(ok, ok))
    start = sock.sendall.call_count

    assert conn._check_reconnect() is False
    verified = conn._cas_info
    conn._record_reply_cas_info(verified if same_object else bytes(verified))
    assert conn._check_reconnect() is False

    assert _function_codes(sock, start) == [CASFunctionCode.CHECK_CAS] * 2
    assert conn._socket is sock


@pytest.mark.asyncio
@pytest.mark.parametrize("same_object", [True, False])
async def test_async_each_out_tran_reply_is_probed_even_with_identical_cas_info(
    same_object: bool,
) -> None:
    conn, cas, open_connection = _async_out_tran()

    assert await conn._check_reconnect() is False
    verified = conn._cas_info
    conn._record_reply_cas_info(verified if same_object else bytes(verified))
    assert await conn._check_reconnect() is False

    open_connection.assert_not_awaited()
    assert cas.kinds() == [(1, CheckCasPacket), (1, CheckCasPacket)]
