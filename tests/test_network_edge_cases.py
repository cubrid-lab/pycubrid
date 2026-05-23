from __future__ import annotations

import asyncio
import socket
import struct
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from pycubrid.aio.connection import AsyncConnection
from pycubrid.connection import Connection
from pycubrid.exceptions import OperationalError
from pycubrid.protocol import CommitPacket


def build_handshake_response(port: int = 0) -> bytes:
    return struct.pack(">i", port)


def build_open_db_response(
    cas_info: bytes | bytearray = b"\x01\x01\x02\x03", session_id: int = 1234
) -> bytes:
    body = cas_info + struct.pack(">i", 0)
    body += b"\x00" * 8
    body += struct.pack(">i", session_id)
    return struct.pack(">i", len(body) - 4) + body


def build_simple_ok_response(cas_info: bytes | bytearray = b"\x01\x01\x02\x03") -> bytes:
    body = cas_info + struct.pack(">i", 0)
    return struct.pack(">i", len(body) - 4) + body


def make_socket_from_chunks(chunks: list[bytes]) -> MagicMock:
    sock = MagicMock()
    queue = list(chunks)

    def recv_into(buffer: memoryview | bytearray, _nbytes: int = 0) -> int:
        if not queue:
            return 0
        chunk = queue.pop(0)
        size = min(len(chunk), len(buffer))
        buffer[:size] = chunk[:size]
        if size < len(chunk):
            queue.insert(0, chunk[size:])
        return size

    sock.recv_into.side_effect = recv_into
    return sock


def make_connected_connection() -> tuple[Connection, MagicMock]:
    open_db = build_open_db_response()
    sock = make_socket_from_chunks([build_handshake_response(), open_db[:4], open_db[4:]])
    with patch("socket.create_connection", return_value=sock):
        conn = Connection("localhost", 33000, "testdb", "dba", "")
    return conn, sock


def make_mock_stream_pair(
    read_chunks: list[bytes] | None = None,
) -> tuple[MagicMock, MagicMock, MagicMock]:
    reader = MagicMock(spec=asyncio.StreamReader)
    reader.readexactly = AsyncMock(side_effect=list(read_chunks or []))
    writer = MagicMock(spec=asyncio.StreamWriter)
    writer.drain = AsyncMock()
    writer.close = MagicMock()
    writer.wait_closed = AsyncMock()
    writer.transport = MagicMock()
    mock_socket = MagicMock()
    writer.transport.get_extra_info.return_value = mock_socket
    return reader, writer, mock_socket


async def raise_timeout_and_close_coro(coro: object, timeout: float | None = None) -> None:
    del timeout
    close = getattr(coro, "close", None)
    if callable(close):
        close()
    raise asyncio.TimeoutError


class TestConnectionNetworkEdgeCases:
    def test_connection_reset_error_during_send_raises_operational_error(self) -> None:
        conn, sock = make_connected_connection()
        sock.sendall.side_effect = ConnectionResetError("reset during send")

        with pytest.raises(OperationalError, match="socket communication failed"):
            conn._send_and_receive(CommitPacket())

        assert conn._connected is False
        assert conn._socket is None

    def test_connection_reset_error_during_recv_raises_operational_error(self) -> None:
        conn, sock = make_connected_connection()
        sock.recv_into.side_effect = ConnectionResetError("reset during recv")

        with pytest.raises(OperationalError, match="socket communication failed"):
            conn._send_and_receive(CommitPacket())

        assert conn._connected is False
        assert conn._socket is None

    def test_broken_pipe_error_during_send_raises_operational_error(self) -> None:
        conn, sock = make_connected_connection()
        sock.sendall.side_effect = BrokenPipeError("broken pipe")

        with pytest.raises(OperationalError, match="socket communication failed"):
            conn._send_and_receive(CommitPacket())

        assert conn._connected is False

    def test_socket_timeout_during_connect_raises_operational_error(self) -> None:
        with patch("socket.create_connection", side_effect=socket.timeout("timed out")):
            with pytest.raises(OperationalError, match="failed to connect"):
                Connection("localhost", 33000, "testdb", "dba", "")

    def test_socket_timeout_during_query_read_raises_operational_error(self) -> None:
        conn, sock = make_connected_connection()
        sock.recv_into.side_effect = socket.timeout("timed out")

        with pytest.raises(OperationalError, match="socket communication failed"):
            conn._send_and_receive(CommitPacket())

        assert conn._connected is False
        assert conn._socket is None

    def test_partial_read_zero_bytes_raises_operational_error(self) -> None:
        conn, sock = make_connected_connection()
        sock.recv_into.side_effect = [0]

        with pytest.raises(OperationalError, match="connection lost during receive"):
            conn._send_and_receive(CommitPacket())

    def test_partial_read_fewer_bytes_than_expected_is_retried(self) -> None:
        conn, _ = make_connected_connection()
        frame = build_simple_ok_response(conn._cas_info)
        partial_sock = make_socket_from_chunks([frame[:2], frame[2:4], frame[4:7], frame[7:]])
        conn._socket = partial_sock

        packet = conn._send_and_receive(CommitPacket())

        assert packet is not None
        assert partial_sock.recv_into.call_count == 4

    def test_cas_info_inactive_triggers_reconnect_on_next_request(self) -> None:
        conn, sock = make_connected_connection()
        inactive_frame = build_simple_ok_response(b"\x00\x01\x02\x03")
        sock.recv_into.side_effect = make_socket_from_chunks(
            [inactive_frame[:4], inactive_frame[4:]]
        ).recv_into.side_effect

        conn._send_and_receive(CommitPacket())

        reconnect_sock = make_socket_from_chunks([inactive_frame[:4], inactive_frame[4:]])

        def reconnect() -> None:
            conn._socket = reconnect_sock
            conn._cas_info = b"\x01\x01\x02\x03"
            conn._connected = True

        conn.connect = MagicMock(side_effect=reconnect)
        conn._send_and_receive(CommitPacket())

        conn.connect.assert_called_once()
        assert sock.close.called

    def test_oserror_network_unreachable_during_connect_raises_operational_error(self) -> None:
        with patch("socket.create_connection", side_effect=OSError("Network is unreachable")):
            with pytest.raises(OperationalError, match="failed to connect"):
                Connection("localhost", 33000, "testdb", "dba", "")

    def test_connection_refused_during_connect_raises_operational_error(self) -> None:
        with patch("socket.create_connection", side_effect=ConnectionRefusedError("refused")):
            with pytest.raises(OperationalError, match="failed to connect"):
                Connection("localhost", 33000, "testdb", "dba", "")

    def test_connect_timeout_parameter_is_passed_to_create_connection(self) -> None:
        open_db = build_open_db_response()
        sock = make_socket_from_chunks([build_handshake_response(), open_db[:4], open_db[4:]])
        with patch("socket.create_connection", return_value=sock) as create_connection:
            Connection("localhost", 33000, "testdb", "dba", "", connect_timeout=1.25)

        create_connection.assert_called_once_with(("localhost", 33000), timeout=1.25)

    def test_read_timeout_parameter_is_applied_to_socket(self) -> None:
        open_db = build_open_db_response()
        sock = make_socket_from_chunks([build_handshake_response(), open_db[:4], open_db[4:]])
        with patch("socket.create_connection", return_value=sock):
            Connection("localhost", 33000, "testdb", "dba", "", read_timeout=4.5)

        sock.settimeout.assert_called_once_with(4.5)


class TestAsyncConnectionNetworkEdgeCases:
    @pytest.mark.asyncio
    async def test_asyncio_timeout_error_during_connect_raises_operational_error(self) -> None:
        conn = AsyncConnection("localhost", 33000, "testdb", "dba", "", connect_timeout=0.5)
        open_connection = AsyncMock(side_effect=raise_timeout_and_close_coro)

        with (
            patch(
                "pycubrid.aio.connection.asyncio.wait_for",
                new=AsyncMock(side_effect=raise_timeout_and_close_coro),
            ),
            patch("pycubrid.aio.connection.asyncio.open_connection", new=open_connection),
        ):
            with pytest.raises(OperationalError, match="could not connect"):
                await conn._open_connection("localhost", 33000)

        open_connection.assert_called_once_with("localhost", 33000)

    @pytest.mark.asyncio
    async def test_connection_reset_error_during_async_recv_raises_operational_error(self) -> None:
        conn = AsyncConnection("localhost", 33000, "testdb", "dba", "")
        conn._connected = True
        conn._cas_info = b"\x01\x01\x02\x03"
        reader, writer, _ = make_mock_stream_pair()
        reader.readexactly = AsyncMock(side_effect=ConnectionResetError("reset during recv"))
        conn._reader = reader
        conn._writer = writer

        with pytest.raises(OperationalError, match="socket communication failed"):
            await conn._send_and_receive(CommitPacket())

        assert conn._connected is False
        assert conn._writer is None

    @pytest.mark.asyncio
    async def test_partial_async_read_zero_bytes_raises_operational_error(self) -> None:
        conn = AsyncConnection("localhost", 33000, "testdb", "dba", "")
        conn._connected = True
        conn._cas_info = b"\x01\x01\x02\x03"
        reader, writer, _ = make_mock_stream_pair()
        reader.readexactly = AsyncMock(
            side_effect=asyncio.IncompleteReadError(partial=b"", expected=4)
        )
        conn._reader = reader
        conn._writer = writer

        with pytest.raises(OperationalError, match="connection lost during receive"):
            await conn._send_and_receive(CommitPacket())

    @pytest.mark.asyncio
    async def test_async_read_timeout_during_query_raises_operational_error(self) -> None:
        conn = AsyncConnection("localhost", 33000, "testdb", "dba", "", read_timeout=0.5)
        conn._connected = True
        conn._cas_info = b"\x01\x01\x02\x03"
        conn._reader, conn._writer, _ = make_mock_stream_pair()

        with patch(
            "pycubrid.aio.connection.asyncio.wait_for",
            new=AsyncMock(side_effect=raise_timeout_and_close_coro),
        ):
            with pytest.raises(OperationalError, match="read timeout"):
                await conn._send_and_receive(CommitPacket())

        assert conn._connected is False
        assert conn._writer is None

    @pytest.mark.asyncio
    async def test_partial_async_read_fewer_bytes_than_expected_is_retried(self) -> None:
        conn = AsyncConnection("localhost", 33000, "testdb", "dba", "")
        conn._connected = True
        conn._cas_info = b"\x01\x01\x02\x03"
        frame = build_simple_ok_response(b"\x01\x01\x02\x03")
        reader, writer, _ = make_mock_stream_pair([frame[:4], frame[4:]])
        conn._reader = reader
        conn._writer = writer

        packet = await conn._send_and_receive(CommitPacket())

        assert packet is not None
        assert writer.write.call_count == 1


class TestExceptionCausePreservation:
    """PEP 3134 ``__cause__`` chaining for transport timeouts (PR #3 Item 3)."""

    def test_sync_socket_timeout_preserves_cause(self) -> None:
        conn, sock = make_connected_connection()
        original = socket.timeout("timed out")
        sock.recv_into.side_effect = original

        with pytest.raises(OperationalError) as excinfo:
            conn._send_and_receive(CommitPacket())

        assert excinfo.value.__cause__ is original

    @pytest.mark.asyncio
    async def test_async_connect_handshake_timeout_preserves_cause(self) -> None:
        conn = AsyncConnection("localhost", 33000, "testdb", "dba", "", read_timeout=0.5)
        reader, writer, _ = make_mock_stream_pair()
        conn._open_connection = AsyncMock(return_value=(reader, writer))  # type: ignore[method-assign]

        async def _raise_timeout(coro: object, timeout: float | None = None) -> None:
            del timeout
            # Close the un-awaited coroutine to silence RuntimeWarning.
            close = getattr(coro, "close", None)
            if callable(close):
                close()
            raise asyncio.TimeoutError

        with patch(
            "pycubrid.aio.connection.asyncio.wait_for",
            new=AsyncMock(side_effect=_raise_timeout),
        ):
            with pytest.raises(OperationalError) as excinfo:
                await conn._connect_locked()

        assert isinstance(excinfo.value.__cause__, asyncio.TimeoutError)

    @pytest.mark.asyncio
    async def test_async_read_timeout_preserves_cause(self) -> None:
        conn = AsyncConnection("localhost", 33000, "testdb", "dba", "", read_timeout=0.5)
        conn._connected = True
        conn._cas_info = b"\x01\x01\x02\x03"
        conn._reader, conn._writer, _ = make_mock_stream_pair()

        async def _raise_timeout(coro: object, timeout: float | None = None) -> None:
            del timeout
            close = getattr(coro, "close", None)
            if callable(close):
                close()
            raise asyncio.TimeoutError

        with patch(
            "pycubrid.aio.connection.asyncio.wait_for",
            new=AsyncMock(side_effect=_raise_timeout),
        ):
            with pytest.raises(OperationalError) as excinfo:
                await conn._send_and_receive(CommitPacket())

        assert isinstance(excinfo.value.__cause__, asyncio.TimeoutError)


class TestSessionStateRestoreOnReconnect:
    """Explicit session state is re-emitted after transparent reconnect (PR #3 Item 1)."""

    def test_explicit_autocommit_is_restored_after_reconnect(self) -> None:
        conn, sock = make_connected_connection()
        ok = build_simple_ok_response(b"\x01\x01\x02\x03")
        sock.recv_into.side_effect = make_socket_from_chunks(
            [ok[:4], ok[4:], ok[:4], ok[4:]]
        ).recv_into.side_effect
        conn.autocommit = True
        assert conn._autocommit_explicitly_set is True

        inactive_frame = build_simple_ok_response(b"\x00\x01\x02\x03")
        sock.recv_into.side_effect = make_socket_from_chunks(
            [inactive_frame[:4], inactive_frame[4:]]
        ).recv_into.side_effect
        conn._send_and_receive(CommitPacket())

        reconnect_sock = make_socket_from_chunks([ok[:4], ok[4:], ok[:4], ok[4:]])

        def reconnect() -> None:
            conn._socket = reconnect_sock
            conn._cas_info = b"\x01\x01\x02\x03"
            conn._connected = True

        conn.connect = MagicMock(side_effect=reconnect)
        restore = MagicMock(wraps=conn._restore_session_state)
        conn._restore_session_state = restore  # type: ignore[method-assign]

        conn._send_and_receive(CommitPacket())

        restore.assert_called_once()
        assert reconnect_sock.sendall.called

    def test_unset_autocommit_is_not_restored(self) -> None:
        conn, sock = make_connected_connection()
        assert conn._autocommit_explicitly_set is False
        ok = build_simple_ok_response(b"\x01\x01\x02\x03")
        inactive = build_simple_ok_response(b"\x00\x01\x02\x03")
        sock.recv_into.side_effect = make_socket_from_chunks(
            [inactive[:4], inactive[4:]]
        ).recv_into.side_effect
        conn._send_and_receive(CommitPacket())

        reconnect_sock = make_socket_from_chunks([ok[:4], ok[4:]])

        def reconnect() -> None:
            conn._socket = reconnect_sock
            conn._cas_info = b"\x01\x01\x02\x03"
            conn._connected = True

        conn.connect = MagicMock(side_effect=reconnect)
        restore = MagicMock(wraps=conn._restore_session_state)
        conn._restore_session_state = restore  # type: ignore[method-assign]

        conn._send_and_receive(CommitPacket())

        restore.assert_called_once()
        assert reconnect_sock.sendall.call_count == 1

    def test_restore_failure_tears_down_connection_with_cause(self) -> None:
        conn, _ = make_connected_connection()
        conn._autocommit = True
        conn._autocommit_explicitly_set = True
        original = OperationalError("broker rejected SET")

        def _fail(*_args: object, **_kwargs: object) -> None:
            raise original

        conn._send_and_receive = _fail  # type: ignore[method-assign]

        with pytest.raises(OperationalError) as excinfo:
            conn._restore_session_state()

        assert excinfo.value.__cause__ is original
        assert conn._connected is False

    @pytest.mark.parametrize(
        "parse_error",
        [
            struct.error("unpack requires more data"),
            ValueError("malformed value"),
            IndexError("buffer overrun"),
            UnicodeDecodeError("utf-8", b"\xff", 0, 1, "invalid start byte"),
        ],
        ids=["struct_error", "value_error", "index_error", "unicode_decode_error"],
    )
    def test_restore_failure_wraps_parse_layer_exceptions(self, parse_error: Exception) -> None:
        # Pins the contract that parse-layer errors raised by
        # _send_and_receive (which would otherwise leak past the original
        # narrow OperationalError/InterfaceError/OSError catch) are also
        # caught and converted to OperationalError, leaving the connection
        # cleanly torn down. Closes Copilot review on PR #167.
        conn, _ = make_connected_connection()
        conn._autocommit = True
        conn._autocommit_explicitly_set = True

        def _fail(*_args: object, **_kwargs: object) -> None:
            raise parse_error

        conn._send_and_receive = _fail  # type: ignore[method-assign]

        with pytest.raises(OperationalError) as excinfo:
            conn._restore_session_state()

        assert excinfo.value.__cause__ is parse_error
        assert conn._connected is False

    @pytest.mark.asyncio
    async def test_async_explicit_autocommit_is_tracked(self) -> None:
        conn = AsyncConnection("localhost", 33000, "testdb", "dba", "")
        conn._connected = True
        conn._cas_info = b"\x01\x01\x02\x03"
        conn._reader, conn._writer, _ = make_mock_stream_pair()
        conn._send_and_receive = AsyncMock(return_value=MagicMock())  # type: ignore[method-assign]

        await conn.set_autocommit(True)

        assert conn._autocommit_explicitly_set is True
        assert conn._autocommit is True

    @pytest.mark.asyncio
    async def test_async_restore_skipped_when_not_explicit(self) -> None:
        conn = AsyncConnection("localhost", 33000, "testdb", "dba", "")
        conn._send_and_receive_locked = AsyncMock()  # type: ignore[method-assign]
        assert conn._autocommit_explicitly_set is False

        await conn._restore_session_state_locked()

        conn._send_and_receive_locked.assert_not_called()

    @pytest.mark.asyncio
    async def test_async_restore_failure_tears_down_with_cause(self) -> None:
        conn = AsyncConnection("localhost", 33000, "testdb", "dba", "")
        conn._connected = True
        conn._reader, conn._writer, _ = make_mock_stream_pair()
        conn._autocommit = True
        conn._autocommit_explicitly_set = True
        original = OperationalError("broker rejected SET")
        conn._send_and_receive_locked = AsyncMock(side_effect=original)  # type: ignore[method-assign]

        with pytest.raises(OperationalError) as excinfo:
            await conn._restore_session_state_locked()

        assert excinfo.value.__cause__ is original
        assert conn._connected is False
        assert conn._writer is None

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "parse_error",
        [
            struct.error("unpack requires more data"),
            ValueError("malformed value"),
            IndexError("buffer overrun"),
            UnicodeDecodeError("utf-8", b"\xff", 0, 1, "invalid start byte"),
        ],
        ids=["struct_error", "value_error", "index_error", "unicode_decode_error"],
    )
    async def test_async_restore_failure_wraps_parse_layer_exceptions(
        self, parse_error: Exception
    ) -> None:
        # Pins the async contract that parse-layer errors raised by
        # _send_and_receive_locked (which would otherwise leak past the
        # original narrow OperationalError/InterfaceError/OSError catch) are
        # also caught and converted to OperationalError, leaving the
        # connection cleanly torn down. Closes Copilot review on PR #167.
        conn = AsyncConnection("localhost", 33000, "testdb", "dba", "")
        conn._connected = True
        conn._reader, conn._writer, _ = make_mock_stream_pair()
        conn._autocommit = True
        conn._autocommit_explicitly_set = True
        conn._send_and_receive_locked = AsyncMock(side_effect=parse_error)  # type: ignore[method-assign]

        with pytest.raises(OperationalError) as excinfo:
            await conn._restore_session_state_locked()

        assert excinfo.value.__cause__ is parse_error
        assert conn._connected is False
        assert conn._writer is None


class TestMidFetchReconnect:
    """Cursor raises OperationalError when result set is lost mid-fetch (PR #3 Item 2)."""

    def test_buffered_rows_remain_accessible_after_reconnect(self) -> None:
        conn, _ = make_connected_connection()
        cursor = conn.cursor()
        cursor._description = (("col", 0, None, None, None, None, None),)
        cursor._rows = [(1,), (2,), (3,)]
        cursor._row_index = 0
        cursor._total_tuple_count = 10
        cursor._query_handle = 42

        conn._invalidate_query_handles_for_reconnect()

        assert cursor.fetchone() == (1,)
        assert cursor.fetchone() == (2,)
        assert cursor.fetchone() == (3,)

    def test_fetch_after_buffer_exhaustion_raises_operational_error(self) -> None:
        conn, _ = make_connected_connection()
        cursor = conn.cursor()
        cursor._description = (("col", 0, None, None, None, None, None),)
        cursor._rows = [(1,)]
        cursor._row_index = 0
        cursor._total_tuple_count = 5
        cursor._query_handle = 42

        conn._invalidate_query_handles_for_reconnect()

        assert cursor.fetchone() == (1,)
        with pytest.raises(OperationalError, match="result set lost"):
            cursor.fetchone()

    def test_execute_resets_invalidated_flag(self) -> None:
        conn, _ = make_connected_connection()
        cursor = conn.cursor()
        cursor._invalidated_by_reconnect = True
        cursor._query_handle = None

        cursor._connection._send_and_receive = MagicMock(  # type: ignore[method-assign]
            return_value=MagicMock(
                query_handle=99,
                statement_type=0,
                columns=[],
                total_tuple_count=0,
                rows=[],
                column_count=0,
                result_infos=[],
            )
        )
        cursor.execute("SELECT 1")

        assert cursor._invalidated_by_reconnect is False

    def test_close_resets_invalidated_flag(self) -> None:
        conn, _ = make_connected_connection()
        cursor = conn.cursor()
        cursor._invalidated_by_reconnect = True
        cursor._query_handle = None

        cursor.close()

        assert cursor._invalidated_by_reconnect is False

    @pytest.mark.asyncio
    async def test_async_fetch_after_buffer_exhaustion_raises(self) -> None:
        from pycubrid.aio.cursor import AsyncCursor

        conn = AsyncConnection("localhost", 33000, "testdb", "dba", "")
        conn._connected = True
        conn._cursors = set()
        cursor = AsyncCursor(conn)
        conn._cursors.add(cursor)
        cursor._description = (("col", 0, None, None, None, None, None),)
        cursor._rows = [(1,)]
        cursor._row_index = 0
        cursor._total_tuple_count = 5
        cursor._query_handle = 42

        conn._invalidate_query_handles_for_reconnect()

        assert await cursor.fetchone() == (1,)
        with pytest.raises(OperationalError, match="result set lost"):
            await cursor.fetchone()


class TestCloseStreamsCancelledError:
    """``_close_streams`` propagates CancelledError without leaking refs (PR #3 Item 5)."""

    @pytest.mark.asyncio
    async def test_cancelled_error_propagates_and_clears_refs(self) -> None:
        conn = AsyncConnection("localhost", 33000, "testdb", "dba", "")
        _, writer, _ = make_mock_stream_pair()
        writer.wait_closed = AsyncMock(side_effect=asyncio.CancelledError())
        conn._writer = writer
        conn._reader = MagicMock(spec=asyncio.StreamReader)

        with pytest.raises(asyncio.CancelledError):
            await conn._close_streams()

        assert conn._writer is None
        assert conn._reader is None


class TestPingSingleAttemptContract:
    """ping() reconnects+restores at most once per call (PR #3 Item 1 fix-up).

    Regression for Oracle Phase 4 BLOCKER: when CAS is inactive and the
    session-state restore fails, the previous implementation reconnected
    *twice* (once via _check_reconnect inside _send_and_receive, then again
    in the except-handler of ping). The fix splits the preflight reconnect
    from the CHECK_CAS request via allow_reconnect=False.
    """

    def test_sync_ping_inactive_cas_restore_failure_attempts_once(self) -> None:
        conn, _ = make_connected_connection()
        conn._autocommit = True
        conn._autocommit_explicitly_set = True
        conn._cas_info = b"\x00\x01\x02\x03"

        connect_calls = 0

        def fake_reconnect() -> None:
            nonlocal connect_calls
            connect_calls += 1
            conn._socket = MagicMock()
            conn._cas_info = b"\x01\x01\x02\x03"
            conn._connected = True

        conn.connect = MagicMock(side_effect=fake_reconnect)  # type: ignore[method-assign]
        restore = MagicMock(wraps=conn._restore_session_state)
        conn._restore_session_state = restore  # type: ignore[method-assign]
        conn._send_and_receive = MagicMock(  # type: ignore[method-assign]
            side_effect=OperationalError("restore SET failed")
        )

        result = conn.ping(reconnect=True)

        assert result is False
        assert connect_calls == 1, f"expected 1 connect, got {connect_calls}"
        assert restore.call_count == 1, f"expected 1 restore, got {restore.call_count}"
        assert conn._connected is False

    @pytest.mark.asyncio
    async def test_async_ping_inactive_cas_restore_failure_attempts_once(self) -> None:
        conn = AsyncConnection("localhost", 33000, "testdb", "dba", "")
        conn._connected = True
        conn._cas_info = b"\x00\x01\x02\x03"
        conn._reader, conn._writer, _ = make_mock_stream_pair()
        conn._autocommit = True
        conn._autocommit_explicitly_set = True

        connect_calls = 0
        restore_calls = 0

        async def counting_connect() -> None:
            nonlocal connect_calls
            connect_calls += 1
            conn._connected = True
            conn._cas_info = b"\x01\x01\x02\x03"

        async def fake_invoke_connect_locked() -> None:
            await counting_connect()

        async def failing_restore() -> None:
            nonlocal restore_calls
            restore_calls += 1
            await conn._close_streams()
            raise OperationalError("restore SET failed")

        conn.connect = counting_connect  # type: ignore[method-assign]
        conn._invoke_connect_locked = fake_invoke_connect_locked  # type: ignore[method-assign]
        conn._restore_session_state_locked = failing_restore  # type: ignore[method-assign]
        conn._close_streams = AsyncMock()  # type: ignore[method-assign]

        result = await conn.ping(reconnect=True)

        assert result is False
        assert connect_calls == 1, f"expected 1 connect, got {connect_calls}"
        assert restore_calls == 1, f"expected 1 restore, got {restore_calls}"


class TestAsyncPositiveRestoreOnReconnect:
    """Positive: async _check_reconnect actually re-emits SetDbParameter (PR #3 Item 1 fix-up).

    Also covers the Copilot race-condition report (PR #167 review):
    ``ping()`` must hold ``_lock`` continuously across connect+restore so a
    concurrent task cannot observe an un-restored session.
    """

    @pytest.mark.asyncio
    async def test_async_ping_holds_lock_across_reconnect_and_restore(self) -> None:
        conn = AsyncConnection("localhost", 33000, "testdb", "dba", "")
        conn._connected = False
        conn._autocommit = True
        conn._autocommit_explicitly_set = True

        connect_started = asyncio.Event()
        restore_order: list[str] = []

        async def fake_invoke_connect_locked() -> None:
            assert conn._lock.locked(), "connect must run under _lock"
            connect_started.set()
            await asyncio.sleep(0)
            conn._connected = True
            restore_order.append("connect")

        async def fake_restore_locked() -> None:
            assert conn._lock.locked(), "restore must run under same _lock hold"
            restore_order.append("restore")

        conn._invoke_connect_locked = fake_invoke_connect_locked  # type: ignore[method-assign]
        conn._restore_session_state_locked = fake_restore_locked  # type: ignore[method-assign]

        async def concurrent_lock_grabber() -> str:
            await connect_started.wait()
            await conn._lock.acquire()
            try:
                return "grabbed"
            finally:
                conn._lock.release()

        grabber = asyncio.create_task(concurrent_lock_grabber())
        result = await conn.ping(reconnect=True)
        await asyncio.wait_for(grabber, timeout=1.0)

        assert result is True
        assert restore_order == ["connect", "restore"]

    @pytest.mark.asyncio
    async def test_async_check_reconnect_invokes_restore_when_explicit(self) -> None:
        conn = AsyncConnection("localhost", 33000, "testdb", "dba", "")
        conn._connected = True
        conn._cas_info = b"\x00\x01\x02\x03"
        conn._reader, conn._writer, _ = make_mock_stream_pair()
        conn._autocommit = False
        conn._autocommit_explicitly_set = True

        sends: list[object] = []

        async def fake_invoke_connect_locked() -> None:
            conn._connected = True
            conn._cas_info = b"\x01\x01\x02\x03"

        async def fake_send_locked(packet: object, *, allow_reconnect: bool = True) -> object:
            del allow_reconnect
            sends.append(packet)
            return MagicMock()

        conn._invoke_connect_locked = fake_invoke_connect_locked  # type: ignore[method-assign]
        conn._close_streams = AsyncMock()  # type: ignore[method-assign]
        conn._send_and_receive_locked = fake_send_locked  # type: ignore[method-assign]

        await conn._check_reconnect(allow_reconnect=True)

        from pycubrid.protocol import SetDbParameterPacket

        assert len(sends) == 1
        assert isinstance(sends[0], SetDbParameterPacket)
        assert sends[0].value == 0
