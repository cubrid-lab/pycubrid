"""Outbound size and transport-failure invariants of a prepared batch (#854).

``executemany(prepared=True)`` encodes every row, then sends one ``PREPARE``
and one ``EXECUTE_ARRAY`` (FC21) frame that carries every row (#839). These
offline checks pin what a caller sees when that frame is too large for the
protocol, and when the transport fails at each step of the exchange:

* before ``PREPARE`` leaves, nothing ran on the server;
* after ``PREPARE`` but before any FC21 byte leaves, only the prepare ran;
* during a partial FC21 send, the CAS cannot have a complete request;
* after the whole FC21 frame was sent, the rows may have run (and, with
  autocommit, been committed) although no complete reply arrived.

In each transport case the physical session is retired with the cursor's
handle, the error is an ``OperationalError`` naming its cause, and no request
is ever sent again on its own: the connection stays closed until the caller
reconnects, and nothing sent after that repeats the batch. The large measured
workloads are in ``tests/test_integration_prepared_large_batch.py`` (opt-in,
``slow``).
"""

from __future__ import annotations

import asyncio
import struct
from collections.abc import Iterator
from typing import Any

import pytest

from pycubrid.constants import CASFunctionCode
from pycubrid.exceptions import DataError, InterfaceError, OperationalError
from pycubrid.packet import PacketWriter
from pycubrid.protocol import ExecuteArrayPacket, _encode_prepared_rows

from .helpers.fault_broker import framed
from .helpers.replay_broker import Reply, Request, Session
from .test_deferred_close import _Driver
from .test_execute_array import ASYNC, INSERT, _CasScript, _functions, _run

AUTOCOMMIT = pytest.mark.parametrize("autocommit", [False, True], ids=["manual", "autocommit"])
CAS_INFO = b"\x00\xff\xff\xff"
# DATA_LENGTH and CAS_INFO precede the function code of every framed request.
HEADER = 8
INT32_MAX = 2**31 - 1
ROWS = [(1, "a"), (2, "b"), (3, "c")]


def _claimed_length(data: bytes | bytearray, claimed: int) -> bytearray:
    """A writer buffer that reports ``claimed`` bytes while holding only ``data``.

    ``PacketWriter.finalize()`` frames ``len(buffer)``, so this exercises the
    int32 ``DATA_LENGTH`` limit without allocating 2 GiB.
    """

    class ClaimedLength(bytearray):
        def __len__(self) -> int:
            return claimed

    return ClaimedLength(data)


# Requests that run SQL or carry rows; session setup and probes are left out.
_STATEMENTS = ("PREPARE", "EXECUTE_ARRAY", "PREPARE_AND_EXECUTE", "EXECUTE_BATCH")


def _requests_of(requests: list[Request], session: int) -> list[str]:
    return [r.function for r in requests if r.session == session and r.function in _STATEMENTS]


def _assert_retired(d: _Driver, cursor: Any) -> None:
    assert d.conn._connected is False
    assert cursor._query_handle is None
    assert cursor.rowcount == -1
    assert cursor._batch_row_results is None


def _assert_stays_closed(d: _Driver, cursor: Any) -> None:
    """Nothing reconnects or resends on its own; the caller must reconnect."""
    with pytest.raises(InterfaceError, match="connection is closed"):
        d.run(cursor.executemany(INSERT, ROWS, prepared=True))
    d.connect()
    assert d.select(d.cursor()) == [(1,)]


# ---------------------------------------------------------------------------
# Outbound frame size
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("payload", [INT32_MAX, INT32_MAX + 1], ids=["int32-max", "over"])
def test_frame_length_is_bounded_by_the_int32_data_length(payload: int) -> None:
    writer = PacketWriter()
    writer._write_byte(CASFunctionCode.EXECUTE_ARRAY)
    writer._buffer = _claimed_length(writer._buffer, HEADER + payload)
    if payload > INT32_MAX:
        with pytest.raises(struct.error):
            writer.finalize(CAS_INFO)
    else:
        data = writer.finalize(CAS_INFO)
        assert struct.unpack(">i", data[:4])[0] == INT32_MAX


def test_frame_size_is_a_fixed_head_plus_the_same_bytes_per_row() -> None:
    """Outbound bytes are predictable from the row shape, with no hidden growth."""

    def size(count: int) -> int:
        rows = _encode_prepared_rows([(i, "v%05d" % i) for i in range(count)], "utf-8")
        packet = ExecuteArrayPacket(7, rows, auto_commit=False, bind_count=2)
        packet.encoding = "utf-8"
        return len(packet.write(CAS_INFO))

    # Function code; handle, query timeout and autocommit as sized arguments.
    head = HEADER + 1 + 8 + 8 + 5
    # Per value: a sized type byte, then the sized value (INT: 4 bytes;
    # STRING: the text and its NUL).
    per_row = (5 + 4 + 4) + (5 + 4 + len("v00000") + 1)
    for count in (1, 10, 1_000):
        assert size(count) == head + count * per_row


@ASYNC
@AUTOCOMMIT
def test_oversized_array_frame_fails_before_any_byte_is_sent(
    asynchronous: bool, autocommit: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    original = PacketWriter.finalize

    def oversized(self: PacketWriter, cas_info: bytes | bytearray) -> bytes:
        if self._header_size and self._buffer[HEADER] == CASFunctionCode.EXECUTE_ARRAY:
            self._buffer = _claimed_length(self._buffer, HEADER + INT32_MAX + 1)
        return original(self, cas_info)

    monkeypatch.setattr(PacketWriter, "finalize", oversized)

    def steps(d: _Driver) -> Any:
        cursor = d.cursor()
        generation = d.conn._physical_generation
        with pytest.raises(DataError, match="too large to serialize") as raised:
            d.run(cursor.executemany(INSERT, ROWS, prepared=True))
        assert isinstance(raised.value.__cause__, struct.error)
        # No FC21 byte left: the session is intact and the prepare handle is
        # the cursor's, released by its next statement.
        assert d.conn._connected is True
        assert d.conn._physical_generation == generation
        handle = cursor._query_handle
        assert handle is not None
        assert cursor.rowcount == -1
        assert cursor._batch_row_results is None
        assert d.select(cursor) == [(1,)]
        return handle

    requests, handle = _run(asynchronous, steps, autocommit=autocommit)
    assert {r.session for r in requests} == {0}
    functions = _functions(requests)
    assert functions[0] == "PREPARE"
    assert "EXECUTE_ARRAY" not in functions
    assert "END_TRAN" not in functions
    # Statement pooling is off, so the release is a CLOSE_REQ in both modes.
    close = [r for r in requests if r.function == "CLOSE_REQ_HANDLE"]
    assert [r.int_arg(0) for r in close] == [handle]


@ASYNC
def test_failing_row_generator_sends_nothing(asynchronous: bool) -> None:
    """A one-shot generator is consumed before any I/O; its error is not wrapped."""

    def rows() -> Iterator[tuple[int, str]]:
        yield from ROWS
        raise RuntimeError("source failed")

    def steps(d: _Driver) -> None:
        cursor = d.cursor()
        assert d.select(cursor) == [(1,)]
        handle = cursor._query_handle
        assert handle is not None
        with pytest.raises(RuntimeError, match="source failed"):
            d.run(cursor.executemany(INSERT, rows(), prepared=True))
        # Rows are encoded before the previous result is released.
        assert d.conn._connected is True
        assert cursor._query_handle == handle

    requests, _ = _run(asynchronous, steps)
    assert _functions(requests) == ["PREPARE_AND_EXECUTE"]


# ---------------------------------------------------------------------------
# Transport faults while sending
# ---------------------------------------------------------------------------


class _FaultySocket:
    """The real socket, except that ``function``'s request fails to send.

    With ``partial`` the first half of its frame is sent first; otherwise none.
    """

    def __init__(self, real: Any, function: int, partial: bool, exc: OSError) -> None:
        self._real = real
        self._function = function
        self._partial = partial
        self._exc = exc

    def sendall(self, data: bytes) -> None:
        if data[HEADER] != self._function:
            self._real.sendall(data)
            return
        if self._partial:
            self._real.sendall(data[: len(data) // 2])
        raise self._exc

    def __getattr__(self, name: str) -> Any:
        return getattr(self._real, name)


def _fail_send(d: _Driver, function: int, partial: bool) -> OSError:
    """Fail the send of ``function``'s request, after half of it when ``partial``."""
    exc = BrokenPipeError(32, "Broken pipe")
    if not d.asynchronous:
        d.conn._socket = _FaultySocket(d.conn._socket, function, partial, exc)
        return exc
    writer = d.conn._writer
    real_write, real_drain = writer.write, writer.drain
    armed = False

    def write(data: bytes) -> None:
        nonlocal armed
        if data[HEADER] != function:
            real_write(data)
            return
        armed = True
        if partial:
            real_write(data[: len(data) // 2])

    async def drain() -> None:
        await real_drain()
        if armed:
            raise exc

    writer.write = write
    writer.drain = drain
    return exc


SEND_FAULTS = pytest.mark.parametrize(
    ("function", "partial", "received"),
    [
        (CASFunctionCode.PREPARE, False, []),
        (CASFunctionCode.EXECUTE_ARRAY, False, ["PREPARE"]),
        (CASFunctionCode.EXECUTE_ARRAY, True, ["PREPARE"]),
    ],
    ids=["before-prepare", "after-prepare-before-array", "partial-array-send"],
)


@ASYNC
@AUTOCOMMIT
@SEND_FAULTS
def test_send_fault_retires_the_session_and_never_resends(
    asynchronous: bool, autocommit: bool, function: int, partial: bool, received: list[str]
) -> None:
    def steps(d: _Driver) -> None:
        cursor = d.cursor()
        exc = _fail_send(d, function, partial)
        with pytest.raises(OperationalError, match="socket communication failed") as raised:
            d.run(cursor.executemany(INSERT, ROWS, prepared=True))
        assert raised.value.__cause__ is exc
        _assert_retired(d, cursor)
        _assert_stays_closed(d, cursor)

    requests, _ = _run(asynchronous, steps, autocommit=autocommit)
    # The CAS never got a complete FC21 frame, on this session or the next.
    assert _requests_of(requests, 0) == received
    assert _requests_of(requests, 1) == ["PREPARE_AND_EXECUTE"]
    assert "EXECUTE_ARRAY" not in _functions(requests)


# ---------------------------------------------------------------------------
# Transport faults after the whole request was sent
# ---------------------------------------------------------------------------


class _CutReplyScript(_CasScript):
    """A CAS that runs the batch, then sends only ``keep`` bytes of the reply."""

    def __init__(self, keep: str) -> None:
        super().__init__()
        self.keep = keep

    def __call__(self, request: Request, session: Session) -> Reply | None:
        reply = super().__call__(request, session)
        if request.function != "EXECUTE_ARRAY" or session.number != 0:
            return reply
        assert reply is not None and reply.body is not None
        wire = framed(reply.body)
        cut = {"nothing": 0, "data-length": 4, "half": len(wire) // 2}[self.keep]
        return Reply(raw=wire[:cut], close=True)


@ASYNC
@AUTOCOMMIT
@pytest.mark.parametrize("keep", ["nothing", "data-length", "half"])
def test_reply_fault_after_send_retires_the_session_and_never_replays(
    asynchronous: bool, autocommit: bool, keep: str
) -> None:
    def steps(d: _Driver) -> None:
        cursor = d.cursor()
        with pytest.raises(OperationalError, match="connection lost during receive") as raised:
            d.run(cursor.executemany(INSERT, ROWS, prepared=True))
        # The sync driver detects EOF itself; asyncio reports it as the cause.
        if asynchronous:
            assert isinstance(raised.value.__cause__, asyncio.IncompleteReadError)
        else:
            assert raised.value.__cause__ is None
        _assert_retired(d, cursor)
        _assert_stays_closed(d, cursor)

    script = _CutReplyScript(keep)
    requests, _ = _run(asynchronous, steps, script=script, autocommit=autocommit)
    # The rows may have run (and been committed): they are sent exactly once.
    assert _requests_of(requests, 0) == ["PREPARE", "EXECUTE_ARRAY"]
    assert _requests_of(requests, 1) == ["PREPARE_AND_EXECUTE"]
    assert _functions(requests).count("EXECUTE_ARRAY") == 1
