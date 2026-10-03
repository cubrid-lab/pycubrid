"""Internal typed FC3 LOB-handle binds and the packed-handle size field (#441).

These pin the wire contract that the native ``bind_lob()`` builds on.
"""

from __future__ import annotations

import struct
from typing import Any
from unittest.mock import MagicMock

import pytest
from hypothesis import given, strategies as st

from pycubrid import protocol
from pycubrid.constants import CUBRIDDataType, CUBRIDStatementType
from pycubrid.exceptions import InterfaceError, OperationalError, ProgrammingError
from pycubrid.lob import Lob
from pycubrid.protocol import ExecutePacket, LOBWritePacket

from .test_compat_prepared import FakeDriver, _owner, _packets, fake_driver  # noqa: F401
from .test_prepared_collection_contract import _frame, _int

BLOB, CLOB = CUBRIDDataType.BLOB, CUBRIDDataType.CLOB
OWNER = object()  # stands in for the owning driver connection
_CAS_INFO = b"\x01\x00\x00\x00"

# Captured through a TCP proxy in front of a CUBRID 11.4 broker from the pinned
# official driver (cubrid-python e75ec36, CCI 7d1eb8f): the packed handles of a
# fetched BLOB ("\x00\x01blob", 6 bytes) and CLOB ("clob 한글", 11 bytes), and the
# FC3 bind pairs `cur.bind_lob(1, lob)` sent for them. The value argument is the
# handle exactly as fetched; the type byte is BLOB (23) or CLOB (24).
FETCHED_BLOB = bytes.fromhex(
    "000000210000000000000006000000386669"
    "6c653a6365735f3339302f6462612e70343431635f6461386261632e303030303137393039"
    "30313037343435323333305f3735363100"
)
FETCHED_CLOB = bytes.fromhex(
    "00000022000000000000000b000000386669"
    "6c653a6365735f3439302f6462612e70343431635f6461386261632e303030303137393039"
    "30313037343435323433325f3735393200"
)
OFFICIAL_BIND_PAIRS = [
    (BLOB, FETCHED_BLOB, "000000011700000048" + FETCHED_BLOB.hex()),
    (CLOB, FETCHED_CLOB, "000000011800000048" + FETCHED_CLOB.hex()),
]
# The same proxy (two captures), raw: `lob = con.lob(); lob.write(first, T);
# lob.write(second, T); cur.bind_lob(1, lob)` for BLOB ("B") and CLOB ("C").
# The handles are the ones each LOB_WRITE request carried, and the pair is
# the FC3 bind. CCI raises the size field after each LOB_WRITE
# (cci_query_execute.c qe_lob_write): 0 -> 10 -> 13 and 0 -> 6 -> 9.
OFFICIAL_WRITTEN = [
    (
        BLOB,
        (b"0123456789", b"abc"),
        bytes.fromhex(
            "00000021000000000000000000000030"
            "66696c653a6365735f3539392f6365735f74656d702e3030303031373930"
            "3930313037343639353031375f3639343600"
        ),
        bytes.fromhex(
            "0000002100000000000000"
            "0a00000030"
            "66696c653a6365735f3539392f6365735f74656d702e3030303031373930"
            "3930313037343639353031375f3639343600"
        ),
        "0000000117000000400000002100000000000000"
        "0d00000030"
        "66696c653a6365735f3539392f6365735f74656d702e3030303031373930"
        "3930313037343639353031375f3639343600",
    ),
    (
        CLOB,
        ("한글".encode("utf-8"), b"abc"),
        bytes.fromhex(
            "00000022000000000000000000000030"
            "66696c653a6365735f3239362f6365735f74656d702e3030303031373930"
            "3931303830383432363430385f3034323300"
        ),
        bytes.fromhex(
            "0000002200000000000000"
            "0600000030"
            "66696c653a6365735f3239362f6365735f74656d702e3030303031373930"
            "3931303830383432363430385f3034323300"
        ),
        "0000000118000000400000002200000000000000"
        "0900000030"
        "66696c653a6365735f3239362f6365735f74656d702e3030303031373930"
        "3931303830383432363430385f3034323300",
    ),
]
NEW_BLOB = OFFICIAL_WRITTEN[0][2]
OFFICIAL_SECOND_WRITE_HANDLE = OFFICIAL_WRITTEN[0][3]
OFFICIAL_WRITTEN_BIND_PAIR = OFFICIAL_WRITTEN[0][4]
OFFICIAL_BOUND_WRITTEN_HANDLE = bytes.fromhex(OFFICIAL_WRITTEN_BIND_PAIR)[9:]


def _bind_pair(binding: Any) -> bytes:
    payload = binding.payload
    return b"\x00\x00\x00\x01" + bytes((binding.type_code,)) + _int(len(payload)) + payload


def _handle(db_type: int = 33, size: int = 0, locator: bytes = b"file:x\x00") -> bytes:
    return struct.pack(">iqi", db_type, size, len(locator)) + locator


# --- golden bytes ----------------------------------------------------------


@pytest.mark.parametrize(("lob_type", "handle", "pair"), OFFICIAL_BIND_PAIRS, ids=["blob", "clob"])
def test_bind_pair_is_byte_identical_to_the_official_driver(
    lob_type: int, handle: bytes, pair: str
) -> None:
    binding = protocol._PreparedLob(lob_type, handle, OWNER, 1)
    assert _bind_pair(binding) == bytes.fromhex(pair)
    frame = ExecutePacket(7, CUBRIDStatementType.INSERT, bindings=(binding,), bind_count=1).write(
        _CAS_INFO
    )
    assert bytes.fromhex(pair) in frame


def test_full_frame_mixes_lob_and_scalar_bindings() -> None:
    blob = protocol._PreparedLob(BLOB, FETCHED_BLOB, OWNER, 1)
    frame = ExecutePacket(
        7,
        CUBRIDStatementType.SELECT,
        bindings=(
            protocol._encode_prepared_scalar(5),
            blob,
            protocol._encode_prepared_scalar(None),
        ),
        bind_count=3,
    ).write(_CAS_INFO)
    assert frame == _frame(
        b"\x00\x00\x00\x01\x08"
        + _int(4)
        + _int(5)
        + b"\x00\x00\x00\x01\x17"
        + _int(len(FETCHED_BLOB))
        + FETCHED_BLOB
        + b"\x00\x00\x00\x01\x00"
        + _int(0)
    )


def test_lob_binding_needs_no_charset_match() -> None:
    # The handle is not text; a packet with another charset still sends it.
    packet = ExecutePacket(
        7,
        CUBRIDStatementType.INSERT,
        bindings=(protocol._PreparedLob(CLOB, FETCHED_CLOB, OWNER, 1),),
        bind_count=1,
    )
    packet.encoding = "euc-kr"
    frame = packet.write(_CAS_INFO)
    assert FETCHED_CLOB in frame


# --- validation ------------------------------------------------------------


@pytest.mark.parametrize(
    ("type_code", "handle", "generation"),
    [
        (CUBRIDDataType.CHAR, _handle(), 1),
        (True, _handle(), 1),
        (23.0, _handle(), 1),
        (BLOB, bytearray(_handle()), 1),
        (BLOB, b"", 1),
        (BLOB, _handle()[:15], 1),
        (BLOB, _handle(locator=b""), 1),
        (BLOB, _handle() + b"x", 1),
        (BLOB, _handle()[:-1], 1),
        (BLOB, _handle(locator=b"file:x"), 1),
        (BLOB, _handle(locator=b"fi\x00le\x00"), 1),
        (BLOB, _handle(db_type=34), 1),
        (CLOB, _handle(db_type=33), 1),
        (BLOB, _handle(db_type=0), 1),
        (BLOB, _handle(size=-1), 1),
        (BLOB, _handle(), True),
        (BLOB, _handle(), "1"),
        (BLOB, _handle(), None),
    ],
)
def test_malformed_lob_bindings_are_rejected(type_code: Any, handle: Any, generation: Any) -> None:
    with pytest.raises(ProgrammingError):
        protocol._PreparedLob(type_code, handle, OWNER, generation)


def test_lob_binding_needs_an_owner() -> None:
    with pytest.raises(ProgrammingError, match="owner"):
        protocol._PreparedLob(BLOB, FETCHED_BLOB, None, 1)


def test_lob_binding_is_immutable() -> None:
    binding = protocol._PreparedLob(BLOB, FETCHED_BLOB, OWNER, 1)
    with pytest.raises(AttributeError):
        setattr(binding, "packed_handle", b"")


def test_execute_packet_still_rejects_unknown_binding_objects() -> None:
    bindings: Any = (object(),)
    packet = ExecutePacket(7, CUBRIDStatementType.INSERT, bindings=bindings, bind_count=1)
    with pytest.raises(ProgrammingError, match="invalid prepared parameter encoding"):
        packet.write(_CAS_INFO)


# --- the packed handle size field ------------------------------------------


def test_size_field_helpers_read_and_rewrite_bytes_4_to_12() -> None:
    assert protocol._packed_lob_size(FETCHED_CLOB) == 11
    rewritten = protocol._packed_lob_handle_after_write(NEW_BLOB, 10)
    assert rewritten == OFFICIAL_SECOND_WRITE_HANDLE
    # Only bytes 4..12 change.
    assert rewritten[:4] == NEW_BLOB[:4] and rewritten[12:] == NEW_BLOB[12:]


def test_size_field_never_shrinks() -> None:
    assert protocol._packed_lob_handle_after_write(OFFICIAL_BOUND_WRITTEN_HANDLE, 5) == (
        OFFICIAL_BOUND_WRITTEN_HANDLE
    )


@pytest.mark.parametrize("handle", [b"", b"lob-handle", _handle()[:15], _handle() + b"x"])
def test_size_field_rewrite_leaves_unparseable_handles_alone(handle: bytes) -> None:
    assert protocol._packed_lob_handle_after_write(handle, 99) == handle


@given(st.integers(0, 2**40), st.integers(0, 2**40))
def test_size_field_is_the_running_maximum(start: int, end: int) -> None:
    handle = _handle(size=start)
    assert protocol._packed_lob_size(protocol._packed_lob_handle_after_write(handle, end)) == max(
        start, end
    )


@pytest.fixture
def lob_connection() -> MagicMock:
    connection = MagicMock()
    connection._ensure_connected = MagicMock()
    sent: list[bytes] = []

    def send_and_receive(packet: object) -> object:
        if isinstance(packet, LOBWritePacket):
            sent.append(packet.packed_lob_handle)
            packet.bytes_written = len(packet.data)
        return packet

    connection._send_and_receive = MagicMock(side_effect=send_and_receive)
    connection.sent_handles = sent
    return connection


@pytest.mark.parametrize(
    ("lob_type", "chunks", "first", "second", "pair"), OFFICIAL_WRITTEN, ids=["blob", "clob"]
)
def test_lob_write_updates_the_size_field_like_cci(
    lob_connection: MagicMock,
    lob_type: int,
    chunks: tuple[bytes, bytes],
    first: bytes,
    second: bytes,
    pair: str,
) -> None:
    lob = Lob(lob_connection, lob_type, first)
    written = lob.write(chunks[0], 0)
    assert written == len(chunks[0])
    assert lob.lob_handle == second
    written = lob.write(chunks[1], len(chunks[0]))
    assert written == len(chunks[1])
    # Each LOB_WRITE carries the handle as it was before that write, as CCI's do.
    assert lob_connection.sent_handles == [first, second]
    binding = protocol._PreparedLob(lob_type, lob.lob_handle, OWNER, 1)
    assert _bind_pair(binding) == bytes.fromhex(pair)


# Client arithmetic only: the server accepts a LOB_WRITE only at the current
# size (any other offset fails with -1016), but the size rule must not
# depend on that.
def test_size_arithmetic_for_a_write_inside_the_value(lob_connection: MagicMock) -> None:
    lob = Lob(lob_connection, BLOB, OFFICIAL_BOUND_WRITTEN_HANDLE)
    lob.write(b"xy", 2)
    assert lob.lob_handle == OFFICIAL_BOUND_WRITTEN_HANDLE


def test_size_arithmetic_for_a_write_past_the_end(lob_connection: MagicMock) -> None:
    lob = Lob(lob_connection, CLOB, _handle(db_type=34, size=3))
    lob.write(b"abc", 100)
    assert protocol._packed_lob_size(lob.lob_handle) == 103


def test_truncated_lob_write_records_the_bytes_written(lob_connection: MagicMock) -> None:
    lob = Lob(lob_connection, BLOB, NEW_BLOB)

    def partial(packet: object) -> object:
        if isinstance(packet, LOBWritePacket):
            packet.bytes_written = 2
        return packet

    lob_connection._send_and_receive.side_effect = partial
    with pytest.raises(OperationalError, match="LOB write truncated"):
        lob.write(b"hello", 4)
    # The server holds 4 + 2 bytes now; the handle says so, as CCI's would.
    assert protocol._packed_lob_size(lob.lob_handle) == 6


@pytest.mark.parametrize("reported", [6, 2**31 - 1])
def test_over_reported_lob_write_keeps_the_handle(lob_connection: MagicMock, reported: int) -> None:
    # CCI rejects bytes_written > length without touching the size
    # (cci_query_execute.c qe_lob_write); so does Lob.write().
    lob = Lob(lob_connection, BLOB, NEW_BLOB)

    def over(packet: object) -> object:
        if isinstance(packet, LOBWritePacket):
            packet.bytes_written = reported
        return packet

    lob_connection._send_and_receive.side_effect = over
    with pytest.raises(OperationalError, match="LOB write truncated"):
        lob.write(b"hello", 0)
    assert lob.lob_handle == NEW_BLOB


def test_failed_or_empty_lob_write_keeps_the_handle(lob_connection: MagicMock) -> None:
    lob = Lob(lob_connection, BLOB, NEW_BLOB)
    written = lob.write(b"", 50)
    assert written == 0
    assert lob.lob_handle == NEW_BLOB
    lob_connection._send_and_receive.side_effect = OperationalError("server error")
    with pytest.raises(OperationalError):
        lob.write(b"abc", 0)
    assert lob.lob_handle == NEW_BLOB


# --- the reconnect fence in the native prepared cursor ----------------------


def test_native_execute_rejects_a_lob_binding_from_another_session(
    fake_driver: FakeDriver,  # noqa: F811
) -> None:
    conn, cur = _owner(fake_driver)
    other = FakeDriver(autocommit=True)  # another connection, also at generation 1
    try:
        cur.prepare("INSERT INTO t VALUES (?)")
        assert other._physical_generation == fake_driver._physical_generation
        # Internal bindings (no public bind_lob yet): an earlier session of
        # this connection, then the same generation number on another one.
        for owner, generation in ((fake_driver, 0), (other, 1), (OWNER, 1)):
            cur._bindings[0] = protocol._PreparedLob(BLOB, FETCHED_BLOB, owner, generation)
            with pytest.raises(InterfaceError, match="another physical session"):
                cur.execute()
        assert _packets(fake_driver, ExecutePacket) == []
        cur._bindings[0] = protocol._PreparedLob(BLOB, FETCHED_BLOB, fake_driver, 1)
        count = cur.execute()
        assert count == 1
        sent = _packets(fake_driver, ExecutePacket)[0]
        assert _bind_pair(sent.bindings[0]) == bytes.fromhex(OFFICIAL_BIND_PAIRS[0][2])
    finally:
        conn.close()
