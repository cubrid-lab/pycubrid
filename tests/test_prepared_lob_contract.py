"""Internal typed FC3 LOB-handle binds and the packed-handle size field (#441).

No public LOB binding API: these pin the wire contract the native
``bind_lob()`` builds on.
"""

from __future__ import annotations

import struct
from typing import Any
from unittest.mock import MagicMock

import pytest
from hypothesis import given, strategies as st

from pycubrid import protocol
from pycubrid.compat import native
from pycubrid.constants import CUBRIDDataType, CUBRIDStatementType
from pycubrid.exceptions import InterfaceError, OperationalError, ProgrammingError
from pycubrid.lob import Lob
from pycubrid.protocol import ExecutePacket, LOBWritePacket

from .test_compat_prepared import FakeDriver, _owner, _packets, fake_driver  # noqa: F401
from .test_prepared_collection_contract import _frame, _int

BLOB, CLOB = CUBRIDDataType.BLOB, CUBRIDDataType.CLOB
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
# The same capture: `lob = con.lob(); lob.write(b"0123456789", "B");
# lob.write(b"abc", "B"); cur.bind_lob(1, lob)`. CCI rewrites the size field
# after each LOB_WRITE (cci_query_execute.c qe_lob_write), so the handle sent
# with the second write says 10 and the bound handle says 13.
_LOCATOR = b"file:ces_599/ces_temp.00001790901074695017_6946\x00"  # 48 bytes with NUL
NEW_BLOB = struct.pack(">iqi", 33, 0, len(_LOCATOR)) + _LOCATOR
OFFICIAL_SECOND_WRITE_HANDLE = struct.pack(">iqi", 33, 10, len(_LOCATOR)) + _LOCATOR
OFFICIAL_BOUND_WRITTEN_HANDLE = struct.pack(">iqi", 33, 13, len(_LOCATOR)) + _LOCATOR
OFFICIAL_WRITTEN_BIND_PAIR = "000000011700000040" + OFFICIAL_BOUND_WRITTEN_HANDLE.hex()


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
    binding = protocol._PreparedLob(lob_type, handle, 1)
    assert _bind_pair(binding) == bytes.fromhex(pair)
    frame = ExecutePacket(7, CUBRIDStatementType.INSERT, bindings=(binding,), bind_count=1).write(
        _CAS_INFO
    )
    assert bytes.fromhex(pair) in frame


def test_full_frame_mixes_lob_and_scalar_bindings() -> None:
    blob = protocol._PreparedLob(BLOB, FETCHED_BLOB, 1)
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
        bindings=(protocol._PreparedLob(CLOB, FETCHED_CLOB, 1),),
        bind_count=1,
    )
    packet.encoding = "euc-kr"
    assert FETCHED_CLOB in packet.write(_CAS_INFO)


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
        protocol._PreparedLob(type_code, handle, generation)


def test_lob_binding_is_immutable() -> None:
    binding = protocol._PreparedLob(BLOB, FETCHED_BLOB, 1)
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


def test_lob_write_updates_the_size_field_like_cci(lob_connection: MagicMock) -> None:
    lob = Lob(lob_connection, BLOB, NEW_BLOB)
    assert lob.write(b"0123456789", 0) == 10
    assert lob.lob_handle == OFFICIAL_SECOND_WRITE_HANDLE
    assert lob.write(b"abc", 10) == 3
    # Each LOB_WRITE carries the handle as it was before that write, as CCI's do.
    assert lob_connection.sent_handles == [NEW_BLOB, OFFICIAL_SECOND_WRITE_HANDLE]
    assert lob.lob_handle == OFFICIAL_BOUND_WRITTEN_HANDLE
    binding = protocol._PreparedLob(BLOB, lob.lob_handle, 1)
    assert _bind_pair(binding) == bytes.fromhex(OFFICIAL_WRITTEN_BIND_PAIR)


def test_lob_write_inside_existing_content_keeps_the_size(lob_connection: MagicMock) -> None:
    lob = Lob(lob_connection, BLOB, OFFICIAL_BOUND_WRITTEN_HANDLE)
    lob.write(b"xy", 2)
    assert lob.lob_handle == OFFICIAL_BOUND_WRITTEN_HANDLE


def test_lob_write_past_the_end_extends_the_size(lob_connection: MagicMock) -> None:
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


def test_failed_or_empty_lob_write_keeps_the_handle(lob_connection: MagicMock) -> None:
    lob = Lob(lob_connection, BLOB, NEW_BLOB)
    assert lob.write(b"", 50) == 0
    assert lob.lob_handle == NEW_BLOB
    lob_connection._send_and_receive.side_effect = OperationalError("server error")
    with pytest.raises(OperationalError):
        lob.write(b"abc", 0)
    assert lob.lob_handle == NEW_BLOB


# --- the reconnect fence in the native prepared cursor ----------------------


def test_native_execute_rejects_a_lob_binding_from_another_generation(
    fake_driver: FakeDriver,  # noqa: F811
) -> None:
    conn, cur = _owner(fake_driver)
    try:
        cur.prepare("INSERT INTO t VALUES (?)")
        # Internal binding (no public bind_lob yet) made for generation 0.
        cur._bindings[0] = protocol._PreparedLob(BLOB, FETCHED_BLOB, 0)
        with pytest.raises(InterfaceError, match="earlier physical session"):
            cur.execute()
        assert _packets(fake_driver, ExecutePacket) == []
        # The binding is not consumed: it stays unusable until replaced.
        cur._bindings[0] = protocol._PreparedLob(BLOB, FETCHED_BLOB, 1)
        assert cur.execute() == 1
        sent = _packets(fake_driver, ExecutePacket)[0]
        assert _bind_pair(sent.bindings[0]) == bytes.fromhex(OFFICIAL_BIND_PAIRS[0][2])
    finally:
        conn.close()


def test_native_cursor_module_exposes_no_lob_binding_yet() -> None:
    assert not hasattr(native.cursor, "bind_lob")
    assert not hasattr(native.cursor, "fetch_lob")
    assert not hasattr(native.connection, "lob")
