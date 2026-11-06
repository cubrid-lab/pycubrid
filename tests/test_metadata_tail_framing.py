"""Undecodable metadata cannot hide damage in the remaining reply (#591)."""

from __future__ import annotations

import asyncio
import struct
from typing import Any
from unittest.mock import MagicMock

import pytest
from hypothesis import given, settings, strategies as st

from pycubrid.constants import CUBRIDDataType
from pycubrid.exceptions import DataError, OperationalError
from pycubrid.packet import PacketReader
from pycubrid.protocol import PrepareAndExecutePacket
from tests.helpers import cas_reply
from tests.test_connection import socket_queue  # noqa: F401
from tests.test_invalid_utf8_response import (
    _async_connection_with_reply,
    _connection_with_reply,
)
from tests.test_protocol_fuzz import _sync_connection

DAMAGE = ["truncated_rows", "negative_inline_count", "truncated_tail", "partial_inline_header"]
STRUCTURAL = (ValueError, IndexError, struct.error)
CASES = [*DAMAGE, "negative_total_count"]


def _packet() -> PrepareAndExecutePacket:
    return PrepareAndExecutePacket("SELECT * FROM fuzz_t", decode_collections=True)


def _seed(rs: cas_reply.ResultSet = cas_reply.STRINGS) -> cas_reply.Seed:
    return cas_reply.prepare_and_execute_reply(rs)


def _undecodable(seed: cas_reply.Seed) -> bytearray:
    reply = bytearray(seed.data)
    reply[seed.metadata_lengths[0] + 4] = 0xFF
    return reply


def _metadata_end(seed: cas_reply.Seed) -> int:
    last = seed.metadata_lengths[-1]
    size = struct.unpack_from(">i", seed.data, last)[0]
    return last + 4 + size + 7  # remaining column flags


def _damaged(damage: str, cut: int = 5) -> bytes:
    seed = _seed()
    reply = _undecodable(seed)
    if damage == "truncated_rows":
        del reply[-cut:]
    elif damage == "negative_inline_count":
        struct.pack_into(">i", reply, seed.counts[-1], -1)
    elif damage == "negative_total_count":
        struct.pack_into(">i", reply, _metadata_end(seed), -1)
    elif damage == "partial_inline_header":
        del reply[seed.counts[-1] - 1 :]
    else:
        assert damage == "truncated_tail"
        del reply[_metadata_end(seed) + 2 :]
    return bytes(reply)


@pytest.mark.parametrize("damage", CASES)
def test_metadata_error_does_not_hide_tail_damage(damage: str) -> None:
    with pytest.raises(STRUCTURAL) as raised:
        _packet().parse(_damaged(damage))
    assert not isinstance(raised.value, DataError)


@pytest.mark.parametrize("damage", CASES)
def test_sync_metadata_tail_damage_retires_connection(
    damage: str,
    socket_queue: list[MagicMock],  # noqa: F811
) -> None:
    conn, sock = _connection_with_reply(socket_queue, _damaged(damage))
    with pytest.raises(OperationalError, match="malformed response from broker") as raised:
        conn._send_and_receive(_packet())
    assert isinstance(raised.value.__cause__, STRUCTURAL)
    assert not isinstance(raised.value.__cause__, DataError)
    assert conn._connected is False
    sock.close.assert_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", CASES)
async def test_async_metadata_tail_damage_retires_connection(damage: str) -> None:
    conn = _async_connection_with_reply(_damaged(damage))
    writer = conn._writer
    assert isinstance(writer, MagicMock)
    with pytest.raises(OperationalError, match="malformed response from broker") as raised:
        await conn._send_and_receive(_packet())
    assert isinstance(raised.value.__cause__, STRUCTURAL)
    assert not isinstance(raised.value.__cause__, DataError)
    assert conn._connected is False
    writer.close.assert_called()


def _complete_invalid_metadata_and_row() -> bytes:
    seed = _seed()
    reply = _undecodable(seed)
    first_cell = next(pos for pos in seed.lengths if pos > seed.counts[-1])
    reply[first_cell + 4] = 0xFF
    return bytes(reply)


def test_complete_reply_keeps_first_metadata_error_even_with_invalid_row_text() -> None:
    with pytest.raises(DataError, match="column metadata is not valid"):
        _packet().parse(_complete_invalid_metadata_and_row())


@pytest.mark.parametrize("use_async", [False, True])
def test_complete_undecodable_reply_keeps_session(use_async: bool) -> None:
    reply = _complete_invalid_metadata_and_row()
    if use_async:
        conn = _async_connection_with_reply(reply)
        with pytest.raises(DataError, match="column metadata is not valid"):
            asyncio.run(conn._send_and_receive(_packet()))
    else:
        conn, _ = _sync_connection(reply)
        with pytest.raises(DataError, match="column metadata is not valid"):
            conn._send_and_receive(_packet())
    assert conn._connected is True


def test_metadata_error_does_not_hide_nested_collection_damage() -> None:
    seed = _seed(cas_reply.COLLECTIONS)
    reply = _undecodable(seed)
    struct.pack_into(">i", reply, seed.element_types[0] + 1, -1)
    with pytest.raises(ValueError) as raised:
        _packet().parse(reply)
    assert not isinstance(raised.value, DataError)


def _mixed_reply(scenario: str) -> bytes:
    if scenario in ("row_then_collection", "row_then_empty_negative_collection", "row_then_lob"):

        def bad_collection(wire: cas_reply.Wire) -> None:
            wire.byte(CUBRIDDataType.INT)
            wire.count(-1)
            if scenario == "row_then_collection":
                wire.length(4)
                wire.i32(1)

        def bad_lob(wire: cas_reply.Wire) -> None:
            wire.i32(CUBRIDDataType.BLOB)
            wire.i64(1)
            wire.length(1000)  # locator does not fit this handle
            wire.raw(b"x")

        first = cas_reply.Value(CUBRIDDataType.STRING, lambda wire: wire.raw(b"\xff\x00"), None)
        second = (
            cas_reply.Value(CUBRIDDataType.BLOB, bad_lob, None)
            if scenario == "row_then_lob"
            else cas_reply.Value(CUBRIDDataType.SET, bad_collection, None)
        )
    else:
        assert scenario == "json_hook_then_truncation"
        first = cas_reply.json_('{"ok": true}', {"ok": True})
        second = cas_reply.int_(123)
    rs = cas_reply.ResultSet(
        "mixed_bad_tail",
        (
            cas_reply.Column("first", first.column_type),
            cas_reply.Column("last", second.column_type),
        ),
        ((first, second),),
    )
    reply = _undecodable(_seed(rs))
    if scenario == "json_hook_then_truncation":
        del reply[-1:]
    return bytes(reply)


@pytest.mark.parametrize(
    "scenario",
    [
        "row_then_collection",
        "row_then_empty_negative_collection",
        "row_then_lob",
        "json_hook_then_truncation",
    ],
)
@pytest.mark.parametrize("use_async", [False, True])
def test_later_cells_are_validated_without_user_hooks(scenario: str, use_async: bool) -> None:
    hook_calls: list[str] = []

    def hostile_hook(value: str) -> Any:
        hook_calls.append(value)
        raise RuntimeError("user hook must not run after invalid metadata")

    packet = _packet()
    packet.json_deserializer = hostile_hook
    # Framing validation must work even when ordinary collection results
    # would be returned as opaque bytes.
    packet.decode_collections = False
    reply = _mixed_reply(scenario)
    if use_async:
        conn = _async_connection_with_reply(reply)
        with pytest.raises(OperationalError, match="malformed response from broker"):
            asyncio.run(conn._send_and_receive(packet))
    else:
        conn, _ = _sync_connection(reply)
        with pytest.raises(OperationalError, match="malformed response from broker"):
            conn._send_and_receive(packet)
    assert conn._connected is False
    assert hook_calls == []


def test_successful_metadata_retains_application_json_hook() -> None:
    calls: list[str] = []
    marker = object()

    def hook(value: str) -> object:
        calls.append(value)
        return marker

    rs = cas_reply.ResultSet(
        "json_hook",
        (cas_reply.Column("value", CUBRIDDataType.JSON),),
        ((cas_reply.json_("{}", {}),),),
    )
    packet = _packet()
    packet.json_deserializer = hook
    packet.parse(_seed(rs).data)
    assert calls == ["{}"]
    assert packet.rows == [(marker,)]


@pytest.mark.parametrize("position", [-1, 4, True, 1.5])
def test_reader_seek_rejects_invalid_position_without_moving(position: Any) -> None:
    reader = PacketReader(b"abc")
    reader._skip_bytes(1)
    with pytest.raises(ValueError, match="position"):
        reader.seek(position)
    assert reader.mark() == 1


def test_reader_mark_seek_can_restore_and_reach_the_end() -> None:
    reader = PacketReader(b"abc")
    start = reader.mark()
    reader._skip_bytes(3)
    assert reader.mark() == 3
    reader.seek(start)
    assert reader._parse_bytes(3) == b"abc"
    reader.seek(3)
    assert reader.bytes_remaining() == 0


@given(damage=st.sampled_from(DAMAGE), cut=st.integers(1, 7))
@settings(deadline=None)
def test_two_field_metadata_and_tail_fuzz(damage: str, cut: int) -> None:
    with pytest.raises(STRUCTURAL) as raised:
        _packet().parse(_damaged(damage, cut))
    assert not isinstance(raised.value, DataError)


@given(damage=st.sampled_from(DAMAGE), cut=st.integers(1, 7), use_async=st.booleans())
@settings(deadline=None)
def test_two_field_tail_fuzz_retires_sync_and_async(damage: str, cut: int, use_async: bool) -> None:
    reply = _damaged(damage, cut)
    if use_async:
        conn = _async_connection_with_reply(reply)
        with pytest.raises(OperationalError, match="malformed response from broker"):
            asyncio.run(conn._send_and_receive(_packet()))
    else:
        # A local scripted socket keeps this property test independent of
        # function-scoped pytest fixtures.
        conn, _ = _sync_connection(reply)
        with pytest.raises(OperationalError, match="malformed response from broker"):
            conn._send_and_receive(_packet())
    assert conn._connected is False
