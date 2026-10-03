"""A conversion error cannot hide malformed later collection elements (#595)."""

from __future__ import annotations

import asyncio
import struct

import pytest

from pycubrid.constants import CUBRIDDataType as T
from pycubrid.constants import CUBRIDStatementType
from pycubrid.exceptions import DataError, OperationalError
from pycubrid.packet import PacketReader
from pycubrid.protocol import ExecutePacket, PrepareAndExecutePacket
from tests.helpers import cas_reply
from tests.test_invalid_utf8_response import _async_connection_with_reply
from tests.test_protocol_fuzz import _sync_connection

DAMAGE = ["short_date", "long_date", "truncated_header", "truncated_payload"]


@pytest.mark.parametrize("size", [1, 4])
def test_collection_header_cannot_read_into_following_bytes(size: int) -> None:
    reader = PacketReader(bytes([T.INT]) + struct.pack(">i", 0), decode_collections=True)
    with pytest.raises(ValueError, match="truncated header"):
        reader._parse_collection(size)
    assert reader.mark() == 0


def _payload(damage: str | None = None) -> bytes:
    first = bytes([T.DATE]) + struct.pack(">ii3h", 2, 6, 0, 0, 0)
    if damage == "short_date":
        return first + struct.pack(">i2h", 4, 2026, 1)
    if damage == "long_date":
        return first + struct.pack(">i4h", 8, 2026, 1, 2, 0)
    if damage == "truncated_header":
        return first + b"\x00\x00"
    if damage == "truncated_payload":
        return first + struct.pack(">i2h", 6, 2026, 1)
    return first + struct.pack(">i3h", 6, 2026, 13, 1)


def _reply(kind: str, bad_metadata: bool, damage: str | None) -> bytes:
    value = cas_reply.Value(T.SEQUENCE, lambda wire: wire.raw(_payload(damage)), None)
    rs = cas_reply.ResultSet(
        "collection_error_remainder",
        (cas_reply.Column("dates", T.SEQUENCE, element_type=T.DATE),),
        ((value,),),
    )
    seed = (
        cas_reply.prepare_and_execute_reply(rs)
        if kind == "fc41"
        else cas_reply.execute_reply(rs, refresh_columns=True)
    )
    reply = bytearray(seed.data)
    if bad_metadata:
        reply[seed.metadata_lengths[0] + 4] = 0xFF
    return bytes(reply)


def _packet(kind: str) -> PrepareAndExecutePacket | ExecutePacket:
    if kind == "fc41":
        return PrepareAndExecutePacket("SELECT dates FROM t", decode_collections=True)
    return ExecutePacket(5, CUBRIDStatementType.SELECT, decode_collections=True)


@pytest.mark.parametrize("damage", DAMAGE)
def test_data_error_does_not_hide_later_collection_damage(damage: str) -> None:
    payload = _payload(damage)
    with pytest.raises((ValueError, IndexError, struct.error)) as raised:
        PacketReader(payload, decode_collections=True)._parse_collection(len(payload))
    assert not isinstance(raised.value, DataError)


@pytest.mark.parametrize("kind", ["fc41", "fc3"])
@pytest.mark.parametrize("bad_metadata", [False, True])
@pytest.mark.parametrize("use_async", [False, True])
@pytest.mark.parametrize("damage", DAMAGE)
def test_malformed_collection_after_data_error_retires_session(
    kind: str, bad_metadata: bool, use_async: bool, damage: str
) -> None:
    reply = _reply(kind, bad_metadata, damage)
    if use_async:
        conn = _async_connection_with_reply(reply)
        with pytest.raises(OperationalError, match="malformed response from broker") as raised:
            asyncio.run(conn._send_and_receive(_packet(kind)))
    else:
        conn, _ = _sync_connection(reply)
        with pytest.raises(OperationalError, match="malformed response from broker") as raised:
            conn._send_and_receive(_packet(kind))
    assert not isinstance(raised.value.__cause__, DataError)
    assert conn._connected is False


def test_complete_collection_preserves_first_conversion_error_and_cause() -> None:
    payload = _payload()
    with pytest.raises(DataError, match="CUBRID DATE value \\(0, 0, 0\\)") as raised:
        PacketReader(payload, decode_collections=True)._parse_collection(len(payload))
    assert isinstance(raised.value.__cause__, ValueError)


@pytest.mark.parametrize("kind", ["fc41", "fc3"])
@pytest.mark.parametrize("bad_metadata", [False, True])
@pytest.mark.parametrize("use_async", [False, True])
def test_complete_collection_keeps_error_and_usable_session(
    kind: str, bad_metadata: bool, use_async: bool
) -> None:
    reply = _reply(kind, bad_metadata, None)
    message = "column metadata is not valid" if bad_metadata else r"CUBRID DATE value \(0, 0, 0\)"
    if use_async:
        conn = _async_connection_with_reply(reply)
        with pytest.raises(DataError, match=message):
            asyncio.run(conn._send_and_receive(_packet(kind)))
    else:
        conn, _ = _sync_connection(reply)
        with pytest.raises(DataError, match=message):
            conn._send_and_receive(_packet(kind))
    assert conn._connected is True


def test_opaque_collection_contract_is_unchanged() -> None:
    payload = _payload("short_date")
    assert PacketReader(payload)._parse_collection(len(payload)) == payload


@pytest.mark.parametrize("element_type", [T.SEQUENCE, 127])
def test_unsupported_collection_members_remain_opaque(element_type: int) -> None:
    payload = bytes([element_type]) + struct.pack(">i", 1) + b"opaque"
    assert PacketReader(payload, decode_collections=True)._parse_collection(len(payload)) == payload
