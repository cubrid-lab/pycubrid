"""Reply parsing gaps found in review (#897, #900).

- An FC20 (``EXECUTE_BATCH``) row error with a negative message length is a
  malformed reply, as it already was for FC21 (``EXECUTE_ARRAY``).
- The FC40 (``GET_LAST_INSERT_ID``) type header counts in the value size; a
  two-byte header in a one-byte value is a malformed reply.
- Complete JSON text that ``json.loads`` cannot decode (an integer past the
  int-digit limit, nesting past the recursion limit) is a ``DataError`` and
  keeps the session, like invalid JSON text (#543).
- ``CUBRIDDataType.NULL`` is an alias of ``UNKNOWN``.
"""

from __future__ import annotations

import json
import struct
from unittest.mock import MagicMock

import pytest

from pycubrid.constants import CUBRIDDataType, CUBRIDStatementType
from pycubrid.exceptions import DataError, OperationalError
from pycubrid.packet import PacketReader
from pycubrid.protocol import (
    BatchExecutePacket,
    GetLastInsertIdPacket,
    PrepareAndExecutePacket,
    _read_value,
)
from tests.test_connection import socket_queue  # noqa: F401
from tests.test_invalid_utf8_response import (
    CAS_INFO,
    _async_connection_with_reply,
    _connection_with_reply,
)
from tests.test_json_decode import _build_select_response

_STMT = bytes((CUBRIDStatementType.INSERT,))
BIG_INT_JSON = b"1" * 5000
DEEP_JSON = b"[" * 100_000 + b"]" * 100_000


def _deep_json_hits_recursion_limit() -> bool:
    # Some interpreters decode this depth without RecursionError (CI saw it on
    # Python 3.15), so the real-value cases run only where json.loads fails.
    try:
        json.loads(DEEP_JSON)
    except RecursionError:
        return True
    return False


_DEEP_MARK = pytest.mark.skipif(
    not _deep_json_hits_recursion_limit(),
    reason="this interpreter's json.loads decodes the nesting depth",
)
_DEEP = pytest.param(DEEP_JSON, id="deep-nesting", marks=_DEEP_MARK)


def _big_int_json_hits_digit_limit() -> bool:
    # PYTHONINTMAXSTRDIGITS=0 (or -X int_max_str_digits=0) disables the limit.
    try:
        json.loads(BIG_INT_JSON)
    except ValueError:
        return True
    return False


_BIG_MARK = pytest.mark.skipif(
    not _big_int_json_hits_digit_limit(),
    reason="the integer string conversion limit is disabled",
)
_BIG = pytest.param(BIG_INT_JSON, id="int-digit-limit", marks=_BIG_MARK)


def _int(value: int) -> bytes:
    return struct.pack(">i", value)


def _batch_body(msg_len: int, message: bytes = b"") -> bytes:
    row = _STMT + _int(-2) + _int(-670) + _int(msg_len) + message
    return CAS_INFO + _int(0) + _int(1) + row + _int(0)


def _last_insert_id_body(value: bytes) -> bytes:
    return CAS_INFO + _int(0) + _int(len(value)) + value


# --- FC20 negative error length (#897) -----------------------------------------


@pytest.mark.parametrize("msg_len", [-1, -(2**31)])
def test_batch_reply_rejects_negative_error_length(msg_len: int) -> None:
    with pytest.raises(ValueError, match="invalid batch execute error length"):
        BatchExecutePacket(["s"]).parse(_batch_body(msg_len))


def test_batch_reply_rejects_overrunning_error_length() -> None:
    with pytest.raises(ValueError, match="invalid batch execute error length"):
        BatchExecutePacket(["s"]).parse(_batch_body(100, b"x\x00"))


def test_batch_reply_keeps_empty_and_real_error_messages() -> None:
    packet = BatchExecutePacket(["s"])
    packet.parse(_batch_body(0))
    assert packet.errors == [{"code": -670, "message": "", "index": 0}]
    packet = BatchExecutePacket(["s"])
    packet.parse(_batch_body(4, b"dup\x00"))
    assert packet.errors == [{"code": -670, "message": "dup", "index": 0}]


def test_sync_negative_batch_error_length_is_malformed(
    socket_queue: list[MagicMock],  # noqa: F811
) -> None:
    conn, _ = _connection_with_reply(socket_queue, _batch_body(-1))
    with pytest.raises(OperationalError, match="malformed response from broker"):
        conn._send_and_receive(BatchExecutePacket(["s"]))
    assert conn._connected is False


@pytest.mark.asyncio
async def test_async_negative_batch_error_length_is_malformed() -> None:
    conn = _async_connection_with_reply(_batch_body(-1))
    with pytest.raises(OperationalError, match="malformed response from broker"):
        await conn._send_and_receive(BatchExecutePacket(["s"]))
    assert conn._connected is False


# --- FC40 type header overrun (#897) -------------------------------------------


@pytest.mark.parametrize("type_byte", [0x80, 0x87, 0xFF])
def test_last_insert_id_rejects_header_longer_than_value(type_byte: int) -> None:
    # Two bytes follow so the second header byte is readable past the value.
    body = CAS_INFO + _int(0) + _int(1) + bytes((type_byte, CUBRIDDataType.NUMERIC)) + b"7\x00"
    with pytest.raises(ValueError, match="shorter than its 2-byte type"):
        GetLastInsertIdPacket().parse(body)


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (bytes((0x80, CUBRIDDataType.NUMERIC)) + b"42\x00", "42"),
        (bytes((CUBRIDDataType.NUMERIC,)) + b"42\x00", "42"),
        (bytes((0x80, CUBRIDDataType.NUMERIC)), ""),
        (bytes((CUBRIDDataType.NUMERIC,)), ""),
    ],
)
def test_last_insert_id_header_layouts(value: bytes, expected: str) -> None:
    packet = GetLastInsertIdPacket()
    packet.parse(_last_insert_id_body(value))
    assert packet.last_insert_id == expected


def test_sync_last_insert_id_header_overrun_is_malformed(
    socket_queue: list[MagicMock],  # noqa: F811
) -> None:
    body = CAS_INFO + _int(0) + _int(1) + bytes((0x80, CUBRIDDataType.NUMERIC))
    conn, _ = _connection_with_reply(socket_queue, body)
    with pytest.raises(OperationalError, match="malformed response from broker"):
        conn._send_and_receive(GetLastInsertIdPacket())
    assert conn._connected is False


@pytest.mark.asyncio
async def test_async_last_insert_id_header_overrun_is_malformed() -> None:
    body = CAS_INFO + _int(0) + _int(1) + bytes((0x80, CUBRIDDataType.NUMERIC))
    conn = _async_connection_with_reply(body)
    with pytest.raises(OperationalError, match="malformed response from broker"):
        await conn._send_and_receive(GetLastInsertIdPacket())
    assert conn._connected is False


# --- undecodable JSON (#900) ---------------------------------------------------


@pytest.mark.parametrize(
    ("text", "cause"),
    [
        pytest.param(BIG_INT_JSON, ValueError, id="int-digit-limit", marks=_BIG_MARK),
        pytest.param(DEEP_JSON, RecursionError, id="deep-nesting", marks=_DEEP_MARK),
    ],
)
def test_undecodable_json_raises_data_error(text: bytes, cause: type[BaseException]) -> None:
    payload = text + b"\x00"
    with pytest.raises(DataError, match="JSON value cannot be decoded") as raised:
        _read_value(
            PacketReader(payload, json_deserializer=json.loads),
            CUBRIDDataType.JSON,
            len(payload),
        )
    assert type(raised.value.__cause__) is cause


def _json_select_body(text: bytes) -> bytes:
    return _build_select_response([(CUBRIDDataType.JSON, "payload")], [text + b"\x00"])


@pytest.mark.parametrize("text", [_BIG, _DEEP])
def test_sync_undecodable_json_keeps_connection(
    socket_queue: list[MagicMock],  # noqa: F811
    text: bytes,
) -> None:
    conn, _ = _connection_with_reply(socket_queue, _json_select_body(text))
    with pytest.raises(DataError, match="JSON value cannot be decoded"):
        conn._send_and_receive(
            PrepareAndExecutePacket("SELECT payload FROM t", json_deserializer=json.loads)
        )
    assert conn._connected is True
    assert conn._socket is not None


@pytest.mark.asyncio
@pytest.mark.parametrize("text", [_BIG, _DEEP])
async def test_async_undecodable_json_keeps_connection(text: bytes) -> None:
    conn = _async_connection_with_reply(_json_select_body(text))
    with pytest.raises(DataError, match="JSON value cannot be decoded"):
        await conn._send_and_receive(
            PrepareAndExecutePacket("SELECT payload FROM t", json_deserializer=json.loads)
        )
    assert conn._connected is True
    assert conn._writer is not None


@pytest.mark.parametrize(
    "error",
    [
        ValueError("Exceeds the limit (4300 digits) for integer string conversion"),
        RecursionError("maximum recursion depth exceeded while decoding a JSON array"),
    ],
    ids=["value-error", "recursion-error"],
)
def test_json_loads_failure_raises_data_error(
    monkeypatch: pytest.MonkeyPatch, error: BaseException
) -> None:
    # Check both branches whatever the interpreter configuration: the built-in
    # deserializer is recognized by identity, so patch json.loads itself.
    def _fail(_value: str) -> None:
        raise error

    monkeypatch.setattr(json, "loads", _fail)
    payload = b"[[]]\x00"
    with pytest.raises(DataError, match="JSON value cannot be decoded") as raised:
        _read_value(
            PacketReader(payload, json_deserializer=json.loads),
            CUBRIDDataType.JSON,
            len(payload),
        )
    assert raised.value.__cause__ is error


def test_invalid_json_text_message_is_unchanged() -> None:
    payload = b'{"a":\x00'
    with pytest.raises(DataError, match="not valid JSON") as raised:
        _read_value(
            PacketReader(payload, json_deserializer=json.loads),
            CUBRIDDataType.JSON,
            len(payload),
        )
    assert isinstance(raised.value.__cause__, json.JSONDecodeError)


# --- NULL / UNKNOWN alias (#900) -----------------------------------------------


def test_null_type_code_is_an_alias_of_unknown() -> None:
    assert CUBRIDDataType.NULL is CUBRIDDataType.UNKNOWN
    assert CUBRIDDataType(0) is CUBRIDDataType.UNKNOWN
    assert CUBRIDDataType(0).name == "UNKNOWN"
    assert "NULL" in CUBRIDDataType.__members__
