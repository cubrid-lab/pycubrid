"""Native constraint errors dispatch by code, including batch metadata (#390)."""

from __future__ import annotations

import struct
from unittest.mock import AsyncMock, MagicMock

import pytest

from pycubrid._cursor_common import _raise_batch_error
from pycubrid.aio.cursor import AsyncCursor
from pycubrid.constants import CUBRIDStatementType
from pycubrid.cursor import Cursor
from pycubrid.error_codes import get_error_description, get_sqlstate
from pycubrid.exceptions import DatabaseError, IntegrityError
from pycubrid.protocol import BatchExecutePacket, PrepareAndExecutePacket
from tests.test_protocol import DEFAULT_CAS_INFO, _build_error_response


def _batch_response(code: int, message: str, protocol_version: int) -> bytes:
    text = message.encode("utf-8") + b"\x00"
    result = -1 if protocol_version > 2 else code
    body = struct.pack(">iiBi", 0, 1, CUBRIDStatementType.INSERT, result)
    if protocol_version > 2:
        body += struct.pack(">i", code)
    body += struct.pack(">i", len(text)) + text
    if protocol_version > 4:
        body += struct.pack(">i", 0)
    return DEFAULT_CAS_INFO + body


@pytest.mark.parametrize(
    ("code", "description"),
    [(-631, "NOT NULL constraint violation"), (-922, "Foreign key constraint violation")],
)
def test_native_constraint_metadata(code: int, description: str) -> None:
    assert get_error_description(code) == description
    assert get_sqlstate(code) == "23000"


@pytest.mark.parametrize("code", [-631, -922])
@pytest.mark.parametrize("message", ["opaque native failure", "제약 오류", "syntax error"])
def test_error_packet_uses_native_code(code: int, message: str) -> None:
    packet = PrepareAndExecutePacket("INSERT INTO t VALUES (NULL)")
    with pytest.raises(IntegrityError) as raised:
        packet.parse(_build_error_response(DEFAULT_CAS_INFO, code, message))
    assert raised.value.code == code
    assert raised.value.errno == code
    assert raised.value.sqlstate == "23000"
    assert raised.value.msg == message


@pytest.mark.parametrize("code", [-631, -922])
@pytest.mark.parametrize("protocol_version", [2, 8])
def test_batch_wire_dispatch_preserves_native_metadata(code: int, protocol_version: int) -> None:
    packet = BatchExecutePacket(["INSERT INTO t VALUES (NULL)"], protocol_version=protocol_version)
    packet.parse(_batch_response(code, "opaque native failure", protocol_version))
    with pytest.raises(IntegrityError) as raised:
        _raise_batch_error(packet.errors[0])
    assert raised.value.code == code
    assert raised.value.errno == code
    assert raised.value.sqlstate == "23000"


@pytest.mark.asyncio
@pytest.mark.parametrize("asynchronous", [False, True], ids=["sync", "async"])
@pytest.mark.parametrize("operation", ["execute", "executemany", "batch"])
@pytest.mark.parametrize("code", [-631, -922])
async def test_cursor_paths_parse_and_dispatch_native_failure(
    asynchronous: bool, operation: str, code: int
) -> None:
    connection = MagicMock()
    connection._timing = None
    connection._cursors = set()
    connection._no_backslash_escapes = False
    connection._physical_generation = 1
    connection._wait_for_setup_if_needed = AsyncMock()
    connection._decode_collections = False
    connection._json_deserializer = None
    connection._protocol_version = 8
    connection.autocommit = False

    def send(packet: object, **kwargs: object) -> None:
        if isinstance(packet, PrepareAndExecutePacket):
            packet.parse(_build_error_response(DEFAULT_CAS_INFO, code, "opaque native failure"))
        else:
            assert isinstance(packet, BatchExecutePacket)
            packet.parse(_batch_response(code, "opaque native failure", 8))

    connection._send_and_receive = (
        AsyncMock(side_effect=send) if asynchronous else MagicMock(side_effect=send)
    )
    cursor = AsyncCursor(connection) if asynchronous else Cursor(connection)
    with pytest.raises(IntegrityError) as raised:
        if isinstance(cursor, AsyncCursor):
            if operation == "execute":
                await cursor.execute("INSERT INTO t VALUES (?)", [None])
            elif operation == "executemany":
                await cursor.executemany("INSERT INTO t VALUES (?)", [[None]])
            else:
                await cursor.executemany_batch(["INSERT INTO t VALUES (NULL)"])
        elif operation == "execute":
            cursor.execute("INSERT INTO t VALUES (?)", [None])
        elif operation == "executemany":
            cursor.executemany("INSERT INTO t VALUES (?)", [[None]])
        else:
            cursor.executemany_batch(["INSERT INTO t VALUES (NULL)"])
    assert raised.value.code == code
    assert raised.value.errno == code
    assert raised.value.sqlstate == "23000"
    assert connection._send_and_receive.call_count == 1


@pytest.mark.parametrize("batch", [False, True])
def test_unknown_native_code_does_not_use_constraint_message(batch: bool) -> None:
    code = -9999
    message = "unique constraint violation"
    with pytest.raises(DatabaseError) as raised:
        if batch:
            _raise_batch_error({"code": code, "message": message})
        else:
            PrepareAndExecutePacket("SELECT 1").parse(
                _build_error_response(DEFAULT_CAS_INFO, code, message)
            )
    assert type(raised.value) is DatabaseError
    assert raised.value.code == code
    assert raised.value.errno == code
    assert raised.value.sqlstate == "HY000"
