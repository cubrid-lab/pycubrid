from __future__ import annotations

import json
import struct
from unittest.mock import MagicMock

import pycubrid
import pytest

from pycubrid.connection import Connection
from pycubrid.constants import CUBRIDDataType, CUBRIDStatementType, DataSize
from pycubrid.cursor import Cursor
from pycubrid.packet import PacketReader
from pycubrid.protocol import ColumnMetaData, FetchPacket, PrepareAndExecutePacket, _read_value


DEFAULT_CAS_INFO = b"\x00\x01\x02\x03"


@pytest.fixture
def socket_queue(monkeypatch: pytest.MonkeyPatch) -> list[MagicMock]:
    queue: list[MagicMock] = []

    def fake_socket(*args: object, **kwargs: object) -> MagicMock:
        del args, kwargs
        if not queue:
            raise AssertionError("socket queue is empty")
        return queue.pop(0)

    monkeypatch.setattr("socket.socket", fake_socket)
    return queue


def _encode_json(value: str) -> bytes:
    return value.encode("utf-8") + b"\x00"


def _build_column_metadata(column_type: int, name: str) -> bytes:
    encoded_name = name.encode("utf-8") + b"\x00"
    buf = bytearray()
    buf.append(column_type)
    buf.extend(struct.pack(">h", 0))
    buf.extend(struct.pack(">i", 0))
    for _ in range(3):
        buf.extend(struct.pack(">i", len(encoded_name)))
        buf.extend(encoded_name)
    buf.append(0)
    buf.extend(struct.pack(">i", 0))
    buf.extend(b"\x00" * 7)
    return bytes(buf)


def _build_row(values: list[bytes | None]) -> bytes:
    row = bytearray()
    row.extend(struct.pack(">i", 0))
    row.extend(b"\x00" * DataSize.OID)
    for value in values:
        if value is None:
            row.extend(struct.pack(">i", 0))
        else:
            row.extend(struct.pack(">i", len(value)))
            row.extend(value)
    return bytes(row)


def _build_result_info(stmt_type: int, result_count: int) -> bytes:
    return (
        bytes([stmt_type])
        + struct.pack(">i", result_count)
        + (b"\x00" * DataSize.OID)
        + struct.pack(">i", 0)
        + struct.pack(">i", 0)
    )


def _build_select_response(columns: list[tuple[int, str]], row_values: list[bytes | None]) -> bytes:
    response = bytearray()
    response.extend(DEFAULT_CAS_INFO)
    response.extend(struct.pack(">i", 1))
    response.extend(struct.pack(">i", 0))
    response.append(CUBRIDStatementType.SELECT)
    response.extend(struct.pack(">i", 0))
    response.append(0)
    response.extend(struct.pack(">i", len(columns)))
    for column_type, name in columns:
        response.extend(_build_column_metadata(column_type, name))
    response.extend(struct.pack(">i", 1))
    response.append(0)
    response.extend(struct.pack(">i", 1))
    response.extend(_build_result_info(CUBRIDStatementType.SELECT, 1))
    response.append(0)
    response.extend(struct.pack(">i", 0))
    response.extend(struct.pack(">i", 0))
    response.extend(struct.pack(">i", 1))
    response.extend(_build_row(row_values))
    return bytes(response)


def test_packet_reader_json_returns_raw_string_by_default() -> None:
    payload = _encode_json('{"a": 1}')
    reader = PacketReader(payload)

    assert reader._parse_json(len(payload)) == '{"a": 1}'


def test_packet_reader_json_uses_json_loads_when_configured() -> None:
    payload = _encode_json('{"a": 1}')
    reader = PacketReader(payload, json_deserializer=json.loads)

    assert reader._parse_json(len(payload)) == {"a": 1}


def test_read_value_json_uses_custom_deserializer() -> None:
    payload = _encode_json('{"ok": true}')
    calls: list[str] = []

    def custom_deserializer(value: str) -> tuple[str, str]:
        calls.append(value)
        return ("custom", value)

    result = _read_value(
        PacketReader(payload, json_deserializer=custom_deserializer),
        CUBRIDDataType.JSON,
        len(payload),
    )

    assert result == ("custom", '{"ok": true}')
    assert calls == ['{"ok": true}']


def test_packet_reader_json_raises_for_malformed_json() -> None:
    payload = _encode_json('{"a":')
    reader = PacketReader(payload, json_deserializer=json.loads)

    with pytest.raises(json.JSONDecodeError):
        reader._parse_json(len(payload))


def test_prepare_and_execute_packet_returns_raw_json_string_when_opted_out() -> None:
    response = _build_select_response(
        [(CUBRIDDataType.JSON, "payload")],
        [_encode_json('{"a": 1}')],
    )

    packet = PrepareAndExecutePacket("SELECT payload", protocol_version=8)
    packet.parse(response)

    assert packet.rows == [('{"a": 1}',)]


def test_prepare_and_execute_packet_decodes_json_when_deserializer_is_set() -> None:
    response = _build_select_response(
        [(CUBRIDDataType.JSON, "payload")],
        [_encode_json('{"a": 1, "b": [2, 3]}')],
    )

    packet = PrepareAndExecutePacket(
        "SELECT payload",
        protocol_version=8,
        json_deserializer=json.loads,
    )
    packet.parse(response)

    assert packet.rows == [({"a": 1, "b": [2, 3]},)]


def test_fetch_packet_decodes_json_when_deserializer_is_set() -> None:
    response = bytearray()
    response.extend(DEFAULT_CAS_INFO)
    response.extend(struct.pack(">i", 0))
    response.extend(struct.pack(">i", 1))
    response.extend(_build_row([_encode_json('{"fetch": true}')]))

    packet = FetchPacket(
        1,
        0,
        columns=[ColumnMetaData(column_type=CUBRIDDataType.JSON, name="payload")],
        json_deserializer=json.loads,
    )
    packet.parse(bytes(response))

    assert packet.rows == [({"fetch": True},)]


def test_connect_passes_json_deserializer(monkeypatch: pytest.MonkeyPatch) -> None:
    captured_kwargs: dict[str, object] = {}

    class DummyConnection:
        def __init__(self, *args, **kwargs) -> None:
            del args
            captured_kwargs.update(kwargs)

    monkeypatch.setattr("pycubrid.connection.Connection", DummyConnection)

    pycubrid.connect(database="testdb", json_deserializer=json.loads)

    assert captured_kwargs["json_deserializer"] is json.loads


def test_connection_stores_json_deserializer(socket_queue: list[MagicMock]) -> None:
    open_db = (
        DEFAULT_CAS_INFO
        + struct.pack(">i", 0)
        + bytes([0, 0, 0, 0, 0x48, 0, 0, 0])
        + struct.pack(">i", 1)
    )
    frame = struct.pack(">i", len(open_db) - 4) + open_db
    sock = MagicMock()
    sock.recv.side_effect = [struct.pack(">i", 0), frame[:4], frame[4:]]

    def _recv_into(buffer: memoryview | bytearray, nbytes: int = 0) -> int:
        chunk = sock.recv(nbytes)
        n = len(chunk)
        buffer[:n] = chunk
        return n

    sock.recv_into.side_effect = _recv_into
    socket_queue.append(sock)

    conn = Connection("localhost", 33000, "testdb", "dba", "", json_deserializer=json.loads)

    assert conn._json_deserializer is json.loads
    assert conn._protocol_version == 8


def test_cursor_threads_json_deserializer_to_packets() -> None:
    connection = MagicMock()
    connection.autocommit = False
    connection._connected = True
    connection._cas_info = DEFAULT_CAS_INFO
    connection._cursors = set()
    connection._ensure_connected = MagicMock()
    connection._protocol_version = 8
    connection._decode_collections = False
    connection._json_deserializer = json.loads

    def send_and_receive(packet: object) -> object:
        if isinstance(packet, PrepareAndExecutePacket):
            assert packet.json_deserializer is json.loads
            packet.query_handle = 1
            packet.statement_type = CUBRIDStatementType.SELECT
            packet.columns = [ColumnMetaData(name="payload", column_type=CUBRIDDataType.JSON)]
            packet.total_tuple_count = 1
            packet.rows = []
            packet.result_infos = []
        elif isinstance(packet, FetchPacket):
            assert packet.json_deserializer is json.loads
            packet.rows = [({"a": 1},)]
        return packet

    connection._send_and_receive.side_effect = send_and_receive

    cursor = Cursor(connection)
    cursor.execute("SELECT payload FROM t")

    assert cursor.fetchone() == ({"a": 1},)
