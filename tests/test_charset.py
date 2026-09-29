"""Connection ``charset`` option: validation, request encoding, reply decoding (#86)."""

from __future__ import annotations

import socket
import struct
import warnings
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from pycubrid import _connection_common, connect
from pycubrid._connection_common import (
    KNOWN_CONNECTION_OPTIONS,
    _codec_is_ascii_safe,
    resolve_charset,
)
from pycubrid.aio import connect as aio_connect
from pycubrid.aio.connection import AsyncConnection
from pycubrid.compat import cubriddb
from pycubrid.connection import Connection
from pycubrid.constants import CUBRIDDataType, CUBRIDStatementType
from pycubrid.exceptions import DataError, ProgrammingError
from pycubrid.packet import PacketReader, PacketWriter
from pycubrid.protocol import (
    BatchExecutePacket,
    ExecutePacket,
    GetSchemaPacket,
    OpenDatabasePacket,
    PrepareAndExecutePacket,
    PreparePacket,
    _encode_prepared_scalar,
    _parse_column_metadata,
    _parse_schema_column_metadata,
    _PreparedScalar,
    _read_value,
)
from tests.test_connection import (  # noqa: F401
    build_handshake_response,
    build_open_db_response,
    make_connected_connection,
    make_socket,
    socket_queue,
)
from tests.test_json_decode import _build_row, _build_select_response

CAS_INFO = b"\x01\x02\x03\x04"
HANGUL = "한글"
EUC_HANGUL = HANGUL.encode("euc-kr")  # b"\xc7\xd1\xb1\xdb"


def _frame(body: bytes) -> bytes:
    return struct.pack(">i", len(body) - 4) + body


def _metadata_entry(column_type: int, name: bytes, default: bytes = b"") -> bytes:
    """One FC2/FC41 column metadata entry with raw (already encoded) names."""
    buf = bytearray((column_type,))
    buf += struct.pack(">hi", 0, 0)
    for text in (name, name, b"t"):
        buf += struct.pack(">i", len(text) + 1) + text + b"\x00"
    buf.append(0)  # is_non_null
    buf += struct.pack(">i", len(default) + 1) + default + b"\x00"
    buf += b"\x00" * 7
    return bytes(buf)


def _select_body(columns: list[tuple[int, bytes]], values: list[bytes | None]) -> bytes:
    """A complete FC41 SELECT reply with raw column names and one row."""
    body = bytearray(CAS_INFO)
    body += struct.pack(">ii", 1, 0)
    body.append(CUBRIDStatementType.SELECT)
    body += struct.pack(">i", 0)
    body.append(0)
    body += struct.pack(">i", len(columns))
    for column_type, name in columns:
        body += _metadata_entry(column_type, name)
    body += struct.pack(">i", 1)
    body.append(0)
    body += struct.pack(">i", 1)
    body.append(CUBRIDStatementType.SELECT)
    body += struct.pack(">i", 1) + b"\x00" * 8 + struct.pack(">ii", 0, 0)
    body.append(0)
    body += struct.pack(">i", 0)
    body += struct.pack(">ii", 0, 1)
    body += _build_row(values)
    return bytes(body)


# --- option validation -------------------------------------------------------


@pytest.mark.parametrize(
    ("given", "codec"),
    [
        ("utf-8", "utf-8"),
        ("UTF-8", "utf-8"),
        ("utf8", "utf-8"),
        ("UTF8", "utf-8"),
        ("euckr", "euc_kr"),
        ("EUCKR", "euc_kr"),
        ("euc-kr", "euc_kr"),
        ("ksc5601", "euc_kr"),
        ("iso88591", "iso8859-1"),
        ("latin-1", "iso8859-1"),
        ("cp1252", "cp1252"),
        ("ascii", "ascii"),
        (None, "utf-8"),
        ("ko_KR.euckr", "euc_kr"),
        ("en_US.utf8", "utf-8"),
        ("en_US.iso88591", "iso8859-1"),
    ],
)
def test_charset_aliases_normalize_to_python_codecs(given: str | None, codec: str) -> None:
    assert resolve_charset(given) == codec


@pytest.mark.parametrize(
    "rejected",
    [
        "utf-16",
        "utf-16-le",
        "utf-32",
        "utf-7",
        "utf-8-sig",
        "shift_jis",
        "big5",
        "gbk",
        "gb18030",
        "cp949",
        "johab",
        "iso2022_kr",
        "hz",
        "rot13",
        "base64",
        "idna",
        "unicode_escape",
    ],
)
def test_non_ascii_compatible_codecs_are_rejected(rejected: str) -> None:
    with pytest.raises(ValueError, match="not ASCII-compatible"):
        resolve_charset(rejected)


def test_unknown_binary_and_non_string_charsets_are_rejected() -> None:
    with pytest.raises(ValueError, match="unknown charset 'klingon'"):
        resolve_charset("klingon")
    with pytest.raises(ValueError, match="'binary' has no text codec"):
        resolve_charset("BINARY")
    with pytest.raises(TypeError, match="charset must be a string"):
        resolve_charset(b"utf8")


@pytest.mark.parametrize("codec", sorted(_connection_common._KNOWN_ASCII_SAFE_CODECS))
def test_known_safe_codecs_pass_the_full_scan(codec: str, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(_connection_common, "_KNOWN_ASCII_SAFE_CODECS", frozenset())
    assert _codec_is_ascii_safe.__wrapped__(codec) is True


def test_charset_is_a_known_option_and_warns_nothing(
    socket_queue: list[MagicMock],  # noqa: F811
) -> None:
    assert "charset" in KNOWN_CONNECTION_OPTIONS
    open_db = build_open_db_response()
    socket_queue.append(make_socket([build_handshake_response(), open_db[:4], open_db[4:]]))
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        conn = connect(database="testdb", charset="euckr")
    assert conn._encoding == "euc_kr"


@pytest.mark.parametrize(
    ("charset", "error"),
    [("utf-16", ValueError), ("nope", ValueError), (b"utf8", TypeError)],
)
def test_invalid_charset_fails_before_any_socket_work(
    charset: Any, error: type[Exception], monkeypatch: pytest.MonkeyPatch
) -> None:
    def no_socket(*args: object, **kwargs: object) -> None:
        raise AssertionError("socket opened before charset validation")

    monkeypatch.setattr(socket, "create_connection", no_socket)
    with pytest.raises(error):
        connect(database="testdb", charset=charset)
    with pytest.raises(error):
        AsyncConnection("localhost", 33000, "testdb", "dba", "", charset=charset)
    with pytest.raises(error):
        cubriddb.Connection("CUBRID:localhost:33000:testdb:::", charset=charset)


@pytest.mark.asyncio
async def test_async_connect_validates_charset_before_opening_a_stream(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    opened = AsyncMock()
    monkeypatch.setattr("asyncio.open_connection", opened)
    with pytest.raises(ValueError, match="not ASCII-compatible"):
        await aio_connect(database="testdb", charset="utf-32")
    opened.assert_not_called()


def test_unencodable_credentials_fail_before_any_socket_work(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(socket, "create_connection", MagicMock(side_effect=AssertionError))
    with pytest.raises(
        DataError,
        match=r"user cannot be encoded as iso8859-1 \(unencodable character at position 0\)",
    ):
        connect(database="testdb", user=HANGUL, charset="latin-1")
    with pytest.raises(DataError, match="password cannot be encoded"):
        AsyncConnection("localhost", 33000, "testdb", "dba", "\U0001f600", charset="euckr")


# --- request encoding ----------------------------------------------------------

# Captured from main before #86: the default charset must not change one byte.
_GOLDEN_PREPARE_AND_EXECUTE = bytes.fromhex(
    "0000006f010203042900000004000000030000002b53454c4543542027ed959ceab880272c2027c3a9"
    "272046524f4d20742057484552452076203d202778270000000001000000000101000000010200"
    "000004000000000000000400000000000000000000000800000000000000000000000400000000"
)
_GOLDEN_BATCH = bytes.fromhex(
    "000000410102030414000000010000000004000000000000001d494e5345525420494e544f2074"
    "2056414c554553202827ed959c2729000000000e44454c4554452046524f4d207400"
)
_GOLDEN_SCHEMA = bytes.fromhex(
    "000000260102030409000000040000000100000004ed919c0000000004ec97b40000000001010000000400000000"
)
# 628 bytes: three NUL-padded 32-byte names, then 532 zero bytes.
_GOLDEN_OPEN_DATABASE = (
    b"testdb".ljust(32, b"\x00") + b"dba".ljust(32, b"\x00") + b"db-pw".ljust(32, b"\x00")
) + bytes(532)


def test_default_charset_requests_are_byte_for_byte_unchanged() -> None:
    sql = "SELECT '한글', 'é' FROM t WHERE v = 'x'"
    written = PrepareAndExecutePacket(sql, auto_commit=True).write(CAS_INFO)
    assert written == _GOLDEN_PREPARE_AND_EXECUTE
    batch = BatchExecutePacket(["INSERT INTO t VALUES ('한')", "DELETE FROM t"])
    written = batch.write(CAS_INFO)
    assert written == _GOLDEN_BATCH
    written = GetSchemaPacket(1, "표", arg2="열").write(CAS_INFO)
    assert written == _GOLDEN_SCHEMA
    open_db = OpenDatabasePacket("testdb", "dba", "db-pw", encoding="utf-8").write()
    assert open_db == _GOLDEN_OPEN_DATABASE


def test_default_connection_stamps_utf8_and_sends_golden_bytes(
    socket_queue: list[MagicMock],  # noqa: F811
) -> None:
    conn, sock = make_connected_connection(socket_queue)
    conn._cas_info = CAS_INFO  # IN_TRAN: no CHECK_CAS probe first
    reply = _frame(CAS_INFO + struct.pack(">i", 0))
    sock.recv.side_effect = [reply[:4], reply[4:]]
    sock.sendall.reset_mock()
    packet = PrepareAndExecutePacket("SELECT '한글', 'é' FROM t WHERE v = 'x'", auto_commit=True)
    try:
        conn._send_and_receive(packet)
    except Exception:  # noqa: BLE001 - only the request bytes matter here
        pass
    assert packet.encoding == "utf-8"
    assert sock.sendall.call_args_list[0].args[0] == _GOLDEN_PREPARE_AND_EXECUTE


def test_request_text_uses_the_connection_codec() -> None:
    packet = PrepareAndExecutePacket(f"SELECT '{HANGUL}'")
    packet.encoding = "euc_kr"
    written = packet.write(CAS_INFO)
    assert b"'" + EUC_HANGUL + b"'\x00" in written
    batch = BatchExecutePacket([f"INSERT INTO t VALUES ('{HANGUL}')"])
    batch.encoding = "euc_kr"
    written = batch.write(CAS_INFO)
    assert EUC_HANGUL in written
    schema = GetSchemaPacket(1, HANGUL)
    schema.encoding = "euc_kr"
    written = schema.write(CAS_INFO)
    assert struct.pack(">i", 5) + EUC_HANGUL + b"\x00" in written


def test_unencodable_text_raises_data_error_without_echoing_it() -> None:
    writer = PacketWriter(encoding="euc_kr")
    with pytest.raises(DataError) as raised:
        writer._write_null_terminated_string("SELECT 'secret \U0001f600'")
    assert str(raised.value) == (
        "text cannot be encoded as euc_kr (unencodable character at position 15)"
    )
    assert "secret" not in repr(raised.value.__cause__) + repr(raised.value.__context__)
    assert len(writer) == 0
    with pytest.raises(DataError, match="cannot be encoded as UTF-8"):
        PacketWriter()._write_null_terminated_string("\ud800")


@pytest.mark.parametrize(
    ("encoding", "value", "expected"),
    [
        # 17 two-byte characters (34 bytes) keep 16 whole characters.
        ("euc_kr", "가" * 17, ("가" * 16).encode("euc-kr")),
        # 11 three-byte characters (33 bytes) keep 10 (30 bytes) plus filler.
        ("utf-8", "가" * 11, ("가" * 10).encode("utf-8") + b"\x00\x00"),
        ("utf-8", "a" * 31 + "é", b"a" * 31 + b"\x00"),
        ("utf-8", "a" * 40, b"a" * 32),
        ("euc_kr", "dba", b"dba" + b"\x00" * 29),
    ],
)
def test_open_database_names_cut_on_character_boundaries(
    encoding: str, value: str, expected: bytes
) -> None:
    written = OpenDatabasePacket("db", value, "", encoding=encoding).write()
    assert len(written) == 628
    assert written[32:64] == expected


def test_open_database_uses_the_connection_codec(
    socket_queue: list[MagicMock],  # noqa: F811
) -> None:
    open_db = build_open_db_response()
    sock = make_socket([build_handshake_response(), open_db[:4], open_db[4:]])
    socket_queue.append(sock)
    Connection("localhost", 33000, "testdb", HANGUL, "", charset="euckr")
    sent = sock.sendall.call_args_list[1].args[0]
    assert sent[32:64] == EUC_HANGUL + b"\x00" * 28


def test_unencodable_parameter_sends_no_bytes_and_keeps_the_session(
    socket_queue: list[MagicMock],  # noqa: F811
) -> None:
    open_db = build_open_db_response()
    sock = make_socket([build_handshake_response(), open_db[:4], open_db[4:]])
    socket_queue.append(sock)
    conn = Connection("localhost", 33000, "testdb", "dba", "", charset="euckr")
    conn._cas_info = CAS_INFO  # IN_TRAN: no CHECK_CAS probe first
    sock.sendall.reset_mock()
    cursor = conn.cursor()
    with pytest.raises(DataError, match="cannot be encoded as euc_kr"):
        cursor.execute("INSERT INTO t VALUES (?)", ("\U0001f600",))
    sock.sendall.assert_not_called()
    assert conn._connected is True
    assert cursor._query_handle is None


def test_reconnect_keeps_the_connection_codec(
    socket_queue: list[MagicMock],  # noqa: F811
) -> None:
    for _ in range(2):
        open_db = build_open_db_response()
        socket_queue.append(make_socket([build_handshake_response(), open_db[:4], open_db[4:]]))
    first = socket_queue[0]
    second = socket_queue[1]
    conn = Connection("localhost", 33000, "testdb", HANGUL, "", charset="euckr")
    conn._drop_connection()
    conn.connect()
    assert conn._encoding == "euc_kr"
    for sock in (first, second):
        assert sock.sendall.call_args_list[1].args[0][32:36] == EUC_HANGUL


@pytest.mark.asyncio
async def test_async_requests_use_the_connection_codec() -> None:
    reply = _frame(CAS_INFO + struct.pack(">i", 0))
    conn = AsyncConnection("localhost", 33000, "testdb", "dba", "", charset="euckr")
    conn._connected = True
    conn._cas_info = CAS_INFO
    reader = MagicMock()
    reader.readexactly = AsyncMock(side_effect=[reply[:4], reply[4:]])
    writer = MagicMock()
    writer.drain = AsyncMock()
    conn._reader = reader
    conn._writer = writer
    packet = BatchExecutePacket([f"INSERT INTO t VALUES ('{HANGUL}')"])
    try:
        await conn._send_and_receive(packet)
    except Exception:  # noqa: BLE001 - only the request bytes matter here
        pass
    assert packet.encoding == "euc_kr"
    assert EUC_HANGUL in writer.write.call_args.args[0]


@pytest.mark.asyncio
async def test_async_unencodable_text_sends_nothing() -> None:
    conn = AsyncConnection("localhost", 33000, "testdb", "dba", "", charset="latin-1")
    conn._connected = True
    conn._cas_info = CAS_INFO
    conn._reader = MagicMock()
    writer = MagicMock()
    writer.drain = AsyncMock()
    conn._writer = writer
    with pytest.raises(DataError, match="cannot be encoded as iso8859-1"):
        await conn._send_and_receive(PrepareAndExecutePacket(f"SELECT '{HANGUL}'"))
    writer.write.assert_not_called()
    assert conn._connected is True


# --- reply decoding --------------------------------------------------------------


@pytest.mark.parametrize(
    "column_type",
    [
        CUBRIDDataType.CHAR,
        CUBRIDDataType.STRING,
        CUBRIDDataType.NCHAR,
        CUBRIDDataType.VARNCHAR,
        CUBRIDDataType.ENUM,
    ],
)
def test_text_values_decode_with_the_connection_codec(column_type: int) -> None:
    payload = EUC_HANGUL + b"\x00"
    reader = PacketReader(payload, encoding="euc_kr")
    assert _read_value(reader, column_type, len(payload)) == HANGUL


def test_undecodable_text_value_names_the_codec() -> None:
    payload = b"\xc7\x00"  # a lone EUC-KR lead byte
    with pytest.raises(DataError, match=r"column value is not valid euc_kr \(invalid byte"):
        _read_value(PacketReader(payload, encoding="euc_kr"), CUBRIDDataType.STRING, 2)
    # EUC-KR bytes read by a default UTF-8 client fail loudly, not silently.
    payload = EUC_HANGUL + b"\x00"
    with pytest.raises(DataError, match="column value is not valid UTF-8"):
        _read_value(PacketReader(payload), CUBRIDDataType.STRING, len(payload))


def test_collection_elements_decode_with_the_connection_codec() -> None:
    element = EUC_HANGUL + b"\x00"
    payload = struct.pack(">Bi", CUBRIDDataType.STRING, 1) + struct.pack(">i", len(element))
    payload += element
    reader = PacketReader(payload, decode_collections=True, encoding="euc_kr")
    assert _read_value(reader, CUBRIDDataType.SET, len(payload)) == frozenset({HANGUL})


def test_json_stays_utf8_whatever_the_connection_codec() -> None:
    payload = '{"k": "한글"}'.encode("utf-8") + b"\x00"
    reader = PacketReader(payload, encoding="euc_kr")
    assert _read_value(reader, CUBRIDDataType.JSON, len(payload)) == '{"k": "한글"}'
    bad = EUC_HANGUL + b"\x00"
    with pytest.raises(DataError, match="JSON value is not valid UTF-8"):
        _read_value(PacketReader(bad, encoding="euc_kr"), CUBRIDDataType.JSON, len(bad))


def test_numeric_and_timezone_text_stay_utf8() -> None:
    numeric = b"12.50\x00"
    reader = PacketReader(numeric, encoding="euc_kr")
    assert str(_read_value(reader, CUBRIDDataType.NUMERIC, len(numeric))) == "12.50"
    tz = struct.pack(">6h", 2024, 1, 2, 3, 4, 5) + b"Asia/Seoul\x00"
    reader = PacketReader(tz, encoding="latin-1")
    value = _read_value(reader, CUBRIDDataType.TIMESTAMPTZ, len(tz))
    assert str(value.tzinfo) == "Asia/Seoul"


def test_metadata_names_and_defaults_decode_with_the_connection_codec() -> None:
    entry = _metadata_entry(CUBRIDDataType.STRING, EUC_HANGUL, default="'가'".encode("euc-kr"))
    column = _parse_column_metadata(PacketReader(entry, encoding="euc_kr"), 1)[0]
    assert (column.name, column.real_name, column.default_value) == (HANGUL, HANGUL, "'가'")
    with pytest.raises(DataError, match="column metadata is not valid UTF-8"):
        _parse_column_metadata(PacketReader(entry), 1)

    schema = struct.pack(">Bhii", CUBRIDDataType.STRING, 0, 0, 5) + EUC_HANGUL + b"\x00"
    columns = _parse_schema_column_metadata(PacketReader(schema, encoding="euc_kr"), 1)
    assert columns[0].name == HANGUL
    with pytest.raises(DataError, match="column metadata is not valid UTF-8"):
        _parse_schema_column_metadata(PacketReader(schema), 1)


def test_select_reply_decodes_names_and_rows_with_the_packet_codec() -> None:
    body = _select_body([(CUBRIDDataType.STRING, EUC_HANGUL)], [EUC_HANGUL + b"\x00"])
    packet = PrepareAndExecutePacket("SELECT 1")
    packet.encoding = "euc_kr"
    packet.parse(body)
    assert packet.columns[0].name == HANGUL
    assert packet.rows == [(HANGUL,)]


def test_metadata_decode_failure_keeps_the_session(
    socket_queue: list[MagicMock],  # noqa: F811
) -> None:
    conn, sock = make_connected_connection(socket_queue)
    body = _select_body([(CUBRIDDataType.STRING, EUC_HANGUL)], [b"a\x00"])
    frame = _frame(body)
    sock.recv.side_effect = [frame[:4], frame[4:8], frame[8:]]
    with pytest.raises(DataError, match="column metadata is not valid UTF-8"):
        conn._send_and_receive(PrepareAndExecutePacket("SELECT v FROM t"))
    assert conn._connected is True


def test_error_messages_decode_with_the_connection_codec_and_replace() -> None:
    message = "테이블 '없음' 이 없습니다".encode("euc-kr")
    body = CAS_INFO + struct.pack(">ii", -1, -493) + message + b"\x00"
    packet = PrepareAndExecutePacket("SELECT 1")
    packet.encoding = "euc_kr"
    with pytest.raises(ProgrammingError) as raised:
        packet.parse(body)
    assert "테이블 '없음' 이 없습니다" in raised.value.msg
    # A message cut inside a character still surfaces with a replacement.
    reader = PacketReader(message[:-1], encoding="euc_kr")
    assert reader._parse_lenient_text(len(message) - 1).endswith("�")


def test_batch_error_messages_decode_with_the_connection_codec() -> None:
    text = "중복".encode("euc-kr") + b"\x00"
    body = struct.pack(">iiBii", 0, 1, CUBRIDStatementType.INSERT, -1, -670)
    body += struct.pack(">i", len(text)) + text + struct.pack(">i", 0)
    packet = BatchExecutePacket(["INSERT INTO t VALUES (1)"], protocol_version=8)
    packet.encoding = "euc_kr"
    packet.parse(CAS_INFO + body)
    assert packet.errors == [{"code": -670, "message": "중복"}]


def test_default_reply_decoding_is_unchanged() -> None:
    body = _build_select_response([(CUBRIDDataType.STRING, "v")], [HANGUL.encode() + b"\x00"])
    packet = PrepareAndExecutePacket("SELECT v FROM t")
    packet.parse(body)
    assert packet.rows == [(HANGUL,)]


# --- prepared compatibility path -----------------------------------------------


def test_prepared_strings_use_the_connection_codec() -> None:
    scalar = _encode_prepared_scalar(HANGUL, "euc_kr")
    assert scalar == _PreparedScalar(CUBRIDDataType.CHAR, EUC_HANGUL + b"\x00", "euc_kr")
    assert _encode_prepared_scalar(HANGUL).payload == HANGUL.encode() + b"\x00"
    with pytest.raises(DataError, match="prepared string cannot be encoded as iso8859-1"):
        _encode_prepared_scalar(HANGUL, "iso8859-1")
    with pytest.raises(DataError, match="prepared CHAR is not valid euc_kr"):
        _PreparedScalar(CUBRIDDataType.CHAR, b"\xc7\x00", "euc_kr")


def test_prepared_execute_rejects_a_binding_for_another_codec() -> None:
    packet = ExecutePacket(
        1,
        CUBRIDStatementType.INSERT,
        bindings=(_encode_prepared_scalar(HANGUL, "euc_kr"),),
        bind_count=1,
    )
    with pytest.raises(ProgrammingError, match="different charset"):
        packet.write(CAS_INFO)
    packet.encoding = "euc_kr"
    written = packet.write(CAS_INFO)
    assert EUC_HANGUL + b"\x00" in written


def test_prepare_packet_reports_the_codec() -> None:
    packet = PreparePacket(f"SELECT '{HANGUL}'")
    packet.encoding = "iso8859-1"
    with pytest.raises(DataError, match="prepared SQL cannot be encoded as iso8859-1"):
        packet.write(CAS_INFO)
    packet.encoding = "euc_kr"
    written = packet.write(CAS_INFO)
    assert EUC_HANGUL in written


# --- review follow-ups -----------------------------------------------------------


def _chain(exc: BaseException) -> str:
    """Render every chained exception, as an error collector would see it."""
    parts = []
    seen: BaseException | None = exc
    while seen is not None:
        parts.append(repr(seen) + repr(getattr(seen, "object", "")))
        seen = seen.__cause__ or seen.__context__
    return "".join(parts)


def test_unencodable_secrets_are_not_kept_in_the_exception_chain(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(socket, "create_connection", MagicMock(side_effect=AssertionError))
    with pytest.raises(DataError) as raised:
        connect(database="testdb", password="pw-secret\U0001f600", charset="euckr")
    assert "pw-secret" not in _chain(raised.value)
    with pytest.raises(DataError) as raised:
        _encode_prepared_scalar("bound-secret\U0001f600", "euc_kr")
    assert "bound-secret" not in _chain(raised.value)


def test_unencodable_native_prepare_sql_is_not_kept_in_the_exception_chain() -> None:
    from pycubrid.compat import native

    owner = native.connection.__new__(native.connection)
    driver = MagicMock()
    driver._encoding = "euc_kr"
    driver._connected = True
    owner._driver = driver
    owner._closed = False
    owner._session_lock = __import__("threading").RLock()
    owner._prepared_owners = set()
    cursor = native.cursor(owner)
    with pytest.raises(DataError, match="prepared SQL cannot be encoded as euc_kr") as raised:
        cursor.prepare("SELECT 'sql-secret\U0001f600'")
    assert "sql-secret" not in _chain(raised.value)
    driver._send_and_receive.assert_not_called()


def test_sync_unencodable_schema_argument_keeps_the_session(
    socket_queue: list[MagicMock],  # noqa: F811
) -> None:
    open_db = build_open_db_response()
    sock = make_socket([build_handshake_response(), open_db[:4], open_db[4:]])
    socket_queue.append(sock)
    conn = Connection("localhost", 33000, "testdb", "dba", "", charset="euckr")
    sock.sendall.reset_mock()
    with pytest.raises(DataError, match="schema argument cannot be encoded"):
        conn.get_schema_info(1, "t\U0001f600")
    with pytest.raises(DataError, match="schema argument cannot be encoded"):
        conn.get_schema_info(4, "t", arg2="c\U0001f600")
    sock.sendall.assert_not_called()
    assert conn._connected is True
    assert conn._socket is sock


@pytest.mark.asyncio
async def test_async_unencodable_schema_argument_keeps_the_session() -> None:
    conn = AsyncConnection("localhost", 33000, "testdb", "dba", "", charset="euckr")
    conn._connected = True
    conn._cas_info = CAS_INFO
    conn._reader = MagicMock()
    writer = MagicMock()
    writer.drain = AsyncMock()
    conn._writer = writer
    with pytest.raises(DataError, match="schema argument cannot be encoded"):
        await conn.get_schema_info(1, "t\U0001f600")
    writer.write.assert_not_called()
    assert conn._connected is True
    assert conn._writer is writer


def test_unencodable_executemany_row_sends_nothing(
    socket_queue: list[MagicMock],  # noqa: F811
) -> None:
    open_db = build_open_db_response()
    sock = make_socket([build_handshake_response(), open_db[:4], open_db[4:]])
    socket_queue.append(sock)
    conn = Connection("localhost", 33000, "testdb", "dba", "", charset="euckr")
    conn._cas_info = CAS_INFO  # IN_TRAN: no CHECK_CAS probe first
    sock.sendall.reset_mock()
    cursor = conn.cursor()
    with pytest.raises(DataError, match="cannot be encoded as euc_kr"):
        cursor.executemany("INSERT INTO t VALUES (?)", [(HANGUL,), ("\U0001f600",)])
    sock.sendall.assert_not_called()
    assert conn._connected is True


def test_cursor_owns_and_releases_the_handle_after_metadata_decode_failure() -> None:
    from pycubrid.cursor import Cursor
    from pycubrid.protocol import CloseQueryPacket

    body = _select_body([(CUBRIDDataType.STRING, EUC_HANGUL)], [b"a\x00"])

    def reply(packet: object, **kwargs: object) -> object:
        if isinstance(packet, PrepareAndExecutePacket):
            packet.parse(body)
        return packet

    connection = MagicMock()
    connection._timing = None
    connection._cursors = set()
    connection.autocommit = True
    connection._protocol_version = 8
    connection._decode_collections = False
    connection._json_deserializer = None
    connection._send_and_receive = MagicMock(side_effect=reply)
    cursor = Cursor(connection)
    with pytest.raises(DataError, match="column metadata is not valid UTF-8"):
        cursor.execute("SELECT 1")
    assert cursor._query_handle == 1
    assert cursor.description is None
    cursor.close()
    closes = [
        c.args[0].query_handle
        for c in connection._send_and_receive.call_args_list
        if isinstance(c.args[0], CloseQueryPacket)
    ]
    assert closes == [1]


def test_codec_encoding_supplementary_characters_is_scanned_fully(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import codecs

    def encode(text: str, errors: str = "strict") -> tuple[bytes, int]:
        out = bytearray()
        for char in text:
            code_point = ord(char)
            if code_point < 0x80:
                out.append(code_point)
            elif code_point == 0x1F600:
                out += b"\xff\x27"  # an apostrophe trail byte beyond the BMP
            elif code_point >= 0x10000:
                out += b"\xff\xfe"
            else:
                raise UnicodeEncodeError("fake", text, 0, 1, "unmapped")
        return bytes(out), len(text)

    def decode(data: bytes, errors: str = "strict") -> tuple[str, int]:
        return bytes(data).decode("ascii"), len(data)

    info = codecs.CodecInfo(encode, decode, name="pycubrid-test-supplementary")

    def search(name: str) -> codecs.CodecInfo | None:
        return info if name == "pycubrid_test_supplementary" else None

    codecs.register(search)
    try:
        with pytest.raises(ValueError, match="not ASCII-compatible"):
            resolve_charset("pycubrid_test_supplementary")
    finally:
        codecs.unregister(search)


def test_locale_without_codec_is_still_rejected() -> None:
    with pytest.raises(ValueError, match="unknown charset"):
        resolve_charset("ko_KR.")
    with pytest.raises(ValueError, match="not ASCII-compatible"):
        resolve_charset("ja_JP.shift_jis")


@pytest.mark.parametrize("text", ["똠", "a뷁b", "\u3164똠"])
def test_hangul_outside_ks_x_1001_is_unencodable_in_euc_kr(text: str) -> None:
    position = next(i for i, c in enumerate(text) if c in "똠뷁")
    with pytest.raises(DataError) as raised:
        PacketWriter(encoding="euc_kr")._write_null_terminated_string(text)
    assert str(raised.value) == (
        f"text cannot be encoded as euc_kr (unencodable character at position {position})"
    )
    with pytest.raises(DataError, match="prepared string cannot be encoded as euc_kr"):
        _encode_prepared_scalar(text, "euc_kr")


def test_hangul_filler_alone_and_ks_x_1001_hangul_still_encode() -> None:
    writer = PacketWriter(encoding="euc_kr")
    writer._write_null_terminated_string("\u3164" + HANGUL)
    assert writer.to_bytes().endswith(b"\xa4\xd4" + EUC_HANGUL + b"\x00")
    # UTF-8 has no makeup sequences: the same text is fine there.
    PacketWriter()._write_null_terminated_string("똠뷁")


def test_unencodable_euc_kr_credential_fails_before_socket_work(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(socket, "create_connection", MagicMock(side_effect=AssertionError))
    with pytest.raises(DataError, match=r"^password cannot be encoded as euc_kr$"):
        connect(database="testdb", password="똠", charset="euckr")


def test_lob_locator_decodes_leniently_with_the_connection_codec() -> None:
    locator = b"file:ces_700/dba." + "한글표".encode("euc-kr") + b".00001_0001\x00"
    handle = struct.pack(">iqi", 24, 4, len(locator)) + locator
    reader = PacketReader(handle, encoding="euc_kr")
    value = _read_value(reader, CUBRIDDataType.CLOB, len(handle))
    assert value["file_locator"] == "file:ces_700/dba.한글표.00001_0001"
    assert value["packed_lob_handle"] == handle
    assert value["lob_length"] == 4
    # A codec mismatch must not fail the fetch: the locator is informational.
    value = _read_value(PacketReader(handle), CUBRIDDataType.BLOB, len(handle))
    assert value["file_locator"].startswith("file:ces_700/dba.\ufffd")
    assert value["packed_lob_handle"] == handle


@pytest.mark.parametrize(
    ("raw", "text"),
    [
        (b"\xa4\xd4", "\u3164"),
        (b"\xa4\xd4\xa4\xa8\xa4\xc7\xa4\xb1", "\u3164\u3138\u3157\u3141"),
        (EUC_HANGUL + b"\xa4\xd4", HANGUL + "\u3164"),
    ],
)
def test_euc_kr_filler_decodes_as_cubrid_stores_it(raw: bytes, text: str) -> None:
    # CUBRID reads each KS X 1001 pair separately; CPython euc_kr would treat
    # A4 D4 as a makeup-sequence start (error, or one composed syllable).
    payload = raw + b"\x00"
    reader = PacketReader(payload, encoding="euc_kr")
    value = _read_value(reader, CUBRIDDataType.STRING, len(payload))
    assert value == text
    entry = _metadata_entry(CUBRIDDataType.STRING, raw)
    columns = _parse_column_metadata(PacketReader(entry, encoding="euc_kr"), 1)
    assert columns[0].name == text


def test_euc_kr_filler_path_still_rejects_cp949_extensions() -> None:
    payload = b"\xa4\xd4\x81\x41\x00"  # filler + a CP949-only syllable
    with pytest.raises(DataError, match=r"not valid euc_kr \(invalid byte at offset 2\)"):
        _read_value(PacketReader(payload, encoding="euc_kr"), CUBRIDDataType.STRING, 5)


def test_unencodable_password_error_omits_the_position(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(socket, "create_connection", MagicMock(side_effect=AssertionError))
    with pytest.raises(DataError) as raised:
        connect(database="testdb", password="abc\U0001f600", charset="euckr")
    assert str(raised.value) == "password cannot be encoded as euc_kr"
