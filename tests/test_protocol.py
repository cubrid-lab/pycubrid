"""Tests for pycubrid.protocol — CAS protocol packet classes."""

from __future__ import annotations

import datetime
import struct
from decimal import Decimal

import pytest

from pycubrid.constants import (
    CASFunctionCode,
    CASProtocol,
    CCIDbParam,
    CCILOBType,
    CCISchemaType,
    CCITransactionType,
    CUBRIDDataType,
    CUBRIDStatementType,
    DataSize,
)
from pycubrid.exceptions import DatabaseError
from pycubrid.packet import PacketReader
from pycubrid.protocol import (
    BatchExecutePacket,
    ClientInfoExchangePacket,
    CloseQueryPacket,
    CloseDatabasePacket,
    ColumnMetaData,
    CommitPacket,
    ExecutePacket,
    FetchPacket,
    GetDbParameterPacket,
    GetEngineVersionPacket,
    GetLastInsertIdPacket,
    GetSchemaPacket,
    LOBNewPacket,
    LOBReadPacket,
    LOBWritePacket,
    OpenDatabasePacket,
    PrepareAndExecutePacket,
    PreparePacket,
    ResultInfo,
    RollbackPacket,
    SetDbParameterPacket,
    _parse_column_metadata,
    _parse_result_infos,
    _parse_row_data,
    _raise_error,
    _read_value,
)


# ---------------------------------------------------------------------------
# Helpers for building mock response data
# ---------------------------------------------------------------------------

DEFAULT_CAS_INFO = b"\x00\x01\x02\x03"


def _build_success_response(cas_info: bytes, response_code: int, extra: bytes = b"") -> bytes:
    """Build a simple success response: casInfo + responseCode + extra."""
    return cas_info + struct.pack(">i", response_code) + extra


def _build_error_response(cas_info: bytes, error_code: int, error_message: str) -> bytes:
    """Build an error response: casInfo + negative responseCode + error body."""
    msg_bytes = error_message.encode("utf-8") + b"\x00"
    body = struct.pack(">i", error_code) + msg_bytes
    return cas_info + struct.pack(">i", -1) + body


def _build_null_terminated_string(value: str) -> bytes:
    """Build a length-prefixed null-terminated string."""
    encoded = value.encode("utf-8") + b"\x00"
    return struct.pack(">i", len(encoded)) + encoded


def _build_column_metadata(
    column_type: int = CUBRIDDataType.STRING,
    scale: int = 0,
    precision: int = 255,
    name: str = "col1",
    real_name: str = "col1",
    table_name: str = "test_table",
    is_nullable: bool = False,
    default_value: str = "",
    two_byte_type: bool = False,
) -> bytes:
    """Build binary column metadata."""
    buf = bytearray()
    if two_byte_type:
        buf.append(0x80 | (column_type >> 8))
        buf.append(column_type & 0xFF)
    else:
        buf.append(column_type)
    buf.extend(struct.pack(">h", scale))
    buf.extend(struct.pack(">i", precision))
    # name
    name_bytes = name.encode("utf-8") + b"\x00"
    buf.extend(struct.pack(">i", len(name_bytes)))
    buf.extend(name_bytes)
    # real_name
    rn_bytes = real_name.encode("utf-8") + b"\x00"
    buf.extend(struct.pack(">i", len(rn_bytes)))
    buf.extend(rn_bytes)
    # table_name
    tn_bytes = table_name.encode("utf-8") + b"\x00"
    buf.extend(struct.pack(">i", len(tn_bytes)))
    buf.extend(tn_bytes)
    # nullable
    buf.append(1 if is_nullable else 0)
    # default_value
    if default_value:
        dv_bytes = default_value.encode("utf-8") + b"\x00"
        buf.extend(struct.pack(">i", len(dv_bytes)))
        buf.extend(dv_bytes)
    else:
        buf.extend(struct.pack(">i", 0))
    # 7 boolean fields
    for _ in range(7):
        buf.append(0)
    return bytes(buf)


def _build_row_data(index: int, values: list[tuple[int, bytes | None]]) -> bytes:
    """Build binary row data for one row.

    Each value is ``(column_type, raw_bytes)`` or ``(column_type, None)`` for NULL.
    """
    buf = bytearray()
    buf.extend(struct.pack(">i", index))
    buf.extend(b"\x00" * DataSize.OID)
    for _, raw in values:
        if raw is None:
            buf.extend(struct.pack(">i", 0))
        else:
            buf.extend(struct.pack(">i", len(raw)))
            buf.extend(raw)
    return bytes(buf)


# ---------------------------------------------------------------------------
# Test Data Classes
# ---------------------------------------------------------------------------


class TestColumnMetaData:
    """Tests for ColumnMetaData dataclass."""

    def test_default_values(self) -> None:
        col = ColumnMetaData()
        assert col.column_type == 0
        assert col.scale == -1
        assert col.precision == -1
        assert col.name == ""
        assert col.real_name == ""
        assert col.table_name == ""
        assert col.is_nullable is False
        assert col.default_value == ""
        assert col.is_auto_increment is False
        assert col.is_unique_key is False
        assert col.is_primary_key is False
        assert col.is_reverse_index is False
        assert col.is_reverse_unique is False
        assert col.is_foreign_key is False
        assert col.is_shared is False

    def test_custom_values(self) -> None:
        col = ColumnMetaData(
            column_type=CUBRIDDataType.INT,
            scale=0,
            precision=10,
            name="id",
            real_name="id",
            table_name="users",
            is_nullable=False,
            is_primary_key=True,
            is_auto_increment=True,
        )
        assert col.column_type == CUBRIDDataType.INT
        assert col.name == "id"
        assert col.is_primary_key is True
        assert col.is_auto_increment is True


class TestResultInfo:
    """Tests for ResultInfo dataclass."""

    def test_default_values(self) -> None:
        info = ResultInfo()
        assert info.stmt_type == 0
        assert info.result_count == 0
        assert info.oid == b""
        assert info.cache_time_sec == 0
        assert info.cache_time_usec == 0

    def test_custom_values(self) -> None:
        info = ResultInfo(
            stmt_type=CUBRIDStatementType.INSERT,
            result_count=5,
            oid=b"\x00" * 8,
            cache_time_sec=100,
            cache_time_usec=500,
        )
        assert info.stmt_type == CUBRIDStatementType.INSERT
        assert info.result_count == 5


# ---------------------------------------------------------------------------
# Test Helper Functions
# ---------------------------------------------------------------------------


class TestRaiseError:
    """Tests for _raise_error helper."""

    def test_raises_database_error(self) -> None:
        error_body = struct.pack(">i", -123) + b"test error\x00"
        reader = PacketReader(error_body)
        with pytest.raises(DatabaseError, match="test error"):
            _raise_error(reader, len(error_body))


class TestParseColumnMetadata:
    """Tests for _parse_column_metadata helper."""

    def test_single_column(self) -> None:
        data = _build_column_metadata(
            column_type=CUBRIDDataType.STRING,
            name="name",
            real_name="name",
            table_name="users",
            is_nullable=True,
        )
        reader = PacketReader(data)
        cols = _parse_column_metadata(reader, 1)
        assert len(cols) == 1
        assert cols[0].column_type == CUBRIDDataType.STRING
        assert cols[0].name == "name"
        assert cols[0].is_nullable is True

    def test_two_byte_type(self) -> None:
        data = _build_column_metadata(
            column_type=CUBRIDDataType.TIMESTAMPTZ,
            two_byte_type=True,
            name="ts",
        )
        reader = PacketReader(data)
        cols = _parse_column_metadata(reader, 1)
        assert cols[0].column_type == CUBRIDDataType.TIMESTAMPTZ

    def test_multiple_columns(self) -> None:
        data = _build_column_metadata(
            column_type=CUBRIDDataType.INT, name="id"
        ) + _build_column_metadata(column_type=CUBRIDDataType.STRING, name="name")
        reader = PacketReader(data)
        cols = _parse_column_metadata(reader, 2)
        assert len(cols) == 2
        assert cols[0].name == "id"
        assert cols[1].name == "name"

    def test_column_with_default_value(self) -> None:
        data = _build_column_metadata(
            column_type=CUBRIDDataType.INT,
            name="count",
            default_value="0",
        )
        reader = PacketReader(data)
        cols = _parse_column_metadata(reader, 1)
        assert cols[0].default_value == "0"

    def test_zero_columns(self) -> None:
        reader = PacketReader(b"")
        cols = _parse_column_metadata(reader, 0)
        assert cols == []


class TestReadValue:
    """Tests for _read_value helper."""

    def test_string_types(self) -> None:
        for dt in (
            CUBRIDDataType.CHAR,
            CUBRIDDataType.STRING,
            CUBRIDDataType.NCHAR,
            CUBRIDDataType.VARNCHAR,
            CUBRIDDataType.ENUM,
        ):
            data = b"hello\x00"
            reader = PacketReader(data)
            result = _read_value(reader, dt, len(data))
            assert result == "hello"

    def test_short(self) -> None:
        data = struct.pack(">h", 42)
        reader = PacketReader(data)
        assert _read_value(reader, CUBRIDDataType.SHORT, 2) == 42

    def test_int(self) -> None:
        data = struct.pack(">i", 12345)
        reader = PacketReader(data)
        assert _read_value(reader, CUBRIDDataType.INT, 4) == 12345

    def test_bigint(self) -> None:
        data = struct.pack(">q", 9876543210)
        reader = PacketReader(data)
        assert _read_value(reader, CUBRIDDataType.BIGINT, 8) == 9876543210

    def test_float(self) -> None:
        data = struct.pack(">f", 3.14)
        reader = PacketReader(data)
        result = _read_value(reader, CUBRIDDataType.FLOAT, 4)
        assert abs(result - 3.14) < 0.01

    def test_double(self) -> None:
        data = struct.pack(">d", 2.71828)
        reader = PacketReader(data)
        result = _read_value(reader, CUBRIDDataType.DOUBLE, 8)
        assert abs(result - 2.71828) < 0.0001

    def test_monetary(self) -> None:
        data = struct.pack(">d", 99.99)
        reader = PacketReader(data)
        result = _read_value(reader, CUBRIDDataType.MONETARY, 8)
        assert abs(result - 99.99) < 0.01

    def test_numeric(self) -> None:
        data = b"123.45\x00"
        reader = PacketReader(data)
        result = _read_value(reader, CUBRIDDataType.NUMERIC, len(data))
        assert result == Decimal("123.45")

    def test_date(self) -> None:
        data = struct.pack(">7h", 2025, 2, 15, 0, 0, 0, 0)  # month 1-based on wire
        reader = PacketReader(data)
        result = _read_value(reader, CUBRIDDataType.DATE, 14)
        assert result == datetime.date(2025, 1, 15)  # month -1 in parse

    def test_time(self) -> None:
        data = struct.pack(">7h", 0, 0, 0, 10, 30, 45, 500)
        reader = PacketReader(data)
        result = _read_value(reader, CUBRIDDataType.TIME, 14)
        assert result == datetime.time(10, 30, 45, 500000)

    def test_datetime(self) -> None:
        data = struct.pack(">7h", 2025, 4, 20, 14, 30, 0, 123)
        reader = PacketReader(data)
        result = _read_value(reader, CUBRIDDataType.DATETIME, 14)
        assert result == datetime.datetime(2025, 3, 20, 14, 30, 0, 123000)

    def test_timestamp(self) -> None:
        data = struct.pack(">7h", 2025, 7, 4, 12, 0, 0, 0)
        reader = PacketReader(data)
        result = _read_value(reader, CUBRIDDataType.TIMESTAMP, 14)
        # timestamp uses _parse_timestamp which reads 6 shorts (no msec)
        assert result.year == 2025
        assert result.month == 6  # month -1
        assert result.day == 4

    def test_object(self) -> None:
        data = struct.pack(">i", 100) + struct.pack(">h", 1) + struct.pack(">h", 2)
        reader = PacketReader(data)
        result = _read_value(reader, CUBRIDDataType.OBJECT, 8)
        assert result == "OID:@100|1|2"

    def test_bit_types(self) -> None:
        for dt in (CUBRIDDataType.BIT, CUBRIDDataType.VARBIT):
            data = b"\x01\x02\x03\x04"
            reader = PacketReader(data)
            result = _read_value(reader, dt, 4)
            assert result == b"\x01\x02\x03\x04"

    def test_collection_types(self) -> None:
        for dt in (CUBRIDDataType.SET, CUBRIDDataType.MULTISET, CUBRIDDataType.SEQUENCE):
            data = b"\xaa\xbb\xcc"
            reader = PacketReader(data)
            result = _read_value(reader, dt, 3)
            assert result == b"\xaa\xbb\xcc"

    def test_blob(self) -> None:
        # Build a LOB handle: db_type(4B) + lobLength(8B) + locatorSize(4B) + locator
        locator = b"/tmp/blob\x00"
        lob_handle = (
            struct.pack(">i", CUBRIDDataType.BLOB)
            + struct.pack(">q", 1024)
            + struct.pack(">i", len(locator))
            + locator
        )
        reader = PacketReader(lob_handle)
        result = _read_value(reader, CUBRIDDataType.BLOB, len(lob_handle))
        assert result["lob_type"] == CUBRIDDataType.BLOB
        assert result["lob_length"] == 1024

    def test_clob(self) -> None:
        locator = b"/tmp/clob\x00"
        lob_handle = (
            struct.pack(">i", CUBRIDDataType.CLOB)
            + struct.pack(">q", 2048)
            + struct.pack(">i", len(locator))
            + locator
        )
        reader = PacketReader(lob_handle)
        result = _read_value(reader, CUBRIDDataType.CLOB, len(lob_handle))
        assert result["lob_type"] == CUBRIDDataType.CLOB
        assert result["lob_length"] == 2048

    def test_null_type(self) -> None:
        reader = PacketReader(b"")
        result = _read_value(reader, CUBRIDDataType.NULL, 0)
        assert result is None

    def test_unknown_type_fallback(self) -> None:
        data = b"\xfe\xed"
        reader = PacketReader(data)
        result = _read_value(reader, 99, 2)
        assert result == b"\xfe\xed"


class TestParseRowData:
    """Tests for _parse_row_data helper."""

    def test_single_row_single_column(self) -> None:
        col = ColumnMetaData(column_type=CUBRIDDataType.INT)
        value_bytes = struct.pack(">i", 42)
        row_data = _build_row_data(0, [(CUBRIDDataType.INT, value_bytes)])
        reader = PacketReader(row_data)
        rows = _parse_row_data(reader, 1, [col], CUBRIDStatementType.SELECT)
        assert len(rows) == 1
        assert rows[0][0] == 42

    def test_null_value(self) -> None:
        col = ColumnMetaData(column_type=CUBRIDDataType.STRING)
        row_data = _build_row_data(0, [(CUBRIDDataType.STRING, None)])
        reader = PacketReader(row_data)
        rows = _parse_row_data(reader, 1, [col], CUBRIDStatementType.SELECT)
        assert rows[0][0] is None

    def test_multiple_rows(self) -> None:
        col = ColumnMetaData(column_type=CUBRIDDataType.SHORT)
        row1 = _build_row_data(0, [(CUBRIDDataType.SHORT, struct.pack(">h", 1))])
        row2 = _build_row_data(1, [(CUBRIDDataType.SHORT, struct.pack(">h", 2))])
        reader = PacketReader(row1 + row2)
        rows = _parse_row_data(reader, 2, [col], CUBRIDStatementType.SELECT)
        assert len(rows) == 2
        assert rows[0][0] == 1
        assert rows[1][0] == 2

    def test_call_statement_type_reads_type_byte(self) -> None:
        """For CALL statements, type byte is prepended to each value."""
        col = ColumnMetaData(column_type=CUBRIDDataType.NULL)
        # type byte (INT=8) + int value
        value_bytes = bytes([CUBRIDDataType.INT]) + struct.pack(">i", 99)
        row_data = _build_row_data(0, [(CUBRIDDataType.NULL, value_bytes)])
        reader = PacketReader(row_data)
        rows = _parse_row_data(reader, 1, [col], CUBRIDStatementType.CALL)
        assert rows[0][0] == 99

    def test_call_with_null_after_type_byte(self) -> None:
        """For CALL statements, when size after type byte is 0."""
        col = ColumnMetaData(column_type=CUBRIDDataType.NULL)
        # Only the type byte, size = 1
        value_bytes = bytes([CUBRIDDataType.INT])
        row_data = _build_row_data(0, [(CUBRIDDataType.NULL, value_bytes)])
        reader = PacketReader(row_data)
        rows = _parse_row_data(reader, 1, [col], CUBRIDStatementType.CALL)
        assert rows[0][0] is None

    def test_zero_rows(self) -> None:
        reader = PacketReader(b"")
        rows = _parse_row_data(reader, 0, [], CUBRIDStatementType.SELECT)
        assert rows == []


class TestParseResultInfos:
    """Tests for _parse_result_infos helper."""

    def test_single_result(self) -> None:
        buf = bytearray()
        buf.append(CUBRIDStatementType.INSERT)
        buf.extend(struct.pack(">i", 1))
        buf.extend(b"\x00" * DataSize.OID)
        buf.extend(struct.pack(">i", 0))
        buf.extend(struct.pack(">i", 0))
        reader = PacketReader(bytes(buf))
        infos = _parse_result_infos(reader, 1)
        assert len(infos) == 1
        assert infos[0].stmt_type == CUBRIDStatementType.INSERT
        assert infos[0].result_count == 1

    def test_zero_results(self) -> None:
        reader = PacketReader(b"")
        infos = _parse_result_infos(reader, 0)
        assert infos == []


# ---------------------------------------------------------------------------
# Test Packet Classes
# ---------------------------------------------------------------------------


class TestClientInfoExchangePacket:
    """Tests for ClientInfoExchangePacket."""

    def test_write(self) -> None:
        pkt = ClientInfoExchangePacket()
        data = pkt.write()
        assert len(data) == 10
        assert data[:5] == b"CUBRK"
        assert data[5] == CASProtocol.CLIENT_JDBC
        assert data[6] == CASProtocol.CAS_VERSION
        assert data[7:10] == b"\x00\x00\x00"

    def test_parse_zero_port(self) -> None:
        pkt = ClientInfoExchangePacket()
        pkt.parse(struct.pack(">i", 0))
        assert pkt.new_connection_port == 0

    def test_parse_nonzero_port(self) -> None:
        pkt = ClientInfoExchangePacket()
        pkt.parse(struct.pack(">i", 33001))
        assert pkt.new_connection_port == 33001


class TestOpenDatabasePacket:
    """Tests for OpenDatabasePacket."""

    def test_write(self) -> None:
        pkt = OpenDatabasePacket("testdb", "dba", "secret")
        data = pkt.write()
        assert len(data) == 628
        # Check db_name in first 32 bytes
        assert data[:6] == b"testdb"
        assert data[6:32] == b"\x00" * 26
        # Check user in next 32 bytes
        assert data[32:35] == b"dba"
        assert data[35:64] == b"\x00" * 29
        # Check password
        assert data[64:70] == b"secret"
        assert data[70:96] == b"\x00" * 26
        # extended_info + reserved
        assert data[96:] == b"\x00" * 532

    def test_parse_success(self) -> None:
        pkt = OpenDatabasePacket("testdb", "dba", "")
        broker_info = bytes([1, 0, 0x47, 0, 1, 0, 0, 0])
        session_id = struct.pack(">i", 42)
        response = (
            DEFAULT_CAS_INFO
            + struct.pack(">i", 0)  # responseCode = 0 (success)
            + broker_info
            + session_id
        )
        pkt.parse(response)
        assert pkt.cas_info == DEFAULT_CAS_INFO
        assert pkt.response_code == 0
        assert pkt.session_id == 42
        assert pkt.broker_info["db_type"] == 1
        assert pkt.broker_info["protocol_version"] == 7

    def test_parse_error(self) -> None:
        pkt = OpenDatabasePacket("testdb", "dba", "")
        response = _build_error_response(DEFAULT_CAS_INFO, -100, "auth failed")
        with pytest.raises(DatabaseError, match="auth failed"):
            pkt.parse(response)


class TestPrepareAndExecutePacket:
    """Tests for PrepareAndExecutePacket."""

    def test_write(self) -> None:
        pkt = PrepareAndExecutePacket("SELECT 1", auto_commit=True)
        data = pkt.write(DEFAULT_CAS_INFO)
        # Should start with 8-byte protocol header
        _ = struct.unpack(">i", data[:4])[0]  # data_len
        assert data[4:8] == DEFAULT_CAS_INFO
        payload = data[8:]
        assert payload[0] == CASFunctionCode.PREPARE_AND_EXECUTE

    def test_parse_select_success(self) -> None:
        pkt = PrepareAndExecutePacket("SELECT 1", protocol_version=7)
        # Build a minimal SELECT response
        col_meta = _build_column_metadata(column_type=CUBRIDDataType.INT, name="1")
        value = struct.pack(">i", 1)
        row = _build_row_data(0, [(CUBRIDDataType.INT, value)])

        # Result info
        result_info_buf = bytearray()
        result_info_buf.append(CUBRIDStatementType.SELECT)
        result_info_buf.extend(struct.pack(">i", 1))
        result_info_buf.extend(b"\x00" * DataSize.OID)
        result_info_buf.extend(struct.pack(">i", 0))
        result_info_buf.extend(struct.pack(">i", 0))

        response = bytearray()
        response.extend(DEFAULT_CAS_INFO)
        response.extend(struct.pack(">i", 1))  # responseCode=queryHandle=1
        response.extend(struct.pack(">i", 0))  # result cache lifetime
        response.append(CUBRIDStatementType.SELECT)  # statement type
        response.extend(struct.pack(">i", 0))  # bind count
        response.append(0)  # is_updatable
        response.extend(struct.pack(">i", 1))  # column count
        response.extend(col_meta)  # column metadata
        response.extend(struct.pack(">i", 1))  # total tuple count
        response.append(0)  # cache reusable
        response.extend(struct.pack(">i", 1))  # result count
        response.extend(result_info_buf)  # result info
        response.append(0)  # includes_column_info (proto > 1)
        response.extend(struct.pack(">i", 0))  # shard_id (proto > 4)
        response.extend(struct.pack(">i", 0))  # fetch_code
        response.extend(struct.pack(">i", 1))  # tuple_count
        response.extend(row)  # row data

        pkt.parse(bytes(response))
        assert pkt.query_handle == 1
        assert pkt.statement_type == CUBRIDStatementType.SELECT
        assert pkt.column_count == 1
        assert len(pkt.columns) == 1
        assert pkt.total_tuple_count == 1
        assert pkt.tuple_count == 1
        assert len(pkt.rows) == 1
        assert pkt.rows[0][0] == 1

    def test_parse_insert_success(self) -> None:
        """Test non-SELECT (INSERT) — no inline fetch data."""
        pkt = PrepareAndExecutePacket("INSERT INTO t VALUES(1)", protocol_version=7)
        col_meta = _build_column_metadata(column_type=CUBRIDDataType.INT, name="c1")
        result_info_buf = bytearray()
        result_info_buf.append(CUBRIDStatementType.INSERT)
        result_info_buf.extend(struct.pack(">i", 1))
        result_info_buf.extend(b"\x00" * DataSize.OID)
        result_info_buf.extend(struct.pack(">i", 0))
        result_info_buf.extend(struct.pack(">i", 0))

        response = bytearray()
        response.extend(DEFAULT_CAS_INFO)
        response.extend(struct.pack(">i", 2))  # queryHandle=2
        response.extend(struct.pack(">i", 0))  # cache lifetime
        response.append(CUBRIDStatementType.INSERT)
        response.extend(struct.pack(">i", 0))  # bind count
        response.append(0)  # updatable
        response.extend(struct.pack(">i", 1))  # column count
        response.extend(col_meta)
        response.extend(struct.pack(">i", 1))  # total tuple count
        response.append(0)  # cache reusable
        response.extend(struct.pack(">i", 1))  # result count
        response.extend(result_info_buf)
        response.append(0)  # includes_column_info
        response.extend(struct.pack(">i", 0))  # shard_id

        pkt.parse(bytes(response))
        assert pkt.query_handle == 2
        assert pkt.statement_type == CUBRIDStatementType.INSERT
        assert pkt.rows == []

    def test_parse_error(self) -> None:
        pkt = PrepareAndExecutePacket("INVALID SQL")
        response = _build_error_response(DEFAULT_CAS_INFO, -493, "syntax error")
        with pytest.raises(DatabaseError, match="syntax error"):
            pkt.parse(response)

    def test_parse_low_protocol_version(self) -> None:
        """Protocol version 1 should skip includes_column_info and shard_id."""
        pkt = PrepareAndExecutePacket("INSERT INTO t VALUES(1)", protocol_version=1)
        result_info_buf = bytearray()
        result_info_buf.append(CUBRIDStatementType.INSERT)
        result_info_buf.extend(struct.pack(">i", 1))
        result_info_buf.extend(b"\x00" * DataSize.OID)
        result_info_buf.extend(struct.pack(">i", 0))
        result_info_buf.extend(struct.pack(">i", 0))

        response = bytearray()
        response.extend(DEFAULT_CAS_INFO)
        response.extend(struct.pack(">i", 3))
        response.extend(struct.pack(">i", 0))
        response.append(CUBRIDStatementType.INSERT)
        response.extend(struct.pack(">i", 0))
        response.append(0)
        response.extend(struct.pack(">i", 0))  # 0 columns
        response.extend(struct.pack(">i", 1))
        response.append(0)
        response.extend(struct.pack(">i", 1))
        response.extend(result_info_buf)
        # No proto > 1 fields

        pkt.parse(bytes(response))
        assert pkt.query_handle == 3

    def test_parse_select_zero_tuples(self) -> None:
        """SELECT with zero tuples — no row data parsed."""
        pkt = PrepareAndExecutePacket("SELECT * FROM empty", protocol_version=7)
        col_meta = _build_column_metadata(column_type=CUBRIDDataType.INT, name="id")
        result_info_buf = bytearray()
        result_info_buf.append(CUBRIDStatementType.SELECT)
        result_info_buf.extend(struct.pack(">i", 0))
        result_info_buf.extend(b"\x00" * DataSize.OID)
        result_info_buf.extend(struct.pack(">i", 0))
        result_info_buf.extend(struct.pack(">i", 0))

        response = bytearray()
        response.extend(DEFAULT_CAS_INFO)
        response.extend(struct.pack(">i", 4))
        response.extend(struct.pack(">i", 0))
        response.append(CUBRIDStatementType.SELECT)
        response.extend(struct.pack(">i", 0))
        response.append(0)
        response.extend(struct.pack(">i", 1))
        response.extend(col_meta)
        response.extend(struct.pack(">i", 0))  # total tuple count = 0
        response.append(0)
        response.extend(struct.pack(">i", 1))
        response.extend(result_info_buf)
        response.append(0)
        response.extend(struct.pack(">i", 0))
        response.extend(struct.pack(">i", 0))  # fetch_code
        response.extend(struct.pack(">i", 0))  # tuple_count = 0

        pkt.parse(bytes(response))
        assert pkt.tuple_count == 0
        assert pkt.rows == []

    def test_parse_select_not_enough_bytes_for_fetch(self) -> None:
        """SELECT but not enough remaining bytes for fetch data."""
        pkt = PrepareAndExecutePacket("SELECT 1", protocol_version=7)
        result_info_buf = bytearray()
        result_info_buf.append(CUBRIDStatementType.SELECT)
        result_info_buf.extend(struct.pack(">i", 0))
        result_info_buf.extend(b"\x00" * DataSize.OID)
        result_info_buf.extend(struct.pack(">i", 0))
        result_info_buf.extend(struct.pack(">i", 0))

        response = bytearray()
        response.extend(DEFAULT_CAS_INFO)
        response.extend(struct.pack(">i", 5))
        response.extend(struct.pack(">i", 0))
        response.append(CUBRIDStatementType.SELECT)
        response.extend(struct.pack(">i", 0))
        response.append(0)
        response.extend(struct.pack(">i", 0))
        response.extend(struct.pack(">i", 0))
        response.append(0)
        response.extend(struct.pack(">i", 1))
        response.extend(result_info_buf)
        response.append(0)
        response.extend(struct.pack(">i", 0))
        # No fetch data at all (< 8 bytes remaining)

        pkt.parse(bytes(response))
        assert pkt.tuple_count == 0
        assert pkt.rows == []


class TestPreparePacket:
    """Tests for PreparePacket."""

    def test_write(self) -> None:
        pkt = PreparePacket("SELECT * FROM t", auto_commit=False)
        data = pkt.write(DEFAULT_CAS_INFO)
        payload = data[8:]
        assert payload[0] == CASFunctionCode.PREPARE

    def test_parse_success(self) -> None:
        pkt = PreparePacket("SELECT * FROM t")
        col_meta = _build_column_metadata(column_type=CUBRIDDataType.STRING, name="col")
        response = bytearray()
        response.extend(DEFAULT_CAS_INFO)
        response.extend(struct.pack(">i", 10))  # queryHandle
        response.extend(struct.pack(">i", 0))  # cache lifetime
        response.append(CUBRIDStatementType.SELECT)
        response.extend(struct.pack(">i", 0))  # bind count
        response.append(0)  # updatable
        response.extend(struct.pack(">i", 1))  # column count
        response.extend(col_meta)

        pkt.parse(bytes(response))
        assert pkt.query_handle == 10
        assert pkt.statement_type == CUBRIDStatementType.SELECT
        assert pkt.column_count == 1
        assert pkt.columns[0].name == "col"

    def test_parse_error(self) -> None:
        pkt = PreparePacket("BAD SQL")
        response = _build_error_response(DEFAULT_CAS_INFO, -1, "parse error")
        with pytest.raises(DatabaseError, match="parse error"):
            pkt.parse(response)


class TestExecutePacket:
    """Tests for ExecutePacket."""

    def test_write_select(self) -> None:
        pkt = ExecutePacket(1, CUBRIDStatementType.SELECT)
        data = pkt.write(DEFAULT_CAS_INFO)
        payload = data[8:]
        assert payload[0] == CASFunctionCode.EXECUTE

    def test_write_insert(self) -> None:
        pkt = ExecutePacket(1, CUBRIDStatementType.INSERT, auto_commit=True)
        data = pkt.write(DEFAULT_CAS_INFO)
        payload = data[8:]
        assert payload[0] == CASFunctionCode.EXECUTE

    def test_parse_insert_success(self) -> None:
        pkt = ExecutePacket(1, CUBRIDStatementType.INSERT, protocol_version=7)
        result_info = bytearray()
        result_info.append(CUBRIDStatementType.INSERT)
        result_info.extend(struct.pack(">i", 1))
        result_info.extend(b"\x00" * DataSize.OID)
        result_info.extend(struct.pack(">i", 0))
        result_info.extend(struct.pack(">i", 0))

        response = bytearray()
        response.extend(DEFAULT_CAS_INFO)
        response.extend(struct.pack(">i", 1))  # total_tuple_count
        response.append(0)  # cache_reusable
        response.extend(struct.pack(">i", 1))  # result_count
        response.extend(result_info)
        response.append(0)  # includes_column_info
        response.extend(struct.pack(">i", 0))  # shard_id

        pkt.parse(bytes(response))
        assert pkt.total_tuple_count == 1
        assert pkt.result_count == 1

    def test_parse_select_with_rows(self) -> None:
        cols = [ColumnMetaData(column_type=CUBRIDDataType.INT, name="id")]
        pkt = ExecutePacket(1, CUBRIDStatementType.SELECT, protocol_version=7)
        value = struct.pack(">i", 42)
        row = _build_row_data(0, [(CUBRIDDataType.INT, value)])

        result_info = bytearray()
        result_info.append(CUBRIDStatementType.SELECT)
        result_info.extend(struct.pack(">i", 1))
        result_info.extend(b"\x00" * DataSize.OID)
        result_info.extend(struct.pack(">i", 0))
        result_info.extend(struct.pack(">i", 0))

        response = bytearray()
        response.extend(DEFAULT_CAS_INFO)
        response.extend(struct.pack(">i", 1))
        response.append(0)
        response.extend(struct.pack(">i", 1))
        response.extend(result_info)
        response.append(0)  # includes_column_info
        response.extend(struct.pack(">i", 0))  # shard_id
        response.extend(struct.pack(">i", 0))  # fetch_code
        response.extend(struct.pack(">i", 1))  # tuple_count
        response.extend(row)

        pkt.parse(bytes(response), columns=cols)
        assert pkt.total_tuple_count == 1
        assert pkt.tuple_count == 1
        assert pkt.rows[0][0] == 42

    def test_parse_select_no_columns(self) -> None:
        """SELECT with no columns provided — should not parse rows."""
        pkt = ExecutePacket(1, CUBRIDStatementType.SELECT, protocol_version=7)
        result_info = bytearray()
        result_info.append(CUBRIDStatementType.SELECT)
        result_info.extend(struct.pack(">i", 0))
        result_info.extend(b"\x00" * DataSize.OID)
        result_info.extend(struct.pack(">i", 0))
        result_info.extend(struct.pack(">i", 0))

        response = bytearray()
        response.extend(DEFAULT_CAS_INFO)
        response.extend(struct.pack(">i", 1))
        response.append(0)
        response.extend(struct.pack(">i", 1))
        response.extend(result_info)
        response.append(0)
        response.extend(struct.pack(">i", 0))
        response.extend(struct.pack(">i", 0))  # fetch_code
        response.extend(struct.pack(">i", 1))  # tuple_count

        pkt.parse(bytes(response))
        assert pkt.rows == []

    def test_parse_error(self) -> None:
        pkt = ExecutePacket(1, CUBRIDStatementType.SELECT)
        response = _build_error_response(DEFAULT_CAS_INFO, -1, "execute error")
        with pytest.raises(DatabaseError, match="execute error"):
            pkt.parse(response)

    def test_parse_low_protocol_version(self) -> None:
        pkt = ExecutePacket(1, CUBRIDStatementType.INSERT, protocol_version=1)
        result_info = bytearray()
        result_info.append(CUBRIDStatementType.INSERT)
        result_info.extend(struct.pack(">i", 1))
        result_info.extend(b"\x00" * DataSize.OID)
        result_info.extend(struct.pack(">i", 0))
        result_info.extend(struct.pack(">i", 0))

        response = bytearray()
        response.extend(DEFAULT_CAS_INFO)
        response.extend(struct.pack(">i", 1))
        response.append(0)
        response.extend(struct.pack(">i", 1))
        response.extend(result_info)

        pkt.parse(bytes(response))
        assert pkt.total_tuple_count == 1

    def test_parse_select_not_enough_bytes(self) -> None:
        """SELECT with no remaining bytes for fetch data."""
        pkt = ExecutePacket(1, CUBRIDStatementType.SELECT, protocol_version=7)
        cols = [ColumnMetaData(column_type=CUBRIDDataType.INT)]
        result_info = bytearray()
        result_info.append(CUBRIDStatementType.SELECT)
        result_info.extend(struct.pack(">i", 0))
        result_info.extend(b"\x00" * DataSize.OID)
        result_info.extend(struct.pack(">i", 0))
        result_info.extend(struct.pack(">i", 0))

        response = bytearray()
        response.extend(DEFAULT_CAS_INFO)
        response.extend(struct.pack(">i", 0))
        response.append(0)
        response.extend(struct.pack(">i", 1))
        response.extend(result_info)
        response.append(0)
        response.extend(struct.pack(">i", 0))
        # No fetch data

        pkt.parse(bytes(response), columns=cols)
        assert pkt.rows == []


class TestFetchPacket:
    """Tests for FetchPacket."""

    def test_write(self) -> None:
        pkt = FetchPacket(query_handle=5, current_tuple_count=10, fetch_size=50)
        data = pkt.write(DEFAULT_CAS_INFO)
        payload = data[8:]
        assert payload[0] == CASFunctionCode.FETCH

    def test_parse_success(self) -> None:
        cols = [ColumnMetaData(column_type=CUBRIDDataType.STRING)]
        pkt = FetchPacket(1, 0, 100)
        value = b"hello\x00"
        row = _build_row_data(0, [(CUBRIDDataType.STRING, value)])

        response = bytearray()
        response.extend(DEFAULT_CAS_INFO)
        response.extend(struct.pack(">i", 0))  # responseCode = 0
        response.extend(struct.pack(">i", 1))  # tuple_count
        response.extend(row)

        pkt.parse(bytes(response), columns=cols)
        assert pkt.tuple_count == 1
        assert pkt.rows[0][0] == "hello"

    def test_parse_no_columns(self) -> None:
        """Fetch without columns — no rows parsed."""
        pkt = FetchPacket(1, 0, 100)
        response = bytearray()
        response.extend(DEFAULT_CAS_INFO)
        response.extend(struct.pack(">i", 0))
        response.extend(struct.pack(">i", 0))

        pkt.parse(bytes(response))
        assert pkt.tuple_count == 0
        assert pkt.rows == []

    def test_parse_error(self) -> None:
        pkt = FetchPacket(1, 0)
        response = _build_error_response(DEFAULT_CAS_INFO, -5, "no more data")
        with pytest.raises(DatabaseError, match="no more data"):
            pkt.parse(response)


class TestCommitPacket:
    """Tests for CommitPacket."""

    def test_write(self) -> None:
        pkt = CommitPacket()
        data = pkt.write(DEFAULT_CAS_INFO)
        payload = data[8:]
        assert payload[0] == CASFunctionCode.END_TRAN

    def test_parse_success(self) -> None:
        pkt = CommitPacket()
        response = _build_success_response(DEFAULT_CAS_INFO, 0)
        pkt.parse(response)  # Should not raise

    def test_parse_error(self) -> None:
        pkt = CommitPacket()
        response = _build_error_response(DEFAULT_CAS_INFO, -1, "commit failed")
        with pytest.raises(DatabaseError, match="commit failed"):
            pkt.parse(response)


class TestRollbackPacket:
    """Tests for RollbackPacket."""

    def test_write(self) -> None:
        pkt = RollbackPacket()
        data = pkt.write(DEFAULT_CAS_INFO)
        payload = data[8:]
        assert payload[0] == CASFunctionCode.END_TRAN

    def test_parse_success(self) -> None:
        pkt = RollbackPacket()
        response = _build_success_response(DEFAULT_CAS_INFO, 0)
        pkt.parse(response)

    def test_parse_error(self) -> None:
        pkt = RollbackPacket()
        response = _build_error_response(DEFAULT_CAS_INFO, -1, "rollback failed")
        with pytest.raises(DatabaseError, match="rollback failed"):
            pkt.parse(response)


class TestCloseDatabasePacket:
    """Tests for CloseDatabasePacket."""

    def test_write(self) -> None:
        pkt = CloseDatabasePacket()
        data = pkt.write(DEFAULT_CAS_INFO)
        payload = data[8:]
        assert payload[0] == CASFunctionCode.CON_CLOSE

    def test_parse_success(self) -> None:
        pkt = CloseDatabasePacket()
        response = _build_success_response(DEFAULT_CAS_INFO, 0)
        pkt.parse(response)

    def test_parse_error(self) -> None:
        pkt = CloseDatabasePacket()
        response = _build_error_response(DEFAULT_CAS_INFO, -1, "close failed")
        with pytest.raises(DatabaseError, match="close failed"):
            pkt.parse(response)


class TestCloseQueryPacket:
    """Tests for CloseQueryPacket."""

    def test_write(self) -> None:
        pkt = CloseQueryPacket(query_handle=42)
        data = pkt.write(DEFAULT_CAS_INFO)
        payload = data[8:]
        assert payload[0] == CASFunctionCode.CLOSE_REQ_HANDLE

    def test_parse_success(self) -> None:
        pkt = CloseQueryPacket(1)
        response = _build_success_response(DEFAULT_CAS_INFO, 0)
        pkt.parse(response)

    def test_parse_error(self) -> None:
        pkt = CloseQueryPacket(1)
        response = _build_error_response(DEFAULT_CAS_INFO, -1, "close query failed")
        with pytest.raises(DatabaseError, match="close query failed"):
            pkt.parse(response)


class TestGetEngineVersionPacket:
    """Tests for GetEngineVersionPacket."""

    def test_write(self) -> None:
        pkt = GetEngineVersionPacket(auto_commit=True)
        data = pkt.write(DEFAULT_CAS_INFO)
        payload = data[8:]
        assert payload[0] == CASFunctionCode.GET_DB_VERSION

    def test_parse_success(self) -> None:
        pkt = GetEngineVersionPacket()
        version = b"11.2.0.0194\x00"
        response = DEFAULT_CAS_INFO + struct.pack(">i", len(version)) + version
        pkt.parse(response)
        assert pkt.engine_version == "11.2.0.0194"

    def test_parse_error(self) -> None:
        pkt = GetEngineVersionPacket()
        response = _build_error_response(DEFAULT_CAS_INFO, -1, "version error")
        with pytest.raises(DatabaseError, match="version error"):
            pkt.parse(response)


class TestGetSchemaPacket:
    """Tests for GetSchemaPacket."""

    def test_write(self) -> None:
        pkt = GetSchemaPacket(CCISchemaType.CLASS, "test_table")
        data = pkt.write(DEFAULT_CAS_INFO)
        payload = data[8:]
        assert payload[0] == CASFunctionCode.SCHEMA_INFO

    def test_parse_success(self) -> None:
        pkt = GetSchemaPacket(CCISchemaType.CLASS)
        response = bytearray()
        response.extend(DEFAULT_CAS_INFO)
        response.extend(struct.pack(">i", 5))  # queryHandle
        response.extend(struct.pack(">i", 10))  # tuple_count
        pkt.parse(bytes(response))
        assert pkt.query_handle == 5
        assert pkt.tuple_count == 10

    def test_parse_error(self) -> None:
        pkt = GetSchemaPacket(CCISchemaType.CLASS)
        response = _build_error_response(DEFAULT_CAS_INFO, -1, "schema error")
        with pytest.raises(DatabaseError, match="schema error"):
            pkt.parse(response)


class TestBatchExecutePacket:
    """Tests for BatchExecutePacket."""

    def test_write(self) -> None:
        pkt = BatchExecutePacket(
            ["INSERT INTO t VALUES(1)", "INSERT INTO t VALUES(2)"],
            auto_commit=True,
        )
        data = pkt.write(DEFAULT_CAS_INFO)
        payload = data[8:]
        assert payload[0] == CASFunctionCode.EXECUTE_BATCH

    def test_parse_success(self) -> None:
        pkt = BatchExecutePacket(["INSERT INTO t VALUES(1)"])
        response = bytearray()
        response.extend(DEFAULT_CAS_INFO)
        response.extend(struct.pack(">i", 2))  # result_count = 2
        # Result 1
        response.append(CUBRIDStatementType.INSERT)
        response.extend(struct.pack(">i", 1))
        # Result 2
        response.append(CUBRIDStatementType.INSERT)
        response.extend(struct.pack(">i", 1))

        pkt.parse(bytes(response))
        assert len(pkt.results) == 2
        assert pkt.results[0] == (CUBRIDStatementType.INSERT, 1)
        assert pkt.results[1] == (CUBRIDStatementType.INSERT, 1)

    def test_parse_error(self) -> None:
        pkt = BatchExecutePacket(["BAD SQL"])
        response = _build_error_response(DEFAULT_CAS_INFO, -1, "batch error")
        with pytest.raises(DatabaseError, match="batch error"):
            pkt.parse(response)


class TestLOBNewPacket:
    """Tests for LOBNewPacket."""

    def test_write(self) -> None:
        pkt = LOBNewPacket(CCILOBType.BLOB)
        data = pkt.write(DEFAULT_CAS_INFO)
        payload = data[8:]
        assert payload[0] == CASFunctionCode.LOB_NEW

    def test_parse_success(self) -> None:
        pkt = LOBNewPacket(CCILOBType.BLOB)
        lob_handle = b"\x01\x02\x03\x04\x05\x06\x07\x08"
        response = (
            DEFAULT_CAS_INFO
            + struct.pack(">i", 0)  # responseCode = success
            + lob_handle
        )
        pkt.parse(response)
        assert pkt.lob_handle == lob_handle

    def test_parse_error(self) -> None:
        pkt = LOBNewPacket(CCILOBType.BLOB)
        response = _build_error_response(DEFAULT_CAS_INFO, -1, "lob error")
        with pytest.raises(DatabaseError, match="lob error"):
            pkt.parse(response)


class TestLOBWritePacket:
    """Tests for LOBWritePacket."""

    def test_write(self) -> None:
        pkt = LOBWritePacket(b"\x01\x02\x03\x04", 0, b"hello world")
        data = pkt.write(DEFAULT_CAS_INFO)
        payload = data[8:]
        assert payload[0] == CASFunctionCode.LOB_WRITE

    def test_parse_success(self) -> None:
        pkt = LOBWritePacket(b"", 0, b"")
        response = _build_success_response(DEFAULT_CAS_INFO, 0)
        pkt.parse(response)

    def test_parse_error(self) -> None:
        pkt = LOBWritePacket(b"", 0, b"")
        response = _build_error_response(DEFAULT_CAS_INFO, -1, "write error")
        with pytest.raises(DatabaseError, match="write error"):
            pkt.parse(response)


class TestLOBReadPacket:
    """Tests for LOBReadPacket."""

    def test_write(self) -> None:
        pkt = LOBReadPacket(b"\x01\x02", 0, 1024)
        data = pkt.write(DEFAULT_CAS_INFO)
        payload = data[8:]
        assert payload[0] == CASFunctionCode.LOB_READ

    def test_parse_success(self) -> None:
        pkt = LOBReadPacket(b"", 0, 5)
        lob_data = b"hello"
        response = (
            DEFAULT_CAS_INFO
            + struct.pack(">i", len(lob_data))  # bytes_read
            + lob_data
        )
        pkt.parse(response)
        assert pkt.bytes_read == 5
        assert pkt.lob_data == b"hello"

    def test_parse_zero_bytes(self) -> None:
        pkt = LOBReadPacket(b"", 0, 0)
        response = _build_success_response(DEFAULT_CAS_INFO, 0)
        pkt.parse(response)
        assert pkt.bytes_read == 0
        assert pkt.lob_data == b""

    def test_parse_error(self) -> None:
        pkt = LOBReadPacket(b"", 0, 0)
        response = _build_error_response(DEFAULT_CAS_INFO, -1, "read error")
        with pytest.raises(DatabaseError, match="read error"):
            pkt.parse(response)


class TestGetLastInsertIdPacket:
    """Tests for GetLastInsertIdPacket."""

    def test_write(self) -> None:
        pkt = GetLastInsertIdPacket()
        data = pkt.write(DEFAULT_CAS_INFO)
        payload = data[8:]
        assert payload[0] == CASFunctionCode.GET_LAST_INSERT_ID

    def test_parse_success(self) -> None:
        pkt = GetLastInsertIdPacket()
        id_str = b"42\x00"
        response = DEFAULT_CAS_INFO + struct.pack(">i", len(id_str)) + id_str
        pkt.parse(response)
        assert pkt.last_insert_id == "42"

    def test_parse_zero_response_code(self) -> None:
        """responseCode=0 means no last insert id."""
        pkt = GetLastInsertIdPacket()
        response = _build_success_response(DEFAULT_CAS_INFO, 0)
        pkt.parse(response)
        assert pkt.last_insert_id == ""

    def test_parse_error(self) -> None:
        pkt = GetLastInsertIdPacket()
        response = _build_error_response(DEFAULT_CAS_INFO, -1, "no insert id")
        with pytest.raises(DatabaseError, match="no insert id"):
            pkt.parse(response)


class TestGetDbParameterPacket:
    """Tests for GetDbParameterPacket."""

    def test_write(self) -> None:
        pkt = GetDbParameterPacket(CCIDbParam.ISOLATION_LEVEL)
        data = pkt.write(DEFAULT_CAS_INFO)
        payload = data[8:]
        assert payload[0] == CASFunctionCode.GET_DB_PARAMETER

    def test_parse_success(self) -> None:
        pkt = GetDbParameterPacket(CCIDbParam.ISOLATION_LEVEL)
        response = (
            DEFAULT_CAS_INFO
            + struct.pack(">i", 0)  # responseCode
            + struct.pack(">i", 4)  # value
        )
        pkt.parse(response)
        assert pkt.value == 4

    def test_parse_error(self) -> None:
        pkt = GetDbParameterPacket(CCIDbParam.ISOLATION_LEVEL)
        response = _build_error_response(DEFAULT_CAS_INFO, -1, "param error")
        with pytest.raises(DatabaseError, match="param error"):
            pkt.parse(response)


class TestSetDbParameterPacket:
    """Tests for SetDbParameterPacket."""

    def test_write(self) -> None:
        pkt = SetDbParameterPacket(CCIDbParam.AUTO_COMMIT, 1)
        data = pkt.write(DEFAULT_CAS_INFO)
        payload = data[8:]
        assert payload[0] == CASFunctionCode.SET_DB_PARAMETER

    def test_parse_success(self) -> None:
        pkt = SetDbParameterPacket(CCIDbParam.AUTO_COMMIT, 1)
        response = _build_success_response(DEFAULT_CAS_INFO, 0)
        pkt.parse(response)

    def test_parse_error(self) -> None:
        pkt = SetDbParameterPacket(CCIDbParam.AUTO_COMMIT, 1)
        response = _build_error_response(DEFAULT_CAS_INFO, -1, "set error")
        with pytest.raises(DatabaseError, match="set error"):
            pkt.parse(response)


# ---------------------------------------------------------------------------
# Write verification tests — ensure exact binary encoding
# ---------------------------------------------------------------------------


class TestCommitRollbackEncoding:
    """Verify commit/rollback write the correct transaction type byte."""

    def test_commit_encodes_commit_type(self) -> None:
        pkt = CommitPacket()
        data = pkt.write(DEFAULT_CAS_INFO)
        payload = data[8:]
        # FC byte + length-prefixed byte: 4-byte len (1) + byte value
        assert payload[0] == CASFunctionCode.END_TRAN
        # add_byte writes 4-byte length prefix (value=1) then the byte
        assert struct.unpack(">i", payload[1:5])[0] == 1  # length = 1
        assert payload[5] == CCITransactionType.COMMIT

    def test_rollback_encodes_rollback_type(self) -> None:
        pkt = RollbackPacket()
        data = pkt.write(DEFAULT_CAS_INFO)
        payload = data[8:]
        assert payload[0] == CASFunctionCode.END_TRAN
        assert struct.unpack(">i", payload[1:5])[0] == 1
        assert payload[5] == CCITransactionType.ROLLBACK


class TestFetchEncoding:
    """Verify FetchPacket write encodes start position correctly."""

    def test_fetch_start_position(self) -> None:
        pkt = FetchPacket(query_handle=7, current_tuple_count=5, fetch_size=50)
        data = pkt.write(DEFAULT_CAS_INFO)
        payload = data[8:]
        # FC + addInt(queryHandle) + addInt(startPos) + ...
        assert payload[0] == CASFunctionCode.FETCH
        # addInt: 4-byte len prefix (4) + 4-byte value
        # queryHandle
        qh_offset = 1 + 4  # FC + length prefix
        assert struct.unpack(">i", payload[qh_offset : qh_offset + 4])[0] == 7
        # startPos = current_tuple_count + 1 = 6
        sp_offset = qh_offset + 4 + 4  # value + next length prefix
        assert struct.unpack(">i", payload[sp_offset : sp_offset + 4])[0] == 6


class TestCloseQueryEncoding:
    """Verify CloseQueryPacket encodes query handle."""

    def test_close_query_handle(self) -> None:
        pkt = CloseQueryPacket(query_handle=99)
        data = pkt.write(DEFAULT_CAS_INFO)
        payload = data[8:]
        assert payload[0] == CASFunctionCode.CLOSE_REQ_HANDLE
        # addInt: 4-byte length (4) + 4-byte value
        assert struct.unpack(">i", payload[5:9])[0] == 99


class TestProtocolHeaderIntegration:
    """Verify protocol headers are correct."""

    def test_header_data_length(self) -> None:
        pkt = CloseDatabasePacket()
        data = pkt.write(DEFAULT_CAS_INFO)
        data_len = struct.unpack(">i", data[:4])[0]
        assert data_len == len(data) - 8  # payload length

    def test_header_cas_info(self) -> None:
        custom_cas = b"\xaa\xbb\xcc\xdd"
        pkt = CommitPacket()
        data = pkt.write(custom_cas)
        assert data[4:8] == custom_cas


class TestMultipleColumnRowParsing:
    """Test parsing rows with multiple columns of different types."""

    def test_mixed_types(self) -> None:
        cols = [
            ColumnMetaData(column_type=CUBRIDDataType.INT),
            ColumnMetaData(column_type=CUBRIDDataType.STRING),
            ColumnMetaData(column_type=CUBRIDDataType.DOUBLE),
        ]
        int_val = struct.pack(">i", 42)
        str_val = b"test\x00"
        dbl_val = struct.pack(">d", 3.14)
        row = _build_row_data(
            0,
            [
                (CUBRIDDataType.INT, int_val),
                (CUBRIDDataType.STRING, str_val),
                (CUBRIDDataType.DOUBLE, dbl_val),
            ],
        )
        reader = PacketReader(row)
        rows = _parse_row_data(reader, 1, cols, CUBRIDStatementType.SELECT)
        assert rows[0][0] == 42
        assert rows[0][1] == "test"
        assert abs(rows[0][2] - 3.14) < 0.001


class TestUnicodeStrings:
    """Test Unicode string handling in packets."""

    def test_open_database_unicode(self) -> None:
        pkt = OpenDatabasePacket("테스트db", "사용자", "비밀번호")
        data = pkt.write()
        assert len(data) == 628

    def test_prepare_unicode_sql(self) -> None:
        pkt = PrepareAndExecutePacket("SELECT '한글' AS name")
        data = pkt.write(DEFAULT_CAS_INFO)
        assert len(data) > 8

    def test_batch_unicode(self) -> None:
        pkt = BatchExecutePacket(["INSERT INTO t VALUES('日本語')"])
        data = pkt.write(DEFAULT_CAS_INFO)
        payload = data[8:]
        assert payload[0] == CASFunctionCode.EXECUTE_BATCH
