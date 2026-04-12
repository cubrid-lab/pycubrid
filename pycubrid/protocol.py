"""CAS protocol packet classes for pycubrid.

Implements serialization/deserialization for all CUBRID CAS broker protocol
packets. Each packet class provides write() for request serialization and
parse() for response deserialization.
"""

from __future__ import annotations

import struct
from dataclasses import dataclass
from typing import Any

from .constants import (
    CASFunctionCode,
    CASProtocol,
    CCIExecutionOption,
    CCIPrepareOption,
    CCITransactionType,
    CUBRIDDataType,
    CUBRIDStatementType,
    DataSize,
)
from .exceptions import DatabaseError, IntegrityError, ProgrammingError
from .packet import PacketReader, PacketWriter


# ---------------------------------------------------------------------------
# Data Classes
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class ColumnMetaData:
    """Metadata for a single result column."""

    column_type: int = 0
    scale: int = -1
    precision: int = -1
    name: str = ""
    real_name: str = ""
    table_name: str = ""
    is_nullable: bool = False
    default_value: str = ""
    is_auto_increment: bool = False
    is_unique_key: bool = False
    is_primary_key: bool = False
    is_reverse_index: bool = False
    is_reverse_unique: bool = False
    is_foreign_key: bool = False
    is_shared: bool = False


@dataclass(slots=True)
class ResultInfo:
    """Result info for each executed statement."""

    stmt_type: int = 0
    result_count: int = 0
    oid: bytes = b""
    cache_time_sec: int = 0
    cache_time_usec: int = 0


# ---------------------------------------------------------------------------
# Helper Functions
# ---------------------------------------------------------------------------


def _raise_error(reader: PacketReader, response_length: int) -> None:
    """Parse an error response and raise the appropriate DB-API exception."""
    error_code, error_message = reader.read_error(response_length)
    msg_lower = error_message.lower()
    if any(
        kw in msg_lower for kw in ("unique", "duplicate", "foreign key", "constraint violation")
    ):
        raise IntegrityError(msg=error_message, code=error_code)
    if any(kw in msg_lower for kw in ("syntax", "unknown class", "does not exist", "not found")):
        raise ProgrammingError(msg=error_message, code=error_code)
    raise DatabaseError(msg=error_message, code=error_code)


def _parse_column_metadata(reader: PacketReader, column_count: int) -> list[ColumnMetaData]:
    """Parse column metadata entries from the reader."""
    columns: list[ColumnMetaData] = []
    for _ in range(column_count):
        legacy_type = reader._parse_byte()
        if legacy_type & 0x80:
            column_type = reader._parse_byte()
        else:
            column_type = legacy_type
        scale = reader._parse_short()
        precision = reader._parse_int()

        name_len = reader._parse_int()
        name = reader._parse_null_terminated_string(name_len)
        real_name_len = reader._parse_int()
        real_name = reader._parse_null_terminated_string(real_name_len)
        table_name_len = reader._parse_int()
        table_name = reader._parse_null_terminated_string(table_name_len)

        is_nullable = reader._parse_byte() == 1
        default_len = reader._parse_int()
        default_value = reader._parse_null_terminated_string(default_len)
        is_auto_increment = reader._parse_byte() == 1
        is_unique_key = reader._parse_byte() == 1
        is_primary_key = reader._parse_byte() == 1
        is_reverse_index = reader._parse_byte() == 1
        is_reverse_unique = reader._parse_byte() == 1
        is_foreign_key = reader._parse_byte() == 1
        is_shared = reader._parse_byte() == 1

        columns.append(
            ColumnMetaData(
                column_type=column_type,
                scale=scale,
                precision=precision,
                name=name,
                real_name=real_name,
                table_name=table_name,
                is_nullable=is_nullable,
                default_value=default_value,
                is_auto_increment=is_auto_increment,
                is_unique_key=is_unique_key,
                is_primary_key=is_primary_key,
                is_reverse_index=is_reverse_index,
                is_reverse_unique=is_reverse_unique,
                is_foreign_key=is_foreign_key,
                is_shared=is_shared,
            )
        )
    return columns


# ---------------------------------------------------------------------------
# Type dispatch: method-name table
# ---------------------------------------------------------------------------

_TYPE_METHOD_NAMES: dict[int, str] = {
    CUBRIDDataType.CHAR: "_parse_null_terminated_string",
    CUBRIDDataType.STRING: "_parse_null_terminated_string",
    CUBRIDDataType.NCHAR: "_parse_null_terminated_string",
    CUBRIDDataType.VARNCHAR: "_parse_null_terminated_string",
    CUBRIDDataType.ENUM: "_parse_null_terminated_string",
    CUBRIDDataType.SHORT: "_parse_short",
    CUBRIDDataType.INT: "_parse_int",
    CUBRIDDataType.BIGINT: "_parse_long",
    CUBRIDDataType.FLOAT: "_parse_float",
    CUBRIDDataType.DOUBLE: "_parse_double",
    CUBRIDDataType.MONETARY: "_parse_double",
    CUBRIDDataType.NUMERIC: "_parse_numeric",
    CUBRIDDataType.DATE: "_parse_date",
    CUBRIDDataType.TIME: "_parse_time",
    CUBRIDDataType.DATETIME: "_parse_datetime",
    CUBRIDDataType.TIMESTAMP: "_parse_timestamp",
    CUBRIDDataType.OBJECT: "_parse_object",
    CUBRIDDataType.BIT: "_parse_bytes",
    CUBRIDDataType.VARBIT: "_parse_bytes",
    CUBRIDDataType.SET: "_parse_bytes",
    CUBRIDDataType.MULTISET: "_parse_bytes",
    CUBRIDDataType.SEQUENCE: "_parse_bytes",
    CUBRIDDataType.BLOB: "read_blob",
    CUBRIDDataType.CLOB: "read_clob",
}


def _resolve_reader(reader: PacketReader, col_type: int) -> Any:
    method_name = _TYPE_METHOD_NAMES.get(col_type)
    if method_name is not None:
        return getattr(reader, method_name)
    return reader._parse_bytes


def _read_value(reader: PacketReader, column_type: int, size: int) -> Any:
    if column_type == CUBRIDDataType.NULL:
        return None
    return _resolve_reader(reader, column_type)(size)


def _parse_row_data(
    reader: PacketReader,
    tuple_count: int,
    columns: list[ColumnMetaData],
    statement_type: int,
) -> list[tuple[Any, ...]]:
    """Parse row data from the reader."""
    is_call_type = statement_type in (
        CUBRIDStatementType.CALL,
        CUBRIDStatementType.EVALUATE,
        CUBRIDStatementType.CALL_SP,
    )
    ncols = len(columns)
    col_types = [col.column_type for col in columns]

    _parse_int = reader._parse_int
    _parse_bytes = reader._parse_bytes
    _parse_byte = reader._parse_byte
    _skip_bytes = reader._skip_bytes
    _null_type = CUBRIDDataType.NULL
    _oid_size = DataSize.OID
    _get = _TYPE_METHOD_NAMES.get
    _getattr = getattr

    if is_call_type or _null_type in col_types:
        col_readers = None
    else:
        col_readers = [_resolve_reader(reader, ct) for ct in col_types]

    rows: list[tuple[Any, ...]] = []
    _rows_append = rows.append

    for _ in range(tuple_count):
        _parse_int()
        _skip_bytes(_oid_size)
        row: list[Any] = [None] * ncols
        if col_readers is not None:
            for i in range(ncols):
                size = _parse_int()
                if size > 0:
                    row[i] = col_readers[i](size)
        else:
            for i in range(ncols):
                size = _parse_int()
                if size <= 0:
                    continue
                ct = col_types[i]
                if is_call_type or ct == _null_type:
                    ct = _parse_byte()
                    size -= 1
                    if size <= 0:
                        continue
                method_name = _get(ct)
                if method_name is not None:
                    row[i] = _getattr(reader, method_name)(size)
                else:
                    row[i] = _parse_bytes(size)
        _rows_append(tuple(row))
    return rows


def _parse_result_infos(reader: PacketReader, result_count: int) -> list[ResultInfo]:
    """Parse result info entries."""
    infos: list[ResultInfo] = []
    for _ in range(result_count):
        stmt_type = reader._parse_byte()
        count = reader._parse_int()
        oid = reader._parse_bytes(DataSize.OID)
        cache_sec = reader._parse_int()
        cache_usec = reader._parse_int()
        infos.append(
            ResultInfo(
                stmt_type=stmt_type,
                result_count=count,
                oid=oid,
                cache_time_sec=cache_sec,
                cache_time_usec=cache_usec,
            )
        )
    return infos


# ---------------------------------------------------------------------------
# Packet Classes
# ---------------------------------------------------------------------------


class ClientInfoExchangePacket:
    """Initial handshake packet (no DATA_LENGTH/CAS_INFO framing)."""

    def __init__(self) -> None:
        self.new_connection_port: int = 0

    def write(self) -> bytes:
        """Serialize the handshake packet (10 bytes, no protocol header)."""
        buf = bytearray()
        buf.extend(CASProtocol.MAGIC_STRING.encode("ascii"))
        buf.append(CASProtocol.CLIENT_JDBC)
        buf.append(CASProtocol.CAS_VERSION)
        buf.extend(b"\x00\x00\x00")
        return bytes(buf)

    def parse(self, data: bytes | bytearray) -> None:
        """Parse the handshake response (4-byte int)."""
        self.new_connection_port = struct.unpack(">i", data[:4])[0]


class OpenDatabasePacket:
    """Open a database connection."""

    def __init__(self, database: str, user: str, password: str) -> None:
        self.database = database
        self.user = user
        self.password = password
        self.cas_info: bytes = b""
        self.response_code: int = 0
        self.broker_info: dict[str, int] = {}
        self.session_id: int = 0

    def write(self) -> bytes:
        """Serialize the open database packet (628 bytes, no header)."""
        writer = PacketWriter(reserve_header=False)
        writer._write_fixed_length_string(self.database, 32)
        writer._write_fixed_length_string(self.user, 32)
        writer._write_fixed_length_string(self.password, 32)
        writer._write_filler(512)  # extended info
        writer._write_filler(20)  # reserved
        return writer.to_bytes()

    def parse(self, data: bytes | bytearray) -> None:
        """Parse the open database response.

        ``data`` starts after the 4-byte DATA_LENGTH prefix, so it begins
        with casInfo(4B).
        """
        reader = PacketReader(data)
        self.cas_info = reader._parse_bytes(DataSize.CAS_INFO)
        self.response_code = reader._parse_int()
        if self.response_code < 0:
            remaining = len(data) - 8  # 4 cas_info + 4 response_code
            _raise_error(reader, remaining)
        broker_bytes = reader._parse_bytes(DataSize.BROKER_INFO)
        self.broker_info = {
            "db_type": broker_bytes[0],
            "protocol_version": broker_bytes[4] & 0x3F,
            "statement_pooling": broker_bytes[2],
        }
        self.session_id = reader._parse_int()


class PrepareAndExecutePacket:
    """Combined prepare-and-execute packet (FC=41)."""

    def __init__(
        self,
        sql: str,
        auto_commit: bool = False,
        protocol_version: int = CASProtocol.VERSION,
    ) -> None:
        self.sql = sql
        self.auto_commit = auto_commit
        self.protocol_version = protocol_version

        self.response_code: int = 0
        self.query_handle: int = 0
        self.statement_type: int = 0
        self.bind_count: int = 0
        self.column_count: int = 0
        self.columns: list[ColumnMetaData] = []
        self.total_tuple_count: int = 0
        self.result_count: int = 0
        self.result_infos: list[ResultInfo] = []
        self.tuple_count: int = 0
        self.rows: list[tuple[Any, ...]] = []

    def write(self, cas_info: bytes) -> bytes:
        """Serialize the prepare-and-execute request."""
        writer = PacketWriter()
        writer._write_byte(CASFunctionCode.PREPARE_AND_EXECUTE)
        writer.add_int(3)  # arg count
        writer._write_null_terminated_string(self.sql)
        writer.add_byte(CCIPrepareOption.NORMAL)
        writer.add_byte(1 if self.auto_commit else 0)
        writer.add_byte(CCIExecutionOption.QUERY_ALL)
        writer.add_int(0)  # max_col_size
        writer.add_int(0)  # max_row_size
        writer._write_int(0)  # NULL
        writer._write_int(DataSize.LONG)  # cache time length
        writer._write_int(0)  # cache time sec
        writer._write_int(0)  # cache time usec
        writer.add_int(0)  # query timeout
        return writer.finalize(cas_info)

    def parse(self, data: bytes | bytearray) -> None:
        """Parse the prepare-and-execute response.

        ``data`` starts after the 4-byte DATA_LENGTH prefix.
        """
        reader = PacketReader(data)
        reader._skip_bytes(DataSize.CAS_INFO)
        self.response_code = reader._parse_int()
        if self.response_code < 0:
            remaining = len(data) - 8
            _raise_error(reader, remaining)

        self.query_handle = self.response_code
        _ = reader._parse_int()  # result cache lifetime
        self.statement_type = reader._parse_byte()
        self.bind_count = reader._parse_int()
        _ = reader._parse_byte()  # is_updatable
        self.column_count = reader._parse_int()
        self.columns = _parse_column_metadata(reader, self.column_count)

        self.total_tuple_count = reader._parse_int()
        _ = reader._parse_byte()  # cache_reusable
        self.result_count = reader._parse_int()
        self.result_infos = _parse_result_infos(reader, self.result_count)

        # Protocol version dependent fields
        if self.protocol_version > 1:
            _ = reader._parse_byte()  # includes_column_info
        if self.protocol_version > 4:
            _ = reader._parse_int()  # shard_id

        # If SELECT, parse inline fetch data
        if self.statement_type == CUBRIDStatementType.SELECT:
            if reader.bytes_remaining() >= 8:
                _ = reader._parse_int()  # fetch_code
                self.tuple_count = reader._parse_int()
                if self.tuple_count > 0:
                    self.rows = _parse_row_data(
                        reader,
                        self.tuple_count,
                        self.columns,
                        self.statement_type,
                    )


class PreparePacket:
    """Prepare a statement (FC=2)."""

    def __init__(self, sql: str, auto_commit: bool = False) -> None:
        self.sql = sql
        self.auto_commit = auto_commit

        self.response_code: int = 0
        self.query_handle: int = 0
        self.statement_type: int = 0
        self.bind_count: int = 0
        self.column_count: int = 0
        self.columns: list[ColumnMetaData] = []

    def write(self, cas_info: bytes) -> bytes:
        """Serialize the prepare request."""
        writer = PacketWriter()
        writer._write_byte(CASFunctionCode.PREPARE)
        writer._write_null_terminated_string(self.sql)
        writer.add_byte(CCIPrepareOption.NORMAL)
        writer.add_byte(1 if self.auto_commit else 0)
        return writer.finalize(cas_info)

    def parse(self, data: bytes | bytearray) -> None:
        """Parse the prepare response."""
        reader = PacketReader(data)
        reader._skip_bytes(DataSize.CAS_INFO)
        self.response_code = reader._parse_int()
        if self.response_code < 0:
            remaining = len(data) - 8
            _raise_error(reader, remaining)

        self.query_handle = self.response_code
        _ = reader._parse_int()  # result cache lifetime
        self.statement_type = reader._parse_byte()
        self.bind_count = reader._parse_int()
        _ = reader._parse_byte()  # is_updatable
        self.column_count = reader._parse_int()
        self.columns = _parse_column_metadata(reader, self.column_count)


class ExecutePacket:
    """Execute a prepared statement (FC=3)."""

    def __init__(
        self,
        query_handle: int,
        statement_type: int,
        auto_commit: bool = False,
        protocol_version: int = CASProtocol.VERSION,
    ) -> None:
        self.query_handle = query_handle
        self.statement_type = statement_type
        self.auto_commit = auto_commit
        self.protocol_version = protocol_version

        self.total_tuple_count: int = 0
        self.result_count: int = 0
        self.result_infos: list[ResultInfo] = []
        self.tuple_count: int = 0
        self.rows: list[tuple[Any, ...]] = []
        self.columns: list[ColumnMetaData] = []

    def write(self, cas_info: bytes) -> bytes:
        """Serialize the execute request."""
        fetch_flag = 1 if self.statement_type == CUBRIDStatementType.SELECT else 0
        writer = PacketWriter()
        writer._write_byte(CASFunctionCode.EXECUTE)
        writer.add_int(self.query_handle)
        writer.add_byte(CCIExecutionOption.NORMAL)
        writer.add_int(0)  # max_col_size
        writer.add_int(0)  # max_row_size
        writer.add_null()  # NULL
        writer._write_int(1)  # bind mode count
        writer._write_byte(fetch_flag)
        writer.add_byte(1 if self.auto_commit else 0)
        writer.add_byte(1)  # forward only
        writer.add_cache_time()
        writer.add_int(0)  # query timeout
        return writer.finalize(cas_info)

    def parse(self, data: bytes | bytearray, columns: list[ColumnMetaData] | None = None) -> None:
        """Parse the execute response."""
        if columns is not None:
            self.columns = columns
        reader = PacketReader(data)
        reader._skip_bytes(DataSize.CAS_INFO)
        response_code = reader._parse_int()
        if response_code < 0:
            remaining = len(data) - 8
            _raise_error(reader, remaining)

        self.total_tuple_count = response_code
        _ = reader._parse_byte()  # cache_reusable
        self.result_count = reader._parse_int()
        self.result_infos = _parse_result_infos(reader, self.result_count)

        if self.protocol_version > 1:
            _ = reader._parse_byte()  # includes_column_info
        if self.protocol_version > 4:
            _ = reader._parse_int()  # shard_id

        if self.statement_type == CUBRIDStatementType.SELECT:
            if reader.bytes_remaining() >= 8:
                _ = reader._parse_int()  # fetch_code
                self.tuple_count = reader._parse_int()
                if self.tuple_count > 0 and self.columns:
                    self.rows = _parse_row_data(
                        reader,
                        self.tuple_count,
                        self.columns,
                        self.statement_type,
                    )


class FetchPacket:
    """Fetch result rows (FC=8)."""

    def __init__(
        self,
        query_handle: int,
        current_tuple_count: int,
        fetch_size: int = 100,
        columns: list[ColumnMetaData] | None = None,
        statement_type: int = CUBRIDStatementType.SELECT,
    ) -> None:
        self.query_handle = query_handle
        self.current_tuple_count = current_tuple_count
        self.fetch_size = fetch_size
        self._columns = columns
        self._statement_type = statement_type

        self.tuple_count: int = 0
        self.rows: list[tuple[Any, ...]] = []

    def write(self, cas_info: bytes) -> bytes:
        """Serialize the fetch request."""
        writer = PacketWriter()
        writer._write_byte(CASFunctionCode.FETCH)
        writer.add_int(self.query_handle)
        writer.add_int(self.current_tuple_count + 1)
        writer.add_int(self.fetch_size)
        writer.add_byte(0)  # case sensitive
        writer.add_int(0)  # resultset index
        return writer.finalize(cas_info)

    def parse(
        self,
        data: bytes | bytearray,
        columns: list[ColumnMetaData] | None = None,
        statement_type: int | None = None,
    ) -> None:
        """Parse the fetch response."""
        reader = PacketReader(data)
        reader._skip_bytes(DataSize.CAS_INFO)
        response_code = reader._parse_int()
        if response_code < 0:
            remaining = len(data) - 8
            _raise_error(reader, remaining)

        effective_columns = columns if columns is not None else self._columns
        effective_stmt_type = statement_type if statement_type is not None else self._statement_type

        self.tuple_count = reader._parse_int()
        if self.tuple_count > 0 and effective_columns:
            self.rows = _parse_row_data(
                reader, self.tuple_count, effective_columns, effective_stmt_type
            )


class CommitPacket:
    """Commit transaction (FC=1)."""

    def write(self, cas_info: bytes) -> bytes:
        """Serialize the commit request."""
        writer = PacketWriter()
        writer._write_byte(CASFunctionCode.END_TRAN)
        writer.add_byte(CCITransactionType.COMMIT)
        return writer.finalize(cas_info)

    def parse(self, data: bytes | bytearray) -> None:
        """Parse the commit response."""
        reader = PacketReader(data)
        reader._skip_bytes(DataSize.CAS_INFO)
        response_code = reader._parse_int()
        if response_code < 0:
            remaining = len(data) - 8
            _raise_error(reader, remaining)


class RollbackPacket:
    """Rollback transaction (FC=1)."""

    def write(self, cas_info: bytes) -> bytes:
        """Serialize the rollback request."""
        writer = PacketWriter()
        writer._write_byte(CASFunctionCode.END_TRAN)
        writer.add_byte(CCITransactionType.ROLLBACK)
        return writer.finalize(cas_info)

    def parse(self, data: bytes | bytearray) -> None:
        """Parse the rollback response."""
        reader = PacketReader(data)
        reader._skip_bytes(DataSize.CAS_INFO)
        response_code = reader._parse_int()
        if response_code < 0:
            remaining = len(data) - 8
            _raise_error(reader, remaining)


class CloseDatabasePacket:
    """Close database connection (FC=31)."""

    def write(self, cas_info: bytes) -> bytes:
        """Serialize the close database request."""
        writer = PacketWriter()
        writer._write_byte(CASFunctionCode.CON_CLOSE)
        return writer.finalize(cas_info)

    def parse(self, data: bytes | bytearray) -> None:
        """Parse the close database response."""
        reader = PacketReader(data)
        reader._skip_bytes(DataSize.CAS_INFO)
        response_code = reader._parse_int()
        if response_code < 0:
            remaining = len(data) - 8
            _raise_error(reader, remaining)


class CloseQueryPacket:
    """Close a query handle (FC=6)."""

    def __init__(self, query_handle: int) -> None:
        self.query_handle = query_handle

    def write(self, cas_info: bytes) -> bytes:
        """Serialize the close query request."""
        writer = PacketWriter()
        writer._write_byte(CASFunctionCode.CLOSE_REQ_HANDLE)
        writer.add_int(self.query_handle)
        return writer.finalize(cas_info)

    def parse(self, data: bytes | bytearray) -> None:
        """Parse the close query response."""
        reader = PacketReader(data)
        reader._skip_bytes(DataSize.CAS_INFO)
        response_code = reader._parse_int()
        if response_code < 0:
            remaining = len(data) - 8
            _raise_error(reader, remaining)


class GetEngineVersionPacket:
    """Get the database engine version (FC=15)."""

    def __init__(self, auto_commit: bool = True) -> None:
        self.auto_commit = auto_commit
        self.engine_version: str = ""

    def write(self, cas_info: bytes) -> bytes:
        """Serialize the get engine version request."""
        writer = PacketWriter()
        writer._write_byte(CASFunctionCode.GET_DB_VERSION)
        writer.add_byte(1 if self.auto_commit else 0)
        return writer.finalize(cas_info)

    def parse(self, data: bytes | bytearray) -> None:
        """Parse the get engine version response."""
        reader = PacketReader(data)
        reader._skip_bytes(DataSize.CAS_INFO)
        response_code = reader._parse_int()
        if response_code < 0:
            remaining = len(data) - 8
            _raise_error(reader, remaining)
        # response_code is 0 on success; version string follows
        version_len = len(data) - DataSize.CAS_INFO - DataSize.INT
        self.engine_version = reader._parse_null_terminated_string(version_len)


class GetSchemaPacket:
    """Get schema information (FC=9)."""

    def __init__(
        self,
        schema_type: int,
        table_name: str = "",
        pattern_match_flag: int = 1,
    ) -> None:
        self.schema_type = schema_type
        self.table_name = table_name
        self.pattern_match_flag = pattern_match_flag

        self.query_handle: int = 0
        self.tuple_count: int = 0

    def write(self, cas_info: bytes) -> bytes:
        """Serialize the get schema request."""
        writer = PacketWriter()
        writer._write_byte(CASFunctionCode.SCHEMA_INFO)
        writer.add_int(self.schema_type)
        writer._write_null_terminated_string(self.table_name)
        writer.add_byte(self.pattern_match_flag)
        return writer.finalize(cas_info)

    def parse(self, data: bytes | bytearray) -> None:
        """Parse the get schema response."""
        reader = PacketReader(data)
        reader._skip_bytes(DataSize.CAS_INFO)
        response_code = reader._parse_int()
        if response_code < 0:
            remaining = len(data) - 8
            _raise_error(reader, remaining)
        self.query_handle = response_code
        self.tuple_count = reader._parse_int()


class BatchExecutePacket:
    """Batch execute multiple SQL statements (FC=20)."""

    def __init__(
        self,
        sql_list: list[str],
        auto_commit: bool = False,
        protocol_version: int = CASProtocol.VERSION,
    ) -> None:
        self.sql_list = sql_list
        self.auto_commit = auto_commit
        self.protocol_version = protocol_version
        self.results: list[tuple[int, int]] = []
        self.errors: list[dict[str, Any]] = []

    def write(self, cas_info: bytes) -> bytes:
        """Serialize the batch execute request."""
        writer = PacketWriter()
        writer._write_byte(CASFunctionCode.EXECUTE_BATCH)
        writer.add_byte(1 if self.auto_commit else 0)
        if self.protocol_version > 3:
            writer.add_int(0)  # timeout
        for sql in self.sql_list:
            writer._write_null_terminated_string(sql)
        return writer.finalize(cas_info)

    def parse(self, data: bytes | bytearray) -> None:
        """Parse the batch execute response."""
        reader = PacketReader(data)
        reader._skip_bytes(DataSize.CAS_INFO)
        response_code = reader._parse_int()
        if response_code < 0:
            remaining = len(data) - 8
            _raise_error(reader, remaining)
        executed_count = reader._parse_int()
        self.results = []
        self.errors = []
        for _ in range(executed_count):
            stmt_type = reader._parse_byte()
            result = reader._parse_int()
            if result < 0:
                error_code = reader._parse_int() if self.protocol_version > 2 else result
                msg_len = reader._parse_int()
                error_msg = reader._parse_null_terminated_string(msg_len)
                self.errors.append({"code": error_code, "message": error_msg})
            else:
                self.results.append((stmt_type, result))
                reader._parse_int()  # unused
                reader._parse_short()  # unused
                reader._parse_short()  # unused
        if self.protocol_version > 4:
            _ = reader._parse_int()  # lastShardId


class LOBNewPacket:
    """Create a new LOB handle (FC=35)."""

    def __init__(self, lob_type: int) -> None:
        self.lob_type = lob_type
        self.lob_handle: bytes = b""

    def write(self, cas_info: bytes) -> bytes:
        """Serialize the LOB new request."""
        writer = PacketWriter()
        writer._write_byte(CASFunctionCode.LOB_NEW)
        writer.add_int(self.lob_type)
        return writer.finalize(cas_info)

    def parse(self, data: bytes | bytearray) -> None:
        """Parse the LOB new response."""
        reader = PacketReader(data)
        reader._skip_bytes(DataSize.CAS_INFO)
        response_code = reader._parse_int()
        if response_code < 0:
            remaining = len(data) - 8
            _raise_error(reader, remaining)
        # Remaining bytes are the LOB handle
        self.lob_handle = reader._parse_bytes(reader.bytes_remaining())


class LOBWritePacket:
    """Write data to a LOB (FC=36)."""

    def __init__(self, packed_lob_handle: bytes, offset: int, data: bytes) -> None:
        self.packed_lob_handle = packed_lob_handle
        self.offset = offset
        self.data = data

    def write(self, cas_info: bytes) -> bytes:
        """Serialize the LOB write request."""
        writer = PacketWriter()
        writer._write_byte(CASFunctionCode.LOB_WRITE)
        writer.add_bytes(self.packed_lob_handle)
        writer.add_long(self.offset)
        writer.add_bytes(self.data)
        return writer.finalize(cas_info)

    def parse(self, data: bytes | bytearray) -> None:
        """Parse the LOB write response."""
        reader = PacketReader(data)
        reader._skip_bytes(DataSize.CAS_INFO)
        response_code = reader._parse_int()
        if response_code < 0:
            remaining = len(data) - 8
            _raise_error(reader, remaining)


class LOBReadPacket:
    """Read data from a LOB (FC=37)."""

    def __init__(self, packed_lob_handle: bytes, offset: int, length: int) -> None:
        self.packed_lob_handle = packed_lob_handle
        self.offset = offset
        self.length = length

        self.bytes_read: int = 0
        self.lob_data: bytes = b""

    def write(self, cas_info: bytes) -> bytes:
        """Serialize the LOB read request."""
        writer = PacketWriter()
        writer._write_byte(CASFunctionCode.LOB_READ)
        writer.add_bytes(self.packed_lob_handle)
        writer.add_long(self.offset)
        writer.add_int(self.length)
        return writer.finalize(cas_info)

    def parse(self, data: bytes | bytearray) -> None:
        """Parse the LOB read response."""
        reader = PacketReader(data)
        reader._skip_bytes(DataSize.CAS_INFO)
        response_code = reader._parse_int()
        if response_code < 0:
            remaining = len(data) - 8
            _raise_error(reader, remaining)
        self.bytes_read = response_code
        if self.bytes_read > 0:
            self.lob_data = reader._parse_bytes(self.bytes_read)


class GetLastInsertIdPacket:
    """Get the last insert ID (FC=40)."""

    def __init__(self) -> None:
        self.last_insert_id: str = ""

    def write(self, cas_info: bytes) -> bytes:
        """Serialize the get last insert ID request."""
        writer = PacketWriter()
        writer._write_byte(CASFunctionCode.GET_LAST_INSERT_ID)
        return writer.finalize(cas_info)

    def parse(self, data: bytes | bytearray) -> None:
        """Parse the get last insert ID response."""
        reader = PacketReader(data)
        reader._skip_bytes(DataSize.CAS_INFO)
        response_code = reader._parse_int()
        if response_code < 0:
            remaining = len(data) - 8
            _raise_error(reader, remaining)
        if response_code > 0:
            self.last_insert_id = reader._parse_null_terminated_string(response_code)


class GetDbParameterPacket:
    """Get a database parameter (FC=4)."""

    def __init__(self, parameter: int) -> None:
        self.parameter = parameter
        self.value: int = 0

    def write(self, cas_info: bytes) -> bytes:
        """Serialize the get db parameter request."""
        writer = PacketWriter()
        writer._write_byte(CASFunctionCode.GET_DB_PARAMETER)
        writer.add_int(self.parameter)
        return writer.finalize(cas_info)

    def parse(self, data: bytes | bytearray) -> None:
        """Parse the get db parameter response."""
        reader = PacketReader(data)
        reader._skip_bytes(DataSize.CAS_INFO)
        response_code = reader._parse_int()
        if response_code < 0:
            remaining = len(data) - 8
            _raise_error(reader, remaining)
        self.value = reader._parse_int()


class SetDbParameterPacket:
    """Set a database parameter (FC=5)."""

    def __init__(self, parameter: int, value: int) -> None:
        self.parameter = parameter
        self.value = value

    def write(self, cas_info: bytes) -> bytes:
        """Serialize the set db parameter request."""
        writer = PacketWriter()
        writer._write_byte(CASFunctionCode.SET_DB_PARAMETER)
        writer.add_int(self.parameter)
        writer.add_int(self.value)
        return writer.finalize(cas_info)

    def parse(self, data: bytes | bytearray) -> None:
        """Parse the set db parameter response."""
        reader = PacketReader(data)
        reader._skip_bytes(DataSize.CAS_INFO)
        response_code = reader._parse_int()
        if response_code < 0:
            remaining = len(data) - 8
            _raise_error(reader, remaining)
