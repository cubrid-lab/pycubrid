"""CAS protocol packet classes for pycubrid.

Implements serialization/deserialization for all CUBRID CAS broker protocol
packets. Each packet class provides write() for request serialization and
parse() for response deserialization.
"""

from __future__ import annotations

import struct
from dataclasses import dataclass
from typing import Any, Sequence

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
from .error_codes import CAS_ERROR_TO_EXCEPTION, _DEFAULT_SQLSTATE, get_sqlstate
from .exceptions import (
    DatabaseError,
    DataError,
    InterfaceError,
    IntegrityError,
    InternalError,
    OperationalError,
    ProgrammingError,
)
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


@dataclass(frozen=True, slots=True)
class _SchemaColumn:
    """Condensed schema metadata; the wire carries no SELECT constraint fields."""

    column_type: int
    scale: int
    precision: int
    name: str


@dataclass(slots=True)
class ResultInfo:
    """Result info for each executed statement."""

    stmt_type: int = 0
    result_count: int = 0
    oid: bytes = b""
    cache_time_sec: int = 0
    cache_time_usec: int = 0


@dataclass(frozen=True, slots=True)
class _PreparedScalar:
    """One validated type/value pair for a prepared FC3 request."""

    type_code: int
    payload: bytes

    def __post_init__(self) -> None:
        if isinstance(self.type_code, bool) or not isinstance(self.type_code, int):
            raise ProgrammingError("invalid prepared parameter type code")
        if not isinstance(self.payload, bytes):
            raise ProgrammingError("invalid prepared parameter payload")
        if self.type_code == CUBRIDDataType.NULL:
            if self.payload:
                raise ProgrammingError("SQL NULL must have an empty payload")
        elif self.type_code == CUBRIDDataType.INT:
            if len(self.payload) != 4:
                raise ProgrammingError("prepared INT must have four value bytes")
        elif self.type_code == CUBRIDDataType.CHAR:
            if not self.payload.endswith(b"\x00") or b"\x00" in self.payload[:-1]:
                raise ProgrammingError("prepared CHAR must have one terminal NUL")
            try:
                self.payload[:-1].decode("utf-8")
            except UnicodeDecodeError:
                raise DataError("prepared CHAR is not valid UTF-8") from None
        else:
            raise ProgrammingError("unsupported prepared parameter type code")


def _encode_prepared_scalar(value: Any) -> _PreparedScalar:
    """Encode the intentionally narrow #439 scalar set without SQL rendering."""
    if value is None:
        return _PreparedScalar(CUBRIDDataType.NULL, b"")
    if isinstance(value, bool):
        raise ProgrammingError("unsupported prepared parameter type")
    if isinstance(value, int):
        if not -(2**31) <= value < 2**31:
            raise DataError("prepared INT value is outside signed 32-bit range")
        return _PreparedScalar(CUBRIDDataType.INT, struct.pack(">i", value))
    if isinstance(value, str):
        if "\x00" in value:
            raise ProgrammingError("prepared string contains NUL")
        try:
            return _PreparedScalar(CUBRIDDataType.CHAR, value.encode("utf-8") + b"\x00")
        except UnicodeEncodeError:
            raise DataError("prepared string cannot be encoded as UTF-8") from None
    raise ProgrammingError("unsupported prepared parameter type")


# ---------------------------------------------------------------------------
# Helper Functions
# ---------------------------------------------------------------------------


# Common CUBRID reserved words that cause confusion
_CUBRID_RESERVED_WORDS = frozenset(
    {
        "absolute",
        "action",
        "add",
        "after",
        "all",
        "allocate",
        "alter",
        "and",
        "any",
        "are",
        "as",
        "asc",
        "assertion",
        "at",
        "attach",
        "attribute",
        "avg",
        "before",
        "between",
        "bit",
        "boolean",
        "both",
        "breadth",
        "by",
        "call",
        "cascade",
        "case",
        "cast",
        "catalog",
        "change",
        "char",
        "character",
        "check",
        "class",
        "clob",
        "close",
        "coalesce",
        "collate",
        "column",
        "commit",
        "connect",
        "connection",
        "constraint",
        "continue",
        "convert",
        "corresponding",
        "count",
        "create",
        "cross",
        "current",
        "current_date",
        "current_time",
        "current_timestamp",
        "current_user",
        "cursor",
        "cycle",
        "data",
        "database",
        "date",
        "datetime",
        "day",
        "deallocate",
        "dec",
        "decimal",
        "declare",
        "default",
        "deferrable",
        "deferred",
        "delete",
        "depth",
        "desc",
        "describe",
        "descriptor",
        "diagnostics",
        "difference",
        "disconnect",
        "distinct",
        "do",
        "domain",
        "double",
        "drop",
        "duplicate",
        "each",
        "else",
        "elseif",
        "end",
        "equals",
        "escape",
        "evaluate",
        "except",
        "exception",
        "exec",
        "execute",
        "exists",
        "external",
        "extract",
        "false",
        "fetch",
        "file",
        "first",
        "float",
        "for",
        "foreign",
        "found",
        "from",
        "full",
        "function",
        "general",
        "get",
        "global",
        "go",
        "goto",
        "grant",
        "group",
        "having",
        "hour",
        "identity",
        "if",
        "ignore",
        "immediate",
        "in",
        "index",
        "indicator",
        "inherit",
        "initially",
        "inner",
        "inout",
        "input",
        "insert",
        "int",
        "integer",
        "intersect",
        "intersection",
        "interval",
        "into",
        "is",
        "isolation",
        "join",
        "key",
        "language",
        "last",
        "leading",
        "leave",
        "left",
        "less",
        "level",
        "like",
        "limit",
        "list",
        "local",
        "loop",
        "lower",
        "match",
        "max",
        "method",
        "min",
        "minute",
        "module",
        "month",
        "multiset",
        "names",
        "national",
        "natural",
        "nchar",
        "next",
        "no",
        "none",
        "not",
        "null",
        "nullif",
        "numeric",
        "object",
        "octet_length",
        "of",
        "off",
        "on",
        "only",
        "open",
        "operation",
        "option",
        "or",
        "order",
        "out",
        "outer",
        "output",
        "overlaps",
        "pad",
        "parameter",
        "partial",
        "position",
        "precision",
        "preserve",
        "primary",
        "prior",
        "private",
        "privileges",
        "procedure",
        "protected",
        "read",
        "real",
        "recursive",
        "ref",
        "references",
        "referencing",
        "relative",
        "rename",
        "replace",
        "resignal",
        "restrict",
        "return",
        "returns",
        "revoke",
        "right",
        "role",
        "rollback",
        "rollup",
        "routine",
        "row",
        "rows",
        "savepoint",
        "schema",
        "scope",
        "scroll",
        "search",
        "second",
        "section",
        "select",
        "sequence",
        "session",
        "session_user",
        "set",
        "seteq",
        "signal",
        "size",
        "smallint",
        "some",
        "space",
        "specific",
        "sql",
        "sqlcode",
        "sqlerror",
        "sqlexception",
        "sqlstate",
        "sqlwarning",
        "statistics",
        "string",
        "structure",
        "subset",
        "subtype",
        "sum",
        "superclass",
        "supersede",
        "sys_connect_by_path",
        "system_user",
        "table",
        "temporary",
        "test",
        "then",
        "there",
        "time",
        "timestamp",
        "timezone_hour",
        "timezone_minute",
        "to",
        "trailing",
        "transaction",
        "translate",
        "translation",
        "trigger",
        "trim",
        "true",
        "truncate",
        "under",
        "union",
        "unique",
        "unknown",
        "update",
        "upper",
        "usage",
        "use",
        "user",
        "using",
        "value",
        "values",
        "varchar",
        "variable",
        "varying",
        "view",
        "virtual",
        "visible",
        "wait",
        "when",
        "whenever",
        "where",
        "while",
        "with",
        "without",
        "work",
        "write",
        "year",
        "zone",
    }
)


def _add_error_hints(error_message: str) -> str:
    """Append helpful hints for common CUBRID error patterns."""
    msg_lower = error_message.lower()

    # Hint for CARDINALITY() server bug
    if "cardinality" in msg_lower and (
        "undefined" in msg_lower or "not found" in msg_lower or "does not exist" in msg_lower
    ):
        error_message += (
            " [Hint: CARDINALITY() has a known bug in CUBRID 11.x and may not work. "
            "Use a subquery with COUNT(*) on TABLE(column) instead. "
            "See: https://github.com/cubrid-lab/.github/issues/3]"
        )
        return error_message

    # Hint for reserved word syntax errors
    if "syntax" in msg_lower and "unexpected" in msg_lower:
        import re

        # Extract the token after "unexpected" — that's the problematic identifier
        match = re.search(r"unexpected\s+'(\w+)'", error_message)
        if match:
            token = match.group(1)
            if token.lower() in _CUBRID_RESERVED_WORDS:
                error_message += (
                    f" [Hint: '{token}' is a CUBRID reserved word. "
                    f"Use double-quotes around the identifier or rename it. "
                    f"See: https://github.com/cubrid-lab/.github/issues/5]"
                )

    return error_message


# Exception class lookup for CAS-code-based dispatch in _raise_error().
_EXCEPTION_CLASSES = {
    "DatabaseError": DatabaseError,
    "DataError": DataError,
    "InterfaceError": InterfaceError,
    "IntegrityError": IntegrityError,
    "InternalError": InternalError,
    "OperationalError": OperationalError,
    "ProgrammingError": ProgrammingError,
}


def _classify_by_text(error_message: str) -> str:
    """Classify an error by message text — fallback for CAS code -1 (ER_DBMS).

    CAS wraps many server-engine errors with the generic code -1 and only the
    message for differentiation. This heuristic preserves backward
    compatibility for those passthrough errors.
    """
    msg_lower = error_message.lower()
    if any(
        kw in msg_lower for kw in ("unique", "duplicate", "foreign key", "constraint violation")
    ):
        return "IntegrityError"
    if any(kw in msg_lower for kw in ("syntax", "unknown class", "does not exist", "not found")):
        return "ProgrammingError"
    return "DatabaseError"


def _raise_error(reader: PacketReader, response_length: int) -> None:
    """Parse an error response and raise the appropriate DB-API exception.

    Dispatches primarily by CAS error code (deterministic, stable across
    CUBRID versions). Falls back to text heuristics ONLY for code -1
    (ER_DBMS), where CAS wraps server-engine errors with a generic code and
    only the message is informative.
    """
    error_code, error_message = reader.read_error(response_length)
    sqlstate = get_sqlstate(error_code)
    error_message = _add_error_hints(error_message)

    # Primary dispatch: CAS error code → exception class
    exc_name = CAS_ERROR_TO_EXCEPTION.get(error_code)

    # Fallback: ER_DBMS (-1) uses text classification (CAS passthrough)
    if exc_name is None and error_code == -1:
        exc_name = _classify_by_text(error_message)

    # Unmapped code → generic DatabaseError
    if exc_name is None:
        exc_name = "DatabaseError"

    exc_class = _EXCEPTION_CLASSES[exc_name]
    raise exc_class(
        msg=error_message,
        code=error_code,
        errno=error_code,
        sqlstate=sqlstate or _DEFAULT_SQLSTATE.get(exc_name, "HY000"),
    )


def _parse_column_type(reader: PacketReader) -> int:
    """Read either type layout while preserving its collection-kind flags."""
    legacy_type = reader._parse_byte()
    column_type = reader._parse_byte() if legacy_type & 0x80 else legacy_type
    collection_kind = legacy_type & 0x60
    if collection_kind:
        column_type = (CUBRIDDataType.SET, CUBRIDDataType.MULTISET, CUBRIDDataType.SEQUENCE)[
            (collection_kind >> 5) - 1
        ]
    return column_type


def _parse_schema_column_metadata(reader: PacketReader, column_count: int) -> list[_SchemaColumn]:
    """Decode compact FC9 columns without inventing ordinary SELECT fields."""
    if column_count < 0:
        raise ValueError("negative schema column count")
    columns: list[_SchemaColumn] = []
    for _ in range(column_count):
        column_type = _parse_column_type(reader)
        scale = reader._parse_short()
        precision = reader._parse_int()
        name_len = reader._parse_int()
        if name_len < 0 or name_len > reader.bytes_remaining():
            raise ValueError("invalid schema column name length")
        name = reader._parse_null_terminated_string(name_len)
        columns.append(_SchemaColumn(column_type, scale, precision, name))
    return columns


def _parse_column_metadata(
    reader: PacketReader, column_count: int, *, strict_lengths: bool = False
) -> list[ColumnMetaData]:
    """Parse column metadata entries from the reader."""
    columns: list[ColumnMetaData] = []
    for _ in range(column_count):
        column_type = _parse_column_type(reader)
        scale = reader._parse_short()
        precision = reader._parse_int()

        name_len = reader._parse_int()
        if strict_lengths and (name_len < 0 or name_len > reader.bytes_remaining()):
            raise ValueError("invalid prepared column name length")
        name = reader._parse_null_terminated_string(name_len)
        real_name_len = reader._parse_int()
        if strict_lengths and (real_name_len < 0 or real_name_len > reader.bytes_remaining()):
            raise ValueError("invalid prepared column real-name length")
        real_name = reader._parse_null_terminated_string(real_name_len)
        table_name_len = reader._parse_int()
        if strict_lengths and (table_name_len < 0 or table_name_len > reader.bytes_remaining()):
            raise ValueError("invalid prepared column table-name length")
        table_name = reader._parse_null_terminated_string(table_name_len)

        # CAS sends is_non_null: zero means the column accepts NULL.
        is_nullable = reader._parse_byte() == 0
        default_len = reader._parse_int()
        if strict_lengths and (default_len < 0 or default_len > reader.bytes_remaining()):
            raise ValueError("invalid prepared column default length")
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


def _parse_prepare_info(reader: PacketReader) -> tuple[int, int, list[ColumnMetaData]]:
    """Parse the FC2 tail also reused by FC3 refreshed-column responses."""
    if reader.bytes_remaining() < 14:
        raise ValueError("truncated prepared column metadata")
    _ = reader._parse_int()  # result cache lifetime
    statement_type = reader._parse_byte()
    bind_count = reader._parse_int()
    _ = reader._parse_byte()  # is_updatable
    column_count = reader._parse_int()
    if bind_count < 0 or column_count < 0:
        raise ValueError("negative prepared bind or column count")
    if column_count > reader.bytes_remaining() // 31:
        raise ValueError("truncated prepared column metadata")
    return (
        statement_type,
        bind_count,
        _parse_column_metadata(reader, column_count, strict_lengths=True),
    )


# ---------------------------------------------------------------------------
# Type dispatch: method-name table
# ---------------------------------------------------------------------------

_TYPE_METHOD_NAMES: dict[int, str] = {
    CUBRIDDataType.CHAR: "_parse_null_terminated_string",
    CUBRIDDataType.STRING: "_parse_null_terminated_string",
    CUBRIDDataType.NCHAR: "_parse_null_terminated_string",
    CUBRIDDataType.VARNCHAR: "_parse_null_terminated_string",
    CUBRIDDataType.ENUM: "_parse_null_terminated_string",
    CUBRIDDataType.JSON: "_parse_json",
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
    CUBRIDDataType.TIMESTAMPTZ: "_parse_timestamptz",
    CUBRIDDataType.TIMESTAMPLTZ: "_parse_timestamptz",
    CUBRIDDataType.DATETIMETZ: "_parse_datetimetz",
    CUBRIDDataType.DATETIMELTZ: "_parse_datetimetz",
    CUBRIDDataType.OBJECT: "_parse_object",
    CUBRIDDataType.BIT: "_parse_bytes",
    CUBRIDDataType.VARBIT: "_parse_bytes",
    CUBRIDDataType.SET: "_parse_collection",
    CUBRIDDataType.MULTISET: "_parse_collection",
    CUBRIDDataType.SEQUENCE: "_parse_collection",
    CUBRIDDataType.BLOB: "read_blob",
    CUBRIDDataType.CLOB: "read_clob",
}


def _resolve_reader(reader: PacketReader, col_type: int) -> Any:
    method_name = _TYPE_METHOD_NAMES.get(col_type)
    if method_name is not None:
        return getattr(reader, method_name)
    return reader._parse_bytes


def _convert_collection_value(column_type: int, value: Any) -> Any:
    if not isinstance(value, list):
        return value
    if column_type == CUBRIDDataType.SET:
        try:
            return frozenset(value)
        except TypeError:
            # Unhashable elements (e.g. dicts from JSON) — return as tuple.
            return tuple(value)
    # MULTISET and SEQUENCE are returned as-is (already list).
    # No other collection types need post-processing.
    return value


def _read_value(reader: PacketReader, column_type: int, size: int) -> Any:
    if column_type == CUBRIDDataType.NULL:
        return None
    return _convert_collection_value(column_type, _resolve_reader(reader, column_type)(size))


def _parse_row_data(
    reader: PacketReader,
    tuple_count: int,
    columns: Sequence[ColumnMetaData | _SchemaColumn],
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
                    row[i] = _convert_collection_value(col_types[i], col_readers[i](size))
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
                    row[i] = _convert_collection_value(ct, _getattr(reader, method_name)(size))
                else:
                    row[i] = _parse_bytes(size)
        _rows_append(tuple(row))
    return rows


def _parse_result_infos(
    reader: PacketReader, result_count: int, *, prepared: bool = False
) -> list[ResultInfo]:
    """Parse result info entries."""
    if result_count < 0:
        raise ValueError("negative execute result count")
    infos: list[ResultInfo] = []
    for _ in range(result_count):
        stmt_type = reader._parse_byte()
        count = reader._parse_int()
        if prepared and count < 0:
            if reader.bytes_remaining() < 8:
                raise ValueError("truncated prepared execution error")
            error_code = reader._parse_int()
            message_size = reader._parse_int()
            if message_size < 0 or message_size > reader.bytes_remaining():
                raise ValueError("invalid prepared execution error length")
            _ = reader._parse_bytes(message_size)  # Broker text may contain bound data.
            exc_name = CAS_ERROR_TO_EXCEPTION.get(error_code, "DatabaseError")
            exc_class = _EXCEPTION_CLASSES[exc_name]
            raise exc_class(
                "prepared statement execution failed",
                code=error_code,
                errno=error_code,
                sqlstate=get_sqlstate(error_code) or _DEFAULT_SQLSTATE[exc_name],
            )
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

    def __init__(self, use_ssl: bool = False) -> None:
        self.new_connection_port: int = 0
        self._use_ssl = use_ssl

    def write(self) -> bytes:
        """Serialize the handshake packet (10 bytes, no protocol header)."""
        buf = bytearray()
        magic = CASProtocol.MAGIC_STRING_SSL if self._use_ssl else CASProtocol.MAGIC_STRING
        buf.extend(magic.encode("ascii"))
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
        """Serialize the open database packet.

        Wire format: database(32) + user(32) + password(32) + extended_info(512)
        + reserved(20) = 628 bytes (no header).
        """
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
        decode_collections: bool = False,
        json_deserializer: Any = None,
    ) -> None:
        self.sql = sql
        self.auto_commit = auto_commit
        self.protocol_version = protocol_version
        self.decode_collections = decode_collections
        self.json_deserializer = json_deserializer

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
        reader = PacketReader(
            data,
            decode_collections=self.decode_collections,
            json_deserializer=self.json_deserializer,
        )
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

    def __init__(
        self,
        sql: str,
        auto_commit: bool = False,
        *,
        prepare_flag: int = CCIPrepareOption.NORMAL,
    ) -> None:
        self.sql = sql
        self.auto_commit = auto_commit
        self.prepare_flag = prepare_flag

        self.response_code: int = 0
        self.query_handle: int = 0
        self.statement_type: int = 0
        self.bind_count: int = 0
        self.column_count: int = 0
        self.columns: list[ColumnMetaData] = []

    def write(self, cas_info: bytes) -> bytes:
        """Serialize the prepare request."""
        if not isinstance(self.sql, str) or "\x00" in self.sql:
            raise ProgrammingError("prepared SQL must be a string without NUL")
        if not isinstance(self.auto_commit, bool):
            raise ProgrammingError("prepared autocommit must be a boolean")
        if (
            isinstance(self.prepare_flag, bool)
            or not isinstance(self.prepare_flag, int)
            or self.prepare_flag not in (CCIPrepareOption.NORMAL, CCIPrepareOption.HOLDABLE)
        ):
            raise ProgrammingError("unsupported prepared statement option")
        writer = PacketWriter()
        writer._write_byte(CASFunctionCode.PREPARE)
        try:
            writer._write_null_terminated_string(self.sql)
        except UnicodeEncodeError:
            raise DataError("prepared SQL cannot be encoded as UTF-8") from None
        writer.add_byte(self.prepare_flag)
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
        self.statement_type, self.bind_count, self.columns = _parse_prepare_info(reader)
        self.column_count = len(self.columns)


class ExecutePacket:
    """Execute a prepared statement (FC=3)."""

    def __init__(
        self,
        query_handle: int,
        statement_type: int,
        auto_commit: bool = False,
        protocol_version: int = CASProtocol.VERSION,
        decode_collections: bool = False,
        json_deserializer: Any = None,
        *,
        bindings: Sequence[_PreparedScalar] = (),
        bind_count: int | None = None,
        forward_only: bool | None = None,
    ) -> None:
        self.query_handle = query_handle
        self.statement_type = statement_type
        self.auto_commit = auto_commit
        self.protocol_version = protocol_version
        self.decode_collections = decode_collections
        self.json_deserializer = json_deserializer
        self.bindings = tuple(bindings)
        self.bind_count = bind_count
        self.forward_only = auto_commit if forward_only is None else forward_only

        self.total_tuple_count: int = 0
        self.result_count: int = 0
        self.result_infos: list[ResultInfo] = []
        self.tuple_count: int = 0
        self.rows: list[tuple[Any, ...]] = []
        self.columns: list[ColumnMetaData] = []

    def write(self, cas_info: bytes) -> bytes:
        """Serialize the execute request."""
        if not isinstance(self.auto_commit, bool):
            raise ProgrammingError("prepared autocommit must be a boolean")
        if self.bind_count is not None and (
            isinstance(self.bind_count, bool)
            or not isinstance(self.bind_count, int)
            or self.bind_count < 0
        ):
            raise ProgrammingError("prepared bind count must be a nonnegative integer")
        if self.bind_count is not None and len(self.bindings) != self.bind_count:
            raise ProgrammingError("prepared parameter count does not match server bind count")
        if not isinstance(self.forward_only, bool):
            raise ProgrammingError("forward_only must be a boolean")
        fetch_flag = 1 if self.statement_type == CUBRIDStatementType.SELECT else 0
        writer = PacketWriter()
        writer._write_byte(CASFunctionCode.EXECUTE)
        writer.add_int(self.query_handle)
        writer.add_byte(CCIExecutionOption.NORMAL)
        writer.add_int(0)  # max_col_size
        writer.add_int(0)  # max_row_size
        writer.add_null()  # NULL
        writer._write_int(1)  # SELECT fetch-flag argument length
        writer._write_byte(fetch_flag)
        writer.add_byte(1 if self.auto_commit else 0)
        writer.add_byte(1 if self.forward_only else 0)
        writer.add_cache_time()
        writer.add_int(0)  # query timeout
        for binding in self.bindings:
            if not isinstance(binding, _PreparedScalar):
                raise ProgrammingError("invalid prepared parameter encoding")
            writer.add_byte(binding.type_code)
            writer.add_bytes(binding.payload)
        return writer.finalize(cas_info)

    def parse(self, data: bytes | bytearray, columns: list[ColumnMetaData] | None = None) -> None:
        """Parse the execute response."""
        if columns is not None:
            self.columns = columns
        reader = PacketReader(
            data,
            decode_collections=self.decode_collections,
            json_deserializer=self.json_deserializer,
        )
        reader._skip_bytes(DataSize.CAS_INFO)
        response_code = reader._parse_int()
        if response_code < 0:
            remaining = len(data) - 8
            _raise_error(reader, remaining)

        self.total_tuple_count = response_code
        _ = reader._parse_byte()  # cache_reusable
        self.result_count = reader._parse_int()
        self.result_infos = _parse_result_infos(reader, self.result_count, prepared=True)

        if self.protocol_version > 1:
            includes_column_info = reader._parse_byte()
            if includes_column_info not in (0, 1):
                raise ValueError("invalid refreshed-column-info flag")
            if includes_column_info:
                self.statement_type, self.bind_count, self.columns = _parse_prepare_info(reader)
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
        columns: Sequence[ColumnMetaData | _SchemaColumn] | None = None,
        statement_type: int = CUBRIDStatementType.SELECT,
        decode_collections: bool = False,
        json_deserializer: Any = None,
    ) -> None:
        self.query_handle = query_handle
        self.current_tuple_count = current_tuple_count
        self.fetch_size = fetch_size
        self._columns = columns
        self._statement_type = statement_type
        self.decode_collections = decode_collections
        self.json_deserializer = json_deserializer

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
        columns: Sequence[ColumnMetaData | _SchemaColumn] | None = None,
        statement_type: int | None = None,
    ) -> None:
        """Parse the fetch response."""
        reader = PacketReader(
            data,
            decode_collections=self.decode_collections,
            json_deserializer=self.json_deserializer,
        )
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


def _write_schema_info_request(
    cas_info: bytes,
    schema_type: int,
    arg1: str | None,
    arg2: str | None,
    flags: int,
    *,
    shard_id: int = 0,
    protocol_version: int = CASProtocol.VERSION,
) -> bytes:
    """Serialize the independently nullable FC9 arguments and shard identifier."""
    writer = PacketWriter()
    writer._write_byte(CASFunctionCode.SCHEMA_INFO)
    writer.add_int(schema_type)
    for argument in (arg1, arg2):
        if argument is None:
            writer.add_null()
        else:
            writer._write_null_terminated_string(argument)
    writer.add_byte(flags)
    if protocol_version >= 5:
        writer.add_int(shard_id)
    return writer.finalize(cas_info)


class GetSchemaPacket:
    """Get schema information (FC=9)."""

    def __init__(
        self,
        schema_type: int,
        table_name: str = "",
        pattern_match_flag: int = 1,
        *,
        arg2: str | None = None,
        protocol_version: int = CASProtocol.VERSION,
    ) -> None:
        self.schema_type = schema_type
        self.table_name = table_name
        self.pattern_match_flag = pattern_match_flag
        self.arg2 = arg2
        self.protocol_version = protocol_version

        self.query_handle: int = 0
        self.tuple_count: int = 0
        self.columns: list[_SchemaColumn] = []
        self._owner: object | None = None

    def write(self, cas_info: bytes) -> bytes:
        """Serialize the get schema request."""
        return _write_schema_info_request(
            cas_info,
            self.schema_type,
            self.table_name,
            self.arg2,
            self.pattern_match_flag,
            protocol_version=self.protocol_version,
        )

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
        if self.tuple_count < 0:
            raise ValueError("negative schema tuple count")
        self.columns = _parse_schema_column_metadata(reader, reader._parse_int())
        if self.tuple_count and not self.columns:
            raise ValueError("missing schema columns for nonempty result")


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
        self.bytes_written: int = 0

    def write(self, cas_info: bytes) -> bytes:
        """Serialize the LOB write request."""
        writer = PacketWriter()
        writer._write_byte(CASFunctionCode.LOB_WRITE)
        writer.add_bytes(self.packed_lob_handle)
        writer.add_long(self.offset)
        writer.add_bytes(self.data)
        return writer.finalize(cas_info)

    def parse(self, data: bytes | bytearray) -> None:
        """Parse the LOB write response.

        On success, ``response_code`` doubles as ``bytes_written`` per CAS protocol.
        """
        reader = PacketReader(data)
        reader._skip_bytes(DataSize.CAS_INFO)
        response_code = reader._parse_int()
        if response_code < 0:
            remaining = len(data) - 8
            _raise_error(reader, remaining)
        self.bytes_written = response_code


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
        """Parse the get last insert ID response.

        The CAS protocol encodes dbval with a variable-length type header:
        - If the first type byte has bit 7 set (``& 0x80``), the header is
          2 bytes (e.g. ``0x83 0x07`` for CCI_U_TYPE_NUMERIC).
        - Otherwise the header is 1 byte (legacy single-byte type).
        """
        reader = PacketReader(data)
        reader._skip_bytes(DataSize.CAS_INFO)
        response_code = reader._parse_int()
        if response_code < 0:
            remaining = len(data) - 8
            _raise_error(reader, remaining)
        value_size = reader._parse_int()
        if value_size > 0:
            type_byte = reader._parse_byte()
            type_header_size = 2 if (type_byte & 0x80) else 1
            if type_header_size == 2:
                reader._skip_bytes(1)  # consume second type byte
            remaining = value_size - type_header_size
            if remaining > 0:
                self.last_insert_id = reader._parse_null_terminated_string(remaining)


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


class CheckCasPacket:
    """Ping/health check the CAS broker connection (FC=32).

    Uses the lightweight ``CHECK_CAS`` function code which verifies
    CAS-to-DB server connectivity without executing SQL.  The official
    JDBC driver uses the same mechanism in ``UConnection.check_cas()``.
    """

    def __init__(self) -> None:
        self.response_code: int = 0

    def write(self, cas_info: bytes) -> bytes:
        """Serialize the check CAS request (function code only, no args)."""
        writer = PacketWriter()
        writer._write_byte(CASFunctionCode.CHECK_CAS)
        return writer.finalize(cas_info)

    def parse(self, data: bytes | bytearray) -> None:
        """Parse the check CAS response.

        ``response_code >= 0`` means the connection is alive.
        ``response_code < 0`` means the CAS-to-DB link is broken.
        """
        reader = PacketReader(data)
        reader._skip_bytes(DataSize.CAS_INFO)
        # CHECK_CAS success may return an empty body (no int) or a zero int.
        # Accept both: empty body → 0 (alive), otherwise parse the int.
        if reader.bytes_remaining() >= DataSize.INT:
            self.response_code = reader._parse_int()
        else:
            self.response_code = 0


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
