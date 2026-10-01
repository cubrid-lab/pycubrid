"""CAS protocol packet classes for pycubrid.

Implements serialization/deserialization for all CUBRID CAS broker protocol
packets. Each packet class provides write() for request serialization and
parse() for response deserialization.
"""

from __future__ import annotations

import struct
from dataclasses import dataclass
from typing import Any, Callable, Sequence

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
from .packet import (
    _STRUCT_INT,
    PacketReader,
    PacketWriter,
    _codec_label,
    _decode_text,
    _encode_text,
)


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
    encoding: str = "utf-8"

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
                self.payload[:-1].decode(self.encoding)
            except UnicodeDecodeError:
                raise DataError(
                    f"prepared CHAR is not valid {_codec_label(self.encoding)}"
                ) from None
        else:
            raise ProgrammingError("unsupported prepared parameter type code")


def _encode_prepared_scalar(value: Any, encoding: str = "utf-8") -> _PreparedScalar:
    """Encode the intentionally narrow #439 scalar set without SQL rendering.

    Strings use ``encoding``, the connection charset (#86).
    """
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
        encoded, _position = _encode_text(value, encoding)
        if encoded is None:
            raise DataError(f"prepared string cannot be encoded as {_codec_label(encoding)}")
        return _PreparedScalar(CUBRIDDataType.CHAR, encoded + b"\x00", encoding)
    raise ProgrammingError("unsupported prepared parameter type")


_COLLECTION_TYPE_CODES = frozenset(
    {CUBRIDDataType.SET, CUBRIDDataType.MULTISET, CUBRIDDataType.SEQUENCE}
)
_COLLECTION_ELEMENT_TYPES = frozenset({CUBRIDDataType.INT, CUBRIDDataType.STRING})


@dataclass(frozen=True, slots=True)
class _PreparedCollection:
    """One validated typed collection for a prepared FC3 request (#482).

    The value argument is ``[element type byte]`` followed by one
    ``int32 length + payload`` per element, with no element count. A ``None``
    element is a NULL element (length 0). Whole SQL NULL is a
    ``_PreparedScalar``, never this type. The broker keeps a partial
    collection when an element length overruns the argument, so every
    element is checked here and the framing is always exact.
    """

    type_code: int
    element_type: int
    elements: tuple[bytes | None, ...]
    encoding: str = "utf-8"

    def __post_init__(self) -> None:
        if (
            isinstance(self.type_code, bool)
            or not isinstance(self.type_code, int)
            or self.type_code not in _COLLECTION_TYPE_CODES
        ):
            raise ProgrammingError("unsupported prepared collection type code")
        if (
            isinstance(self.element_type, bool)
            or not isinstance(self.element_type, int)
            or self.element_type not in _COLLECTION_ELEMENT_TYPES
        ):
            raise ProgrammingError("unsupported prepared collection element type")
        if type(self.elements) is not tuple:
            raise ProgrammingError("prepared collection elements must be a tuple")
        for element in self.elements:
            if element is None:
                continue
            if type(element) is not bytes:
                raise ProgrammingError("invalid prepared collection element payload")
            if self.element_type == CUBRIDDataType.INT:
                if len(element) != 4:
                    raise ProgrammingError("prepared INT element must have four value bytes")
            else:
                if not element.endswith(b"\x00") or b"\x00" in element[:-1]:
                    raise ProgrammingError("prepared string element must have one terminal NUL")
                try:
                    # Server-compatible decoding: rejects EUC-KR makeup
                    # sequences that _encode_text() never produces.
                    _decode_text(element[:-1], self.encoding)
                except UnicodeDecodeError:
                    raise DataError(
                        f"prepared string element is not valid {_codec_label(self.encoding)}"
                    ) from None

    @property
    def payload(self) -> bytes:
        """Return the exact FC3 value argument (without its own length prefix)."""
        parts = [bytes((self.element_type,))]
        for element in self.elements:
            if element is None:
                parts.append(b"\x00\x00\x00\x00")
            else:
                parts.append(struct.pack(">i", len(element)))
                parts.append(element)
        return b"".join(parts)


def _collection_int(value: int | str) -> int:
    """Return an INT element from an int or a canonical ASCII decimal string."""
    if isinstance(value, str):
        digits = value[1:] if value.startswith("-") else value
        if (
            not digits
            or not digits.isascii()
            or not digits.isdigit()
            or (digits[0] == "0" and value != "0")
        ):
            raise ProgrammingError("prepared INT element string is not a canonical integer")
        # Longer strings are out of range; this also stays below int()'s
        # digit limit.
        number = int(value) if len(digits) <= 10 else 2**31
    else:
        number = value
    if not -(2**31) <= number < 2**31:
        raise DataError("prepared INT element is outside signed 32-bit range")
    return number


def _encode_prepared_collection(
    values: Any, type_code: int, element_type: int, encoding: str = "utf-8"
) -> _PreparedCollection:
    """Encode a flat tuple as one typed SET/MULTISET/SEQUENCE bind (#482).

    INT elements accept ``int`` (not ``bool``), or canonical decimal strings
    as the official call shape ``('1', '2')`` sends; one collection must not
    mix the two. STRING elements accept ``str`` only and use ``encoding``, the
    connection charset. ``None`` is a NULL element and an empty tuple is an
    empty collection. Everything else, including nested containers, is
    rejected before any bytes are built; messages never echo the value.
    """
    if type(values) is not tuple:
        raise ProgrammingError("prepared collection must be a tuple")
    # Validate the codes before the elements so a bad code is not reported
    # as an element error.
    _PreparedCollection(type_code, element_type, ())
    encoded: list[bytes | None] = []
    int_kind: type | None = None
    for value in values:
        if value is None:
            encoded.append(None)
            continue
        if isinstance(value, (tuple, list, set, frozenset, dict)):
            raise ProgrammingError("nested prepared collections are not supported")
        if element_type == CUBRIDDataType.INT:
            if isinstance(value, bool) or not isinstance(value, (int, str)):
                raise ProgrammingError("unsupported prepared INT element type")
            kind = str if isinstance(value, str) else int
            if int_kind is None:
                int_kind = kind
            elif int_kind is not kind:
                raise ProgrammingError("prepared INT collection mixes int and string elements")
            encoded.append(struct.pack(">i", _collection_int(value)))
            continue
        if not isinstance(value, str):
            raise ProgrammingError("unsupported prepared string element type")
        if "\x00" in value:
            raise ProgrammingError("prepared string element contains NUL")
        text, _position = _encode_text(value, encoding)
        if text is None:
            raise DataError(
                f"prepared string element cannot be encoded as {_codec_label(encoding)}"
            )
        encoded.append(text + b"\x00")
    return _PreparedCollection(type_code, element_type, tuple(encoded), encoding)


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
    error = exc_class(
        msg=error_message,
        code=error_code,
        errno=error_code,
        sqlstate=sqlstate or _DEFAULT_SQLSTATE.get(exc_name, "HY000"),
    )
    setattr(error, "_cas_server_error", True)
    raise error


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
        name = reader._parse_metadata_text(name_len)
        columns.append(_SchemaColumn(column_type, scale, precision, name))
    return columns


def _parse_column_metadata(reader: PacketReader, column_count: int) -> list[ColumnMetaData]:
    """Parse FC2/FC3/FC41 column metadata entries from the reader.

    The text decoder reads a non-positive length as empty, so each metadata
    length is bounded here first: a negative one is framing damage (#555).

    Text that the connection codec cannot decode raises ``DataError``, which is
    only for a complete reply (#492, #512). Before re-raising it, walk the
    whole metadata again by its declared lengths without decoding, so framing
    damage in a later column still fails as malformed (#581).
    """
    columns, error = _parse_column_metadata_deferred(reader, column_count)
    if error is not None:
        raise error
    return columns


def _parse_column_metadata_deferred(
    reader: PacketReader, column_count: int
) -> tuple[list[ColumnMetaData], DataError | None]:
    """Retain column types and defer text errors until the packet is framed.

    Malformed metadata wins immediately: the bounds walk may raise ValueError,
    IndexError or struct.error. Only a complete metadata block returns a saved
    DataError for the packet parser to report after checking its remaining tail.
    """
    if column_count < 0:
        raise ValueError("negative prepared column count")
    start = reader.mark()
    try:
        return _read_column_metadata(reader, column_count, decode=True), None
    except DataError as error:
        reader.seek(start)
        return _read_column_metadata(reader, column_count, decode=False), error


def _read_column_metadata(
    reader: PacketReader, column_count: int, *, decode: bool
) -> list[ColumnMetaData]:
    """Read ``column_count`` metadata entries; ``decode=False`` only checks bounds."""

    def text(length: int) -> str:
        if decode:
            return reader._parse_metadata_text(length)
        reader._skip_bytes(length)
        return ""

    columns: list[ColumnMetaData] = []
    for _ in range(column_count):
        column_type = _parse_column_type(reader)
        scale = reader._parse_short()
        precision = reader._parse_int()

        name_len = reader._parse_int()
        if name_len < 0 or name_len > reader.bytes_remaining():
            raise ValueError("invalid prepared column name length")
        name = text(name_len)
        real_name_len = reader._parse_int()
        if real_name_len < 0 or real_name_len > reader.bytes_remaining():
            raise ValueError("invalid prepared column real-name length")
        real_name = text(real_name_len)
        table_name_len = reader._parse_int()
        if table_name_len < 0 or table_name_len > reader.bytes_remaining():
            raise ValueError("invalid prepared column table-name length")
        table_name = text(table_name_len)

        # CAS sends is_non_null: zero means the column accepts NULL.
        is_nullable = reader._parse_byte() == 0
        default_len = reader._parse_int()
        if default_len < 0 or default_len > reader.bytes_remaining():
            raise ValueError("invalid prepared column default length")
        default_value = text(default_len)
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


def _parse_prepare_info(
    reader: PacketReader,
) -> tuple[int, int, list[ColumnMetaData], DataError | None]:
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
    columns, error = _parse_column_metadata_deferred(reader, column_count)
    return statement_type, bind_count, columns, error


# ---------------------------------------------------------------------------
# Type dispatch: method-name table
# ---------------------------------------------------------------------------

_TYPE_METHOD_NAMES: dict[int, str] = {
    CUBRIDDataType.CHAR: "_parse_text_value",
    CUBRIDDataType.STRING: "_parse_text_value",
    CUBRIDDataType.NCHAR: "_parse_text_value",
    CUBRIDDataType.VARNCHAR: "_parse_text_value",
    CUBRIDDataType.ENUM: "_parse_text_value",
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


# Wire width of the cell values whose readers do not consume the cell size
# themselves (#523). A negative entry is the minimum width of a TZ value, whose
# zone string takes the rest of the cell. Every other reader (text, NUMERIC,
# JSON, bytes, collections, LOBs) consumes exactly the size it is given.
_FIXED_CELL_WIDTHS: dict[int, int] = {
    CUBRIDDataType.SHORT: DataSize.SHORT,
    CUBRIDDataType.INT: DataSize.INT,
    CUBRIDDataType.BIGINT: DataSize.LONG,
    CUBRIDDataType.FLOAT: DataSize.FLOAT,
    CUBRIDDataType.DOUBLE: DataSize.DOUBLE,
    CUBRIDDataType.MONETARY: DataSize.DOUBLE,
    CUBRIDDataType.DATE: 6,
    CUBRIDDataType.TIME: 6,
    CUBRIDDataType.TIMESTAMP: 12,
    CUBRIDDataType.DATETIME: 14,
    CUBRIDDataType.OBJECT: DataSize.OBJECT,
    CUBRIDDataType.TIMESTAMPTZ: -12,
    CUBRIDDataType.TIMESTAMPLTZ: -12,
    CUBRIDDataType.DATETIMETZ: -14,
    CUBRIDDataType.DATETIMELTZ: -14,
}


def _cell_size_mismatch(column_type: int, size: int) -> ValueError:
    """A row cell whose size does not fit its fixed-width value: a malformed reply."""
    return ValueError(
        f"row cell size {size} does not fit a {CUBRIDDataType(column_type).name} value"
    )


def _check_cell_size(column_type: int, size: int) -> None:
    width = _FIXED_CELL_WIDTHS.get(column_type)
    if width is not None and (size != width if width > 0 else size < -width):
        raise _cell_size_mismatch(column_type, size)


def _parse_cell_type(reader: PacketReader, size: int) -> tuple[int, int]:
    """Read the type header of a CALL / NULL-typed row cell; return (type, value size).

    Protocol 7+ brokers write the header as column metadata does (#542):
    ``0x80 | collection bits | charset``, then the type byte. Older brokers write
    one type byte. The header counts in the cell size, so a header longer than
    the size is a malformed reply.
    """
    start = reader.mark()
    column_type = _parse_column_type(reader)
    header_size = reader.mark() - start
    if header_size > size:
        reader.seek(start)
        raise ValueError(f"row cell size {size} is shorter than its {header_size}-byte type")
    return column_type, size - header_size


def _check_row_data_bounds(
    reader: PacketReader,
    rows_start: int,
    tuple_count: int,
    col_types: Sequence[int],
    typed: Sequence[bool],
) -> None:
    """Walk ``tuple_count`` rows by declared sizes; raise if the reply is malformed.

    A size past the end of the reply raises ``ValueError`` from ``_skip_bytes``
    (#383), and a fixed-width value whose size disagrees with its width raises
    too (#523). ``typed`` marks CALL/NULL-typed columns, whose cells start with
    their own one- or two-byte type header, counted in the size (#542).
    """
    reader.seek(rows_start)
    for _ in range(tuple_count):
        reader._parse_int()
        reader._skip_bytes(DataSize.OID)
        for column_type, is_typed in zip(col_types, typed):
            size = reader._parse_int()
            if size <= 0:
                continue
            if is_typed:
                column_type, size = _parse_cell_type(reader, size)
                if size == 0:
                    continue
            _check_cell_size(column_type, size)
            reader._skip_bytes(size)


def _deferred_value_reader(
    reader: PacketReader, parse_value: Callable[[int], Any]
) -> Callable[[int], Any]:
    """Keep validating later cells when metadata already holds a DataError."""

    def read(size: int) -> Any:
        start = reader.mark()
        try:
            return parse_value(size)
        except DataError:
            # Value readers validate their own declared payload before a
            # DataError. Resume at the next cell, not a shallow row re-walk.
            reader.seek(start + size)
            return None

    return read


def _parse_row_data(
    reader: PacketReader,
    tuple_count: int,
    columns: Sequence[ColumnMetaData | _SchemaColumn],
    statement_type: int,
    *,
    defer_data_errors: bool = False,
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
    _skip_bytes = reader._skip_bytes
    _null_type = CUBRIDDataType.NULL
    _oid_size = DataSize.OID
    _get = _TYPE_METHOD_NAMES.get
    _unpack_int = _STRUCT_INT.unpack_from
    _int_size = DataSize.INT
    buffer = reader._buffer
    _getattr = getattr

    if is_call_type or _null_type in col_types:
        col_readers = None
    else:
        col_readers = [_resolve_reader(reader, ct) for ct in col_types]
        if defer_data_errors:
            col_readers = [_deferred_value_reader(reader, parse) for parse in col_readers]

    # Every cell value must use exactly the bytes its size word declares. The
    # fixed-width readers (INT, DATE, OID, ...) do not look at the size, so a
    # size past the end of the reply, or one that disagrees with the type's
    # width, was silently accepted; check it before reading (#383, #523).
    widths = [_FIXED_CELL_WIDTHS.get(ct) for ct in col_types]
    # Only a SET column converts its value (#559): every other type returns the
    # value unchanged from _convert_collection_value, so skip that call.
    converts = [ct == CUBRIDDataType.SET for ct in col_types]

    rows: list[tuple[Any, ...]] = []
    _rows_append = rows.append

    rows_start = reader.mark()
    try:
        for _ in range(tuple_count):
            _parse_int()
            _skip_bytes(_oid_size)
            row: list[Any] = [None] * ncols
            if col_readers is not None:
                for i in range(ncols):
                    # Inline _parse_int(): one call frame less per cell (#559).
                    offset = reader._offset
                    size = _unpack_int(buffer, offset)[0]
                    reader._offset = offset + _int_size
                    if size > 0:
                        width = widths[i]
                        if width is not None and (size != width if width > 0 else size < -width):
                            raise _cell_size_mismatch(col_types[i], size)
                        value = col_readers[i](size)
                        row[i] = (
                            _convert_collection_value(col_types[i], value) if converts[i] else value
                        )
            else:
                for i in range(ncols):
                    size = _parse_int()
                    if size <= 0:
                        continue
                    ct = col_types[i]
                    if is_call_type or ct == _null_type:
                        ct, size = _parse_cell_type(reader, size)
                        if size == 0:
                            continue
                    _check_cell_size(ct, size)
                    method_name = _get(ct)
                    if method_name is not None:
                        parse = _getattr(reader, method_name)
                        if defer_data_errors:
                            parse = _deferred_value_reader(reader, parse)
                        row[i] = _convert_collection_value(ct, parse(size))
                    else:
                        row[i] = _parse_bytes(size)
            _rows_append(tuple(row))
    except DataError:
        # A value the client cannot represent (invalid text #492, unknown zone
        # #413, zero date #512) is a data problem only when the reply is
        # complete. Re-walk the row data by its declared sizes first, so a
        # short reply still fails as framing damage (#383), not DataError.
        typed = [is_call_type or ct == _null_type for ct in col_types]
        _check_row_data_bounds(reader, rows_start, tuple_count, col_types, typed)
        raise
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
            error = exc_class(
                "prepared statement execution failed",
                code=error_code,
                errno=error_code,
                sqlstate=get_sqlstate(error_code) or _DEFAULT_SQLSTATE[exc_name],
            )
            setattr(error, "_cas_server_error", True)
            raise error
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


class _CasPacket:
    """Base of the CAS packets that carry connection-charset text (#86).

    ``encoding`` is the Python codec for SQL text, character values,
    metadata names and error messages. The owning connection sets it to its
    ``charset`` before ``write()``; the default keeps the UTF-8 wire format.
    """

    encoding: str = "utf-8"


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


class OpenDatabasePacket(_CasPacket):
    """Open a database connection."""

    def __init__(self, database: str, user: str, password: str, *, encoding: str = "utf-8") -> None:
        self.database = database
        self.user = user
        self.password = password
        self.encoding = encoding
        self.cas_info: bytes = b""
        self.response_code: int = 0
        self.broker_info: dict[str, int] = {}
        self.session_id: int = 0

    def write(self) -> bytes:
        """Serialize the open database packet.

        Wire format: database(32) + user(32) + password(32) + extended_info(512)
        + reserved(20) = 628 bytes (no header). Each name is encoded with the
        connection charset and cut to 32 bytes on a character boundary.
        """
        writer = PacketWriter(reserve_header=False, encoding=self.encoding)
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
        reader = PacketReader(data, encoding=self.encoding)
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


class PrepareAndExecutePacket(_CasPacket):
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
        # Handles the CAS releases before preparing this statement (#488): the
        # prepare arguments after the auto-commit flag (JDBC's wire format).
        self.deferred_close_handles: tuple[int, ...] = ()
        # Set by the transport when this reply's transaction freed its ID (#584).
        self._query_handle_retired = False

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
        writer = PacketWriter(encoding=self.encoding)
        writer._write_byte(CASFunctionCode.PREPARE_AND_EXECUTE)
        writer.add_int(3 + len(self.deferred_close_handles))  # prepare arg count
        writer._write_null_terminated_string(self.sql)
        writer.add_byte(CCIPrepareOption.NORMAL)
        writer.add_byte(1 if self.auto_commit else 0)
        for handle in self.deferred_close_handles:
            writer.add_int(handle)
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
            encoding=self.encoding,
        )
        reader._skip_bytes(DataSize.CAS_INFO)
        self.response_code = reader._parse_int()
        if self.response_code < 0:
            remaining = len(data) - 8
            _raise_error(reader, remaining)

        self.query_handle = self.response_code
        # Same layout as the FC2 tail, so the same count checks apply (#581).
        self.statement_type, self.bind_count, self.columns, metadata_error = _parse_prepare_info(
            reader
        )
        self.column_count = len(self.columns)

        if metadata_error is not None:
            position = reader.mark()
            # Validate without application hooks or opaque collection results.
            # Neither can bypass framing after metadata is already invalid.
            reader = PacketReader(data, decode_collections=True, encoding=self.encoding)
            reader.seek(position)

        self.total_tuple_count = reader._parse_int()
        if self.total_tuple_count < 0:
            raise ValueError("negative total tuple count")
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
            if 0 < reader.bytes_remaining() < 8:
                raise ValueError("truncated inline fetch header")
            if reader.bytes_remaining() >= 8:
                _ = reader._parse_int()  # fetch_code
                self.tuple_count = reader._parse_int()
                if self.tuple_count < 0:
                    raise ValueError("negative tuple count")
                if self.tuple_count > 0:
                    self.rows = _parse_row_data(
                        reader,
                        self.tuple_count,
                        self.columns,
                        self.statement_type,
                        defer_data_errors=metadata_error is not None,
                    )
        if metadata_error is not None:
            raise metadata_error


class PreparePacket(_CasPacket):
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
        writer = PacketWriter(encoding=self.encoding)
        writer._write_byte(CASFunctionCode.PREPARE)
        try:
            writer._write_null_terminated_string(self.sql)
        except DataError:
            raise DataError(
                f"prepared SQL cannot be encoded as {_codec_label(self.encoding)}"
            ) from None
        writer.add_byte(self.prepare_flag)
        writer.add_byte(1 if self.auto_commit else 0)
        return writer.finalize(cas_info)

    def parse(self, data: bytes | bytearray) -> None:
        """Parse the prepare response."""
        reader = PacketReader(data, encoding=self.encoding)
        reader._skip_bytes(DataSize.CAS_INFO)
        self.response_code = reader._parse_int()
        if self.response_code < 0:
            remaining = len(data) - 8
            _raise_error(reader, remaining)

        self.query_handle = self.response_code
        self.statement_type, self.bind_count, self.columns, metadata_error = _parse_prepare_info(
            reader
        )
        self.column_count = len(self.columns)
        if metadata_error is not None:
            raise metadata_error


class ExecutePacket(_CasPacket):
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
        bindings: Sequence[_PreparedScalar | _PreparedCollection] = (),
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
            if isinstance(binding, _PreparedScalar):
                has_text = binding.type_code == CUBRIDDataType.CHAR
            elif isinstance(binding, _PreparedCollection):
                has_text = binding.element_type == CUBRIDDataType.STRING
            else:
                raise ProgrammingError("invalid prepared parameter encoding")
            if has_text and binding.encoding != self.encoding:
                raise ProgrammingError("prepared string was encoded for a different charset")
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
            encoding=self.encoding,
        )
        reader._skip_bytes(DataSize.CAS_INFO)
        response_code = reader._parse_int()
        if response_code < 0:
            remaining = len(data) - 8
            _raise_error(reader, remaining)

        self.total_tuple_count = response_code
        metadata_error = None
        _ = reader._parse_byte()  # cache_reusable
        self.result_count = reader._parse_int()
        self.result_infos = _parse_result_infos(reader, self.result_count, prepared=True)

        if self.protocol_version > 1:
            includes_column_info = reader._parse_byte()
            if includes_column_info not in (0, 1):
                raise ValueError("invalid refreshed-column-info flag")
            if includes_column_info:
                self.statement_type, self.bind_count, self.columns, metadata_error = (
                    _parse_prepare_info(reader)
                )
        if metadata_error is not None:
            position = reader.mark()
            reader = PacketReader(data, decode_collections=True, encoding=self.encoding)
            reader.seek(position)
        if self.protocol_version > 4:
            _ = reader._parse_int()  # shard_id

        if self.statement_type == CUBRIDStatementType.SELECT:
            if 0 < reader.bytes_remaining() < 8:
                raise ValueError("truncated inline fetch header")
            if reader.bytes_remaining() >= 8:
                _ = reader._parse_int()  # fetch_code
                self.tuple_count = reader._parse_int()
                if self.tuple_count < 0:
                    raise ValueError("negative tuple count")
                if self.tuple_count > 0 and self.columns:
                    self.rows = _parse_row_data(
                        reader,
                        self.tuple_count,
                        self.columns,
                        self.statement_type,
                        defer_data_errors=metadata_error is not None,
                    )
        if metadata_error is not None:
            raise metadata_error


class FetchPacket(_CasPacket):
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
            encoding=self.encoding,
        )
        reader._skip_bytes(DataSize.CAS_INFO)
        response_code = reader._parse_int()
        if response_code < 0:
            remaining = len(data) - 8
            _raise_error(reader, remaining)

        effective_columns = columns if columns is not None else self._columns
        effective_stmt_type = statement_type if statement_type is not None else self._statement_type

        self.tuple_count = reader._parse_int()
        if self.tuple_count < 0:
            # Would read as an empty page and end the result set early (#523).
            raise ValueError("negative FETCH tuple count")
        if self.tuple_count > 0 and effective_columns:
            self.rows = _parse_row_data(
                reader, self.tuple_count, effective_columns, effective_stmt_type
            )


class CommitPacket(_CasPacket):
    """Commit transaction (FC=1)."""

    def write(self, cas_info: bytes) -> bytes:
        """Serialize the commit request."""
        writer = PacketWriter()
        writer._write_byte(CASFunctionCode.END_TRAN)
        writer.add_byte(CCITransactionType.COMMIT)
        return writer.finalize(cas_info)

    def parse(self, data: bytes | bytearray) -> None:
        """Parse the commit response."""
        reader = PacketReader(data, encoding=self.encoding)
        reader._skip_bytes(DataSize.CAS_INFO)
        response_code = reader._parse_int()
        if response_code < 0:
            remaining = len(data) - 8
            _raise_error(reader, remaining)


class RollbackPacket(_CasPacket):
    """Rollback transaction (FC=1)."""

    def write(self, cas_info: bytes) -> bytes:
        """Serialize the rollback request."""
        writer = PacketWriter()
        writer._write_byte(CASFunctionCode.END_TRAN)
        writer.add_byte(CCITransactionType.ROLLBACK)
        return writer.finalize(cas_info)

    def parse(self, data: bytes | bytearray) -> None:
        """Parse the rollback response."""
        reader = PacketReader(data, encoding=self.encoding)
        reader._skip_bytes(DataSize.CAS_INFO)
        response_code = reader._parse_int()
        if response_code < 0:
            remaining = len(data) - 8
            _raise_error(reader, remaining)


class CloseDatabasePacket(_CasPacket):
    """Close database connection (FC=31)."""

    def write(self, cas_info: bytes) -> bytes:
        """Serialize the close database request."""
        writer = PacketWriter()
        writer._write_byte(CASFunctionCode.CON_CLOSE)
        return writer.finalize(cas_info)

    def parse(self, data: bytes | bytearray) -> None:
        """Parse the close database response."""
        reader = PacketReader(data, encoding=self.encoding)
        reader._skip_bytes(DataSize.CAS_INFO)
        response_code = reader._parse_int()
        if response_code < 0:
            remaining = len(data) - 8
            _raise_error(reader, remaining)


class CloseQueryPacket(_CasPacket):
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
        reader = PacketReader(data, encoding=self.encoding)
        reader._skip_bytes(DataSize.CAS_INFO)
        response_code = reader._parse_int()
        if response_code < 0:
            remaining = len(data) - 8
            _raise_error(reader, remaining)


class GetEngineVersionPacket(_CasPacket):
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
        reader = PacketReader(data, encoding=self.encoding)
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
    encoding: str = "utf-8",
) -> bytes:
    """Serialize the independently nullable FC9 arguments and shard identifier."""
    writer = PacketWriter(encoding=encoding)
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


class GetSchemaPacket(_CasPacket):
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
            encoding=self.encoding,
        )

    def parse(self, data: bytes | bytearray) -> None:
        """Parse the get schema response."""
        reader = PacketReader(data, encoding=self.encoding)
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


class BatchExecutePacket(_CasPacket):
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
        writer = PacketWriter(encoding=self.encoding)
        writer._write_byte(CASFunctionCode.EXECUTE_BATCH)
        writer.add_byte(1 if self.auto_commit else 0)
        if self.protocol_version > 3:
            writer.add_int(0)  # timeout
        for sql in self.sql_list:
            writer._write_null_terminated_string(sql)
        return writer.finalize(cas_info)

    def parse(self, data: bytes | bytearray) -> None:
        """Parse the batch execute response."""
        reader = PacketReader(data, encoding=self.encoding)
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
                error_msg = reader._parse_lenient_text(msg_len)
                self.errors.append({"code": error_code, "message": error_msg})
            else:
                self.results.append((stmt_type, result))
                reader._parse_int()  # unused
                reader._parse_short()  # unused
                reader._parse_short()  # unused
        if self.protocol_version > 4:
            _ = reader._parse_int()  # lastShardId


class LOBNewPacket(_CasPacket):
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
        reader = PacketReader(data, encoding=self.encoding)
        reader._skip_bytes(DataSize.CAS_INFO)
        response_code = reader._parse_int()
        if response_code < 0:
            remaining = len(data) - 8
            _raise_error(reader, remaining)
        # Remaining bytes are the LOB handle
        self.lob_handle = reader._parse_bytes(reader.bytes_remaining())


class LOBWritePacket(_CasPacket):
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
        reader = PacketReader(data, encoding=self.encoding)
        reader._skip_bytes(DataSize.CAS_INFO)
        response_code = reader._parse_int()
        if response_code < 0:
            remaining = len(data) - 8
            _raise_error(reader, remaining)
        self.bytes_written = response_code


class LOBReadPacket(_CasPacket):
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
        reader = PacketReader(data, encoding=self.encoding)
        reader._skip_bytes(DataSize.CAS_INFO)
        response_code = reader._parse_int()
        if response_code < 0:
            remaining = len(data) - 8
            _raise_error(reader, remaining)
        # A count past the end of the reply raises before any field is set (#383).
        if response_code > 0:
            self.lob_data = reader._parse_bytes(response_code)
        self.bytes_read = response_code


class GetLastInsertIdPacket(_CasPacket):
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
        reader = PacketReader(data, encoding=self.encoding)
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


class GetDbParameterPacket(_CasPacket):
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
        reader = PacketReader(data, encoding=self.encoding)
        reader._skip_bytes(DataSize.CAS_INFO)
        response_code = reader._parse_int()
        if response_code < 0:
            remaining = len(data) - 8
            _raise_error(reader, remaining)
        self.value = reader._parse_int()


class CheckCasPacket(_CasPacket):
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
        reader = PacketReader(data, encoding=self.encoding)
        reader._skip_bytes(DataSize.CAS_INFO)
        # CHECK_CAS success may return an empty body (no int) or a zero int.
        # Accept both: empty body → 0 (alive), otherwise parse the int.
        if reader.bytes_remaining() >= DataSize.INT:
            self.response_code = reader._parse_int()
        else:
            self.response_code = 0


class SetDbParameterPacket(_CasPacket):
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
        reader = PacketReader(data, encoding=self.encoding)
        reader._skip_bytes(DataSize.CAS_INFO)
        response_code = reader._parse_int()
        if response_code < 0:
            remaining = len(data) - 8
            _raise_error(reader, remaining)
