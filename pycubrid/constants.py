"""CAS protocol constants for pycubrid.

Ported from the node-cubrid reference implementation:
- ``src/constants/CASConstants.js``
- ``src/constants/DataTypes.js``

All enumerations use :class:`enum.IntEnum` or :class:`enum.IntFlag` so they
can be used directly as integer values in wire-protocol operations.
"""

from __future__ import annotations

from enum import IntEnum, IntFlag


# ---------------------------------------------------------------------------
# CAS Function Codes (41 functions)
# ---------------------------------------------------------------------------


class CASFunctionCode(IntEnum):
    """CAS broker function codes sent as the first byte of a request payload."""

    END_TRAN = 1
    PREPARE = 2
    EXECUTE = 3
    GET_DB_PARAMETER = 4
    SET_DB_PARAMETER = 5
    CLOSE_REQ_HANDLE = 6
    CURSOR = 7
    FETCH = 8
    SCHEMA_INFO = 9
    OID_GET = 10
    OID_PUT = 11
    DEPRECATED1 = 12
    DEPRECATED2 = 13
    DEPRECATED3 = 14
    GET_DB_VERSION = 15
    GET_CLASS_NUM_OBJS = 16
    OID_CMD = 17
    COLLECTION = 18
    NEXT_RESULT = 19
    EXECUTE_BATCH = 20
    EXECUTE_ARRAY = 21
    CURSOR_UPDATE = 22
    GET_ATTR_TYPE_STR = 23
    GET_QUERY_INFO = 24
    DEPRECATED4 = 25
    SAVEPOINT = 26
    PARAMETER_INFO = 27
    XA_PREPARE = 28
    XA_RECOVER = 29
    XA_END_TRAN = 30
    CON_CLOSE = 31
    CHECK_CAS = 32
    MAKE_OUT_RS = 33
    GET_GENERATED_KEYS = 34
    LOB_NEW = 35
    LOB_WRITE = 36
    LOB_READ = 37
    END_SESSION = 38
    GET_ROW_COUNT = 39
    GET_LAST_INSERT_ID = 40
    PREPARE_AND_EXECUTE = 41


# ---------------------------------------------------------------------------
# CUBRID Data Types
# ---------------------------------------------------------------------------


class CUBRIDDataType(IntEnum):
    """CUBRID CCI unified type codes used on the wire."""

    UNKNOWN = 0
    NULL = 0
    CHAR = 1
    STRING = 2
    NCHAR = 3
    VARNCHAR = 4
    BIT = 5
    VARBIT = 6
    NUMERIC = 7
    INT = 8
    SHORT = 9
    MONETARY = 10
    FLOAT = 11
    DOUBLE = 12
    DATE = 13
    TIME = 14
    TIMESTAMP = 15
    SET = 16
    MULTISET = 17
    SEQUENCE = 18
    OBJECT = 19
    RESULTSET = 20
    BIGINT = 21
    DATETIME = 22
    BLOB = 23
    CLOB = 24
    ENUM = 25
    TIMESTAMPTZ = 29
    TIMESTAMPLTZ = 30
    DATETIMETZ = 31
    DATETIMELTZ = 32


# ---------------------------------------------------------------------------
# CUBRID Statement Types
# ---------------------------------------------------------------------------


class CUBRIDStatementType(IntEnum):
    """Statement type codes returned after PREPARE."""

    ALTER_CLASS = 0
    ALTER_SERIAL = 1
    COMMIT_WORK = 2
    REGISTER_DATABASE = 3
    CREATE_CLASS = 4
    CREATE_INDEX = 5
    CREATE_TRIGGER = 6
    CREATE_SERIAL = 7
    DROP_DATABASE = 8
    DROP_CLASS = 9
    DROP_INDEX = 10
    DROP_LABEL = 11
    DROP_TRIGGER = 12
    DROP_SERIAL = 13
    EVALUATE = 14
    RENAME_CLASS = 15
    ROLLBACK_WORK = 16
    GRANT = 17
    REVOKE = 18
    STATISTICS = 19
    INSERT = 20
    SELECT = 21
    UPDATE = 22
    DELETE = 23
    CALL = 24
    GET_ISO_LVL = 25
    GET_TIMEOUT = 26
    GET_OPT_LVL = 27
    SET_OPT_LVL = 28
    SCOPE = 29
    GET_TRIGGER = 30
    SET_TRIGGER = 31
    SAVEPOINT = 32
    PREPARE = 33
    ATTACH = 34
    USE = 35
    REMOVE_TRIGGER = 36
    RENAME_TRIGGER = 37
    ON_LDB = 38
    GET_LDB = 39
    SET_LDB = 40
    GET_STATS = 41
    CREATE_USER = 42
    DROP_USER = 43
    ALTER_USER = 44
    CALL_SP = 0x7E
    UNKNOWN = 0x7F


# ---------------------------------------------------------------------------
# Transaction Types
# ---------------------------------------------------------------------------


class CCITransactionType(IntEnum):
    """Transaction end-type codes for ``END_TRAN``."""

    COMMIT = 1
    ROLLBACK = 2


# ---------------------------------------------------------------------------
# Prepare Options
# ---------------------------------------------------------------------------


class CCIPrepareOption(IntFlag):
    """Flags for the PREPARE function."""

    NORMAL = 0x00
    INCLUDE_OID = 0x01
    UPDATABLE = 0x02
    QUERY_INFO = 0x04
    HOLDABLE = 0x08
    CALL = 0x40


# ---------------------------------------------------------------------------
# Execution Options
# ---------------------------------------------------------------------------


class CCIExecutionOption(IntFlag):
    """Flags for the EXECUTE function."""

    NORMAL = 0x00
    ASYNC = 0x01
    QUERY_ALL = 0x02
    QUERY_INFO = 0x04
    ONLY_QUERY_PLAN = 0x08
    THREAD = 0x10
    HOLDABLE = 0x20


# ---------------------------------------------------------------------------
# Schema Types
# ---------------------------------------------------------------------------


class CCISchemaType(IntEnum):
    """Schema introspection type codes for ``SCHEMA_INFO``."""

    CLASS = 1
    VCLASS = 2
    QUERY_SPEC = 3
    ATTRIBUTE = 4
    CLASS_ATTRIBUTE = 5
    METHOD = 6
    CLASS_METHOD = 7
    METHOD_FILE = 8
    SUPERCLASS = 9
    SUBCLASS = 10
    CONSTRAINT = 11
    TRIGGER = 12
    CLASS_PRIVILEGE = 13
    ATTR_PRIVILEGE = 14
    DIRECT_SUPER_CLASS = 15
    PRIMARY_KEY = 16
    IMPORTED_KEYS = 17
    EXPORTED_KEYS = 18
    CROSS_REFERENCE = 19


# ---------------------------------------------------------------------------
# LOB Types
# ---------------------------------------------------------------------------


class CCILOBType(IntEnum):
    """LOB type codes for ``LOB_NEW``."""

    BLOB = 33
    CLOB = 34


# ---------------------------------------------------------------------------
# DB Parameters
# ---------------------------------------------------------------------------


class CCIDbParam(IntEnum):
    """Database parameter codes for ``GET_DB_PARAMETER`` / ``SET_DB_PARAMETER``."""

    ISOLATION_LEVEL = 1
    LOCK_TIMEOUT = 2
    MAX_STRING_LENGTH = 3
    AUTO_COMMIT = 4


# ---------------------------------------------------------------------------
# Isolation Levels
# ---------------------------------------------------------------------------


class CUBRIDIsolationLevel(IntEnum):
    """CUBRID isolation level codes (dual-granularity model)."""

    UNKNOWN = 0x00
    COMMIT_CLASS_UNCOMMIT_INSTANCE = 0x01
    COMMIT_CLASS_COMMIT_INSTANCE = 0x02
    REP_CLASS_UNCOMMIT_INSTANCE = 0x03
    REP_CLASS_COMMIT_INSTANCE = 0x04
    REP_CLASS_REP_INSTANCE = 0x05
    SERIALIZABLE = 0x06
    DEFAULT = 0x01


# ---------------------------------------------------------------------------
# OID Commands
# ---------------------------------------------------------------------------


class OidCommand(IntEnum):
    """OID operation commands."""

    DROP_BY_OID = 1
    IS_INSTANCE = 2
    GET_READ_LOCK_BY_OID = 3
    GET_WRITE_LOCK_BY_OID = 4
    GET_CLASS_NAME_BY_OID = 5


# ---------------------------------------------------------------------------
# Collection Commands
# ---------------------------------------------------------------------------


class CUBRIDCollectionCommand(IntEnum):
    """Collection manipulation commands."""

    GET_COLLECTION_VALUE = 1
    GET_SIZE_OF_COLLECTION = 2
    DROP_ELEMENT_IN_SET = 3
    ADD_ELEMENT_TO_SET = 4
    DROP_ELEMENT_IN_SEQUENCE = 5
    INSERT_ELEMENT_INTO_SEQUENCE = 6
    PUT_ELEMENT_ON_SEQUENCE = 7


# ---------------------------------------------------------------------------
# Connection Status
# ---------------------------------------------------------------------------


class ConnectionStatus(IntEnum):
    """Internal connection status codes."""

    OUT_TRAN = 0
    IN_TRAN = 1
    CLOSE = 2
    CLOSE_AND_CONNECT = 3


# ---------------------------------------------------------------------------
# Cursor Position
# ---------------------------------------------------------------------------


class CCICursorPosition(IntEnum):
    """Cursor positioning constants."""

    FIRST = 0
    CURRENT = 1
    LAST = 2


# ---------------------------------------------------------------------------
# Statement Execution Type
# ---------------------------------------------------------------------------


class StmtType(IntEnum):
    """Statement execution type codes."""

    NORMAL = 0
    GET_BY_OID = 1
    GET_SCHEMA_INFO = 2
    GET_AUTOINCREMENT_KEYS = 3


# ---------------------------------------------------------------------------
# Query Execution Mode
# ---------------------------------------------------------------------------


class QueryExecutionMode(IntEnum):
    """Query execution mode constants."""

    SYNC = 0
    ASYNC = 1


# ---------------------------------------------------------------------------
# Schema Pattern Match Flags
# ---------------------------------------------------------------------------


class CCISchemaPatternMatchFlag(IntFlag):
    """Pattern matching flags for schema introspection."""

    CLASS_NAME = 0x01
    ATTR_NAME = 0x02


# ---------------------------------------------------------------------------
# CAS Protocol Constants
# ---------------------------------------------------------------------------


class CASProtocol:
    """CAS protocol constants."""

    MAGIC_STRING: str = "CUBRK"
    CLIENT_JDBC: int = 3
    PROTO_INDICATOR: int = 0x40
    VERSION: int = 7
    CAS_VERSION: int = PROTO_INDICATOR | VERSION


# ---------------------------------------------------------------------------
# Data Sizes (bytes)
# ---------------------------------------------------------------------------


class DataSize:
    """Wire format data sizes in bytes."""

    UNSPECIFIED: int = 0
    BYTE: int = 1
    BOOL: int = 1
    SHORT: int = 2
    INT: int = 4
    FLOAT: int = 4
    LONG: int = 8
    DOUBLE: int = 8
    OBJECT: int = 8
    OID: int = 8
    BROKER_INFO: int = 8
    DATE: int = 14
    TIME: int = 14
    DATETIME: int = 14
    TIMESTAMP: int = 14
    RESULTSET: int = 4
    DATA_LENGTH: int = 4
    CAS_INFO: int = 4


# ---------------------------------------------------------------------------
# Common CUBRID Error Codes
# ---------------------------------------------------------------------------


class ErrorCode(IntEnum):
    """Commonly encountered CUBRID CAS error codes."""

    # CAS errors
    ER_NO_ERROR = 0
    ER_DBMS = -1
    ER_INTERNAL = -2
    ER_NO_MORE_MEMORY = -3
    ER_COMMUNICATION = -4
    ER_NO_MORE_DATA = -5
    ER_TRAN_TYPE = -6
    ER_STRING_PARAM = -7
    ER_TYPE_CONVERSION = -8
    ER_BIND_INDEX = -9
    ER_ARGS = -10
    ER_IS_CLOSED = -11
    ER_ISOLATION_LEVEL = -12
    ER_NO_SHARD_AVAILABLE = -13
    ER_INVALID_CURSOR_POS = -14
    ER_STMT_POOLING = -15
