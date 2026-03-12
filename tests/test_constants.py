"""Tests for pycubrid.constants — CAS protocol constants."""

from __future__ import annotations

import enum

import pytest

from pycubrid.constants import (
    CASFunctionCode,
    CASProtocol,
    CCICursorPosition,
    CCIDbParam,
    CCIExecutionOption,
    CCILOBType,
    CCIPrepareOption,
    CCISchemaPatternMatchFlag,
    CCISchemaType,
    CCITransactionType,
    ConnectionStatus,
    CUBRIDCollectionCommand,
    CUBRIDDataType,
    CUBRIDIsolationLevel,
    CUBRIDStatementType,
    DataSize,
    ErrorCode,
    OidCommand,
    QueryExecutionMode,
    StmtType,
)


# ---------------------------------------------------------------------------
# CAS Function Code tests
# ---------------------------------------------------------------------------


class TestCASFunctionCode:
    """Verify CAS function codes are correct IntEnum values."""

    def test_is_int_enum(self) -> None:
        assert issubclass(CASFunctionCode, enum.IntEnum)

    def test_end_tran(self) -> None:
        assert CASFunctionCode.END_TRAN == 1

    def test_prepare(self) -> None:
        assert CASFunctionCode.PREPARE == 2

    def test_execute(self) -> None:
        assert CASFunctionCode.EXECUTE == 3

    def test_fetch(self) -> None:
        assert CASFunctionCode.FETCH == 8

    def test_schema_info(self) -> None:
        assert CASFunctionCode.SCHEMA_INFO == 9

    def test_get_db_version(self) -> None:
        assert CASFunctionCode.GET_DB_VERSION == 15

    def test_execute_batch(self) -> None:
        assert CASFunctionCode.EXECUTE_BATCH == 20

    def test_con_close(self) -> None:
        assert CASFunctionCode.CON_CLOSE == 31

    def test_lob_new(self) -> None:
        assert CASFunctionCode.LOB_NEW == 35

    def test_lob_write(self) -> None:
        assert CASFunctionCode.LOB_WRITE == 36

    def test_lob_read(self) -> None:
        assert CASFunctionCode.LOB_READ == 37

    def test_get_last_insert_id(self) -> None:
        assert CASFunctionCode.GET_LAST_INSERT_ID == 40

    def test_prepare_and_execute(self) -> None:
        assert CASFunctionCode.PREPARE_AND_EXECUTE == 41

    def test_close_req_handle(self) -> None:
        assert CASFunctionCode.CLOSE_REQ_HANDLE == 6

    def test_total_members(self) -> None:
        # 41 function codes total
        assert len(CASFunctionCode) == 41


# ---------------------------------------------------------------------------
# CUBRID Data Type tests
# ---------------------------------------------------------------------------


class TestCUBRIDDataType:
    """Verify CUBRID data type codes."""

    def test_is_int_enum(self) -> None:
        assert issubclass(CUBRIDDataType, enum.IntEnum)

    @pytest.mark.parametrize(
        ("member", "value"),
        [
            ("CHAR", 1),
            ("STRING", 2),
            ("NCHAR", 3),
            ("VARNCHAR", 4),
            ("BIT", 5),
            ("VARBIT", 6),
            ("NUMERIC", 7),
            ("INT", 8),
            ("SHORT", 9),
            ("MONETARY", 10),
            ("FLOAT", 11),
            ("DOUBLE", 12),
            ("DATE", 13),
            ("TIME", 14),
            ("TIMESTAMP", 15),
            ("SET", 16),
            ("MULTISET", 17),
            ("SEQUENCE", 18),
            ("OBJECT", 19),
            ("RESULTSET", 20),
            ("BIGINT", 21),
            ("DATETIME", 22),
            ("BLOB", 23),
            ("CLOB", 24),
            ("ENUM", 25),
            ("TIMESTAMPTZ", 29),
            ("TIMESTAMPLTZ", 30),
            ("DATETIMETZ", 31),
            ("DATETIMELTZ", 32),
        ],
    )
    def test_data_type_value(self, member: str, value: int) -> None:
        assert CUBRIDDataType[member] == value

    def test_unknown_is_zero(self) -> None:
        assert CUBRIDDataType.UNKNOWN == 0


# ---------------------------------------------------------------------------
# Statement Type tests
# ---------------------------------------------------------------------------


class TestCUBRIDStatementType:
    """Verify statement type codes."""

    def test_is_int_enum(self) -> None:
        assert issubclass(CUBRIDStatementType, enum.IntEnum)

    def test_select(self) -> None:
        assert CUBRIDStatementType.SELECT == 21

    def test_insert(self) -> None:
        assert CUBRIDStatementType.INSERT == 20

    def test_update(self) -> None:
        assert CUBRIDStatementType.UPDATE == 22

    def test_delete(self) -> None:
        assert CUBRIDStatementType.DELETE == 23

    def test_call_sp(self) -> None:
        assert CUBRIDStatementType.CALL_SP == 0x7E

    def test_unknown(self) -> None:
        assert CUBRIDStatementType.UNKNOWN == 0x7F


# ---------------------------------------------------------------------------
# Transaction Type tests
# ---------------------------------------------------------------------------


class TestCCITransactionType:
    """Verify transaction type codes."""

    def test_commit(self) -> None:
        assert CCITransactionType.COMMIT == 1

    def test_rollback(self) -> None:
        assert CCITransactionType.ROLLBACK == 2


# ---------------------------------------------------------------------------
# Prepare Option tests
# ---------------------------------------------------------------------------


class TestCCIPrepareOption:
    """Verify prepare option flags."""

    def test_is_int_flag(self) -> None:
        assert issubclass(CCIPrepareOption, enum.IntFlag)

    def test_normal(self) -> None:
        assert CCIPrepareOption.NORMAL == 0x00

    def test_include_oid(self) -> None:
        assert CCIPrepareOption.INCLUDE_OID == 0x01

    def test_call(self) -> None:
        assert CCIPrepareOption.CALL == 0x40

    def test_combinable(self) -> None:
        combined = CCIPrepareOption.INCLUDE_OID | CCIPrepareOption.UPDATABLE
        assert combined == 0x03


# ---------------------------------------------------------------------------
# Execution Option tests
# ---------------------------------------------------------------------------


class TestCCIExecutionOption:
    """Verify execution option flags."""

    def test_is_int_flag(self) -> None:
        assert issubclass(CCIExecutionOption, enum.IntFlag)

    def test_normal(self) -> None:
        assert CCIExecutionOption.NORMAL == 0x00

    def test_query_all(self) -> None:
        assert CCIExecutionOption.QUERY_ALL == 0x02

    def test_holdable(self) -> None:
        assert CCIExecutionOption.HOLDABLE == 0x20

    def test_combinable(self) -> None:
        combined = CCIExecutionOption.QUERY_ALL | CCIExecutionOption.HOLDABLE
        assert combined == 0x22


# ---------------------------------------------------------------------------
# Schema Type tests
# ---------------------------------------------------------------------------


class TestCCISchemaType:
    """Verify schema introspection type codes."""

    def test_is_int_enum(self) -> None:
        assert issubclass(CCISchemaType, enum.IntEnum)

    def test_class(self) -> None:
        assert CCISchemaType.CLASS == 1

    def test_primary_key(self) -> None:
        assert CCISchemaType.PRIMARY_KEY == 16

    def test_imported_keys(self) -> None:
        assert CCISchemaType.IMPORTED_KEYS == 17

    def test_cross_reference(self) -> None:
        assert CCISchemaType.CROSS_REFERENCE == 19

    def test_total_members(self) -> None:
        assert len(CCISchemaType) == 19


# ---------------------------------------------------------------------------
# LOB Type tests
# ---------------------------------------------------------------------------


class TestCCILOBType:
    """Verify LOB type codes."""

    def test_blob(self) -> None:
        assert CCILOBType.BLOB == 33

    def test_clob(self) -> None:
        assert CCILOBType.CLOB == 34


# ---------------------------------------------------------------------------
# DB Param tests
# ---------------------------------------------------------------------------


class TestCCIDbParam:
    """Verify database parameter codes."""

    def test_isolation_level(self) -> None:
        assert CCIDbParam.ISOLATION_LEVEL == 1

    def test_lock_timeout(self) -> None:
        assert CCIDbParam.LOCK_TIMEOUT == 2

    def test_auto_commit(self) -> None:
        assert CCIDbParam.AUTO_COMMIT == 4


# ---------------------------------------------------------------------------
# Isolation Level tests
# ---------------------------------------------------------------------------


class TestCUBRIDIsolationLevel:
    """Verify isolation level codes."""

    def test_is_int_enum(self) -> None:
        assert issubclass(CUBRIDIsolationLevel, enum.IntEnum)

    def test_serializable(self) -> None:
        assert CUBRIDIsolationLevel.SERIALIZABLE == 0x06

    def test_default(self) -> None:
        assert CUBRIDIsolationLevel.DEFAULT == 0x01

    def test_rep_class_rep_instance(self) -> None:
        assert CUBRIDIsolationLevel.REP_CLASS_REP_INSTANCE == 0x05


# ---------------------------------------------------------------------------
# Protocol Constants tests
# ---------------------------------------------------------------------------


class TestCASProtocol:
    """Verify CAS protocol constants."""

    def test_magic_string(self) -> None:
        assert CASProtocol.MAGIC_STRING == "CUBRK"

    def test_client_jdbc(self) -> None:
        assert CASProtocol.CLIENT_JDBC == 3

    def test_proto_indicator(self) -> None:
        assert CASProtocol.PROTO_INDICATOR == 0x40

    def test_version(self) -> None:
        assert CASProtocol.VERSION == 7

    def test_cas_version(self) -> None:
        assert CASProtocol.CAS_VERSION == 0x40 | 7
        assert CASProtocol.CAS_VERSION == 0x47


# ---------------------------------------------------------------------------
# Data Size tests
# ---------------------------------------------------------------------------


class TestDataSize:
    """Verify wire format data sizes."""

    def test_int(self) -> None:
        assert DataSize.INT == 4

    def test_short(self) -> None:
        assert DataSize.SHORT == 2

    def test_long(self) -> None:
        assert DataSize.LONG == 8

    def test_byte(self) -> None:
        assert DataSize.BYTE == 1

    def test_float(self) -> None:
        assert DataSize.FLOAT == 4

    def test_double(self) -> None:
        assert DataSize.DOUBLE == 8

    def test_oid(self) -> None:
        assert DataSize.OID == 8

    def test_broker_info(self) -> None:
        assert DataSize.BROKER_INFO == 8

    def test_date(self) -> None:
        assert DataSize.DATE == 14

    def test_cas_info(self) -> None:
        assert DataSize.CAS_INFO == 4

    def test_data_length(self) -> None:
        assert DataSize.DATA_LENGTH == 4

    def test_resultset(self) -> None:
        assert DataSize.RESULTSET == 4


# ---------------------------------------------------------------------------
# Error Code tests
# ---------------------------------------------------------------------------


class TestErrorCode:
    """Verify error code values."""

    def test_is_int_enum(self) -> None:
        assert issubclass(ErrorCode, enum.IntEnum)

    def test_no_error(self) -> None:
        assert ErrorCode.ER_NO_ERROR == 0

    def test_dbms(self) -> None:
        assert ErrorCode.ER_DBMS == -1

    def test_communication(self) -> None:
        assert ErrorCode.ER_COMMUNICATION == -4

    def test_is_closed(self) -> None:
        assert ErrorCode.ER_IS_CLOSED == -11


# ---------------------------------------------------------------------------
# Other enum tests
# ---------------------------------------------------------------------------


class TestOtherEnums:
    """Verify remaining enum types."""

    def test_oid_command(self) -> None:
        assert OidCommand.DROP_BY_OID == 1
        assert OidCommand.GET_CLASS_NAME_BY_OID == 5

    def test_collection_command(self) -> None:
        assert CUBRIDCollectionCommand.GET_COLLECTION_VALUE == 1
        assert CUBRIDCollectionCommand.PUT_ELEMENT_ON_SEQUENCE == 7

    def test_connection_status(self) -> None:
        assert ConnectionStatus.OUT_TRAN == 0
        assert ConnectionStatus.CLOSE_AND_CONNECT == 3

    def test_cursor_position(self) -> None:
        assert CCICursorPosition.FIRST == 0
        assert CCICursorPosition.LAST == 2

    def test_stmt_type(self) -> None:
        assert StmtType.NORMAL == 0
        assert StmtType.GET_AUTOINCREMENT_KEYS == 3

    def test_query_execution_mode(self) -> None:
        assert QueryExecutionMode.SYNC == 0
        assert QueryExecutionMode.ASYNC == 1

    def test_schema_pattern_match_flag(self) -> None:
        assert issubclass(CCISchemaPatternMatchFlag, enum.IntFlag)
        assert CCISchemaPatternMatchFlag.CLASS_NAME == 0x01
        assert CCISchemaPatternMatchFlag.ATTR_NAME == 0x02
