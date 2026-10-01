"""Exact wire-byte coverage for the remaining CAS request packets (#562).

``test_prepared_packet_contract.py`` already freezes FC2/FC3 (``PreparePacket``
and ``ExecutePacket`` scalar bindings, including NULL, empty strings and
int32 boundaries) and ``test_prepared_collection_contract.py`` already
freezes the FC3 typed collection payload (#482). This module covers every
other ``_CasPacket`` request class in ``pycubrid.protocol`` that only had
function-code or partial-field checks in ``test_protocol.py``: FC41
(including the #488 deferred-close id list), FETCH, END_TRAN, CON_CLOSE,
CLOSE_REQ_HANDLE, GET_DB_VERSION, SCHEMA_INFO, EXECUTE_BATCH, LOB_NEW,
LOB_WRITE, LOB_READ, GET_LAST_INSERT_ID, GET_DB_PARAMETER, CHECK_CAS and
SET_DB_PARAMETER.

Expected bytes are derived independently from the CAS broker source at the
commits already pinned by this repo for request-byte work (see
``docs/PREPARED_BINDING_DESIGN.md``): CUBRID/cubrid
``11.4 6b2bc75527c8bad94d9ad8aba961638efdfb3269`` and
``10.2 d56a158c06ee6ef917c9db7c52b778c63871bdd7``, both under
``src/broker/``. Each test cites the exact ``cas_function.c``/``cas_net_buf.c``
function and line range read while writing it, never ``pycubrid.protocol``
itself. The general "argument = 4-byte big-endian length + payload" framing
is the same one ``_arguments()`` (imported below) already decodes for the
FC2/FC3 contract tests; every CAS ``fn_*`` handler reads its arguments with
the matching ``net_arg_get_*`` macro/function from ``cas_net_buf.h``.
"""

from __future__ import annotations

import struct

import pytest

from pycubrid import protocol
from pycubrid.constants import (
    CASFunctionCode,
    CCIDbParam,
    CCILOBType,
    CCISchemaType,
    CCITransactionType,
)
from pycubrid.exceptions import DataError

from .test_prepared_packet_contract import _CAS_INFO, _arguments


# ---------------------------------------------------------------------------
# FC41 PREPARE_AND_EXECUTE, including the #488 deferred-close id list
# ---------------------------------------------------------------------------
#
# cas_function.c fn_prepare_and_execute (11.4:843-867, 10.2:746-770) reads
# argv[0] as the prepare-argument count, hands fn_prepare_internal
# (11.4:324-443, 10.2:301-?) that many args starting at argv+1, then always
# calls fn_execute_internal with a *hardcoded* argc of 10 starting right
# after the prepare args (11.4:858, 10.2:761).
#
# fn_prepare_internal reads: sql (argv[0], 11.4:343/10.2:320), flag
# (argv[1], 11.4:345/10.2:321), and only if argc > 2: auto_commit_mode
# (argv[2]) then one net_arg_get_int per remaining arg as a deferred-close
# handle to free immediately (11.4:349-356, 10.2:325-332) — confirming the
# count field doubles as "3 fixed args + one int per queued handle" and the
# ids are plain ints with no extra framing of their own.
#
# fn_execute_internal, when given a non-NULL prepared_srv_h_id (the FC41
# path), takes the handle from that out-param instead of consuming an argv
# slot, and derives fetch_flag/auto_commit_mode/forward_only_cursor from the
# just-prepared handle's own state instead of reading them from the wire
# (11.4:505-562, 10.2:464-521) — it only reads flag, max_col_size, max_row,
# param_mode (11.4:535-538, 10.2:489-492), then cache_time and (since this
# driver always claims protocol >= 1) query_timeout (11.4:594-600,
# 10.2:551-556). That is 6 wire arguments, not 10: PREPARE_AND_EXECUTE's
# embedded EXECUTE never sends query_handle, fetch_flag, auto_commit or
# forward_only as separate arguments the way standalone FC3 does.


def test_prepare_and_execute_exact_request_without_deferred_close() -> None:
    packet = protocol.PrepareAndExecutePacket("SELECT 1", auto_commit=True)
    args = _arguments(packet.write(_CAS_INFO), CASFunctionCode.PREPARE_AND_EXECUTE)
    assert args == [
        struct.pack(">i", 3),  # prepare arg count = 3 fixed + 0 deferred handles
        b"SELECT 1\x00",
        b"\x00",  # prepare flag: CCIPrepareOption.NORMAL
        b"\x01",  # auto_commit
        b"\x02",  # execution option: CCIExecutionOption.QUERY_ALL
        b"\x00" * 4,  # max_col_size
        b"\x00" * 4,  # max_row_size
        b"",  # NULL param_mode (zero-length arg, net_arg_get_str size<=0)
        b"\x00" * 8,  # cache time: sec=0, usec=0
        b"\x00" * 4,  # query timeout
    ]


def test_prepare_and_execute_exact_request_with_deferred_close_ids() -> None:
    packet = protocol.PrepareAndExecutePacket("SELECT * FROM t")
    packet.deferred_close_handles = (101, 202, 303)
    args = _arguments(packet.write(_CAS_INFO), CASFunctionCode.PREPARE_AND_EXECUTE)
    assert args == [
        struct.pack(">i", 6),  # 3 fixed + 3 deferred handles
        b"SELECT * FROM t\x00",
        b"\x00",
        b"\x00",  # auto_commit defaults to False
        struct.pack(">i", 101),
        struct.pack(">i", 202),
        struct.pack(">i", 303),
        b"\x02",
        b"\x00" * 4,
        b"\x00" * 4,
        b"",
        b"\x00" * 8,
        b"\x00" * 4,
    ]


def test_prepare_and_execute_exact_request_with_single_deferred_close_id() -> None:
    """One queued handle: the count field is 3+1 and exactly one int follows."""
    packet = protocol.PrepareAndExecutePacket("DELETE FROM t")
    packet.deferred_close_handles = (7,)
    args = _arguments(packet.write(_CAS_INFO), CASFunctionCode.PREPARE_AND_EXECUTE)
    assert args[0] == struct.pack(">i", 4)
    assert args[4] == struct.pack(">i", 7)  # the deferred-close id itself
    assert args[5] == b"\x02"  # execution option follows immediately after


# ---------------------------------------------------------------------------
# FC8 FETCH
# ---------------------------------------------------------------------------
#
# fn_fetch (11.4:1144-1167, 10.2:1054-1077) reads exactly five arguments in
# order: srv_h_id (int), cursor_pos (int), fetch_count (int), fetch_flag
# (char), result_set_index (int).


def test_fetch_exact_request() -> None:
    packet = protocol.FetchPacket(query_handle=7, current_tuple_count=9, fetch_size=50)
    args = _arguments(packet.write(_CAS_INFO), CASFunctionCode.FETCH)
    assert args == [
        struct.pack(">i", 7),
        struct.pack(">i", 10),  # cursor_pos = current_tuple_count + 1
        struct.pack(">i", 50),
        b"\x00",  # case-sensitive flag, always 0
        struct.pack(">i", 0),  # result set index, always 0
    ]


def test_fetch_exact_request_at_zero_tuple_count() -> None:
    """Boundary: the first fetch of a result set (current_tuple_count == 0)."""
    packet = protocol.FetchPacket(query_handle=1, current_tuple_count=0, fetch_size=1)
    args = _arguments(packet.write(_CAS_INFO), CASFunctionCode.FETCH)
    assert args[1] == struct.pack(">i", 1)  # cursor_pos starts at 1, not 0


# ---------------------------------------------------------------------------
# FC1 END_TRAN (commit and rollback)
# ---------------------------------------------------------------------------
#
# fn_end_tran (11.4:162-179, 10.2:152-169) reads exactly one argument,
# tran_type, as a char via net_arg_get_char and rejects anything other than
# CCI_TRAN_COMMIT/CCI_TRAN_ROLLBACK.


def test_commit_exact_request() -> None:
    args = _arguments(protocol.CommitPacket().write(_CAS_INFO), CASFunctionCode.END_TRAN)
    assert args == [bytes((CCITransactionType.COMMIT,))]


def test_rollback_exact_request() -> None:
    args = _arguments(protocol.RollbackPacket().write(_CAS_INFO), CASFunctionCode.END_TRAN)
    assert args == [bytes((CCITransactionType.ROLLBACK,))]


# ---------------------------------------------------------------------------
# FC31 CON_CLOSE
# ---------------------------------------------------------------------------
#
# fn_con_close (11.4:2121-2127, 10.2:1998-2003) makes no net_arg_get_* call
# at all: the function code is the entire request.


def test_close_database_exact_request() -> None:
    args = _arguments(protocol.CloseDatabasePacket().write(_CAS_INFO), CASFunctionCode.CON_CLOSE)
    assert args == []


# ---------------------------------------------------------------------------
# FC6 CLOSE_REQ_HANDLE
# ---------------------------------------------------------------------------
#
# fn_close_req_handle (11.4:1082-1095, 10.2:988-1001) reads srv_h_id (int)
# from argv[0]; a second auto_commit_mode char argument is read only if
# argc > 1, and pycubrid never sends it.


def test_close_query_exact_request() -> None:
    packet = protocol.CloseQueryPacket(query_handle=42)
    args = _arguments(packet.write(_CAS_INFO), CASFunctionCode.CLOSE_REQ_HANDLE)
    assert args == [struct.pack(">i", 42)]


# ---------------------------------------------------------------------------
# FC15 GET_DB_VERSION
# ---------------------------------------------------------------------------
#
# fn_get_db_version (11.4:1273-1296, 10.2:1179-1202) reads exactly one char
# argument, auto_commit_mode.


@pytest.mark.parametrize("auto_commit", [True, False])
def test_get_engine_version_exact_request(auto_commit: bool) -> None:
    packet = protocol.GetEngineVersionPacket(auto_commit=auto_commit)
    args = _arguments(packet.write(_CAS_INFO), CASFunctionCode.GET_DB_VERSION)
    assert args == [b"\x01" if auto_commit else b"\x00"]


# ---------------------------------------------------------------------------
# FC9 SCHEMA_INFO
# ---------------------------------------------------------------------------
#
# fn_schema_info (11.4:1192-1226, 10.2:1098-1132) reads schema_type (int),
# arg1 (str), arg2 (str), flag (char), each via net_arg_get_str/_int/_char,
# then shard_id (int) only if the client advertises PROTOCOL_V5.
# net_arg_get_str (cas_net_buf.c 11.4:526-540, 10.2:529-543) treats a
# zero-or-negative length argument as NULL (``*value = NULL``) and anything
# with length > 0 — including a 1-byte "just the NUL terminator" argument
# for an empty Python string — as a real (non-NULL) string.


def test_get_schema_exact_request_with_table_pattern_and_shard_id() -> None:
    packet = protocol.GetSchemaPacket(
        CCISchemaType.CLASS,
        table_name="my_table",
        pattern_match_flag=1,
        arg2="pat%",
        protocol_version=5,
    )
    args = _arguments(packet.write(_CAS_INFO), CASFunctionCode.SCHEMA_INFO)
    assert args == [
        struct.pack(">i", CCISchemaType.CLASS),
        b"my_table\x00",
        b"pat%\x00",
        b"\x01",
        struct.pack(">i", 0),  # shard_id, sent only for protocol_version >= 5
    ]


def test_get_schema_exact_request_null_arg2_omits_shard_id_below_v5() -> None:
    packet = protocol.GetSchemaPacket(CCISchemaType.ATTRIBUTE, table_name="t", protocol_version=4)
    args = _arguments(packet.write(_CAS_INFO), CASFunctionCode.SCHEMA_INFO)
    assert args == [
        struct.pack(">i", CCISchemaType.ATTRIBUTE),
        b"t\x00",
        b"",  # arg2=None: zero-length NULL argument, not a string
        b"\x01",
    ]


def test_get_schema_exact_request_empty_table_name_is_not_null() -> None:
    """Boundary: table_name="" must send a non-NULL (length>0) argument."""
    packet = protocol.GetSchemaPacket(CCISchemaType.CLASS, table_name="", protocol_version=4)
    args = _arguments(packet.write(_CAS_INFO), CASFunctionCode.SCHEMA_INFO)
    assert args[1] == b"\x00"  # length 1 (NUL terminator only): not the NULL marker (b"")
    assert args[1] != b""


# ---------------------------------------------------------------------------
# FC20 EXECUTE_BATCH
# ---------------------------------------------------------------------------
#
# fn_execute_batch (11.4:1689-1711, 10.2:1587-1609) reads auto_commit_mode
# (char) first, then query_timeout (int) only if the client advertises
# PROTOCOL_V4; every remaining argument is one SQL statement string, read by
# ux_execute_batch from argv + arg_index.


def test_batch_execute_exact_request_below_v4_omits_timeout() -> None:
    packet = protocol.BatchExecutePacket(
        ["INSERT INTO t VALUES(1)", "INSERT INTO t VALUES(2)"],
        auto_commit=True,
        protocol_version=3,
    )
    args = _arguments(packet.write(_CAS_INFO), CASFunctionCode.EXECUTE_BATCH)
    assert args == [
        b"\x01",
        b"INSERT INTO t VALUES(1)\x00",
        b"INSERT INTO t VALUES(2)\x00",
    ]


def test_batch_execute_exact_request_at_v4_includes_timeout() -> None:
    packet = protocol.BatchExecutePacket(["SELECT 1"], auto_commit=False, protocol_version=4)
    args = _arguments(packet.write(_CAS_INFO), CASFunctionCode.EXECUTE_BATCH)
    assert args == [
        b"\x00",
        struct.pack(">i", 0),  # query timeout, sent only for protocol_version > 3
        b"SELECT 1\x00",
    ]


def test_batch_execute_exact_request_empty_statement_list() -> None:
    """Boundary: no SQL statements still sends the two fixed arguments."""
    packet = protocol.BatchExecutePacket([], auto_commit=False, protocol_version=4)
    args = _arguments(packet.write(_CAS_INFO), CASFunctionCode.EXECUTE_BATCH)
    assert args == [b"\x00", struct.pack(">i", 0)]


def test_batch_execute_rejects_unencodable_statement_before_any_bytes() -> None:
    """An unencodable statement raises before write() returns anything to send."""
    packet = protocol.BatchExecutePacket(["SELECT 1", "SELECT '\ud800'"])
    with pytest.raises(DataError):
        packet.write(_CAS_INFO)


# ---------------------------------------------------------------------------
# FC35/36/37 LOB_NEW, LOB_WRITE, LOB_READ
# ---------------------------------------------------------------------------
#
# fn_lob_new (11.4:2599-2625, 10.2:2445-2471) reads exactly one int,
# lob_type. fn_lob_write (11.4:2643-2666, 10.2:2489-2512) reads a LOB value
# (the packed handle), a bigint offset, then a str (the data). fn_lob_read
# (11.4:2685-2707, 10.2:2531-2553) reads the same handle and offset, then an
# int length instead of a str.


@pytest.mark.parametrize("lob_type", [CCILOBType.BLOB, CCILOBType.CLOB])
def test_lob_new_exact_request(lob_type: int) -> None:
    args = _arguments(protocol.LOBNewPacket(lob_type).write(_CAS_INFO), CASFunctionCode.LOB_NEW)
    assert args == [struct.pack(">i", lob_type)]


def test_lob_write_exact_request() -> None:
    handle = b"\x00\x00\x00\x21lob-locator-bytes"
    packet = protocol.LOBWritePacket(handle, offset=12345, data=b"payload-bytes")
    args = _arguments(packet.write(_CAS_INFO), CASFunctionCode.LOB_WRITE)
    assert args == [handle, struct.pack(">q", 12345), b"payload-bytes"]


def test_lob_write_exact_request_empty_data() -> None:
    """Boundary: a zero-length write still sends a (present, empty) data argument."""
    handle = b"handle"
    packet = protocol.LOBWritePacket(handle, offset=0, data=b"")
    args = _arguments(packet.write(_CAS_INFO), CASFunctionCode.LOB_WRITE)
    assert args == [handle, struct.pack(">q", 0), b""]


def test_lob_read_exact_request() -> None:
    handle = b"\x00\x00\x00\x22lob-locator-bytes"
    packet = protocol.LOBReadPacket(handle, offset=99, length=256)
    args = _arguments(packet.write(_CAS_INFO), CASFunctionCode.LOB_READ)
    assert args == [handle, struct.pack(">q", 99), struct.pack(">i", 256)]


# ---------------------------------------------------------------------------
# FC40 GET_LAST_INSERT_ID
# ---------------------------------------------------------------------------
#
# fn_get_last_insert_id (11.4:310-315, 10.2:287-292) calls
# ux_get_last_insert_id(net_buf) directly and makes no net_arg_get_* call:
# like CON_CLOSE, the function code is the entire request.


def test_get_last_insert_id_exact_request() -> None:
    packet = protocol.GetLastInsertIdPacket()
    args = _arguments(packet.write(_CAS_INFO), CASFunctionCode.GET_LAST_INSERT_ID)
    assert args == []


# ---------------------------------------------------------------------------
# FC4 GET_DB_PARAMETER / FC5 SET_DB_PARAMETER
# ---------------------------------------------------------------------------
#
# fn_get_db_parameter (11.4:871-883, 10.2:774-786) reads one int,
# param_name. fn_set_db_parameter (11.4:961-980, 10.2:867-?) reads
# param_name (int) then a second int whose meaning depends on param_name
# (isolation level, lock timeout, ...); pycubrid always sends exactly two
# ints regardless of which parameter is addressed.


@pytest.mark.parametrize(
    "parameter", [CCIDbParam.ISOLATION_LEVEL, CCIDbParam.LOCK_TIMEOUT, CCIDbParam.MAX_STRING_LENGTH]
)
def test_get_db_parameter_exact_request(parameter: int) -> None:
    packet = protocol.GetDbParameterPacket(parameter)
    args = _arguments(packet.write(_CAS_INFO), CASFunctionCode.GET_DB_PARAMETER)
    assert args == [struct.pack(">i", parameter)]


def test_set_db_parameter_exact_request() -> None:
    packet = protocol.SetDbParameterPacket(CCIDbParam.LOCK_TIMEOUT, 5000)
    args = _arguments(packet.write(_CAS_INFO), CASFunctionCode.SET_DB_PARAMETER)
    assert args == [
        struct.pack(">i", CCIDbParam.LOCK_TIMEOUT),
        struct.pack(">i", 5000),
    ]


def test_set_db_parameter_exact_request_negative_value() -> None:
    """Boundary: a negative value (e.g. lock_timeout=-1, infinite wait) round-trips as-is."""
    packet = protocol.SetDbParameterPacket(CCIDbParam.LOCK_TIMEOUT, -1)
    args = _arguments(packet.write(_CAS_INFO), CASFunctionCode.SET_DB_PARAMETER)
    assert args[1] == struct.pack(">i", -1)


# ---------------------------------------------------------------------------
# FC32 CHECK_CAS
# ---------------------------------------------------------------------------
#
# fn_check_cas (11.4:2130-2156, 10.2:2006-2032) branches on argc: with
# argc == 1 it reads a client message string; otherwise (pycubrid's case,
# argc == 0) it runs the lightweight ux_check_connection() ping without
# reading any argument.


def test_check_cas_exact_request() -> None:
    packet = protocol.CheckCasPacket()
    args = _arguments(packet.write(_CAS_INFO), CASFunctionCode.CHECK_CAS)
    assert args == []
