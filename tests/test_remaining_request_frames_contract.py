"""Exact wire-byte coverage for the remaining CAS request packets (#562).

Existing coverage inventoried before adding cases here (per #562's
acceptance criteria):

- ``test_prepared_packet_contract.py`` already freezes FC2/FC3
  (``PreparePacket`` and ``ExecutePacket`` scalar bindings, including NULL,
  empty strings and int32 boundaries) and ``test_prepared_collection_contract.py``
  already freezes the FC3 typed collection payload (#482). Reused via the
  ``_arguments()`` helper imported below, not duplicated.
- ``test_charset.py`` (``_GOLDEN_PREPARE_AND_EXECUTE``/``_GOLDEN_BATCH``/
  ``_GOLDEN_SCHEMA``, lines ~219-247) already freezes one full exact frame
  each for FC41, FC20 and FC9 — captured from a known-good run to pin #86
  charset regressions, not derived from CAS source. It exercises FC41 with
  zero deferred-close handles and FC20/FC9 at the default protocol version
  only, so it does not cover the #488 deferred-close id list or the
  protocol-version-gated fields below.
- ``test_schema_wire.py`` (``test_schema_request_nullable_strings_and_versions``
  and ``test_schema_request_default_version_and_shard``, lines ~83-116)
  already asserts exact FC9 bytes across ``protocol_version`` in
  ``[4, 5, 8]`` and every combination of ``None``/``""``/real-text
  ``arg1``/``arg2`` — the entire NULL/empty/shard-id matrix this file would
  otherwise re-cover. Its expected bytes are built with a hand-rolled
  ``_string()`` helper that mirrors the encoder's own NULL-vs-length-prefix
  logic rather than citing CAS source, so this file adds one CAS-source-cited
  FC9 case instead of repeating that matrix.

This module covers every other ``_CasPacket`` request class in
``pycubrid.protocol`` that only had function-code or partial-field checks in
``test_protocol.py``: FC41 (specifically the #488 deferred-close id list,
not already covered above), FETCH, END_TRAN, CON_CLOSE, CLOSE_REQ_HANDLE,
GET_DB_VERSION, one independently-sourced SCHEMA_INFO case, the
protocol-version boundary of EXECUTE_BATCH, LOB_NEW, LOB_WRITE, LOB_READ,
GET_LAST_INSERT_ID, GET_DB_PARAMETER, CHECK_CAS and SET_DB_PARAMETER. Two
invalid-input cases (an unencodable EXECUTE_BATCH statement and an
unencodable FC41 SQL string) assert that write() raises before returning
anything to send; the FC41 one goes further and drives the real
``Connection._send_and_receive`` path to assert ``sock.sendall`` is never
called and the session stays usable, the way
test_prepared_collection_contract.py's invalid-binding case does for FC3.

Expected bytes are derived independently from the CAS/CCI C source at the
commits already pinned by this repo for request-byte work (see
``docs/PREPARED_BINDING_DESIGN.md``): CUBRID/cubrid
``11.4 6b2bc75527c8bad94d9ad8aba961638efdfb3269`` and
``10.2 d56a158c06ee6ef917c9db7c52b778c63871bdd7`` (``src/broker/``), and
CUBRID/cubrid-cci ``7d1eb8f40f04089b8218d08e36e2c24a2de11b24``
(``src/cci/cas_cci.h``). Every function code and protocol constant used
below is therefore a plain ``int`` literal taken straight from that C
source — never imported from ``pycubrid.constants`` — so that a wrong value
in ``pycubrid.constants`` (which both the packet under test and a
same-named-enum expectation would otherwise share) cannot make a request
byte and its expectation drift together and still pass. The general
"argument = 4-byte big-endian length + payload" framing is the same one
``_arguments()`` (imported below) already decodes for the FC2/FC3 contract
tests; every CAS ``fn_*`` handler reads its arguments with the matching
``net_arg_get_*`` macro/function from ``cas_net_buf.h``.
"""

from __future__ import annotations

import struct

import pytest

from pycubrid import protocol
from pycubrid.exceptions import DataError

from .test_network_edge_cases import make_connected_connection
from .test_prepared_packet_contract import _CAS_INFO, _arguments

# CAS_FC_* request function codes, src/broker/cas_protocol.h (11.4:170-221;
# identical in 10.2). Plain ints, not pycubrid.constants.CASFunctionCode.
_FC_END_TRAN = 1
_FC_GET_DB_PARAMETER = 4
_FC_SET_DB_PARAMETER = 5
_FC_CLOSE_REQ_HANDLE = 6
_FC_FETCH = 8
_FC_SCHEMA_INFO = 9
_FC_GET_DB_VERSION = 15
_FC_EXECUTE_BATCH = 20
_FC_CON_CLOSE = 31
_FC_CHECK_CAS = 32
_FC_LOB_NEW = 35
_FC_LOB_WRITE = 36
_FC_LOB_READ = 37
_FC_GET_LAST_INSERT_ID = 40
_FC_PREPARE_AND_EXECUTE = 41


def _packed_lob_handle(db_type: int, lob_size: int, locator: bytes) -> bytes:
    """Build a realistic packed LOB handle.

    Mirrors net_arg_get_lob_handle's read order (cas_net_buf.c 11.4:733-757,
    10.2:736-760): db_type (int), lob_size (bigint), locator_size (int, the
    locator's byte length *including* its NUL terminator per
    net_buf_cp_lob_handle's own comment, 11.4:257-269), then the
    NUL-terminated locator itself. This is the payload LOBWritePacket/
    LOBReadPacket's ``packed_lob_handle`` argument carries unchanged from
    whatever LOBNewPacket.lob_handle the broker returned.
    """
    terminated = locator + b"\x00"
    return struct.pack(">iqi", db_type, lob_size, len(terminated)) + terminated


# ---------------------------------------------------------------------------
# FC41 PREPARE_AND_EXECUTE, including the #488 deferred-close id list
# ---------------------------------------------------------------------------
#
# cas_function.c fn_prepare_and_execute (11.4:843-867, 10.2:746-770) reads
# argv[0] as the prepare-argument count, hands fn_prepare_internal
# (11.4:325-439, 10.2:302-399) that many args starting at argv+1, then always
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
#
# test_charset.py's _GOLDEN_PREPARE_AND_EXECUTE already pins one such frame
# with zero deferred-close handles; the two tests below add the #488 case it
# doesn't cover.


def test_prepare_and_execute_exact_request_with_deferred_close_ids() -> None:
    packet = protocol.PrepareAndExecutePacket("SELECT * FROM t")
    packet.deferred_close_handles = (101, 202, 303)
    args = _arguments(packet.write(_CAS_INFO), _FC_PREPARE_AND_EXECUTE)
    assert args == [
        struct.pack(">i", 6),  # 3 fixed + 3 deferred handles
        b"SELECT * FROM t\x00",
        b"\x00",  # prepare flag: CCI_PREPARE_NORMAL
        b"\x00",  # auto_commit defaults to False
        struct.pack(">i", 101),
        struct.pack(">i", 202),
        struct.pack(">i", 303),
        b"\x02",  # execution option: CCI_EXEC_QUERY_ALL
        b"\x00" * 4,  # max_col_size
        b"\x00" * 4,  # max_row_size
        b"",  # NULL param_mode (zero-length arg, net_arg_get_str size<=0)
        b"\x00" * 8,  # cache time: sec=0, usec=0
        b"\x00" * 4,  # query timeout
    ]


def test_prepare_and_execute_exact_request_with_single_deferred_close_id() -> None:
    """One queued handle: the count field is 3+1 and exactly one int follows."""
    packet = protocol.PrepareAndExecutePacket("DELETE FROM t")
    packet.deferred_close_handles = (7,)
    args = _arguments(packet.write(_CAS_INFO), _FC_PREPARE_AND_EXECUTE)
    assert args[0] == struct.pack(">i", 4)
    assert args[4] == struct.pack(">i", 7)  # the deferred-close id itself
    assert args[5] == b"\x02"  # execution option follows immediately after


def test_prepare_and_execute_unencodable_sql_sends_nothing() -> None:
    """Invalid input through the real send path: no bytes ever reach the socket.

    ``write()`` raising before ``finalize()`` returns is necessary but not
    sufficient; this exercises the actual ``Connection._send_and_receive``
    path (like test_prepared_collection_contract.py's
    ``test_invalid_collection_binding_sends_nothing_and_keeps_session``) and
    asserts ``sock.sendall`` is never called and the session stays usable.
    """
    conn, sock = make_connected_connection()
    sock.sendall.reset_mock()
    generation = conn._physical_generation
    packet = protocol.PrepareAndExecutePacket("SELECT '\ud800'")
    with pytest.raises(DataError):
        conn._send_and_receive(packet, expected_generation=generation)
    sock.sendall.assert_not_called()
    assert conn._connected is True
    assert conn._physical_generation == generation


# ---------------------------------------------------------------------------
# FC8 FETCH
# ---------------------------------------------------------------------------
#
# fn_fetch (11.4:1144-1167, 10.2:1054-1077) reads exactly five arguments in
# order: srv_h_id (int), cursor_pos (int), fetch_count (int), fetch_flag
# (char), result_set_index (int).


def test_fetch_exact_request() -> None:
    packet = protocol.FetchPacket(query_handle=7, current_tuple_count=9, fetch_size=50)
    args = _arguments(packet.write(_CAS_INFO), _FC_FETCH)
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
    args = _arguments(packet.write(_CAS_INFO), _FC_FETCH)
    assert args[1] == struct.pack(">i", 1)  # cursor_pos starts at 1, not 0


# ---------------------------------------------------------------------------
# FC1 END_TRAN (commit and rollback)
# ---------------------------------------------------------------------------
#
# fn_end_tran (11.4:162-179, 10.2:152-169) reads exactly one argument,
# tran_type, as a char via net_arg_get_char and rejects anything other than
# CCI_TRAN_COMMIT(1)/CCI_TRAN_ROLLBACK(2) (src/cci/cas_cci.h:152-153).


def test_commit_exact_request() -> None:
    args = _arguments(protocol.CommitPacket().write(_CAS_INFO), _FC_END_TRAN)
    assert args == [b"\x01"]  # CCI_TRAN_COMMIT


def test_rollback_exact_request() -> None:
    args = _arguments(protocol.RollbackPacket().write(_CAS_INFO), _FC_END_TRAN)
    assert args == [b"\x02"]  # CCI_TRAN_ROLLBACK


# ---------------------------------------------------------------------------
# FC31 CON_CLOSE
# ---------------------------------------------------------------------------
#
# fn_con_close (11.4:2121-2127, 10.2:1998-2003) makes no net_arg_get_* call
# at all: the function code is the entire request.


def test_close_database_exact_request() -> None:
    args = _arguments(protocol.CloseDatabasePacket().write(_CAS_INFO), _FC_CON_CLOSE)
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
    args = _arguments(packet.write(_CAS_INFO), _FC_CLOSE_REQ_HANDLE)
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
    args = _arguments(packet.write(_CAS_INFO), _FC_GET_DB_VERSION)
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
#
# test_schema_wire.py already asserts this exact byte shape across every
# protocol_version/None/""/text combination; this is the one CAS-source-cited
# case this file adds (CCI_SCH_CLASS=1, src/cci/cas_cci.h:421).


def test_get_schema_exact_request_with_table_pattern_and_shard_id() -> None:
    packet = protocol.GetSchemaPacket(
        1,  # CCI_SCH_CLASS
        table_name="my_table",
        pattern_match_flag=1,
        arg2="pat%",
        protocol_version=5,
    )
    args = _arguments(packet.write(_CAS_INFO), _FC_SCHEMA_INFO)
    assert args == [
        struct.pack(">i", 1),  # CCI_SCH_CLASS
        b"my_table\x00",
        b"pat%\x00",
        b"\x01",
        struct.pack(">i", 0),  # shard_id, sent only for protocol_version >= 5
    ]


# ---------------------------------------------------------------------------
# FC20 EXECUTE_BATCH
# ---------------------------------------------------------------------------
#
# fn_execute_batch (11.4:1689-1711, 10.2:1587-1609) reads auto_commit_mode
# (char) first, then query_timeout (int) only if the client advertises
# PROTOCOL_V4; every remaining argument is one SQL statement string, read by
# ux_execute_batch from argv + arg_index. test_charset.py's _GOLDEN_BATCH
# already pins one frame at the default (>= PROTOCOL_V4) protocol version;
# the case below adds the omitted-timeout side of that boundary.


def test_batch_execute_exact_request_below_v4_omits_timeout() -> None:
    packet = protocol.BatchExecutePacket(
        ["INSERT INTO t VALUES(1)", "INSERT INTO t VALUES(2)"],
        auto_commit=True,
        protocol_version=3,
    )
    args = _arguments(packet.write(_CAS_INFO), _FC_EXECUTE_BATCH)
    assert args == [
        b"\x01",
        b"INSERT INTO t VALUES(1)\x00",
        b"INSERT INTO t VALUES(2)\x00",
    ]


def test_batch_execute_exact_request_empty_statement_list() -> None:
    """Boundary: no SQL statements still sends the two fixed arguments."""
    packet = protocol.BatchExecutePacket([], auto_commit=False, protocol_version=4)
    args = _arguments(packet.write(_CAS_INFO), _FC_EXECUTE_BATCH)
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
# lob_type, and rejects anything other than CCI_U_TYPE_BLOB(23)/
# CCI_U_TYPE_CLOB(24) (src/cci/cas_cci.h:357-358 — the production
# pycubrid.lob.Lob.create() path validates the same two values via
# CUBRIDDataType.BLOB/CLOB, pycubrid/lob.py:52-53; pycubrid.constants also
# defines an unrelated, unused CCILOBType with different numbers (33/34)
# that no production code path feeds into LOBNewPacket).  fn_lob_write
# (11.4:2643-2666, 10.2:2489-2512) reads a LOB value (the packed handle), a
# bigint offset, then a str (the data). fn_lob_read (11.4:2685-2707,
# 10.2:2531-2553) reads the same handle and offset, then an int length
# instead of a str.


@pytest.mark.parametrize("lob_type", [23, 24])  # CCI_U_TYPE_BLOB, CCI_U_TYPE_CLOB
def test_lob_new_exact_request(lob_type: int) -> None:
    args = _arguments(protocol.LOBNewPacket(lob_type).write(_CAS_INFO), _FC_LOB_NEW)
    assert args == [struct.pack(">i", lob_type)]


def test_lob_write_exact_request() -> None:
    handle = _packed_lob_handle(23, 1024, b"/cubrid_lob/550e8400-abc123")  # CCI_U_TYPE_BLOB
    packet = protocol.LOBWritePacket(handle, offset=12345, data=b"payload-bytes")
    args = _arguments(packet.write(_CAS_INFO), _FC_LOB_WRITE)
    assert args == [handle, struct.pack(">q", 12345), b"payload-bytes"]


def test_lob_write_exact_request_empty_data() -> None:
    """Boundary: a zero-length write still sends a (present, empty) data argument."""
    handle = _packed_lob_handle(24, 0, b"/cubrid_lob/empty-clob")  # CCI_U_TYPE_CLOB
    packet = protocol.LOBWritePacket(handle, offset=0, data=b"")
    args = _arguments(packet.write(_CAS_INFO), _FC_LOB_WRITE)
    assert args == [handle, struct.pack(">q", 0), b""]


def test_lob_read_exact_request() -> None:
    handle = _packed_lob_handle(24, 2048, b"/cubrid_lob/660f9511-def456")  # CCI_U_TYPE_CLOB
    packet = protocol.LOBReadPacket(handle, offset=99, length=256)
    args = _arguments(packet.write(_CAS_INFO), _FC_LOB_READ)
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
    args = _arguments(packet.write(_CAS_INFO), _FC_GET_LAST_INSERT_ID)
    assert args == []


# ---------------------------------------------------------------------------
# FC4 GET_DB_PARAMETER / FC5 SET_DB_PARAMETER
# ---------------------------------------------------------------------------
#
# fn_get_db_parameter (11.4:871-883, 10.2:774-786) reads one int,
# param_name. fn_set_db_parameter (11.4:961-1050, 10.2:867-956) reads
# param_name (int) then a second int whose meaning depends on param_name
# (isolation level, lock timeout, ...); pycubrid always sends exactly two
# ints regardless of which parameter is addressed. Parameter codes are
# CCI_PARAM_ISOLATION_LEVEL=1, CCI_PARAM_LOCK_TIMEOUT=2,
# CCI_PARAM_MAX_STRING_LENGTH=3 (src/cci/cas_cci.h:408-410).


@pytest.mark.parametrize("parameter", [1, 2, 3])
def test_get_db_parameter_exact_request(parameter: int) -> None:
    packet = protocol.GetDbParameterPacket(parameter)
    args = _arguments(packet.write(_CAS_INFO), _FC_GET_DB_PARAMETER)
    assert args == [struct.pack(">i", parameter)]


def test_set_db_parameter_exact_request() -> None:
    packet = protocol.SetDbParameterPacket(2, 5000)  # CCI_PARAM_LOCK_TIMEOUT
    args = _arguments(packet.write(_CAS_INFO), _FC_SET_DB_PARAMETER)
    assert args == [struct.pack(">i", 2), struct.pack(">i", 5000)]


def test_set_db_parameter_exact_request_negative_value() -> None:
    """Boundary: a negative value (e.g. lock_timeout=-1, infinite wait) round-trips as-is."""
    packet = protocol.SetDbParameterPacket(2, -1)  # CCI_PARAM_LOCK_TIMEOUT
    args = _arguments(packet.write(_CAS_INFO), _FC_SET_DB_PARAMETER)
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
    args = _arguments(packet.write(_CAS_INFO), _FC_CHECK_CAS)
    assert args == []
