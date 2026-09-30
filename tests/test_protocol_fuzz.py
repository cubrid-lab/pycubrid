"""Fuzz CAS response-packet parsing (issue #339).

The driver wraps ``packet.parse(response_body)`` in ``_send_and_receive`` and
catches exactly ``(ValueError, struct.error, IndexError, UnicodeDecodeError)``,
re-raising as :class:`OperationalError` ("malformed response from broker") and
invalidating the connection. Any exception a ``parse()`` raises *outside* that
caught set therefore leaks to the caller as a raw, non-DB-API exception — a
protocol-desynchronization / robustness defect.

These tests feed mutated broker responses straight into each response packet's
``parse()`` and assert the **leak contract**: whatever exception escapes must be
a member of the caught set (or the parse must succeed). A failure here surfaces a
real bug (e.g. ``decimal.InvalidOperation`` from NUMERIC parsing, issue #231, or
``OverflowError``/``MemoryError`` from an unchecked length).

``parse()`` receives ``data`` that begins after the 4-byte DATA_LENGTH prefix
(i.e. it starts with the 4-byte CAS_INFO), matching ``_send_and_receive``.

Realistic seeds (#523). The header-only seeds above never reach column metadata
or row cells, so the second half of this module seeds from well-formed replies
built by :mod:`tests.helpers.cas_reply`: execute replies (FC41, FC2, FC3) with
metadata for strings, numbers, NUMERIC, temporal and TZ types, BIT/VARBIT,
OIDs, collections, LOB handles and JSON; multi-row FETCH replies, including
CALL and NULL-typed layouts; and schema, batch and LOB replies. The oracle is
the documented contract:

* an unmutated seed decodes to exactly the values it was built from;
* a mutated reply either parses, raises a structural error (reported as
  ``OperationalError('malformed response from broker')``, session closed), a
  server error, or ``DataError`` only when the reply is complete (#383, #512, #543);
* a parsed FETCH reply never has cells whose declared sizes overrun it.

Mutations aim at framing: truncation at field and cell boundaries, length and
count words that disagree with their payload, collection element types and
counts, and dropped or duplicated cells. Example budgets come from the
Hypothesis profile (``pr`` per PR, ``nightly`` in the bug-hunt workflow); a
failure prints a ``@reproduce_failure`` blob and is kept in ``.hypothesis/``
for replay (#359).
"""

from __future__ import annotations

import asyncio
import datetime
import json
import struct
import sys
from collections.abc import Callable
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from hypothesis import given, note, settings, strategies as st

from pycubrid import protocol
from pycubrid.aio.connection import AsyncConnection
from pycubrid.connection import Connection
from pycubrid.constants import CUBRIDDataType, CUBRIDStatementType, DataSize
from pycubrid.exceptions import DataError, OperationalError
from pycubrid.exceptions import Error as DBAPIError
from tests.helpers import cas_reply
from tests.test_connection import build_handshake_response, build_open_db_response, make_socket
from tests.test_invalid_utf8_response import _async_connection_with_reply

# Two acceptable outcomes for parse() on malformed/hostile bytes:
#
# 1. A *structural* exception the sync/async ``_send_and_receive`` wrappers
#    catch around ``packet.parse()`` and convert to OperationalError:
STRUCTURAL_CAUGHT: tuple[type[BaseException], ...] = (
    ValueError,
    struct.error,
    IndexError,
    UnicodeDecodeError,
)
# 2. A proper DB-API error the parser raises deliberately (e.g. ``_raise_error``
#    turning a negative response_code into a ``DatabaseError``). These are
#    already DB-API compliant and pass straight through to the caller.
#
# Anything *else* — decimal.InvalidOperation, OverflowError, MemoryError,
# TypeError, KeyError, ... — is a raw non-DB-API leak and a real defect.
ACCEPTABLE: tuple[type[BaseException], ...] = STRUCTURAL_CAUGHT + (DBAPIError,)


def _cas_info(status: int = 1) -> bytes:
    """A 4-byte CAS_INFO block (first byte = status)."""
    return bytes([status, 0, 0, 0])


# ---------------------------------------------------------------------------
# Well-formed base responses per packet, then Hypothesis mutates the bytes.
# For "simple" packets the body is CAS_INFO(4) + response_code(4[+error]).
# ---------------------------------------------------------------------------


def _simple_ok() -> bytes:
    """CAS_INFO + response_code=0 (generic success for simple packets)."""
    return _cas_info() + struct.pack(">i", 0)


def _simple_error() -> bytes:
    """CAS_INFO + negative response_code + error payload."""
    # negative code triggers _raise_error(reader, remaining)
    body = _cas_info() + struct.pack(">i", -1)
    # error payload layout varies by protocol; just append a plausible blob
    body += struct.pack(">i", -1)  # inner errno
    body += struct.pack(">i", 3) + b"abc"  # message length + bytes
    return body


SIMPLE_PACKETS = [
    protocol.CommitPacket,
    protocol.RollbackPacket,
    protocol.CloseDatabasePacket,
    lambda: protocol.CloseQueryPacket(query_handle=1),
    protocol.GetEngineVersionPacket,
]


@st.composite
def _mutated(draw: st.DrawFn, data: bytes) -> bytes:
    """Apply one or more structural mutations to a byte response."""
    b = bytearray(data)
    n_ops = draw(st.integers(min_value=1, max_value=3))
    for _ in range(n_ops):
        op = draw(
            st.sampled_from(
                [
                    "truncate",
                    "extend",
                    "flip",
                    "zero_all",
                    "neg_code",
                    "huge_len_field",
                    "insert",
                    "delete",
                ]
            )
        )
        if not b and op in ("flip", "delete"):
            continue
        if op == "truncate":
            cut = draw(st.integers(min_value=0, max_value=len(b)))
            b = b[:cut]
        elif op == "extend":
            extra = draw(st.integers(min_value=1, max_value=64))
            b.extend(draw(st.binary(min_size=extra, max_size=extra)))
        elif op == "flip" and b:
            idx = draw(st.integers(min_value=0, max_value=len(b) - 1))
            b[idx] ^= draw(st.integers(min_value=1, max_value=255))
        elif op == "zero_all":
            b = bytearray(len(b))
        elif op == "neg_code" and len(b) >= 8:
            # Force the response_code (bytes 4:8) negative to drive error paths.
            b[4:8] = struct.pack(">i", -abs(draw(st.integers(min_value=1, max_value=2**31 - 1))))
        elif op == "huge_len_field" and len(b) >= 12:
            # Overwrite an interior int field with a huge/negative "length".
            pos = draw(st.integers(min_value=8, max_value=len(b) - 4))
            val = draw(st.sampled_from([2**31 - 1, -1, -(2**31), 2**30, 0]))
            b[pos : pos + 4] = struct.pack(">i", val)
        elif op == "insert":
            pos = draw(st.integers(min_value=0, max_value=len(b)))
            chunk = draw(st.binary(min_size=1, max_size=16))
            b[pos:pos] = chunk
        elif op == "delete" and b:
            pos = draw(st.integers(min_value=0, max_value=len(b) - 1))
            del b[pos]
    return bytes(b)


def _assert_leak_contract(parse_call: Callable[[], object]) -> None:
    """Run parse; any escaping exception must be within ACCEPTABLE."""
    try:
        parse_call()
    except ACCEPTABLE:
        return
    except BaseException as exc:  # noqa: BLE001 - we are asserting on the type
        raise AssertionError(
            f"parse() leaked a non-DB-API exception outside the caught set: "
            f"{type(exc).__module__}.{type(exc).__name__}: {exc!r}"
        ) from exc


class TestSimplePacketFuzz:
    @given(base=st.sampled_from(["ok", "error"]), data=st.data())
    @settings(deadline=None)
    def test_simple_packets_never_leak(self, base: str, data: st.DataObject) -> None:
        factory = data.draw(st.sampled_from(SIMPLE_PACKETS))
        seed = _simple_ok() if base == "ok" else _simple_error()
        mutated = data.draw(_mutated(seed))
        pkt = factory()
        _assert_leak_contract(lambda: pkt.parse(mutated))


class TestHandshakeFuzz:
    @given(data=st.binary(min_size=0, max_size=64))
    @settings(deadline=None)
    def test_client_info_exchange_short(self, data: bytes) -> None:
        """ClientInfoExchange.parse reads 4 bytes; sub-4-byte input must not leak."""
        pkt = protocol.ClientInfoExchangePacket()
        _assert_leak_contract(lambda: pkt.parse(data))

    @given(data=st.data())
    @settings(deadline=None)
    def test_open_database_never_leaks(self, data: st.DataObject) -> None:
        # Minimal well-formed-ish OpenDatabase response then mutate.
        seed = (
            _cas_info()
            + struct.pack(">i", 0)  # response_code ok
            + bytes(DataSize.BROKER_INFO)  # broker_info block
            + struct.pack(">i", 12345)  # session id
        )
        mutated = data.draw(_mutated(seed))
        pkt = protocol.OpenDatabasePacket(database="d", user="u", password="")
        _assert_leak_contract(lambda: pkt.parse(mutated))


class TestPrepareExecuteFuzz:
    @given(data=st.data(), decode_collections=st.booleans())
    @settings(deadline=None)
    def test_prepare_and_execute_never_leaks(
        self, data: st.DataObject, decode_collections: bool
    ) -> None:
        # A believable prepare+execute prefix; mutation drives it off the rails.
        seed = (
            _cas_info()
            + struct.pack(">i", 1)  # response_code / query_handle
            + struct.pack(">i", 0)  # result cache lifetime
            + bytes([0])  # statement_type
            + struct.pack(">i", 0)  # bind_count
            + bytes([0])  # is_updatable
            + struct.pack(">i", 0)  # column_count
            + struct.pack(">i", 0)  # total_tuple_count
            + bytes([0])  # cache_reusable
            + struct.pack(">i", 0)  # result_count
            + bytes([0])  # includes_column_info (proto > 1)
            + struct.pack(">i", 0)  # shard_id (proto > 4)
        )
        mutated = data.draw(_mutated(seed))
        pkt = protocol.PrepareAndExecutePacket(
            sql="SELECT 1", decode_collections=decode_collections
        )
        _assert_leak_contract(lambda: pkt.parse(mutated))

    @given(data=st.data())
    @settings(deadline=None)
    def test_prepare_never_leaks(self, data: st.DataObject) -> None:
        seed = (
            _cas_info()
            + struct.pack(">i", 1)
            + struct.pack(">i", 0)
            + bytes([0])
            + struct.pack(">i", 0)
            + bytes([0])
            + struct.pack(">i", 0)
        )
        mutated = data.draw(_mutated(seed))
        pkt = protocol.PreparePacket(sql="SELECT 1")
        _assert_leak_contract(lambda: pkt.parse(mutated))


class TestRawByteFloods:
    """Fully random byte blobs must never leak from any parser."""

    @given(blob=st.binary(min_size=0, max_size=512), data=st.data())
    @settings(deadline=None)
    def test_random_blob_into_simple(self, blob: bytes, data: st.DataObject) -> None:
        factory = data.draw(st.sampled_from(SIMPLE_PACKETS))
        pkt = factory()
        _assert_leak_contract(lambda: pkt.parse(blob))

    @given(blob=st.binary(min_size=0, max_size=512))
    @settings(deadline=None)
    def test_random_blob_into_prepare_execute(self, blob: bytes) -> None:
        pkt = protocol.PrepareAndExecutePacket(sql="SELECT 1", decode_collections=True)
        _assert_leak_contract(lambda: pkt.parse(blob))


# ---------------------------------------------------------------------------
# Realistic seeds: column metadata, populated rows, schema/batch/LOB (#523)
# ---------------------------------------------------------------------------


def _same(actual: object, expected: object) -> bool:
    """Exact equality that also pins types, float bits and time zones."""
    if type(actual) is not type(expected):
        return False
    if isinstance(expected, float):
        assert isinstance(actual, float)
        return struct.pack(">d", actual) == struct.pack(">d", expected)
    if isinstance(expected, datetime.datetime):
        assert isinstance(actual, datetime.datetime)
        return (
            actual == expected
            and actual.tzinfo == expected.tzinfo
            and actual.utcoffset() == expected.utcoffset()
            and actual.fold == expected.fold
        )
    if isinstance(expected, (list, tuple)):
        assert isinstance(actual, (list, tuple))
        return len(actual) == len(expected) and all(_same(a, e) for a, e in zip(actual, expected))
    if isinstance(expected, dict):
        assert isinstance(actual, dict)
        return actual.keys() == expected.keys() and all(
            _same(actual[k], expected[k]) for k in expected
        )
    return actual == expected


def _assert_rows(actual: list[tuple[Any, ...]], expected: list[tuple[Any, ...]]) -> None:
    assert len(actual) == len(expected)
    for row_index, (got, want) in enumerate(zip(actual, expected)):
        assert len(got) == len(want), f"row {row_index} width"
        for col_index, (g, w) in enumerate(zip(got, want)):
            assert _same(g, w), f"row {row_index} col {col_index}: {g!r} != {w!r}"


def _outcome(call: Callable[[], object]) -> BaseException | None:
    try:
        call()
    except Exception as exc:  # noqa: BLE001 - the oracle classifies it
        return exc
    return None


def _assert_documented(
    exc: BaseException | None, *, reply_complete: Callable[[], bool] | None = None
) -> None:
    """Only the documented outcomes of a parse may escape.

    * a structural error, which ``_send_and_receive`` reports as
      ``OperationalError('malformed response from broker')``;
    * a server-reported DB-API error (negative response code);
    * ``DataError`` for a value Python cannot represent, and only when the reply
      is complete (``reply_complete``, where the target can check it: FETCH
      replies, whose rows start at a fixed offset; execute replies share the
      same ``_parse_row_data`` completeness check).

    A structural error is not further classified, with one exception:
    ``json.JSONDecodeError`` is a ``ValueError`` subclass, so it would
    otherwise pass the ``STRUCTURAL_CAUGHT`` check below even though invalid
    JSON text in a complete reply (``json_deserializer=json.loads``) must be
    classified as ``DataError`` like any other unrepresentable value, not as
    a structural error (#543). It is checked first and explicitly rejected:
    a bare (unwrapped) ``JSONDecodeError`` escaping ``parse()`` is always a
    regression, never a documented outcome.
    """
    if isinstance(exc, json.JSONDecodeError):
        raise AssertionError(
            f"json.JSONDecodeError escaped parse() unwrapped instead of being "
            f"raised as DataError (#543): {exc!r}"
        ) from exc
    if exc is None or isinstance(exc, STRUCTURAL_CAUGHT):
        return
    if isinstance(exc, DBAPIError) and getattr(exc, "_cas_server_error", False):
        return
    if isinstance(exc, DataError):
        if reply_complete is not None:
            assert reply_complete(), f"DataError for a reply that is not complete: {exc!r}"
        return
    raise AssertionError(
        f"parse() raised an undocumented exception: "
        f"{type(exc).__module__}.{type(exc).__name__}: {exc!r}"
    ) from exc


_INT32 = st.integers(min_value=-(2**31), max_value=2**31 - 1)
_TYPE_BYTES = sorted({int(t) for t in CUBRIDDataType} | {0x20, 0x60, 0x80, 0xE8, 0xFF})


def _put_int(b: bytearray, pos: int, value: int) -> None:
    value = max(-(2**31), min(2**31 - 1, value))
    b[pos : pos + 4] = struct.pack(">i", value)


@st.composite
def _framing_mutation(draw: st.DrawFn, seed: cas_reply.Seed) -> bytes:
    """Aim at the fields framing bugs hide behind, then maybe add generic noise.

    ``length``: a cell, text or LOB length that disagrees with its payload;
    ``count``: a tuple/column/result/collection count that is off by one,
    zero, negative or huge; ``element_type``: a collection element type that
    does not match its elements; then at most one cut: truncation at (or one
    byte around) a field boundary, or dropping / duplicating whole fields.
    """
    b = bytearray(seed.data)
    rewrites = draw(st.lists(st.sampled_from(("length", "count", "element_type")), max_size=3))
    for op in rewrites:
        if op == "length" and seed.lengths:
            pos = draw(st.sampled_from(seed.lengths))
            old = struct.unpack_from(">i", b, pos)[0]
            new = draw(
                st.sampled_from([old - 1, old + 1, 0, -1, -2, old * 2, old + len(b)]) | _INT32
            )
            _put_int(b, pos, new)
        elif op == "count" and seed.counts:
            pos = draw(st.sampled_from(seed.counts))
            old = struct.unpack_from(">i", b, pos)[0]
            new = draw(st.sampled_from([old - 1, old + 1, 0, -1, 2**31 - 1, -(2**31)]))
            _put_int(b, pos, new)
        elif op == "element_type" and seed.element_types:
            pos = draw(st.sampled_from(seed.element_types))
            b[pos] = draw(st.sampled_from(_TYPE_BYTES))
    cut = draw(st.sampled_from(("none", "truncate", "drop", "duplicate")))
    if cut == "truncate" and seed.boundaries:
        pos = draw(st.sampled_from(seed.boundaries)) + draw(st.integers(-1, 1))
        b = b[: max(0, min(pos, len(b)))]
    elif cut in ("drop", "duplicate") and len(seed.boundaries) >= 2:
        start, end = sorted(
            draw(st.lists(st.sampled_from(seed.boundaries), min_size=2, max_size=2))
        )
        chunk = b[start:end]
        if cut == "drop":
            del b[start:end]
        else:
            b[start:start] = chunk
    if draw(st.booleans()):
        return draw(_mutated(bytes(b)))
    return bytes(b)


_Case = tuple[cas_reply.ResultSet, cas_reply.Seed]

_FETCH_SEEDS: list[_Case] = [(rs, cas_reply.fetch_reply(rs)) for rs in cas_reply.RESULT_SETS]
_PAE_SEEDS: list[_Case] = [
    (rs, cas_reply.prepare_and_execute_reply(rs)) for rs in cas_reply.RESULT_SETS
]
_EXECUTE_SEEDS: list[_Case] = [
    (rs, cas_reply.execute_reply(rs, refresh_columns=refresh))
    for rs in cas_reply.RESULT_SETS
    for refresh in (True, False)
]
_PREPARE_SEEDS: list[_Case] = [(rs, cas_reply.prepare_reply(rs)) for rs in cas_reply.RESULT_SETS]


def _seed_id(case: _Case) -> str:
    return case[1].name


def _cases(cases: list[_Case]) -> st.SearchStrategy[_Case]:
    """Draw a seed by name, so a falsifying example prints one short line."""
    by_name = {seed.name: (rs, seed) for rs, seed in cases}
    return st.sampled_from(sorted(by_name)).map(by_name.__getitem__)


def _fetch_packet(
    rs: cas_reply.ResultSet, *, decode_collections: bool, json_loads: bool
) -> protocol.FetchPacket:
    return protocol.FetchPacket(
        query_handle=7,
        current_tuple_count=0,
        columns=rs.metadata(),
        statement_type=rs.statement_type,
        decode_collections=decode_collections,
        json_deserializer=json.loads if json_loads else None,
    )


def _check_fetch(
    pkt: protocol.FetchPacket, reply: bytes, ncols: int, exc: BaseException | None
) -> None:
    """FETCH oracle: documented errors only; a parsed reply's cells fit inside it."""
    _assert_documented(exc, reply_complete=lambda: cas_reply.fetch_rows_fit(reply, ncols))
    if exc is None and ncols:
        assert cas_reply.fetch_rows_fit(reply, ncols), (
            "FETCH parsed a reply whose tuple count is negative or whose declared "
            "cell sizes overrun it"
        )
        assert len(pkt.rows) == pkt.tuple_count
        assert all(len(row) == ncols for row in pkt.rows)


# --- unmutated seeds decode exactly --------------------------------------------


_DECODE_MODES = [
    pytest.param(True, True, id="decoded-json"),
    pytest.param(True, False, id="decoded"),
    pytest.param(False, False, id="raw-collections"),
]


class TestRealisticSeedsRoundTrip:
    """Every seed the fuzzers mutate is itself a valid reply with known values."""

    @pytest.mark.parametrize("decode_collections, json_loads", _DECODE_MODES)
    @pytest.mark.parametrize("case", _FETCH_SEEDS, ids=_seed_id)
    def test_fetch_reply_round_trips(
        self,
        case: _Case,
        decode_collections: bool,
        json_loads: bool,
    ) -> None:
        rs, seed = case
        pkt = _fetch_packet(rs, decode_collections=decode_collections, json_loads=json_loads)
        pkt.parse(seed.data)
        assert pkt.tuple_count == len(rs.rows)
        expected = rs.expected(decode_collections=decode_collections, json_loads=json_loads)
        _assert_rows(pkt.rows, expected)
        assert cas_reply.fetch_rows_fit(seed.data, len(rs.columns))

    @pytest.mark.parametrize("decode_collections, json_loads", _DECODE_MODES)
    @pytest.mark.parametrize("case", _PAE_SEEDS, ids=_seed_id)
    def test_prepare_and_execute_reply_round_trips(
        self,
        case: _Case,
        decode_collections: bool,
        json_loads: bool,
    ) -> None:
        rs, seed = case
        pkt = protocol.PrepareAndExecutePacket(
            sql="SELECT 1",
            decode_collections=decode_collections,
            json_deserializer=json.loads if json_loads else None,
        )
        pkt.parse(seed.data)
        assert pkt.query_handle == 7
        assert pkt.statement_type == rs.statement_type
        assert pkt.bind_count == 0
        assert pkt.column_count == len(rs.columns)
        assert pkt.columns == rs.metadata()
        assert pkt.total_tuple_count == len(rs.rows)
        assert pkt.result_count == 1
        assert pkt.result_infos == cas_reply.expected_result_infos(rs)
        if rs.statement_type == CUBRIDStatementType.SELECT:
            assert pkt.tuple_count == len(rs.rows)
            expected = rs.expected(decode_collections=decode_collections, json_loads=json_loads)
            _assert_rows(pkt.rows, expected)
        else:
            # Only SELECT carries its first page inline; a CALL result is fetched.
            assert pkt.tuple_count == 0
            assert pkt.rows == []

    @pytest.mark.parametrize("case", _EXECUTE_SEEDS, ids=_seed_id)
    def test_execute_reply_round_trips(self, case: _Case) -> None:
        rs, seed = case
        refreshed = seed.name.startswith("execute_refreshed")
        pkt = protocol.ExecutePacket(
            query_handle=5, statement_type=rs.statement_type, decode_collections=True
        )
        pkt.parse(seed.data, columns=None if refreshed else rs.metadata())
        assert pkt.total_tuple_count == len(rs.rows)
        assert pkt.result_infos == cas_reply.expected_result_infos(rs)
        assert pkt.columns == rs.metadata()
        if refreshed:
            assert pkt.bind_count == 0
        if rs.statement_type == CUBRIDStatementType.SELECT:
            assert pkt.tuple_count == len(rs.rows)
            _assert_rows(pkt.rows, rs.expected(decode_collections=True, json_loads=False))
        else:
            assert pkt.rows == []

    @pytest.mark.parametrize("case", _PREPARE_SEEDS, ids=_seed_id)
    def test_prepare_reply_round_trips(self, case: _Case) -> None:
        rs, seed = case
        pkt = protocol.PreparePacket(sql="SELECT 1")
        pkt.parse(seed.data)
        assert pkt.query_handle == 5
        assert pkt.statement_type == rs.statement_type
        assert pkt.bind_count == 2
        assert pkt.column_count == len(rs.columns)
        assert pkt.columns == rs.metadata()

    def test_schema_reply_and_its_fetch_round_trip(self) -> None:
        rs = cas_reply.SCHEMA_RESULT
        schema = protocol.GetSchemaPacket(schema_type=1, table_name="fuzz_t")
        schema.parse(cas_reply.schema_reply(rs).data)
        assert schema.query_handle == 11
        assert schema.tuple_count == len(rs.rows)
        assert schema.columns == [c.schema() for c in rs.columns]
        fetch = protocol.FetchPacket(11, 0, columns=schema.columns)
        fetch.parse(cas_reply.fetch_reply(rs).data)
        _assert_rows(fetch.rows, rs.expected(decode_collections=True, json_loads=False))

    def test_batch_reply_round_trips(self) -> None:
        pkt = protocol.BatchExecutePacket(sql_list=["x"] * len(cas_reply.BATCH_STATEMENTS))
        pkt.parse(cas_reply.batch_reply().data)
        ok = [s for s in cas_reply.BATCH_STATEMENTS if s.result >= 0]
        failed = [s for s in cas_reply.BATCH_STATEMENTS if s.result < 0]
        assert pkt.results == [(s.statement_type, s.result) for s in ok]
        assert pkt.errors == [{"code": s.error_code, "message": s.message} for s in failed]

    def test_lob_replies_round_trip(self) -> None:
        new = protocol.LOBNewPacket(CUBRIDDataType.BLOB)
        new.parse(cas_reply.lob_new_reply().data)
        assert new.lob_handle == cas_reply.LOB_HANDLE.expected["packed_lob_handle"]

        read = protocol.LOBReadPacket(new.lob_handle, offset=0, length=64)
        read.parse(cas_reply.lob_read_reply().data)
        assert read.bytes_read == len(cas_reply.LOB_BYTES)
        assert read.lob_data == cas_reply.LOB_BYTES

        write = protocol.LOBWritePacket(new.lob_handle, offset=0, data=b"x" * 4096)
        write.parse(cas_reply.lob_write_reply().data)
        assert write.bytes_written == 4096


# --- mutated realistic seeds keep the documented contract ------------------------


class TestFetchReplyFuzz:
    """FETCH row-cell decoding against non-empty column metadata (#523)."""

    @given(
        case=_cases(_FETCH_SEEDS),
        decode_collections=st.booleans(),
        json_loads=st.booleans(),
        data=st.data(),
    )
    @settings(deadline=None)
    def test_fetch_reply_keeps_contract(
        self,
        case: _Case,
        decode_collections: bool,
        json_loads: bool,
        data: st.DataObject,
    ) -> None:
        rs, seed = case
        note(seed.name)
        reply = data.draw(_framing_mutation(seed))
        pkt = _fetch_packet(rs, decode_collections=decode_collections, json_loads=json_loads)
        _check_fetch(pkt, reply, len(rs.columns), _outcome(lambda: pkt.parse(reply)))

    @given(data=st.data())
    @settings(deadline=None)
    def test_schema_then_fetch_keeps_contract(self, data: st.DataObject) -> None:
        """A (possibly mutated) FC9 reply's columns drive a (possibly mutated) FETCH."""
        rs = cas_reply.SCHEMA_RESULT
        schema_reply = data.draw(_framing_mutation(cas_reply.schema_reply(rs)))
        schema = protocol.GetSchemaPacket(schema_type=1, table_name="fuzz_t")
        exc = _outcome(lambda: schema.parse(schema_reply))
        _assert_documented(exc)
        if exc is not None:
            return
        assert schema.tuple_count >= 0
        if schema.tuple_count:
            assert schema.columns
        fetch_reply = data.draw(_framing_mutation(cas_reply.fetch_reply(rs)))
        fetch = protocol.FetchPacket(11, 0, columns=schema.columns)
        _check_fetch(
            fetch, fetch_reply, len(schema.columns), _outcome(lambda: fetch.parse(fetch_reply))
        )


class TestExecuteReplyFuzz:
    """Execute replies with populated column metadata and inline rows (#523)."""

    @given(case=_cases(_PAE_SEEDS), decode_collections=st.booleans(), data=st.data())
    @settings(deadline=None)
    def test_prepare_and_execute_reply_keeps_contract(
        self,
        case: _Case,
        decode_collections: bool,
        data: st.DataObject,
    ) -> None:
        _, seed = case
        note(seed.name)
        reply = data.draw(_framing_mutation(seed))
        pkt = protocol.PrepareAndExecutePacket(
            sql="SELECT 1", decode_collections=decode_collections, json_deserializer=json.loads
        )
        exc = _outcome(lambda: pkt.parse(reply))
        _assert_documented(exc)
        if exc is None and pkt.tuple_count > 0 and pkt.rows:
            assert len(pkt.rows) == pkt.tuple_count
            assert all(len(row) == len(pkt.columns) for row in pkt.rows)

    @given(case=_cases(_EXECUTE_SEEDS), data=st.data())
    @settings(deadline=None)
    def test_execute_reply_keeps_contract(self, case: _Case, data: st.DataObject) -> None:
        rs, seed = case
        note(seed.name)
        reply = data.draw(_framing_mutation(seed))
        pkt = protocol.ExecutePacket(
            query_handle=5, statement_type=rs.statement_type, decode_collections=True
        )
        refreshed = seed.name.startswith("execute_refreshed")
        exc = _outcome(lambda: pkt.parse(reply, columns=None if refreshed else rs.metadata()))
        _assert_documented(exc)
        if exc is None and pkt.rows:
            assert len(pkt.rows) == pkt.tuple_count
            assert all(len(row) == len(pkt.columns) for row in pkt.rows)

    @given(case=_cases(_PREPARE_SEEDS), data=st.data())
    @settings(deadline=None)
    def test_prepare_reply_keeps_contract(self, case: _Case, data: st.DataObject) -> None:
        _, seed = case
        note(seed.name)
        reply = data.draw(_framing_mutation(seed))
        pkt = protocol.PreparePacket(sql="SELECT 1")
        exc = _outcome(lambda: pkt.parse(reply))
        _assert_documented(exc)
        if exc is None:
            assert pkt.bind_count >= 0
            assert pkt.column_count == len(pkt.columns)


class TestBatchAndLobReplyFuzz:
    @given(data=st.data())
    @settings(deadline=None)
    def test_batch_reply_keeps_contract(self, data: st.DataObject) -> None:
        reply = data.draw(_framing_mutation(cas_reply.batch_reply()))
        pkt = protocol.BatchExecutePacket(sql_list=["x"] * len(cas_reply.BATCH_STATEMENTS))
        _assert_documented(_outcome(lambda: pkt.parse(reply)))

    @given(
        make=st.sampled_from(
            [cas_reply.lob_new_reply, cas_reply.lob_read_reply, cas_reply.lob_write_reply]
        ),
        data=st.data(),
    )
    @settings(deadline=None)
    def test_lob_replies_keep_contract(
        self, make: Callable[[], cas_reply.Seed], data: st.DataObject
    ) -> None:
        seed = make()
        note(seed.name)
        reply = data.draw(_framing_mutation(seed))
        packets: dict[str, Any] = {
            "lob_new": protocol.LOBNewPacket(CUBRIDDataType.BLOB),
            "lob_read": protocol.LOBReadPacket(b"h", offset=0, length=64),
            "lob_write": protocol.LOBWritePacket(b"h", offset=0, data=b"x"),
        }
        pkt = packets[seed.name]
        exc = _outcome(lambda: pkt.parse(reply))
        _assert_documented(exc)
        if exc is None and seed.name == "lob_read":
            # A successful read returns exactly the bytes it counted (#383).
            assert len(pkt.lob_data) == max(pkt.bytes_read, 0)


# --- the same contract seen through the connection -----------------------------


def _sync_connection(reply: bytes) -> tuple[Connection, MagicMock]:
    open_db = build_open_db_response()
    sock = make_socket([build_handshake_response(), open_db[:4], open_db[4:]])
    with patch("socket.create_connection", return_value=sock):
        conn = Connection("localhost", 33000, "testdb", "dba", "")
    frame = struct.pack(">i", len(reply) - DataSize.CAS_INFO) + reply
    sock.recv.side_effect = [frame[:4], frame[4:8], frame[8:]]
    return conn, sock


class TestFetchThroughConnection:
    """What the caller sees: OperationalError (closed) or DataError (kept), never raw."""

    @given(
        case=_cases(_FETCH_SEEDS),
        use_async=st.booleans(),
        decode_collections=st.booleans(),
        json_loads=st.booleans(),
        data=st.data(),
    )
    @settings(deadline=None)
    def test_fetch_errors_reach_caller_as_documented(
        self,
        case: _Case,
        use_async: bool,
        decode_collections: bool,
        json_loads: bool,
        data: st.DataObject,
    ) -> None:
        rs, seed = case
        note(seed.name)
        reply = data.draw(_framing_mutation(seed))
        # Keep the reply framable: DATA_LENGTH covers at least CAS_INFO.
        reply = reply if len(reply) >= DataSize.CAS_INFO else seed.data[: DataSize.CAS_INFO]
        pkt = _fetch_packet(rs, decode_collections=decode_collections, json_loads=json_loads)
        conn: Connection | AsyncConnection
        if use_async:
            conn = _async_connection_with_reply(reply)
            exc = _outcome(lambda: asyncio.run(conn._send_and_receive(pkt)))
        else:
            conn, _ = _sync_connection(reply)
            exc = _outcome(lambda: conn._send_and_receive(pkt))
        ncols = len(rs.columns)
        if exc is None:
            assert conn._connected is True
            _check_fetch(pkt, reply, ncols, None)
        elif getattr(exc, "_cas_server_error", False):
            assert conn._connected is True  # a server-reported error keeps the session
        elif isinstance(exc, OperationalError):
            assert exc.msg == "malformed response from broker"
            assert isinstance(exc.__cause__, STRUCTURAL_CAUGHT)
            # json.JSONDecodeError is a ValueError, so it would otherwise pass
            # the check above even though it must be wrapped as DataError, not
            # left to close the connection as framing damage (#543).
            assert not isinstance(exc.__cause__, json.JSONDecodeError), (
                f"invalid JSON text closed the connection instead of raising "
                f"DataError (#543): {exc.__cause__!r}"
            )
            assert conn._connected is False
        elif isinstance(exc, DataError):
            assert conn._connected is True
            assert cas_reply.fetch_rows_fit(reply, ncols), "DataError for an incomplete reply"
        else:
            raise AssertionError(
                f"undocumented exception reached the caller: {type(exc).__name__}: {exc!r}"
            ) from exc


# --- fixed regressions the fuzzers now reach (#383, #523) ----------------------


def _last_cell_truncated(rs: cas_reply.ResultSet) -> bytes:
    """A FETCH reply cut one byte short of its last cell."""
    return cas_reply.fetch_reply(rs).data[:-1]


_LAST_CELLS = {
    "varbit": cas_reply.bits(b"\x01\x02\x03\xff"),
    "string": cas_reply.text("h\u00e9llo"),
    "numeric": cas_reply.numeric("12345.6789"),
    "json": cas_reply.json_("[1, 2]", [1, 2]),
    "blob": cas_reply.lob(CUBRIDDataType.BLOB, 10, "file:/cubrid/lob/t.0001"),
    "sequence": cas_reply.collection(
        CUBRIDDataType.SEQUENCE, CUBRIDDataType.INT, [cas_reply.int_(1), cas_reply.int_(2)]
    ),
}


def _id_then(name: str) -> cas_reply.ResultSet:
    """``SELECT id, v``: two rows, the last cell of the reply is a ``name`` value."""
    value = _LAST_CELLS[name]
    element = CUBRIDDataType.INT if name == "sequence" else 0
    return cas_reply.ResultSet(
        f"id_then_{name}",
        (
            cas_reply.Column("id", CUBRIDDataType.INT),
            cas_reply.Column("v", value.column_type, element_type=element),
        ),
        ((cas_reply.int_(1), value), (cas_reply.int_(2), value)),
    )


@pytest.mark.parametrize("decode_collections", [True, False])
@pytest.mark.parametrize("last_cell", sorted(_LAST_CELLS))
def test_truncated_cell_after_nonzero_column_metadata_is_malformed(
    last_cell: str, decode_collections: bool
) -> None:
    """Failing-first for #383/#523: pre-#533 these short cells decoded as complete values."""
    rs = _id_then(last_cell)
    pkt = _fetch_packet(rs, decode_collections=decode_collections, json_loads=False)
    with pytest.raises(STRUCTURAL_CAUGHT):
        pkt.parse(_last_cell_truncated(rs))


def test_truncated_cell_closes_the_connection_as_malformed() -> None:
    rs = _id_then("varbit")
    reply = _last_cell_truncated(rs)
    conn, sock = _sync_connection(reply)
    pkt = _fetch_packet(rs, decode_collections=True, json_loads=False)
    with pytest.raises(OperationalError, match="malformed response from broker"):
        conn._send_and_receive(pkt)
    assert conn._connected is False
    sock.close.assert_called()


def test_negative_cell_length_is_sql_null_and_keeps_row_alignment() -> None:
    """Every non-positive cell size is SQL NULL: nothing is read, the next cell stays aligned."""
    rs = cas_reply.NUMBERS
    seed = cas_reply.fetch_reply(rs)
    reply = bytearray(seed.data)
    first_cell_length = seed.lengths[0]  # row 1, c_short
    _put_int(reply, first_cell_length, -7)
    # The two payload bytes of the cell become part of the gap: drop them.
    del reply[first_cell_length + 4 : first_cell_length + 6]
    pkt = _fetch_packet(rs, decode_collections=True, json_loads=False)
    pkt.parse(bytes(reply))
    expected = rs.expected(decode_collections=True, json_loads=False)
    expected[0] = (None,) + expected[0][1:]
    _assert_rows(pkt.rows, expected)


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
