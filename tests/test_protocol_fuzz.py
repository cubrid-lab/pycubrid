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
"""

from __future__ import annotations

import struct
import sys
from collections.abc import Callable

import pytest
from hypothesis import given, settings, strategies as st

from pycubrid import protocol
from pycubrid.constants import DataSize
from pycubrid.exceptions import Error as DBAPIError

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


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
