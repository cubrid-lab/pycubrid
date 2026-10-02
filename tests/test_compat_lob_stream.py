"""Stateful sync native LOB operations without changing ordinary ``Lob`` (#442)."""

from __future__ import annotations

import struct
from typing import Any

import pytest

from pycubrid.compat import native
from pycubrid.constants import CUBRIDDataType
from pycubrid.exceptions import (
    DatabaseError,
    InterfaceError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
)
from pycubrid.protocol import LOBNewPacket, LOBReadPacket, LOBWritePacket, _PreparedLob

from .test_compat_prepared import DSN, FakeDriver


class StreamDriver(FakeDriver):
    """A broker that owns byte-valued LOBs and caps each read at three bytes."""

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.values: dict[bytes, bytes] = {}
        self.next_write_count: int | None = None
        self.bad_new_handle = False
        self.discarded = False
        self.read_cap = 3
        self.read_plan: list[bytes | tuple[int, bytes] | Exception] = []
        self.drop_on_read_error = False
        self.fail_write_call: int | None = None
        self.write_calls = 0

    def _check_reconnect(self) -> bool:
        return False

    def _discard_uncertain_prepared_session(self) -> None:
        self.discarded = True
        self._connected = False

    def _send_and_receive(
        self,
        packet: Any,
        *,
        allow_reconnect: bool = True,
        expected_generation: int | None = None,
    ) -> Any:
        if not isinstance(packet, (LOBNewPacket, LOBReadPacket, LOBWritePacket)):
            return super()._send_and_receive(packet, expected_generation=expected_generation)
        assert allow_reconnect is False
        assert expected_generation == self._physical_generation
        self.requests.append((packet, expected_generation))
        if isinstance(packet, LOBNewPacket):
            if self.bad_new_handle:
                packet.lob_handle = b"damaged"
                return packet
            locator = f"native-{len(self.values)}".encode() + b"\x00"
            self.values[locator] = b""
            db_type = 33 if packet.lob_type == CUBRIDDataType.BLOB else 34
            packet.lob_handle = struct.pack(">iqi", db_type, 0, len(locator)) + locator
        elif isinstance(packet, LOBWritePacket):
            self.write_calls += 1
            if self.write_calls == self.fail_write_call:
                self.fail_write_call = None
                raise DatabaseError("second LOB_WRITE failed", errno=-1016)
            locator = packet.packed_lob_handle[16:]
            before = self.values[locator]
            assert packet.offset == len(before)
            count = self.next_write_count
            self.next_write_count = None
            packet.bytes_written = len(packet.data) if count is None else count
            self.values[locator] = before + packet.data[: packet.bytes_written]
        else:
            locator = packet.packed_lob_handle[16:]
            if self.read_plan:
                reply = self.read_plan.pop(0)
                if isinstance(reply, Exception):
                    if self.drop_on_read_error:
                        self._connected = False
                        self._socket = None
                    raise reply
                packet.bytes_read, packet.lob_data = (
                    reply if isinstance(reply, tuple) else (len(reply), reply)
                )
            else:
                packet.lob_data = self.values[locator][
                    packet.offset : packet.offset + min(packet.length, self.read_cap)
                ]
                packet.bytes_read = len(packet.lob_data)
        return packet


@pytest.fixture
def conn(monkeypatch: pytest.MonkeyPatch) -> native.connection:
    monkeypatch.setattr(native, "_DriverConnection", StreamDriver)
    owner = native.connect(DSN)
    try:
        yield owner
    finally:
        owner.close()


@pytest.mark.parametrize("escape_mode", [False, True])
def test_created_blob_writes_and_reads_from_a_byte_position(
    conn: native.connection, escape_mode: bool
) -> None:
    # LOB packets carry bytes/offsets, not SQL literals, under either mode.
    conn._driver._no_backslash_escapes = escape_mode
    lob = conn.lob()
    written = lob.write(b"abc")
    assert written is None
    assert lob._origin == native._CREATED
    position = lob.seek(0, native.SEEK_CUR)
    assert position == 3
    written = lob.write("def")
    assert written is None
    position = lob.seek(0, native.SEEK_SET)
    assert position == 0
    value = lob.read(2)
    assert value == "ab"
    position = lob.seek(0)  # default SEEK_CUR, no separate tell method
    assert position == 2
    value = lob.read()
    assert value == "cdef"
    value = lob.read()  # safe EOF, unlike the official CCI error
    assert value == ""
    position = lob.seek(1, native.SEEK_END)  # end minus offset
    assert position == 5
    value = lob.read(1)
    assert value == "f"


def test_clob_utf8_split_error_advances_the_byte_position(conn: native.connection) -> None:
    lob = conn.lob()
    written = lob.write("A한éB", "C")
    assert written is None
    position = lob.seek(0)
    assert position == 7
    position = lob.seek(1, native.SEEK_SET)
    assert position == 1
    with pytest.raises(UnicodeDecodeError):
        lob.read(1)
    position = lob.seek(0)
    assert position == 2
    position = lob.seek(0, native.SEEK_SET)
    assert position == 0
    value = lob.read()
    assert value == "A한éB"


def test_non_append_write_and_closed_stream_fail_before_io(conn: native.connection) -> None:
    empty = conn.lob()
    empty.seek(2, native.SEEK_SET)
    with pytest.raises(NotSupportedError, match="append"):
        empty.write(b"x")
    position = empty.seek(0)
    assert position == 2
    assert conn._driver.requests == []

    lob = conn.lob()
    lob.write(b"abc")
    lob.seek(1, native.SEEK_SET)
    count = len(conn._driver.requests)
    with pytest.raises(NotSupportedError, match="append"):
        lob.write(b"z")
    assert len(conn._driver.requests) == count
    position = lob.seek(0)
    assert position == 1
    lob.close()
    with pytest.raises(InterfaceError, match="closed"):
        lob.read()
    with pytest.raises(InterfaceError, match="closed"):
        lob.write(b"z")


def test_seek_past_end_is_a_virtual_position_without_a_hole_write(
    conn: native.connection,
) -> None:
    lob = conn.lob()
    lob.write(b"abc")
    position = lob.seek(100, native.SEEK_SET)
    assert position == 100
    value = lob.read()
    assert value == ""
    position = lob.seek(0)
    assert position == 100
    count = len(conn._driver.requests)
    with pytest.raises(NotSupportedError, match="append"):
        lob.write(b"z")
    assert len(conn._driver.requests) == count


def test_empty_write_creates_a_bindable_handle_without_write_io(conn: native.connection) -> None:
    lob = conn.lob()
    written = lob.write(b"", "C")
    assert written is None
    assert lob._lob_type == CUBRIDDataType.CLOB
    assert lob._origin == native._CREATED
    position = lob.seek(0, native.SEEK_END)
    assert position == 0
    value = lob.read()
    assert value == ""
    assert [type(packet) for packet, _ in conn._driver.requests] == [LOBNewPacket]
    cur = conn.cursor()
    cur.prepare("INSERT INTO t (c) VALUES (?)")
    cur.bind_lob(1, lob)
    assert isinstance(cur._bindings[0], _PreparedLob)


def test_invalid_values_fail_before_creating_or_reading_a_handle(conn: native.connection) -> None:
    lob = conn.lob()

    class HookedText(str):
        def encode(self, *args: Any, **kwargs: Any) -> bytes:
            raise AssertionError("user encode hook must not run")

    class HookedType(str):
        def upper(self) -> str:
            raise AssertionError("user type hook must not run")

    for value in (None, 1, bytearray(b"x")):
        with pytest.raises(TypeError):
            lob.write(value)
    with pytest.raises(TypeError):
        lob.write(HookedText("x"))
    with pytest.raises(TypeError):
        lob.write(b"x", HookedType("B"))
    with pytest.raises(ProgrammingError, match="lob type"):
        lob.write(b"x", "X")
    assert conn._driver.requests == []
    lob.write("한", "C")
    count = len(conn._driver.requests)
    for length in (-1, True, "1"):
        with pytest.raises(InterfaceError, match="read length"):
            lob.read(length)
    with pytest.raises(ProgrammingError, match="whence"):
        lob.seek(0, 9)
    with pytest.raises(InterfaceError, match="out of range"):
        lob.seek(-1, native.SEEK_SET)
    position = lob.seek(0)
    assert position == 3
    assert len(conn._driver.requests) == count


def test_stale_physical_session_cannot_read_write_seek_or_bind(conn: native.connection) -> None:
    lob = conn.lob()
    lob.write(b"abc")
    cur = conn.cursor()
    cur.prepare("INSERT INTO t (b) VALUES (?)")
    count = len(conn._driver.requests)
    conn._driver._physical_generation += 1
    for operation in (lob.read, lambda: lob.write(b"d"), lambda: lob.seek(0, native.SEEK_SET)):
        with pytest.raises(InterfaceError, match="earlier physical session"):
            operation()
    with pytest.raises(InterfaceError, match="earlier physical session"):
        cur.bind_lob(1, lob)
    assert len(conn._driver.requests) == count


def test_full_read_caps_wire_lengths_and_advances_only_received_bytes(
    conn: native.connection,
) -> None:
    lob = conn.lob()
    locator = b"native-huge\x00"
    conn._driver.values[locator] = b"xyz"
    packed = struct.pack(">iqi", 33, 1 << 32, len(locator)) + locator
    lob._set(
        CUBRIDDataType.BLOB,
        packed,
        native._FETCHED,
        (conn._driver, conn._driver._physical_generation),
        committed=True,
    )
    value = lob.read()
    assert value == "xyz"
    position = lob.seek(0)
    assert position == 3
    lengths = [
        packet.length for packet, _ in conn._driver.requests if isinstance(packet, LOBReadPacket)
    ]
    assert lengths and max(lengths) <= 64 * 1024


def test_binding_snapshots_the_size_before_a_later_append(conn: native.connection) -> None:
    lob = conn.lob()
    lob.write(b"abc")
    cur = conn.cursor()
    cur.prepare("INSERT INTO t (b) VALUES (?)")
    cur.bind_lob(1, lob)
    before = cur._bindings[0]
    assert isinstance(before, _PreparedLob)
    lob.write(b"d")
    assert struct.unpack_from(">q", before.packed_handle, 4)[0] == 3
    cur.bind_lob(1, lob)
    after = cur._bindings[0]
    assert isinstance(after, _PreparedLob)
    assert struct.unpack_from(">q", after.packed_handle, 4)[0] == 4


def test_short_write_keeps_confirmed_size_and_position(conn: native.connection) -> None:
    lob = conn.lob()
    conn._driver.next_write_count = 2
    with pytest.raises(OperationalError, match="LOB write truncated"):
        lob.write(b"abcd")
    assert lob._origin == native._CREATED
    position = lob.seek(0)
    assert position == 2
    assert struct.unpack_from(">q", lob._handle, 4)[0] == 2
    written = lob.write(b"cd")
    assert written is None
    position = lob.seek(0, native.SEEK_SET)
    assert position == 0
    value = lob.read()
    assert value == "abcd"


def test_new_handle_with_bad_framing_retires_the_session(conn: native.connection) -> None:
    conn._driver.bad_new_handle = True
    lob = conn.lob()
    with pytest.raises(OperationalError, match="malformed response from broker"):
        lob.write(b"x")
    assert conn._driver.discarded is True
    assert lob._handle is None


def test_replacing_driver_with_the_same_generation_cannot_reuse_handle(
    conn: native.connection,
) -> None:
    lob = conn.lob()
    lob.write(b"abc")
    old = conn._driver
    replacement = StreamDriver(autocommit=True)
    replacement._physical_generation = old._physical_generation
    conn._driver = replacement
    with pytest.raises(InterfaceError, match="earlier physical session"):
        lob.read()
    with pytest.raises(InterfaceError, match="earlier physical session"):
        lob.write(b"d")
    assert replacement.requests == []


def test_large_value_uses_bounded_write_and_read_requests(conn: native.connection) -> None:
    conn._driver.read_cap = 80_000
    lob = conn.lob()
    value = "A" * 70_000
    written = lob.write(value)
    assert written is None
    writes = [packet for packet, _ in conn._driver.requests if isinstance(packet, LOBWritePacket)]
    assert [len(packet.data) for packet in writes] == [65_536, 4_464]
    position = lob.seek(0, native.SEEK_SET)
    assert position == 0
    observed = lob.read()
    assert observed == value
    reads = [packet for packet, _ in conn._driver.requests if isinstance(packet, LOBReadPacket)]
    assert [packet.length for packet in reads] == [65_536, 4_464]


def test_later_chunk_server_error_keeps_earlier_confirmed_bytes(conn: native.connection) -> None:
    conn._driver.fail_write_call = 2
    lob = conn.lob()
    with pytest.raises(DatabaseError, match="second LOB_WRITE failed"):
        lob.write(b"A" * 70_000)
    position = lob.seek(0)
    assert position == 65_536
    assert struct.unpack_from(">q", lob._handle, 4)[0] == 65_536
    written = lob.write(b"z")
    assert written is None
    position = lob.seek(0)
    assert position == 65_537


def test_short_reply_then_server_error_keeps_position_and_resumes_at_next_byte(
    conn: native.connection,
) -> None:
    lob = conn.lob()
    lob.write(b"abcdef")
    lob.seek(0, native.SEEK_SET)
    conn._driver.read_plan = [b"abc", DatabaseError("read failed", errno=-1016), b"def"]
    with pytest.raises(DatabaseError, match="read failed"):
        lob.read(6)
    position = lob.seek(0)
    assert position == 3
    value = lob.read()
    assert value == "def"
    reads = [packet for packet, _ in conn._driver.requests if isinstance(packet, LOBReadPacket)]
    assert [packet.offset for packet in reads] == [0, 3, 3]


def test_short_reply_then_transport_error_preserves_prefix_but_retires_io(
    conn: native.connection,
) -> None:
    lob = conn.lob()
    lob.write(b"abcdef")
    lob.seek(0, native.SEEK_SET)
    conn._driver.read_plan = [b"abc", OperationalError("transport lost")]
    conn._driver.drop_on_read_error = True
    with pytest.raises(OperationalError, match="transport lost"):
        lob.read(6)
    assert lob._position == 3
    count = len(conn._driver.requests)
    with pytest.raises(InterfaceError, match="earlier physical session"):
        lob.read()
    assert len(conn._driver.requests) == count


def test_overlong_or_mismatched_read_reply_never_counts_its_own_bytes(
    conn: native.connection,
) -> None:
    lob = conn.lob()
    lob.write(b"abcdef")
    lob.seek(0, native.SEEK_SET)
    conn._driver.read_plan = [b"abc", (4, b"defg")]
    with pytest.raises(OperationalError, match="exceeding requested"):
        lob.read(6)
    assert lob._position == 3
    conn._driver.read_plan = [(2, b"def")]
    with pytest.raises(OperationalError, match="count does not match"):
        lob.read()
    assert lob._position == 3


def test_short_utf8_replies_decode_only_after_joining_accepted_bytes(
    conn: native.connection,
) -> None:
    lob = conn.lob()
    lob.write("A한B", "C")
    lob.seek(0, native.SEEK_SET)
    conn._driver.read_plan = [b"A\xed", b"\x95\x9cB"]
    value = lob.read()
    assert value == "A한B"
    assert lob._position == 5
