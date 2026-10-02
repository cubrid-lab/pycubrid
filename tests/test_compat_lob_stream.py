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
    assert lob.write(b"abc") is None
    assert lob._origin == native._CREATED
    assert lob.seek(0, native.SEEK_CUR) == 3
    assert lob.write("def") is None
    assert lob.seek(0, native.SEEK_SET) == 0
    assert lob.read(2) == "ab"
    assert lob.seek(0) == 2  # default SEEK_CUR, no separate tell method
    assert lob.read() == "cdef"
    assert lob.read() == ""  # safe EOF, unlike the official CCI error
    assert lob.seek(1, native.SEEK_END) == 5  # end minus offset
    assert lob.read(1) == "f"


def test_clob_utf8_split_error_advances_the_byte_position(conn: native.connection) -> None:
    lob = conn.lob()
    assert lob.write("A한éB", "C") is None
    assert lob.seek(0) == 7
    assert lob.seek(1, native.SEEK_SET) == 1
    with pytest.raises(UnicodeDecodeError):
        lob.read(1)
    assert lob.seek(0) == 2
    assert lob.seek(0, native.SEEK_SET) == 0
    assert lob.read() == "A한éB"


def test_non_append_write_and_closed_stream_fail_before_io(conn: native.connection) -> None:
    empty = conn.lob()
    empty.seek(2, native.SEEK_SET)
    with pytest.raises(NotSupportedError, match="append"):
        empty.write(b"x")
    assert empty.seek(0) == 2
    assert conn._driver.requests == []

    lob = conn.lob()
    lob.write(b"abc")
    lob.seek(1, native.SEEK_SET)
    count = len(conn._driver.requests)
    with pytest.raises(NotSupportedError, match="append"):
        lob.write(b"z")
    assert len(conn._driver.requests) == count
    assert lob.seek(0) == 1
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
    assert lob.seek(100, native.SEEK_SET) == 100
    assert lob.read() == ""
    assert lob.seek(0) == 100
    count = len(conn._driver.requests)
    with pytest.raises(NotSupportedError, match="append"):
        lob.write(b"z")
    assert len(conn._driver.requests) == count


def test_empty_write_creates_a_bindable_handle_without_write_io(conn: native.connection) -> None:
    lob = conn.lob()
    assert lob.write(b"", "C") is None
    assert lob._lob_type == CUBRIDDataType.CLOB
    assert lob._origin == native._CREATED
    assert lob.seek(0, native.SEEK_END) == 0
    assert lob.read() == ""
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
    assert lob.seek(0) == 3
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
    assert lob.read() == "xyz"
    assert lob.seek(0) == 3
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
    assert lob.seek(0) == 2
    assert struct.unpack_from(">q", lob._handle, 4)[0] == 2
    assert lob.write(b"cd") is None
    assert lob.seek(0, native.SEEK_SET) == 0
    assert lob.read() == "abcd"


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
    assert lob.write(value) is None
    writes = [packet for packet, _ in conn._driver.requests if isinstance(packet, LOBWritePacket)]
    assert [len(packet.data) for packet in writes] == [65_536, 4_464]
    assert lob.seek(0, native.SEEK_SET) == 0
    assert lob.read() == value
    reads = [packet for packet, _ in conn._driver.requests if isinstance(packet, LOBReadPacket)]
    assert [packet.length for packet in reads] == [65_536, 4_464]


def test_later_chunk_server_error_keeps_earlier_confirmed_bytes(conn: native.connection) -> None:
    conn._driver.fail_write_call = 2
    lob = conn.lob()
    with pytest.raises(DatabaseError, match="second LOB_WRITE failed"):
        lob.write(b"A" * 70_000)
    assert lob.seek(0) == 65_536
    assert struct.unpack_from(">q", lob._handle, 4)[0] == 65_536
    assert lob.write(b"z") is None
    assert lob.seek(0) == 65_537
