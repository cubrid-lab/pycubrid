"""Public sync ``compat.native`` collection binding: ``set()``/``imports()``/``bind_set()`` (#440)."""

from __future__ import annotations

from typing import Any

import pytest
from hypothesis import given, settings, strategies as st

import pycubrid.types
from pycubrid.compat import native
from pycubrid.constants import CUBRIDDataType
from pycubrid.exceptions import (
    DataError,
    InterfaceError,
    NotSupportedError,
    ProgrammingError,
)
from pycubrid.protocol import ExecutePacket, _PreparedCollection, _PreparedScalar

from .test_compat_prepared import DSN, FakeDriver, _owner, _packets

SET, MULTISET, SEQUENCE = CUBRIDDataType.SET, CUBRIDDataType.MULTISET, CUBRIDDataType.SEQUENCE
CHAR, STRING, INT = CUBRIDDataType.CHAR, CUBRIDDataType.STRING, CUBRIDDataType.INT

# FC3 bind pairs ([int32 1][u_type][int32 len][value]) sent by the pinned official
# driver (cubrid-python e75ec36, CCI 7d1eb8f) for `s.imports(data, type)` then
# `cur.bind_set(1, s)`, captured through a TCP proxy in front of a CUBRID 11.4
# broker. Every element is STRING (2) whatever `type` is; the kind is SET (16).
OFFICIAL_BIND_PAIRS = [
    (("1", "-20", "0"), INT, "00000001100000001502000000023100000000042d323000000000023000"),
    # The official call imported ("b", "a", "한", "NULL"): its "NULL" sentinel
    # is pycubrid's None, and both send a length-0 NULL element.
    (
        ("b", "a", "한", None),
        STRING,
        "0000000110000000190200000002620000000002610000000004ed959c0000000000",
    ),
    ((), INT, "00000001100000000102"),
]


@pytest.fixture
def fake_driver(monkeypatch: pytest.MonkeyPatch) -> FakeDriver:
    FakeDriver.created.clear()
    monkeypatch.setattr(native, "_DriverConnection", FakeDriver)
    conn = native.connect(DSN)
    conn._driver._native_connection = conn
    return conn._driver


def _bind_pair(binding: _PreparedScalar | _PreparedCollection) -> bytes:
    payload = binding.payload
    return (
        b"\x00\x00\x00\x01"
        + bytes((binding.type_code,))
        + len(payload).to_bytes(4, "big")
        + payload
    )


def _imported(conn: native.connection, data: tuple[Any, ...], type_: int, **kw: Any) -> Any:
    s = conn.set()
    s.imports(data, type_, **kw)
    return s


@pytest.mark.parametrize(("data", "type_", "pair"), OFFICIAL_BIND_PAIRS)
def test_default_kind_is_byte_identical_to_the_official_driver(
    fake_driver: FakeDriver, data: tuple[Any, ...], type_: int, pair: str
) -> None:
    conn, cur = _owner(fake_driver)
    try:
        cur.prepare("INSERT INTO t VALUES (?)")
        cur.bind_set(1, _imported(conn, data, type_))
        cur.execute()
        packet = _packets(fake_driver, ExecutePacket)[0]
        assert _bind_pair(packet.bindings[0]) == bytes.fromhex(pair)
        # The pair is what the request frame carries.
        frame = packet.write(b"\x00" * 4)
        assert bytes.fromhex(pair) in frame
    finally:
        conn.close()


def test_python_int_elements_send_the_official_digit_string_bytes(
    fake_driver: FakeDriver,
) -> None:
    conn, _cur = _owner(fake_driver)
    try:
        as_int = _imported(conn, (1, -20, 0), INT)._binding
        as_text = _imported(conn, ("1", "-20", "0"), INT)._binding
        assert as_int == as_text
        assert _bind_pair(as_int) == bytes.fromhex(OFFICIAL_BIND_PAIRS[0][2])
    finally:
        conn.close()


@pytest.mark.parametrize(
    ("kind", "wire"), [(SET, SET), (MULTISET, SEQUENCE), (SEQUENCE, SEQUENCE), (16, SET)]
)
@pytest.mark.parametrize(
    "type_",
    [
        CHAR,
        STRING,
        INT,
        CUBRIDDataType.NUMERIC,
        CUBRIDDataType.DATE,
        CUBRIDDataType.DATETIME,
        CUBRIDDataType.BIGINT,
        0,
        99,
    ],
)
def test_kind_and_element_type_on_the_wire(
    fake_driver: FakeDriver, kind: int, wire: int, type_: int
) -> None:
    conn, _cur = _owner(fake_driver)
    try:
        binding = _imported(conn, ("7", None, "7"), type_, kind=kind)._binding
        # MULTISET is never sent: the broker rejects it with -454.
        assert binding.type_code == wire
        assert binding.element_type == STRING
        assert binding.elements == (b"7\x00", None, b"7\x00")
    finally:
        conn.close()


def test_int_and_digit_string_elements_may_be_mixed(fake_driver: FakeDriver) -> None:
    conn, _cur = _owner(fake_driver)
    try:
        mixed = _imported(conn, (1, "2", None), INT)._binding
        assert mixed == _imported(conn, ("1", "2", None), INT)._binding
    finally:
        conn.close()


def test_str_subclass_cannot_choose_the_encoded_bytes(fake_driver: FakeDriver) -> None:
    class Hostile(str):
        def encode(self, *args: Any, **kwargs: Any) -> bytes:  # pragma: no cover - must not run
            raise TypeError("hostile encode")

        def __str__(self) -> str:  # pragma: no cover - must not run
            return "other"

    conn, _cur = _owner(fake_driver)
    try:
        binding = _imported(conn, (Hostile("a"),), STRING)._binding
        assert binding.elements == (b"a\x00",)
    finally:
        conn.close()


def test_null_empty_and_literal_null_elements_are_kept(fake_driver: FakeDriver) -> None:
    conn, _cur = _owner(fake_driver)
    try:
        binding = _imported(conn, (None, "", "NULL", "한"), STRING, kind=SEQUENCE)._binding
        assert binding.elements == (None, b"\x00", b"NULL\x00", "한".encode() + b"\x00")
        assert _imported(conn, (), STRING)._binding.payload == b"\x02"
    finally:
        conn.close()


@pytest.mark.parametrize(
    ("data", "type_", "kind", "error"),
    [
        (["1"], INT, SET, InterfaceError),
        ("12", INT, SET, InterfaceError),
        (None, INT, SET, InterfaceError),
        (("1",), CUBRIDDataType.BIT, SET, NotSupportedError),
        (("1",), CUBRIDDataType.VARBIT, SET, NotSupportedError),
        (("1",), 5, SET, NotSupportedError),
        (("1",), True, SET, InterfaceError),
        (("1",), "8", SET, InterfaceError),
        (("1",), 8.0, SET, InterfaceError),
        ((1,), CUBRIDDataType.NUMERIC, SET, ProgrammingError),
        ((1,), CUBRIDDataType.BIGINT, SET, ProgrammingError),
        (("1",), INT, CUBRIDDataType.OBJECT, ProgrammingError),
        (("1",), INT, True, ProgrammingError),
        (("1",), INT, 16.0, ProgrammingError),
        ((True,), INT, SET, ProgrammingError),
        ((1.5,), INT, SET, ProgrammingError),
        ((b"1",), INT, SET, ProgrammingError),
        ((("1",),), INT, SET, ProgrammingError),
        ((frozenset({"1"}),), STRING, SET, ProgrammingError),
        ((1,), STRING, SET, ProgrammingError),
        ((1,), CHAR, SET, ProgrammingError),
        (("a\x00b",), STRING, SET, ProgrammingError),
        (("\ud800",), STRING, SET, DataError),
        ((2**63,), INT, SET, DataError),
        ((-(2**63) - 1,), INT, SET, DataError),
        ((10**5000,), INT, SET, DataError),
    ],
)
def test_invalid_imports_raise_and_keep_the_previous_value(
    fake_driver: FakeDriver, data: Any, type_: Any, kind: Any, error: type[Exception]
) -> None:
    conn, _cur = _owner(fake_driver)
    try:
        s = _imported(conn, ("keep",), STRING)
        before = s._binding
        with pytest.raises(error) as caught:
            s.imports(data, type_, kind=kind)
        assert s._binding is before
        # Messages never echo the value.
        assert "a\x00b" not in str(caught.value) and "keep" not in str(caught.value)
        assert not fake_driver.requests
    finally:
        conn.close()


def test_euckr_connection_encodes_elements_with_its_charset(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(native, "_DriverConnection", FakeDriver)
    conn = native.connect(DSN)
    conn._driver._encoding = "euc_kr"
    try:
        s = _imported(conn, ("한",), STRING)
        assert s._binding.elements == ("한".encode("euc_kr") + b"\x00",)
        with pytest.raises(DataError):
            s.imports(("똠",), STRING)  # Outside KS X 1001: an 8-byte makeup sequence.
    finally:
        conn.close()


def test_set_imported_for_another_charset_cannot_be_bound(
    fake_driver: FakeDriver, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn, cur = _owner(fake_driver)
    try:
        cur.prepare("INSERT INTO t VALUES (?)")
        s = _imported(conn, ("a",), STRING)
        fake_driver._encoding = "euc_kr"
        with pytest.raises(ProgrammingError):
            cur.bind_set(1, s)
        assert cur._bindings == [None]
    finally:
        conn.close()


def test_bind_set_snapshots_the_value_and_reexecutes_on_one_handle(
    fake_driver: FakeDriver,
) -> None:
    conn, cur = _owner(fake_driver)
    try:
        cur.prepare("INSERT INTO t VALUES (?, ?)")
        s = _imported(conn, (3, 1, 3), INT, kind=MULTISET)
        cur.bind_param(1, 1)
        cur.bind_set(2, s)
        s.imports(("x",), STRING)  # A later import does not change the bound value.
        cur.execute()
        cur.bind_param(1, 2)
        cur.bind_set(2, s)
        cur.execute()
        first, second = (p.bindings[1] for p in _packets(fake_driver, ExecutePacket))
        assert (first.type_code, first.elements) == (SEQUENCE, (b"3\x00", b"1\x00", b"3\x00"))
        assert (second.type_code, second.elements) == (SET, (b"x\x00",))
        assert len({p.query_handle for p in _packets(fake_driver, ExecutePacket)}) == 1
    finally:
        conn.close()


def test_unimported_set_binds_sql_null_like_the_official_driver(fake_driver: FakeDriver) -> None:
    conn, cur = _owner(fake_driver)
    try:
        cur.prepare("INSERT INTO t VALUES (?)")
        cur.bind_set(1, conn.set())
        cur.execute()
        bound = _packets(fake_driver, ExecutePacket)[0].bindings[0]
        assert (bound.type_code, bound.payload) == (CUBRIDDataType.NULL, b"")
    finally:
        conn.close()


@pytest.mark.parametrize(
    ("index", "value", "error"),
    [
        (1, ("1",), InterfaceError),
        (1, None, InterfaceError),
        (1, pycubrid.types.Set([1]), InterfaceError),
        (1, frozenset({"1"}), InterfaceError),
        (0, "set", ProgrammingError),
        (2, "set", ProgrammingError),
        (True, "set", ProgrammingError),
    ],
)
def test_bad_bind_set_raises_without_io_or_replacing_the_slot(
    fake_driver: FakeDriver, index: Any, value: Any, error: type[Exception]
) -> None:
    conn, cur = _owner(fake_driver)
    try:
        cur.prepare("INSERT INTO t VALUES (?)")
        cur.bind_param(1, 5)
        before = list(cur._bindings)
        requests = len(fake_driver.requests)
        if value == "set":
            value = _imported(conn, ("1",), INT)
        with pytest.raises(error):
            cur.bind_set(index, value)
        assert cur._bindings == before
        assert len(fake_driver.requests) == requests
    finally:
        conn.close()


def test_bind_set_follows_cursor_and_session_ownership(fake_driver: FakeDriver) -> None:
    conn, cur = _owner(fake_driver)
    s = _imported(conn, ("1",), INT)
    with pytest.raises(InterfaceError):
        cur.bind_set(1, s)  # No prepared statement yet.
    cur.prepare("INSERT INTO t VALUES (?)")
    fake_driver._physical_generation += 1
    with pytest.raises(InterfaceError):
        cur.bind_set(1, s)
    cur.close()
    with pytest.raises(InterfaceError):
        cur.bind_set(1, s)
    conn.close()
    with pytest.raises(InterfaceError):
        conn.set()
    with pytest.raises(InterfaceError):
        native.set(conn)
    with pytest.raises(InterfaceError):
        native.set(object())
    # The value object holds no server resource and stays importable.
    s.imports(("2",), INT)


def test_set_constructor_matches_connection_factory(fake_driver: FakeDriver) -> None:
    conn, _cur = _owner(fake_driver)
    try:
        assert type(conn.set()) is native.set
        direct = native.set(conn)
        direct.imports(("a",), STRING)
        assert direct._binding == _imported(conn, ("a",), STRING)._binding
    finally:
        conn.close()


_ELEMENT = st.one_of(
    st.none(), st.integers(-(2**63), 2**63 - 1), st.text().filter(lambda t: "\x00" not in t)
)


@settings(max_examples=200, deadline=None)
@given(
    values=st.lists(_ELEMENT, max_size=8),
    kind=st.sampled_from([SET, MULTISET, SEQUENCE]),
)
def test_framing_is_exact_and_matches_the_decimal_text(values: list[Any], kind: int) -> None:
    s = native.set.__new__(native.set)
    s._encoding = "utf-8"
    s._binding = None
    try:
        s.imports(tuple(values), INT, kind=kind)
    except DataError:
        # Lone surrogates cannot be encoded; nothing was imported.
        assert any(isinstance(v, str) for v in values)
        assert s._binding is None
        return
    binding = s._binding
    assert binding.type_code != MULTISET
    payload = binding.payload
    assert payload[0] == STRING
    offset, decoded = 1, []
    while offset < len(payload):
        size = int.from_bytes(payload[offset : offset + 4], "big", signed=True)
        offset += 4
        decoded.append(None if size == 0 else payload[offset : offset + size - 1].decode())
        offset += size
    assert offset == len(payload)
    assert decoded == [None if v is None else str(v) for v in values]
