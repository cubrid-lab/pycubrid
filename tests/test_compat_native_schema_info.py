"""Native ``connection.schema_info()`` first-row contract over real FC9/FC8/FC6 parsers (#1011)."""

from __future__ import annotations

import struct
from typing import Any

import pytest

from pycubrid.compat import native
from pycubrid.constants import CCISchemaType
from pycubrid.constants import CUBRIDDataType as T
from pycubrid.exceptions import (
    DatabaseError,
    DataError,
    InterfaceError,
    NotSupportedError,
    OperationalError,
)
from pycubrid.protocol import FetchPacket, GetSchemaPacket

from .helpers.cas_reply import Column, ResultSet, fetch_reply, int_, schema_reply, short, text
from .helpers.replay_broker import IN_TRAN, OUT_TRAN, error_body, ok_body, split_args, with_status
from . import test_compat_native_utilities as _utilities
from .test_compat_native_utilities import _codes, _guarded, _load
from .test_network_edge_cases import make_socket_from_chunks

owned = _utilities.owned  # the shared socket-fake native owner fixture

_CLASS_COLUMNS = (
    Column("NAME", T.STRING, precision=255),
    Column("TYPE", T.SHORT),
    Column("REMARKS", T.STRING, precision=2048),
)
_ATTRIBUTE_COLUMNS = (
    Column("ATTR_NAME", T.STRING, precision=255),
    Column("DOMAIN", T.SHORT),
    Column("SCALE", T.SHORT),
    Column("PRECISION", T.INT),
    Column("INDEXED", T.SHORT),
    Column("NON_NULL", T.SHORT),
    Column("SHARED", T.SHORT),
    Column("UNIQUE", T.SHORT),
    Column("DEFAULT", T.STRING),
    Column("ATTR_ORDER", T.INT),
    Column("CLASS_NAME", T.STRING, precision=255),
    Column("SOURCE_CLASS", T.STRING, precision=255),
    Column("IS_KEY", T.SHORT),
    Column("REMARKS", T.STRING, precision=1024),
)


def _class_rows(*names: str) -> ResultSet:
    return ResultSet("class", _CLASS_COLUMNS, tuple((text(n), short(2), None) for n in names))


def _attribute_rows(domain: int | None = -32766) -> ResultSet:
    row = (
        text("name"),
        None if domain is None else short(domain),
        short(0),
        int_(20),
        short(0),
        short(1),
        short(0),
        short(0),
        text("'x'"),
        int_(2),
        text("dba.si_t"),
        text("dba.si_t"),
        short(0),
        text(""),
    )
    return ResultSet("attribute", _ATTRIBUTE_COLUMNS, (row,))


def _schema(rs: ResultSet, *, handle: int = 11, count: int | None = None) -> bytes:
    body = bytes(schema_reply(rs, query_handle=handle).data)
    if count is not None:
        body = body[:8] + struct.pack(">i", count) + body[12:]
    return with_status(body, IN_TRAN)


def _page(rs: ResultSet, rows: int = 1) -> bytes:
    page = ResultSet(rs.name, rs.columns, rs.rows[:rows])
    return with_status(bytes(fetch_reply(page).data), IN_TRAN)


def _requests(sock: Any) -> list[tuple[int, tuple[bytes, ...]]]:
    return [(call.args[0][8], split_args(call.args[0][9:])) for call in sock.sendall.call_args_list]


@pytest.mark.parametrize(
    ("args", "kwargs", "error"),
    [
        ((), {}, TypeError),
        ((1,), {}, TypeError),
        ((1, None), {}, TypeError),
        ((4, "t", None), {}, TypeError),
        ((1, b"t"), {}, TypeError),
        ((1, "t", b"a"), {}, TypeError),
        ((1, "t", "a", 1), {}, TypeError),
        ((), {"schema_type": 1, "class_name": "t"}, TypeError),
        ((1,), {"class_name": "t"}, TypeError),
        ((1, "t"), {"attr_name": "a"}, TypeError),
        (("1", "t"), {}, TypeError),
        ((1.0, "t"), {}, TypeError),
        ((2**31, "t"), {}, OverflowError),
        ((-(2**31) - 1, "t"), {}, OverflowError),
        ((1, "t\x00"), {}, ValueError),
        ((4, "t", "a\x00"), {}, ValueError),
        ((0, "t"), {}, InterfaceError),
        ((21, "t"), {}, InterfaceError),
        ((-1, "t"), {}, InterfaceError),
        ((1, "\ud800"), {}, DataError),
        ((4, "t", "\ud800"), {}, DataError),
    ],
)
def test_arguments_are_checked_before_any_request(
    owned, args: tuple[Any, ...], kwargs: dict[str, Any], error: type[BaseException]
) -> None:
    owner, driver, sock = owned
    with pytest.raises(error):
        owner.schema_info(*args, **kwargs)
    assert _codes(sock) == []
    assert driver._connected


@pytest.mark.parametrize("schema_type", [3, *range(6, 21)])
def test_unverified_schema_types_fail_explicitly_before_any_request(
    owned, schema_type: int
) -> None:
    owner, driver, sock = owned
    with pytest.raises(NotSupportedError):
        owner.schema_info(schema_type, "t")
    with pytest.raises(NotSupportedError):
        owner.schema_info(schema_type, "t", "a")
    assert _codes(sock) == []
    assert driver._connected


def test_closed_connection_is_rejected_without_io(owned) -> None:
    owner, driver, sock = owned
    owner._closed = True
    with pytest.raises(InterfaceError):
        owner.schema_info(1, "t")
    assert _codes(sock) == []


@pytest.mark.parametrize(
    ("schema_type", "flag"),
    [(1, 1), (2, 1), (True, 1), (CCISchemaType.VCLASS, 1)],
)
@pytest.mark.parametrize("mode", [False, True])
def test_class_types_return_the_first_row_as_a_list_and_close_the_owned_handle(
    owned, schema_type: Any, flag: int, mode: bool
) -> None:
    owner, driver, sock = owned
    if mode:
        owner.set_autocommit(True)
    rs = _class_rows("dba.si_child", "dba.si_t")
    _load(sock, _schema(rs, handle=23), _page(rs), ok_body(OUT_TRAN))
    row = owner.schema_info(schema_type, "si_%")
    assert type(row) is list
    assert row == ["dba.si_child", 2, None]
    assert [type(value) for value in row] == [str, int, type(None)]
    requests = _requests(sock)
    assert [code for code, _ in requests] == [9, 8, 6]
    fc9 = requests[0][1]
    assert fc9[:4] == (
        struct.pack(">i", int(schema_type)),
        b"si_%\x00",
        b"",
        bytes([flag]),
    )
    assert requests[1][1][:3] == (struct.pack(">i", 23), struct.pack(">i", 1), struct.pack(">i", 1))
    assert requests[2][1][0] == struct.pack(">i", 23)
    assert driver._schema_results == {}
    assert driver._connected
    _guarded(driver)


@pytest.mark.parametrize("schema_type", [4, 5])
def test_attribute_types_send_pattern_flag_two_and_explicit_attribute(
    owned, schema_type: int
) -> None:
    owner, driver, sock = owned
    rs = _attribute_rows()
    _load(sock, _schema(rs), _page(rs), ok_body(IN_TRAN))
    row = owner.schema_info(schema_type, "si_t", "n%")
    assert row == [
        "name",
        2,
        0,
        20,
        0,
        1,
        0,
        0,
        "'x'",
        2,
        "dba.si_t",
        "dba.si_t",
        0,
        "",
    ]
    assert all(type(value) in (str, int) for value in row)
    fc9 = _requests(sock)[0][1]
    assert fc9[:4] == (
        struct.pack(">i", schema_type),
        b"si_t\x00",
        b"n%\x00",
        b"\x02",
    )
    _guarded(driver)


def test_explicit_attribute_name_is_sent_for_class_types_too(owned) -> None:
    owner, _driver, sock = owned
    rs = _class_rows("dba.si_t")
    _load(sock, _schema(rs), _page(rs), ok_body(IN_TRAN))
    assert owner.schema_info(1, "si_t", "") == ["dba.si_t", 2, None]
    assert _requests(sock)[0][1][1:4] == (b"si_t\x00", b"\x00", b"\x01")


@pytest.mark.parametrize(
    ("wire", "expected"),
    [
        (-32766, 2),  # 0x8002: protocol-mask bit + STRING
        (-32760, 8),  # 0x8008: INT
        (-24568, 8),  # 0xA008: SET bits stripped like CCI, element INT remains
        (-8170, 22),  # 0xE016: SEQUENCE of DATETIME
        (0x0315, 0x0315),  # charset-like low bits are kept, as CCI keeps them
        (None, None),
    ],
)
def test_attribute_domain_strips_collection_and_protocol_bits_like_cci(
    owned, wire: int | None, expected: int | None
) -> None:
    owner, _driver, sock = owned
    rs = _attribute_rows(wire)
    _load(sock, _schema(rs), _page(rs), ok_body(IN_TRAN))
    row = owner.schema_info(4, "si_t")
    assert row is not None and row[1] == expected


def test_class_type_column_is_not_masked(owned) -> None:
    owner, _driver, sock = owned
    rs = ResultSet("class", _CLASS_COLUMNS, ((text("t"), short(-32766), None),))
    _load(sock, _schema(rs), _page(rs), ok_body(IN_TRAN))
    assert owner.schema_info(1, "t") == ["t", -32766, None]


@pytest.mark.parametrize("schema_type", [1, 2, 4, 5])
def test_no_row_returns_none_and_still_closes_the_handle(owned, schema_type: int) -> None:
    owner, driver, sock = owned
    columns = _ATTRIBUTE_COLUMNS if schema_type >= 4 else _CLASS_COLUMNS
    _load(sock, _schema(ResultSet("empty", columns, ()), handle=31), ok_body(IN_TRAN))
    assert owner.schema_info(schema_type, "missing") is None
    requests = _requests(sock)
    assert [code for code, _ in requests] == [9, 6]
    assert requests[1][1][0] == struct.pack(">i", 31)
    assert driver._connected
    _guarded(driver)


def test_unexpected_column_type_fails_and_closes_the_handle(owned) -> None:
    owner, driver, sock = owned
    rs = ResultSet(
        "odd", (Column("NAME", T.STRING), Column("TYPE", T.DATETIME)), ((text("t"), None),)
    )
    _load(sock, _schema(rs), ok_body(IN_TRAN))
    with pytest.raises(NotSupportedError):
        owner.schema_info(1, "t")
    assert _codes(sock) == [9, 6]
    assert driver._connected


def test_complete_schema_error_propagates_without_cleanup_or_retirement(owned) -> None:
    owner, driver, sock = owned
    _load(sock, error_body(IN_TRAN, -226, "Empty component list in class."))
    with pytest.raises(DatabaseError) as caught:
        owner.schema_info(1, "t")
    assert caught.value is driver.utility_errors[0]
    assert caught.value.errno == -226
    assert _codes(sock) == [9]
    assert driver._connected


def test_malformed_schema_reply_retires_the_session_without_cleanup(owned) -> None:
    owner, driver, sock = owned
    _load(sock, with_status(b"\x00\x00\x00\x00" + struct.pack(">ii", 11, -1), IN_TRAN))
    with pytest.raises(OperationalError):
        owner.schema_info(1, "t")
    assert _codes(sock) == [9]
    assert not driver._connected


def test_undecodable_metadata_retires_the_fenced_session_without_cleanup(owned) -> None:
    owner, driver, sock = owned
    body = bytearray(_schema(_class_rows("t"), handle=41))
    body[body.index(b"NAME")] = 0xFF
    _load(sock, bytes(body))
    with pytest.raises(OperationalError):
        owner.schema_info(1, "t")
    # The fenced utility request discards the session, so its handle dies with it.
    assert _codes(sock) == [9]
    assert not driver._connected


def test_fetch_server_error_preserves_the_error_and_closes_the_handle(owned) -> None:
    owner, driver, sock = owned
    _load(
        sock,
        _schema(_class_rows("t")),
        error_body(IN_TRAN, -1012, "fetch failed"),
        ok_body(IN_TRAN),
    )
    with pytest.raises(DatabaseError) as caught:
        owner.schema_info(1, "t")
    assert caught.value is driver.utility_errors[0]
    assert _codes(sock) == [9, 8, 6]
    assert driver._connected


def test_empty_fetch_page_cannot_become_no_row(owned) -> None:
    owner, driver, sock = owned
    rs = _class_rows("t")
    _load(sock, _schema(rs), _page(rs, rows=0), ok_body(IN_TRAN))
    with pytest.raises(OperationalError):
        owner.schema_info(1, "t")
    assert _codes(sock) == [9, 8, 6]
    assert driver._connected


def test_cleanup_failure_after_success_raises_and_retires_the_session(owned) -> None:
    owner, driver, sock = owned
    rs = _class_rows("t")
    _load(sock, _schema(rs), _page(rs), error_body(IN_TRAN, -1012, "close failed"))
    with pytest.raises(DatabaseError):
        owner.schema_info(1, "t")
    assert _codes(sock) == [9, 8, 6]
    assert not driver._connected


def test_cleanup_failure_after_primary_failure_keeps_the_primary(owned) -> None:
    owner, driver, sock = owned
    _load(
        sock,
        _schema(_class_rows("t")),
        error_body(IN_TRAN, -1012, "fetch failed"),
        error_body(IN_TRAN, -1013, "close failed"),
    )
    with pytest.raises(DatabaseError) as caught:
        owner.schema_info(1, "t")
    assert caught.value is driver.utility_errors[0]
    assert caught.value.errno == -1012
    assert not driver._connected


def test_replaced_session_is_never_used_for_fetch_or_cleanup(
    owned, monkeypatch: pytest.MonkeyPatch
) -> None:
    owner, driver, sock = owned
    rs = _class_rows("t")
    _load(sock, _schema(rs))
    replacement = make_socket_from_chunks([])
    original = GetSchemaPacket.parse

    def parse(packet: GetSchemaPacket, data: bytes) -> None:
        original(packet, data)
        driver._socket = replacement
        driver._physical_generation += 1

    monkeypatch.setattr(GetSchemaPacket, "parse", parse)
    with pytest.raises((InterfaceError, OperationalError)):
        owner.schema_info(1, "t")
    replacement.sendall.assert_not_called()
    replacement.close.assert_not_called()
    assert _codes(sock) == [9]
    _guarded(driver)


def test_active_ordinary_schema_result_rejects_autocommit_schema_info(owned) -> None:
    owner, driver, sock = owned
    owner.set_autocommit(True)
    schema = GetSchemaPacket(1, "t")
    schema.query_handle = 99
    driver._register_schema_result(schema)
    with pytest.raises(InterfaceError):
        owner.schema_info(1, "t")
    assert _codes(sock) == []
    assert schema in driver._schema_results


def test_fetch_requests_one_row_with_schema_columns(owned, monkeypatch: pytest.MonkeyPatch) -> None:
    owner, _driver, sock = owned
    rs = _class_rows("a", "b")
    _load(sock, _schema(rs), _page(rs), ok_body(IN_TRAN))
    seen: list[FetchPacket] = []
    original = FetchPacket.parse

    def parse(packet: FetchPacket, *args: Any) -> None:
        seen.append(packet)
        original(packet, *args)

    monkeypatch.setattr(FetchPacket, "parse", parse)
    assert owner.schema_info(1, "%") == ["a", 2, None]
    assert len(seen) == 1 and seen[0].current_tuple_count == 0 and seen[0].fetch_size == 1


class _DisguisedText(str):
    """A str subclass whose overrides would hide NUL and change the sent bytes."""

    def __contains__(self, item: object) -> bool:
        return False

    def encode(self, *args: Any, **kwargs: Any) -> bytes:
        return b"other"

    def __str__(self) -> str:
        return "other"


@pytest.mark.parametrize("position", [1, 2])
def test_str_subclass_cannot_hide_nul(owned, position: int) -> None:
    owner, driver, sock = owned
    args: list[Any] = [4, "t", "a"]
    args[position] = _DisguisedText("a\x00b")
    with pytest.raises(ValueError):
        owner.schema_info(*args)
    assert _codes(sock) == []
    assert driver._connected


def test_str_subclass_sends_its_stored_text(owned) -> None:
    owner, _driver, sock = owned
    rs = _attribute_rows()
    _load(sock, _schema(rs), _page(rs), ok_body(IN_TRAN))
    assert owner.schema_info(4, _DisguisedText("si_t"), _DisguisedText("name")) is not None
    assert _requests(sock)[0][1][1:3] == (b"si_t\x00", b"name\x00")


@pytest.mark.parametrize("rows", [0, 1])
def test_interrupt_right_after_schema_reply_still_closes_the_handle(
    owned, monkeypatch: pytest.MonkeyPatch, rows: int
) -> None:
    owner, driver, sock = owned
    columns = _CLASS_COLUMNS
    rs = ResultSet("class", columns, _class_rows("t").rows[:rows])
    _load(sock, _schema(rs, handle=57), ok_body(IN_TRAN))
    original = native.connection._utility_request

    def interrupted(self: Any, packet: Any, state: Any) -> None:
        original(self, packet, state)
        if isinstance(packet, GetSchemaPacket):
            raise KeyboardInterrupt

    monkeypatch.setattr(native.connection, "_utility_request", interrupted)
    with pytest.raises(KeyboardInterrupt):
        owner.schema_info(1, "t")
    requests = _requests(sock)
    assert [code for code, _ in requests] == [9, 6]
    assert requests[1][1][0] == struct.pack(">i", 57)
    assert driver._connected
