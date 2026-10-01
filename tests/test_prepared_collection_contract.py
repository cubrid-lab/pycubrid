"""Internal typed FC3 collection wire contract; no public collection API (#482)."""

from __future__ import annotations

import json
import struct
from pathlib import Path
from typing import Any

import pytest
from hypothesis import given, strategies as st

from pycubrid import protocol
from pycubrid.constants import CUBRIDDataType, CUBRIDStatementType
from pycubrid.exceptions import DataError, OperationalError, ProgrammingError

from .test_network_edge_cases import make_connected_connection

_CAS_INFO = b"\x01\x00\x00\x00"
_SET = CUBRIDDataType.SET
_MULTISET = CUBRIDDataType.MULTISET
_SEQUENCE = CUBRIDDataType.SEQUENCE
_INT = CUBRIDDataType.INT
_STRING = CUBRIDDataType.STRING
_KINDS = [_SET, _MULTISET, _SEQUENCE]

# Fixed FC3 arguments for handle 7, SELECT, manual commit (see
# test_prepared_packet_contract.py).
_FIXED_ARGS = (
    b"\x00\x00\x00\x04\x00\x00\x00\x07"  # query handle
    b"\x00\x00\x00\x01\x00"  # execution option NORMAL
    b"\x00\x00\x00\x04\x00\x00\x00\x00"  # max_col_size
    b"\x00\x00\x00\x04\x00\x00\x00\x00"  # max_row_size
    b"\x00\x00\x00\x00"  # NULL
    b"\x00\x00\x00\x01\x01"  # SELECT fetch flag
    b"\x00\x00\x00\x01\x00"  # autocommit off
    b"\x00\x00\x00\x01\x00"  # not forward-only
    b"\x00\x00\x00\x08"
    + b"\x00" * 8  # cache time
    + b"\x00\x00\x00\x04\x00\x00\x00\x00"  # query timeout
)


def _int(value: int) -> bytes:
    return struct.pack(">i", value)


def _frame(binding_args: bytes) -> bytes:
    body = _CAS_INFO + b"\x03" + _FIXED_ARGS + binding_args
    return struct.pack(">i", len(body) - 4) + body


def _execute(*bindings: protocol._PreparedScalar | protocol._PreparedCollection) -> bytes:
    packet = protocol.ExecutePacket(
        7, CUBRIDStatementType.SELECT, bindings=bindings, bind_count=len(bindings)
    )
    return packet.write(_CAS_INFO)


def _broker_parse(value_arg: bytes) -> tuple[int | None, list[bytes | None], bool]:
    """Mirror cas_execute.c netval_to_dbval for SET/MULTISET/SEQUENCE.

    11.4 cas_execute.c:4436-4440 turns a value argument of length <= 0 into
    whole SQL NULL; 4788-4857 (10.2: 4266-4340) skip the element-type byte
    and loop while bytes remain. When an element length overruns what is
    left (11.4:4821-4823, 10.2:4299-4301) the broker silently stops and
    keeps the partial collection. Returns (element type, elements,
    truncated).
    """
    if len(value_arg) <= 0:
        return None, [], False
    element_type = value_arg[0]
    cursor = 1
    remain = len(value_arg) - 1
    elements: list[bytes | None] = []
    while remain > 0:
        size = struct.unpack_from(">i", value_arg.ljust(cursor + 4, b"\x00"), cursor)[0]
        if size + 4 > remain:
            return element_type, elements, True
        if size <= 0:
            # netval_to_dbval() reads a length <= 0 as a NULL element and
            # consumes only the length word.
            elements.append(None)
            size = 0
        else:
            elements.append(value_arg[cursor + 4 : cursor + 4 + size])
        cursor += size + 4
        remain -= size + 4
    return element_type, elements, False


# --- golden bytes ----------------------------------------------------------


def test_full_frame_for_int_set_is_exact() -> None:
    binding = protocol._encode_prepared_collection((1, None, -2), _SET, _INT)
    value = b"\x08" + _int(4) + _int(1) + _int(0) + _int(4) + _int(-2)
    assert _execute(binding) == _frame(b"\x00\x00\x00\x01\x10" + _int(len(value)) + value)
    assert len(value) == 1 + (4 + 4) + 4 + (4 + 4)


def test_full_frame_for_empty_string_sequence_is_exact() -> None:
    binding = protocol._encode_prepared_collection((), _SEQUENCE, _STRING)
    assert _execute(binding) == _frame(b"\x00\x00\x00\x01\x12\x00\x00\x00\x01\x02")


def test_collection_and_scalar_bindings_share_one_frame() -> None:
    frame = _execute(
        protocol._encode_prepared_scalar(5),
        protocol._encode_prepared_collection(("a",), _MULTISET, _STRING),
        protocol._encode_prepared_scalar(None),
    )
    assert frame == _frame(
        b"\x00\x00\x00\x01\x08"
        + _int(4)
        + _int(5)
        + b"\x00\x00\x00\x01\x11"
        + _int(7)
        + b"\x02"
        + _int(2)
        + b"a\x00"
        + b"\x00\x00\x00\x01\x00"
        + _int(0)
    )


_CCI_GOLDEN = json.loads(
    (Path(__file__).parent / "fixtures" / "cci_collection_golden.json").read_text("utf-8")
)


@pytest.mark.parametrize("kind", _KINDS, ids=["set", "multiset", "sequence"])
@pytest.mark.parametrize("case", _CCI_GOLDEN["cases"], ids=lambda case: case["name"])
def test_value_matches_cci_generated_golden(kind: int, case: dict[str, Any]) -> None:
    # Independent of protocol.py: bytes produced by CCI cci_set_make() at
    # the commit recorded in the fixture.
    assert _CCI_GOLDEN["cci_commit"] == "79d0888"
    binding = protocol._encode_prepared_collection(
        tuple(case["values"]), kind, case["element_type"]
    )
    assert binding.payload == bytes.fromhex(case["cci_value_hex"])


_INT_CASES = {
    "empty": ((), b""),
    "null": ((None,), _int(0)),
    "all-null": ((None, None), _int(0) * 2),
    "dup": ((3, 3), (_int(4) + _int(3)) * 2),
    "order": ((9, 1, 5), _int(4) + _int(9) + _int(4) + _int(1) + _int(4) + _int(5)),
    "edges": ((-(2**31), 2**31 - 1), _int(4) + b"\x80\x00\x00\x00" + _int(4) + b"\x7f\xff\xff\xff"),
    "digit-strings": (
        ("1", "-20", "0"),
        _int(4) + _int(1) + _int(4) + _int(-20) + _int(4) + _int(0),
    ),
}
_STRING_CASES = {
    "empty": ((), b""),
    "null": ((None,), _int(0)),
    "all-null": ((None, None), _int(0) * 2),
    "dup": (("x", "x"), (_int(2) + b"x\x00") * 2),
    "order": (("b", "a"), _int(2) + b"b\x00" + _int(2) + b"a\x00"),
    "empty-string": (("", None), _int(1) + b"\x00" + _int(0)),
    "korean": (("한글",), _int(7) + "한글".encode() + b"\x00"),
    "null-word": (("NULL",), _int(5) + b"NULL\x00"),
}


@pytest.mark.parametrize("kind", _KINDS, ids=["set", "multiset", "sequence"])
@pytest.mark.parametrize(
    ("element_type", "values", "elements"),
    [(_INT, *case) for case in _INT_CASES.values()]
    + [(_STRING, *case) for case in _STRING_CASES.values()],
    ids=[f"int-{name}" for name in _INT_CASES] + [f"string-{name}" for name in _STRING_CASES],
)
def test_collection_argument_pair_is_exact(
    kind: int, element_type: int, values: tuple[object, ...], elements: bytes
) -> None:
    binding = protocol._encode_prepared_collection(values, kind, element_type)
    value = bytes((element_type,)) + elements
    assert _execute(binding) == _frame(
        b"\x00\x00\x00\x01" + bytes((kind,)) + _int(len(value)) + value
    )


def test_euc_kr_string_elements_use_the_connection_charset() -> None:
    binding = protocol._encode_prepared_collection(("한",), _SET, _STRING, "euc_kr")
    packet = protocol.ExecutePacket(7, CUBRIDStatementType.SELECT, bindings=(binding,))
    packet.encoding = "euc_kr"
    frame = packet.write(_CAS_INFO)
    assert frame.endswith(_int(8) + b"\x02" + _int(3) + "한".encode("euc_kr") + b"\x00")


def test_euc_kr_payloads_are_checked_the_way_the_server_reads_them() -> None:
    # CUBRID reads each EUC-KR pair as one KS X 1001 character: a lone Hangul
    # filler is valid (CPython's euc_kr rejects it), and the 8-byte makeup
    # sequence CPython decodes as 똠 is four jamo. A CP949-only character is
    # still rejected.
    filler = protocol._PreparedCollection(_SET, _STRING, (b"\xa4\xd4\x00",), "euc_kr")
    makeup = "똠".encode("euc_kr") + b"\x00"
    jamo = protocol._PreparedCollection(_SET, _STRING, (makeup,), "euc_kr")
    assert (filler.elements, jamo.elements) == ((b"\xa4\xd4\x00",), (makeup,))
    assert protocol._encode_prepared_collection(("\u3164ㄸㅗㅁ",), _SET, _STRING, "euc_kr") == jamo
    with pytest.raises(DataError):
        protocol._PreparedCollection(
            _SET, _STRING, (b"\xa4\xd4" + "똠".encode("cp949") + b"\x00",), "euc_kr"
        )


def test_string_elements_encoded_for_another_charset_are_rejected() -> None:
    binding = protocol._encode_prepared_collection(("a",), _SET, _STRING, "euc_kr")
    with pytest.raises(ProgrammingError, match="charset"):
        _execute(binding)


def test_collection_binding_is_immutable() -> None:
    binding = protocol._encode_prepared_collection((1,), _SET, _INT)
    with pytest.raises(AttributeError):
        setattr(binding, "elements", (None,))
    assert isinstance(binding.elements, tuple)


# --- rejection before any bytes -------------------------------------------


@pytest.mark.parametrize(
    ("values", "element_type", "error"),
    [
        ([1, 2], _INT, ProgrammingError),
        ({1, 2}, _INT, ProgrammingError),
        (None, _INT, ProgrammingError),
        ("12", _INT, ProgrammingError),
        ((1, "2"), _INT, ProgrammingError),
        (("1", 2), _INT, ProgrammingError),
        ((True,), _INT, ProgrammingError),
        ((1.0,), _INT, ProgrammingError),
        ((b"1",), _INT, ProgrammingError),
        (((1,),), _INT, ProgrammingError),
        (([1],), _INT, ProgrammingError),
        (("01",), _INT, ProgrammingError),
        (("-0",), _INT, ProgrammingError),
        (("+1",), _INT, ProgrammingError),
        ((" 1",), _INT, ProgrammingError),
        (("-",), _INT, ProgrammingError),
        (("",), _INT, ProgrammingError),
        (("١",), _INT, ProgrammingError),
        ((2**31,), _INT, DataError),
        ((str(-(2**31) - 1),), _INT, DataError),
        (("9" * 5000,), _INT, DataError),
        ((1,), _STRING, ProgrammingError),
        (("a", 1), _STRING, ProgrammingError),
        ((b"a",), _STRING, ProgrammingError),
        ((("a",),), _STRING, ProgrammingError),
        ((frozenset({"a"}),), _STRING, ProgrammingError),
        (("a\x00b",), _STRING, ProgrammingError),
        (("\ud800",), _STRING, DataError),
    ],
)
def test_unsupported_collections_are_rejected_without_echo(
    values: object, element_type: int, error: type[Exception]
) -> None:
    with pytest.raises(error) as caught:
        protocol._encode_prepared_collection(values, _SET, element_type)
    assert repr(values) not in str(caught.value)


@pytest.mark.parametrize(
    ("type_code", "element_type"),
    [
        (CUBRIDDataType.NULL, _INT),
        (CUBRIDDataType.CHAR, _INT),
        (True, _INT),
        (_SET, CUBRIDDataType.CHAR),
        (_SET, CUBRIDDataType.BIGINT),
        (_SET, _SET),
        (_SET, True),
    ],
)
def test_unsupported_type_codes_are_rejected(type_code: int, element_type: int) -> None:
    with pytest.raises(ProgrammingError, match="type"):
        protocol._encode_prepared_collection((), type_code, element_type)


@pytest.mark.parametrize(
    ("element_type", "elements", "error"),
    [
        (_INT, [b"\x00" * 4], ProgrammingError),
        (_INT, (b"\x00" * 5,), ProgrammingError),
        (_INT, (b"",), ProgrammingError),
        (_INT, (bytearray(4),), ProgrammingError),
        (_STRING, (b"a",), ProgrammingError),
        (_STRING, (b"a\x00b\x00",), ProgrammingError),
        (_STRING, (b"\xff\x00",), DataError),
    ],
)
def test_malformed_element_payloads_cannot_be_constructed(
    element_type: int, elements: Any, error: type[Exception]
) -> None:
    with pytest.raises(error):
        protocol._PreparedCollection(_SET, element_type, elements)


def test_broker_mirror_truncates_on_overrunning_element_length() -> None:
    # The hazard exact framing avoids: one bad length silently drops the rest.
    value = b"\x08" + _int(4) + _int(1) + _int(9) + _int(2)
    assert _broker_parse(value) == (8, [_int(1)], True)
    # A negative length is a NULL element that advances only its length word
    # (live: 08|-5|4|7 stores [None, 7]).
    assert _broker_parse(b"\x08" + _int(-5) + _int(4) + _int(7)) == (8, [None, _int(7)], False)
    assert _broker_parse(b"") == (None, [], False)
    assert _broker_parse(b"\x08") == (8, [], False)


# --- property: lengths always add up exactly -------------------------------

_int_elements = st.one_of(st.none(), st.integers(min_value=-(2**31), max_value=2**31 - 1))
_str_elements = st.one_of(
    st.none(),
    st.text(alphabet=st.characters(blacklist_categories=("Cs",), blacklist_characters="\x00")),
)


@given(
    kind=st.sampled_from(_KINDS),
    data=st.one_of(
        st.tuples(st.just(_INT), st.lists(_int_elements, max_size=12)),
        st.tuples(
            st.just(_INT),
            st.lists(
                st.one_of(
                    st.none(),
                    st.integers(min_value=-(2**31), max_value=2**31 - 1).map(str),
                ),
                max_size=12,
            ),
        ),
        st.tuples(st.just(_STRING), st.lists(_str_elements, max_size=12)),
    ),
)
def test_lengths_always_add_up_and_broker_mirror_never_truncates(
    kind: int, data: tuple[int, list[Any]]
) -> None:
    element_type, values = data
    binding = protocol._encode_prepared_collection(tuple(values), kind, element_type)
    frame = _execute(binding)
    assert struct.unpack_from(">i", frame)[0] == len(frame) - 8
    pair = frame[len(_frame(b"")) :]
    assert pair[:5] == b"\x00\x00\x00\x01" + bytes((kind,))
    value_length = struct.unpack_from(">i", pair, 5)[0]
    value = pair[9:]
    expected: list[bytes | None] = []
    for item in values:
        if item is None:
            expected.append(None)
        elif element_type == _INT:
            expected.append(_int(int(item)))
        else:
            expected.append(str(item).encode() + b"\x00")
    assert value_length == len(value) == 1 + sum(4 + len(item or b"") for item in expected)
    assert _broker_parse(value) == (element_type, expected, False)


# --- fault: nothing is written and the generation fence stays --------------


def test_invalid_collection_binding_sends_nothing_and_keeps_session() -> None:
    conn, sock = make_connected_connection()
    sock.sendall.reset_mock()
    generation = conn._physical_generation
    binding = protocol._encode_prepared_collection(("a",), _SET, _STRING, "euc_kr")
    packet = protocol.ExecutePacket(7, CUBRIDStatementType.SELECT, bindings=(binding,))
    with pytest.raises(ProgrammingError):
        conn._send_and_receive(packet, expected_generation=generation)
    sock.sendall.assert_not_called()
    assert conn._connected is True
    assert conn._physical_generation == generation


def test_collection_execute_honors_expected_generation() -> None:
    conn, sock = make_connected_connection()
    sock.sendall.reset_mock()
    binding = protocol._encode_prepared_collection((1,), _SET, _INT)
    packet = protocol.ExecutePacket(7, CUBRIDStatementType.SELECT, bindings=(binding,))
    conn._physical_generation = 2
    with pytest.raises(OperationalError, match="prepared|generation|session"):
        conn._send_and_receive(packet, expected_generation=1)
    sock.sendall.assert_not_called()
    assert conn._connected is True
