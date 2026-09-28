"""Internal FC2/FC3 scalar wire contract; no public prepared API (#475)."""

from __future__ import annotations

import struct

import pytest
from hypothesis import HealthCheck, given, settings, strategies as st

from pycubrid import protocol
from pycubrid.constants import CCIPrepareOption, CUBRIDStatementType
from pycubrid.exceptions import DataError, ProgrammingError

_CAS_INFO = b"\x01\x00\x00\x00"


def _arguments(frame: bytes, function_code: int) -> list[bytes]:
    assert struct.unpack_from(">i", frame)[0] == len(frame) - 8
    assert frame[4:8] == _CAS_INFO
    assert frame[8] == function_code
    payload = memoryview(frame)[9:]
    args: list[bytes] = []
    offset = 0
    while offset < len(payload):
        length = struct.unpack_from(">i", payload, offset)[0]
        assert length >= 0
        offset += 4
        args.append(bytes(payload[offset : offset + length]))
        offset += length
    assert offset == len(payload)
    return args


def test_prepare_exact_holdable_request() -> None:
    packet = protocol.PreparePacket(
        "SELECT ?", auto_commit=True, prepare_flag=CCIPrepareOption.HOLDABLE
    )
    frame = packet.write(_CAS_INFO)
    assert _arguments(frame, 2) == [b"SELECT ?\x00", b"\x08", b"\x01"]


def test_execute_exact_scalar_argument_pairs() -> None:
    values = tuple(protocol._encode_prepared_scalar(value) for value in (42, "a'\\한", None))
    packet = protocol.ExecutePacket(
        7,
        CUBRIDStatementType.SELECT,
        auto_commit=True,
        bindings=values,
        bind_count=3,
    )
    args = _arguments(packet.write(_CAS_INFO), 3)
    assert len(args) == 16  # ten fixed arguments plus two per bind
    assert args[:10] == [
        struct.pack(">i", 7),
        b"\x00",
        b"\x00" * 4,
        b"\x00" * 4,
        b"",
        b"\x01",
        b"\x01",
        b"\x01",
        b"\x00" * 8,
        b"\x00" * 4,
    ]
    assert args[10:] == [
        b"\x08",
        struct.pack(">i", 42),
        b"\x01",
        "a'\\한".encode("utf-8") + b"\x00",
        b"\x00",
        b"",
    ]


def test_execute_empty_string_is_not_sql_null() -> None:
    values = tuple(protocol._encode_prepared_scalar(value) for value in ("", None))
    packet = protocol.ExecutePacket(5, CUBRIDStatementType.SELECT, bindings=values, bind_count=2)
    args = _arguments(packet.write(_CAS_INFO), 3)
    assert args[10:] == [b"\x01", b"\x00", b"\x00", b""]
    assert args[7] == b"\x00"  # Manual-commit mode is not forward-only.


@pytest.mark.parametrize("value", [-(2**31), 0, 2**31 - 1])
def test_signed_int32_edges_are_exact(value: int) -> None:
    packet = protocol.ExecutePacket(
        1,
        CUBRIDStatementType.SELECT,
        bindings=(protocol._encode_prepared_scalar(value),),
        bind_count=1,
    )
    frame = packet.write(_CAS_INFO)
    assert _arguments(frame, 3)[10:] == [b"\x08", struct.pack(">i", value)]


def test_bind_count_mismatch_rejects_before_frame_is_built() -> None:
    packet = protocol.ExecutePacket(
        1,
        CUBRIDStatementType.SELECT,
        bindings=(protocol._encode_prepared_scalar(42),),
        bind_count=2,
    )
    with pytest.raises(ProgrammingError, match="count"):
        packet.write(_CAS_INFO)


@pytest.mark.parametrize(
    ("type_code", "payload", "error"),
    [
        (0, b"\x00", ProgrammingError),
        (8, b"\x00", ProgrammingError),
        (1, b"not terminated", ProgrammingError),
        (1, b"\xff\x00", DataError),
        (255, b"", ProgrammingError),
    ],
)
def test_malformed_prepared_scalar_cannot_be_serialized(
    type_code: int, payload: bytes, error: type[Exception]
) -> None:
    with pytest.raises(error):
        protocol._PreparedScalar(type_code, payload)


def test_unsupported_prepare_flag_rejects_without_sql_leak() -> None:
    packet = protocol.PreparePacket("SELECT 'sensitive'", prepare_flag=1)
    with pytest.raises(ProgrammingError) as caught:
        packet.write(_CAS_INFO)
    assert "sensitive" not in str(caught.value)


@pytest.mark.parametrize(
    ("prepare_flag", "autocommit"),
    [(0.0, False), (8.0, False), (CCIPrepareOption.NORMAL, 1)],
)
def test_prepare_rejects_non_boolean_or_non_integer_controls(
    prepare_flag: object, autocommit: object
) -> None:
    packet = protocol.PreparePacket("SELECT 1", auto_commit=autocommit, prepare_flag=prepare_flag)
    with pytest.raises(ProgrammingError):
        packet.write(_CAS_INFO)


@pytest.mark.parametrize("bind_count", [True, 1.0])
def test_execute_rejects_non_integer_bind_count(bind_count: object) -> None:
    packet = protocol.ExecutePacket(
        1,
        CUBRIDStatementType.SELECT,
        bindings=(protocol._encode_prepared_scalar(42),),
        bind_count=bind_count,
    )
    with pytest.raises(ProgrammingError):
        packet.write(_CAS_INFO)


def test_execute_rejects_non_boolean_autocommit() -> None:
    packet = protocol.ExecutePacket(1, CUBRIDStatementType.SELECT, auto_commit=1, forward_only=True)
    with pytest.raises(ProgrammingError):
        packet.write(_CAS_INFO)


@pytest.mark.parametrize(
    ("sql", "error"),
    [("SELECT '\x00'", ProgrammingError), ("SELECT '\ud800'", DataError)],
)
def test_prepare_rejects_invalid_sql_before_serialization(sql: str, error: type[Exception]) -> None:
    with pytest.raises(error) as caught:
        protocol.PreparePacket(sql).write(_CAS_INFO)
    assert sql not in str(caught.value)


@pytest.mark.parametrize(
    ("value", "error"),
    [
        (True, ProgrammingError),
        (b"bytes", ProgrammingError),
        ("a\x00b", ProgrammingError),
        ("\ud800", DataError),
        (2**31, DataError),
        (-(2**31) - 1, DataError),
    ],
)
def test_prepared_scalar_rejects_unsupported_values(value: object, error: type[Exception]) -> None:
    with pytest.raises(error) as caught:
        protocol._encode_prepared_scalar(value)
    assert repr(value) not in str(caught.value)


@given(st.text(alphabet=st.characters(blacklist_categories=("Cs",)), max_size=40))
def test_string_values_remain_separate_from_sql_template(value: str) -> None:
    if "\x00" in value:
        with pytest.raises(ProgrammingError):
            protocol._encode_prepared_scalar(value)
        return
    packet = protocol.ExecutePacket(
        9,
        CUBRIDStatementType.SELECT,
        bindings=(protocol._encode_prepared_scalar(value),),
        bind_count=1,
    )
    frame = packet.write(_CAS_INFO)
    args = _arguments(frame, 3)
    assert args[10:] == [b"\x01", value.encode("utf-8") + b"\x00"]
    assert b"SELECT ?" not in frame


@given(
    st.text(alphabet="abcxyz0123456789'\\", min_size=1, max_size=30),
    st.text(alphabet="defuvw0123456789'\\", min_size=1, max_size=30),
)
@settings(max_examples=35)
def test_generated_sql_and_value_are_distinct_wire_arguments(sql_tag: str, value: str) -> None:
    sql = f"SELECT ? /*{sql_tag}*/"
    secret = "VALUE:" + value
    prepare_args = _arguments(protocol.PreparePacket(sql).write(_CAS_INFO), 2)
    packet = protocol.ExecutePacket(
        1,
        CUBRIDStatementType.SELECT,
        bindings=(protocol._encode_prepared_scalar(secret),),
        bind_count=1,
    )
    frame = packet.write(_CAS_INFO)
    execute_args = _arguments(frame, 3)
    assert prepare_args[0] == sql.encode("utf-8") + b"\x00"
    assert b"VALUE:" not in prepare_args[0]
    assert execute_args[10:] == [b"\x01", secret.encode("utf-8") + b"\x00"]
    assert sql.encode("utf-8") not in frame


@given(
    fragment=st.text(alphabet="abcxyz0123456789'\\", min_size=1, max_size=30),
    invalid=st.sampled_from(["\x00", "\ud800"]),
)
@settings(max_examples=35, suppress_health_check=[HealthCheck.function_scoped_fixture])
def test_invalid_sql_and_values_are_redacted_before_wire(
    fragment: str, invalid: str, caplog: pytest.LogCaptureFixture
) -> None:
    secret = "CUBRID:private.example:33000:testdb:S3CR3T_" + fragment
    sql = "SELECT '" + secret + invalid + "'"
    error = ProgrammingError if invalid == "\x00" else DataError
    caplog.set_level("DEBUG")
    with pytest.raises(error) as sql_failure:
        protocol.PreparePacket(sql).write(_CAS_INFO)
    with pytest.raises(error) as value_failure:
        protocol._encode_prepared_scalar(secret + invalid)
    assert secret not in str(sql_failure.value)
    assert secret not in str(value_failure.value)
    assert secret not in caplog.text
