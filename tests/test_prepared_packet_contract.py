"""Internal FC2/FC21 scalar wire contract (#475, #839)."""

from __future__ import annotations

import struct
from typing import Any

import pytest
from hypothesis import HealthCheck, given, settings, strategies as st

from pycubrid import protocol
from pycubrid.constants import CCIPrepareOption
from pycubrid.exceptions import DataError, NotSupportedError, ProgrammingError

_CAS_INFO = b"\x01\x00\x00\x00"
_FC_EXECUTE_ARRAY = 21
_FIXED_ARGS = 3  # handle, query timeout, autocommit


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


def _execute(*values: object, auto_commit: bool = False) -> protocol.ExecuteArrayPacket:
    """Build one row of ``values``; FC21 writes each pair as a single execution would."""
    row = tuple(protocol._encode_prepared_scalar(value) for value in values)
    return protocol.ExecuteArrayPacket(7, (row,), auto_commit=auto_commit, bind_count=len(row))


def test_prepare_exact_holdable_request() -> None:
    packet = protocol.PreparePacket(
        "SELECT ?", auto_commit=True, prepare_flag=CCIPrepareOption.HOLDABLE
    )
    frame = packet.write(_CAS_INFO)
    assert _arguments(frame, 2) == [b"SELECT ?\x00", b"\x08", b"\x01"]


def test_execute_exact_scalar_argument_pairs() -> None:
    packet = _execute(42, "a'\\한", None, auto_commit=True)
    args = _arguments(packet.write(_CAS_INFO), _FC_EXECUTE_ARRAY)
    assert len(args) == 9  # three fixed arguments plus two per bind
    assert args[:_FIXED_ARGS] == [struct.pack(">i", 7), b"\x00" * 4, b"\x01"]
    assert args[_FIXED_ARGS:] == [
        b"\x08",
        struct.pack(">i", 42),
        b"\x01",
        "a'\\한".encode("utf-8") + b"\x00",
        b"\x00",
        b"",
    ]


def test_execute_empty_string_is_not_sql_null() -> None:
    args = _arguments(_execute("", None).write(_CAS_INFO), _FC_EXECUTE_ARRAY)
    assert args[_FIXED_ARGS:] == [b"\x01", b"\x00", b"\x00", b""]
    assert args[2] == b"\x00"  # Manual-commit mode.


@pytest.mark.parametrize("value", [-(2**31), 0, 2**31 - 1])
def test_signed_int32_edges_are_exact(value: int) -> None:
    frame = _execute(value).write(_CAS_INFO)
    assert _arguments(frame, _FC_EXECUTE_ARRAY)[_FIXED_ARGS:] == [
        b"\x08",
        struct.pack(">i", value),
    ]


def test_bind_count_mismatch_rejects_before_frame_is_built() -> None:
    packet = _execute(42)
    packet.bind_count = 2
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
        (True, b"\x00\x00\x00\x01", ProgrammingError),
        ("8", b"\x00\x00\x00\x01", ProgrammingError),
        (8, bytearray(b"\x00\x00\x00\x01"), ProgrammingError),
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
    packet: Any = _execute(42)
    packet.bind_count = bind_count
    with pytest.raises(ProgrammingError):
        packet.write(_CAS_INFO)


def test_execute_rejects_non_boolean_autocommit() -> None:
    packet: Any = _execute(42)
    packet.auto_commit = 1
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
        (object(), ProgrammingError),
        (b"", NotSupportedError),
        ("a\x00b", ProgrammingError),
        ("\ud800", DataError),
        (10**38, DataError),
        (-(10**38), DataError),
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
    frame = _execute(value).write(_CAS_INFO)
    args = _arguments(frame, _FC_EXECUTE_ARRAY)
    assert args[_FIXED_ARGS:] == [b"\x01", value.encode("utf-8") + b"\x00"]
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
    frame = _execute(secret).write(_CAS_INFO)
    execute_args = _arguments(frame, _FC_EXECUTE_ARRAY)
    assert prepare_args[0] == sql.encode("utf-8") + b"\x00"
    assert b"VALUE:" not in prepare_args[0]
    assert execute_args[_FIXED_ARGS:] == [b"\x01", secret.encode("utf-8") + b"\x00"]
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
