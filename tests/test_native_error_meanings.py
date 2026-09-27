"""Native syntax/semantic/communication meanings do not depend on text (#391)."""

from __future__ import annotations

import pytest

from pycubrid._cursor_common import _raise_batch_error
from pycubrid.error_codes import get_error_description, get_sqlstate
from pycubrid.exceptions import DatabaseError, OperationalError, ProgrammingError
from pycubrid.protocol import BatchExecutePacket, PrepareAndExecutePacket
from tests.test_native_integrity_errors import _batch_response
from tests.test_protocol import DEFAULT_CAS_INFO, _build_error_response


CASES = [
    (-493, ProgrammingError, "42000", "Syntax error"),
    (-494, ProgrammingError, "42000", "Semantic error"),
    (-671, OperationalError, "08S01", "Communication error"),
]


@pytest.mark.parametrize(("code", "error_class", "state", "description"), CASES)
def test_native_error_meaning(
    code: int, error_class: type[DatabaseError], state: str, description: str
) -> None:
    assert get_error_description(code) == description
    assert get_sqlstate(code) == state


@pytest.mark.parametrize(("code", "error_class", "state", "description"), CASES)
@pytest.mark.parametrize("message", ["opaque native failure", "오류", "foreign key violation"])
@pytest.mark.parametrize("batch", [False, True])
def test_native_error_wire_dispatch(
    code: int,
    error_class: type[DatabaseError],
    state: str,
    description: str,
    message: str,
    batch: bool,
) -> None:
    with pytest.raises(error_class) as raised:
        if batch:
            packet = BatchExecutePacket(["SELEC 1"])
            packet.parse(_batch_response(code, message, 8))
            _raise_batch_error(packet.errors[0])
        else:
            PrepareAndExecutePacket("SELEC 1").parse(
                _build_error_response(DEFAULT_CAS_INFO, code, message)
            )
    assert type(raised.value) is error_class
    assert raised.value.code == raised.value.errno == code
    assert raised.value.sqlstate == state
    assert raised.value.msg == message
    assert description in str(raised.value)


@pytest.mark.parametrize("code", [-4, -21003, -99999])
def test_batch_uses_known_sqlstate_or_existing_unknown_default(code: int) -> None:
    with pytest.raises(DatabaseError) as raised:
        _raise_batch_error({"code": code, "message": "opaque native failure"})
    assert raised.value.sqlstate == (get_sqlstate(code) or "HY000")
    assert raised.value.code == raised.value.errno == code
