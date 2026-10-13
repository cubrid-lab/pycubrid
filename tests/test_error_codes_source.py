"""Every mapped error code is a native CUBRID server code (#895).

The expected names are copied from CUBRID ``src/base/error_code.h``:
https://github.com/CUBRID/cubrid/blob/a761f59ad7a5ee25e6de4c42847a947e0fd7574e/src/base/error_code.h
(release/11.4). release/10.2 (ed716d002ed58078cd1887008da2196215a2331c) defines
the same values, except that ``ER_TRUNCATE_PK_REFERRED`` is new in 11.4.
"""

from __future__ import annotations

import inspect
import re

import pytest

from pycubrid import error_codes
from pycubrid.constants import ErrorCode
from pycubrid.error_codes import (
    CAS_ERROR_TO_EXCEPTION,
    CAS_ERROR_TO_SQLSTATE,
    CUBRID_ERROR_CODES,
)

ERROR_CODE_H: dict[int, str] = {
    0: "NO_ERROR",
    -1: "ER_FAILED",
    -2: "ER_GENERIC_ERROR",
    -3: "ER_OUT_OF_VIRTUAL_MEMORY",
    -4: "ER_INTERRUPTED",
    -13: "ER_IO_READ",
    -14: "ER_IO_WRITE",
    -15: "ER_IO_WRITE_OUT_OF_SPACE",
    -111: "ER_TM_SERVER_DOWN_UNILATERALLY_ABORTED",
    -493: "ER_PT_SYNTAX",
    -494: "ER_PT_SEMANTIC",
    -631: "ER_NULL_CONSTRAINT_VIOLATION",
    -670: "ER_BTREE_UNIQUE_FAILED",
    -671: "ER_CSS_RECV_OR_SEND",
    -922: "ER_FK_INVALID",
    -924: "ER_FK_RESTRICT",
    -1284: "ER_TRUNCATE_PK_REFERRED",
}

TABLES = {
    "CUBRID_ERROR_CODES": CUBRID_ERROR_CODES,
    "CAS_ERROR_TO_SQLSTATE": CAS_ERROR_TO_SQLSTATE,
    "CAS_ERROR_TO_EXCEPTION": CAS_ERROR_TO_EXCEPTION,
}


@pytest.mark.parametrize("name", sorted(TABLES))
def test_every_mapped_code_is_defined_in_error_code_h(name: str) -> None:
    assert set(TABLES[name]) <= set(ERROR_CODE_H)


@pytest.mark.parametrize("name", sorted(TABLES))
def test_every_entry_cites_its_error_code_h_name(name: str) -> None:
    source = inspect.getsource(error_codes)
    block = source[source.index(f"{name}:") :]
    block = block[: block.index("\n}")]
    cited = {
        int(code): constant
        for code, constant in re.findall(r"^\s+(-?\d+): .*?# (NO_ERROR|ER_[A-Z_]+)", block, re.M)
    }
    assert cited == {code: ERROR_CODE_H[code] for code in TABLES[name]}


def test_interrupted_is_not_a_communication_failure() -> None:
    assert CUBRID_ERROR_CODES[-4] == "Interrupted"
    assert CAS_ERROR_TO_SQLSTATE[-4] == "HY008"
    assert CAS_ERROR_TO_EXCEPTION[-4] == "OperationalError"
    assert [code for code, state in CAS_ERROR_TO_SQLSTATE.items() if state.startswith("08")] == [
        -671
    ]


@pytest.mark.parametrize(
    "code",
    [
        -5,  # ER_MHT_NOTFOUND, was "No more data"
        -6,  # ER_MHT_NULL_HASHTABLE, was "Invalid transaction type"
        -7,  # ER_IO_FORMAT_BAD_NPAGES, was "Invalid string parameter"
        -8,  # ER_IO_FORMAT_FAIL, was "Type conversion error"
        -9,  # ER_IO_FORMAT_OUT_OF_SPACE, was "Invalid bind index"
        -10,  # ER_IO_MOUNT_FAIL, was "Invalid arguments"
        -11,  # ER_IO_MOUNT_LOCKED, was "Handle is closed"
        -12,  # ER_IO_DISMOUNT_FAIL, was "Invalid isolation level"
        -394,  # ER_TX_ENDPOINT_TOO_LARGE, was "Column not found" (that is -494)
        -21001,  # JDBC UErrorCode.ER_NOT_OBJECT, never sent by the broker
        -21003,  # JDBC UErrorCode.ER_COMMUNICATION, never sent by the broker
    ],
)
def test_misnumbered_codes_are_not_mapped(code: int) -> None:
    for table in TABLES.values():
        assert code not in table


def test_error_code_enum_matches_error_code_h() -> None:
    for member in ErrorCode:
        expected = ERROR_CODE_H[member.value]
        assert member.name == ("ER_NO_ERROR" if expected == "NO_ERROR" else expected)
