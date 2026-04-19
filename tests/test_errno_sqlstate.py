"""Tests for errno/sqlstate propagation in protocol._raise_error."""

from __future__ import annotations

import struct

import pytest

from pycubrid.error_codes import CAS_ERROR_TO_SQLSTATE, get_sqlstate
from pycubrid.exceptions import DatabaseError, IntegrityError, ProgrammingError
from pycubrid.packet import PacketReader
from pycubrid.protocol import _raise_error


DEFAULT_CAS_INFO = b"\x00\x01\x02\x03"


def _build_error_body(error_code: int, error_message: str) -> bytes:
    msg_bytes = error_message.encode("utf-8") + b"\x00"
    return struct.pack(">i", error_code) + msg_bytes


class TestRaiseErrorErrnoSqlstate:
    def test_integrity_error_has_errno_and_sqlstate(self) -> None:
        body = _build_error_body(-670, "Unique constraint violation")
        reader = PacketReader(body)
        with pytest.raises(IntegrityError) as exc_info:
            _raise_error(reader, len(body))
        assert exc_info.value.errno == -670
        assert exc_info.value.sqlstate == "23000"
        assert exc_info.value.code == -670

    def test_programming_error_has_errno_and_sqlstate(self) -> None:
        body = _build_error_body(-494, "Syntax error in SQL")
        reader = PacketReader(body)
        with pytest.raises(ProgrammingError) as exc_info:
            _raise_error(reader, len(body))
        assert exc_info.value.errno == -494
        assert exc_info.value.sqlstate == "42000"

    def test_database_error_has_errno_and_sqlstate(self) -> None:
        body = _build_error_body(-1, "DBMS error occurred")
        reader = PacketReader(body)
        with pytest.raises(DatabaseError) as exc_info:
            _raise_error(reader, len(body))
        assert exc_info.value.errno == -1
        assert exc_info.value.sqlstate == "HY000"

    def test_foreign_key_integrity_error(self) -> None:
        body = _build_error_body(-671, "Foreign key constraint violation")
        reader = PacketReader(body)
        with pytest.raises(IntegrityError) as exc_info:
            _raise_error(reader, len(body))
        assert exc_info.value.errno == -671
        assert exc_info.value.sqlstate == "23000"

    def test_unknown_error_code_uses_default_sqlstate(self) -> None:
        body = _build_error_body(-99999, "Unknown error")
        reader = PacketReader(body)
        with pytest.raises(DatabaseError) as exc_info:
            _raise_error(reader, len(body))
        assert exc_info.value.errno == -99999
        assert exc_info.value.sqlstate == "HY000"

    def test_table_not_found_programming_error(self) -> None:
        body = _build_error_body(-493, "Table does not exist")
        reader = PacketReader(body)
        with pytest.raises(ProgrammingError) as exc_info:
            _raise_error(reader, len(body))
        assert exc_info.value.errno == -493
        assert exc_info.value.sqlstate == "42S02"

    def test_communication_error_sqlstate(self) -> None:
        body = _build_error_body(-4, "Communication error with server")
        reader = PacketReader(body)
        with pytest.raises(DatabaseError) as exc_info:
            _raise_error(reader, len(body))
        assert exc_info.value.errno == -4
        assert exc_info.value.sqlstate == "08S01"

    def test_auth_failed_sqlstate(self) -> None:
        body = _build_error_body(-21001, "Authentication failed")
        reader = PacketReader(body)
        with pytest.raises(DatabaseError) as exc_info:
            _raise_error(reader, len(body))
        assert exc_info.value.errno == -21001
        assert exc_info.value.sqlstate == "28000"


class TestGetSqlstate:
    def test_known_code_returns_sqlstate(self) -> None:
        assert get_sqlstate(-670) == "23000"
        assert get_sqlstate(-494) == "42000"
        assert get_sqlstate(-4) == "08S01"

    def test_unknown_code_returns_none(self) -> None:
        assert get_sqlstate(-99999) is None

    def test_all_mapped_codes_have_5char_sqlstate(self) -> None:
        for code, state in CAS_ERROR_TO_SQLSTATE.items():
            assert len(state) == 5, f"SQLSTATE for code {code} should be 5 chars"


class TestDatabaseErrorReprStr:
    def test_repr_includes_errno_and_sqlstate(self) -> None:
        err = DatabaseError(msg="test", code=-670, errno=-670, sqlstate="23000")
        r = repr(err)
        assert "errno=-670" in r
        assert "sqlstate='23000'" in r

    def test_str_includes_errno_and_sqlstate(self) -> None:
        err = DatabaseError(msg="test error", code=-670, errno=-670, sqlstate="23000")
        s = str(err)
        assert "errno=-670" in s
        assert "sqlstate='23000'" in s

    def test_str_no_errno_no_sqlstate(self) -> None:
        err = DatabaseError(msg="plain error")
        assert str(err) == "plain error"
