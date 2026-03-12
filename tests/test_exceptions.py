"""Tests for pycubrid.exceptions — PEP 249 exception hierarchy."""

from __future__ import annotations

import pytest

from pycubrid.exceptions import (
    DatabaseError,
    DataError,
    Error,
    IntegrityError,
    InterfaceError,
    InternalError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
    Warning,
)


# ---------------------------------------------------------------------------
# Exception hierarchy tests
# ---------------------------------------------------------------------------


class TestExceptionHierarchy:
    """Verify PEP 249 exception inheritance tree."""

    def test_warning_is_exception(self) -> None:
        assert issubclass(Warning, Exception)

    def test_error_is_exception(self) -> None:
        assert issubclass(Error, Exception)

    def test_interface_error_is_error(self) -> None:
        assert issubclass(InterfaceError, Error)

    def test_database_error_is_error(self) -> None:
        assert issubclass(DatabaseError, Error)

    def test_data_error_is_database_error(self) -> None:
        assert issubclass(DataError, DatabaseError)

    def test_operational_error_is_database_error(self) -> None:
        assert issubclass(OperationalError, DatabaseError)

    def test_integrity_error_is_database_error(self) -> None:
        assert issubclass(IntegrityError, DatabaseError)

    def test_internal_error_is_database_error(self) -> None:
        assert issubclass(InternalError, DatabaseError)

    def test_programming_error_is_database_error(self) -> None:
        assert issubclass(ProgrammingError, DatabaseError)

    def test_not_supported_error_is_database_error(self) -> None:
        assert issubclass(NotSupportedError, DatabaseError)

    def test_warning_not_error(self) -> None:
        assert not issubclass(Warning, Error)

    def test_interface_error_not_database_error(self) -> None:
        assert not issubclass(InterfaceError, DatabaseError)


# ---------------------------------------------------------------------------
# Exception construction and attributes
# ---------------------------------------------------------------------------


class TestExceptionAttributes:
    """Verify exception attributes and construction."""

    def test_warning_default(self) -> None:
        w = Warning()
        assert w.msg == ""
        assert w.code == 0
        assert str(w) == ""

    def test_warning_with_message(self) -> None:
        w = Warning("data truncated")
        assert w.msg == "data truncated"
        assert str(w) == "data truncated"

    def test_error_default(self) -> None:
        e = Error()
        assert e.msg == ""
        assert e.code == 0

    def test_error_with_code(self) -> None:
        e = Error("connection failed", code=1001)
        assert e.msg == "connection failed"
        assert e.code == 1001

    def test_database_error_attributes(self) -> None:
        e = DatabaseError("duplicate key", errno=-670, sqlstate="23000")
        assert e.msg == "duplicate key"
        assert e.errno == -670
        assert e.sqlstate == "23000"
        assert e.code == 0

    def test_database_error_default_attributes(self) -> None:
        e = DatabaseError()
        assert e.errno is None
        assert e.sqlstate is None

    @pytest.mark.parametrize(
        "exc_class",
        [
            DataError,
            OperationalError,
            IntegrityError,
            InternalError,
            ProgrammingError,
            NotSupportedError,
        ],
    )
    def test_database_error_subclass_has_errno_sqlstate(
        self,
        exc_class: type[DatabaseError],
    ) -> None:
        e = exc_class("test", errno=-123, sqlstate="HY000")
        assert e.errno == -123
        assert e.sqlstate == "HY000"
        assert isinstance(e, DatabaseError)


# ---------------------------------------------------------------------------
# __repr__ tests
# ---------------------------------------------------------------------------


class TestExceptionRepr:
    """Verify __repr__ formatting."""

    def test_warning_repr(self) -> None:
        assert repr(Warning("truncated")) == "Warning('truncated')"

    def test_error_repr(self) -> None:
        assert repr(Error("fail")) == "Error('fail')"

    def test_interface_error_repr(self) -> None:
        assert repr(InterfaceError("closed")) == "InterfaceError('closed')"

    def test_database_error_repr_simple(self) -> None:
        assert repr(DatabaseError("err")) == "DatabaseError('err')"

    def test_database_error_repr_with_errno(self) -> None:
        r = repr(DatabaseError("err", errno=-1))
        assert "errno=-1" in r
        assert r.startswith("DatabaseError(")

    def test_database_error_repr_with_sqlstate(self) -> None:
        r = repr(DatabaseError("err", sqlstate="23000"))
        assert "sqlstate='23000'" in r

    def test_database_error_repr_full(self) -> None:
        r = repr(DatabaseError("err", errno=-1, sqlstate="23000"))
        assert "errno=-1" in r
        assert "sqlstate='23000'" in r

    def test_subclass_repr(self) -> None:
        r = repr(ProgrammingError("syntax error", errno=-493))
        assert r.startswith("ProgrammingError(")
        assert "errno=-493" in r


# ---------------------------------------------------------------------------
# Raisability tests
# ---------------------------------------------------------------------------


class TestExceptionRaisable:
    """Verify all exceptions can be raised and caught."""

    @pytest.mark.parametrize(
        "exc_class",
        [
            Warning,
            Error,
            InterfaceError,
            DatabaseError,
            DataError,
            OperationalError,
            IntegrityError,
            InternalError,
            ProgrammingError,
            NotSupportedError,
        ],
    )
    def test_raise_and_catch(self, exc_class: type[Exception]) -> None:
        with pytest.raises(exc_class):
            raise exc_class("test message")

    def test_catch_database_error_catches_subclass(self) -> None:
        with pytest.raises(DatabaseError):
            raise IntegrityError("foreign key violation")

    def test_catch_error_catches_interface_error(self) -> None:
        with pytest.raises(Error):
            raise InterfaceError("already closed")
