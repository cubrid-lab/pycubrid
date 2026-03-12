from __future__ import annotations

import pycubrid


class TestModuleInterface:
    def test_apilevel(self) -> None:
        assert pycubrid.apilevel == "2.0"

    def test_threadsafety(self) -> None:
        assert pycubrid.threadsafety in (0, 1, 2, 3)

    def test_paramstyle(self) -> None:
        assert pycubrid.paramstyle in ("qmark", "numeric", "named", "format", "pyformat")

    def test_connect_callable(self) -> None:
        assert callable(pycubrid.connect)


class TestExceptionHierarchy:
    def test_warning_is_exception(self) -> None:
        assert issubclass(pycubrid.Warning, Exception)

    def test_error_is_exception(self) -> None:
        assert issubclass(pycubrid.Error, Exception)

    def test_interface_error(self) -> None:
        assert issubclass(pycubrid.InterfaceError, pycubrid.Error)

    def test_database_error(self) -> None:
        assert issubclass(pycubrid.DatabaseError, pycubrid.Error)

    def test_data_error(self) -> None:
        assert issubclass(pycubrid.DataError, pycubrid.DatabaseError)

    def test_operational_error(self) -> None:
        assert issubclass(pycubrid.OperationalError, pycubrid.DatabaseError)

    def test_integrity_error(self) -> None:
        assert issubclass(pycubrid.IntegrityError, pycubrid.DatabaseError)

    def test_internal_error(self) -> None:
        assert issubclass(pycubrid.InternalError, pycubrid.DatabaseError)

    def test_programming_error(self) -> None:
        assert issubclass(pycubrid.ProgrammingError, pycubrid.DatabaseError)

    def test_not_supported_error(self) -> None:
        assert issubclass(pycubrid.NotSupportedError, pycubrid.DatabaseError)


class TestTypeObjects:
    def test_string(self) -> None:
        assert pycubrid.STRING is not None

    def test_binary(self) -> None:
        assert pycubrid.BINARY is not None

    def test_number(self) -> None:
        assert pycubrid.NUMBER is not None

    def test_datetime(self) -> None:
        assert pycubrid.DATETIME is not None

    def test_rowid(self) -> None:
        assert pycubrid.ROWID is not None


class TestConstructors:
    def test_date(self) -> None:
        date_value = pycubrid.Date(2026, 1, 1)
        assert date_value is not None

    def test_time(self) -> None:
        time_value = pycubrid.Time(12, 30, 0)
        assert time_value is not None

    def test_timestamp(self) -> None:
        timestamp_value = pycubrid.Timestamp(2026, 1, 1, 12, 30, 0)
        assert timestamp_value is not None

    def test_binary(self) -> None:
        binary_value = pycubrid.Binary(b"hello")
        assert binary_value is not None
