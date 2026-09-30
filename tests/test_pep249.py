from __future__ import annotations

import datetime

import pycubrid
from pycubrid.constants import CUBRIDDataType as T


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


# PEP 249: each type object compares equal to the type codes of its group and
# unequal to every other code a result column can report.
_TYPE_OBJECT_MEMBERS = {
    "STRING": {
        T.CHAR,
        T.STRING,
        T.NCHAR,
        T.VARNCHAR,
        T.ENUM,
        T.CLOB,
        T.JSON,
    },
    "BINARY": {T.BIT, T.VARBIT, T.BLOB},
    "NUMBER": {
        T.NUMERIC,
        T.INT,
        T.SHORT,
        T.MONETARY,
        T.FLOAT,
        T.DOUBLE,
        T.BIGINT,
    },
    "DATETIME": {
        T.DATE,
        T.TIME,
        T.TIMESTAMP,
        T.DATETIME,
        T.TIMESTAMPTZ,
        T.TIMESTAMPLTZ,
        T.DATETIMETZ,
        T.DATETIMELTZ,
    },
    "ROWID": {T.OBJECT},
}


def _assert_type_object_group(name: str) -> None:
    type_object = getattr(pycubrid, name)
    members = _TYPE_OBJECT_MEMBERS[name]
    assert {code for code in T if code == type_object} == members
    assert all(code != type_object for code in set(T) - members)


class TestTypeObjects:
    def test_string(self) -> None:
        _assert_type_object_group("STRING")

    def test_binary(self) -> None:
        _assert_type_object_group("BINARY")

    def test_number(self) -> None:
        _assert_type_object_group("NUMBER")

    def test_datetime(self) -> None:
        _assert_type_object_group("DATETIME")

    def test_rowid(self) -> None:
        _assert_type_object_group("ROWID")


class TestConstructors:
    def test_date(self) -> None:
        date_value = pycubrid.Date(2026, 1, 1)
        assert type(date_value) is datetime.date
        assert date_value == datetime.date(2026, 1, 1)

    def test_time(self) -> None:
        time_value = pycubrid.Time(12, 30, 0)
        assert type(time_value) is datetime.time
        assert time_value == datetime.time(12, 30, 0)

    def test_timestamp(self) -> None:
        timestamp_value = pycubrid.Timestamp(2026, 1, 1, 12, 30, 0)
        assert type(timestamp_value) is datetime.datetime
        assert timestamp_value == datetime.datetime(2026, 1, 1, 12, 30, 0)
        assert timestamp_value.tzinfo is None

    def test_binary(self) -> None:
        binary_value = pycubrid.Binary(b"hello")
        assert type(binary_value) is bytes
        assert binary_value == b"hello"


_EXCEPTION_NAMES = (
    "Warning",
    "Error",
    "InterfaceError",
    "DatabaseError",
    "DataError",
    "OperationalError",
    "IntegrityError",
    "InternalError",
    "ProgrammingError",
    "NotSupportedError",
)


class TestConnectionExceptionAttributes:
    """PEP 249 optional extension: exception classes exposed on Connection."""

    def test_sync_connection_class_has_exception_attributes(self) -> None:
        from pycubrid.connection import Connection

        for name in _EXCEPTION_NAMES:
            assert getattr(Connection, name) is getattr(pycubrid, name), name

    def test_async_connection_class_has_exception_attributes(self) -> None:
        from pycubrid.aio.connection import AsyncConnection

        for name in _EXCEPTION_NAMES:
            assert getattr(AsyncConnection, name) is getattr(pycubrid, name), name

    def test_sync_connection_instance_has_exception_attributes(self) -> None:
        from pycubrid.connection import Connection

        # Use __new__ to get an instance without opening a real connection.
        conn = Connection.__new__(Connection)
        for name in _EXCEPTION_NAMES:
            assert getattr(conn, name) is getattr(pycubrid, name), name

    def test_async_connection_instance_has_exception_attributes(self) -> None:
        from pycubrid.aio.connection import AsyncConnection

        # Use __new__ to get an instance without opening a real connection.
        conn = AsyncConnection.__new__(AsyncConnection)
        for name in _EXCEPTION_NAMES:
            assert getattr(conn, name) is getattr(pycubrid, name), name
