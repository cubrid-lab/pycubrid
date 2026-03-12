"""PEP 249 exception hierarchy for pycubrid.

Exception hierarchy::

    Exception
    ├── Warning
    └── Error
        ├── InterfaceError
        └── DatabaseError
            ├── DataError
            ├── OperationalError
            ├── IntegrityError
            ├── InternalError
            ├── ProgrammingError
            └── NotSupportedError
"""

from __future__ import annotations


class Warning(Exception):
    """Exception raised for important warnings.

    For example, data truncation during insertion.
    Defined by PEP 249.
    """

    def __init__(self, msg: str = "", code: int = 0) -> None:
        self.msg = msg
        self.code = code
        super().__init__(msg)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.msg!r})"


class Error(Exception):
    """Base class for all pycubrid errors.

    Defined by PEP 249.
    """

    def __init__(self, msg: str = "", code: int = 0) -> None:
        self.msg = msg
        self.code = code
        super().__init__(msg)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.msg!r})"


class InterfaceError(Error):
    """Exception raised for errors related to the database interface.

    Raised when a method is called with invalid arguments, a connection
    is used after being closed, or other interface-level problems occur.
    Defined by PEP 249.
    """


class DatabaseError(Error):
    """Exception raised for errors related to the database itself.

    Base class for all database-side errors. Subclasses provide more
    specific error categorization.
    Defined by PEP 249.
    """

    def __init__(
        self,
        msg: str = "",
        code: int = 0,
        errno: int | None = None,
        sqlstate: str | None = None,
    ) -> None:
        super().__init__(msg, code)
        self.errno = errno
        self.sqlstate = sqlstate

    def __repr__(self) -> str:
        parts = [repr(self.msg)]
        if self.errno is not None:
            parts.append(f"errno={self.errno}")
        if self.sqlstate is not None:
            parts.append(f"sqlstate={self.sqlstate!r}")
        return f"{self.__class__.__name__}({', '.join(parts)})"


class DataError(DatabaseError):
    """Exception raised for errors due to problems with processed data.

    For example, division by zero, numeric value out of range, etc.
    Defined by PEP 249.
    """


class OperationalError(DatabaseError):
    """Exception raised for errors related to the database's operation.

    For example, unexpected disconnect, memory allocation error,
    transaction processing error, etc.
    Defined by PEP 249.
    """


class IntegrityError(DatabaseError):
    """Exception raised when the relational integrity of the database is affected.

    For example, a foreign key check fails or a duplicate key is detected.
    Defined by PEP 249.
    """


class InternalError(DatabaseError):
    """Exception raised when the database encounters an internal error.

    For example, the cursor is no longer valid or the transaction is
    out of sync.
    Defined by PEP 249.
    """


class ProgrammingError(DatabaseError):
    """Exception raised for programming errors.

    For example, table not found, syntax error in SQL statement,
    wrong number of parameters, etc.
    Defined by PEP 249.
    """


class NotSupportedError(DatabaseError):
    """Exception raised when a method or database API is not supported.

    For example, calling ``rollback()`` on a connection that does not
    support transactions or calling an API that is not supported by
    the database.
    Defined by PEP 249.
    """
