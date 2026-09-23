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

This module also defines :class:`UnknownConnectionOptionWarning`, which is a
Python *warning category* rather than part of the PEP 249 hierarchy above.
"""

from __future__ import annotations

from .error_codes import get_error_description


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
        parts = [repr(self.msg)]
        if self.code != 0:
            parts.append(f"code={self.code}")
            description = get_error_description(self.code)
            if description is not None:
                parts.append(f"description={description!r}")
        return f"{self.__class__.__name__}({', '.join(parts)})"


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
            description = get_error_description(self.errno)
            if description is not None:
                parts.append(f"description={description!r}")
        if self.sqlstate is not None:
            parts.append(f"sqlstate={self.sqlstate!r}")
        return f"{self.__class__.__name__}({', '.join(parts)})"

    def __str__(self) -> str:
        message = self.msg
        details: list[str] = []
        if self.errno is not None:
            details.append(f"errno={self.errno}")
            description = get_error_description(self.errno)
            if description is not None:
                details.append(f"description={description!r}")
        if self.sqlstate is not None:
            details.append(f"sqlstate={self.sqlstate!r}")
        if not details:
            return message
        if message:
            return f"{message} ({', '.join(details)})"
        return f"({', '.join(details)})"


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


class UnknownConnectionOptionWarning(UserWarning):
    """Warning category for connection options pycubrid does not recognise.

    This is a Python warning category (a :class:`UserWarning` subclass), not a
    PEP 249 exception. It is deliberately distinct from :class:`Warning` above,
    which is the PEP 249 *database* warning and is raised, never warned.

    A connection constructor collects unrecognised keywords in ``**kwargs`` and
    used to drop them silently, so a typo such as ``read_timout=30`` left the
    option with no effect and gave the caller no signal (issue #377). Such
    keywords are now reported through this category instead.

    The default is a warning rather than a :class:`TypeError` because wrapper
    layers (connection pools, ORM dialects) legitimately forward extra keywords,
    and rejecting them outright would be a breaking change under
    ``RELEASE_POLICY.md``. Callers who want strictness can opt in::

        import warnings
        import pycubrid

        warnings.simplefilter("error", pycubrid.UnknownConnectionOptionWarning)

    or silence it entirely with ``"ignore"`` in place of ``"error"``.
    """
