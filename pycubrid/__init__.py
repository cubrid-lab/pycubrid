"""pycubrid — Pure Python DB-API 2.0 driver for CUBRID."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

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
from pycubrid.types import (
    BINARY,
    DATETIME,
    NUMBER,
    ROWID,
    STRING,
    Binary,
    Date,
    DateFromTicks,
    Time,
    Timestamp,
    TimestampFromTicks,
    TimeFromTicks,
)
from pycubrid.lob import Lob

if TYPE_CHECKING:
    from pycubrid.connection import Connection

__version__ = "0.4.0"

# PEP 249 module-level attributes
apilevel = "2.0"
threadsafety = 1  # Threads may share the module but not connections
paramstyle = "qmark"  # Question mark style: WHERE name = ?


def connect(
    host: str = "localhost",
    port: int = 33000,
    database: str = "",
    user: str = "dba",
    password: str = "",
    **kwargs: Any,
) -> Connection:
    """Create a new database connection.

    PEP 249 module-level constructor.

    Args:
        host: CUBRID server hostname or IP address.
        port: CUBRID broker port (default 33000).
        database: Database name.
        user: Database user (default ``"dba"``).
        password: Database password (default ``""``).
        **kwargs: Additional connection parameters.

    Returns:
        A new :class:`~pycubrid.connection.Connection` instance.
    """
    from pycubrid.connection import Connection

    return Connection(
        host=host,
        port=port,
        database=database,
        user=user,
        password=password,
        **kwargs,
    )


__all__ = [
    "__version__",
    "apilevel",
    "threadsafety",
    "paramstyle",
    "connect",
    # Exceptions
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
    # Type objects
    "STRING",
    "BINARY",
    "NUMBER",
    "DATETIME",
    "ROWID",
    # Constructors
    "Date",
    "Time",
    "Timestamp",
    "DateFromTicks",
    "TimeFromTicks",
    "TimestampFromTicks",
    "Binary",
    "Lob",
]
