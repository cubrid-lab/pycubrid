from __future__ import annotations

CUBRID_ERROR_CODES: dict[int, str] = {
    0: "No error",
    -1: "DBMS error",
    -2: "Internal error",
    -3: "Out of memory",
    -4: "Communication error",
    -5: "No more data",
    -6: "Invalid transaction type",
    -7: "Invalid string parameter",
    -8: "Type conversion error",
    -9: "Invalid bind index",
    -10: "Invalid arguments",
    -11: "Handle is closed",
    -12: "Invalid isolation level",
    -13: "No shard available",
    -14: "Invalid cursor position",
    -15: "Statement pooling error",
    -111: "Invalid operation",
    -394: "Column not found",
    -493: "Table not found",
    -494: "Syntax error",
    -670: "Unique constraint violation",
    -671: "Foreign key constraint violation",
    -21001: "Authentication failed",
    -21003: "Connection refused",
}

# Standard SQLSTATE codes mapped from CUBRID CAS error codes.
# Uses ISO/ANSI SQL standard states for interoperability.
CAS_ERROR_TO_SQLSTATE: dict[int, str] = {
    -1: "HY000",  # General DBMS error
    -2: "HY000",  # Internal error
    -3: "HY001",  # Memory allocation error
    -4: "08S01",  # Communication link failure
    -6: "25000",  # Invalid transaction state
    -7: "22023",  # Invalid parameter value
    -8: "22018",  # Invalid character value for cast
    -9: "07009",  # Invalid descriptor index
    -10: "07001",  # Wrong number of parameters
    -11: "24000",  # Invalid cursor state
    -12: "25000",  # Invalid transaction state
    -14: "24000",  # Invalid cursor position
    -394: "42S22",  # Column not found
    -493: "42S02",  # Table not found
    -494: "42000",  # Syntax error
    -670: "23000",  # Integrity constraint violation (unique)
    -671: "23000",  # Integrity constraint violation (FK)
    -21001: "28000",  # Invalid authorization
    -21003: "08004",  # Connection rejected
}


def get_error_description(code: int) -> str | None:
    return CUBRID_ERROR_CODES.get(code)


def get_sqlstate(code: int) -> str | None:
    return CAS_ERROR_TO_SQLSTATE.get(code)
