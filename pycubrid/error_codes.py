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


def get_error_description(code: int) -> str | None:
    return CUBRID_ERROR_CODES.get(code)
