"""Native CUBRID error codes known to the driver, with SQLSTATE and class mappings.

Every key is a server error code that a CAS error reply carries after a
``DBMS_ERROR_INDICATOR``, defined in CUBRID ``src/base/error_code.h`` with the
same value on release/10.2 (ed716d002ed58078cd1887008da2196215a2331c) and
release/11.4 (a761f59ad7a5ee25e6de4c42847a947e0fd7574e); the comment on each
entry names its ``ER_*`` constant (#895). ``ER_TRUNCATE_PK_REFERRED`` exists
only in 11.4. Client-side driver codes (CCI ``CCI_ER_*``, JDBC ``UErrorCode``)
are never sent by the broker and are not listed.
"""

from __future__ import annotations

CUBRID_ERROR_CODES: dict[int, str] = {
    0: "No error",  # NO_ERROR
    -1: "Generic failure",  # ER_FAILED
    -2: "Internal error",  # ER_GENERIC_ERROR
    -3: "Out of memory",  # ER_OUT_OF_VIRTUAL_MEMORY
    -4: "Interrupted",  # ER_INTERRUPTED
    -13: "I/O read error",  # ER_IO_READ
    -14: "I/O write error",  # ER_IO_WRITE
    -15: "Out of disk space",  # ER_IO_WRITE_OUT_OF_SPACE
    -111: "Transaction aborted by the server",  # ER_TM_SERVER_DOWN_UNILATERALLY_ABORTED
    -493: "Syntax error",  # ER_PT_SYNTAX
    -494: "Semantic error",  # ER_PT_SEMANTIC
    -631: "NOT NULL constraint violation",  # ER_NULL_CONSTRAINT_VIOLATION
    -670: "Unique constraint violation",  # ER_BTREE_UNIQUE_FAILED
    -671: "Communication error",  # ER_CSS_RECV_OR_SEND
    -922: "Foreign key constraint violation",  # ER_FK_INVALID
    -924: "Update/delete restricted by foreign key",  # ER_FK_RESTRICT
    -1284: "Truncate restricted by foreign key",  # ER_TRUNCATE_PK_REFERRED
}

# Standard SQLSTATE codes mapped from native CUBRID error codes.
# Uses ISO/ANSI SQL standard states for interoperability.
CAS_ERROR_TO_SQLSTATE: dict[int, str] = {
    -1: "HY000",  # ER_FAILED: general error
    -2: "HY000",  # ER_GENERIC_ERROR: general error
    -3: "HY001",  # ER_OUT_OF_VIRTUAL_MEMORY: memory allocation error
    -4: "HY008",  # ER_INTERRUPTED: operation canceled, not a link failure
    -13: "HY000",  # ER_IO_READ
    -14: "HY000",  # ER_IO_WRITE
    -15: "HY000",  # ER_IO_WRITE_OUT_OF_SPACE
    -493: "42000",  # ER_PT_SYNTAX: not a specific missing-table condition
    -494: "42000",  # ER_PT_SEMANTIC
    -631: "23000",  # ER_NULL_CONSTRAINT_VIOLATION
    -670: "23000",  # ER_BTREE_UNIQUE_FAILED
    -671: "08S01",  # ER_CSS_RECV_OR_SEND: communication link failure
    -922: "23000",  # ER_FK_INVALID
    -924: "23000",  # ER_FK_RESTRICT
    -1284: "23000",  # ER_TRUNCATE_PK_REFERRED
}

# Native CUBRID error code → PEP 249 exception class name.
# Used by protocol._raise_error() for deterministic dispatch;
# text heuristics are only used as a fallback for code -1 (ER_FAILED).
CAS_ERROR_TO_EXCEPTION: dict[int, str] = {
    # IntegrityError (SQLSTATE 23xxx) — constraint violations
    -631: "IntegrityError",  # ER_NULL_CONSTRAINT_VIOLATION
    -670: "IntegrityError",  # ER_BTREE_UNIQUE_FAILED
    -922: "IntegrityError",  # ER_FK_INVALID
    -924: "IntegrityError",  # ER_FK_RESTRICT
    -1284: "IntegrityError",  # ER_TRUNCATE_PK_REFERRED (11.4; 10.2 reports -924)
    # ProgrammingError (SQLSTATE 42xxx) — SQL/object errors
    -493: "ProgrammingError",  # ER_PT_SYNTAX
    -494: "ProgrammingError",  # ER_PT_SEMANTIC
    # OperationalError — resource, interruption and communication failures
    -3: "OperationalError",  # ER_OUT_OF_VIRTUAL_MEMORY
    -4: "OperationalError",  # ER_INTERRUPTED
    -13: "OperationalError",  # ER_IO_READ
    -14: "OperationalError",  # ER_IO_WRITE
    -15: "OperationalError",  # ER_IO_WRITE_OUT_OF_SPACE
    -671: "OperationalError",  # ER_CSS_RECV_OR_SEND
    # InternalError — server-internal fault
    -2: "InternalError",  # ER_GENERIC_ERROR
}

# Default SQLSTATE when CAS_ERROR_TO_SQLSTATE has no entry for a code.
_DEFAULT_SQLSTATE: dict[str, str] = {
    "IntegrityError": "23000",
    "ProgrammingError": "42000",
    "OperationalError": "HY000",
    "InternalError": "HY000",
    "DataError": "22000",
    "InterfaceError": "HY000",
    "DatabaseError": "HY000",
}


def get_error_description(code: int) -> str | None:
    return CUBRID_ERROR_CODES.get(code)


def get_sqlstate(code: int) -> str | None:
    return CAS_ERROR_TO_SQLSTATE.get(code)
