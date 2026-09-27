"""Check the pinned API catalog's accounting, not native-driver compatibility."""

from __future__ import annotations

import inspect
import json
from pathlib import Path
from typing import Any

import pycubrid
import pycubrid.connection
import pycubrid.constants
import pycubrid.cursor
import pycubrid.lob
import pycubrid.types
import pytest

CATALOG_PATH = Path(__file__).parent / "fixtures" / "official_api_inventory.json"
CATALOG = json.loads(CATALOG_PATH.read_text(encoding="utf-8"))
OPERATIONS: dict[str, dict[str, Any]] = {row["id"]: row for row in CATALOG["operations"]}


def test_inventory_identity_and_records() -> None:
    assert CATALOG["format_version"] == 1
    assert CATALOG["upstream"]["revision"] == "e75ec36b2a92b8829a49a967a29a1fbb9d7c322b"
    assert CATALOG["upstream"]["runtime_module"] == "_cubrid"
    assert len(OPERATIONS) == len(CATALOG["operations"])
    assert {row["namespace"] for row in OPERATIONS.values()} == {
        "CUBRIDdb",
        "documented_cubrid",
    }
    for row in OPERATIONS.values():
        assert row["id"].startswith(row["namespace"] + ".")
        assert row["signature"] and isinstance(row["defaults"], dict)
        assert row["contract"]["returns"] and row["contract"]["errors"]
        assert row["pycubrid"]["relation"] in {
            "analogue",
            "different",
            "gap",
            "internal_analogue",
            "upstream_stub",
            "upstream_discrepancy",
        }
        assert row["pycubrid"]["note"] and row["tracking"]
        assert all(isinstance(number, int) and number > 0 for number in row["tracking"])
        assert row["sources"]
        for source in row["sources"]:
            assert source["path"] in {
                "CUBRIDdb/__init__.py",
                "CUBRIDdb/connections.py",
                "CUBRIDdb/cursors.py",
                "CUBRIDdb/FIELD_TYPE.py",
                "cubrid_ext/python_cubrid.c",
            }
            assert isinstance(source["line"], int) and source["line"] > 0
        if row["pycubrid"]["target"] is None:
            assert row["pycubrid"]["relation"] in {"gap", "upstream_discrepancy"}
        assert not any(alias.startswith("CUBRIDdb.connection.") for alias in row.get("aliases", []))


@pytest.mark.parametrize(
    "row",
    [row for row in CATALOG["operations"] if row["pycubrid"]["target"] is not None],
    ids=lambda row: row["id"],
)
def test_named_current_target_exists_without_connection(row: dict[str, Any]) -> None:
    target = row["pycubrid"]["target"]
    prefix, *attributes = target.split(".")
    assert prefix == "pycubrid"
    value: object = pycubrid
    for name in attributes:
        value = inspect.getattr_static(value, name)


@pytest.mark.parametrize(
    "owner,names",
    [
        ("", "connect escape_string"),
        (
            "connection",
            "close cursor lob set commit rollback ping server_version client_version "
            "set_autocommit set_isolation_level insert_id schema_info escape_string batch_execute",
        ),
        (
            "cursor",
            "close prepare bind_param bind_lob bind_set execute affected_rows fetch_row "
            "fetch_lob data_seek num_fields num_rows row_tell row_seek result_info next_result",
        ),
        ("lob", "export imports write read seek close"),
        ("set", "imports"),
    ],
)
def test_native_public_method_tables_are_accounted(owner: str, names: str) -> None:
    prefix = "documented_cubrid." + (owner + "." if owner else "")
    actual = {
        row["id"][len(prefix) :]
        for row in OPERATIONS.values()
        if row["id"].startswith(prefix)
        and row["kind"] in {"method", "function", "protocol"}
        and "." not in row["id"][len(prefix) :]
    }
    assert actual == set(names.split())


def test_wrapper_operations_and_documented_attributes_are_accounted() -> None:
    methods = {
        "connections.Connection": "set_fetch_value_converter cursor set_autocommit get_autocommit "
        "commit rollback set ping get_last_insert_id close escape_string server_version batch_execute",
        "cursors.BaseCursor": "close execute executemany fetchone fetchmany fetchall setinputsizes "
        "setoutputsizes nextset callproc __iter__ next __next__",
    }
    for owner, names in methods.items():
        assert {f"CUBRIDdb.{owner}.{name}" for name in names.split()} <= OPERATIONS.keys()
    for name in ("BaseCursor", "Cursor", "DictCursor"):
        assert f"CUBRIDdb.cursors.{name}" in OPERATIONS
    for name in ("autocommit", "charset", "fetch_value_converter", "connection", "default_cursor"):
        assert f"CUBRIDdb.connections.Connection.{name}" in OPERATIONS
    for name in ("arraysize", "rowcount", "description", "charset", "con"):
        assert f"CUBRIDdb.cursors.BaseCursor.{name}" in OPERATIONS
    fields = "CHAR VARCHAR NCHAR VARNCHAR BIT VARBIT NUMERIC INT SMALLINT MONETARY FLOAT DOUBLE "
    fields += "DATE TIME TIMESTAMP SET MULTISET SEQUENCE OBJECT BIGINT DATETIME BLOB CLOB STRING"
    assert {f"CUBRIDdb.FIELD_TYPE.{name}" for name in fields.split()} <= OPERATIONS.keys()
    assert "CUBRIDdb.cursors.INT_MIN" in OPERATIONS
    assert "CUBRIDdb.cursors.INT_MAX" in OPERATIONS


def test_native_constants_constructors_and_exception_exports_are_accounted() -> None:
    constants = {
        "CUBRID_EXEC_ASYNC",
        "CUBRID_EXEC_QUERY_ALL",
        "CUBRID_EXEC_QUERY_INFO",
        "CUBRID_EXEC_ONLY_QUERY_PLAN",
        "CUBRID_EXEC_THREAD",
        "CUBRID_REP_CLASS_COMMIT_INSTANCE",
        "CUBRID_REP_CLASS_REP_INSTANCE",
        "CUBRID_SERIALIZABLE",
        "SEEK_CUR",
        "SEEK_SET",
        "SEEK_END",
    }
    schema = "TABLE VIEW QUERY_SPEC ATTRIBUTE TABLE_ATTRIBUTE METHOD TABLE_METHOD METHOD_FILE "
    schema += "SUPERTABLE SUBTABLE CONSTRAINT TRIGGER TABLE_PRIVILEGE COLUMN_PRIVILEGE "
    schema += "DIRECT_SUPER_TABLE PRIMARY_KEY IMPORTED_KEYS EXPORTED_KEYS CROSS_REFERENCE"
    constants.update("CUBRID_SCH_" + name for name in schema.split())
    actual = {
        row["id"].removeprefix("documented_cubrid.")
        for row in OPERATIONS.values()
        if row["namespace"] == "documented_cubrid" and row["kind"] == "constant"
    }
    assert actual == constants
    for name in ("connection", "cursor", "lob", "set"):
        assert OPERATIONS[f"documented_cubrid.{name}"]["kind"] == "constructor"
    exceptions = "Error InterfaceError DatabaseError DataError OperationalError IntegrityError "
    exceptions += "InternalError ProgrammingError NotSupportedError"
    for name in exceptions.split():
        assert OPERATIONS[f"documented_cubrid.{name}"]["aliases"] == [f"CUBRIDdb.{name}"]
    for name in ("DATE", "TIME", "TIMESTAMP", "Cursor", "DictCursor"):
        assert OPERATIONS[f"CUBRIDdb.{name}"]["kind"] == "advertised_export"


def test_discrepancies_and_public_gaps_cannot_be_parity_passes() -> None:
    assert OPERATIONS["documented_cubrid.cursor.bind_param"]["defaults"] == {"bind_type": 0}
    assert OPERATIONS["documented_cubrid.cursor.execute"]["defaults"] == {
        "option": 0,
        "max_col_size": 0,
    }
    assert (
        "not_null,scale,precision"
        in OPERATIONS["documented_cubrid.cursor.result_info"]["contract"]["returns"]
    )
    for name in ("prepare", "bind_param", "bind_set", "bind_lob", "fetch_lob", "result_info"):
        row = OPERATIONS[f"documented_cubrid.cursor.{name}"]
        assert row["pycubrid"]["target"] is None and row["tracking"]
    assert (
        OPERATIONS["CUBRIDdb.cursors.BaseCursor.callproc"]["pycubrid"]["relation"]
        == "upstream_stub"
    )
    assert "charset" in OPERATIONS["CUBRIDdb.connections.Connection.__init__"]["defaults"]
    assert OPERATIONS["CUBRIDdb.connections.Connection.charset"]["pycubrid"]["relation"] == "gap"
    assert "int" in OPERATIONS["documented_cubrid.connection.insert_id"]["contract"]["returns"]
    assert "list" in OPERATIONS["documented_cubrid.connection.schema_info"]["contract"]["returns"]
    for name in ("althosts", "rctime"):
        assert f"documented_cubrid.connection.url_options.{name}" in OPERATIONS
    exclusions = CATALOG["exclusions"]
    assert any(row["surface"] == "_cubrid.cursor._set_charset_name" for row in exclusions)
    assert all(row["reason"] for row in exclusions)
