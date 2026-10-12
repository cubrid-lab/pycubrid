"""Fresh import orders and preserved cursor/exception boundaries (#561, #856)."""

from __future__ import annotations

import ast
import subprocess
import sys
from pathlib import Path

import pytest

from pycubrid._cursor_common import _raise_batch_error
from pycubrid.exceptions import DatabaseError, ProgrammingError


@pytest.mark.parametrize(
    "first",
    [
        "pycubrid",
        "pycubrid.connection",
        "pycubrid.cursor",
        "pycubrid.aio",
        "pycubrid.aio.connection",
        "pycubrid.aio.cursor",
        "pycubrid._cursor_common",
        "pycubrid.exceptions",
        "pycubrid.protocol",
        "pycubrid._prepared_binding",
    ],
)
def test_import_order_preserves_real_cursor_factories_and_aliases(first: str) -> None:
    code = """
import importlib
import sys
import weakref
from pathlib import Path
importlib.import_module(sys.argv[1])
import pycubrid
from pycubrid import _connection_common, _cursor_common, connection, cursor
from pycubrid.aio.connection import AsyncConnection
from pycubrid.aio.cursor import AsyncCursor
from pycubrid.exceptions import InterfaceError
from pycubrid.timing import TimingStats

assert Path(pycubrid.__file__).resolve().parent.parent == Path.cwd()
assert connection._resolve_ssl_context is _connection_common.resolve_ssl_context
assert _connection_common._resolve_ssl_context is _connection_common.resolve_ssl_context
assert cursor._DML_BATCH_VERBS is _cursor_common.DML_BATCH_VERBS
assert cursor._extract_first_keyword is _cursor_common.extract_first_keyword
assert cursor._split_on_placeholders is _cursor_common.split_on_placeholders
for factory, expected in ((connection.Connection, cursor.Cursor), (AsyncConnection, AsyncCursor)):
    conn = factory.__new__(factory)
    conn._connected = True
    conn._cursors = weakref.WeakSet()
    conn._fetch_size = 37
    conn._timing = TimingStats()
    cur = conn.cursor()
    assert type(cur) is expected
    assert cur._connection is conn and cur in conn._cursors
    assert cur._fetch_size == 37 and cur._timing is conn._timing
    conn._connected = False
    try:
        conn.cursor()
    except InterfaceError as error:
        assert str(error) == 'connection is closed'
    else:
        raise AssertionError('closed connection constructed a cursor')
"""
    result = subprocess.run(
        [sys.executable, "-c", code, first],
        cwd=Path(__file__).resolve().parents[1],
        capture_output=True,
        text=True,
        check=False,
        timeout=10,
    )
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.parametrize(
    "code, expected, sqlstate",
    [(-493, ProgrammingError, "42000"), (-9999, DatabaseError, "HY000")],
)
def test_shared_batch_error_preserves_known_and_unknown_metadata(
    code: int, expected: type[DatabaseError], sqlstate: str
) -> None:
    with pytest.raises(expected) as caught:
        _raise_batch_error({"code": code, "message": "batch failure"})
    assert type(caught.value) is expected
    assert caught.value.msg == "batch failure"
    assert caught.value.code == caught.value.errno == code
    assert caught.value.sqlstate == sqlstate


_PACKAGE = Path(__file__).resolve().parents[1] / "pycubrid"


def _module_defined_names(source: str) -> set[str]:
    """Return the names a module binds at top level by definition, not import."""
    names: set[str] = set()
    for node in ast.parse(source).body:
        if isinstance(node, (ast.FunctionDef, ast.ClassDef)):
            names.add(node.name)
        elif isinstance(node, ast.Assign):
            names.update(t.id for t in node.targets if isinstance(t, ast.Name))
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            names.add(node.target.id)
    return names


def test_prepared_binding_names_are_reexported_from_protocol_as_the_same_objects() -> None:
    # The binding types and encoders moved out of protocol.py (#856); every
    # name, private or not, keeps resolving on pycubrid.protocol to the very
    # same object, so isinstance() checks and existing imports are unchanged.
    from pycubrid import _prepared_binding, protocol

    names = _module_defined_names((_PACKAGE / "_prepared_binding.py").read_text())
    assert {"_PreparedScalar", "_encode_prepared_rows", "_write_prepared_binding"} <= names
    for name in sorted(names):
        assert getattr(protocol, name) is getattr(_prepared_binding, name), name
    # protocol.py defines none of them itself (no second copy to drift).
    assert not names & _module_defined_names((_PACKAGE / "protocol.py").read_text())


def _module_path(module: str) -> Path | None:
    """Return the source file of a pycubrid module or subpackage, if it exists."""
    path = _PACKAGE / (module.removeprefix("pycubrid").lstrip(".").replace(".", "/") + ".py")
    if path.exists():
        return path
    init = path.with_suffix("") / "__init__.py"
    return init if init.exists() else None


def _package_imports(module: str) -> set[str]:
    """Return the pycubrid modules ``module`` imports (relative or absolute).

    ``from . import protocol`` and ``from pycubrid import protocol`` name the
    module in the alias, not in ``ImportFrom.module``, so an alias that is a
    submodule of the target counts as an import of that submodule.
    """
    path = _module_path(module)
    assert path is not None, module
    package = module if path.name == "__init__.py" else module.rsplit(".", 1)[0]
    found: set[str] = set()
    for node in ast.walk(ast.parse(path.read_text())):
        if isinstance(node, ast.ImportFrom):
            if node.level:
                base = package.split(".")[: len(package.split(".")) - node.level + 1]
                target = ".".join(base + ([node.module] if node.module else []))
            else:
                target = node.module or ""
            if target.startswith("pycubrid."):
                found.add(target)
            if target == "pycubrid" or target.startswith("pycubrid."):
                found.update(
                    f"{target}.{alias.name}"
                    for alias in node.names
                    if _module_path(f"{target}.{alias.name}") is not None
                )
        elif isinstance(node, ast.Import):
            found.update(a.name for a in node.names if a.name.startswith("pycubrid."))
    return found


def test_prepared_binding_does_not_import_protocol_directly_or_transitively() -> None:
    # The dependency is one-way (protocol -> _prepared_binding); a back edge
    # would make the import order matter (#856).
    seen: set[str] = set()
    pending = ["pycubrid._prepared_binding"]
    while pending:
        module = pending.pop()
        if module in seen:
            continue
        seen.add(module)
        pending.extend(_package_imports(module))
    assert "pycubrid.protocol" not in seen, sorted(seen)
