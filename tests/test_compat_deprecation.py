"""The retirement warning for pycubrid.compat (#1120)."""

from __future__ import annotations

import subprocess
import sys
import warnings
from collections.abc import Callable
from pathlib import Path
from typing import Any
from unittest import mock

import pytest

from pycubrid import InterfaceError
from pycubrid.compat import cubriddb, native
from pycubrid.compat._deprecation import MESSAGE, MIGRATION_URL

ROOT = Path(__file__).resolve().parent.parent
BAD_URL = "not-a-cubrid-url"
FACTORIES: list[Callable[..., Any]] = [
    native.connect,
    native.connection,
    cubriddb.connect,
    cubriddb.Connect,
    cubriddb.connection,
    cubriddb.Connection,
]


def _run(*args: str, cwd: Path | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, *args], capture_output=True, text=True, timeout=60, cwd=cwd, check=False
    )


@pytest.mark.parametrize("factory", FACTORIES, ids=lambda f: f"{f.__module__}.{f.__name__}")
def test_every_factory_warns_once_at_the_caller(factory: Callable[..., Any]) -> None:
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        with pytest.raises(InterfaceError):
            factory(BAD_URL)

    assert [w.category for w in caught] == [FutureWarning]
    assert str(caught[0].message) == MESSAGE
    assert caught[0].filename == __file__


def test_the_message_names_the_guide_and_the_removal() -> None:
    assert MIGRATION_URL in MESSAGE
    assert "will be removed" in MESSAGE
    assert MIGRATION_URL.startswith("https://cubrid-lab.github.io/pycubrid/")


def test_the_url_points_at_a_published_guide() -> None:
    name = MIGRATION_URL.rstrip("/").rsplit("/", 1)[-1]
    assert (ROOT / "docs" / f"{name}.md").is_file()
    assert (ROOT / "docs" / "ko" / f"{name}.md").is_file()
    nav = (ROOT / "mkdocs.yml").read_text(encoding="utf-8")
    assert f" {name}.md" in nav
    assert f" ko/{name}.md" in nav


def test_warnings_as_errors_stops_before_any_socket_work() -> None:
    with mock.patch.object(native._DriverConnection, "__init__") as driver_init:
        with warnings.catch_warnings():
            warnings.simplefilter("error", FutureWarning)
            with pytest.raises(FutureWarning):
                native.connect("CUBRID:localhost:33000:testdb:::")
    driver_init.assert_not_called()


def test_a_suppressed_warning_keeps_the_factory_working() -> None:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", FutureWarning)
        with pytest.raises(InterfaceError):
            native.connect(BAD_URL)


def test_importing_ordinary_and_compat_modules_does_not_warn() -> None:
    result = _run(
        "-W",
        "error",
        "-c",
        "import pycubrid, pycubrid.aio, pycubrid.compat, pycubrid.compat.native, "
        "pycubrid.compat.cubriddb, pycubrid.compat.cursors",
    )
    assert result.returncode == 0, result.stderr


def test_warning_is_visible_by_default_from_an_imported_module(tmp_path: Path) -> None:
    (tmp_path / "app_module.py").write_text(
        "from pycubrid.compat import native\n"
        "def run():\n"
        "    try:\n"
        "        native.connect('bad')\n"
        "    except Exception:\n"
        "        pass\n",
        encoding="utf-8",
    )
    result = _run("-c", "import app_module; app_module.run()", cwd=tmp_path)
    assert result.returncode == 0, result.stderr
    assert "FutureWarning" in result.stderr
    assert MIGRATION_URL in result.stderr
    assert "app_module.py:4" in result.stderr


def test_ordinary_connect_does_not_warn() -> None:
    import pycubrid

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        with mock.patch.object(pycubrid.connection.Connection, "__init__", return_value=None):
            pycubrid.connect(host="localhost", database="testdb", user="dba")
    assert [w for w in caught if w.category is FutureWarning] == []
