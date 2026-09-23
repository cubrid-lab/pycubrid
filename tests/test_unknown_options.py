"""Unknown connection options are surfaced, not silently dropped (issue #377).

``Connection``/``AsyncConnection`` read a fixed set of options out of
``**kwargs`` and used to discard everything else without a word, so a typo such
as ``read_timout=30`` left the option with no effect and gave the caller no
signal. These tests pin the replacement contract: unknown keywords raise an
``UnknownConnectionOptionWarning`` (which callers can escalate to an error via
the standard ``warnings`` machinery), known keywords still work untouched, and
the allow-list cannot drift away from the real constructor signatures.
"""

from __future__ import annotations

import inspect
import re
import warnings
from pathlib import Path
from typing import Any

import pytest

import pycubrid
import pycubrid.aio
from pycubrid._connection_common import KNOWN_CONNECTION_OPTIONS
from pycubrid.aio.connection import AsyncConnection
from pycubrid.connection import Connection
from pycubrid.exceptions import UnknownConnectionOptionWarning

PACKAGE_ROOT = Path(pycubrid.__file__).parent


@pytest.fixture
def offline_connection(monkeypatch: pytest.MonkeyPatch) -> None:
    """Make ``Connection(...)`` construct without touching a socket.

    These tests are about keyword handling in ``__init__``; the handshake is
    covered by ``test_connection.py``.
    """

    def _no_connect(self: Connection) -> None:
        self._connected = True

    monkeypatch.setattr(Connection, "connect", _no_connect)


def build_connection(**kwargs: Any) -> Connection:
    return Connection("localhost", 33000, "testdb", "dba", "", **kwargs)


class TestUnknownOptionsWarn:
    def test_typoed_option_warns_and_is_still_ignored(self, offline_connection: None) -> None:
        """Regression test for the issue's example: ``read_timout`` for ``read_timeout``."""
        with pytest.warns(UnknownConnectionOptionWarning) as record:
            conn = build_connection(read_timout=30)

        assert len(record) == 1
        message = str(record[0].message)
        assert "read_timout" in message
        assert "did you mean 'read_timeout'?" in message
        # The option really is inert — the warning is the only signal there is.
        assert conn._read_timeout is None

    def test_camel_case_option_suggests_snake_case(self, offline_connection: None) -> None:
        with pytest.warns(UnknownConnectionOptionWarning) as record:
            build_connection(connectTimeout=5)

        assert "did you mean 'connect_timeout'?" in str(record[0].message)

    def test_unrecognisable_option_lists_supported_options(self, offline_connection: None) -> None:
        with pytest.warns(UnknownConnectionOptionWarning) as record:
            build_connection(totally_bogus=1)

        message = str(record[0].message)
        assert "'totally_bogus'" in message
        assert "did you mean" not in message
        for option in KNOWN_CONNECTION_OPTIONS:
            assert option in message

    def test_multiple_unknown_options_reported_in_one_warning(
        self, offline_connection: None
    ) -> None:
        with pytest.warns(UnknownConnectionOptionWarning) as record:
            build_connection(zzz_one=1, zzz_two=2)

        assert len(record) == 1
        message = str(record[0].message)
        assert "options" in message  # plural
        assert "'zzz_one'" in message and "'zzz_two'" in message

    def test_module_level_connect_warns(self, offline_connection: None) -> None:
        with pytest.warns(UnknownConnectionOptionWarning, match="read_timout"):
            pycubrid.connect(host="localhost", database="testdb", read_timout=30)

    @pytest.mark.asyncio
    async def test_async_connection_warns(self) -> None:
        with pytest.warns(UnknownConnectionOptionWarning, match="read_timout"):
            AsyncConnection("localhost", 33000, "testdb", "dba", "", read_timout=30)

    def test_warning_points_at_the_caller_not_at_pycubrid(self, offline_connection: None) -> None:
        """A warning blamed on pycubrid's own source would be useless to the caller."""
        with pytest.warns(UnknownConnectionOptionWarning) as direct:
            build_connection(read_timout=30)
        with pytest.warns(UnknownConnectionOptionWarning) as via_connect:
            pycubrid.connect(host="localhost", database="testdb", read_timout=30)

        for record in (direct[0], via_connect[0]):
            assert not Path(record.filename).is_relative_to(PACKAGE_ROOT), record.filename
        assert Path(direct[0].filename).name == "test_unknown_options.py"
        assert Path(via_connect[0].filename).name == "test_unknown_options.py"

    def test_callers_can_escalate_the_warning_to_an_error(self, offline_connection: None) -> None:
        with warnings.catch_warnings():
            warnings.simplefilter("error", UnknownConnectionOptionWarning)
            with pytest.raises(UnknownConnectionOptionWarning):
                build_connection(read_timout=30)

    def test_callers_can_silence_the_warning(self, offline_connection: None) -> None:
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("ignore", UnknownConnectionOptionWarning)
            build_connection(read_timout=30)
        assert caught == []


class TestKnownOptionsUnchanged:
    def test_known_kwargs_still_apply(self, offline_connection: None) -> None:
        with warnings.catch_warnings():
            warnings.simplefilter("error", UnknownConnectionOptionWarning)
            conn = build_connection(
                connect_timeout=7.5,
                read_timeout=11.0,
                no_backslash_escapes=True,
                enable_timing=True,
                fetch_size=42,
            )

        assert conn._connect_timeout == 7.5
        assert conn._read_timeout == 11.0
        assert conn._no_backslash_escapes is True
        assert conn.timing_stats is not None
        assert conn._fetch_size == 42

    def test_no_kwargs_does_not_warn(self, offline_connection: None) -> None:
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            build_connection()
        assert caught == []

    @pytest.mark.asyncio
    async def test_async_known_kwargs_still_apply(self) -> None:
        with warnings.catch_warnings():
            warnings.simplefilter("error", UnknownConnectionOptionWarning)
            conn = AsyncConnection(
                "localhost",
                33000,
                "testdb",
                "dba",
                "",
                connect_timeout=7.5,
                read_timeout=11.0,
                decode_collections=True,
                json_deserializer=None,
                no_backslash_escapes=True,
                enable_timing=True,
            )

        assert conn._connect_timeout == 7.5
        assert conn._read_timeout == 11.0
        assert conn._decode_collections is True
        assert conn._no_backslash_escapes is True
        assert conn.timing_stats is not None


class TestAllowListDoesNotDrift:
    """The allow-list is hand-written; keep it tied to the real signatures."""

    @staticmethod
    def _signature_options(func: Any) -> set[str]:
        return {
            name
            for name, param in inspect.signature(func).parameters.items()
            if name != "self" and param.kind is not inspect.Parameter.VAR_KEYWORD
        }

    @staticmethod
    def _kwargs_get_options(*relative_paths: str) -> set[str]:
        pattern = re.compile(r"""kwargs\.get\(\s*["'](\w+)["']""")
        names: set[str] = set()
        for relative in relative_paths:
            names |= set(pattern.findall((PACKAGE_ROOT / relative).read_text()))
        return names

    def test_allow_list_matches_public_signatures_and_kwargs_reads(self) -> None:
        declared = set()
        for func in (
            pycubrid.connect,
            pycubrid.aio.connect,
            Connection.__init__,
            AsyncConnection.__init__,
        ):
            declared |= self._signature_options(func)
        declared |= self._kwargs_get_options("connection.py", "aio/connection.py")

        assert declared == set(KNOWN_CONNECTION_OPTIONS), (
            "KNOWN_CONNECTION_OPTIONS drifted from the connection constructors: "
            f"missing={sorted(declared - KNOWN_CONNECTION_OPTIONS)} "
            f"stale={sorted(KNOWN_CONNECTION_OPTIONS - declared)}"
        )

    def test_warning_class_is_exported(self) -> None:
        assert "UnknownConnectionOptionWarning" in pycubrid.__all__
        assert pycubrid.UnknownConnectionOptionWarning is UnknownConnectionOptionWarning
        # A Python warning category, not the PEP 249 database warning.
        assert issubclass(UnknownConnectionOptionWarning, UserWarning)
        assert not issubclass(UnknownConnectionOptionWarning, pycubrid.Error)
        assert not issubclass(UnknownConnectionOptionWarning, pycubrid.Warning)
