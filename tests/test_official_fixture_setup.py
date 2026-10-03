"""Positioning fixture shape and cleanup without a database or oracle process."""

from __future__ import annotations

import re
from collections.abc import Sequence

import pytest

from . import test_official_differential as differential

pytestmark = pytest.mark.repo_tooling


class RecordingCursor:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[object, ...] | None]] = []
        self.batches: list[tuple[str, tuple[tuple[object, ...], ...]]] = []
        self.preexisting = False
        self.fail_insert = False
        self.insert_rowcount = 1537
        self.rowcount = -1
        self.closed = False

    def execute(self, sql: str, parameters: Sequence[object] | None = None) -> RecordingCursor:
        self.calls.append((sql, None if parameters is None else tuple(parameters)))
        if sql.startswith("INSERT"):
            if self.fail_insert:
                raise RuntimeError("fixture insert failed")
            self.rowcount = self.insert_rowcount
        return self

    def executemany(self, sql: str, parameters: Sequence[Sequence[object]]) -> RecordingCursor:
        self.batches.append((sql, tuple(tuple(row) for row in parameters)))
        if self.fail_insert:
            raise RuntimeError("fixture insert failed")
        self.rowcount = self.insert_rowcount
        return self

    def fetchall(self) -> list[tuple[str]]:
        return [("odnav444_fixture",)] if self.preexisting else []

    def close(self) -> None:
        self.closed = True


class RecordingConnection:
    def __init__(self) -> None:
        self.setup = RecordingCursor()
        self.autocommit = True
        self.closed = False

    def cursor(self) -> RecordingCursor:
        return self.setup

    def close(self) -> None:
        self.closed = True


@pytest.fixture
def ordinary(monkeypatch: pytest.MonkeyPatch) -> RecordingConnection:
    connection = RecordingConnection()
    monkeypatch.setattr(differential, "_ordinary", lambda: connection)
    return connection


def test_one_insert_has_exact_scalar_parameters_and_keeps_setup_mode(
    ordinary: RecordingConnection, monkeypatch: pytest.MonkeyPatch
) -> None:
    official = object()
    monkeypatch.setattr(differential, "_cubrid", official)
    observed: list[object] = []

    def observe(module: object) -> str:
        assert ordinary.autocommit is True
        assert ordinary.setup.rowcount == 1537
        observed.append(module)
        return "native" if module is differential.native else "official"

    assert differential._positioning_fixture(observe) == ("native", "official")
    assert ordinary.setup.batches == []
    inserts = [(sql, params) for sql, params in ordinary.setup.calls if sql.startswith("INSERT")]
    assert len(inserts) == 1
    sql, params = inserts[0]
    assert re.fullmatch(
        r"INSERT INTO odnav444_fixture VALUES \(\?,\s*\?\)(?:,\s*\(\?,\s*\?\))*", sql
    )
    assert sql.count("?") == 3074 and sql.count("(") == sql.count(")") == 1537
    assert params is not None and len(params) == 3074
    assert params[::2] == tuple(range(1, 1538))
    assert params[1::2] == tuple(str(value) for value in range(1, 1538))
    assert all(type(value) is int for value in params[::2])
    assert all(type(value) is str for value in params[1::2])
    assert observed == [differential.native, official]
    assert ordinary.setup.calls[0] == (
        "SELECT class_name FROM db_class WHERE class_name=?",
        ("odnav444_fixture",),
    )
    assert ordinary.setup.calls[-1] == ("DROP TABLE odnav444_fixture", None)
    assert ordinary.autocommit is True and ordinary.closed and ordinary.setup.closed


@pytest.mark.parametrize("rowcount", [0, 1536])
def test_incomplete_insert_is_rejected_before_observers(
    ordinary: RecordingConnection, rowcount: int
) -> None:
    ordinary.setup.insert_rowcount = rowcount
    observed: list[object] = []

    def observe(module: object) -> str:
        observed.append(module)
        return "must not run"

    with pytest.raises(AssertionError):
        differential._positioning_fixture(observe)
    assert observed == []
    assert ordinary.setup.calls[-1] == ("DROP TABLE odnav444_fixture", None)
    assert ordinary.closed and ordinary.setup.closed and ordinary.autocommit is True


def test_existing_table_is_refused_without_create_insert_or_drop(
    ordinary: RecordingConnection,
) -> None:
    ordinary.setup.preexisting = True
    with pytest.raises(AssertionError, match="already exists"):
        differential._positioning_fixture(lambda _module: pytest.fail("observer must not run"))
    assert len(ordinary.setup.calls) == 1 and ordinary.setup.batches == []
    assert ordinary.closed and ordinary.setup.closed


def test_insert_failure_cleans_only_owned_fixture(ordinary: RecordingConnection) -> None:
    ordinary.setup.fail_insert = True
    with pytest.raises(RuntimeError, match="fixture insert failed"):
        differential._positioning_fixture(lambda _module: pytest.fail("observer must not run"))
    assert ordinary.setup.calls[-1] == ("DROP TABLE odnav444_fixture", None)
    assert ordinary.closed and ordinary.setup.closed and ordinary.autocommit is True


@pytest.mark.parametrize("failed_observer", [1, 2])
def test_observer_failure_keeps_owned_cleanup(
    ordinary: RecordingConnection, failed_observer: int
) -> None:
    observed: list[object] = []

    def observe(module: object) -> str:
        observed.append(module)
        if len(observed) == failed_observer:
            raise RuntimeError("observer failed")
        return "first result"

    with pytest.raises(RuntimeError, match="observer failed"):
        differential._positioning_fixture(observe)
    assert len(observed) == failed_observer
    assert ordinary.setup.calls[-1] == ("DROP TABLE odnav444_fixture", None)
    assert ordinary.closed and ordinary.setup.closed and ordinary.autocommit is True
