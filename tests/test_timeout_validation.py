"""Regression coverage for connection timeout validation (issue #367)."""

from unittest.mock import patch

import pytest

from pycubrid.aio.connection import AsyncConnection
from pycubrid.connection import Connection


INVALID_TIMEOUTS = (-1, -0.25, float("nan"), float("inf"), float("-inf"))


def _sync_connection(**kwargs):
    return Connection("localhost", 33000, "testdb", "dba", "", **kwargs)


def _async_connection(**kwargs):
    return AsyncConnection("localhost", 33000, "testdb", "dba", "", **kwargs)


@pytest.mark.parametrize("field", ("connect_timeout", "read_timeout"))
@pytest.mark.parametrize("value", INVALID_TIMEOUTS)
def test_sync_rejects_invalid_timeout_before_socket(field: str, value: float) -> None:
    with patch("pycubrid.connection.socket.create_connection") as create_connection:
        with pytest.raises(ValueError, match=field):
            _sync_connection(**{field: value})
    create_connection.assert_not_called()


@pytest.mark.parametrize("field", ("connect_timeout", "read_timeout"))
@pytest.mark.parametrize("value", INVALID_TIMEOUTS)
def test_async_rejects_invalid_timeout_during_init(field: str, value: float) -> None:
    with pytest.raises(ValueError, match=field):
        _async_connection(**{field: value})


@pytest.mark.parametrize("field", ("connect_timeout", "read_timeout"))
@pytest.mark.parametrize("value", (None, 0, 0.0, 1, 1.5))
def test_common_timeout_validation_preserves_supported_values(
    field: str, value: float | None
) -> None:
    conn = AsyncConnection("localhost", 33000, "testdb", "dba", "", **{field: value})
    assert getattr(conn, f"_{field}") == value
