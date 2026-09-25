"""Helpers for empty executemany() no-op result-state reset (issue #376)."""

from __future__ import annotations

from typing import Any


def reset_empty_executemany(cursor: Any) -> Any:
    """Clear prior result-set state after a no-op empty executemany()."""
    cursor._description = None
    cursor._rows = []
    cursor._row_index = 0
    cursor._fetched_count = 0
    cursor._query_handle = None
    cursor._rowcount = 0
    return cursor
