"""Opt-in wide-value and two-connection ``executemany(prepared=True)`` run (#881).

Runs ``scripts/measure_prepared_wide_rows.py`` for 1,000 rows per connection,
for every value profile and mode, each case in fresh child processes. It checks
that every row arrives on every connection and that the ``EXECUTE_ARRAY``
frames have exactly the predicted size, and prints the measured heap peak, RSS
growth and time for the record. It sets no memory or time limit: those are
measured, not assumed. The 10,000-row figures in ``docs/PERFORMANCE.md`` come
from a manual run of the script.

Marked ``slow``, so the PR, main-push and release integration lanes
(``integration and not slow``) skip it; only the weekly bug-hunt slow lane and a
manual run select it::

    CUBRID_TEST_URL=cubrid://dba@localhost:33000/testdb \\
        pytest tests/test_integration_prepared_wide_rows.py -m "integration and slow" -s
"""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import pytest

pytestmark = [pytest.mark.integration, pytest.mark.slow, pytest.mark.no_escape_pin]

_SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "measure_prepared_wide_rows.py"
_SPEC = importlib.util.spec_from_file_location("_pycubrid_measure_prepared_wide_rows", _SCRIPT)
assert _SPEC is not None and _SPEC.loader is not None
measure_prepared_wide_rows = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = measure_prepared_wide_rows
_SPEC.loader.exec_module(measure_prepared_wide_rows)

ROWS = 1_000
# FC21 head: DATA_LENGTH, CAS_INFO, function code, sized handle, timeout and
# autocommit. Each bound value is a sized type byte (5 bytes) and a sized
# value (4 bytes and the value): the INT id is 13 bytes per row.
_HEAD = 8 + 1 + 8 + 8 + 5
_ID = 5 + 4 + 4
_VALUE = {
    "text-1k": 1000 + 1,  # ASCII text and a NUL
    "text-utf8-8k": 2700 * 3 + 1,  # UTF-8 Hangul and a NUL
    "bytes-1k": 1024,
    # element type byte, then per element a length word and "eKK-NNNNNNNNN" and a NUL
    "sequence-50": 1 + 50 * (4 + 13 + 1),
    # the packed handle, whose length the child reports
    "blob": None,
}


def _cases() -> list[tuple[str, str]]:
    return [
        (profile, mode)
        for profile in measure_prepared_wide_rows.PROFILES
        for mode in measure_prepared_wide_rows.MODES
        if profile != "blob" or mode in ("sync", "sync-2conn")
    ]


@pytest.mark.parametrize(("profile", "mode"), _cases())
def test_wide_prepared_batch(profile: str, mode: str) -> None:
    result = measure_prepared_wide_rows.measure(profile, mode, ROWS)
    print(json.dumps(result, sort_keys=True))
    connections = 2 if mode.endswith("2conn") else 1
    assert result["counts"] == [ROWS] * connections
    value = _VALUE[profile]
    if value is not None:
        values = [value] * connections
    else:
        # One packed handle per connection; its locator length comes from the server.
        values = result["blob_handle_bytes"]
        assert len(values) == connections
    expected = sorted(_HEAD + ROWS * (_ID + 5 + 4 + v) for v in values)
    assert result["execute_array_frames"] == expected
