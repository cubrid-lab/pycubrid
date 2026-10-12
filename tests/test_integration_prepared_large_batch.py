"""Opt-in large ``executemany(prepared=True)`` measurement (#854).

Runs ``scripts/measure_prepared_batch.py`` for 10,000 and 100,000 rows, sync and
async, with a list and with a one-shot generator, each case in fresh child
processes. It checks that every row arrives, that ``rowcount`` adds up and that
the ``EXECUTE_ARRAY`` frame has exactly the predicted size, and prints the
measured peak RSS, ``tracemalloc`` peak, request bytes and time for the record.
It sets no memory or time limit: those are measured, not assumed.

Marked ``slow``, so the PR, main-push and release integration lanes
(``integration and not slow``) skip it; only the weekly bug-hunt slow lane and a
manual run select it::

    CUBRID_TEST_URL=cubrid://dba@localhost:33000/testdb \\
        pytest tests/test_integration_prepared_large_batch.py -m "integration and slow" -s
"""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import pytest

pytestmark = [pytest.mark.integration, pytest.mark.slow, pytest.mark.no_escape_pin]

_SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "measure_prepared_batch.py"
_SPEC = importlib.util.spec_from_file_location("_pycubrid_measure_prepared_batch", _SCRIPT)
assert _SPEC is not None and _SPEC.loader is not None
measure_prepared_batch = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = measure_prepared_batch
_SPEC.loader.exec_module(measure_prepared_batch)

# FC21 head: DATA_LENGTH, CAS_INFO, function code, sized handle, timeout and
# autocommit. Per row: INT id, STRING "v%09d" (10 bytes and a NUL), INT n, each
# a sized type byte and a sized value.
_HEAD = 8 + 1 + 8 + 8 + 5
_PER_ROW = 2 * (5 + 4 + 4) + (5 + 4 + 11)


@pytest.mark.parametrize("rows", [10_000, 100_000])
@pytest.mark.parametrize("mode", measure_prepared_batch.MODES)
def test_large_prepared_batch(mode: str, rows: int) -> None:
    result = measure_prepared_batch.measure(mode, rows)
    print(json.dumps(result, sort_keys=True))
    assert result["rowcount"] == rows
    assert result["count"] == rows
    assert result["execute_array_bytes"] == _HEAD + rows * _PER_ROW
    assert result["prepare_bytes"] > 0
