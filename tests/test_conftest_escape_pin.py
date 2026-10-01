"""Regression test for the escape-mode opt-out classification (#524).

Before this fix, ``tests/conftest.py``'s autouse ``_skip_backslash_probe``
fixture decided whether to skip the backslash-escape-mode default pin by
checking whether a filename *substring* (e.g. ``"test_integration"``)
appeared anywhere in the test's path. That silently opted every
``test_integration_*.py`` module out of the pin, whether or not it actually
negotiated against a live server: a brand new ``test_integration_foo.py``
module would have opted out just by being named that way, without ever
asking to.

The opt-out is now the explicit ``no_escape_pin`` marker (carried directly by
the modules that need it, alongside the existing ``integration`` marker for
those that also gate on a live server), so filename similarity has no effect
on the decision any more. This test drives the real fixture through
``pytester`` in a fresh subprocess and checks both directions: an unmarked
module that merely *looks like* an integration test stays pinned, and a
module carrying the marker still opts out.
"""

from __future__ import annotations

from pathlib import Path

import pytest

pytest_plugins = ["pytester"]

_REPO_ROOT = Path(__file__).resolve().parents[1]

_CONFTEST = f"""
import sys

sys.path.insert(0, {str(_REPO_ROOT)!r})

# Re-export the real autouse fixture under test; pytest recognizes any
# ``@pytest.fixture``-decorated object present in a conftest's namespace,
# regardless of where it was originally defined.
from tests.conftest import _skip_backslash_probe  # noqa: F401
"""

_CHECK_STILL_PINNED = """
import pycubrid.connection as connmod

_ORIGINAL = connmod.Connection._negotiate_backslash_escapes


def test_negotiation_was_pinned():
    # The pin was applied: negotiation is monkeypatched away from the real
    # method, i.e. this module did NOT opt out.
    assert connmod.Connection._negotiate_backslash_escapes is not _ORIGINAL
"""

_CHECK_OPTED_OUT = """
import pytest

import pycubrid.connection as connmod

pytestmark = pytest.mark.no_escape_pin

_ORIGINAL = connmod.Connection._negotiate_backslash_escapes


def test_negotiation_left_untouched():
    # Real negotiation stays active: this module opted out of the pin.
    assert connmod.Connection._negotiate_backslash_escapes is _ORIGINAL
"""


def test_lookalike_filename_without_marker_is_not_opted_out(pytester: pytest.Pytester) -> None:
    """A new ``test_integration_foo.py`` with no marker must stay pinned.

    Regression for #524: under the old filename-substring list, merely
    naming a module ``test_integration_<anything>.py`` matched the
    ``"test_integration"`` fragment and silently skipped the pin. It must now
    be pinned like any other unmarked test, because it carries neither the
    ``integration`` marker nor the explicit ``no_escape_pin`` marker.
    """
    pytester.makeconftest(_CONFTEST)
    pytester.makepyfile(test_integration_foo=_CHECK_STILL_PINNED)
    result = pytester.runpytest_inprocess("-p", "no:cacheprovider")
    result.assert_outcomes(passed=1)


def test_explicit_marker_still_opts_a_module_out(pytester: pytest.Pytester) -> None:
    """A module carrying ``@pytest.mark.no_escape_pin`` still opts out."""
    pytester.makeconftest(_CONFTEST)
    pytester.makepyfile(test_something_live=_CHECK_OPTED_OUT)
    result = pytester.runpytest_inprocess("-p", "no:cacheprovider")
    result.assert_outcomes(passed=1)
