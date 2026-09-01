"""Guard the shared ``no_backslash_escapes`` default across escape helpers.

CUBRID's server default is ``no_backslash_escapes=yes`` (a backslash is an
ordinary literal character). The public escaping helpers must agree on that
default so callers who omit the keyword get identical, server-consistent
behavior regardless of which helper they reach for. This test fails loudly if
any helper's default drifts out of sync (see issue #293).
"""

from __future__ import annotations

import inspect

from pycubrid._cursor_common import (
    CursorParamsMixin,
    bind_parameters,
    escape_string,
    format_parameter,
    split_on_placeholders,
)


def _default(func: object) -> bool:
    return inspect.signature(func).parameters["no_backslash_escapes"].default


class TestEscapeDefaults:
    def test_all_helpers_default_to_true(self):
        helpers = {
            "escape_string": escape_string,
            "format_parameter": format_parameter,
            "bind_parameters": bind_parameters,
            "split_on_placeholders": split_on_placeholders,
            "_escape_string": CursorParamsMixin._escape_string,
        }
        defaults = {name: _default(func) for name, func in helpers.items()}
        assert all(value is True for value in defaults.values()), defaults
