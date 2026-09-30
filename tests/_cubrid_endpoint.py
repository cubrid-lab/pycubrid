"""Single source of truth for the live CUBRID test endpoint (issues #522, #432).

Every integration module, ``tests/conftest.py`` and ``scripts/wait_for_cubrid.py``
resolve the server they talk to through :func:`resolve_endpoint`, so the
readiness probe, the fail-closed gate and the tests can never disagree about
which broker is under test.

Configuration
-------------
Integration is *enabled* when ``CUBRID_TEST_URL`` or ``CUBRID_TEST_HOST`` is
set to a non-empty value (:func:`is_configured`). The endpoint is then resolved
field by field, first match wins:

1. the per-field variable ``CUBRID_TEST_HOST`` / ``CUBRID_TEST_PORT`` /
   ``CUBRID_TEST_DB`` / ``CUBRID_TEST_USER`` / ``CUBRID_TEST_PASSWORD``;
2. the matching component of ``CUBRID_TEST_URL``
   (``cubrid://user[:password]@host[:port]/database``);
3. the documented default ``localhost`` / ``33000`` / ``testdb`` / ``dba`` / ``""``.

A ``CUBRID_TEST_URL`` without a scheme (for example ``1``) only enables
integration and contributes no endpoint fields. Empty per-field variables count
as unset, except ``CUBRID_TEST_PASSWORD``
where an empty value is the explicit empty password. Per-field variables keep
their historical precedence, so existing setups that export both behave as
before; a URL naming a non-default host or port is no longer silently ignored.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from contextlib import closing
from dataclasses import dataclass
from typing import Any
from urllib.parse import unquote, urlsplit

import pycubrid

URL_VAR = "CUBRID_TEST_URL"
HOST_VAR = "CUBRID_TEST_HOST"
PORT_VAR = "CUBRID_TEST_PORT"
DB_VAR = "CUBRID_TEST_DB"
USER_VAR = "CUBRID_TEST_USER"
PASSWORD_VAR = "CUBRID_TEST_PASSWORD"

DEFAULT_HOST = "localhost"
DEFAULT_PORT = 33000
DEFAULT_DB = "testdb"
DEFAULT_USER = "dba"
DEFAULT_PASSWORD = ""


@dataclass(frozen=True)
class CubridEndpoint:
    host: str
    port: int
    database: str
    user: str
    password: str

    def connect_kwargs(self) -> dict[str, Any]:
        return {
            "host": self.host,
            "port": self.port,
            "database": self.database,
            "user": self.user,
            "password": self.password,
        }

    def describe(self) -> str:
        """Human-readable endpoint without the password."""
        return f"{self.user}@{self.host}:{self.port}/{self.database}"


def is_configured(environ: Mapping[str, str] | None = None) -> bool:
    """True when the environment asks for a live integration run."""
    env = os.environ if environ is None else environ
    return bool(env.get(URL_VAR) or env.get(HOST_VAR))


def resolve_endpoint(environ: Mapping[str, str] | None = None) -> CubridEndpoint:
    """Resolve the test endpoint (per-field variables > URL > defaults).

    Raises ``ValueError`` for a malformed ``CUBRID_TEST_URL`` (foreign scheme,
    missing host, non-numeric port or a database name containing ``/`` or
    ``:``) so a typo surfaces instead of silently falling back to
    ``localhost:33000``.
    """
    env = os.environ if environ is None else environ
    raw_url = env.get(URL_VAR, "")
    # A scheme-less value (e.g. "1") is only the on/off switch (#432).
    url = urlsplit(raw_url if "://" in raw_url else "")
    if url.scheme and url.scheme.lower() != "cubrid":
        raise ValueError(f"{URL_VAR} must use the cubrid:// scheme, got {url.scheme!r}")
    if url.scheme and not url.hostname:
        raise ValueError(f"{URL_VAR} must name a host: cubrid://user@host[:port]/database")
    url_db = unquote(url.path.strip("/"))
    if "/" in url_db or ":" in url_db:
        raise ValueError(f"{URL_VAR} has an invalid database name {url_db!r}")
    try:
        url_port = url.port
    except ValueError as exc:
        raise ValueError(f"{URL_VAR} has an invalid port: {exc}") from None

    password = env.get(PASSWORD_VAR)
    if password is None:
        password = unquote(url.password) if url.password is not None else DEFAULT_PASSWORD
    # An explicit URL port (even 0) is kept so a bad one fails the probe
    # instead of silently falling back to the default broker.
    port = env.get(PORT_VAR) or (url_port if url_port is not None else DEFAULT_PORT)
    return CubridEndpoint(
        host=env.get(HOST_VAR) or url.hostname or DEFAULT_HOST,
        port=int(port),
        database=env.get(DB_VAR) or url_db or DEFAULT_DB,
        user=env.get(USER_VAR) or unquote(url.username or "") or DEFAULT_USER,
        password=password,
    )


def probe(
    endpoint: CubridEndpoint,
    *,
    connect_timeout: float | None = None,
    read_timeout: float | None = None,
) -> None:
    """Connect and run ``SELECT 1``; raise on any failure, always releasing resources.

    Timeouts default to ``CUBRID_TEST_CONNECT_TIMEOUT`` / ``CUBRID_TEST_READ_TIMEOUT``
    (5 seconds) so a broker that accepts TCP but stalls cannot hang the run.
    """
    if connect_timeout is None:
        connect_timeout = float(os.environ.get("CUBRID_TEST_CONNECT_TIMEOUT", "5"))
    if read_timeout is None:
        read_timeout = float(os.environ.get("CUBRID_TEST_READ_TIMEOUT", "5"))
    with closing(
        pycubrid.connect(
            **endpoint.connect_kwargs(),
            connect_timeout=connect_timeout,
            read_timeout=read_timeout,
        )
    ) as conn:
        with closing(conn.cursor()) as cur:
            cur.execute("SELECT 1")


# Resolved once at import for the module-level constants the suites share. A
# malformed URL must not break collection (and so the offline suite): fall back
# to the defaults here; the conftest gate re-resolves and turns the ValueError
# into an error on every integration test.
try:
    ENDPOINT = resolve_endpoint()
except ValueError:
    ENDPOINT = resolve_endpoint({})
TEST_HOST = ENDPOINT.host
TEST_PORT = ENDPOINT.port
TEST_DB = ENDPOINT.database
TEST_USER = ENDPOINT.user
TEST_PASSWORD = ENDPOINT.password
