"""pycubrid.aio — Async (asyncio) interface for the pycubrid CUBRID driver."""

from __future__ import annotations

import ssl as ssl_module
from typing import Any

from pycubrid.aio.connection import AsyncConnection


async def connect(
    host: str = "localhost",
    port: int = 33000,
    database: str = "",
    user: str = "dba",
    password: str = "",  # nosec B107 — PEP 249 default empty password
    decode_collections: bool = False,
    json_deserializer: Any = None,
    ssl: bool | ssl_module.SSLContext | None = None,
    charset: str = "utf-8",
    **kwargs: Any,
) -> AsyncConnection:
    """Create a new async database connection.

    Args:
        host: CUBRID server hostname or IP address.
        port: CUBRID broker port (default 33000).
        database: Database name.
        user: Database user (default ``"dba"``).
        password: Database password (default ``""``).
        charset: Python codec (or CUBRID name ``utf8``/``euckr``/``iso88591``)
            for SQL text, bound strings, credentials, character values,
            metadata names and error messages; set it to the database
            charset. JSON is always UTF-8. Default ``"utf-8"``.
        **kwargs: Additional connection parameters (``autocommit``,
            ``fetch_size``, ``connect_timeout``, ``read_timeout``,
            ``no_backslash_escapes``, ``enable_timing``). An unrecognised
            keyword is ignored, but reports an
            :class:`~pycubrid.exceptions.UnknownConnectionOptionWarning`
            so that a typo is not swallowed silently.

    Returns:
        A connected :class:`AsyncConnection` instance.
    """
    connection_kwargs: dict[str, Any] = {
        "host": host,
        "port": port,
        "database": database,
        "user": user,
        "password": password,
        "decode_collections": decode_collections,
        "json_deserializer": json_deserializer,
        "charset": charset,
        **kwargs,
    }
    if ssl is not None:
        connection_kwargs["ssl"] = ssl

    conn = AsyncConnection(**connection_kwargs)
    await conn.connect()
    return conn


__all__ = [
    "connect",
    "AsyncConnection",
]
