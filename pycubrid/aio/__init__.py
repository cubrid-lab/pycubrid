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
    **kwargs: Any,
) -> AsyncConnection:
    """Create a new async database connection.

    Args:
        host: CUBRID server hostname or IP address.
        port: CUBRID broker port (default 33000).
        database: Database name.
        user: Database user (default ``"dba"``).
        password: Database password (default ``""``).
        **kwargs: Additional connection parameters
            (``autocommit``, ``connect_timeout``).

    Returns:
        A connected :class:`AsyncConnection` instance.
    """
    autocommit = kwargs.pop("autocommit", False)
    connection_kwargs: dict[str, Any] = {
        "host": host,
        "port": port,
        "database": database,
        "user": user,
        "password": password,
        "decode_collections": decode_collections,
        "json_deserializer": json_deserializer,
        **kwargs,
    }
    if ssl is not None:
        connection_kwargs["ssl"] = ssl

    conn = AsyncConnection(**connection_kwargs)
    await conn.connect()
    if autocommit:
        await conn.set_autocommit(True)
    return conn


__all__ = [
    "connect",
    "AsyncConnection",
]
