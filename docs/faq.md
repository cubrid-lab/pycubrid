# Frequently Asked Questions

Common pycubrid usage questions with practical answers.

---

??? "How do I connect to a remote CUBRID server?"
    Use the remote host and broker port directly in `connect()`.

    ```python
    from __future__ import annotations

    import pycubrid

    conn = pycubrid.connect(
        host="db.example.com",
        port=33000,
        database="production",
        user="app_user",
        password="secret",
        connect_timeout=15.0,
    )
    ```

    !!! warning
        If your deployment uses broker-to-CAS port redirection, ensure network rules allow the redirected CAS port(s).

??? "What Python versions are supported?"
    pycubrid follows its package support matrix and CI-tested versions.

    !!! note
        Check the latest compatibility table in [Support Matrix](SUPPORT_MATRIX.md).

??? "How do I handle LOB data?"
    For most applications, pass `str`/`bytes` directly in parameterized SQL.

    ```python
    cur.execute(
        "INSERT INTO docs (title, content) VALUES (?, ?)",
        ["Guide", "Large CLOB text..."],
    )
    ```

    For advanced control, use `conn.create_lob(...)` and the returned `Lob` object.

    !!! tip
        Read LOB metadata and lifecycle details in [Examples](EXAMPLES.md#lob-handling).

??? "How do I use connection pooling?"
    pycubrid itself does not ship a built-in pool. Recommended options:

    1. SQLAlchemy engine pooling (`pool_size`, `max_overflow`, `pool_pre_ping`)
    2. A lightweight application-level queue-based pool for simple scripts

    ```python
    from sqlalchemy import create_engine

    engine = create_engine(
        "cubrid+pycubrid://dba@localhost:33000/testdb",
        pool_size=5,
        pool_pre_ping=True,
    )
    ```

??? "What character encodings are supported?"
    pycubrid supports standard CUBRID character types and Python `str` values.

    !!! note
        For multilingual text workloads (NCHAR/VARNCHAR), verify your database collation and server/client charset settings together.

??? "How do I handle errors properly?"
    Catch specific DB-API exceptions first (`IntegrityError`, `ProgrammingError`, `OperationalError`) and fall back to `Error`.

    ```python
    import pycubrid

    try:
        cur.execute("INSERT INTO users (email) VALUES (?)", ["duplicate@example.com"])
        conn.commit()
    except pycubrid.IntegrityError:
        conn.rollback()
        raise
    except pycubrid.Error:
        conn.rollback()
        raise
    ```

    See [Troubleshooting](TROUBLESHOOTING.md) for categorized error diagnosis.
