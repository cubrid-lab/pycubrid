# pycubrid

Pure Python DB-API 2.0 driver for CUBRID database with no C extensions.

> 한국어: [문서 홈](ko/index.md)

## Key features

- Full PEP 249 (DB-API 2.0) compliant connection and cursor interface
- Direct CUBRID CAS wire protocol implementation in pure Python
- Typed package support (`py.typed`) for modern IDEs and static analysis
- LOB support for CLOB and BLOB operations

## Start here

pycubrid lets a Python program talk to a CUBRID database, a separate server
that must already be running; installing pycubrid does not install CUBRID.
[Your First CUBRID Connection](quickstart.md) is the first of five short
lessons: it prepares a database (your own, or a disposable local one in Docker)
and Python, and the next lessons run a first query, save a row, commit and roll
back, and finish safely.

## Quick install

```bash
pip install pycubrid
```

## Minimal example

```python
import pycubrid

conn = pycubrid.connect(host="localhost", port=33000, database="testdb", user="dba", password="")
cur = conn.cursor()
cur.execute("SELECT 1")
print(cur.fetchone())
conn.close()
```

## Documentation

- Learn: [Your First CUBRID Connection](quickstart.md), then the lessons it links.
- Look up: [API Reference](API_REFERENCE.md),
  [Connection Options](CONNECTION.md),
  [Passing Values Safely (Parameter Binding Contract)](PARAMETER_BINDING.md),
  [Type Mapping](TYPES.md) and [Support Matrix](SUPPORT_MATRIX.md).
- Get help: [Troubleshooting](TROUBLESHOOTING.md), [FAQ](faq.md), and for a
  security problem the private reporting route in
  [SECURITY.md](https://github.com/cubrid-lab/pycubrid/blob/main/SECURITY.md).
- Contribute: [Development](DEVELOPMENT.md), [Architecture](ARCHITECTURE.md) and
  [Protocol](PROTOCOL.md).

## Project links

- [GitHub](https://github.com/cubrid-lab/pycubrid)
- [PyPI](https://pypi.org/project/pycubrid/)
- [Changelog](https://github.com/cubrid-lab/pycubrid/blob/main/CHANGELOG.md)
- [Contributing](https://github.com/cubrid-lab/pycubrid/blob/main/CONTRIBUTING.md)

## Ecosystem

Part of the cubrid-lab Python ecosystem:

- **pycubrid** — Pure-Python DB-API 2.0 driver for CUBRID (sync + native asyncio)
- sqlalchemy-cubrid — SQLAlchemy 2.0–2.1 dialect + Alembic
- cubrid-cookbook-python — 68 runnable examples and application templates
- cubrid-mcp-server — MCP server — natural-language access for LLM clients
