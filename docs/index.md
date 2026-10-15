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
[Your First CUBRID Connection](quickstart.md) walks you from preparing a
database (your own, or a disposable local one in Docker) to a first query and
a first saved row.

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

- [Your First CUBRID Connection](quickstart.md)
- [Connection Guide](CONNECTION.md)
- [Type Mapping](TYPES.md)
- [Parameter Binding Contract](PARAMETER_BINDING.md)
- [API Reference](API_REFERENCE.md)

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
