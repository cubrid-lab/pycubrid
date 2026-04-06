# pycubrid

Pure Python DB-API 2.0 driver for CUBRID database with no C extensions.

## Key features

- Full PEP 249 (DB-API 2.0) compliant connection and cursor interface
- Direct CUBRID CAS wire protocol implementation in pure Python
- Typed package support (`py.typed`) for modern IDEs and static analysis
- LOB support for CLOB and BLOB operations

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

- [Getting Started](CONNECTION.md)
- [User Guide](TYPES.md)
- [API Reference](API_REFERENCE.md)

## Project links

- [GitHub](https://github.com/cubrid-labs/pycubrid)
- [PyPI](https://pypi.org/project/pycubrid/)
- [Changelog](https://github.com/cubrid-labs/pycubrid/blob/main/CHANGELOG.md)
- [Contributing](https://github.com/cubrid-labs/pycubrid/blob/main/CONTRIBUTING.md)
