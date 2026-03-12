# pycubrid

**Pure Python DB-API 2.0 driver for CUBRID**

[![PyPI](https://img.shields.io/pypi/v/pycubrid.svg)](https://pypi.org/project/pycubrid/)
[![CI](https://github.com/sqlalchemy-cubrid/pycubrid/actions/workflows/ci.yml/badge.svg)](https://github.com/sqlalchemy-cubrid/pycubrid/actions/workflows/ci.yml)
[![Python 3.10+](https://img.shields.io/badge/python-3.10%2B-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

---

## Overview

`pycubrid` is a pure Python database driver for CUBRID that implements Python DB-API 2.0
(PEP 249). It is designed for portability and easy installation with no native C extension build
step.

## Features

- Pure Python implementation (no C build dependencies)
- DB-API 2.0 compatible exception hierarchy and type objects
- CUBRID CAS protocol constants and wire-level primitive definitions
- Typed package (`py.typed`) for modern IDE and static analysis support
- Offline test suite for fast verification and high coverage

## Installation

```bash
pip install pycubrid
```

## Quick Start

```python
import pycubrid

conn = pycubrid.connect(
    host="localhost",
    port=33000,
    database="testdb",
    user="dba",
    password="",
)

cur = conn.cursor()
cur.execute("SELECT 1")
row = cur.fetchone()
print(row)

cur.close()
conn.close()
```

## PEP 249 Compliance

`pycubrid` follows Python DB-API 2.0 module contract:

- `apilevel = "2.0"`
- `threadsafety = 1`
- `paramstyle = "qmark"`
- Full standard exception hierarchy
- Standard type objects and constructors

## Supported CUBRID Versions

The project targets CUBRID 11.x series and is validated in CI against:

- 11.2
- 11.4

## SQLAlchemy Integration

`pycubrid` works as a driver for [sqlalchemy-cubrid](https://github.com/sqlalchemy-cubrid/sqlalchemy-cubrid) — the SQLAlchemy 2.0 dialect for CUBRID:

```bash
pip install "sqlalchemy-cubrid[pycubrid]"
```

```python
from sqlalchemy import create_engine, text

engine = create_engine("cubrid+pycubrid://dba@localhost:33000/testdb")

with engine.connect() as conn:
    result = conn.execute(text("SELECT 1"))
    print(result.scalar())
```

All SQLAlchemy features (ORM, Core, Alembic migrations, schema reflection) work transparently with the pycubrid driver.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for development setup, test commands, and pull request
guidelines.

## License

MIT - see [LICENSE](LICENSE).
