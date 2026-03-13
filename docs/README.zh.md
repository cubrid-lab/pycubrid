# pycubrid

**CUBRID 纯 Python DB-API 2.0 驱动**

[🇰🇷 한국어](README.ko.md) · [🇺🇸 English](../README.md) · [🇨🇳 中文](README.zh.md) · [🇮🇳 हिन्दी](README.hi.md) · [🇩🇪 Deutsch](README.de.md) · [🇷🇺 Русский](README.ru.md)

[![PyPI](https://img.shields.io/pypi/v/pycubrid.svg)](https://pypi.org/project/pycubrid/)
[![CI](https://github.com/cubrid-labs/pycubrid/actions/workflows/ci.yml/badge.svg)](https://github.com/cubrid-labs/pycubrid/actions/workflows/ci.yml)
[![Python 3.10+](https://img.shields.io/badge/python-3.10%2B-blue.svg)](https://www.python.org/downloads/)
[![Coverage 99%](https://img.shields.io/badge/coverage-99%25-brightgreen.svg)](DEVELOPMENT.md#code-coverage)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

---

## 为什么选择 pycubrid？

CUBRID 是一款高性能开源关系型数据库，在韩国公共部门和企业环境中广泛使用。
现有的 C 扩展驱动（`CUBRIDdb`）存在构建依赖和平台兼容性问题。

**pycubrid** 解决了这些问题：

- **纯 Python 实现** — 无需 C 编译，仅需 `pip install` 即可安装
- **完全兼容 PEP 249（DB-API 2.0）** — 标准异常体系、类型对象、游标接口
- **471 个离线测试**，**99% 以上代码覆盖率** — 无需数据库即可运行
- **PEP 561 类型包** — 支持现代 IDE 和静态分析工具
- **直接实现 CUBRID CAS 协议** — 无需额外中间件
- **LOB（CLOB/BLOB）支持** — 处理大文本和二进制数据

## 环境要求

- Python 3.10+
- CUBRID 数据库服务器 10.2+

## 安装

```bash
pip install pycubrid
```

## 快速入门

### 基本连接

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
cur.execute("SELECT 1 + 1")
print(cur.fetchone())  # (2,)

cur.close()
conn.close()
```

### 使用上下文管理器

```python
import pycubrid

with pycubrid.connect(host="localhost", port=33000, database="testdb", user="dba") as conn:
    with conn.cursor() as cur:
        cur.execute("CREATE TABLE IF NOT EXISTS cookbook_users (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(100))")
        cur.execute("INSERT INTO cookbook_users (name) VALUES (?)", ("张三",))
        conn.commit()

        cur.execute("SELECT * FROM cookbook_users")
        for row in cur:
            print(row)
```

### 参数绑定

```python
# qmark 风格（问号占位符）
cur.execute("SELECT * FROM users WHERE name = ? AND age > ?", ("Alice", 25))

# 使用 executemany 批量插入
data = [("Alice", 30), ("Bob", 25), ("Charlie", 35)]
cur.executemany("INSERT INTO users (name, age) VALUES (?, ?)", data)
conn.commit()
```

## PEP 249 兼容性

| 属性 | 值 |
|---|---|
| `apilevel` | `"2.0"` |
| `threadsafety` | `1`（连接不可跨线程共享） |
| `paramstyle` | `"qmark"`（位置参数 `?`） |

- 完整标准异常体系：`Warning`、`Error`、`InterfaceError`、`DatabaseError`、`OperationalError`、`IntegrityError`、`InternalError`、`ProgrammingError`、`NotSupportedError`
- 标准类型对象：`STRING`、`BINARY`、`NUMBER`、`DATETIME`、`ROWID`
- 标准构造函数：`Date()`、`Time()`、`Timestamp()`、`Binary()`、`DateFromTicks()`、`TimeFromTicks()`、`TimestampFromTicks()`

## 支持的 CUBRID 版本

CI 中已验证：

- 11.2
- 11.4

## SQLAlchemy 集成

pycubrid 可作为 [sqlalchemy-cubrid](https://github.com/cubrid-labs/sqlalchemy-cubrid)（CUBRID 的 SQLAlchemy 2.0 方言）的驱动使用：

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

所有 SQLAlchemy 功能（ORM、Core、Alembic 迁移、模式反射）均可通过 pycubrid 驱动透明使用。

## 文档

| 指南 | 说明 |
|---|---|
| [连接](CONNECTION.md) | 连接字符串、URL 格式、配置选项、连接池 |
| [类型映射](TYPES.md) | 完整类型映射、CUBRID 特有类型、集合类型 |
| [API 参考](API_REFERENCE.md) | 完整 API 文档 — 模块、类、函数 |
| [协议](PROTOCOL.md) | CAS 线路协议参考 |
| [开发指南](DEVELOPMENT.md) | 开发环境设置、测试、Docker、覆盖率、CI/CD |
| [示例](EXAMPLES.md) | 实用使用示例和代码 |

## 兼容性

| | Python 3.10 | Python 3.11 | Python 3.12 | Python 3.13 |
|---|:---:|:---:|:---:|:---:|
| **离线测试** | ✅ | ✅ | ✅ | ✅ |
| **CUBRID 11.4** | ✅ | -- | ✅ | -- |
| **CUBRID 11.2** | ✅ | -- | ✅ | -- |

## 贡献

贡献指南请参阅 [CONTRIBUTING.md](../CONTRIBUTING.md)，开发环境设置请参阅 [docs/DEVELOPMENT.md](DEVELOPMENT.md)。

## 安全

请通过电子邮件报告安全漏洞 — 详情请参阅 [SECURITY.md](../SECURITY.md)。请勿就安全问题提交公开 Issue。

## 许可证

MIT — 参见 [LICENSE](../LICENSE)。
