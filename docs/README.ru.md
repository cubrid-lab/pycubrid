# pycubrid

**Чистый Python DB-API 2.0 драйвер для CUBRID**

[🇰🇷 한국어](README.ko.md) · [🇺🇸 English](../README.md) · [🇨🇳 中文](README.zh.md) · [🇮🇳 हिन्दी](README.hi.md) · [🇩🇪 Deutsch](README.de.md) · [🇷🇺 Русский](README.ru.md)

[![PyPI](https://img.shields.io/pypi/v/pycubrid.svg)](https://pypi.org/project/pycubrid/)
[![CI](https://github.com/cubrid-labs/pycubrid/actions/workflows/ci.yml/badge.svg)](https://github.com/cubrid-labs/pycubrid/actions/workflows/ci.yml)
[![Python 3.10+](https://img.shields.io/badge/python-3.10%2B-blue.svg)](https://www.python.org/downloads/)
[![Coverage 99%](https://img.shields.io/badge/coverage-99%25-brightgreen.svg)](DEVELOPMENT.md#code-coverage)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

---

## Почему pycubrid?

CUBRID — это высокопроизводительная реляционная база данных с открытым исходным
кодом, широко используемая в государственном и корпоративном секторе Кореи.
Существующий драйвер на основе C-расширения (`CUBRIDdb`) имел проблемы с
зависимостями сборки и совместимостью платформ.

**pycubrid** решает эти проблемы:

- **Чистая реализация на Python** — без зависимостей от C-сборки, установка через `pip install`
- **Полное соответствие PEP 249 (DB-API 2.0)** — стандартная иерархия исключений, объекты типов, интерфейс курсора
- **471 оффлайн-тест**, **покрытие кода более 99 %** — запуск без базы данных
- **Типизированный пакет PEP 561** — поддержка современных IDE и средств статического анализа
- **Прямая реализация протокола CUBRID CAS** — без дополнительного промежуточного ПО
- **Поддержка LOB (CLOB/BLOB)** — обработка больших текстовых и бинарных данных

## Требования

- Python 3.10+
- Сервер базы данных CUBRID 10.2+

## Установка

```bash
pip install pycubrid
```

## Быстрый старт

### Базовое подключение

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

### Использование контекстного менеджера

```python
import pycubrid

with pycubrid.connect(host="localhost", port=33000, database="testdb", user="dba") as conn:
    with conn.cursor() as cur:
        cur.execute("CREATE TABLE IF NOT EXISTS cookbook_users (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(100))")
        cur.execute("INSERT INTO cookbook_users (name) VALUES (?)", ("Иван",))
        conn.commit()

        cur.execute("SELECT * FROM cookbook_users")
        for row in cur:
            print(row)
```

### Привязка параметров

```python
# Стиль qmark (знак вопроса)
cur.execute("SELECT * FROM users WHERE name = ? AND age > ?", ("Alice", 25))

# Пакетная вставка с executemany
data = [("Alice", 30), ("Bob", 25), ("Charlie", 35)]
cur.executemany("INSERT INTO users (name, age) VALUES (?, ?)", data)
conn.commit()
```

## Соответствие PEP 249

| Атрибут | Значение |
|---|---|
| `apilevel` | `"2.0"` |
| `threadsafety` | `1` (соединения не могут разделяться между потоками) |
| `paramstyle` | `"qmark"` (позиционные параметры `?`) |

- Полная стандартная иерархия исключений: `Warning`, `Error`, `InterfaceError`, `DatabaseError`, `OperationalError`, `IntegrityError`, `InternalError`, `ProgrammingError`, `NotSupportedError`
- Стандартные объекты типов: `STRING`, `BINARY`, `NUMBER`, `DATETIME`, `ROWID`
- Стандартные конструкторы: `Date()`, `Time()`, `Timestamp()`, `Binary()`, `DateFromTicks()`, `TimeFromTicks()`, `TimestampFromTicks()`

## Поддерживаемые версии CUBRID

Проверены в CI:

- 11.2
- 11.4

## Интеграция с SQLAlchemy

pycubrid работает как драйвер для [sqlalchemy-cubrid](https://github.com/cubrid-labs/sqlalchemy-cubrid) — диалекта SQLAlchemy 2.0 для CUBRID:

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

Все функции SQLAlchemy (ORM, Core, миграции Alembic, рефлексия схемы) прозрачно работают с драйвером pycubrid.

## Документация

| Руководство | Описание |
|---|---|
| [Подключение](CONNECTION.md) | Строки подключения, формат URL, конфигурация, пул соединений |
| [Сопоставление типов](TYPES.md) | Полное сопоставление типов, специфичные для CUBRID типы, коллекции |
| [Справочник API](API_REFERENCE.md) | Полная документация API — модули, классы, функции |
| [Протокол](PROTOCOL.md) | Справочник по проводному протоколу CAS |
| [Руководство разработчика](DEVELOPMENT.md) | Среда разработки, тестирование, Docker, покрытие, CI/CD |
| [Примеры](EXAMPLES.md) | Практические примеры использования и код |

## Совместимость

| | Python 3.10 | Python 3.11 | Python 3.12 | Python 3.13 |
|---|:---:|:---:|:---:|:---:|
| **Оффлайн-тесты** | ✅ | ✅ | ✅ | ✅ |
| **CUBRID 11.4** | ✅ | -- | ✅ | -- |
| **CUBRID 11.2** | ✅ | -- | ✅ | -- |

## Участие в проекте

Руководство по участию — [CONTRIBUTING.md](../CONTRIBUTING.md), настройка среды разработки — [docs/DEVELOPMENT.md](DEVELOPMENT.md).

## Безопасность

Сообщайте об уязвимостях по электронной почте — подробности в [SECURITY.md](../SECURITY.md). Не создавайте публичные Issue для вопросов безопасности.

## Лицензия

MIT — см. [LICENSE](../LICENSE).
