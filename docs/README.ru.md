# pycubrid

**Чистый Python-драйвер DB-API 2.0 для базы данных CUBRID** — без C-расширений, без компиляции, реализует интерфейс PEP 249 (DB-API 2.0).

[🇰🇷 한국어](README.ko.md) · [🇺🇸 English](../README.md) · [🇨🇳 中文](README.zh.md) · [🇮🇳 हिन्दी](README.hi.md) · [🇩🇪 Deutsch](README.de.md) · [🇷🇺 Русский](README.ru.md)

<!-- BADGES:START -->
[![PyPI version](https://img.shields.io/pypi/v/pycubrid)](https://pypi.org/project/pycubrid)
[![python version](https://img.shields.io/pypi/pyversions/pycubrid)](https://www.python.org)
[![ci workflow](https://github.com/cubrid-labs/pycubrid/actions/workflows/ci.yml/badge.svg)](https://github.com/cubrid-labs/pycubrid/actions/workflows/ci.yml)
[![integration-full workflow](https://github.com/cubrid-labs/pycubrid/actions/workflows/integration-full.yml/badge.svg)](https://github.com/cubrid-labs/pycubrid/actions/workflows/integration-full.yml)
[![coverage](https://codecov.io/gh/cubrid-labs/pycubrid/branch/main/graph/badge.svg)](https://codecov.io/gh/cubrid-labs/pycubrid)
[![license](https://img.shields.io/github/license/cubrid-labs/pycubrid)](https://github.com/cubrid-labs/pycubrid/blob/main/LICENSE)
[![GitHub stars](https://img.shields.io/github/stars/cubrid-labs/pycubrid)](https://github.com/cubrid-labs/pycubrid)
[![docs](https://img.shields.io/badge/docs-GitHub%20Pages-blue)](https://cubrid-labs.github.io/pycubrid/)
<!-- BADGES:END -->

---

> **Статус: Beta.** Основной публичный API следует semantic versioning; в минорных релизах могут появляться новые возможности и исправления ошибок, пока проект находится в активной разработке.

## Почему pycubrid?

CUBRID — это высокопроизводительная реляционная база данных с открытым исходным
кодом, широко используемая в корейском государственном секторе и корпоративных
приложениях. Существующий драйвер на основе C-расширения (`CUBRIDdb`) имел
зависимости сборки и проблемы совместимости с платформами.

**pycubrid** решает эти проблемы:

- **Чистая реализация на Python** — без зависимостей от C-сборки, установка только через `pip install`
- **Реализует PEP 249 (DB-API 2.0)** — стандартная иерархия исключений, объекты типов и интерфейс курсора
- **770 офлайн-тестов / 811 всего** при **97,29 % покрытия кода** — большинство тестов запускаются без базы данных
- **TLS/SSL для синхронных подключений** — опционально `ssl=True` (проверенный контекст) или пользовательский `ssl.SSLContext` в `connect()`
- **Нативная поддержка asyncio** — async/await API через `pycubrid.aio` для приложений с высокой конкурентностью
- **Типизированный пакет PEP 561** — маркер `py.typed` для современных IDE и инструментов статического анализа
- **Прямая реализация протокола CUBRID CAS** — без дополнительного промежуточного ПО
- **Поддержка LOB (CLOB/BLOB)** — работа с большими текстовыми и бинарными данными

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

### Контекстный менеджер

```python
import pycubrid

with pycubrid.connect(host="localhost", port=33000, database="testdb", user="dba") as conn:
    with conn.cursor() as cur:
        cur.execute("CREATE TABLE IF NOT EXISTS cookbook_users (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(100))")
        cur.execute("INSERT INTO cookbook_users (name) VALUES (?)", ("Alice",))
        conn.commit()

        cur.execute("SELECT * FROM cookbook_users")
        for row in cur:
            print(row)
```

### Async

```python
import asyncio
import pycubrid.aio

async def main():
    conn = await pycubrid.aio.connect(
        host="localhost", port=33000, database="testdb", user="dba"
    )
    cur = conn.cursor()
    await cur.execute("SELECT 1 + 1")
    print(await cur.fetchone())  # (2,)
    await cur.close()
    await conn.close()

asyncio.run(main())
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

### Параметризованные запросы

```python
sql = "SELECT * FROM users WHERE department = ?"

cur.execute(sql, ("Engineering",))
engineers = cur.fetchall()

cur.execute(sql, ("Marketing",))
marketers = cur.fetchall()
```

## Соответствие PEP 249

| Атрибут | Значение |
|---|---|
| `apilevel` | `"2.0"` |
| `threadsafety` | `1` (соединения нельзя разделять между потоками) |
| `paramstyle` | `"qmark"` (позиционные параметры `?`) |

- Полная стандартная иерархия исключений: `Warning`, `Error`, `InterfaceError`, `DatabaseError`, `OperationalError`, `IntegrityError`, `InternalError`, `ProgrammingError`, `NotSupportedError`
- Стандартные объекты типов: `STRING`, `BINARY`, `NUMBER`, `DATETIME`, `ROWID`
- Стандартные конструкторы: `Date()`, `Time()`, `Timestamp()`, `Binary()`, `DateFromTicks()`, `TimeFromTicks()`, `TimestampFromTicks()`

## Возможности

- **Чистый Python** — без C-расширений, без компиляции, работает везде, где запускается Python
- **Полная DB-API 2.0** — `connect()`, `Cursor`, `fetchone/many/all`, `executemany`, `callproc`
- **Параметризованные запросы** — `cursor.execute(sql, params)` с серверным `PREPARE_AND_EXECUTE`
- **Пакетные операции** — `executemany()` и `executemany_batch()` для массовых вставок
- **Поддержка LOB** — `create_lob()`, чтение и запись столбцов CLOB и BLOB
- **Интроспекция схемы** — `get_schema_info()` для таблиц, столбцов, индексов и ограничений
- **Управление автокоммитом** — свойство `connection.autocommit` для управления транзакциями
- **Определение версии сервера** — `connection.get_server_version()` возвращает строку версии (например, `"11.2.0.0378"`)
- **Итератор курсора** — можно проходить результаты `for row in cursor`
- **Контекстные менеджеры** — конструкции `with` для соединений и курсоров
- **Async-поддержка** — `pycubrid.aio.connect()` с `AsyncConnection` и `AsyncCursor` для циклов событий asyncio

## Поддерживаемые версии CUBRID

Проект ориентирован на CUBRID 10.x и 11.x и валидируется в CI для следующих версий:

- 10.2
- 11.0
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

Возможности SQLAlchemy (ORM, Core, миграции Alembic, рефлексия схемы) доступны через драйвер pycubrid при использовании с sqlalchemy-cubrid.

## Документация

| Руководство | Описание |
|---|---|
| [Подключение](CONNECTION.md) | Строки подключения, формат URL, конфигурация |
| [Сопоставление типов](TYPES.md) | Полное сопоставление типов, специфичные для CUBRID типы, типы коллекций |
| [Справочник API](API_REFERENCE.md) | Полная документация API — модули, классы, функции |
| [Протокол](PROTOCOL.md) | Справочник по CAS wire protocol |
| [Разработка](DEVELOPMENT.md) | Среда разработки, тестирование, Docker, покрытие, CI/CD |
| [Примеры](EXAMPLES.md) | Практические примеры использования с кодом |
| [Устранение неполадок](TROUBLESHOOTING.md) | Ошибки подключения, проблемы запросов, работа с LOB, отладка |

## Совместимость

| | Python 3.10 | Python 3.11 | Python 3.12 | Python 3.13 | Python 3.14 |
|---|:---:|:---:|:---:|:---:|:---:|
| **Оффлайн-тесты** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **CUBRID 11.4** | ✅ | -- | -- | -- | ✅ |
| **CUBRID 11.2** | ✅ | -- | -- | -- | ✅ |
| **CUBRID 11.0** | ✅ | -- | -- | -- | ✅ |
| **CUBRID 10.2** | ✅ | -- | -- | -- | ✅ |

CI запускает указанную выше матрицу при каждом PR/push (Python 3.10 + 3.14 как опорные версии × все версии CUBRID).
Полная матрица **5 × 4** Python × CUBRID выполняется каждую ночь, при релизах с тегами и вручную через `workflow_dispatch`.

## Архитектура

```mermaid
graph TD
    app[Application]
    pycubrid[pycubrid Connection/Cursor]
    cas[CAS Protocol]
    server[CUBRID Server]

    app --> pycubrid
    pycubrid --> cas
    cas --> server
```

```mermaid
graph TD
    root[pycubrid/]
    init[__init__.py - Public API connect(), types, exceptions, __version__]
    connection[connection.py - Connection class connect/commit/rollback/cursor/LOB]
    cursor[cursor.py - Cursor class execute/fetch/executemany/callproc/iterator]
    types[types.py - DB-API 2.0 type objects and constructors]
    exceptions[exceptions.py - PEP 249 exception hierarchy]
    constants[constants.py - CAS function codes, data types, protocol constants]
    protocol[protocol.py - CAS wire protocol packet classes (18 packet types)]
    packet[packet.py - Low-level packet reader/writer]
    lob[lob.py - LOB support]
    typed[py.typed - PEP 561 marker]

    root --> init
    root --> connection
    root --> cursor
    root --> types
    root --> exceptions
    root --> constants
    root --> protocol
    root --> packet
    root --> lob
    root --> typed
    root --> aio
    aio[aio/ - AsyncConnection, AsyncCursor, async connect()]
```

## FAQ

### Как подключиться к CUBRID из Python?

```python
import pycubrid
conn = pycubrid.connect(host="localhost", port=33000, database="testdb", user="dba")
```

### Как установить pycubrid?

`pip install pycubrid` — без C-расширений и инструментов сборки.

### Какой стиль параметров использует pycubrid?

Стиль вопросительного знака (`qmark`): `cursor.execute("SELECT * FROM users WHERE id = ?", (1,))`

### Работает ли pycubrid с SQLAlchemy?

Да. Установите `pip install "sqlalchemy-cubrid[pycubrid]"` и используйте URL подключения `cubrid+pycubrid://dba@localhost:33000/testdb`.

### Какие версии Python поддерживаются?

Python 3.10, 3.11, 3.12, 3.13 и 3.14.

### Поддерживает ли pycubrid LOB (CLOB/BLOB)?

Да. Можно вставлять строки/байты напрямую в столбцы CLOB/BLOB. При чтении столбцы LOB возвращают данные, доступные через курсор.

### Является ли pycubrid потокобезопасным?

У pycubrid `threadsafety = 1`, то есть соединения нельзя разделять между потоками. Создавайте отдельное соединение для каждого потока.

### Какие версии CUBRID поддерживаются?

Версии CUBRID 10.2, 11.0, 11.2 и 11.4 тестируются в CI.

### Поддерживает ли pycubrid async/await?

Да. Используйте `pycubrid.aio.connect()` для нативной поддержки asyncio. Поверхность async API похожа на sync API: `await conn.ping(reconnect=...)` выполняет тот же нативный health-check `CHECK_CAS`, что и sync `Connection.ping()`, `create_lob()` по-прежнему остаётся только sync-методом, а изменение автокоммита выполняется через `await conn.set_autocommit(...)`, а не через setter свойства.


## Связанные проекты

- [sqlalchemy-cubrid](https://github.com/cubrid-labs/sqlalchemy-cubrid) — диалект SQLAlchemy 2.0 для CUBRID
- [cubrid-python-cookbook](https://github.com/cubrid-labs/cubrid-python-cookbook) — готовые к продакшену примеры Python для CUBRID


## Дорожная карта

См. [`ROADMAP.md`](../ROADMAP.md), чтобы узнать о направлении проекта и следующих этапах.

Для обзора по всей экосистеме см. [CUBRID Labs Ecosystem Roadmap](https://github.com/cubrid-labs/.github/blob/main/ROADMAP.md) и [Project Board](https://github.com/orgs/cubrid-labs/projects/2).

## Участие в проекте

См. [CONTRIBUTING.md](../CONTRIBUTING.md) с рекомендациями и [docs/DEVELOPMENT.md](DEVELOPMENT.md) с настройкой среды разработки.

## Безопасность

Сообщайте об уязвимостях по электронной почте — см. [SECURITY.md](../SECURITY.md). Не создавайте публичные issues по вопросам безопасности.

## Лицензия

MIT — см. [LICENSE](../LICENSE).
