# pycubrid

**Reiner Python DB-API 2.0 Treiber für CUBRID**

[🇰🇷 한국어](README.ko.md) · [🇺🇸 English](../README.md) · [🇨🇳 中文](README.zh.md) · [🇮🇳 हिन्दी](README.hi.md) · [🇩🇪 Deutsch](README.de.md) · [🇷🇺 Русский](README.ru.md)

[![PyPI](https://img.shields.io/pypi/v/pycubrid.svg)](https://pypi.org/project/pycubrid/)
[![CI](https://github.com/cubrid-labs/pycubrid/actions/workflows/ci.yml/badge.svg)](https://github.com/cubrid-labs/pycubrid/actions/workflows/ci.yml)
[![Python 3.10+](https://img.shields.io/badge/python-3.10%2B-blue.svg)](https://www.python.org/downloads/)
[![Coverage 99%](https://img.shields.io/badge/coverage-99%25-brightgreen.svg)](DEVELOPMENT.md#code-coverage)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

---

## Warum pycubrid?

CUBRID ist eine leistungsstarke Open-Source-relationale Datenbank, die in
koreanischen öffentlichen Einrichtungen und Unternehmensanwendungen weit
verbreitet ist. Der bestehende C-Erweiterungstreiber (`CUBRIDdb`) hatte
Build-Abhängigkeiten und Plattformkompatibilitätsprobleme.

**pycubrid** löst diese Probleme:

- **Reine Python-Implementierung** — keine C-Build-Abhängigkeiten, Installation nur mit `pip install`
- **Vollständige PEP 249 (DB-API 2.0) Konformität** — Standard-Ausnahmehierarchie, Typobjekte, Cursor-Schnittstelle
- **471 Offline-Tests**, **über 99 % Codeabdeckung** — ohne Datenbank ausführbar
- **PEP 561 Typ-Paket** — Unterstützung für moderne IDEs und statische Analysewerkzeuge
- **Direkte Implementierung des CUBRID CAS-Protokolls** — keine zusätzliche Middleware erforderlich
- **LOB-Unterstützung (CLOB/BLOB)** — Verarbeitung großer Text- und Binärdaten

## Anforderungen

- Python 3.10+
- CUBRID-Datenbankserver 10.2+

## Installation

```bash
pip install pycubrid
```

## Schnellstart

### Grundlegende Verbindung

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

### Kontextmanager verwenden

```python
import pycubrid

with pycubrid.connect(host="localhost", port=33000, database="testdb", user="dba") as conn:
    with conn.cursor() as cur:
        cur.execute("CREATE TABLE IF NOT EXISTS cookbook_users (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(100))")
        cur.execute("INSERT INTO cookbook_users (name) VALUES (?)", ("Max",))
        conn.commit()

        cur.execute("SELECT * FROM cookbook_users")
        for row in cur:
            print(row)
```

### Parameterbindung

```python
# qmark-Stil (Fragezeichen)
cur.execute("SELECT * FROM users WHERE name = ? AND age > ?", ("Alice", 25))

# Batch-Einfügung mit executemany
data = [("Alice", 30), ("Bob", 25), ("Charlie", 35)]
cur.executemany("INSERT INTO users (name, age) VALUES (?, ?)", data)
conn.commit()
```

## PEP 249 Konformität

| Eigenschaft | Wert |
|---|---|
| `apilevel` | `"2.0"` |
| `threadsafety` | `1` (Verbindungen können nicht zwischen Threads geteilt werden) |
| `paramstyle` | `"qmark"` (Positionsparameter `?`) |

- Vollständige Standard-Ausnahmehierarchie: `Warning`, `Error`, `InterfaceError`, `DatabaseError`, `OperationalError`, `IntegrityError`, `InternalError`, `ProgrammingError`, `NotSupportedError`
- Standard-Typobjekte: `STRING`, `BINARY`, `NUMBER`, `DATETIME`, `ROWID`
- Standard-Konstruktoren: `Date()`, `Time()`, `Timestamp()`, `Binary()`, `DateFromTicks()`, `TimeFromTicks()`, `TimestampFromTicks()`

## Unterstützte CUBRID-Versionen

In CI verifiziert:

- 11.2
- 11.4

## SQLAlchemy-Integration

pycubrid funktioniert als Treiber für [sqlalchemy-cubrid](https://github.com/cubrid-labs/sqlalchemy-cubrid) — den SQLAlchemy 2.0 Dialekt für CUBRID:

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

Alle SQLAlchemy-Funktionen (ORM, Core, Alembic-Migrationen, Schema-Reflexion) funktionieren transparent mit dem pycubrid-Treiber.

## Dokumentation

| Leitfaden | Beschreibung |
|---|---|
| [Verbindung](CONNECTION.md) | Verbindungszeichenfolgen, URL-Format, Konfiguration, Verbindungspools |
| [Typzuordnung](TYPES.md) | Vollständige Typzuordnung, CUBRID-spezifische Typen, Sammlungstypen |
| [API-Referenz](API_REFERENCE.md) | Vollständige API-Dokumentation — Module, Klassen, Funktionen |
| [Protokoll](PROTOCOL.md) | CAS-Wire-Protokoll-Referenz |
| [Entwicklungsleitfaden](DEVELOPMENT.md) | Entwicklungsumgebung, Tests, Docker, Abdeckung, CI/CD |
| [Beispiele](EXAMPLES.md) | Praktische Anwendungsbeispiele und Code |

## Kompatibilität

| | Python 3.10 | Python 3.11 | Python 3.12 | Python 3.13 |
|---|:---:|:---:|:---:|:---:|
| **Offline-Tests** | ✅ | ✅ | ✅ | ✅ |
| **CUBRID 11.4** | ✅ | -- | ✅ | -- |
| **CUBRID 11.2** | ✅ | -- | ✅ | -- |

## Mitwirken

Beitragsrichtlinien finden Sie in [CONTRIBUTING.md](../CONTRIBUTING.md), die Entwicklungsumgebung in [docs/DEVELOPMENT.md](DEVELOPMENT.md).

## Sicherheit

Melden Sie Sicherheitslücken per E-Mail — Details finden Sie in [SECURITY.md](../SECURITY.md). Erstellen Sie keine öffentlichen Issues für Sicherheitsprobleme.

## Lizenz

MIT — siehe [LICENSE](../LICENSE).
