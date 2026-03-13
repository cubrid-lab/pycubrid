# pycubrid

**CUBRID के लिए शुद्ध Python DB-API 2.0 ड्राइवर**

[🇰🇷 한국어](README.ko.md) · [🇺🇸 English](../README.md) · [🇨🇳 中文](README.zh.md) · [🇮🇳 हिन्दी](README.hi.md) · [🇩🇪 Deutsch](README.de.md) · [🇷🇺 Русский](README.ru.md)

[![PyPI](https://img.shields.io/pypi/v/pycubrid.svg)](https://pypi.org/project/pycubrid/)
[![CI](https://github.com/cubrid-labs/pycubrid/actions/workflows/ci.yml/badge.svg)](https://github.com/cubrid-labs/pycubrid/actions/workflows/ci.yml)
[![Python 3.10+](https://img.shields.io/badge/python-3.10%2B-blue.svg)](https://www.python.org/downloads/)
[![Coverage 99%](https://img.shields.io/badge/coverage-99%25-brightgreen.svg)](DEVELOPMENT.md#code-coverage)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

---

## pycubrid क्यों?

CUBRID एक उच्च-प्रदर्शन ओपन-सोर्स रिलेशनल डेटाबेस है, जो कोरियाई सार्वजनिक
क्षेत्र और एंटरप्राइज़ अनुप्रयोगों में व्यापक रूप से उपयोग किया जाता है।
मौजूदा C एक्सटेंशन ड्राइवर (`CUBRIDdb`) में बिल्ड निर्भरता और प्लेटफ़ॉर्म
संगतता की समस्याएं थीं।

**pycubrid** इन समस्याओं को हल करता है:

- **शुद्ध Python कार्यान्वयन** — कोई C बिल्ड निर्भरता नहीं, केवल `pip install` से इंस्टॉल
- **PEP 249 (DB-API 2.0) पूर्ण अनुपालन** — मानक अपवाद पदानुक्रम, प्रकार ऑब्जेक्ट, कर्सर इंटरफ़ेस
- **471 ऑफ़लाइन परीक्षण**, **99% से अधिक कोड कवरेज** — डेटाबेस के बिना चलाएं
- **PEP 561 टाइप पैकेज** — आधुनिक IDE और स्थैतिक विश्लेषण उपकरण समर्थन
- **CUBRID CAS प्रोटोकॉल** का सीधा कार्यान्वयन — कोई अतिरिक्त मिडलवेयर नहीं
- **LOB (CLOB/BLOB) समर्थन** — बड़े टेक्स्ट और बाइनरी डेटा को संभालें

## आवश्यकताएं

- Python 3.10+
- CUBRID डेटाबेस सर्वर 10.2+

## इंस्टॉलेशन

```bash
pip install pycubrid
```

## त्वरित शुरुआत

### बुनियादी कनेक्शन

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

### कॉन्टेक्स्ट मैनेजर का उपयोग

```python
import pycubrid

with pycubrid.connect(host="localhost", port=33000, database="testdb", user="dba") as conn:
    with conn.cursor() as cur:
        cur.execute("CREATE TABLE IF NOT EXISTS cookbook_users (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(100))")
        cur.execute("INSERT INTO cookbook_users (name) VALUES (?)", ("राम",))
        conn.commit()

        cur.execute("SELECT * FROM cookbook_users")
        for row in cur:
            print(row)
```

### पैरामीटर बाइंडिंग

```python
# qmark शैली (प्रश्न चिह्न)
cur.execute("SELECT * FROM users WHERE name = ? AND age > ?", ("Alice", 25))

# executemany के साथ बैच इंसर्ट
data = [("Alice", 30), ("Bob", 25), ("Charlie", 35)]
cur.executemany("INSERT INTO users (name, age) VALUES (?, ?)", data)
conn.commit()
```

## PEP 249 अनुपालन

| विशेषता | मान |
|---|---|
| `apilevel` | `"2.0"` |
| `threadsafety` | `1` (कनेक्शन थ्रेड्स में साझा नहीं किए जा सकते) |
| `paramstyle` | `"qmark"` (स्थितीय पैरामीटर `?`) |

- पूर्ण मानक अपवाद पदानुक्रम: `Warning`, `Error`, `InterfaceError`, `DatabaseError`, `OperationalError`, `IntegrityError`, `InternalError`, `ProgrammingError`, `NotSupportedError`
- मानक प्रकार ऑब्जेक्ट: `STRING`, `BINARY`, `NUMBER`, `DATETIME`, `ROWID`
- मानक कंस्ट्रक्टर: `Date()`, `Time()`, `Timestamp()`, `Binary()`, `DateFromTicks()`, `TimeFromTicks()`, `TimestampFromTicks()`

## समर्थित CUBRID संस्करण

CI में सत्यापित:

- 11.2
- 11.4

## SQLAlchemy एकीकरण

pycubrid [sqlalchemy-cubrid](https://github.com/cubrid-labs/sqlalchemy-cubrid) — CUBRID के लिए SQLAlchemy 2.0 डायलेक्ट — के ड्राइवर के रूप में काम करता है:

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

सभी SQLAlchemy सुविधाएं (ORM, Core, Alembic माइग्रेशन, स्कीमा रिफ्लेक्शन) pycubrid ड्राइवर के साथ पारदर्शी रूप से काम करती हैं।

## प्रलेखन

| मार्गदर्शिका | विवरण |
|---|---|
| [कनेक्शन](CONNECTION.md) | कनेक्शन स्ट्रिंग, URL प्रारूप, कॉन्फ़िगरेशन, कनेक्शन पूल |
| [प्रकार मैपिंग](TYPES.md) | पूर्ण प्रकार मैपिंग, CUBRID-विशिष्ट प्रकार, संग्रह प्रकार |
| [API संदर्भ](API_REFERENCE.md) | पूर्ण API प्रलेखन — मॉड्यूल, क्लास, फ़ंक्शन |
| [प्रोटोकॉल](PROTOCOL.md) | CAS वायर प्रोटोकॉल संदर्भ |
| [विकास मार्गदर्शिका](DEVELOPMENT.md) | विकास वातावरण, परीक्षण, Docker, कवरेज, CI/CD |
| [उदाहरण](EXAMPLES.md) | व्यावहारिक उपयोग उदाहरण और कोड |

## संगतता

| | Python 3.10 | Python 3.11 | Python 3.12 | Python 3.13 |
|---|:---:|:---:|:---:|:---:|
| **ऑफ़लाइन परीक्षण** | ✅ | ✅ | ✅ | ✅ |
| **CUBRID 11.4** | ✅ | -- | ✅ | -- |
| **CUBRID 11.2** | ✅ | -- | ✅ | -- |

## योगदान

योगदान दिशानिर्देशों के लिए [CONTRIBUTING.md](../CONTRIBUTING.md) और विकास वातावरण सेटअप के लिए [docs/DEVELOPMENT.md](DEVELOPMENT.md) देखें।

## सुरक्षा

सुरक्षा कमजोरियों की रिपोर्ट ईमेल के माध्यम से करें — विवरण के लिए [SECURITY.md](../SECURITY.md) देखें। सुरक्षा मुद्दों के लिए सार्वजनिक Issue न बनाएं।

## लाइसेंस

MIT — [LICENSE](../LICENSE) देखें।
