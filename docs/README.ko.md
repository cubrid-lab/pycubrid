# pycubrid

**CUBRID를 위한 순수 Python DB-API 2.0 드라이버**

[🇰🇷 한국어](README.ko.md) · [🇺🇸 English](../README.md) · [🇨🇳 中文](README.zh.md) · [🇮🇳 हिन्दी](README.hi.md) · [🇩🇪 Deutsch](README.de.md) · [🇷🇺 Русский](README.ru.md)

[![PyPI](https://img.shields.io/pypi/v/pycubrid.svg)](https://pypi.org/project/pycubrid/)
[![CI](https://github.com/cubrid-labs/pycubrid/actions/workflows/ci.yml/badge.svg)](https://github.com/cubrid-labs/pycubrid/actions/workflows/ci.yml)
[![Python 3.10+](https://img.shields.io/badge/python-3.10%2B-blue.svg)](https://www.python.org/downloads/)
[![Coverage 99%](https://img.shields.io/badge/coverage-99%25-brightgreen.svg)](DEVELOPMENT.md#code-coverage)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

---

## 왜 pycubrid인가?

CUBRID는 고성능 오픈소스 관계형 데이터베이스로, 한국 공공기관 및 기업 환경에서
널리 사용되고 있습니다. 기존 C 확장 드라이버(`CUBRIDdb`)는 빌드 의존성과
플랫폼 호환성 문제가 있었습니다.

**pycubrid**는 이러한 문제를 해결합니다:

- **순수 Python 구현** — C 빌드 의존성 없이 `pip install`만으로 설치
- **PEP 249(DB-API 2.0) 완전 준수** — 표준 예외 체계, 타입 객체, 커서 인터페이스
- **471개 오프라인 테스트**, **99% 이상 코드 커버리지** — 데이터베이스 없이도 실행 가능
- **PEP 561 타입 패키지** — `py.typed` 마커로 최신 IDE 및 정적 분석 도구 지원
- **CUBRID CAS 프로토콜** 직접 구현 — 별도의 미들웨어 불필요
- **LOB(CLOB/BLOB) 지원** — 대용량 텍스트 및 바이너리 데이터 처리

## 요구 사항

- Python 3.10+
- CUBRID 데이터베이스 서버 10.2+

## 설치

```bash
pip install pycubrid
```

## 빠른 시작

### 기본 연결

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

### 컨텍스트 매니저 사용

```python
import pycubrid

with pycubrid.connect(host="localhost", port=33000, database="testdb", user="dba") as conn:
    with conn.cursor() as cur:
        cur.execute("CREATE TABLE IF NOT EXISTS cookbook_users (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(100))")
        cur.execute("INSERT INTO cookbook_users (name) VALUES (?)", ("홍길동",))
        conn.commit()

        cur.execute("SELECT * FROM cookbook_users")
        for row in cur:
            print(row)
```

### 매개변수 바인딩

```python
# qmark 스타일 (물음표)
cur.execute("SELECT * FROM users WHERE name = ? AND age > ?", ("Alice", 25))

# executemany를 사용한 배치 삽입
data = [("Alice", 30), ("Bob", 25), ("Charlie", 35)]
cur.executemany("INSERT INTO users (name, age) VALUES (?, ?)", data)
conn.commit()
```

## PEP 249 준수 사항

| 속성 | 값 |
|---|---|
| `apilevel` | `"2.0"` |
| `threadsafety` | `1` (연결은 스레드 간 공유 불가) |
| `paramstyle` | `"qmark"` (위치 매개변수 `?`) |

- 완전한 표준 예외 체계: `Warning`, `Error`, `InterfaceError`, `DatabaseError`, `OperationalError`, `IntegrityError`, `InternalError`, `ProgrammingError`, `NotSupportedError`
- 표준 타입 객체: `STRING`, `BINARY`, `NUMBER`, `DATETIME`, `ROWID`
- 표준 생성자: `Date()`, `Time()`, `Timestamp()`, `Binary()`, `DateFromTicks()`, `TimeFromTicks()`, `TimestampFromTicks()`

## 지원 CUBRID 버전

CI에서 검증된 CUBRID 버전:

- 11.2
- 11.4

## SQLAlchemy 연동

pycubrid는 [sqlalchemy-cubrid](https://github.com/cubrid-labs/sqlalchemy-cubrid) — CUBRID용 SQLAlchemy 2.0 방언의 드라이버로 사용됩니다:

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

모든 SQLAlchemy 기능 (ORM, Core, Alembic 마이그레이션, 스키마 리플렉션)이 pycubrid 드라이버와 투명하게 동작합니다.

## 문서

| 가이드 | 설명 |
|---|---|
| [연결](CONNECTION.md) | 연결 문자열, URL 형식, 설정 옵션, 커넥션 풀 |
| [타입 매핑](TYPES.md) | 전체 타입 매핑, CUBRID 전용 타입, 컬렉션 타입 |
| [API 레퍼런스](API_REFERENCE.md) | 전체 API 문서 — 모듈, 클래스, 함수 |
| [프로토콜](PROTOCOL.md) | CAS 와이어 프로토콜 레퍼런스 |
| [개발 가이드](DEVELOPMENT.md) | 개발 환경 설정, 테스트, Docker, 커버리지, CI/CD |
| [예제](EXAMPLES.md) | 실용적인 사용 예제와 코드 |

## 호환성

| | Python 3.10 | Python 3.11 | Python 3.12 | Python 3.13 |
|---|:---:|:---:|:---:|:---:|
| **오프라인 테스트** | ✅ | ✅ | ✅ | ✅ |
| **CUBRID 11.4** | ✅ | -- | ✅ | -- |
| **CUBRID 11.2** | ✅ | -- | ✅ | -- |

## 기여하기

기여 가이드라인은 [CONTRIBUTING.md](../CONTRIBUTING.md)를, 개발 환경 설정은 [docs/DEVELOPMENT.md](DEVELOPMENT.md)를 참고하세요.

## 보안

보안 취약점은 이메일로 제보해 주세요 — 자세한 내용은 [SECURITY.md](../SECURITY.md)를 참고하세요. 보안 관련 사항은 공개 이슈로 등록하지 마세요.

## 라이선스

MIT — [LICENSE](../LICENSE) 참조.
