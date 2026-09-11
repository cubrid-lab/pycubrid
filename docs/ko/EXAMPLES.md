# 사용 예제 (한국어)

> 🌐 [EXAMPLES.md](https://github.com/cubrid-lab/pycubrid/blob/main/docs/EXAMPLES.md)의 번역입니다. 영어 원문이 표준이며, 페이지 번역은 경고 수준의 동기화 규칙을 따릅니다.

기본 CRUD부터 고급 기능까지 pycubrid의 실용적인 예제 모음.

---

## 목차

- [기본 연결](#기본-연결)
- [TLS / SSL 연결](#tls--ssl-연결)
- [CRUD 연산](#crud-연산)
  - [테이블 생성](#테이블-생성)
  - [행 삽입](#행-삽입)
  - [행 조회](#행-조회)
  - [행 수정](#행-수정)
  - [행 삭제](#행-삭제)
- [트랜잭션](#트랜잭션)
  - [수동 커밋/롤백](#수동-커밋롤백)
  - [컨텍스트 매니저](#컨텍스트-매니저)
  - [오토커밋 모드](#오토커밋-모드)
- [파라미터화 쿼리](#파라미터화-쿼리)
- [배치 연산](#배치-연산)
  - [executemany](#executemany)
  - [executemany_batch](#executemany_batch)
- [가져오기 전략](#가져오기-전략)
- [커서 이터레이터](#커서-이터레이터)
- [컬럼 메타데이터](#컬럼-메타데이터)
- [LOB 처리](#lob-처리)
- [스키마 내성(인트로스펙션)](#스키마-내성인트로스펙션)
- [저장 프로시저](#저장-프로시저)
- [날짜와 시간](#날짜와-시간)
- [에러 처리](#에러-처리)
- [SQLAlchemy 연동](#sqlalchemy-연동)
- [커넥션 풀링 패턴](#커넥션-풀링-패턴)

---

## 기본 연결

```python
import pycubrid

# CUBRID 연결
conn = pycubrid.connect(
    host="localhost",
    port=33000,
    database="testdb",
    user="dba",
    password="",
)

# 서버 버전 확인
print(f"Server: {conn.get_server_version()}")

# 작업 수행...
cur = conn.cursor()
cur.execute("SELECT 1 + 1")
print(cur.fetchone())  # (2,)

# 정리
cur.close()
conn.close()
```

!!! tip
    문장마다 열고 닫는 대신 관련 연산끼리 하나의 연결을 유지하세요.

---

## TLS / SSL 연결

CUBRID는 STARTTLS 방식 업그레이드를 사용합니다: 드라이버가 평문 소켓을 열고, 브로커와 `CUBRS` 핸드셰이크 매직을 교환한 뒤, `OPEN_DATABASE`를 보내기 전에 라이브 전송을 TLS로 업그레이드합니다. `pycubrid.connect()`와 `pycubrid.aio.connect()` 모두 같은 `ssl` 파라미터를 받습니다.

### 동기 — 기본 검증 컨텍스트

```python
import pycubrid

# ssl=True는 ssl.create_default_context()에 minimum_version = TLSv1_2를 사용
conn = pycubrid.connect(
    host="cubrid.example.com",
    port=33000,
    database="testdb",
    user="dba",
    password="secret",
    ssl=True,
)
print(conn.get_server_version())
conn.close()
```

### 동기 — 커스텀 CA 번들

```python
import ssl
import pycubrid

ctx = ssl.create_default_context(cafile="/etc/ssl/my-ca.pem")
ctx.minimum_version = ssl.TLSVersion.TLSv1_2

conn = pycubrid.connect(
    host="cubrid.example.com",
    port=33000,
    database="testdb",
    user="dba",
    password="secret",
    ssl=ctx,
)
conn.close()
```

### 비동기

```python
import asyncio
import pycubrid.aio

async def main() -> None:
    conn = await pycubrid.aio.connect(
        host="cubrid.example.com",
        port=33000,
        database="testdb",
        user="dba",
        password="secret",
        ssl=True,
    )
    cur = conn.cursor()
    await cur.execute("SELECT 1")
    print(await cur.fetchone())
    await cur.close()
    await conn.close()

asyncio.run(main())
```

!!! warning "Python 3.10 비동기 TLS"
    `loop.start_tls()`는 Python 3.10에서 인증서 검증 실패 시 멈출 수 있습니다(Python 3.10의 알려진 CPython asyncio TLS 핸드셰이크 버그, 3.13/3.14에서 수정). 3.10에서 프로덕션 비동기 TLS를 사용하려면 인증서 체인을 사전에 검증하거나 동기 경로를 사용하세요. [문제 해결](TROUBLESHOOTING.md#async-tls-handshake-hangs-on-python-310)과 [#156](https://github.com/cubrid-lab/pycubrid/issues/156)을 참고하세요.

!!! note
    TLS 연결이 성공하려면 브로커의 `cubrid_broker.conf`에 `SSL=ON`이 설정되어 있어야 합니다. 전체 내용은 [연결 가이드](CONNECTION.md#ssltls)를 참고하세요.

---

## CRUD 연산

```mermaid
flowchart LR
    A[CREATE TABLE] --> B[INSERT]
    B --> C[SELECT]
    C --> D[UPDATE]
    D --> E[DELETE]
    E --> F[COMMIT]
```

### 테이블 생성

```python
import pycubrid

conn = pycubrid.connect(database="testdb")
cur = conn.cursor()

cur.execute("""
    CREATE TABLE IF NOT EXISTS users (
        id INT AUTO_INCREMENT PRIMARY KEY,
        name VARCHAR(100) NOT NULL,
        email VARCHAR(200) UNIQUE,
        age INT DEFAULT 0,
        created_at DATETIME DEFAULT SYS_DATETIME
    )
""")
conn.commit()

cur.close()
conn.close()
```

### 행 삽입

```python
conn = pycubrid.connect(database="testdb")
cur = conn.cursor()

# 단일 삽입
cur.execute(
    "INSERT INTO users (name, email, age) VALUES (?, ?, ?)",
    ["Alice", "alice@example.com", 30],
)
print(f"Inserted ID: {cur.lastrowid}")

# 다중 삽입
users = [
    ["Bob", "bob@example.com", 25],
    ["Carol", "carol@example.com", 28],
    ["Dave", "dave@example.com", 35],
]
cur.executemany(
    "INSERT INTO users (name, email, age) VALUES (?, ?, ?)",
    users,
)
print(f"Inserted {cur.rowcount} rows")

conn.commit()
cur.close()
conn.close()
```

!!! warning
    SQL에 문자열 보간을 사용하지 마세요. 항상 `?` 플레이스홀더와 파라미터 리스트/튜플을 사용하세요.

### 행 조회

```python
conn = pycubrid.connect(database="testdb")
cur = conn.cursor()

# 전체 행
cur.execute("SELECT id, name, email, age FROM users ORDER BY id")
for row in cur.fetchall():
    print(f"  {row[0]}: {row[1]} ({row[2]}) age={row[3]}")

# 필터링된 쿼리
cur.execute("SELECT name, age FROM users WHERE age > ?", [27])
print(f"\nUsers older than 27:")
for name, age in cur:
    print(f"  {name}: {age}")

cur.close()
conn.close()
```

### 행 수정

```python
conn = pycubrid.connect(database="testdb")
cur = conn.cursor()

cur.execute(
    "UPDATE users SET age = ? WHERE name = ?",
    [31, "Alice"],
)
print(f"Updated {cur.rowcount} row(s)")

conn.commit()
cur.close()
conn.close()
```

### 행 삭제

```python
conn = pycubrid.connect(database="testdb")
cur = conn.cursor()

cur.execute("DELETE FROM users WHERE name = ?", ["Dave"])
print(f"Deleted {cur.rowcount} row(s)")

conn.commit()
cur.close()
conn.close()
```

---

## 트랜잭션

### 수동 커밋/롤백

```python
conn = pycubrid.connect(database="testdb")
cur = conn.cursor()

try:
    cur.execute("INSERT INTO users (name, email) VALUES (?, ?)",
                ["Eve", "eve@example.com"])
    cur.execute("INSERT INTO users (name, email) VALUES (?, ?)",
                ["Frank", "frank@example.com"])
    conn.commit()
    print("Transaction committed")
except pycubrid.Error as e:
    conn.rollback()
    print(f"Transaction rolled back: {e}")
finally:
    cur.close()
    conn.close()
```

### 컨텍스트 매니저

연결 컨텍스트 매니저는 성공 시 자동 커밋, 예외 시 자동 롤백합니다:

```python
with pycubrid.connect(database="testdb") as conn:
    cur = conn.cursor()
    cur.execute("INSERT INTO users (name, email) VALUES (?, ?)",
                ["Grace", "grace@example.com"])
    # 예외 없이 `with` 블록을 벗어나면 자동 커밋
    # 예외가 발생하면 자동 롤백
```

!!! note
    컨텍스트 매니저 사용 시 성공 경로에서 명시적 `conn.commit()`은 필요 없습니다.

### 오토커밋 모드

```python
# 오토커밋 명시적 활성화 (기본은 False)
conn = pycubrid.connect(database="testdb", autocommit=True)
cur = conn.cursor()

# 각 문장이 즉시 커밋 — 명시적 커밋 불필요
cur.execute("INSERT INTO users (name) VALUES (?)", ["Heidi"])
cur.execute("INSERT INTO users (name) VALUES (?)", ["Ivan"])

# 동적으로 토글도 가능
conn.autocommit = False
cur.execute("INSERT INTO users (name) VALUES (?)", ["Judy"])
conn.commit()  # 이제 수동 커밋 필요

cur.close()
conn.close()
```

---

## 파라미터화 쿼리

pycubrid는 `qmark` 파라미터 방식 — `?` 플레이스홀더를 사용합니다:

```python
cur = conn.cursor()

# 위치 파라미터 (list 또는 tuple)
cur.execute("SELECT * FROM users WHERE name = ? AND age > ?", ["Alice", 25])

# 지원 타입
import datetime
from decimal import Decimal

cur.execute("""
    INSERT INTO cookbook_products (name, price, available, launch_date)
    VALUES (?, ?, ?, ?)
""", [
    "Widget",                              # str  → 'Widget'
    Decimal("19.99"),                       # Decimal → 19.99
    True,                                  # bool → 1
    datetime.date(2025, 6, 15),            # date → DATE'2025-06-15'
])

# None은 NULL로 매핑
cur.execute("INSERT INTO users (name, email) VALUES (?, ?)",
            ["Nobody", None])
```

### 파라미터 바인딩 참조

| 파라미터 형태 | 예제 | 비고 |
|---|---|---|
| `list` / `tuple` | `cur.execute(sql, ["Alice", 25])` | 위치 명확성에 권장 |
| `None` | `cur.execute(sql, [None])` | SQL `NULL`로 인코딩 |

!!! danger
    문자열이 아닌 시퀀스 대신 스칼라나 매핑을 전달하면(예: `params="Alice"` 또는 `params={"name": "Alice"}`) `ProgrammingError`가 발생합니다.

---

## 배치 연산

### executemany

같은 SQL을 서로 다른 파라미터 세트로 실행:

```python
cur = conn.cursor()

users = [
    ("Alice", 30),
    ("Bob", 25),
    ("Carol", 28),
]
cur.executemany(
    "INSERT INTO users (name, age) VALUES (?, ?)",
    users,
)
print(f"Inserted {cur.rowcount} rows")  # 3
conn.commit()
```

### executemany_batch

**서로 다른** SQL 문을 한 번의 서버 왕복으로 실행:

```python
cur = conn.cursor()

results = cur.executemany_batch([
    "INSERT INTO users (name, age) VALUES ('Xena', 40)",
    "INSERT INTO users (name, age) VALUES ('Yuri', 22)",
    "UPDATE users SET age = 26 WHERE name = 'Bob'",
])

for stmt_type, count in results:
    print(f"Statement type {stmt_type}: affected {count} row(s)")

conn.commit()
```

---

## 가져오기 전략

```python
cur = conn.cursor()
cur.execute("SELECT id, name FROM users ORDER BY id")

# fetchone — 한 번에 한 행
row = cur.fetchone()
print(f"First: {row}")

# fetchmany — N행 배치
batch = cur.fetchmany(3)
print(f"Next 3: {batch}")

# fetchall — 나머지 전체
rest = cur.fetchall()
print(f"Remaining: {len(rest)} rows")
```

### 배열 크기

`fetchmany()`의 기본 배치 크기 조절:

```python
cur.arraysize = 50
cur.execute("SELECT * FROM users")
batch = cur.fetchmany()  # 최대 50행 가져옴
```

---

## 커서 이터레이터

```python
cur = conn.cursor()
cur.execute("SELECT name, age FROM users")

for name, age in cur:
    print(f"{name} is {age} years old")
```

---

## 컬럼 메타데이터

```python
cur = conn.cursor()
cur.execute("SELECT id, name, email, age FROM users")

print("Columns:")
for col in cur.description:
    print(f"  {col[0]:15s} type={col[1]:3d}  precision={col[4]}  nullable={col[6]}")

# 출력:
#   id              type=  8  precision=10  nullable=False
#   name            type=  2  precision=100  nullable=False
#   email           type=  2  precision=200  nullable=True
#   age             type=  8  precision=10  nullable=True
```

---

## LOB 처리

### LOB 데이터 삽입

대부분의 사용 사례에서는 문자열이나 바이트를 직접 삽입하면 됩니다:

```python
cur = conn.cursor()

# CLOB — 텍스트 직접 삽입
cur.execute("""
    CREATE TABLE IF NOT EXISTS cookbook_documents (
        id INT AUTO_INCREMENT PRIMARY KEY,
        title VARCHAR(100),
        content CLOB
    )
""")
conn.commit()

cur.execute(
    "INSERT INTO cookbook_documents (title, content) VALUES (?, ?)",
    ["Report", "This is a large text document..."],
)
conn.commit()
```

### LOB 데이터 읽기

LOB 컬럼은 메타데이터가 담긴 dict를 반환합니다:

```python
cur.execute("SELECT title, content FROM cookbook_documents WHERE id = 1")
row = cur.fetchone()

title = row[0]       # "Report"
lob_info = row[1]    # dict
print(f"LOB type: {lob_info['lob_type']}")        # 24 (CLOB)
print(f"LOB length: {lob_info['lob_length']}")     # 바이트 길이
print(f"Locator: {lob_info['file_locator']}")      # 서버 파일 경로
```

### Lob 클래스 사용

세밀한 LOB 제어가 필요할 때:

```python
from pycubrid.constants import CUBRIDDataType

# 서버에 LOB 핸들 생성
lob = conn.create_lob(CUBRIDDataType.CLOB)  # 24

# 데이터 쓰기
lob.write(b"Hello, CUBRID LOB!")

# 데이터 다시 읽기
data = lob.read(length=1024, offset=0)
print(data)  # b"Hello, CUBRID LOB!"
```

---

## 스키마 내성(인트로스펙션)

```python
from pycubrid.constants import CCISchemaType

# 모든 테이블 나열
packet = conn.get_schema_info(CCISchemaType.CLASS)
print(f"Found {packet.tuple_count} tables")

# 특정 테이블의 컬럼 나열
packet = conn.get_schema_info(CCISchemaType.ATTRIBUTE, table_name="users")
print(f"Table has {packet.tuple_count} columns")

# 기본 키 정보 조회
packet = conn.get_schema_info(CCISchemaType.PRIMARY_KEY, table_name="users")
print(f"Primary key entries: {packet.tuple_count}")
```

---

## 저장 프로시저

```python
cur = conn.cursor()

# 저장 프로시저 생성
cur.execute("""
    CREATE OR REPLACE PROCEDURE cookbook_greet(name VARCHAR)
    AS LANGUAGE JAVA
    NAME 'com.example.Greet.greet(java.lang.String)'
""")
conn.commit()

# 호출
cur.callproc("cookbook_greet", ["World"])
```

> **참고:** CUBRID 저장 프로시저는 Java 기반입니다. Java 클래스가 서버에 등록되어 있는지 확인하세요.

---

## 날짜와 시간

```python
import datetime
import pycubrid

conn = pycubrid.connect(database="testdb")
cur = conn.cursor()

cur.execute("""
    CREATE TABLE IF NOT EXISTS cookbook_events (
        id INT AUTO_INCREMENT PRIMARY KEY,
        event_name VARCHAR(100),
        event_date DATE,
        event_time TIME,
        event_ts DATETIME
    )
""")
conn.commit()

# Python datetime 객체로 삽입
cur.execute(
    "INSERT INTO cookbook_events (event_name, event_date, event_time, event_ts) VALUES (?, ?, ?, ?)",
    [
        "Launch Party",
        datetime.date(2025, 6, 15),
        datetime.time(14, 30, 0),
        datetime.datetime(2025, 6, 15, 14, 30, 0),
    ],
)
conn.commit()

# 다시 읽기 — Python datetime 객체로 반환
cur.execute("SELECT event_name, event_date, event_time, event_ts FROM cookbook_events")
row = cur.fetchone()
print(f"Event: {row[0]}")
print(f"Date:  {row[1]}")  # datetime.date(2025, 6, 15)
print(f"Time:  {row[2]}")  # datetime.time(14, 30)
print(f"TS:    {row[3]}")  # datetime.datetime(2025, 6, 15, 14, 30)

# PEP 249 생성자 사용
d = pycubrid.Date(2025, 1, 1)
t = pycubrid.Time(12, 0, 0)
ts = pycubrid.Timestamp(2025, 1, 1, 12, 0, 0)

cur.close()
conn.close()
```

---

## 에러 처리

### 구체적 에러 잡기

```python
import pycubrid

conn = pycubrid.connect(database="testdb")
cur = conn.cursor()

try:
    cur.execute("INSERT INTO users (email) VALUES (?)", ["duplicate@example.com"])
    cur.execute("INSERT INTO users (email) VALUES (?)", ["duplicate@example.com"])
    conn.commit()
except pycubrid.IntegrityError as e:
    print(f"Duplicate key: {e.msg}")
    conn.rollback()
except pycubrid.ProgrammingError as e:
    print(f"SQL error: {e.msg}")
    conn.rollback()
except pycubrid.OperationalError as e:
    print(f"Connection error: {e.msg}")
except pycubrid.Error as e:
    print(f"Database error: {e.msg} (code={e.code})")
    conn.rollback()
finally:
    cur.close()
    conn.close()
```

### 닫힌 상태 확인

```python
conn = pycubrid.connect(database="testdb")
conn.close()

try:
    cur = conn.cursor()
except pycubrid.InterfaceError as e:
    print(f"Expected: {e.msg}")  # "connection is closed"
```

---

## SQLAlchemy 연동

pycubrid는 [sqlalchemy-cubrid](https://github.com/cubrid-lab/sqlalchemy-cubrid)의 드라이버로 동작합니다:

```python
from sqlalchemy import create_engine, text, Column, Integer, String
from sqlalchemy.orm import DeclarativeBase, Session

# pycubrid 드라이버로 연결
engine = create_engine("cubrid+pycubrid://dba@localhost:33000/testdb")

# Raw SQL
with engine.connect() as conn:
    result = conn.execute(text("SELECT 1 + 1"))
    print(result.scalar())  # 2

# ORM
class Base(DeclarativeBase):
    pass

class User(Base):
    __tablename__ = "cookbook_sa_users"
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(100))

Base.metadata.create_all(engine)

with Session(engine) as session:
    session.add(User(name="Alice"))
    session.commit()

    users = session.query(User).all()
    for u in users:
        print(f"{u.id}: {u.name}")
```

---

## 커넥션 풀링 패턴

pycubrid 자체에는 커넥션 풀이 없지만, 간단한 패턴이나 SQLAlchemy 내장 풀을 사용할 수 있습니다:

### queue를 사용한 간단한 풀

```python
import queue
import pycubrid

class ConnectionPool:
    def __init__(self, size: int = 5, **connect_kwargs):
        self._pool: queue.Queue = queue.Queue(maxsize=size)
        self._connect_kwargs = connect_kwargs
        for _ in range(size):
            self._pool.put(pycubrid.connect(**connect_kwargs))

    def get(self) -> pycubrid.connection.Connection:
        return self._pool.get()

    def put(self, conn) -> None:
        self._pool.put(conn)

    def close_all(self) -> None:
        while not self._pool.empty():
            conn = self._pool.get_nowait()
            conn.close()

# 사용
pool = ConnectionPool(size=3, database="testdb")

conn = pool.get()
try:
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM users")
    print(cur.fetchone())
    cur.close()
finally:
    pool.put(conn)

pool.close_all()
```

### SQLAlchemy 풀 (권장)

```python
from sqlalchemy import create_engine

engine = create_engine(
    "cubrid+pycubrid://dba@localhost:33000/testdb",
    pool_size=5,
    max_overflow=10,
    pool_pre_ping=True,
)
```
