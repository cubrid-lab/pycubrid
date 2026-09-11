# 타입 시스템 (한국어)

> 🌐 [TYPES.md](https://github.com/cubrid-lab/pycubrid/blob/main/docs/TYPES.md)의 번역입니다. 영어 원문이 표준이며, 페이지 번역은 경고 수준의 동기화 규칙을 따릅니다.

pycubrid의 PEP 249 타입 객체·생성자·CUBRID CCI 데이터 타입 코드의 완전한 참조.

---

## 목차

- [개요](#개요)
- [선언 SQL 타입 vs Fetch/Description 타입](#선언-sql-타입-vs-fetchdescription-타입)
- [DBAPIType 클래스](#dbapitype-클래스)
- [PEP 249 타입 객체](#pep-249-타입-객체)
  - [STRING](#string)
  - [BINARY](#binary)
  - [NUMBER](#number)
  - [DATETIME](#datetime)
  - [ROWID](#rowid)
- [PEP 249 생성자](#pep-249-생성자)
- [CUBRID CCI_U_TYPE 코드](#cubrid-cci_u_type-코드)
- [Fetch 변환 표](#fetch-변환-표)
- [LOB 타입 처리](#lob-타입-처리)
- [컬렉션 타입](#컬렉션-타입)
  - [JSON 컬럼](#json-컬럼)
  - [`decode_collections`](#decode_collections)
- [사용 예제](#사용-예제)

---

## 선언 SQL 타입 vs Fetch/Description 타입

이 가이드는 타입 정보의 두 가지 관점을 구분합니다:

- **선언 SQL 타입**은 DDL에서 정의하고 SQL에서 바인딩하는 컬럼 타입입니다 — `VARCHAR`, `JSON`, `SET`, `BLOB`, `TIMESTAMPTZ` 등.
- **Fetch/description 타입**은 `cursor.description[n][1]`로 노출되는 `CUBRIDDataType` 정수 코드와, pycubrid가 와이어 데이터를 디코딩한 후 반환하는 Python 객체입니다.

아래 PEP 249 타입 객체 섹션은 선언 SQL 타입을 표준 DB-API 패밀리로 묶습니다. 이후 섹션은 raw `CUBRIDDataType` 코드와 fetch 시 반환되는 정확한 Python 값을 나열합니다.

---

## 개요

pycubrid는 DB-API 2.0 (PEP 249) 타입 시스템을 다음으로 구현합니다:

- **타입 객체 5종** — `STRING`, `BINARY`, `NUMBER`, `DATETIME`, `ROWID`
- **생성자 7종** — `Date`, `Time`, `Timestamp`, `DateFromTicks`, `TimeFromTicks`, `TimestampFromTicks`, `Binary`
- **CCI 데이터 타입 코드 27+종** — CUBRID 와이어 타입을 Python 타입으로 매핑 (JSON·컬렉션 지원 포함)

타입 객체는 `cursor.description`의 타입 코드와 비교할 수 있습니다:

```python
import pycubrid

conn = pycubrid.connect(host="localhost", port=33000, database="testdb")
cur = conn.cursor()
cur.execute("SELECT name, age, created_at FROM users")

for col in cur.description:
    col_name = col[0]
    col_type = col[1]

    if col_type == pycubrid.STRING:
        print(f"{col_name} is a string column")
    elif col_type == pycubrid.NUMBER:
        print(f"{col_name} is a numeric column")
    elif col_type == pycubrid.DATETIME:
        print(f"{col_name} is a date/time column")
```

---

## DBAPIType 클래스

`DBAPIType` 클래스는 PEP 249 타입 비교 프로토콜을 구현합니다. 각 인스턴스는 정수 타입 코드의 `frozenset`을 감싸며, 그 집합의 모든 코드와 같음 비교됩니다.

```python
from pycubrid.types import DBAPIType

# 커스텀 타입 객체 생성
MY_TYPE = DBAPIType("MY_TYPE", frozenset({1, 2, 3}))

# 정수 타입 코드와 비교
assert MY_TYPE == 1      # True — 1은 집합에 있음
assert MY_TYPE == 4      # False — 4는 집합에 없음
assert MY_TYPE != 4      # True

# 다른 DBAPIType 인스턴스와 비교
OTHER = DBAPIType("OTHER", frozenset({1, 2, 3}))
assert MY_TYPE == OTHER  # True — 같은 값 집합
```

### 메서드

| 메서드 | 설명 |
|---|---|
| `__eq__(other)` | `other`(int)가 `values`에 있거나, `other`(DBAPIType)가 같은 `values` 집합을 가지면 `True` |
| `__ne__(other)` | `__eq__`의 부정 |
| `__hash__()` | 값의 frozen 집합 기반 해시 |
| `__repr__()` | `DBAPIType('name')` 반환 |

---

## PEP 249 타입 객체

### STRING

문자열 기반 컬럼을 기술합니다.

```python
from pycubrid import STRING
```

| 멤버 타입 | CCI 코드 | CUBRID SQL 타입 |
|---|---|---|
| CHAR | 1 | `CHAR(n)` |
| STRING (VARCHAR) | 2 | `VARCHAR(n)` |
| NCHAR | 3 | `NCHAR(n)` |
| VARNCHAR | 4 | `NCHAR VARYING(n)` |
| ENUM | 25 | `ENUM` |
| CLOB | 24 | `CLOB` |
| JSON | 34 | `JSON` |

### BINARY

바이너리 데이터 컬럼을 기술합니다.

```python
from pycubrid import BINARY
```

| 멤버 타입 | CCI 코드 | CUBRID SQL 타입 |
|---|---|---|
| BIT | 5 | `BIT(n)` |
| VARBIT | 6 | `BIT VARYING(n)` |
| BLOB | 23 | `BLOB` |

### NUMBER

숫자 컬럼을 기술합니다.

```python
from pycubrid import NUMBER
```

| 멤버 타입 | CCI 코드 | CUBRID SQL 타입 | Python 타입 |
|---|---|---|---|
| SHORT | 9 | `SMALLINT` | `int` |
| INT | 8 | `INTEGER` | `int` |
| BIGINT | 21 | `BIGINT` | `int` |
| FLOAT | 11 | `FLOAT` | `float` |
| DOUBLE | 12 | `DOUBLE` | `float` |
| NUMERIC | 7 | `NUMERIC(p, s)` | `Decimal` |
| MONETARY | 10 | `MONETARY` | `float` |

### DATETIME

날짜와 시간 컬럼을 기술합니다.

```python
from pycubrid import DATETIME
```

| 멤버 타입 | CCI 코드 | CUBRID SQL 타입 | Python 타입 |
|---|---|---|---|
| DATE | 13 | `DATE` | `datetime.date` |
| TIME | 14 | `TIME` | `datetime.time` |
| TIMESTAMP | 15 | `TIMESTAMP` | `datetime.datetime` |
| DATETIME | 22 | `DATETIME` | `datetime.datetime` |
| TIMESTAMPTZ | 29 | `TIMESTAMPTZ` | `datetime.datetime` |
| TIMESTAMPLTZ | 30 | `TIMESTAMPLTZ` | `datetime.datetime` |
| DATETIMETZ | 31 | `DATETIMETZ` | `datetime.datetime` |
| DATETIMELTZ | 32 | `DATETIMELTZ` | `datetime.datetime` |

### ROWID

행 식별자 컬럼을 기술합니다.

```python
from pycubrid import ROWID
```

| 멤버 타입 | CCI 코드 | CUBRID SQL 타입 | Python 타입 |
|---|---|---|---|
| OBJECT | 19 | `OBJECT` (OID) | `str` (`"OID:@page\|slot\|volume"`) |

---

## PEP 249 생성자

이 함수들은 쿼리 파라미터로 사용할 수 있는 Python 객체를 만듭니다.

| 생성자 | 시그니처 | 반환 | 설명 |
|---|---|---|---|
| `Date` | `(year, month, day)` | `datetime.date` | 달력 날짜 |
| `Time` | `(hour, minute, second)` | `datetime.time` | 시각 |
| `Timestamp` | `(year, month, day, hour, minute, second)` | `datetime.datetime` | 날짜와 시간 |
| `DateFromTicks` | `(ticks)` | `datetime.date` | Unix 타임스탬프로부터 날짜 |
| `TimeFromTicks` | `(ticks)` | `datetime.time` | Unix 타임스탬프로부터 시각 |
| `TimestampFromTicks` | `(ticks)` | `datetime.datetime` | Unix 타임스탬프로부터 datetime |
| `Binary` | `(value)` | `bytes` | `bytes`, `bytearray`, 또는 `str`(UTF-8)로부터 바이너리 데이터 |

### 사용

```python
import pycubrid

# 날짜/시간 생성자
d = pycubrid.Date(2025, 3, 15)
t = pycubrid.Time(14, 30, 0)
ts = pycubrid.Timestamp(2025, 3, 15, 14, 30, 0)

# Unix 타임스탬프로부터
d2 = pycubrid.DateFromTicks(1710500000.0)
t2 = pycubrid.TimeFromTicks(1710500000.0)
ts2 = pycubrid.TimestampFromTicks(1710500000.0)

# 바이너리 생성자
b1 = pycubrid.Binary(b"\x00\x01\x02")           # bytes로부터
b2 = pycubrid.Binary(bytearray([0, 1, 2]))      # bytearray로부터
b3 = pycubrid.Binary("hello")                    # str로부터 → UTF-8 인코딩된 bytes
```

---

## CUBRID CCI_U_TYPE 코드

이 정수 코드들은 CAS 와이어 프로토콜에서 사용되며 `cursor.description[n][1]`에 나타납니다.

| 상수 | 코드 | CUBRID 타입 | 분류 |
|---|---|---|---|
| `UNKNOWN` / `NULL` | 0 | NULL | — |
| `CHAR` | 1 | `CHAR(n)` | STRING |
| `STRING` | 2 | `VARCHAR(n)` | STRING |
| `NCHAR` | 3 | `NCHAR(n)` | STRING |
| `VARNCHAR` | 4 | `NCHAR VARYING(n)` | STRING |
| `BIT` | 5 | `BIT(n)` | BINARY |
| `VARBIT` | 6 | `BIT VARYING(n)` | BINARY |
| `NUMERIC` | 7 | `NUMERIC(p, s)` | NUMBER |
| `INT` | 8 | `INTEGER` | NUMBER |
| `SHORT` | 9 | `SMALLINT` | NUMBER |
| `MONETARY` | 10 | `MONETARY` | NUMBER |
| `FLOAT` | 11 | `FLOAT` | NUMBER |
| `DOUBLE` | 12 | `DOUBLE` | NUMBER |
| `DATE` | 13 | `DATE` | DATETIME |
| `TIME` | 14 | `TIME` | DATETIME |
| `TIMESTAMP` | 15 | `TIMESTAMP` | DATETIME |
| `SET` | 16 | `SET` | 컬렉션 |
| `MULTISET` | 17 | `MULTISET` | 컬렉션 |
| `SEQUENCE` | 18 | `SEQUENCE` / `LIST` | 컬렉션 |
| `OBJECT` | 19 | `OBJECT` (OID) | ROWID |
| `RESULTSET` | 20 | 결과 집합 | — |
| `BIGINT` | 21 | `BIGINT` | NUMBER |
| `DATETIME` | 22 | `DATETIME` | DATETIME |
| `BLOB` | 23 | `BLOB` | BINARY |
| `CLOB` | 24 | `CLOB` | STRING |
| `ENUM` | 25 | `ENUM` | STRING |
| `JSON` | 34 | `JSON` | STRING |
| `TIMESTAMPTZ` | 29 | `TIMESTAMPTZ` | DATETIME |
| `TIMESTAMPLTZ` | 30 | `TIMESTAMPLTZ` | DATETIME |
| `DATETIMETZ` | 31 | `DATETIMETZ` | DATETIME |
| `DATETIMELTZ` | 32 | `DATETIMELTZ` | DATETIME |

이 코드들은 `CUBRIDDataType` enum으로 사용할 수 있습니다:

```python
from pycubrid.constants import CUBRIDDataType

print(CUBRIDDataType.INT)       # CUBRIDDataType.INT (8)
print(CUBRIDDataType.VARCHAR)   # AttributeError — CUBRIDDataType.STRING (2)을 사용하세요
```

---

## Fetch 변환 표

fetch 시 pycubrid가 CUBRID 와이어 타입을 Python 객체로 변환하는 방식:

| CUBRID 타입 | CCI 코드 | Python 타입 | 비고 |
|---|---|---|---|
| `CHAR`, `VARCHAR`, `NCHAR`, `NCHAR VARYING`, `ENUM` | 1–4, 25 | `str` | Null 종단, UTF-8 디코딩 |
| `SHORT` (SMALLINT) | 9 | `int` | 16비트 부호 있는 정수 |
| `INTEGER` | 8 | `int` | 32비트 부호 있는 정수 |
| `BIGINT` | 21 | `int` | 64비트 부호 있는 정수 |
| `FLOAT` | 11 | `float` | IEEE 754 단정밀도 |
| `DOUBLE`, `MONETARY` | 12, 10 | `float` | IEEE 754 배정밀도 |
| `NUMERIC` / `DECIMAL` | 7 | `Decimal` | 정확한 숫자 (문자열 파싱) |
| `DATE` | 13 | `datetime.date` | 달력 날짜 |
| `TIME` | 14 | `datetime.time` | 시각 |
| `TIMESTAMP` | 15 | `datetime.datetime` | 날짜 + 시간 (microsecond = 0) |
| `DATETIME` | 22 | `datetime.datetime` | 날짜 + 시간 + 밀리초 |
| `TIMESTAMPTZ`, `TIMESTAMPLTZ` | 29, 30 | `datetime.datetime` | 타임존 포함 타임스탬프 (초 정밀도, microsecond = 0) |
| `DATETIMETZ`, `DATETIMELTZ` | 31, 32 | `datetime.datetime` | 타임존 포함 datetime (밀리초 정밀도) |
| `BIT`, `BIT VARYING` | 5, 6 | `bytes` | raw 바이너리 데이터 |
| `JSON` | 34 | `str` 또는 `Any` | 기본은 raw JSON 문자열; `json_deserializer=` 설정 시 디코딩됨 |
| `SET`, `MULTISET`, `SEQUENCE` | 16, 17, 18 | `bytes` 또는 디코딩된 컬렉션 | `decode_collections=True`일 때만 디코딩 |
| `OBJECT` (OID) | 19 | `str` | 형식: `"OID:@page\|slot\|volume"` |
| `BLOB` | 23 | `dict` | LOB 핸들 (아래 참고) |
| `CLOB` | 24 | `dict` | LOB 핸들 (아래 참고) |
| `NULL` / `UNKNOWN` | 0 | `None` | — |

---

## LOB 타입 처리

LOB 컬럼(BLOB과 CLOB)은 내용으로 반환되지 **않습니다**. 대신, `Lob` 클래스와 함께 실제 데이터를 읽을 수 있는 **핸들 딕셔너리**를 반환합니다.

### LOB 핸들 구조

BLOB 또는 CLOB 컬럼을 fetch하면 다음을 받습니다:

```python
{
    "lob_type": 23,                    # CUBRIDDataType.BLOB (23) 또는 CLOB (24)
    "lob_length": 1024,                # LOB 내용의 바이트 크기
    "file_locator": "file://.../...",   # 서버 측 파일 로케이터
    "packed_lob_handle": b"...",        # Lob.read()용 raw 핸들 바이트
}
```

### LOB 내용 읽기

```python
from pycubrid.lob import Lob
from pycubrid.constants import CUBRIDDataType

conn = pycubrid.connect(host="localhost", port=33000, database="testdb")
cur = conn.cursor()

# CLOB 컬럼에 문자열 데이터 직접 삽입
cur.execute("INSERT INTO cookbook_docs (content) VALUES ('Hello, CLOB!')")
conn.commit()

# fetch는 LOB 핸들 dict를 반환, 내용이 아님
cur.execute("SELECT content FROM cookbook_docs WHERE id = 1")
row = cur.fetchone()
lob_handle = row[0]  # lob_type, lob_length 등을 담은 dict

# 내용을 읽으려면 packed 핸들로 Lob 클래스 사용
lob = Lob(conn, CUBRIDDataType.CLOB, lob_handle["packed_lob_handle"])
content = lob.read(lob_handle["lob_length"])
print(content)  # b'Hello, CLOB!'
```

> **중요**: `Lob` 객체를 쿼리 파라미터로 전달할 수 없습니다. CLOB 컬럼에는 문자열을, BLOB 컬럼에는 바이트를 직접 삽입하세요.

---

## 컬렉션 타입

CUBRID의 컬렉션 타입(`SET`, `MULTISET`, `SEQUENCE`)은 하위 호환을 위해 기본적으로 raw `bytes`로 반환됩니다. `connect()` 또는 `pycubrid.aio.connect()`에 `decode_collections=True`를 전달하면, pycubrid는 지원되는 컬렉션 페이로드를 Python 컨테이너로 디코딩합니다.

| CUBRID 타입 | CCI 코드 | `decode_collections=False` | `decode_collections=True` |
|---|---|---|---|
| `SET` | 16 | `bytes` | 해시 가능하면 `frozenset`, 아니면 `tuple` |
| `MULTISET` | 17 | `bytes` | `list` |
| `SEQUENCE` | 18 | `bytes` | `list` |

참고:

- 중첩 컬렉션 페이로드는 raw `bytes`로 유지됩니다.
- 알 수 없는 컬렉션 요소 타입은 raw `bytes`로 폴백됩니다.
- `SET` 값은 모든 디코딩된 요소가 해시 가능하면 `frozenset`으로 정규화됩니다.
  집합에 해시 불가능한 요소가 있으면(예: 디코딩된 JSON 객체), 예외 없이 내용을 보존하기 위해 `tuple`을 반환합니다.

### JSON 컬럼

JSON 컬럼은 CUBRID 타입 코드 `34`를 사용합니다.

| 연결 옵션 | fetch 결과 |
|---|---|
| 기본 (`json_deserializer=None`) | raw JSON `str` |
| `json_deserializer=json.loads` | 디코딩된 Python 객체 |
| `json_deserializer=custom_callable` | 콜러블의 반환 값 |

이 옵트인 동작은 애플리케이션이 JSON 파싱을 지연시키는 것을 선호할 때 fetch를 할당-가볍게 유지합니다.

### `decode_collections`

`decode_collections`는 `pycubrid.connect()`와 `pycubrid.aio.connect()` 양쪽의 연결 수준 스위치입니다.

| 설정 | 결과 |
|---|---|
| `False` (기본) | 컬렉션 페이로드를 raw CAS 와이어 `bytes`로 반환 |
| `True` | 지원되는 `SET`, `MULTISET`, `SEQUENCE` 페이로드를 Python 컨테이너로 디코딩 |

---

## 사용 예제

### cursor.description으로 타입 확인

```python
import pycubrid

conn = pycubrid.connect(host="localhost", port=33000, database="testdb")
cur = conn.cursor()
cur.execute("SELECT * FROM users LIMIT 1")

for col in cur.description:
    name, type_code, _, _, precision, scale, nullable = col
    if type_code == pycubrid.STRING:
        print(f"  {name}: STRING (precision={precision})")
    elif type_code == pycubrid.NUMBER:
        print(f"  {name}: NUMBER (precision={precision}, scale={scale})")
    elif type_code == pycubrid.DATETIME:
        print(f"  {name}: DATETIME")
    elif type_code == pycubrid.BINARY:
        print(f"  {name}: BINARY")
    elif type_code == pycubrid.ROWID:
        print(f"  {name}: ROWID (OID)")
    else:
        print(f"  {name}: type_code={type_code}")

cur.close()
conn.close()
```

### 생성자로 파라미터 바인딩

```python
import pycubrid

conn = pycubrid.connect(host="localhost", port=33000, database="testdb")
cur = conn.cursor()

# 파라미터 값으로 PEP 249 생성자 사용
cur.execute(
    "INSERT INTO cookbook_events (event_date, event_time, created_at) VALUES (?, ?, ?)",
    [
        pycubrid.Date(2025, 12, 25),
        pycubrid.Time(10, 0, 0),
        pycubrid.Timestamp(2025, 3, 15, 14, 30, 0),
    ],
)
conn.commit()
cur.close()
conn.close()
```

### CUBRIDDataType enum 사용

```python
from pycubrid.constants import CUBRIDDataType

# 특정 타입 코드 확인
assert CUBRIDDataType.INT == 8
assert CUBRIDDataType.STRING == 2
assert CUBRIDDataType.DATETIME == 22

# 타입 분기 로직에 사용
def describe_type(code: int) -> str:
    match code:
        case CUBRIDDataType.INT | CUBRIDDataType.BIGINT | CUBRIDDataType.SHORT:
            return "integer"
        case CUBRIDDataType.FLOAT | CUBRIDDataType.DOUBLE:
            return "floating point"
        case CUBRIDDataType.NUMERIC:
            return "exact decimal"
        case CUBRIDDataType.STRING | CUBRIDDataType.CHAR:
            return "text"
        case _:
            return f"other ({code})"
```

---

*참고: [API 참조](API_REFERENCE.md) · [예제](EXAMPLES.md) · [연결 가이드](CONNECTION.md)*
