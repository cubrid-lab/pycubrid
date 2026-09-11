# 문제 해결 가이드 (한국어)

> 🌐 [TROUBLESHOOTING.md](https://github.com/cubrid-lab/pycubrid/blob/main/docs/TROUBLESHOOTING.md)의 번역입니다. 영어 원문이 표준이며, 페이지 번역은 경고 수준의 동기화 규칙을 따릅니다.

pycubrid의 흔한 문제에 대한 종합 해결책 — 연결 오류, 쿼리 문제, 타입 불일치, LOB 처리, 성능 튜닝, Docker 설정.

---

## 목차

- [연결 문제](#연결-문제)
  - [포트 33000에서 ConnectionRefusedError](#포트-33000에서-connectionrefusederror)
  - [인증 실패](#인증-실패)
  - [TimeoutError 또는 socket.timeout](#timeouterror-또는-sockettimeout)
  - [연결이 예기치 않게 닫힘](#연결이-예기치-않게-닫힘)
  - [브로커 포트 리다이렉트 실패](#브로커-포트-리다이렉트-실패)
  - [Python 3.10에서 비동기 TLS 핸드셰이크 멈춤](#python-310에서-비동기-tls-핸드셰이크-멈춤)
- [쿼리 문제](#쿼리-문제)
  - [ProgrammingError: SQL 구문](#programmingerror-sql-구문)
  - [파라미터 바인딩 오류](#파라미터-바인딩-오류)
  - [파라미터 수 불일치](#파라미터-수-불일치)
  - [예약어 충돌](#예약어-충돌)
  - [빈 결과 집합](#빈-결과-집합)
- [트랜잭션 문제](#트랜잭션-문제)
  - [삽입 후 데이터가 저장되지 않음](#삽입-후-데이터가-저장되지-않음)
  - [오토커밋 동작](#오토커밋-동작)
  - [교착 상태](#교착-상태)
- [타입 매핑 문제](#타입-매핑-문제)
  - [날짜/시간 처리](#날짜시간-처리)
  - [Decimal 정밀도 손실](#decimal-정밀도-손실)
  - [NULL 처리](#null-처리)
  - [불리언 값](#불리언-값)
  - [유니코드 / NCHAR 인코딩](#유니코드--nchar-인코딩)
- [LOB (CLOB/BLOB) 문제](#lob-clobblob-문제)
  - [LOB 컬럼이 데이터가 아니라 dict를 반환](#lob-컬럼이-데이터가-아니라-dict를-반환)
  - [Lob 객체를 파라미터로 전달할 수 없음](#lob-객체를-파라미터로-전달할-수-없음)
  - [LOB 크기 제한](#lob-크기-제한)
- [커서 문제](#커서-문제)
  - [InterfaceError: Cursor is closed](#interfaceerror-cursor-is-closed)
  - [fetchone()이 예기치 않게 None 반환](#fetchone이-예기치-않게-none-반환)
  - [SELECT 후 rowcount가 -1](#rowcount가--1-after-select)
  - [executemany() 성능](#executemany-성능)
- [Prepared Statement 문제](#prepared-statement-문제)
  - [execute(sql, params) 패턴](#executesql-params-패턴)
  - [파라미터화 실행과 직접 실행 혼용](#파라미터화-실행과-직접-실행-혼용)
- [Docker 문제](#docker-문제)
  - [컨테이너는 시작되는데 연결 안 됨](#컨테이너는-시작되는데-연결-안-됨)
  - [데이터베이스를 찾을 수 없음](#데이터베이스를-찾을-수-없음)
  - [컨테이너 헬스 체크](#컨테이너-헬스-체크)
- [SQLAlchemy 연동 문제](#sqlalchemy-연동-문제)
  - [잘못된 연결 URL 형식](#잘못된-연결-url-형식)
  - [오토커밋 충돌](#오토커밋-충돌)
  - [커넥션 풀 고갈](#커넥션-풀-고갈)
- [성능 문제](#성능-문제)
  - [느린 쿼리](#느린-쿼리)
  - [높은 메모리 사용](#높은-메모리-사용)
  - [연결 오버헤드](#연결-오버헤드)
- [디버깅 기법](#디버깅-기법)

---

## 문제 해결 의사결정 트리

```mermaid
flowchart TD
    A["Start: pycubrid error observed"] --> B{Connection established?}
    B -->|No| C[Check broker status and port reachability]
    C --> D{Authentication error?}
    D -->|Yes| E[Verify user/password and database]
    D -->|No| F[Set connect_timeout and inspect network/firewall]
    B -->|Yes| G{Query or transaction failure?}
    G -->|Query| H[Validate SQL syntax and placeholder count]
    G -->|Transaction| I[Confirm autocommit and explicit commit/rollback]
    G -->|LOB| J[Validate LOB type and read/write flow]
    H --> K[Catch specific pycubrid exceptions]
    I --> K
    J --> K
    E --> K
    F --> K
    K --> L[Use debug script and logs, then escalate with reproducible case]
```

!!! tip
    첫 번째로 실패한 연산(`connect`, `execute`, `fetch`, `commit`)에서 시작해 한 번에 하나의 변수만 분리하세요.

!!! note
    대부분의 프로덕션 장애는 네 가지 버킷 중 하나로 빠르게 분류됩니다: 연결성, 인증, SQL/바인딩, 트랜잭션 상태.

---

## 연결 문제

### 포트 33000에서 ConnectionRefusedError

**증상:**

```
ConnectionRefusedError: [Errno 111] Connection refused
```

**원인과 해결:**

!!! warning
    Docker 기반 설정에서 CUBRID 기동은 비동기입니다. 컨테이너가 성공적으로 시작했다고 브로커가 이미 접속을 받는 것은 아닙니다.

1. **CUBRID 브로커가 실행 중이 아님**

   ```bash
   # 브로커 실행 확인
   cubrid broker status

   # 브로커 시작
   cubrid broker start
   ```

2. **잘못된 포트** — 브로커가 다른 포트에 설정되어 있을 수 있습니다.

   ```bash
   # 설정에서 브로커 포트 확인
   cat $CUBRID/conf/cubrid_broker.conf | grep BROKER_PORT
   ```

3. **Docker 컨테이너 미준비** — CUBRID 컨테이너는 초기화에 몇 초 걸립니다.

   ```bash
   # 컨테이너 상태 확인
   docker compose ps

   # 헬스 체크 대기
   docker compose up -d
   sleep 5  # 브로커 초기화 대기

   # 로그로 확인
   docker compose logs cubrid | tail -20
   ```

4. **방화벽이나 네트워크** — 포트 33000이 차단되어 있을 수 있습니다.

   ```bash
   # 포트 연결성 테스트
   nc -zv localhost 33000

   # macOS/Linux
   telnet localhost 33000
   ```

---

### 인증 실패

**증상:**

```
OperationalError: Authentication failed
```

**해결:**

CUBRID 기본 `dba` 사용자에게는 **비밀번호가 없습니다**. 설정했다면 일치해야 합니다:

```python
# 기본 — 비밀번호 없음
conn = pycubrid.connect(
    host="localhost",
    port=33000,
    database="testdb",
    user="dba",
)

# 비밀번호 사용
conn = pycubrid.connect(
    host="localhost",
    port=33000,
    database="testdb",
    user="dba",
    password="your_password",
)
```

**흔한 실수:**
- 사용자에게 비밀번호가 설정되어 있는데 `password=""` 전달
- 비밀번호 없는 사용자에게 비밀번호 전달 (일부 CUBRID 버전은 거부)
- 잘못된 사용자 이름 — CUBRID 사용자 이름은 대소문자를 구분하지 않지만 존재해야 함

---

### TimeoutError 또는 socket.timeout

**증상:**

```
TimeoutError: [Errno 110] Connection timed out
socket.timeout: timed out
```

**해결:**

1. **느린 네트워크를 위한 타임아웃 증가**:

   ```python
   conn = pycubrid.connect(
       host="remote-server.example.com",
       port=33000,
       database="testdb",
       user="dba",
       connect_timeout=30.0,  # 30초 타임아웃
   )
   ```

2. **서버 도달 가능성 확인:**

   ```bash
   ping remote-server.example.com
   nc -zv remote-server.example.com 33000
   ```

3. 클라이언트와 서버 사이의 **네트워크 방화벽 확인**.

---

### 연결이 예기치 않게 닫힘

**증상:**

```
OperationalError: Connection is closed
InterfaceError: Connection is closed
```

**원인:**

- **서버 측 세션 타임아웃** — CUBRID 브로커에는 `SESSION_TIMEOUT` 설정이 있습니다. 기본값은 300초(5분) 동안 활동 없음입니다.
- **브로커 재시작** — 브로커가 재시작되면 기존 연결이 모두 종료됩니다.
- **네트워크 중단** — 일시적인 네트워크 장애가 TCP 연결을 끊습니다.
- **유휴 연결 정리** — 브로커가 자원을 freeing하기 위해 유휴 연결을 닫을 수 있습니다.

**해결:** 이 오류가 발생하면 새 연결을 만드세요:

```python
import pycubrid

def get_connection():
    return pycubrid.connect(
        host="localhost",
        port=33000,
        database="testdb",
        user="dba",
    )

conn = get_connection()
try:
    cur = conn.cursor()
    cur.execute("SELECT 1")
except pycubrid.OperationalError:
    # 연결 유실 시 재연결
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT 1")
```

**장기 실행 애플리케이션**에서는 커넥션 풀링이 있는 SQLAlchemy를 사용하세요 — 재연결을 자동 처리합니다:

```python
from sqlalchemy import create_engine

engine = create_engine(
    "cubrid+pycubrid://dba@localhost:33000/testdb",
    pool_pre_ping=True,  # 사용 전 연결 테스트
    pool_recycle=1800,    # 30분마다 연결 재활용
)
```

---

### 브로커 포트 리다이렉트 실패

**증상:**

```
OperationalError: ... (during connection handshake)
```

**배경:** pycubrid가 33000 포트에 연결하면 CUBRID 브로커가 연결을 다른 CAS(CUBRID Application Server) 포트로 리다이렉트할 수 있습니다. 리다이렉트 포트에 도달할 수 없으면 연결이 실패합니다.

**해결:**

1. **CAS 프로세스 실행 확인:**

   ```bash
   cubrid broker status -b  # 브로커와 CAS 프로세스 상세 표시
   ```

2. **모든 CAS 포트 도달 가능성 확인** — 포트 포워딩을 사용하는 Docker에서는 33000 포트만 노출될 수 있습니다. 브로커가 다른 포트로 리다이렉트하면 그 포트가 포워딩되지 않은 경우 연결이 실패합니다.

   **Docker 해결** — 포트 범위를 노출하거나, 브로커가 연결을 재사용하도록(포트 0 모드) 구성하세요:

   ```yaml
   # docker-compose.yml
   services:
     cubrid:
       image: cubrid/cubrid:11.2
       ports:
         - "33000:33000"
       environment:
         CUBRID_DB: testdb
   ```

   기본 Docker 이미지는 단일 포트 접근에 맞게 올바르게 구성되어 있습니다. Docker에서 이 오류가 보이면 브로커 설정을 덮어쓰고 있지 않은지 확인하세요.

---

### Python 3.10에서 비동기 TLS 핸드셰이크 멈춤

> **pycubrid 1.x (#156 이후)부터** Python 3.10에서 `loop.start_tls()` 직전에 자동 사전 검증 TLS 프로브가 실행됩니다. 검증 실패는 이제 연결 타임아웃 내에 `OperationalError`(`ssl.SSLError`에서 체이닝)로 나타나며, 3.11+ 동작과 일치합니다. 최신 pycubrid 릴리스에서 여전히 멈춤이 보이면 재현 사례와 함께 이슈를 제출해 주세요.

**증상** (워크어라운드 전 / 구 릴리스): `await pycubrid.aio.connect(..., ssl=True)` (또는 `ssl=<SSLContext>`)가 예외를 던지는 대신 무한히 멈춥니다. 코루틴이 반환되지 않고 예외도 없습니다. 주로 테스트 타임아웃이나 Python 3.10에서 멈춘 요청 핸들러로 나타납니다.

**원인**: Python 3.10의 알려진 CPython asyncio TLS 핸드셰이크 버그 — `asyncio.AbstractEventLoop.start_tls()`가 인증서 검증 실패(만료/신뢰할 수 없는 CA, 호스트명 불일치, 브로커가 예상과 다른 인증서 제시) 시 `ssl.SSLCertVerificationError`를 던지는 대신 멈출 수 있습니다. pycubrid는 CUBRID의 STARTTLS 방식 업그레이드(평문 `CUBRS` 핸드셰이크 후 `loop.start_tls()`)를 사용하므로, 이 CPython 버그가 업그레이드 단계에서 완전 멈춤으로 나타났습니다. 버그는 Python 3.13과 3.14에서 수정되었습니다. pycubrid에서 [#156](https://github.com/cubrid-lab/pycubrid/issues/156)으로 추적.

**pycubrid의 회피 방법**: Python 3.10에서 `AsyncConnection`은 실제 `loop.start_tls()` 직전에 `loop.run_in_executor()`로 동기 `ssl.SSLContext.wrap_socket()` 사전 프로브를 같은 유효 엔드포인트에 대해 실행합니다(같은 `SSLContext`와 `server_hostname=self._host` 사용). 프로브는 검증 실패를 동기적으로 드러내고 `ssl.SSLError` → `OperationalError`를 전파합니다. 순비용: Python 3.10에서만 연결당 TCP 왕복 한 번 추가.

**주의점**:

- 프로브는 프로브와 실제 업그레이드 사이의 **인증서 로테이션에 최선을 다함** (작지만 실재하는 경쟁).
- TLS 검증 외의 프로브 실패 모드(브로커 거부, 연결 리셋)는 실제 핸드셰이크로 의도적으로 넘어가 실제 연결 경로가 정규 오류를 내게 합니다.
- Python 3.13+ 업그레이드가 여전히 권장되는 장기 해결책입니다.

**빠른 확인** (여전히 멈춤이 보일 때):

1. **TCP 연결이 아니라 TLS인지 확인**: 같은 브로커에 `ssl=False`로 시도하세요. 빨리 성공하면 멈춤은 TLS 업그레이드에 있습니다.
2. **Python 3.10 특유인지 확인**: 같은 코드를 Python 3.13이나 3.14에서 실행하세요. 거기서 빨리 예외가 나면 3.10 전용 비동기 TLS 핸드셰이크 버그입니다 — 사전 프로브가 포함된 pycubrid 릴리스를 쓰고 있어야 합니다.
3. **동기 경로 시도**: `pycubrid.connect(..., ssl=True)`는 영향받지 않는 `ssl.SSLContext.wrap_socket()`(블로킹)을 사용하며 즉시 `ssl.SSLCertVerificationError`를 던져 어느 인증서 검사가 실패했는지 정확히 알려줍니다.
4. **디버그 로깅 활성화**: `logging.getLogger("pycubrid").setLevel(logging.DEBUG)` 후 멈춤 직전 마지막 로그 라인을 찾으세요.

**워크어라운드** (하나면 충분):

- **Python을 3.13 또는 3.14로 업그레이드** (권장 — 근본 원인 해결).
- **pycubrid 업그레이드** — #156 사전 프로브가 포함된 릴리스로.
- **동기 API 사용** — 3.10에서 브로커 검증만을 위해 TLS가 필요하다면 연결 수립을 동기로 하고 동기 코드 경로로 진행.
- **커스텀 `ssl.SSLContext` 전달** — 시스템 신뢰 저장소에 의존하지 말고 올바른 CA 번들을 로드(`context.load_verify_locations(cafile=...)`)해 가장 흔한 검증 실패를 제거.

**진단**: 제어 가능한 브로커에서 재현 가능하면 패킷 트레이스를 캡처하세요(tcpdump/Wireshark, 포트 33000) — 평문 `CUBRS` 교환이 완료되고 TLS ClientHello가 나간 뒤 클라이언트 측에서 ServerHello 처리가 없는 것이 보일 것입니다. 그것이 3.10 전용 비동기 TLS 핸드셰이크 버그의 시그니처입니다.

---

## 쿼리 문제

### ProgrammingError: SQL 구문

**증상:**

```
ProgrammingError: Syntax error ...
```

**흔한 원인:**

1. **MySQL/PostgreSQL 전용 구문 사용** — CUBRID에는 자체 SQL 방언이 있습니다:

   ```python
   # 잘못됨 — CUBRID는 쉼표 구문 LIMIT을 지원하지 않음
   cur.execute("SELECT * FROM users LIMIT 0, 10")

   # 올바름 — LIMIT과 OFFSET 사용
   cur.execute("SELECT * FROM users LIMIT 10 OFFSET 0")
   ```

2. **예약어를 식별자로 사용** — 큰따옴표로 감싸세요:

   ```python
   # 잘못됨 — 'value'는 예약어
   cur.execute("SELECT value FROM config")

   # 올바름 — 식별자 인용
   cur.execute('SELECT "value" FROM config')

   # 더 나음 — 예약어 회피
   cur.execute("SELECT val FROM config")
   ```

3. **세미콜론 누락은 문제없음** — pycubrid는 끝 세미콜론을 요구하지 않습니다 (일부 맥락에서는 오류를 일으킬 수 있습니다).

---

### 파라미터 바인딩 오류

**증상:**

```
ProgrammingError: Cannot convert parameter ...
```

!!! danger
    pycubrid는 qmark 플레이스홀더(`?`)만 지원합니다. `%s`, `:name` 혼용이나 f-string SQL 구성은 미묘한 런타임 오류나 SQL 인젝션 위험을 자주 일으킵니다.

**pycubrid는 `qmark` 파라미터 방식을 사용합니다** (물음표). named 파라미터나 포맷 문자열을 쓰지 마세요:

```python
# 올바름 — qmark 방식
cur.execute("SELECT * FROM users WHERE name = ? AND age > ?", ("Alice", 25))

# 잘못됨 — named 파라미터 (미지원)
cur.execute("SELECT * FROM users WHERE name = :name", {"name": "Alice"})

# 잘못됨 — 포맷 문자열 (SQL 인젝션 위험!)
cur.execute(f"SELECT * FROM users WHERE name = '{name}'")

# 잘못됨 — %s 방식 (미지원)
cur.execute("SELECT * FROM users WHERE name = %s", ("Alice",))
```

**파라미터로 지원되는 Python 타입:**

| Python 타입 | SQL 결과 |
|---|---|
| `None` | `NULL` |
| `bool` | `1` 또는 `0` |
| `int`, `float` | 숫자 리터럴 |
| `Decimal` | 숫자 리터럴 |
| `str` | `'escaped_string'` |
| `bytes` | `X'hex_string'` |
| `datetime.date` | `DATE'YYYY-MM-DD'` |
| `datetime.time` | `TIME'HH:MM:SS'` |
| `datetime.datetime` | `DATETIME'YYYY-MM-DD HH:MM:SS.mmm'` |

---

### 파라미터 수 불일치

**증상:**

```
ProgrammingError: Incorrect number of bindings supplied
```

**해결:** `?` 플레이스홀더 수가 파라미터 수와 일치하도록 하세요:

```python
# 잘못됨 — 플레이스홀더 2개, 파라미터 1개
cur.execute("INSERT INTO users (name, age) VALUES (?, ?)", ("Alice",))

# 올바름 — 플레이스홀더 2개, 파라미터 2개
cur.execute("INSERT INTO users (name, age) VALUES (?, ?)", ("Alice", 30))
```

**단일 파라미터**는 튜플로 전달하세요 (값 그대로가 아니라):

```python
# 잘못됨 — 문자열은 iterable이라 각 글자가 파라미터가 됨
cur.execute("SELECT * FROM users WHERE name = ?", "Alice")

# 올바름 — 튜플로 감싸기
cur.execute("SELECT * FROM users WHERE name = ?", ("Alice",))
```

---

### 예약어 충돌

컬럼/테이블 이름과 자주 충돌하는 **CUBRID 예약어**:

| 예약어 | 안전한 대안 |
|---|---|
| `value` | `val`, `item_value` |
| `count` | `cnt`, `item_count` |
| `data` | `file_data`, `raw_data` |
| `level` | `user_level`, `access_level` |
| `name` | 보통 문제없지만 문제 발생 시 확인 |
| `status` | `item_status` |
| `type` | `item_type` |
| `action` | `user_action` |

**예약어를 식별자로 쓰려면** 큰따옴표로 감싸세요:

```python
cur.execute('CREATE TABLE "order" (id INT, "value" VARCHAR(100))')
cur.execute('SELECT "value" FROM "order"')
```

---

### 빈 결과 집합

**증상:** 데이터를 기대했는데 `fetchone()`이 `None`을 반환하거나 `fetchall()`이 `[]`를 반환.

**흔한 원인:**

1. **커밋되지 않은 INSERT** — 데이터가 삽입됐지만 커밋되지 않음:

   ```python
   cur.execute("INSERT INTO users (name) VALUES (?)", ("Alice",))
   conn.commit()  # 이걸 빼먹지 마세요!
   cur.execute("SELECT * FROM users WHERE name = ?", ("Alice",))
   print(cur.fetchall())
   ```

2. **다른 연결** — 각 연결은 자신의 트랜잭션 뷰를 가집니다. 한 연결에서 커밋되지 않은 데이터는 다른 연결에 보이지 않습니다.

3. **대소문자 구분** — CUBRID 문자열 비교는 기본적으로 대소문자를 구분합니다:

   ```python
   # 서로 다른 결과를 반환
   cur.execute("SELECT * FROM users WHERE name = ?", ("alice",))
   cur.execute("SELECT * FROM users WHERE name = ?", ("Alice",))
   ```

---

## 트랜잭션 문제

### 삽입 후 데이터가 저장되지 않음

**증상:** 데이터가 오류 없이 삽입됐는데, 다른 연결이나 재연결 후의 쿼리에서 데이터가 없음.

**원인:** 생성자를 직접 사용할 때 pycubrid의 `autocommit`은 기본적으로 `False`입니다. `conn.commit()`을 명시적으로 호출해야 합니다.

```python
conn = pycubrid.connect(host="localhost", port=33000, database="testdb", user="dba")

cur = conn.cursor()
cur.execute("INSERT INTO users (name) VALUES (?)", ("Alice",))
conn.commit()  # 필수! 이게 없으면 close 시 데이터 유실
conn.close()
```

**또는 성공 시 자동 커밋하는 컨텍스트 매니저 사용:**

```python
with pycubrid.connect(host="localhost", port=33000, database="testdb", user="dba") as conn:
    cur = conn.cursor()
    cur.execute("INSERT INTO users (name) VALUES (?)", ("Alice",))
    # 성공적 종료 시 자동 커밋
```

---

### 오토커밋 동작

**증상:** 예기치 않은 커밋 또는 롤백 동작.

**핵심 사실:**

| 시나리오 | autocommit | 동작 |
|---|---|---|
| 기본 생성자 | `False` (드라이버 기본) | 명시적 `commit()` 필요 |
| SQLAlchemy 경유 | `False` (방언이 설정) | SQLAlchemy가 트랜잭션 관리 |
| 컨텍스트 매니저 종료 | N/A | 성공 시 커밋, 예외 시 롤백 |

**모드 전환:**

```python
# 현재 모드 확인
print(conn.autocommit)  # False

# 수동 트랜잭션 제어를 위해 비활성화
conn.autocommit = False

cur.execute("INSERT INTO users (name) VALUES (?)", ("Alice",))
cur.execute("INSERT INTO users (name) VALUES (?)", ("Bob",))
conn.commit()  # 두 INSERT가 함께 커밋됨
```

---

### 교착 상태

**증상:**

```
OperationalError: Deadlock detected ...
```

**CUBRID는 행 수준 잠금을 사용합니다.** 교착 상태는 두 연결이 서로에게 필요한 잠금을 보유할 때 발생합니다.

**예방:**

1. 트랜잭션을 짧게 유지
2. 테이블에 일관된 순서로 접근
3. 행을 미리 잠그려면 `SELECT ... FOR UPDATE` 사용
4. 적절한 격리 수준 설정

```python
# 교착 방지를 위해 갱신 전 행 잠금
cur.execute("SELECT * FROM accounts WHERE id = ? FOR UPDATE", (1,))
cur.execute("UPDATE accounts SET balance = balance - 100 WHERE id = ?", (1,))
conn.commit()
```

---

## 타입 매핑 문제

### 날짜/시간 처리

**CUBRID 타입 → Python 타입 매핑:**

| CUBRID 타입 | Python 타입 | 예 |
|---|---|---|
| `DATE` | `datetime.date` | `date(2025, 1, 15)` |
| `TIME` | `datetime.time` | `time(14, 30, 0)` |
| `DATETIME` | `datetime.datetime` | `datetime(2025, 1, 15, 14, 30, 0)` |
| `TIMESTAMP` | `datetime.datetime` | `datetime(2025, 1, 15, 14, 30, 0)` |

**흔한 문제 — 날짜 문자열 삽입:**

```python
# 올바름 — Python datetime 객체 사용
from datetime import date, datetime

cur.execute("INSERT INTO events (event_date) VALUES (?)", (date(2025, 1, 15),))
cur.execute("INSERT INTO events (event_time) VALUES (?)", (datetime(2025, 1, 15, 14, 30, 0),))

# 역시 올바름 — CUBRID는 SQL에서 날짜 리터럴 문자열을 받음
cur.execute("INSERT INTO events (event_date) VALUES (DATE'2025-01-15')")
```

---

### Decimal 정밀도 손실

**증상:** Decimal 값이 삽입·조회 시 정밀도를 잃음.

**해결:** 정확한 숫자 값에는 `decimal.Decimal`을 사용하세요:

```python
from decimal import Decimal

# 올바름 — 정밀도 보존
cur.execute("INSERT INTO products (price) VALUES (?)", (Decimal("19.99"),))

# 위험 — float는 고유한 정밀도 문제가 있음
cur.execute("INSERT INTO products (price) VALUES (?)", (19.99,))
```

---

### NULL 처리

**NULL 삽입:**

```python
cur.execute("INSERT INTO users (name, email) VALUES (?, ?)", ("Alice", None))
```

**결과에서 NULL 확인:**

```python
cur.execute("SELECT email FROM users")
row = cur.fetchone()
if row[0] is None:
    print("Email is NULL")
```

---

### 불리언 값

**CUBRID에는 네이티브 BOOLEAN 타입이 없습니다.** `SMALLINT`(0/1)를 사용하세요:

```python
# 불리언형 값 삽입
cur.execute("INSERT INTO settings (is_active) VALUES (?)", (True,))   # 1로 저장
cur.execute("INSERT INTO settings (is_active) VALUES (?)", (False,))  # 0으로 저장

# 불리언형 값 읽기
cur.execute("SELECT is_active FROM settings")
row = cur.fetchone()
is_active = bool(row[0])  # SMALLINT를 다시 bool로 변환
```

---

### 유니코드 / NCHAR 인코딩

**CUBRID는 유니코드를 지원합니다** — `NCHAR`와 `NCHAR VARYING` 타입을 통해. pycubrid는 UTF-8 인코딩을 투명하게 처리합니다:

```python
# 유니코드 문자열이 그대로 동작
cur.execute("INSERT INTO users (name) VALUES (?)", ("김영선",))
cur.execute("INSERT INTO users (name) VALUES (?)", ("日本語テスト",))

cur.execute("SELECT name FROM users")
for row in cur:
    print(row[0])  # 올바르게 출력: 김영선, 日本語テスト
```

---

## LOB (CLOB/BLOB) 문제

### LOB 컬럼이 데이터가 아니라 dict를 반환

**증상:** CLOB/BLOB 컬럼을 fetch하면 실제 데이터 대신 딕셔너리가 반환됨.

```python
cur.execute("SELECT clob_col FROM my_table")
row = cur.fetchone()
print(row[0])
# {'lob_type': 24, 'lob_length': 1234, 'file_locator': '...', 'packed_lob_handle': b'...'}
```

**이것은 예상된 동작입니다.** CUBRID의 CAS 프로토콜은 LOB 내용이 아니라 LOB 메타데이터를 반환합니다. LOB 내용을 읽으려면 LOB 핸들을 별도로 사용해야 합니다.

**회피 — 일반 문자열/바이트로 삽입하고 조회:**

```python
# CLOB 컬럼에 문자열 직접 삽입
cur.execute("INSERT INTO docs (content) VALUES (?)", ("Large text content here...",))
conn.commit()

# fetch는 CLOB/BLOB 컬럼의 메타데이터 dict를 반환; packed 핸들로 내용 읽기
```

---

### Lob 객체를 파라미터로 전달할 수 없음

**증상:**

```
ProgrammingError: Cannot convert parameter of type Lob
```

**`Lob` 객체는 쿼리 파라미터로 사용할 수 없습니다.** 문자열/바이트를 직접 삽입하세요:

```python
# 잘못됨 — Lob 객체는 파라미터로 전달 불가
lob = conn.create_lob(24)  # CLOB
lob.write(b"data")
cur.execute("INSERT INTO docs (content) VALUES (?)", (lob,))  # 오류!

# 올바름 — 문자열 직접 전달
cur.execute("INSERT INTO docs (content) VALUES (?)", ("Large text content",))

# 올바름 — BLOB에는 바이트 전달
cur.execute("INSERT INTO docs (binary_data) VALUES (?)", (b"\x89PNG\r\n...",))
```

---

### LOB 크기 제한

CUBRID LOB 크기 제한은 서버 설정에 따라 다릅니다. 기본 최댓값은 대부분의 사용 사례에 충분하지만, 극도로 큰 객체는 서버 측 설정 조정이 필요할 수 있습니다.

몇 메가바이트보다 큰 파일은 다음을 고려하세요:

1. 파일 내용 대신 데이터베이스에 파일 경로 저장
2. 큰 내용을 청크로 분할
3. CUBRID의 파일 저장소 설정 옵션 사용

---

## 커서 문제

### InterfaceError: Cursor is closed

**증상:**

```
InterfaceError: Cursor is closed
```

**원인:**

1. **명시적으로 닫힌 커서** — `cur.close()` 호출 후 다시 사용
2. **연결 종료** — 연결을 닫으면 모든 커서가 닫힘
3. **컨텍스트 매니저 종료** — `with conn.cursor() as cur:`는 종료 시 커서를 닫음

**해결:** 새 커서를 만드세요:

```python
cur = conn.cursor()
cur.execute("SELECT 1")
```

---

### fetchone()이 예기치 않게 None 반환

**가능한 원인:**

1. **결과 집합에 행이 없음** — 쿼리가 0행을 반환
2. **이미 소비됨** — 이전 `fetchone()`이나 `fetchall()`이 모든 행을 소비
3. **비-SELECT 문** — `INSERT`, `UPDATE`, `DELETE`는 행을 만들지 않음

```python
cur.execute("SELECT * FROM users")
row1 = cur.fetchone()  # 첫 행 또는 None
row2 = cur.fetchone()  # 둘째 행 또는 None
# ... None이 나올 때까지 계속 (행 없음)
```

**결과를 다시 읽으려면** 쿼리를 다시 실행하세요:

```python
cur.execute("SELECT * FROM users")
all_rows = cur.fetchall()  # 한 번에 전체
# cur.fetchone()은 이제 None 반환 — 결과 이미 소비
```

---

### SELECT 후 rowcount가 -1

**이것은 올바른 PEP 249 동작입니다.** `rowcount`는 INSERT, UPDATE, DELETE 문에만 의미가 있습니다:

```python
cur.execute("SELECT * FROM users")
print(cur.rowcount)  # -1 (SELECT에 대해 정의되지 않음)

cur.execute("UPDATE users SET name = 'Bob' WHERE id = 1")
print(cur.rowcount)  # 1 (한 행 영향)

cur.execute("DELETE FROM users WHERE id > 100")
print(cur.rowcount)  # 삭제된 행 수
```

---

### executemany() 성능

**대량 삽입 등 비-SELECT DML의 경우**, `executemany()`는 각 파라미터 세트를 별도 왕복으로 실행하지 **않습니다**. 각 바인딩 문을 렌더링하고 전체 배치를 하나의 `BatchExecutePacket`으로 보냅니다. `SELECT` 문만 결과 집합 의미론을 보존하기 위해 파라미터별 루프로 폴백합니다. 이미 완성된 서로 다른 SQL 문자열이 있고 하나의 배치 요청으로 보내려면 `executemany_batch()`를 사용하세요:

```python
# 표준 executemany — 비-SELECT DML은 하나의 요청으로 배치
data = [("Alice", 30), ("Bob", 25), ("Charlie", 35)]
cur.executemany("INSERT INTO users (name, age) VALUES (?, ?)", data)

# executemany_batch — 여러 문을 하나의 요청으로
sql_list = [
    "INSERT INTO users (name, age) VALUES ('Alice', 30)",
    "INSERT INTO users (name, age) VALUES ('Bob', 25)",
    "INSERT INTO users (name, age) VALUES ('Charlie', 35)",
]
cur.executemany_batch(sql_list)
```

**성능 비교:**

| 방법 | 왕복 | 최적 용도 |
|---|---|---|
| 루프에서 `execute()` | N | 소수의 행 |
| INSERT/UPDATE/DELETE의 `executemany()` | 1 | 파라미터화 대량 DML |
| SELECT의 `executemany()` | N | 별도 파라미터 세트로 반복 SELECT |
| `executemany_batch()` | 1 | 서로 다른 SQL 문 다수 |

---

## Prepared Statement 문제

### execute(sql, params) 패턴

**올바른 패턴:**

```python
sql = "SELECT * FROM users WHERE department = ?"

# SQL + 파라미터로 실행
cur.execute(sql, ("Engineering",))
engineers = cur.fetchall()

cur.execute(sql, ("Marketing",))
marketers = cur.fetchall()
```

**핵심:**

- 항상 SQL 문자열을 `execute()`의 첫 인자로 전달
- 파라미터 값을 둘째 인자로 전달
- 각 호출은 CAS `PREPARE_AND_EXECUTE`를 사용 — 별도 prepare 단계가 필요 없음

---

### 파라미터화 실행과 직접 실행 혼용

**하나의 커서에서 파라미터화 실행과 직접 SQL 실행을 안전하게 혼용할 수 있습니다:**

```python
cur.execute("SELECT * FROM users WHERE id = ?", (1,))

cur.execute("SELECT * FROM departments")

cur.execute("SELECT * FROM users WHERE id = ?", (2,))
```

**모범 사례:** 각 호출 지점에서 SQL을 명시적으로 유지하세요:

```python
sql = "SELECT * FROM users WHERE id = ?"

cur.execute(sql, (1,))
cur.execute(sql, (2,))
cur.execute("SELECT COUNT(*) FROM users")
```

---

## Docker 문제

### 컨테이너는 시작되는데 연결 안 됨

**확인 1: 컨테이너가 실제로 실행 중인지:**

```bash
docker compose ps
# "running" 상태가 보여야 함
```

**확인 2: 초기화 대기** — CUBRID는 시작에 몇 초 걸립니다:

```bash
docker compose up -d
sleep 10  # 전체 초기화 대기

# 연결 테스트
python3 -c "
import pycubrid
conn = pycubrid.connect(host='localhost', port=33000, database='testdb', user='dba')
print('Connected!')
print('Version:', conn.get_server_version())
conn.close()
"
```

**확인 3: 포트 매핑이 올바른지:**

```bash
docker compose ps
# 33000->33000/tcp가 표시되는지 확인
```

---

### 데이터베이스를 찾을 수 없음

**증상:**

```
OperationalError: ... database 'mydb' not found
```

**Docker 이미지는 `CUBRID_DB`에 지정된 데이터베이스만 생성합니다:**

```yaml
# docker-compose.yml
services:
  cubrid:
    image: cubrid/cubrid:11.2
    environment:
      CUBRID_DB: testdb  # 이 데이터베이스만 생성됨
```

**해결:** 둘 중 하나:

1. `CUBRID_DB`를 연결의 데이터베이스 이름과 일치하도록 설정
2. 컨테이너 안에서 수동으로 데이터베이스 생성:

   ```bash
   docker compose exec cubrid cubrid createdb mydb
   docker compose exec cubrid cubrid server start mydb
   ```

---

### 컨테이너 헬스 체크

**docker-compose.yml에 헬스 체크를 추가하세요:**

```yaml
services:
  cubrid:
    image: cubrid/cubrid:11.2
    container_name: cubrid-test
    ports:
      - "33000:33000"
    environment:
      CUBRID_DB: testdb
    healthcheck:
      test: ["CMD", "cubrid", "broker", "status"]
      interval: 10s
      timeout: 5s
      retries: 5
      start_period: 15s
```

**테스트에서 헬스 체크 대기:**

```bash
docker compose up -d --wait
# 헬스 체크 통과 후에만 진행
```

---

## SQLAlchemy 연동 문제

### 잘못된 연결 URL 형식

**pycubrid의 올바른 URL 형식:**

```python
# pycubrid 드라이버
engine = create_engine("cubrid+pycubrid://dba@localhost:33000/testdb")

# 비밀번호 포함
engine = create_engine("cubrid+pycubrid://dba:password@localhost:33000/testdb")
```

**흔한 실수:**

```python
# 잘못됨 — 드라이버 지정 누락 (C 확장 드라이버가 기본값)
engine = create_engine("cubrid://dba@localhost:33000/testdb")

# 잘못됨 — 포트 형식 오류
engine = create_engine("cubrid+pycubrid://dba@localhost/testdb?port=33000")

# 잘못됨 — 스킴 오류
engine = create_engine("pycubrid://dba@localhost:33000/testdb")
```

---

### 오토커밋 충돌

**증상:** `session.commit()`을 호출하지 않았는데 데이터가 커밋됨.

**원인:** CUBRID 서버 기본값은 `autocommit=True`입니다. SQLAlchemy의 pycubrid 방언은 새 연결마다 `autocommit=False`로 설정하지만, 방언이 잘못 구성되면 서버 기본값이 적용됩니다.

**해결:** 연결 URL에 `cubrid+pycubrid://`를 사용해 오토커밋을 올바르게 관리하는 방언을 로드하세요.

---

### 커넥션 풀 고갈

**증상:**

```
TimeoutError: QueuePool limit of size 5 overflow 10 reached
```

**해결:** 커넥션 풀 튜닝:

```python
from sqlalchemy import create_engine

engine = create_engine(
    "cubrid+pycubrid://dba@localhost:33000/testdb",
    pool_size=10,        # 최대 지속 연결
    max_overflow=20,     # pool_size를 넘는 추가 연결
    pool_timeout=30,     # 가용 연결 대기 초
    pool_pre_ping=True,  # 사용 전 연결 테스트
    pool_recycle=1800,   # 30분마다 연결 재활용
)
```

**연결이 풀로 반환되도록 하세요:**

```python
# 올바름 — 컨텍스트 매니저가 연결 반환
with engine.connect() as conn:
    result = conn.execute(text("SELECT 1"))

# 잘못됨 — 연결이 반환되지 않음
conn = engine.connect()
result = conn.execute(text("SELECT 1"))
# conn.close()가 호출되지 않음!
```

---

## 성능 문제

### 느린 쿼리

**진단 단계:**

1. **애플리케이션 코드에서 쿼리 실행 시간 확인**:

   ```python
   import time

   start = time.perf_counter()
   cur.execute("SELECT * FROM large_table WHERE status = ?", ("active",))
   rows = cur.fetchall()
   elapsed = time.perf_counter() - start
   print(f"Query took {elapsed:.3f}s, returned {len(rows)} rows")
   ```

2. **자주 조회되는 컬럼에 인덱스 추가**:

   ```sql
   CREATE INDEX idx_status ON large_table (status);
   ```

3. **`LIMIT`으로 결과 집합 크기 제한**:

   ```python
   cur.execute("SELECT * FROM large_table LIMIT 100")
   ```

---

### 높은 메모리 사용

**증상:** 큰 결과 집합에서 Python 프로세스가 과도한 메모리 소비.

**원인:** `fetchall()`이 모든 행을 한 번에 메모리에 적재.

**해결:** 큰 결과 집합에는 `fetchone()`이나 `fetchmany()`를 사용하세요:

```python
# 잘못됨 — 100만 행 전체를 메모리에 적재
cur.execute("SELECT * FROM large_table")
rows = cur.fetchall()  # 메모리에 100만 행!

# 올바름 — 한 번에 한 행 처리
cur.execute("SELECT * FROM large_table")
for row in cur:  # 이터레이터 프로토콜 — 배치로 fetch
    process(row)

# 역시 올바름 — 청크 단위 fetch
cur.execute("SELECT * FROM large_table")
while True:
    batch = cur.fetchmany(1000)
    if not batch:
        break
    for row in batch:
        process(row)
```

---

### 연결 오버헤드

**증상:** 연결 열기가 느림.

**원인:** `pycubrid.connect()`마다 TCP 핸드셰이크 + CAS 브로커 핸드셰이크 + 데이터베이스 열기(왕복 3회 이상) 수행.

**애플리케이션용 해결:** SQLAlchemy 커넥션 풀링 사용:

```python
from sqlalchemy import create_engine

# 커넥션 풀이 기존 연결 재사용
engine = create_engine(
    "cubrid+pycubrid://dba@localhost:33000/testdb",
    pool_size=5,
    pool_pre_ping=True,
)
```

**스크립트용 해결:** 반복해서 열고 닫는 대신 단일 연결 재사용.

---

## 디버깅 기법

### 상세 로깅 활성화

pycubrid는 `pycubrid.connection`, `pycubrid.cursor`, `pycubrid.lob`, 비동기 모듈에 **옵트인 DEBUG 로깅**을 포함합니다. Python 로깅 설정으로 활성화하세요:

```python
import logging
import pycubrid

logging.basicConfig(level=logging.DEBUG)

conn = pycubrid.connect(host="localhost", port=33000, database="testdb", user="dba")

# 연결 상태 확인
print(f"Server version: {conn.get_server_version()}")
print(f"Autocommit: {conn.autocommit}")

# 쿼리 후 커서 상태 확인
cur = conn.cursor()
cur.execute("SELECT * FROM users")
print(f"Description: {cur.description}")
print(f"Row count: {cur.rowcount}")
```

드라이버의 디버그 로그는 바인딩된 파라미터 값을 출력하지 않도록 의도되어 있습니다.

### 서버 버전 확인

```python
conn = pycubrid.connect(host="localhost", port=33000, database="testdb", user="dba")
version = conn.get_server_version()
print(f"CUBRID version: {version}")  # 예: "11.2.0.0378"
conn.close()
```

### 연결 테스트 스크립트

빠른 검증용으로 `test_connection.py`로 저장하세요:

```python
#!/usr/bin/env python3
"""Quick pycubrid connection test."""
import sys
import pycubrid

try:
    conn = pycubrid.connect(
        host="localhost",
        port=33000,
        database="testdb",
        user="dba",
    )
    print(f"✅ Connected to CUBRID {conn.get_server_version()}")

    cur = conn.cursor()
    cur.execute("SELECT 1 + 1")
    result = cur.fetchone()
    print(f"✅ Query result: {result[0]}")

    cur.execute("SELECT COUNT(*) FROM db_class")
    count = cur.fetchone()[0]
    print(f"✅ System tables: {count}")

    cur.close()
    conn.close()
    print("✅ All checks passed")

except pycubrid.OperationalError as e:
    print(f"❌ Connection failed: {e}")
    sys.exit(1)
except pycubrid.ProgrammingError as e:
    print(f"❌ Query failed: {e}")
    sys.exit(1)
```

### SQLAlchemy 디버그 로깅

```python
import logging

logging.basicConfig()
logging.getLogger("sqlalchemy.engine").setLevel(logging.DEBUG)

engine = create_engine("cubrid+pycubrid://dba@localhost:33000/testdb", echo=True)
```

이것은 모든 SQL 문, 파라미터, 실행 시간을 보여줍니다.

---

*참고: [연결 가이드](CONNECTION.md) · [API 참조](API_REFERENCE.md) · [예제](EXAMPLES.md) · [개발 가이드](DEVELOPMENT.md)*
