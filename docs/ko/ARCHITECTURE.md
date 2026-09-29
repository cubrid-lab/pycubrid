# 아키텍처 (한국어)

> 🌐 [ARCHITECTURE.md](https://github.com/cubrid-lab/pycubrid/blob/main/docs/ARCHITECTURE.md)의 번역입니다. 영어 원문이 표준이며, 페이지 번역은 경고 수준의 동기화 규칙을 따릅니다.

## 설계 목표

- **순수 Python**: 시스템 의존성 0 — C 확장이나 CCI 라이브러리가 필요 없고 Python이 돌아가는 어디서나 실행됩니다.
- **완전한 PEP 249 준수**: 최대 호환성을 위해 Python Database API Specification v2.0을 구현합니다.
- **CAS 바이너리 프로토콜 v8**: pycubrid가 사용하는 현재 CUBRID CAS 프로토콜 버전을 대상으로 합니다.
- **단일 연결 동기 모델**: 표준 애플리케이션 통합에 적합한 안정적인 블로킹 I/O 모델.
- **PEP 561 타입 지정**: 현대적 IDE 지원과 정적 분석을 위한 완전한 타입 힌트.

## 전체 흐름

### 1단계: 연결 핸드셰이크

```mermaid
sequenceDiagram
    participant App
    participant pycubrid as pycubrid.connect()
    participant Broker as Broker (port 33000)
    participant CAS as CAS Process
    participant DB as CUBRID Database

    App->>pycubrid: connect(host, port, database, user, password)
    rect rgb(230, 245, 255)
      note over pycubrid, CAS: Phase 1 — Broker Handshake
      pycubrid->>Broker: TCP connect to port 33000
      alt ssl truthy
        pycubrid->>Broker: ClientInfoExchange ("CUBRS" + CLIENT_JDBC=3 + v8)
      else plaintext
        pycubrid->>Broker: ClientInfoExchange ("CUBRK" + CLIENT_JDBC=3 + v8)
      end
      Broker-->>pycubrid: status int32 (0 ok / >0 redirect port / <0 fail-fast)
      opt status > 0 (redirect)
        pycubrid->>CAS: TCP reconnect to redirected port (no rehandshake)
      end
      opt ssl truthy
        pycubrid->>CAS: TLS upgrade (start_tls / wrap_socket)
      end
    end
    rect rgb(230, 255, 230)
      note over pycubrid, DB: Phase 2 — Database Session
      pycubrid->>CAS: OpenDatabase (db, user, password — 628B raw)
      CAS->>DB: Authenticate + open session
      DB-->>CAS: Session established
      CAS-->>pycubrid: CAS Info (4B) + response_code (4B) + Broker Info (8B) + Session ID (4B)
    end
    pycubrid-->>App: Connection object
```

### 2단계: 쿼리 수명 주기

```mermaid
sequenceDiagram
    participant App
    participant Cursor
    participant Connection
    participant CAS

    App->>Cursor: execute("SELECT ...", params)
    rect rgb(255, 245, 230)
      note over Cursor, CAS: SQL Execution
      Cursor->>Connection: _send_and_receive(PrepareAndExecutePacket)
      Connection->>CAS: [4B length][4B cas_info][FC=41 + SQL + params]
      CAS-->>Connection: [4B length][4B cas_info][result metadata + inline rows]
      Connection-->>Cursor: Parsed response (columns, rows)
    end
    Cursor-->>App: None (results buffered)

    App->>Cursor: fetchall()
    alt All rows in initial fetch
      Cursor-->>App: Buffered rows
    else More rows on server
      Cursor->>Connection: _send_and_receive(FetchPacket)
      Connection->>CAS: [4B length][4B cas_info][FC=8 + handle + offset]
      CAS-->>Connection: [4B length][4B cas_info][row data]
      Connection-->>Cursor: Additional rows
      Cursor-->>App: All rows
    end

    App->>Cursor: close()
    Cursor->>Connection: _send_and_receive(CloseQueryPacket)
    Connection->>CAS: [FC=6 + query_handle]
```

## CAS 재연결

`CAS_INFO[0]`은 연결 상태가 아니라 트랜잭션 상태입니다(`0` = OUT_TRAN,
`1` = IN_TRAN). 정상적인 commit, rollback 또는 autocommit 응답의 OUT_TRAN은
같은 물리 세션을 유지합니다. 전송 실패 후 불확실한 임의의 SQL 요청을 새 연결에서
자동 재실행하지 않습니다.

그래도 CAS는 OUT_TRAN 응답 직후 소켓을 닫을 수 있습니다. CAS 메모리 재시작
(`APPL_SERVER_MAX_SIZE`), `cubrid broker reset`, CAS 프로세스보다 많은
클라이언트가 대기할 때의 CHANGE CLIENT가 그 예입니다. 그래서 드라이버는 JDBC
`UClientSideConnection.checkReconnect`처럼 직전 응답이 OUT_TRAN이면 다음 요청 전에
`CHECK_CAS`를 보냅니다(#485). CAS가 살아 있으면 세션을 유지합니다. 검사가
실패할 때만 요청당 한 번 세션을 교체하며, 이스케이프 모드 검사와 autocommit을
복원하고 SQL은 재실행하지 않습니다. 잃어버린 세션의 핸들에 대한 `CLOSE_REQ`는
건너뛰고, 그 결과에 대한 FETCH는 `OperationalError`를 발생시키며, 재접속 실패도
`OperationalError`를 발생시킵니다. commit과 rollback은 닫히지 않은 커서가 가진
쿼리 핸들에 먼저 `CLOSE_REQ`를 보내므로, 트랜잭션보다 오래 유지되는 CAS 세션에
핸들이 쌓이지 않습니다.

```mermaid
sequenceDiagram
    participant App
    participant Connection
    participant Broker
    participant CAS

    App->>Connection: commit()/rollback()
    Connection->>CAS: 열린 커서 핸들마다 CLOSE_REQ, 그다음 END_TRAN
    CAS-->>Connection: CAS_INFO[0]=0 (OUT_TRAN)
    App->>Connection: 다음 요청
    Connection->>CAS: CHECK_CAS (FC=32)
    alt CAS 정상
      CAS-->>Connection: 정상 응답
      note over Connection,CAS: 같은 소켓과 세션 유지
    else CAS가 소켓을 닫음 (재시작, reset, CHANGE CLIENT)
      Connection->>Broker: 한 번 재접속, 이스케이프 모드 검사, autocommit 복원
      note over Connection: 원래 요청은 새 세션에서 한 번 전송
    end
    App->>Connection: ping(reconnect=True)
    opt 기존 소켓이 연결됨
      Connection->>CAS: CHECK_CAS (FC=32)
      CAS-->>Connection: 정상 또는 음수 응답 (CAS–DB 링크 장애)
    end
    opt 연결 끊김, 음수 CHECK_CAS 또는 전송/프로토콜 실패
      Connection->>Connection: 이전 전송과 쿼리 핸들 폐기
      Connection->>Broker: ClientInfoExchange ("CUBRK"/"CUBRS")
      Broker-->>Connection: status int32 (0 / >0 redirect / <0 fail)
      opt status > 0 (redirect)
        Connection->>CAS: TCP reconnect to redirected port (no rehandshake)
      end
      opt ssl truthy
        Connection->>CAS: TLS upgrade (start_tls / wrap_socket)
      end
      Connection->>CAS: OpenDatabase
      CAS-->>Connection: 새 세션
      note over Connection: 명시적으로 설정한 autocommit만 복원
    end
```

`ping(reconnect=False)`는 열린 소켓을 검사하지만 음수 `CHECK_CAS` 응답에도
재접속하지 않습니다.
`ping(reconnect=True)`를 통한 복구는 명시적이며 한 번만 시도합니다. 중단된
SQL을 재시도해도 안전한지는 호출자가 판단해야 합니다.

## 모듈 경계

```mermaid
flowchart TD
    init["__init__.py<br/>Public API & PEP 249 globals"]
    conn["connection.py<br/>TCP socket, transactions, LOB"]
    cursor["cursor.py<br/>execute, fetch, callproc"]
    protocol["protocol.py<br/>18 CAS packet classes"]
    packet["packet.py<br/>PacketReader / PacketWriter"]
    constants["constants.py<br/>CASFunctionCode, CUBRIDDataType"]
    types["types.py<br/>DBAPIType, STRING, NUMBER, ..."]
    exceptions["exceptions.py<br/>PEP 249 exception hierarchy"]
    lob["lob.py<br/>Lob class (BLOB/CLOB)"]

    init --> conn
    init --> types
    init --> exceptions
    init --> lob
    conn --> protocol
    conn --> packet
    conn --> cursor
    conn --> exceptions
    cursor --> protocol
    cursor --> packet
    cursor --> exceptions
    protocol --> packet
    protocol --> constants
    protocol --> exceptions
    lob --> conn
```

- **`__init__.py` — 공개 API와 PEP 249 전역**: 패키지의 진입점. `connect` 함수, 예외 계층, DB-API 타입 객체를 노출합니다.
- **`connection.py` — TCP 소켓과 트랜잭션 관리**: CAS에 대한 물리적 TCP 연결을 관리하고, 트랜잭션(커밋/롤백)을 처리하며, LOB 연산의 소유자 역할을 합니다.
- **`cursor.py` — SQL 실행과 결과 조회**: `Cursor` 객체를 구현해 SQL 준비·실행·다양한 fetch 연산을 처리하고 결과 상태를 유지합니다.
- **`protocol.py` — CAS 패킷 클래스**: CUBRID CAS 함수 코드에 대응하는 18개 전문 패킷 클래스를 정의하고, 특정 요청·응답의 직렬화/역직렬화를 담당합니다.
- **`packet.py` — PacketReader / PacketWriter**: 와이어 형식 읽기·쓰기의 저수준 유틸리티를 제공하고 바이트 순서와 원시 타입 직렬화를 처리합니다. 둘 다 연결 `charset` 코덱을 사용합니다(#86): 연결은 `write()` 전에 모든 패킷에 코덱을 설정하므로 SQL 텍스트, 자격 증명, 문자 값, 컬럼 이름, 오류 텍스트, LOB 로케이터는 데이터베이스 문자셋을 쓰고, 가져온 JSON, NUMERIC, 타임존 이름은 UTF-8을 유지합니다. [문자 인코딩](CONNECTION.md#문자-인코딩)을 참고하세요.
- **`constants.py` — CAS 상수**: CAS 함수 코드, CUBRID 데이터 타입, 기타 프로토콜 수준 상수의 열거형을 담습니다.
- **`types.py` — DB-API 타입**: PEP 249가 요구하는 타입 객체를 정의하고 CUBRID 타입과 Python 타입 간 매핑을 관리합니다.
- **`exceptions.py` — PEP 249 예외**: DB-API 2.0 사양이 요구하는 표준 예외 계층을 구현합니다.
- **`lob.py` — LOB 관리**: 대형 객체 데이터(BLOB/CLOB)를 다루는 `Lob` 클래스를 구현해 청크 단위 읽기·쓰기 인터페이스를 제공합니다.

## 패킷 형식

```text
┌─────────────────┬──────────────┬─────────────────────────┐
│  Data Length     │  CAS Info    │  Payload                │
│  (4 bytes)       │  (4 bytes)   │  (variable length)      │
│  big-endian int  │  session     │  [FC byte][arguments…]  │
└─────────────────┴──────────────┴─────────────────────────┘
```

핸드셰이크 패킷(`ClientInfoExchange`)은 이 프레이밍을 사용하지 **않습니다**. 초기 브로커 협상을 위해 특화된 10바이트 고정 헤더를 사용합니다.

## 타입 디스패치

```mermaid
flowchart TD
    wire["Wire Data<br/>[4B size][raw bytes]"]
    dispatch{"_TYPE_READERS<br/>dict dispatch<br/>O(1) lookup"}

    wire --> dispatch

    dispatch -->|"1-4, 25"| str["str<br/>(connection charset,<br/>default UTF-8)"]
    dispatch -->|"5, 6"| bytes["bytes<br/>(raw binary)"]
    dispatch -->|"7"| decimal["Decimal<br/>(string-parsed)"]
    dispatch -->|"8"| int32["int<br/>(4B signed)"]
    dispatch -->|"9"| int16["int<br/>(2B signed)"]
    dispatch -->|"21"| int64["int<br/>(8B signed)"]
    dispatch -->|"11"| float32["float<br/>(IEEE 754 single)"]
    dispatch -->|"12, 10"| float64["float<br/>(IEEE 754 double)"]
    dispatch -->|"13"| date["datetime.date"]
    dispatch -->|"14"| time["datetime.time"]
    dispatch -->|"15, 22, 29-32"| datetime["datetime.datetime"]
    dispatch -->|"19"| oid["str (OID)"]
    dispatch -->|"23, 24"| lob["dict (LOB handle)"]
    dispatch -->|"16-18"| collection["bytes (opaque)"]
```

## 핵심 설계 결정

- **C 확장 대신 순수 Python**: 시스템 의존성 0은 Python이 있는 어디서나 드라이버가 돌아가게 하여 배포를 단순화하고 교차 컴파일 문제를 피합니다.
- **CAS 프로토콜 v8**: 드라이버는 JSON 인식 파싱 경로와 현대적 기능 지원을 포함한 현재 브로커 프로토콜을 대상으로 하며, 레거시 프로토콜 개정을 위한 호환 코드를 담지 않습니다.
- **드라이버 측 바인딩을 가진 `qmark` 파라미터 방식**: 파라미터는 `?` 플레이스홀더를 사용합니다. 드라이버가 최종 SQL을 CAS 브로커로 보내기 전에 값을 로컬에서 이스케이프·보간합니다(문자열·바이트·날짜·decimal·None→NULL의 타입 인식 이스케이프). 이것은 서버 측 prepared statement 바인딩이 아니며 — 브로커는 완전한 SQL 문자열을 받습니다. 이 설계는 PREPARE를 위한 프로토콜 왕복을 없애고 구현을 단순화하면서, 엄격한 타입 디스패치 이스케이프로 주입 안전성을 유지합니다.
- **dict 기반 타입 디스패치**: `_TYPE_READERS` 딕셔너리 활용은 O(1) 조회 성능을 제공해 반복 조건 검사 대비 고속 결과 파싱을 보장합니다.
- **불투명 컬렉션 타입**: SET, MULTISET, SEQUENCE 타입을 raw `bytes`로 반환하는 것은 표준 애플리케이션에서 드물게 쓰이는 기능을 위해 재귀 파싱의 성능 오버헤드와 복잡성을 피합니다.
- **JDBC 클라이언트로 식별**: 핸드셰이크 중 `CLIENT_JDBC=3`을 보내 CAS가 pycubrid를 공식 JDBC 드라이버와 같은 안정성과 기능 집합으로 다루게 합니다.

## 공개 API 경계

```python
# 모듈 수준 속성 (PEP 249)
apilevel = "2.0"
threadsafety = 1
paramstyle = "qmark"

# 생성자
connect(host, port, database, user, password, **kwargs) -> Connection

# 예외
Warning, Error, InterfaceError, DatabaseError, DataError,
OperationalError, IntegrityError, InternalError,
ProgrammingError, NotSupportedError

# 타입 객체
STRING, BINARY, NUMBER, DATETIME, ROWID

# 생성자들
Date, Time, Timestamp, DateFromTicks, TimeFromTicks, TimestampFromTicks, Binary

# 확장
Lob, get_error_description
```

## 이 패키지가 소유하는 것 / 소유하지 않는 것

- **소유**: CAS 와이어 프로토콜 구현, PEP 249 인터페이스, 타입 변환, 연결 수명 주기, LOB 지원.
- **소유하지 않음**: 커넥션 풀링(SQLAlchemy 사용), ORM(sqlalchemy-cubrid 사용), 스키마 마이그레이션(Alembic 사용), 쿼리 빌딩(SQLAlchemy Core 사용).

## 관련 문서

- [프로토콜 참조](PROTOCOL.md)
- [연결 가이드](CONNECTION.md)
- [타입 시스템](TYPES.md)
- [API 참조](API_REFERENCE.md)
- [지원 매트릭스](SUPPORT_MATRIX.md)

## CUBRID 서버 라이선스 관계

pycubrid는 CAS 와이어 프로토콜을 사용하는 독립적인 순수 Python 클라이언트 구현입니다. CUBRID 서버 소스를 포함하지 않고 서버 라이브러리를 링크하지 않으므로, 어떤 라이선스 체제에서도 서버의 파생물이 아닙니다.

CUBRID 서버 엔진은 Apache License 2.0으로, 공식 API/커넥터는 BSD로 배포됩니다(업스트림 `COPYING`, http://www.cubrid.org/cubrid). 이전의 GPL v2+ 체제는 더 이상 적용되지 않습니다 — 그 체제에서조차 TCP over 와이어 프로토콜 클라이언트는 영향을 받지 않았습니다. 어떤 카피레프트 의무도 이 코드베이스에 미치지 않습니다.

`cubrid/cubrid` Docker 이미지(10.2–11.4)는 라이브 서버에 대한 통합 테스트를 실행하기 위해서만 CI에서 사용되며, pycubrid와 함께 배포되지 않습니다.
