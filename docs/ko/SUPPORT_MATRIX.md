# 지원 매트릭스 (한국어)

> 🌐 [SUPPORT_MATRIX.md](https://github.com/cubrid-lab/pycubrid/blob/main/docs/SUPPORT_MATRIX.md)의 번역입니다. 영어 원문이 표준이며, 페이지 번역은 경고 수준의 동기화 규칙을 따릅니다.

pycubrid 릴리스의 호환성과 기능 지원.

> **참조:** 현재 버전 `1.3.0`. 릴리스별 상세는 [`CHANGELOG.md`](https://github.com/cubrid-lab/pycubrid/blob/main/CHANGELOG.md)를 참고하세요.

---

## 버전 호환성

### Python

| Python 버전 | 상태 |
|---|---|
| 3.10 | ✅ 지원 |
| 3.11 | ✅ 지원 |
| 3.12 | ✅ 지원 |
| 3.13 | ✅ 지원 |
| 3.14 | ✅ 지원 |
| < 3.10 | ❌ 미지원 |

### CUBRID 서버

| CUBRID 버전 | 상태 | 비고 |
|---|---|---|
| 11.4 | ✅ 지원 | 최신 안정 버전 |
| 11.2 | ✅ 지원 | |
| 11.0 | ✅ 지원 | |
| 10.2 | ✅ 지원 | 최소 테스트 버전 |
| < 10.2 | ❌ 미지원 | 현재 드라이버는 CAS 프로토콜 v8 대상 |

### CI 매트릭스

| 차원 | PR / push | 나이틀리 + 태그 + dispatch |
|---|---|---|
| 오프라인 테스트 | Python 3.10, 3.11, 3.12, 3.13, 3.14 | 동일 |
| 통합 테스트 | Python {3.10, 3.14} × CUBRID {10.2, 11.0, 11.2, 11.4} = 8잡 | Python {3.10, 3.11, 3.12, 3.13, 3.14} × CUBRID {10.2, 11.0, 11.2, 11.4} = 20잡 |

5 × 4 전체 통합 매트릭스는 `.github/workflows/integration-full.yml`이 나이틀리 일정, 태그 릴리스, `workflow_dispatch` 요청 시 실행합니다.

---

## 기능 지원

### PEP 249 (DB-API 2.0)

| 기능 | 상태 | 비고 |
|---|---|---|
| `apilevel` | ✅ `"2.0"` | |
| `threadsafety` | ✅ `1` | 스레드는 모듈 공유 가능, 연결은 불가 |
| `paramstyle` | ✅ `"qmark"` | `?` 파라미터 마커 |
| `connect()` | ✅ | 모듈 수준 생성자 |
| `Connection` | ✅ | 전체 수명 주기: 커밋, 롤백, 종료, 오토커밋 |
| `Cursor` | ✅ | execute, executemany, fetch*, callproc, description, rowcount |
| `Cursor.nextset()` | ✅ | 1.2.0부터 (#79) |
| 예외 계층 | ✅ | PEP 249 예외 클래스 전체 10종 |
| `DatabaseError`의 `errno` / `sqlstate` | ✅ | 1.2.0부터 (#71) — SQLSTATE 매핑 19종 |
| 타입 객체 | ✅ | STRING, BINARY, NUMBER, DATETIME, ROWID |
| 타입 생성자 | ✅ | Date, Time, Timestamp, *FromTicks, Binary |
| 컨텍스트 매니저 | ✅ | Connection과 Cursor 모두 |

### 연결 기능

| 기능 | 상태 | 도입 | 비고 |
|---|---|---|---|
| `connect_timeout` | ✅ | 1.0.0 | 연결 단계 타임아웃 (초) |
| `read_timeout` (동기) | ✅ | 1.2.0 (#81) | recv별 소켓 타임아웃 |
| `read_timeout` (비동기) | ✅ | 1.2.0 (#82) | `asyncio.wait_for` 래핑 |
| `fetch_size` | ✅ | 1.2.0 (#81) | 구성 가능한 결과 배치 크기 (기본 100) |
| `autocommit` 속성 | ✅ | 1.0.0 | `Connection`에서 조회/설정 |
| `Connection.ping()` | ✅ | 1.2.0 (#70) | 네이티브 CHECK_CAS 헬스 체크, SQL 불필요 |
| `get_server_version()` | ✅ | 1.0.0 | 버전 문자열 반환 (예: `"11.2.0.0378"`) |
| `get_last_insert_id()` | ✅ | 1.0.0 | AUTO_INCREMENT INSERT 이후 |
| 스키마 내성 | ✅ | 1.0.0 | `Connection.get_schema_info()` |
| 듀얼스택 주소 폴백 (동기) | ✅ | 1.0.0 | `getaddrinfo` IPv4/IPv6 순회 |
| 듀얼스택 주소 폴백 (비동기) | ✅ | 1.2.0 (#83) | 비동기 대응 |
| CAS 재연결 | ✅ | 1.0.0 | 브로커 `INACTIVE` 상태에서 자동 재연결 |

### TLS / SSL

| 기능 | 상태 | 도입 | 비고 |
|---|---|---|---|
| 동기 TLS — `ssl=True` (검증 컨텍스트) | ✅ | 1.3.0 (#85) | 기본 보안 컨텍스트. TLS 1.2 최소 강제 (#145) |
| 동기 TLS — `ssl=ssl.SSLContext(...)` | ✅ | 1.3.0 (#85) | 커스텀 컨텍스트 (호출자가 최소 TLS 버전 제어) |
| 동기 TLS — `ssl=False` / `None` | ✅ | 1.3.0 | 평문 (기본) |
| 비동기 TLS | ✅ | 1.4.0 | STARTTLS 방식 업그레이드: 평문 `CUBRS` 핸드셰이크 후 `OPEN_DATABASE` 전에 `loop.start_tls()` (`ssl_handshake_timeout` 제한) (#136, #154). 기본 컨텍스트는 TLS 1.2 최소 강제 (#145). Python 3.10은 인증서 검증 실패 시 `start_tls()` 멈춤이 알려져 있음(3.10의 알려진 CPython 비동기 TLS 핸드셰이크 버그) — #156으로 추적. |

### 비동기 (`pycubrid.aio`)

| 기능 | 상태 | 도입 | 비고 |
|---|---|---|---|
| `pycubrid.aio.connect()` | ✅ | 1.1.0 | 유사한 비동기 서피스. `AsyncConnection.ping()`은 1.3.2에 추가 (네이티브 `CHECK_CAS` FC=32). `create_lob()`은 동기 전용 유지. |
| `AsyncCursor` execute / fetch / executemany / callproc | ✅ | 1.1.0 | `await`를 사용하는 동기 유사 커서 API. 연결 오토커밋 변경은 속성 세터 대신 `set_autocommit()` 사용 |
| `AsyncConnection.commit()` / `rollback()` / `close()` | ✅ | 1.1.0 | |
| 비동기 컨텍스트 매니저 | ✅ | 1.1.0 | 연결과 커서 모두 `async with` |
| 비동기 `read_timeout` | ✅ | 1.2.0 (#82) | |
| 비동기 듀얼스택 폴백 | ✅ | 1.2.0 (#83) | |
| 비동기 파라미터 바인딩 동등성 | ✅ | 1.2.0 (#76, #77) | 동기와 `_escape_string` 공유 |
| 비동기 TLS | ✅ | 1.4.0 | STARTTLS 방식 업그레이드: 평문 `CUBRS` 핸드셰이크 후 `OPEN_DATABASE` 전에 `loop.start_tls()` (`ssl_handshake_timeout` 제한) (#136, #154). 기본 컨텍스트는 TLS 1.2 최소 강제 (#145). Python 3.10은 인증서 검증 실패 시 `start_tls()` 멈춤이 알려져 있음 — #156으로 추적. |

### 드라이버 수준 진단

| 기능 | 상태 | 도입 | 비고 |
|---|---|---|---|
| 선택적 타이밍 훅 (`enable_timing=True`) | ✅ | 1.0.0 (#54) | 기본 꺼짐. 비활성 시 오버헤드 0 — [PERFORMANCE.md](PERFORMANCE.md#타이밍--프로파일링-훅) 참고 |
| `PYCUBRID_ENABLE_TIMING` 환경 변수 | ✅ | 1.0.0 | 참값: `1`, `true`, `yes` (대소문자 무시) |
| `Connection.timing_stats` | ✅ | 1.0.0 | `TimingStats` 또는 `None` 반환 |
| `TimingStats` (connect / execute / fetch / close) | ✅ | 1.0.0 | 나노초 정밀도, 스레드 안전 |
| DEBUG 로깅 (`pycubrid.connection`, `pycubrid.cursor`, `pycubrid.lob`, `pycubrid.aio.*`) | ✅ | 1.x | 드라이버가 연결·커서·LOB·비동기 연산의 옵트인 디버그 로그 출력 |

### 데이터 타입

| CUBRID 타입 | Python 타입 | 상태 | 비고 |
|---|---|---|---|
| INTEGER, BIGINT, SMALLINT, SHORT | `int` | ✅ | |
| FLOAT, DOUBLE, MONETARY | `float` | ✅ | |
| NUMERIC, DECIMAL | `decimal.Decimal` | ✅ | |
| CHAR, VARCHAR, NCHAR, NVARCHAR, STRING | `str` | ✅ | |
| DATE | `datetime.date` | ✅ | |
| TIME | `datetime.time` | ✅ | |
| DATETIME, TIMESTAMP | `datetime.datetime` | ✅ | naive (tzinfo 없음) |
| DATETIMETZ, TIMESTAMPTZ | `datetime.datetime` (tz 포함) | ✅ | 1.2.0부터 (#78) — IANA 타임존 키 |
| BIT, VARBIT | `bytes` | ✅ | |
| BLOB | `dict` | ✅ | `lob_type`, `lob_length`, `file_locator`, `packed_lob_handle`을 가진 LOB 핸들 dict[^lob] |
| CLOB | `dict` | ✅ | `lob_type`, `lob_length`, `file_locator`, `packed_lob_handle`을 가진 LOB 핸들 dict[^lob] |
| JSON | `Any` (디시리얼라이저 경유) | ✅ | 1.2.0부터 (#72) — `connect()`의 옵트인 `json_deserializer=`; CAS 프로토콜 v8 |
| SET | `frozenset` | ✅ | 1.2.0부터 (#73) — `connect()`의 옵트인 `decode_collections=True` |
| MULTISET | `list` | ✅ | 1.2.0부터 (#73) — 옵트인 `decode_collections=True` |
| SEQUENCE | `list` | ✅ | 1.2.0부터 (#73) — 옵트인 `decode_collections=True` |
| 컬렉션 (기본, `decode_collections=False`) | `bytes` | ⚠️ | 하위 호환을 위한 raw CAS 와이어 형식 |
| OBJECT (OID) | `str` | ⚠️ | `OID:@page\|slot\|volume`로 디코딩. 고수준 OID API 없음 |
| NULL | `None` | ✅ | |

### 문장 / 커서

| 기능 | 상태 | 도입 | 비고 |
|---|---|---|---|
| `cursor.execute(sql, params)` | ✅ | 1.0.0 | 서버 측 `PREPARE_AND_EXECUTE` |
| `cursor.executemany(sql, seq)` | ✅ | 1.0.0 | 비-SELECT DML을 `BatchExecutePacket`으로 배치. SELECT만 행별 루프로 폴백 |
| `cursor.executemany_batch(sql_list, auto_commit=None)` | ✅ | 1.0.0 | 단일 왕복 `BatchExecutePacket` |
| `cursor.callproc(name, params)` | ✅ | 1.0.0 | 저장 프로시저 호출 |
| `cursor.fetchone() / fetchmany() / fetchall()` | ✅ | 1.0.0 | |
| 이터레이터 프로토콜 (`for row in cursor`) | ✅ | 1.0.0 | |
| `cursor.description` | ✅ | 1.0.0 | PEP 249 7-튜플 |
| `cursor.rowcount` | ✅ | 1.0.0 | |
| `cursor.lastrowid` | ✅ | 1.0.0 | |

### LOB

| 기능 | 상태 | 도입 | 비고 |
|---|---|---|---|
| `Connection.create_lob(BLOB)` | ✅ | 1.0.0 | |
| `Connection.create_lob(CLOB)` | ✅ | 1.0.0 | |
| LOB 읽기/쓰기 | ✅ | 1.0.0 | |
| BLOB/CLOB 컬럼에 `bytes`/`str` 직접 삽입 | ✅ | 1.0.0 | `Lob` 파라미터 바인딩(미지원)보다 권장 |

### 성능 최적화

| 기능 | 상태 | 도입 | 비고 |
|---|---|---|---|
| `socket.recv_into` 제로카피 수신 | ✅ | 1.0.0 | |
| `TCP_NODELAY` 활성 | ✅ | 1.0.0 | |
| 커서 클래스 캐시 | ✅ | 1.0.0 | |
| 사전 컴파일된 `struct` 패커 | ✅ | 1.0.0 | |
| fetch용 타입 디스패치 테이블 | ✅ | 1.0.0 | |
| 슬라이스 기반 fetch | ✅ | 1.0.0 | |
| 배치 executemany (`BatchExecutePacket`) | ✅ | 1.0.0 | |

---

## 운영

| 관심사 | 상태 | 비고 |
|---|---|---|
| 순수 Python (C 확장 없음) | ✅ | `pip install pycubrid`만으로 끝 |
| PEP 561 타입 패키지 | ✅ | `py.typed` 포함 |
| 커넥션 풀링 | ❌ | 내장 없음. SQLAlchemy `QueuePool`이나 외부 풀 사용 — [연결 가이드](CONNECTION.md#커넥션-풀링) 참고 |
| 외부 프로파일링 라이브러리 의존성 | ❌ | 표준 라이브러리의 `time.perf_counter_ns`만 사용 |

---

## 테스트 커버리지

| 지표 | 값 |
|---|---|
| 오프라인 테스트 | 770 |
| 전체 테스트 | 811 |
| 통합 잡 (PR / push) | 8 (Python {3.10, 3.14} × CUBRID 4버전) |
| 통합 잡 (나이틀리 + 태그 + dispatch) | 20 (Python 5버전 × CUBRID 4버전) |
| 스트레스 테스트 | 스레드 (워커 16 × insert 25, 리더 32) 및 `asyncio.gather` (워커 16, 리더 32) |
| 재연결 / 네트워크 엣지 케이스 | 리셋·타임아웃·broken pipe·부분 읽기를 다루는 17개 테스트 |
| 커버리지 하한 | 95% (CI 강제) |

---

[^lob]: LOB 컬럼 fetch는 내용이 아니라 핸들 딕셔너리를 반환합니다. 바이트를 읽으려면 `packed_lob_handle`과 함께 `pycubrid.lob.Lob`을 사용하거나, CLOB/BLOB 값을 쓸 때 `str`/`bytes`를 직접 삽입하세요.

*참고: [연결 가이드](CONNECTION.md) · [타입 시스템](TYPES.md) · [API 참조](API_REFERENCE.md) · [성능 가이드](PERFORMANCE.md) · [변경 이력](https://github.com/cubrid-lab/pycubrid/blob/main/CHANGELOG.md)*
