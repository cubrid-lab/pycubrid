# 지원 매트릭스 (한국어)

> 🌐 [SUPPORT_MATRIX.md](https://github.com/cubrid-lab/pycubrid/blob/main/docs/SUPPORT_MATRIX.md)의 번역입니다. 영어 원문이 표준이며, 페이지 번역은 경고 수준의 동기화 규칙을 따릅니다.

pycubrid 릴리스의 호환성과 기능 지원.

> **참조:** 현재 소스 버전은 [`pycubrid/__init__.py`](https://github.com/cubrid-lab/pycubrid/blob/main/pycubrid/__init__.py)의 `pycubrid.__version__`입니다. 게시된 릴리스 상세는 [`CHANGELOG.md`](https://github.com/cubrid-lab/pycubrid/blob/main/CHANGELOG.md)와 [릴리스](https://github.com/cubrid-lab/pycubrid/releases)를 참고하세요.

---

## 버전 호환성

### Python

| Python 버전 | 상태 |
|---|---|
| 3.11 | ✅ 지원 |
| 3.12 | ✅ 지원 |
| 3.13 | ✅ 지원 |
| 3.14 | ✅ 지원 |
| 3.15 | 🧪 지원 준비 중 (공식 지원 아님) |
| 3.10 | ❌ 1.10.0부터 미지원 (1.9.x까지 지원) |
| < 3.10 | ❌ 미지원 |

**Python 3.10 지원 종료:** Python 3.10은 2026-10-01에 공식 지원이
종료됐습니다([PEP 619](https://peps.python.org/pep-0619/#310-lifespan)).
1.8.x와 1.9.x는 Python 3.10을 지원하며, 1.9.0에 사전 안내가 포함됐습니다.
1.10.0부터 패키지는 Python 3.11 이상을 요구하므로(`requires-python =
">=3.11"`), Python 3.10의 `pip`은 계속 1.9.x를 설치합니다. 수정은 최신 릴리스
라인에만 제공되므로 1.9.x의 추가 릴리스는 없습니다. 업그레이드하기 전에
Python을 업그레이드하고 가상 환경을 새로 만든 뒤 애플리케이션을 검증하세요.

**Python 3.15 지원 준비:** 2026-10-03 기준 Python 3.15는
[3.15.0rc3 미리 보기](https://www.python.org/downloads/release/python-3150rc3/)이며
정식 출시는 2026-10-09로 예정돼 있습니다. 수동 실행 전용 Ubuntu/표준 GIL 검사는
고정 커밋의 오프라인 회귀 검사와 wheel/sdist의 새 환경 설치를 검증합니다.
검사 설정이 있다는 사실은 통과 증거가 아닙니다. 아직 Python 3.15의 공식 지원을
선언하지 않습니다. 정식 출시 후 의존성·패키징·오프라인·실 CUBRID 검증을 통과하면
지원 목록에 추가하며, free-threaded 빌드는 이번 준비 범위에 포함하지 않습니다.

### CUBRID 서버

| CUBRID 버전 | 상태 | 비고 |
|---|---|---|
| 11.4 | ✅ 지원 | 최신 안정 버전 |
| 11.2 | ✅ 지원 | |
| 11.0 | ✅ 지원 | |
| 10.2 | ✅ 지원 | 최소 테스트 버전 |
| < 10.2 | ❌ 미지원 | 현재 드라이버는 CAS 프로토콜 v8 대상 |

### CI 매트릭스

| 검증 | 일상 실행 | 전체 호환성 |
| --- | --- | --- |
| 오프라인 | Ubuntu/Python 3.12 PR 스모크. 고위험 PR은 커버리지를 제외한 전체 회귀. main/변경이 있는 주간 실행은 95% 커버리지의 전체 스위트 | 로컬 전체 테스트는 그대로 사용 가능 |
| 라이브 통합 | 고위험 PR은 최신 엔드포인트. main/변경이 있는 주간 실행은 최저·최신 엔드포인트 | 수동 실행과 모든 릴리스에서 Python 3.11–3.14 × CUBRID 10.2/11.0/11.2/11.4 |

[CI 실행 정책](CI_POLICY.md)을 참고하세요. 이 정책은 각 실행이 무엇을 테스트할지를
고를 뿐, 위에 나열한 지원 버전을 정의하지 않습니다. 대표 PR 검사는 지원되는 모든
조합에 대한 증거가 아닙니다.

### CUBRID 버전 간 서버 동작 차이

버전 차분 테스트(`tests/test_version_differential.py`)는 지원되는 모든 서버에서 같은
문장에 대해 pycubrid가 반환하는 것을 비교합니다. 오류 클래스, `errno`와 `sqlstate`,
`rowcount`, `lastrowid`, `description`, 각 값의 Python 타입과 값이 대상입니다.
pycubrid는 모든 버전을 같은 방식으로 디코딩합니다. 아래 차이는 서버에서 비롯된
것이며(`csql`에서도 같은 결과가 나옵니다), 스위트가 허용하는 차이는 이것뿐입니다.
각 항목은 업스트림 링크와 함께 `tests/helpers/version_matrix.py`에 있습니다.

| 동작 | 10.2 | 11.0 | 11.2 | 11.4 |
|---|---|---|---|---|
| `CHAR(n)`/`VARCHAR(n)`/`BIT VARYING(n)` 컬럼보다 긴 문자열/비트 값 | 조용히 잘림 | `ProgrammingError` -494 | `ProgrammingError` -494 | `ProgrammingError` -494 |
| `REGEXP_LIKE` / `REGEXP_*` 함수 | 정의되지 않음 (-494) | 사용 가능 | 사용 가능 | 사용 가능 |
| 조건으로 쓴 단순 값 (`IF(1, ...)`, `WHERE 1`) | 허용 | 허용 | `ProgrammingError` -493 | `ProgrammingError` -493 |
| `'a' \|\| 'b'` / `CONCAT`의 타입 | CHAR | VARCHAR | VARCHAR | VARCHAR |
| `CAST('a' AS VARCHAR) = 'a '` | 참 | 거짓 | 거짓 | 거짓 |
| FK로 참조되는 부모 테이블의 `TRUNCATE` | `IntegrityError` -924 | `IntegrityError` -924 | `IntegrityError` -1284 | `IntegrityError` -1284 |
| `COUNT(*)`의 타입 | INTEGER | INTEGER | BIGINT | BIGINT |
| 정수와 NUMERIC의 혼합 (`(5) * (0.100)`) | NUMERIC(14,3) | NUMERIC(14,3) | NUMERIC(19,3). 17자리 이상 BIGINT는 오버플로 (-427) | NUMERIC(14,3) |
| 3바이트 UTF-8 문자가 있는 텍스트에 대한 `REGEXP` 연산자 | 일치 | 절대 일치하지 않음 | 절대 일치하지 않음 | 절대 일치하지 않음 |
| 3바이트 UTF-8 문자가 있는 텍스트에 대한 `REGEXP_*` | 정의되지 않음 | NULL | 0 | 0 |

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
| `Cursor.nextset()` | ✅ | 1.2.0부터 (#79) — `NotSupportedError` 발생, CUBRID는 다중 결과 집합이 없음 |
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
| 스키마 getter | ✅ | 1.0.0 | 원시 `GetSchemaPacket`; 기존 위치 인자 유지 |
| 소유권이 있는 스키마 행 | ✅ | 1.8.0 (#456) | 동기/비동기 `fetch_schema_info()` / `close_schema_info()`, 키워드 전용 `arg2=None`; #457 실서버 행렬은 10.2/11.4에서 CLASS/VCLASS/ATTRIBUTE/CONSTRAINT/PRIMARY_KEY/IMPORTED_KEYS/EXPORTED_KEYS를 검증하며 네이티브 동등성이나 다른 스키마 코드는 인증하지 않음 |
| 듀얼스택 주소 폴백 (동기) | ✅ | 1.0.0 | `getaddrinfo` IPv4/IPv6 순회 |
| 듀얼스택 주소 폴백 (비동기) | ✅ | 1.2.0 (#83) | 비동기 대응 |
| 명시적 연결 복구 | ✅ | 1.2.0 (#70); 1.8.0 (#471, #485) | `ping(reconnect=True)`는 연결 끊김, 음수 `CHECK_CAS`(CAS–DB 링크 장애) 또는 검사 중 전송/프로토콜 오류 후 재접속할 수 있음; `CAS_INFO[0]=0`은 OUT_TRAN이며 세션을 유지함. 다음 요청 전에 OUT_TRAN 세션을 `CHECK_CAS`로 확인하고, 검사가 실패할 때만(CAS 재시작, broker reset, CHANGE CLIENT) SQL 재실행 없이 한 번 재접속함(#485). 자동 이스케이프 모드는 새 물리 세션마다 감지하고 명시적 모드는 유지하며, 감지 실패 시 `False` 반환. 이전 세대에서 바인딩한 비동기 파라미터 SQL은 전송 전 거부. 동적 `SET` 또는 이기종 페일오버 보장은 아님. |
| 알 수 없는 옵션 보고 | ✅ | 1.8.0 (#377) | 인식하지 못한 연결 키워드는 무시되지만 `UnknownConnectionOptionWarning`을 냅니다(철자 제안 포함). `warnings.simplefilter("error", ...)`로 오류로 올릴 수 있습니다 |

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
| `cursor.execute(sql, params)` | ✅ | 1.0.0 | 드라이버 측 리터럴 바인딩. 렌더링된 SQL을 `PREPARE_AND_EXECUTE`로 전송(서버 측 타입 바인딩 없음) — [PARAMETER_BINDING.md](PARAMETER_BINDING.md) 참고 |
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
| 오프라인 회귀 검사 | `make test`; 현재 사례는 [테스트 트리](https://github.com/cubrid-lab/pycubrid/tree/main/tests) 참조 |
| 대표 통합 검사 | 고위험 PR: 최신 조합; main/변경이 있는 주간 실행: 최저·최신 조합 |
| 전체 통합 (릴리스 workflow_call + 수동 dispatch) | 16 (Python 4버전 × CUBRID 4버전) |
| 스트레스 테스트 | 스레드 (워커 16 × insert 25, 리더 32) 및 `asyncio.gather` (워커 16, 리더 32) |
| 재연결 / 네트워크 엣지 케이스 | 현재 테스트 트리의 리셋·타임아웃·broken pipe·부분 읽기 회귀 검사 |
| 전체 실행 커버리지 | 강제되는 95% 하한; [측정 결과](https://codecov.io/gh/cubrid-lab/pycubrid)이며 일반 PR 스모크의 주장은 아님 |

---

[^lob]: LOB 컬럼 fetch는 내용이 아니라 핸들 딕셔너리를 반환합니다. 바이트를 읽으려면 `packed_lob_handle`과 함께 `pycubrid.lob.Lob`을 사용하거나, CLOB/BLOB 값을 쓸 때 `str`/`bytes`를 직접 삽입하세요.

*참고: [연결 가이드](CONNECTION.md) · [타입 시스템](TYPES.md) · [API 참조](API_REFERENCE.md) · [성능 가이드](PERFORMANCE.md) · [변경 이력](https://github.com/cubrid-lab/pycubrid/blob/main/CHANGELOG.md)*

