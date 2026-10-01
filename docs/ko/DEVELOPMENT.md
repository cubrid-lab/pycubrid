# 개발 가이드 (한국어)

> 🌐 [DEVELOPMENT.md](https://github.com/cubrid-lab/pycubrid/blob/main/docs/DEVELOPMENT.md)의 번역입니다. 영어 원문이 표준이며, 페이지 번역은 경고 수준의 동기화 규칙을 따릅니다.

pycubrid를 설정·테스트·기여하는 데 필요한 모든 것.

---

## 목차

- [사전 준비](#사전-준비)
- [설치](#설치)
- [프로젝트 구조](#프로젝트-구조)
- [테스트 실행](#테스트-실행)
  - [오프라인 테스트](#오프라인-테스트)
  - [동기/비동기 재생 패리티](#동기비동기-재생-패리티)
  - [통합 테스트](#통합-테스트)
  - [코드 커버리지](#코드-커버리지)
- [Docker 설정](#docker-설정)
- [코드 스타일](#코드-스타일)
- [Makefile 명령](#makefile-명령)
- [CI/CD](#cicd)
- [아키텍처 개요](#아키텍처-개요)
- [새 패킷 타입 추가](#새-패킷-타입-추가)
- [새 데이터 타입 추가](#새-데이터-타입-추가)
- [릴리스 절차](#릴리스-절차)

---

## 사전 준비

| 요구사항      | 버전    | 비고 |
|---------------|---------|------|
| Python        | 3.10+   | `X \| Y` 유니언 구문, `match` 문 사용 |
| Docker        | 최신    | 통합 테스트에만 필요 |
| CUBRID Server | 10.2–11.4 | Docker 또는 로컬 설치 |

---

## 설치

```bash
# 저장소 클론
git clone https://github.com/cubrid-lab/pycubrid.git
cd pycubrid

# dev 의존성과 함께 개발 모드로 설치
pip install -e ".[dev]"

# 또는 Makefile 사용
make install
```

### dev 의존성

| 패키지      | 용도 |
|-------------|------|
| `pytest`    | 테스트 프레임워크 |
| `pytest-cov`| 커버리지 리포트 |
| `ruff`      | 린터와 포매터 |

---

## 프로젝트 구조

```mermaid
graph TD
    root[pycubrid/]

    pkg["pycubrid/ - Main package (9 modules)"]
    tests[tests/ - Test suite]
    docs[docs/ - Documentation]
    pyproject[pyproject.toml - Package configuration]
    makefile[Makefile - Development commands]
    compose[docker-compose.yml - CUBRID container setup]
    changelog[CHANGELOG.md - Release history]
    contributing[CONTRIBUTING.md - Contribution guidelines]
    license[LICENSE - MIT license]
    readme[README.md - Project overview]

    root --> pkg
    root --> tests
    root --> docs
    root --> pyproject
    root --> makefile
    root --> compose
    root --> changelog
    root --> contributing
    root --> license
    root --> readme

    pkg --> init[__init__.py - Public API, PEP 249 module attributes]
    pkg --> connection["connection.py - Connection class (TCP, CAS handshake)"]
    pkg --> cursor["cursor.py - Cursor class (execute, fetch, iterate)"]
    pkg --> types[types.py - PEP 249 type objects and constructors]
    pkg --> exceptions[exceptions.py - Full PEP 249 exception hierarchy]
    pkg --> constants["constants.py - CAS protocol enums (41 function codes, 27+ types)"]
    pkg --> protocol["protocol.py - 20 packet classes (serialize/deserialize)"]
    pkg --> packet[packet.py - PacketWriter + PacketReader primitives]
    pkg --> lob["lob.py - LOB (BLOB/CLOB) support"]
    pkg --> typed[py.typed - PEP 561 marker]

    tests --> conftest["conftest.py - Shared fixtures (mock connection, mock socket)"]
    tests --> test_connection[test_connection.py - Connection lifecycle tests]
    tests --> test_cursor[test_cursor.py - Cursor operations tests]
    tests --> test_types[test_types.py - Type object tests]
    tests --> test_exceptions[test_exceptions.py - Exception hierarchy tests]
    tests --> test_constants[test_constants.py - Constants enumeration tests]
    tests --> test_protocol[test_protocol.py - Packet serialization/deserialization tests]
    tests --> test_packet[test_packet.py - PacketWriter/PacketReader tests]
    tests --> test_lob[test_lob.py - LOB tests]
    tests --> test_pep249[test_pep249.py - PEP 249 compliance tests]
    tests --> test_integration["test_integration.py - Live DB integration tests (requires Docker)"]
    tests --> test_suite[test_suite.py - Extended test suite]

    docs --> doc_connection[CONNECTION.md - Connection guide]
    docs --> doc_types[TYPES.md - Type system reference]
    docs --> doc_api[API_REFERENCE.md - Complete API documentation]
    docs --> doc_protocol[PROTOCOL.md - CAS protocol reference]
    docs --> doc_dev[DEVELOPMENT.md - This file]
    docs --> doc_examples[EXAMPLES.md - Usage examples]
```

---

## 테스트 실행

### 오프라인 테스트

대부분의 테스트는 **오프라인**입니다 — CUBRID 연결을 목킹하고 데이터베이스 없이 패킷 직렬화, 커서 로직, 타입 매핑, 예외 처리를 테스트합니다.

```bash
# 커버리지와 함께 모든 오프라인 테스트 실행
pytest tests/ -v --ignore=tests/test_integration.py \
  --cov=pycubrid --cov-report=term-missing --cov-fail-under=95

# 또는 Makefile 사용
make test
```

### 빠른 드라이버 테스트 vs. 저장소 도구 점검

오프라인 테스트 중 `repo_tooling` 마커가 붙은 하위 집합(`pyproject.toml`에
등록됨)은 저장소 정책과 도구를 점검합니다 — docs-sync 스크립트, PR 제목
검증기, 릴리스 스크립트, 워크플로 YAML 계약, 공유 품질 게이트 등입니다(#558).
이들은 pycubrid 드라이버 동작을 전혀 포함하지 않고 `pycubrid` 자체의
커버리지에서도 제외되므로, `offline-tests` 매트릭스 대신 전용
`repo-tooling-tests` CI job에서 실행되어 일상적인 드라이버 피드백 속도를
유지합니다. 점검을 옮겨도 CI가 그것을 요구하는지 여부는 바뀌지 않습니다:
`repo-tooling-tests`는 `offline-tests`와 마찬가지로 CI Gate에서 여전히 필수
job입니다.

```bash
# 빠른 드라이버 레인 — 목킹된 드라이버 동작만 (offline-tests가 실행하는 것)
pytest tests/ -m "not integration and not repo_tooling" -v

# 저장소 도구 레인 — 정책/도구 점검 (repo-tooling-tests가 실행하는 것)
pytest tests/ -m "repo_tooling" -v

# 두 레인을 모두, 여전히 오프라인으로 (실제 CUBRID 서버 없이)
pytest tests/ -m "not integration" -v
```

모듈은 파일 이동이나 경로 기반 수집 규칙이 아니라 명시적인
`pytestmark = pytest.mark.repo_tooling`으로 도구 레인에 포함됩니다. 따라서
디스크에서 재구성할 필요가 없고 `pytest tests/`에서 조용히 빠지는 테스트도
없습니다(모든 마커는 기본 수집에 추가적일 뿐이며, 실행 시 선택하거나
제외하는 것은 `-m`뿐입니다). `docs-sync.yml`은 의존성을 설치하지 않고
`test_docs_reason.py`를 순수 `python -m unittest discover`로 실행하므로, 이
모듈은(같은 위험이 있는 `test_pr_title.py`도) `pytest`를
`try`/`except ModuleNotFoundError`로 가져오고, 없으면 빈 `pytestmark`로
대체합니다 — 그곳에서는 마커 자체가 의미가 없기 때문입니다. pytest로만
실행되는 모듈에는 이 보호 장치가 필요 없습니다.

### 동기/비동기 재생 패리티

`tests/test_replay_parity.py`는 동기 `Connection`과 비동기 `AsyncConnection`이
같은 브로커에 대해 같게 동작하는지를 오프라인으로 몇 초 안에 검사합니다.
`tests/helpers/replay_broker.py`는 스레드 기반의 프로세스 내 CAS 브로커입니다.
TCP 세션을 여러 번 받을 수 있어 재접속이 실제로 일어나고, 각 요청에 시나리오별
*스크립트*로 응답하며(지정하지 않은 요청에는 `tests/helpers/cas_reply.py`로 만든
결정적인 기본 응답), 받은 모든 요청을 기록합니다.

시나리오는 공개 연산(`open`, `close`, `connect`, `execute`, `fetchall`,
`commit`, `ping` 등)의 목록과 스크립트로 이루어집니다. 두 드라이버에서 각각 새
브로커로 재생한 뒤 네 가지를 비교합니다: 단계별 결과(반환값 또는 예외 타입),
보낸 요청 전체(세션, CAS 함수, 되돌려 보낸 CAS_INFO, 인자), 이후 연결을 계속 쓸 수
있는지(`ping(reconnect=False)`), TCP 세션 수. 시나리오는 드라이버가 *의도적으로*
다른 항목을 이유와 함께 적고, 그 밖의 차이는 모두 실패입니다. 아직 고치지 않은
알려진 차이는 `unintended=`로 기록해 strict `xfail`로 실행합니다. 각 시나리오의
`check`는 두 관측 결과 모두에 실행되므로 시나리오가 원래 경로를 조용히 벗어날 수
없습니다.

```bash
pytest tests/test_replay_parity.py -v
```

다루는 범위: 연결/종료, 종료 후 재접속, autocommit 설정/복원(명시적 설정, 생성자,
미설정), commit/rollback에 의한 핸들 무효화, OUT_TRAN `CHECK_CAS` 검사, 한 번의
`CHECK_CAS` 복구(autocommit 복원과 이스케이프 모드 재감지 포함)와 복구 실패, 교체된
세션에 바인딩된 SQL(세대 펜스, #471/#485), 복구 여부별 `ping()` 실패, 잘못된
응답과 잘린 응답(#533), 세션을 유지하는 `DataError`(#512), fetch 페이지
`DataError` 계약(#536). 태스크 취소는 `pycubrid.aio`에만 있으므로 대신
`tests/test_async_cancellation.py`에서 다룹니다.

**작업별 라운드트립 예산(#557):** `Observation.step_functions(i)`는
`steps[i]`만 실행하는 동안 보낸 정확하고 순서가 있는 CAS 함수 목록을
반환합니다 — connect/setup(생성자 autocommit 세터와 백슬래시 이스케이프 모드
프로브를 포함하는 0번 단계)과 다른 모든 단계로부터 분리됩니다.
`FIRST_INSERT_BUDGET`, `REUSED_CURSOR_INSERT_BUDGET`,
`SELECT_TO_INSERT_BUDGET`, `MANUAL_INSERT_EXECUTE_BUDGET` /
`MANUAL_INSERT_COMMIT_BUDGET`, `FETCH_PAGINATION_BUDGET`,
`ESCAPE_EXPLICIT_*` / `ESCAPE_AUTOMATIC_*` 예산이 이 정확한 시퀀스에 이름을
붙입니다. 각각 리스트 동등성으로 검사하므로, 누락된 안전 요청(예: 빠진
`CHECK_CAS` 생존 확인)과 추가된 라운드트립을 똑같이 잡아냅니다 — 어느 쪽도
"최적화"로 통과할 수 없습니다. 각 예산은 작업 성공과 연결 재사용 가능 여부도
확인하며, fetch 시나리오는 반환 행을 검사합니다. 따라서 요청 수가 같더라도
잘못된 응답이나 결과로 성공할 수 없습니다. 이들의 스크립트(`_autocommit_insert`,
`_manual_insert_last_insert_id`)는 INSERT의 `PREPARE_AND_EXECUTE`에
명시적인 `OUT_TRAN`(autocommit: 암묵적 트랜잭션이 이미 커밋됨) 또는
`IN_TRAN`(수동: `commit()`을 위해 열어둠) 상태로 응답하고,
`GET_LAST_INSERT_ID`에 올바른 형식의 값을 줍니다 — 브로커의 일반 기본
응답(단순 응답 코드)은 드라이버가 (정확하게) 손상된 응답으로 거부합니다.
이 시나리오들은 향후 라운드트립 축소 작업(#419/#488/#525)의 재현 기준선이며,
프로덕션 최적화와 `CHECK_CAS` 제거는 이 작업의 범위 밖입니다.

시나리오를 추가하려면 `SCENARIOS`에 단계, `_on(...)`으로 만든 스크립트(예: 응답 후
CAS를 재활용하는 `_hang_up_after_ok`), `check`를 갖춘 `Scenario`를 추가합니다.

**의도된 차이** (실패 아님):

| 차이 | 이유 |
|---|---|
| `AsyncConnection(...)`은 연결하지 않고 `await pycubrid.aio.connect(...)`가 연결 | `__init__`은 await할 수 없음 |
| 비동기 autocommit은 `await conn.set_autocommit(v)`로 설정 | 프로퍼티 setter는 await할 수 없음 |
| 비동기 I/O 메서드는 모두 코루틴 | asyncio API |
| 비동기 `create_lob()`은 `NotSupportedError`를 발생시키고 `LOB_NEW`를 보내지 않음 | 비동기 LOB 미지원(시나리오 `create_lob`) |
| 태스크 취소 | 비동기 전용, 패리티에서 제외 |

**하니스가 찾은 의도치 않은 차이** (모두 #521에서 수정):

| 차이 | 수정 |
|---|---|
| 동기 `close()` 후 `connect()`가 명시적 `autocommit`을 다시 보내지 않음(#520) | `connect()`가 모든 대체 세션에서 명시적 세션 상태를 복원 |
| 동기 `connect(autocommit=True)`가 재접속하는 프로퍼티 setter를 사용: `SET_DB_PARAMETER`와 `COMMIT` 사이에 `CHECK_CAS`가 추가되고, 그 사이 CAS가 재활용되면 두 요청이 다른 세션으로 나뉘며, 실패 시 원래 오류가 나오고 소켓이 열린 채 남음 | 비동기처럼 연 세션에서만 적용하고 실패 시 `OperationalError` |
| 동기 `ping(reconnect=False)`가 `CHECK_CAS` 음수 응답 후에도 세션을 유지해 다음 요청이 조용히 재접속함 | 비동기처럼 손상된 세션을 닫음 |
| `CHECK_CAS` 대체 세션에서 비동기 이스케이프 감지가 `auto_commit=0`을 보냄. 다른 모든 이스케이프 감지(두 드라이버)는 연결의 값을 보냄 | 대체 세션의 감지도 연결의 값을 보냄 |

같은 시점에 공통 결함(패리티 차이 아님)도 수정했습니다: 자동 이스케이프 감지에서
감지 `ROLLBACK` 직후 CAS가 재활용되면 autocommit 적용 전에 `connect()`가
실패했습니다. 이제 두 드라이버 모두 그 OUT_TRAN 세션을 먼저 `CHECK_CAS`로
확인하고 한 번 교체합니다.

또 다른 공통 결함도 #551에서 수정했습니다(시나리오
`autocommit_setter_survives_recycle_after_set_db_parameter`): 공개 autocommit
setter는 두 요청 사이에 CAS가 재활용되면 `SET_DB_PARAMETER`와 `COMMIT`을 서로 다른
CAS 세션으로 보냈습니다. 이제 `COMMIT` 전에 새 값을 기록하므로 그 요청의 한 번뿐인
재접속이 대체 세션에 새 값을 먼저 복원합니다. `COMMIT`이 실패하면 연결을 닫고 이전
값을 유지합니다.

### 통합 테스트

통합 테스트(`integration` 마커)는 실행 중인 CUBRID 인스턴스가 필요합니다.
가장 간단한 방법은 Makefile로 Docker를 사용하는 것입니다:

```bash
make integration                          # 브로커를 localhost:33000에 게시
make integration CUBRID_TEST_PORT=33522   # 다른 컨테이너가 쓰지 않는 포트 사용
```

`make integration`은 compose 서비스를 시작하고, 고정 시간 대기 대신
`scripts/wait_for_cubrid.py`로 준비 상태를 기다리며(약 3분 안에 준비되지 않으면
실행 실패), 모든 엔드포인트 필드를 명시적으로 설정해
`-m "integration and not tls"`를 실행합니다. 이어서
`scripts/check_integration_lanes.py --results`로 JUnit 보고서를 검사해 전부
건너뛰었거나 분류되지 않은 skip이 있으면 실패시키고, 컨테이너는 항상 제거합니다.
TLS 테스트는 SSL이 켜진 브로커가 필요합니다.
[비동기 TLS 통합 테스트](#비동기-tls-통합-테스트)를 참고하세요.

**통합 테스트 활성화와 엔드포인트 선택의 구분.** 통합 테스트는
`CUBRID_TEST_URL` 또는 `CUBRID_TEST_HOST`가 비어 있지 않은 값으로 설정되면
*활성화*됩니다. *엔드포인트*는 모든 통합 모듈, `tests/conftest.py` 게이트,
`scripts/wait_for_cubrid.py`가 함께 쓰는 공유 헬퍼 `tests/_cubrid_endpoint.py`가
필드별로 결정합니다:

1. 필드별 변수 `CUBRID_TEST_HOST`, `CUBRID_TEST_PORT`, `CUBRID_TEST_DB`,
   `CUBRID_TEST_USER`, `CUBRID_TEST_PASSWORD`가 우선합니다.
2. 없으면 `CUBRID_TEST_URL=cubrid://user[:password]@host[:port]/database`의
   해당 구성 요소를 사용합니다.
3. 그것도 없으면 기본값 `localhost`, `33000`, `testdb`, `dba`, 빈 비밀번호를 씁니다.

비어 있는 필드별 변수는 설정되지 않은 것으로 보지만, `CUBRID_TEST_PASSWORD=""`는
명시적인 빈 비밀번호입니다. 스킴이 없는 `CUBRID_TEST_URL`(예: `1`)은 통합 테스트를
활성화하기만 합니다. 다른 스킴, 호스트 누락, 숫자가 아닌 포트, 잘못된 데이터베이스
이름을 가진 `CUBRID_TEST_URL`은 모든 통합 테스트를 error로 만듭니다(오프라인
테스트에는 영향 없음).
CI처럼 URL과 필드별 변수를 함께 내보내면 이전과 똑같이 동작하며, 기본값이 아닌
호스트나 포트를 가리키는 URL은 이제 조용히 `localhost:33000`을 테스트하는 대신
그대로 사용됩니다.

이미 실행 중인 서버(Docker 수명 주기 없음)를 대상으로 할 때는 엔드포인트를
명시적으로, 가급적 필드별 변수로 지정하세요:

```bash
CUBRID_TEST_HOST=127.0.0.1 CUBRID_TEST_PORT=33522 \
  CUBRID_TEST_DB=testdb CUBRID_TEST_USER=dba CUBRID_TEST_PASSWORD= \
  pytest tests/ -m "integration and not slow and not tls" -v
# 동일: CUBRID_TEST_URL="cubrid://dba@127.0.0.1:33522/testdb" pytest ...
```

**skip과 error.** 엔드포인트가 설정되지 않으면 통합 테스트는 건너뛰므로 인자 없는
`pytest`는 계속 통과합니다. 엔드포인트가 설정되어 있으면 게이트가 세션당 한 번
(`SELECT 1`, 5초 제한 시간) 접속을 확인하고, 연결할 수 없으면 모든 일반 통합
테스트가 건너뛰는 대신 엔드포인트(비밀번호 제외)와 연결 오류를 담아 **error**로
보고되며 pytest는 0이 아닌 코드로 종료합니다. 어떤 테스트 모듈도 import 시점에
서버에 접속하지 않습니다. 통합 테스트를 건너뛰려면 `CUBRID_TEST_URL`과
`CUBRID_TEST_HOST`를 모두 해제하세요.

`tests/test_integration_cas_reconnect.py`의 CAS 재활용 회귀 테스트(#485)는 서버
컨테이너 안에서 `broker_changer`로 브로커 파라미터를 바꾸고 `cubrid broker reset`을
실행합니다. `CUBRID_TEST_DOCKER_CONTAINER`가 그 컨테이너를 가리키지 않으면 건너뜁니다.
예: `export CUBRID_TEST_DOCKER_CONTAINER="$(docker compose ps -q cubrid)"`.
`make integration`과 CI 통합 레인은 이 값을 설정하며, 모든 변경은 테스트가 끝나기 전에
원래대로 복원됩니다.

#### 비동기 TLS 통합 테스트

`tests/test_aio_ssl_integration.py`는 `pycubrid.aio`의 비동기 TLS 커버리지를 추가합니다.
저장소의 기본 `docker-compose.yml`은 평문 브로커만 시작하므로, 별도의 TLS 활성 브로커를 가리키지 않는 한 이 테스트들은 건너뜁니다.

필요에 따라 일반 통합 변수에 이 TLS 오버라이드를 추가로 내보내세요:

```bash
export CUBRID_TLS_TEST_HOST=localhost
export CUBRID_TLS_TEST_PORT=33001
export CUBRID_TLS_TEST_DB=testdb
export CUBRID_TLS_TEST_USER=dba
export CUBRID_TLS_TEST_PASSWORD=

# 선택: ssl.SSLContext/load_verify_locations()용 사설 CA 번들.
export CUBRID_TLS_TEST_CA_FILE="$PWD/certs/ca.pem"

# 선택: 호스트명 불일치 커버리지용 대체 도달 가능 호스트/IP.
export CUBRID_TLS_TEST_MISMATCH_HOST=127.0.0.1

# test_aio_ssl_connect_default_context가 사설 CA를 쓰면,
# pytest 실행 전에 프로세스 기본 신뢰 저장소도 그 CA로 지정.
export SSL_CERT_FILE="$CUBRID_TLS_TEST_CA_FILE"
```

브로커 측 TLS가 이미 활성화되어 있어야 하고(`cubrid_broker.conf`의 `SSL=ON`) 브로커 인증서가 `CUBRID_TLS_TEST_HOST`와 일치해야 합니다. 그런 다음:

```bash
pytest tests/test_aio_ssl_integration.py -v
```

##### CI의 자동화된 TLS 커버리지

일상 개발에서 위 단계를 로컬로 실행할 필요는 없습니다 — `.github/workflows/integration-full.yml`에 다음을 수행하는 `integration-tls` 잡(Python {3.10, 3.14} × CUBRID 11.4)이 포함되어 있습니다:

1. CUBRID 11.4 컨테이너를 수동으로 시작 (컨테이너 기동 후 브로커 설정을 패치할 수 있도록).
2. 새 자가 서명 인증서를 생성하고(`CN=localhost`, `SAN=DNS:localhost`), `cas_ssl_cert.{crt,key}`로 컨테이너에 주입한 뒤 `BROKER1`의 `SSL=OFF` → `SSL=ON`으로 전환하고 브로커를 재시작해 새 인증서가 반영되게 함.
3. 생성된 CA 번들을 `CUBRID_TLS_TEST_CA_FILE`과 `SSL_CERT_FILE`로 Python 테스트 픽스처에 내보냄.
4. 실제 TLS 핸드셰이크로 브로커를 프로브하고, TLS가 실제로 서비스 중이 아니면 잡을 크게 실패시킴 — 조용한 스킵은 명시적으로 거부됨.
5. `CUBRID_TLS_TEST_*` 환경 변수를 자동 연결해 TLS 브로커에 대해 `tests/test_aio_ssl_integration.py`를 실행.

> **Python 3.10 참고**: 드라이버의 인증서 검증 preflight가 알려진 비동기 TLS 검증
> 문제를 처리합니다([#156](https://github.com/cubrid-lab/pycubrid/issues/156)). TLS 레인은
> 호스트 이름 검증 실패를 포함해 선택된 모든 테스트가 실행되어야 하며, 브로커 설정
> 누락으로 인한 스킵은 허용하지 않습니다. 브로커 상태 확인 및 재시작은 서비스 소유자
> `cubrid`로 실행해 실제 브로커를 제어합니다.

이 잡은 `integration-full`의 나머지와 같은 트리거(나이틀리, `workflow_dispatch`, 그리고 `release.yml`이 호출하는 릴리스 게이트)로 실행됩니다.

### 코드 커버리지

현재 테스트 지표:

| 지표 | 값 |
|--------|-------|
| 오프라인 테스트 | 471 |
| 통합 테스트 | 41 |
| 문장 커버리지 | 99.88% |
| 문장 수 | 1,134 |
| 미커버 | 1 |
| CI 임계값 | 95% |

```bash
# HTML 커버리지 리포트 생성
pytest tests/ --ignore=tests/test_integration.py \
  --cov=pycubrid --cov-report=html

# 브라우저에서 열기
open htmlcov/index.html
```

---

## Docker 설정

### docker-compose.yml

```yaml
services:
  cubrid:
    image: cubrid/cubrid:11.2
    container_name: cubrid-test
    ports:
      - "33000:33000"
    environment:
      CUBRID_DB: testdb
```

### 명령

```bash
# 기본 CUBRID 11.2로 시작
docker compose up -d

# 특정 버전으로 시작
CUBRID_VERSION=11.4 docker compose up -d

# 컨테이너 상태 확인
docker compose ps

# 로그 보기
docker compose logs -f cubrid

# 중지 및 정리
docker compose down -v
```

### 연결 상세

| 파라미터 | 값 |
|-----------|-------|
| Host | `localhost` |
| Port | `33000` |
| Database | `testdb` |
| User | `dba` |
| Password | (비어 있음) |

---

## 코드 스타일

### Ruff 설정

```toml
[tool.ruff]
line-length = 100
target-version = "py310"
```

### 관례

- **임포트**: 모든 모듈에 `from __future__ import annotations`
- **타입 힌트**: 완전한 타이핑. PEP 561 준수 (`py.typed`)
- **super()**: 항상 `super().__init__()`, 절대 `super(ClassName, self)` 아님
- **행 길이**: 100자
- **독스트링**: 모든 공개 메서드와 클래스에 Google 스타일
- **네이밍**:
  - 클래스: `PascalCase` (예: `PacketWriter`, `ColumnMetaData`)
  - private 메서드: `_underscore_prefix` (예: `_parse_byte`, `_write_int`)
  - 상수: `UPPER_SNAKE_CASE` (예: `CAS_INFO`, `DATA_LENGTH`)

### 린팅

```bash
# 문제 확인
make lint

# 자동 수정
make format

# 도구 버전과 현재 환경을 별도로 검사
make tooling-check
```

Make와 일반/정기 CI는 `Makefile`의 `LINT_PATHS` 목록을 공유합니다:
`pycubrid tests scripts demos examples`. Ruff CLI와 훅은 명시적으로 Python/pyi만 검사하므로
Markdown은 이 포맷 범위에 포함하지 않습니다. 훅에도 같은 관리 파일 범위를 적용하며,
Mypy는 기존의 엄격한 패키지 전용 검사를 유지합니다. 검사는 현재 Python 환경의
도구를 사용하므로 `.[dev]`를 설치하고 해당 환경을 활성화하세요.

Ruff/Mypy의 정확한 버전은 `pyproject.toml`에서 관리합니다. Ruff와 Mypy
pre-commit 훅은 `repo: local` / `language: system` 훅으로, 같은 활성 환경에서
`python3 -m ruff`와 `python3 -m mypy`를 직접 호출합니다. 따라서 별도로 맞춰야 할
훅 버전(`rev:`)이 없습니다: dev 핀을 올리고(Dependabot의 `pip` 생태계가 정확히 이
작업을 수행합니다) `.[dev]`를 다시 설치하면 충분합니다. 커밋 시 훅이 실행되길
원한다면 그 환경(또는 이를 설치한 venv)을 항상 활성화해두세요. 그렇지 않으면
Ruff/Mypy가 없거나, 고정된 버전 대신 오래되거나 전역에 설치된 버전이 조용히
실행됩니다. dev 핀을 갱신하고 `.[dev]`를 다시 설치한 뒤 `make check-all` 및
`pre-commit run --all-files`를 실행하세요. `make tooling-check`는 린트/포맷/타입
검사 전에 핀, 설치 버전, 훅 범위 및 CI 범위의 불일치를 실패 처리합니다.

### 안티패턴 (절대 금지)

- SQL 쿼리에 f-string 보간 금지 (SQL 인젝션 위험)
- `super(ClassName, self)` 금지 — `super()`만 사용
- Python 2 구조 금지
- 빈 `except` 블록 금지 (`close()` 같은 정리 경로는 예외)
- 타입 억제 금지 (설명 없는 `# type: ignore`)

---

## Makefile 명령

| 명령 | 설명 |
|---------|-------------|
| `make install` | 모든 의존성과 함께 개발 모드 설치 |
| `make test` | 커버리지와 함께 오프라인 테스트 실행 |
| `make lint` | ruff check + format 검사 실행 |
| `make format` | 린트와 포맷 문제 자동 수정 |
| `make integration` | Docker → 준비 대기 → 통합 테스트 → skip 검사 → 정리 (`CUBRID_TEST_PORT=<port>`로 브로커 포트 변경) |
| `make integration-local` | 이미 실행 중인 서버에 대한 통합 테스트 (`CUBRID_TEST_URL` 또는 `CUBRID_TEST_HOST`/`PORT`) |
| `make clean` | 빌드 산출물 제거 |

---

## CI/CD

일반 및 전체 통합 워크플로는 테스트 전에
`python scripts/wait_for_cubrid.py`를 실행합니다. 이 스크립트는 테스트
스위트와 똑같이 엔드포인트를 결정합니다(필드별 `CUBRID_TEST_HOST`,
`CUBRID_TEST_PORT`, `CUBRID_TEST_DB`, `CUBRID_TEST_USER`,
`CUBRID_TEST_PASSWORD` → `CUBRID_TEST_URL` → 기본값
`localhost:33000/testdb`, 사용자 `dba`, 빈 비밀번호).
`SELECT 1` 확인을 5초 간격으로 최대 30회 시도하며, 모두 실패하면
잡을 실패 처리해 테스트 단계가 실행되지 않습니다.
연결 및 읽기 제한 시간은 각각 5초이며 `CUBRID_TEST_CONNECT_TIMEOUT`과
`CUBRID_TEST_READ_TIMEOUT`으로 변경할 수 있습니다. 접속에 성공해도 `SELECT 1`이
실패하면 준비되지 않은 것으로 처리합니다. 성공/실패 및 커서 정리 오류 시에도
커서와 연결을 닫습니다.

통합 테스트는 파일 이름이나 고정된 테스트 수 대신 pytest 마커로 레인을 배정합니다:

| 레인 | 선택식 | 실행 워크플로 |
|---|---|---|
| 일반 | `integration and not slow and not tls` | PR/push CI, 전체 호환성 매트릭스, 나이틀리 bug hunt |
| 장시간 | `integration and slow and not tls` | 나이틀리/수동 bug hunt의 soak, chaos, 동시성 stress |
| TLS | `integration and tls` | 일반 CI와 전체 워크플로의 전용 TLS 잡 |
| 공식 드라이버 차분 | `integration and official_differential` | 일반 CI와 전체 워크플로의 필수 `official-differential` 잡 (Python 3.10, CUBRID 10.2와 11.4) |

`python scripts/check_integration_lanes.py`는 현재 마커 목록을 수집하고 각 레인의 실제
워크플로 명령을 확인합니다. JUnit 검사(`--results FILE`)는 알려지지 않은 스킵,
빈 실행 또는 전체 스킵을 실패 처리합니다. 공식 드라이버 차분이 자기 레인 밖에서
스킵되는 경우는 `official-lane-only`로, `/proc`가 없는 플랫폼도 명시적으로
분류합니다. 브로커/TLS 설정 누락은 CI에서 허용하는 스킵이 아니며,
`--lane official`은 어떤 스킵도 허용하지 않습니다. 나이틀리 bug hunt의 별도 오프라인 protocol, fault-broker,
placeholder 검사는 확장된 Hypothesis 프로필로 유지됩니다.

공식 드라이버 차분(#446)은 고정 소스에서 빌드한 공식 `CUBRIDdb`/`_cubrid`
드라이버와 pycubrid를 비교합니다. 로컬에서 재현하려면(Linux x86_64, git,
CMake 3.21 이상, C 컴파일러, Python 3.10 헤더 필요) 다음을 실행합니다.

```bash
python3.10 scripts/build_official_oracle.py --out .official-oracle
PYTHONPATH=.official-oracle PYCUBRID_OFFICIAL_ORACLE_REQUIRED=1 \
  PYCUBRID_OFFICIAL_ORACLE_MANIFEST=.official-oracle/oracle.json \
  PYCUBRID_DIFFERENTIAL_EVIDENCE=official-evidence.jsonl \
  CUBRID_TEST_URL=cubrid://dba@localhost:33000/testdb \
  python3.10 -m pytest tests/ -m "integration and official_differential"
python scripts/check_official_differential.py --evidence official-evidence.jsonl
```

새로 추가하거나 바꾼 주장은 `tests/fixtures/official_differential_claims.json`에
기록합니다. 해당 케이스는 `tests/test_official_differential.py`의 `CASES`에
추가한 뒤 `python scripts/check_official_differential.py --write-docs`를
실행합니다. 차이는 수정하거나, 이유·이슈·양쪽 관측값을 갖춘 `deviation`으로
기록합니다. 현재 출력에 맞추려고 기대값만 고치지 마세요.
[호환성 가이드](UPSTREAM_COMPATIBILITY.md#공식-드라이버-차분-게이트-446)를 참고하세요.

`tests/test_protocol_fuzz.py`는 `tests/helpers/cas_reply.py`가 만든 실제와 같은
브로커 응답을 변형합니다(#523). 모든 주요 타입의 컬럼 메타데이터와 행 데이터를
담은 실행 응답과 FETCH 응답, 그리고 스키마, 배치, LOB 응답이 포함됩니다. 각 시드는
디코딩 기댓값과 길이 워드, 개수, 필드 경계의 오프셋을 기록하므로 변형하지 않은
시드는 정확한 왕복 검사가 되고, 변형은 잘림과 길이/개수 불일치를 겨냥합니다.
새 컬럼 조합을 시드로 추가하려면 `RESULT_SETS`에 `ResultSet`을 추가하세요. FETCH와
실행 응답 fuzz 대상이 이를 가져옵니다. 새 응답 빌더에는 별도의 왕복 테스트와 fuzz
대상이 필요합니다. 예제 수는 Hypothesis 프로필에서
정합니다(`pr`: 대상마다 50개, 모듈 전체 약 2초; `nightly`: 1000개).

### 문서 예외와 기여자 검증 기록

문서 게이트는 따옴표 인용, HTML 주석, 코드 블록 밖의 독립된 물리적 소스 줄에 실제
이유가 있는 `Docs: not needed -`만 인정합니다. 일반 설명 바로 옆 줄에 둘 수 있으며,
별도 문단이나 빈 줄이 필수인 것은 아닙니다.
앞의 공백은 0~3개까지 허용하며, 들여쓴 코드와 HTML 인용/pre/code 블록의 예시는
예외 승인을 부여하지 않습니다.
기존 `docs-not-needed` 라벨 예외는 별도로 유지됩니다. `make docs-reason-check`는
헬퍼 doctest와 실제 이벤트 JSON 기반 워크플로 사례를 실행하며, `make check-all`과
docs-sync CI에서도 이 검사를 실행합니다.

기여자는 실제 실행한 명령/결과와 실행하지 않은 검사/이유를 기록하고, 선택적 AI
리뷰는 별도로 구분합니다. 유지보수자는 내부 리뷰, 실제 이슈 라벨 및 명시적으로
승인한 `translations-deferred` 후속 작업을 조율합니다. PR 본문의 번역 도움 요청은
승인을 부여하지 않습니다. 한국어 README 동기화는 필수이며 다른 번역은 권고 수준입니다.

공유 doc-lint와 CodeQL 호출은 `workflow_call` 입력을 확인한 검토된 커밋 SHA를
사용합니다. 기존 권한, 권고 수준 롤아웃 및 필수 게이트는 유지됩니다. doc-lint가
main 기반 설정/스캐너를 내려받으므로 호출자 핀만으로 이 자산까지 고정되지는 않습니다.

### GitHub Actions 워크플로

| 워크플로 | 트리거 | 설명 |
|----------|---------|-------------|
| `ci.yml` | main 푸시, PR | 린트 + 오프라인 테스트 (Python 3.10–3.14) + 통합 |
| `integration-full.yml` | 야간, 수동 실행, `release.yml`에서 호출 | 전체 Python × CUBRID 호환성 매트릭스 |
| `prepare-release.yml` | 수동 실행 (`-f version=X.Y.Z`) | `chore: release vX.Y.Z` PR 생성 (날짜가 있는 CHANGELOG 섹션 + 버전 갱신) |
| `release.yml` | main 푸시, 복구용 수동 실행 | 병합된 릴리스 PR 감지 후 전체 매트릭스, 빌드, 태그 + GitHub Release + PyPI, cookbook 검증 |

### CI 매트릭스

- **오프라인**: Python 3.10, 3.11, 3.12, 3.13, 3.14
- **통합**: Python {3.10, 3.12} × CUBRID {11.2, 11.4}

---

## 아키텍처 개요

```mermaid
graph TD
    user[User Code] --> init[__init__.py - Module API connect/types/exceptions]
    init --> connection[connection.py - TCP socket, CAS handshake, session]
    connection --> cursor[cursor.py - SQL execution, parameter binding, fetch]
    cursor --> protocol[protocol.py - 20 packet classes serialize/deserialize]
    protocol --> packet[packet.py - PacketWriter + PacketReader binary I/O]
    packet --> constants[constants.py - CAS function codes, data types, enums]

    types[types.py - PEP 249 types] --> cursor
    exceptions[exceptions.py - PEP 249 errors] --> connection
    lob[lob.py - LOB objects] --> connection
```

### 데이터 흐름

1. **사용자**가 `cursor.execute("SELECT ...")` 호출
2. **Cursor**가 파라미터를 바인딩하고 `PrepareAndExecutePacket` 생성
3. **Connection**이 `_send_and_receive(packet)` 호출
4. **PacketWriter**가 프로토콜 헤더로 요청 직렬화
5. **Socket**이 CAS 서버로 바이트 전송
6. **Socket**이 응답 바이트 수신
7. **PacketReader**가 응답 역직렬화
8. **Packet**이 컬럼 메타데이터와 행 데이터 파싱
9. **Cursor**가 `fetchone()`/`fetchall()`을 위해 행 저장

---

## 새 패킷 타입 추가

새 CAS 함수를 추가하려면:

1. `constants.py`의 `CASFunctionCode`에 함수 코드 추가:

   ```python
   class CASFunctionCode(IntEnum):
       # ... 기존 코드들 ...
       MY_NEW_FUNCTION = 42
   ```

2. `protocol.py`에 패킷 클래스 생성:

   ```python
   class MyNewPacket:
       """Description (FC=42)."""

       def __init__(self, arg1: int) -> None:
           self.arg1 = arg1
           self.result: str = ""

       def write(self, cas_info: bytes) -> bytes:
           writer = PacketWriter()
           writer._write_byte(CASFunctionCode.MY_NEW_FUNCTION)
           writer.add_int(self.arg1)
           payload = writer.to_bytes()
           header = build_protocol_header(len(payload), cas_info)
           return header + payload

       def parse(self, data: bytes) -> None:
           reader = PacketReader(data)
           _ = reader._parse_bytes(DataSize.CAS_INFO)
           response_code = reader._parse_int()
           if response_code < 0:
               remaining = len(data) - 8
               _raise_error(reader, remaining)
           # 결과별 데이터 파싱
           self.result = reader._parse_null_terminated_string(response_code)
   ```

3. `tests/test_protocol.py`에 테스트 추가:

   ```python
   def test_my_new_packet_write():
       packet = MyNewPacket(arg1=42)
       data = packet.write(b"\x00\x00\x00\x00")
       assert len(data) > 8  # 헤더 + 페이로드
   ```

---

## 새 데이터 타입 추가

새 CUBRID 데이터 타입을 지원하려면:

1. `constants.py`의 `CUBRIDDataType`에 타입 코드 추가
2. `protocol.py`의 `_read_value()`에 리더 추가
3. `packet.py`의 `PacketWriter`에 라이터 추가 (필요 시)
4. 읽기와 쓰기 양쪽에 테스트 추가

---

## 릴리스 절차

릴리스는 유지보수자 전용이며 [RELEASING.md](https://github.com/cubrid-lab/pycubrid/blob/main/RELEASING.md)를 따릅니다:
`prepare-release.yml`이 릴리스 PR(버전 갱신 + 날짜가 있는 CHANGELOG 섹션, `make release-check VERSION=X.Y.Z`로 확인)을
엽니다. 검토 후 squash 병합하면 `release.yml`이 전체 매트릭스, 한 번의 빌드, 태그, PyPI 게시, cookbook 검증을
자동으로 수행합니다. 태그 푸시나 게시를 수동으로 하지 않습니다.
