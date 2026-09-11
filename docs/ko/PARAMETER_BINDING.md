# 파라미터 바인딩 (한국어)

> 🌐 [PARAMETER_BINDING.md](https://github.com/cubrid-lab/pycubrid/blob/main/docs/PARAMETER_BINDING.md)의 번역입니다. 영어 원문이 표준이며, 페이지 번역은 경고 수준의 동기화 규칙을 따릅니다.

pycubrid 1.x의 드라이버 측 파라미터 바인딩 계약.

이 문서는 pycubrid가 Python 값을 SQL 리터럴로 변환해 CUBRID로 보내는 방식의 **권위 있는 명세**입니다. 이 문서의 모든 주장은 구현(`pycubrid/_cursor_common.py`)과 동작을 고정하는 단위 테스트에 대한 인용으로 뒷받침됩니다. 아래 규칙의 모든 변경은 [`RELEASE_POLICY.md`](https://github.com/cubrid-lab/pycubrid/blob/main/RELEASE_POLICY.md)가 다루는 계약 변경입니다.

---

## 목차

- [개요](#개요)
- [플레이스홀더 방식](#플레이스홀더-방식)
- [타입 매핑 (보장)](#타입-매핑-보장)
- [문자열 이스케이프](#문자열-이스케이프)
  - [이스케이프 모드 협상](#이스케이프-모드-협상)
  - [리터럴 모드](#리터럴-모드-no_backslash_escapestrue)
  - [이스케이프 처리 모드](#이스케이프-처리-모드-no_backslash_escapesfalse)
- [플레이스홀더 토크나이저](#플레이스홀더-토크나이저)
- [`executemany`](#executemany)
- [비보장과 명시적 한계](#비보장과-명시적-한계)
- [호환성 정책 (1.x)](#호환성-정책-1x)
- [고정 테스트](#고정-테스트)
- [참고 자료](#참고-자료)

---

## 개요

pycubrid는 **드라이버 측 리터럴 바인딩**을 수행합니다. `cursor.execute(sql, parameters)`를 호출하면 드라이버는:

1. `sql`을 따옴표 밖·주석 밖의 `?` 플레이스홀더를 기준으로 세그먼트 분할(`split_on_placeholders`, `pycubrid/_cursor_common.py:46-119`).
2. 플레이스홀더 수가 `len(parameters)`와 일치하는지 검증(`pycubrid/_cursor_common.py:199-203`).
3. 각 Python 값을 `format_parameter`로 SQL 리터럴 문자열로 변환(`pycubrid/_cursor_common.py:141-181`).
4. 세그먼트와 렌더링된 리터럴을 하나의 SQL 문자열로 연결(`pycubrid/_cursor_common.py:204-208`).
5. 완전히 렌더링된 SQL을 `PrepareAndExecutePacket`으로 CUBRID에 전송(`pycubrid/cursor.py:139-150`, `pycubrid/aio/cursor.py:114-117`).

바인딩 구현은 `CursorParamsMixin`을 통해 동기(`Cursor`)와 비동기(`AsyncCursor`) 경로에서 **그대로 공유**됩니다(`pycubrid/_cursor_common.py:237-257`). 동기·비동기 바인딩에 동작 차이가 없으며, 동등성은 `tests/test_aio_cursor_parity.py`와 `tests/test_split_placeholders.py`로 강제됩니다.

**이것은 서버 측 prepared-statement 바인딩이 아닙니다.** pycubrid는 파라미터 값을 별도의 타입 페이로드로 보내지 않으며, 브로커는 execute마다 완전한 SQL 텍스트를 받습니다. [비보장과 명시적 한계](#비보장과-명시적-한계)를 참고하세요.

---

## 플레이스홀더 방식

- `paramstyle = "qmark"` (`pycubrid/__init__.py:46`), PEP 249 준수.
- 플레이스홀더는 위치 기반 `?`입니다. named·numeric·pyformat 플레이스홀더는 **없습니다**.
- `execute()`의 `parameters` 인자는 `str`/`bytes`/`bytearray`가 아닌 `Sequence`여야 합니다. 매핑은 `ProgrammingError`로 거부됩니다(`pycubrid/_cursor_common.py:194-197`). 정확한 메시지 문구는 참고 사항이며 [비보장과 명시적 한계](#비보장과-명시적-한계)를 참고하세요.
- 플레이스홀더 수 불일치는 `ProgrammingError`를 발생시킵니다(`pycubrid/_cursor_common.py:199-203`). 정확한 메시지 문구는 참고 사항입니다.

---

## 타입 매핑 (보장)

다음 표는 1.x의 **권위 있는 타입→리터럴 매핑**입니다. 모든 행은 `pycubrid/_cursor_common.py`의 구현 위치와 동작을 고정하는 테스트를 인용합니다.

> 각 오류 사례에서 발생하는 **예외 클래스**(예: `ProgrammingError`)는 계약의 일부이며, 표에 보이는 **메시지 문구**는 예시일 뿐이며 1.x 내에서 다듬어질 수 있습니다. [비보장과 명시적 한계](#비보장과-명시적-한계)를 참고하세요.

| Python 타입 | SQL 리터럴 | 구현 | 고정 테스트 |
|---|---|---|---|
| `None` | `NULL` | `_cursor_common.py:143-144` | `tests/test_param_security.py:95-97` |
| `bool` | `1` (True) / `0` (False) | `_cursor_common.py:145-146` | `tests/test_param_security.py:98-102` |
| `int` | `str(value)` (10진수) | `_cursor_common.py:177-180` | `tests/test_param_security.py:107-109` |
| `float` | `str(value)`; `nan`/`inf`/`-inf`는 `ProgrammingError` 발생 (현재 메시지: `"nan and inf are not supported by CUBRID"`) | `_cursor_common.py:177-180` | `tests/test_param_security.py:132-142` |
| `decimal.Decimal` | `str(value)` (따옴표 없음) | `_cursor_common.py:175-176` | `tests/test_param_security.py:113-115` |
| `str` | 작은따옴표 리터럴; [문자열 이스케이프](#문자열-이스케이프) 적용; NUL(`U+0000`)과 Ctrl-Z(`U+001A`, `\x1a`)는 각각 `ProgrammingError` 발생 (현재 메시지: `"string parameter contains null byte"`, `"string parameter contains Ctrl-Z (0x1A) byte"`) | `_cursor_common.py:147-148, 124-138` | `tests/test_param_security.py:27-84` |
| `bytes`, `bytearray` | `X'<hex>'` (소문자 hex) | `_cursor_common.py:149-150` | `tests/test_param_security.py:104-106, 144-145` |
| `datetime.datetime` (naive) | `DATETIME'YYYY-MM-DD HH:MM:SS.mmm'` — 마이크로초는 밀리초로 절사(`value.microsecond // 1000`) | `_cursor_common.py:151-152, 170` | `tests/test_param_security.py:124-127` |
| `datetime.datetime` (tz 포함) | `DATETIMETZ'YYYY-MM-DD HH:MM:SS.mmm <tz>'` — `<tz>`는 `tzinfo.key`가 있으면 그 값(예: `Asia/Seoul`), 없으면 `±HH:MM` 숫자 오프셋 | `_cursor_common.py:151-169` | `tests/test_param_security.py:147-169` |
| `datetime.date` | `DATE'YYYY-MM-DD'` | `_cursor_common.py:171-172` | `tests/test_param_security.py:116-118` |
| `datetime.time` | `TIME'HH:MM:SS'` — 마이크로초 버림 | `_cursor_common.py:173-174` | `tests/test_param_security.py:120-122` |
| 그 외 전부 | `ProgrammingError` (현재 메시지: `"unsupported parameter type"`) | `_cursor_common.py:181` | `tests/test_param_security.py:128-130`; `tests/test_cursor.py:233-235` |

### 바인딩 값으로 명시적으로 미지원

- `datetime.timedelta` — 분기가 없음; `ProgrammingError("unsupported parameter type")` 발생.
- `pycubrid.Lob` — `Lob` 인스턴스는 SQL 리터럴로 변환되지 않습니다. `BLOB`/`BIT` 타입 컬럼에는 raw `bytes`를 삽입하고, 대형 객체 워크플로우에는 `Connection.create_lob()`과 LOB 쓰기 API를 사용하세요(`pycubrid/lob.py`, `pycubrid/connection.py:333-339`).
- 컬렉션(`list`, `tuple`, `set`, `frozenset`, `dict`)을 단일 바인딩 값으로 — 실행 가능한 메시지와 함께 `ProgrammingError` 발생 (현재 문구: `cannot bind a collection (list/tuple/set/frozenset/dict) as a single parameter; pycubrid does not auto-expand IN (?, ?, ...) — expand the placeholders explicitly in the SQL`). **자동 `IN (?, ?, ?)` 확장은 없습니다**. SQL에 플레이스홀더를 명시적으로 펼치세요.
- 임의의 Python 객체 — `ProgrammingError("unsupported parameter type")` 발생.

---

## 문자열 이스케이프

문자열 이스케이프는 `escape_string`(`pycubrid/_cursor_common.py`)이 수행합니다. 동작은 `no_backslash_escapes` 연결 플래그(`pycubrid/_connection_common.py`)에 따라 달라집니다. 기본적으로 이 플래그는 연결 시점에 라이브 서버에서 **자동 협상**됩니다([이스케이프 모드 협상](#이스케이프-모드-협상) 참고). 감지를 덮어쓰려면 `pycubrid.connect(..., no_backslash_escapes=True|False)`로 명시적으로 전달하세요.

모든 모드에서:

- 리터럴은 작은따옴표로 감싸집니다.
- 입력의 NUL(`U+0000`)은 `ProgrammingError` 발생(`pycubrid/_cursor_common.py:130-131`). 이것은 무조건적이며 양쪽 모드에 적용됩니다. 정확한 메시지 문구는 참고 사항입니다.
- 작은따옴표는 doubling됩니다(`'` → `''`).
- 유니코드 코드 포인트(UTF-16이 서로게이트 페어로 인코딩할 비-BMP 문자 포함)는 변경 없이 통과합니다(`tests/test_param_security.py::TestEscapeString::test_unicode_passthrough`, `::test_unicode_non_bmp_passthrough`).

### 이스케이프 모드 협상

CUBRID의 `no_backslash_escapes` **시스템 파라미터 기본값은 `yes`**입니다 — 백슬래시는 이스케이프 마커가 아니라 일반 리터럴 문자입니다([CUBRID 매뉴얼, literal.rst](https://github.com/CUBRID/cubrid-manual/blob/master/en/sql/literal.rst): *"백슬래시 이스케이프를 사용하려면 cubrid.conf의 no_backslash_escapes를 no로 설정해야 합니다. 하지만 기본값은 yes입니다."*). 드라이버가 그런 서버에 대해 맹목적으로 백슬래시를 doubling하면 `C:\temp\file`이 `C:\\temp\\file`로 저장됩니다 — 조용한 데이터 오염입니다 (이슈 #255).

서버의 실제 설정이 무엇이든 정확을 유지하기 위해, `no_backslash_escapes`가 `connect()`에 전달되지 **않으면** 드라이버는 연결 시점에 `SELECT CHAR_LENGTH('\\')`(SQL 리터럴 `'\\'`, 백슬래시 두 문자)로 라이브 서버를 한 번 프로브합니다:

- 결과 `2` → 서버가 두 백슬래시를 그대로 둠 → **리터럴 모드**, 드라이버는 `no_backslash_escapes=True`로 고정(백슬래시를 doubling하지 않음).
- 결과 `1` → 서버가 쌍을 언이스케이프함 → **이스케이프 처리 모드**, 드라이버는 `no_backslash_escapes=False`로 고정.
- 그 외 값이나 프로브 오류 → `OperationalError` 발생. 드라이버는 이스케이프 모드를 추측하지 않습니다. 잘못된 값은 문자열 이스케이프를 조용히 오염시키고(SQL 인젝션도 가능) 때문입니다. 프로브를 실행할 수 없을 때는 `no_backslash_escapes`를 명시적으로 전달해 감지를 건너뛰세요.

`no_backslash_escapes=True` 또는 `False`를 명시적으로 전달하면 프로브를 완전히 건너뜁니다. 협상은 물리적 연결당 한 번 일어나며 투명한 재연결에서 보존됩니다.

### 리터럴 모드 (`no_backslash_escapes=True`)

자동 협상이 기본 CUBRID 서버(`no_backslash_escapes=yes`)에서 선택하는 모드입니다:

1. 작은따옴표 doubling(`'` → `''`).
2. 백슬래시와 제어 문자는 **그대로 둡니다**(서버가 일반 문자로 취급하므로 바이트 단위로 왕복합니다).
3. NUL 거부는 여전히 적용됩니다.
4. 결과를 작은따옴표로 감쌉니다.

`tests/test_aio_cursor_parity.py:99-105`, `tests/test_backslash_negotiation.py`, 라이브 왕복 스위트 `tests/test_integration.py::TestBackslashRoundTrip`로 고정됩니다.

### 이스케이프 처리 모드 (`no_backslash_escapes=False`)

서버가 `no_backslash_escapes=no`로 실행 중이거나 명시적으로 고정했을 때 선택됩니다. 드라이버가 백슬래시를 doubling해서 서버가 다시 언이스케이프하게 합니다:

1. 백슬래시 doubling(`\` → `\\`).
2. 작은따옴표 doubling(`'` → `''`).
3. `\r`과 `\n`은 각각 백슬래시 접두(`\n` → `\\n` 등).
4. `\x1a`(Ctrl-Z)는 안전한 CUBRID 리터럴 이스케이프가 없어 **양쪽** 이스케이프 모드에서 `ProgrammingError` 발생([문자열 이스케이프](#문자열-이스케이프) 참고).
5. 결과를 작은따옴표로 감쌉니다.

`tests/test_param_security.py:27-55`와 `tests/test_aio_cursor_parity.py:87-96`로 고정됩니다.

---

## 플레이스홀더 토크나이저

`split_on_placeholders`(`pycubrid/_cursor_common.py:46-119`)는 SQL 텍스트를 파싱하고 **실행 가능한 SQL**에 나타나는 `?` 문자만을 기준으로 세그먼트를 분할합니다 — 문자열 리터럴·식별자 따옴표·주석 내부는 절대 아닙니다. 구체적으로 토크나이저는 다음을 인식하고 건너뜁니다:

- 작은따옴표 문자열 리터럴(`'...'`), doubling된 따옴표 이스케이프(`''`) 포함.
- 큰따옴표 식별자(`"..."`), doubling된 따옴표 이스케이프(`""`) 포함.
- 행 주석(`-- ... <EOL>`).
- 블록 주석(`/* ... */`).

위의 어느 것이든 내부의 `?`는 SQL 텍스트의 일부이며 플레이스홀더로 **취급되지 않습니다**. 이것은 `tests/test_split_placeholders.py` 전체로 고정됩니다.

치환 단계는 세그먼트와 렌더링된 리터럴을 연결합니다(`pycubrid/_cursor_common.py:204-208`); 순진한 `str.replace("?", ...)`를 수행하지 않으므로, 한 파라미터의 렌더링된 값에 리터럴 `?`가 있어도 다음 플레이스홀더를 실수로 소비하지 않습니다.

---

## `executemany`

DML 동사(`INSERT`, `UPDATE`, `DELETE`, `MERGE`)에 대해 `executemany`는:

1. 파라미터 행마다 `_bind_parameters(sql, params)`를 한 번 호출해 행마다 완전히 렌더링된 SQL 문자열을 만듭니다.
2. 렌더링된 SQL 문자열 리스트를 하나의 `BatchExecutePacket`으로 보냅니다(`pycubrid/cursor.py:252-257`, `pycubrid/aio/cursor.py:206-211`), `executemany`에서 `executemany_batch`로 디스패치(`pycubrid/cursor.py:217`, `pycubrid/aio/cursor.py:191`).

비-DML 문장에서는 `executemany`가 행별 `execute` 루프로 폴백합니다(`pycubrid/cursor.py:220-238`, `pycubrid/aio/cursor.py:176-187`).

각 행은 위의 동일한 타입 매핑 규칙으로 독립적으로 바인딩됩니다.

---

## 비보장과 명시적 한계

다음 동작들은 **명시적으로 계약 밖**이며 메이저 버전 없이 변경될 수 있습니다. 호출자가 암시적으로 의존하지 않도록 나열합니다.

- **서버 측 prepared-statement 바인딩 없음.** 드라이버는 클라이언트에서 파라미터를 SQL 텍스트로 렌더링합니다. CUBRID는 `execute`마다 완전한 SQL 문자열을 받습니다. 별도의 타입 파라미터 페이로드도 클라이언트 측 문장 핸들 캐시도 없습니다. 성능 특성, 쿼리 플랜 캐싱, 로그 출력이 이 설계를 반영합니다.
- **식별자는 이스케이프되지 않음.** 식별자를 보간하는 코드 경로(특히 `Cursor.callproc`, `pycubrid/cursor.py:328-336`)는 따옴표 없이 식별자를 SQL 텍스트에 박습니다. 애플리케이션은 신뢰할 수 없는 입력에서 받은 식별자를 검증해야 합니다. 파라미터 바인딩(`?`)은 **값에만** 적용되며 식별자에는 절대 적용되지 않습니다.
- **바인딩 계층의 타입 객체 동일성.** PEP 249 타입 객체(`STRING`, `BINARY` 등)는 `cursor.description`을 기술하며 파라미터 바인딩 중 참조되지 않습니다.
- **서버 측 타입 강제 변환의 정규화 없음.** 드라이버는 SQL 리터럴을 렌더링하고, CUBRID가 자기 규칙에 따라 리터럴을 대상 컬럼 타입으로 강제 변환합니다. 드라이버는 대상 컬럼에 맞춰 정밀도·스케일·문자셋을 조정하지 않습니다.
- **드라이버 강제 길이 제한 없음.** 문자열, 바이트 버퍼, 렌더링된 SQL은 Python 메모리와 CUBRID 서버 제한으로만 제한됩니다.
- **자동 `IN` 절 확장 없음.** 단일 바인딩 값으로 전달된 `list`/`tuple`/`set`은 `ProgrammingError`를 발생시킵니다. SQL 텍스트에서 플레이스홀더를 직접 펼치세요 (예: `f"... WHERE id IN ({','.join('?' * len(ids))})"`에 `params=tuple(ids)`).
- **예외 메시지.** `ProgrammingError` 메시지의 정확한 문구("unsupported parameter type", "wrong number of parameters", "string parameter contains null byte", "nan and inf are not supported by CUBRID", "parameters must be a sequence")는 계약의 일부가 아닙니다. **예외 클래스**는 계약이며, **메시지 문구**는 다듬어질 수 있습니다.
- **위에 나열되지 않은 동작적 미묘함.** [타입 매핑](#타입-매핑-보장)이나 [문자열 이스케이프](#문자열-이스케이프)에 열거되지 않은 것은 보장이 아닙니다. 공개 API 서피스 게이트(`scripts/check_public_api.py`)는 동작 드리프트를 감지하지 않습니다. 구조적 서피스 변경만 감지합니다. [`RELEASE_POLICY.md`](https://github.com/cubrid-lab/pycubrid/blob/main/RELEASE_POLICY.md) §"게이트가 감지하지 *않는* 것"을 참고하세요.

---

## 호환성 정책 (1.x)

1.x 라인 내에서 다음 변경들은 [`RELEASE_POLICY.md`](https://github.com/cubrid-lab/pycubrid/blob/main/RELEASE_POLICY.md)의 관리를 받습니다.

### 마이너(`1.y` → `1.y+1`) 릴리스에서 허용

- 새 Python 타입 지원 추가(예: `uuid.UUID`, `datetime.timedelta`) — 추가 전용이며 [타입 매핑](#타입-매핑-보장) 표에 새 행이 추가됨.
- 예외 **클래스**를 유지하면서 예외 **메시지** 개선.
- 이미 위에서 다루는 입력에 대해 생성되는 SQL 리터럴이나 발생하는 예외 클래스를 바꾸지 않는 `escape_string`, `format_parameter`, `split_on_placeholders`의 내부 리팩터링.
- 바인딩 동작을 수정하는 새 연결 플래그 추가 — 단, 기본값이 이 문서의 규칙을 보존할 때만.

### 메이저(`1.y` → `2.0`) 릴리스 필요

- [타입 매핑](#타입-매핑-보장) 표의 행 제거 또는 이름 변경.
- 표에 이미 있는 입력에 대한 SQL 리터럴 변경(예: `bytes`의 `X'<hex>'`를 다른 표현으로, 기본 `datetime` 정밀도 변경, tz 접미사 형식 변경).
- NUL / NaN / Inf 거부의 강화 또는 완화.
- `no_backslash_escapes` 기본값 변경.
- `paramstyle`을 `"qmark"`에서 변경.
- `Mapping` 파라미터 거부 변경(즉, named-parameter 지원의 재도입 또는 제거).

### 지원 중단 흐름

미래의 메이저 릴리스에서 제거될 동작은 먼저 `CHANGELOG.md > ### Deprecated`에 **deprecated**로 표시되고, 문서화된 대체 수단과 함께 메이저 버전 제거 전 최소 한 개 마이너 릴리스의 마이그레이션 기간이 주어집니다.

---

## 고정 테스트

다음 테스트 모듈이 위에 문서화된 동작을 고정합니다. 이 중 어느 것이든 CI 실패는 계약 회귀입니다:

- `tests/test_param_security.py` — 타입별 포매팅, NUL 거부, NaN/Inf 거부, datetime tz 렌더링, Decimal 처리, bytes hex 렌더링.
- `tests/test_split_placeholders.py` — 따옴표 문자열, 따옴표 식별자, 행 주석, 블록 주석 전반의 플레이스홀더 토크나이저 동작과 `_bind_parameters` 통합.
- `tests/test_cursor.py` (183-235행) — 다중 타입 파라미터 시퀀스의 종단 간 바인딩, 플레이스홀더 수 검증, `Mapping`/`str` 파라미터 거부.
- `tests/test_aio_cursor_parity.py` (76-105행) — NUL 거부, 기본 이스케이프, `no_backslash_escapes` 모드의 동기/비동기 동등성.

이 테스트들이 이 계약의 실행 가능한 명세를 제공합니다.

---

## 참고 자료

- 구현: [`pycubrid/_cursor_common.py`](https://github.com/cubrid-lab/pycubrid/blob/main/pycubrid/_cursor_common.py)
- 동기 진입점: [`pycubrid/cursor.py`](https://github.com/cubrid-lab/pycubrid/blob/main/pycubrid/cursor.py)
- 비동기 진입점: [`pycubrid/aio/cursor.py`](https://github.com/cubrid-lab/pycubrid/blob/main/pycubrid/aio/cursor.py)
- 연결 플래그: [`pycubrid/_connection_common.py`](https://github.com/cubrid-lab/pycubrid/blob/main/pycubrid/_connection_common.py)
- 릴리스 정책: [`RELEASE_POLICY.md`](https://github.com/cubrid-lab/pycubrid/blob/main/RELEASE_POLICY.md)
- 타입 시스템 (fetch / description 측): [TYPES.md](TYPES.md)
- API 참조: [API_REFERENCE.md](API_REFERENCE.md)
