# 공식 드라이버 API 목록과 호환성 설계

[기계 판독 가능한 카탈로그](https://github.com/cubrid-lab/pycubrid/blob/main/tests/fixtures/official_api_inventory.json)는
공식 [CUBRID/cubrid-python 소스 스냅샷](https://github.com/CUBRID/cubrid-python/tree/e75ec36b2a92b8829a49a967a29a1fbb9d7c322b)에
드라이버가 선언한 공개 연산을 기록합니다. 소스 참조, 시그니처, 기본값,
반환값·오류 관찰 결과, 고정된 pycubrid 기준선 7e0aad8의 대응 항목과
명시적 공백을 담습니다.
이는 [#396](https://github.com/cubrid-lab/pycubrid/issues/396) 범위에서
[#436](https://github.com/cubrid-lab/pycubrid/issues/436)이 제공하는 목록 산출물입니다.

이 문서는 소스 항목을 목록화한 것입니다. 기능 패리티, 공식 드라이버의 성공적인
실행, 완전한 API 상위 집합을 인증하지 않습니다. 비슷한 이름이나 기존 패킷
클래스만으로는 충분한 증거가 되지 않습니다.
[#438](https://github.com/cubrid-lab/pycubrid/issues/438)의 선택된 additive 설계를
아래에 기록합니다. 명시적 네임스페이스는 이제 생성·종료만 제공합니다(#465).
이 카탈로그는 후속 실행 API나 네이티브 동등성을 인증하지 않으며 기존 1.x
기본값도 바꾸지 않습니다.
카탈로그의 대상 매핑은 과거 기준선으로 유지하며 #465의 생성 기능은
이 가이드에 따로 기록합니다.

## 카탈로그 읽기

각 연산에는 안정적인 `id`가 있습니다. `CUBRIDdb`는 래퍼 연산을,
`documented_cubrid`는 네이티브 `_cubrid` 모듈의 문서화되거나 내보낸 공개 표면을
식별합니다. 두 네임스페이스는 구별됩니다. 래퍼의 `execute(query, args)`는
네이티브 prepared 커서의 `execute(option, max_col_size)`와 같지 않습니다.

`sources`는 고정된 upstream 리비전의 경로와 줄을 참조합니다. 모든 행에는
시그니처·기본값 맵, 관찰 가능한 반환값·오류 설명, 현재 pycubrid 대상 또는
명시적 부재가 있습니다. 대상은 소스 수준의 대응 항목이지 **비교 테스트의 통과**가
아닙니다. `internal_analogue`는 호환되는 공개 export 없이 대응 프로토콜 열거형만
내부에 존재한다는 뜻입니다. `tracking`은 기존 작업을 참조합니다. 호환성 결정이
논의 대상 기능을 그 자체로 구현하는 것은 아닙니다.
네이티브 오류 프로필은 코드·facility 변환과 두 요소로 이루어진 예외 인자 튜플을
별도로 기록합니다. 예외 이름이 같아도 오류 코드, 인자 또는 Python의 인자 검증
동작까지 같다고 인증할 수는 없습니다.

래퍼는 네이티브 공개 이름을 가져옵니다. 네이티브 행의 `aliases`와 최상위
재내보내기 규칙이 이를 기록하며, 별칭을 새로운 기능으로 세지 않습니다.
래퍼는 네이티브 모듈의 `connection` 이름을 자신의 팩터리로 대체합니다.
네이티브 연결 연산은 래퍼의 `connection` 속성을 통해 접근하며, 존재하지 않는
`CUBRIDdb.connection.<method>` 별칭을 통하는 것이 아닙니다.
공유 래퍼 커서 메서드는 `cursors.BaseCursor`에 기록되어 있으며,
`cursors.Cursor`와 `cursors.DictCursor`가 모두 상속합니다. 래퍼가 재내보낸 표준
라이브러리 클래스는 Python 생성자 동작을 유지합니다. 카탈로그는 상속된 모든
내장 메서드를 새로운 드라이버 연산으로 나열하지 않습니다.

## 결정 또는 구현이 필요한 차이

| 공개 표면 | 기존 pycubrid 경로 또는 미완료 작업 |
| --- | --- |
| 생성자, autocommit, 스레딩 | 네이티브 초기화는 autocommit을 켭니다. 기존 pycubrid는 수동 commit과 `threadsafety=1`을 유지합니다. #465는 명시적 CUBRID/UTF-8 생성자와 별칭만 제공하며 공유·설정·CCI URL/HA 옵션은 별도 작업입니다. |
| 문자셋, dict 커서, 변환기 | 공식 래퍼는 이 옵션들을 노출합니다. pycubrid에는 동등한 설정 가능 표면이 없습니다. 문자셋 제안 #86이 누락된 인코딩 옵션을 추적합니다. UTF-8 전용 동작을 문서화한 것은 구현 증거가 아닙니다. UTF-8 기본값은 바뀌지 않습니다. |
| Prepare/타입 지정 바인딩 | 패킷 클래스가 존재해도 공개 네이티브 prepare/bind/execute 기능은 없습니다. #418/#439, 타입 지정 컬렉션 핸들은 #440, LOB 핸들은 #441에서 추적합니다. |
| LOB 커서·파일 동작 | pycubrid는 명시적 오프셋으로 bytes를 읽고 쓰며, 공식 드라이버의 변경 가능한 위치·암묵적 생성 인터페이스와 다릅니다. seek와 읽기·쓰기 계약은 #442, 파일 가져오기·내보내기는 #443입니다. |
| 결과 탐색과 메타데이터 | 절대·상대 seek와 위치는 #444, 15필드 결과 메타데이터는 #398과 함께 #445에서 추적합니다. 네이티브 next_result는 존재하지만, 래퍼의 nextset 스텁과 pycubrid의 미지원 nextset이 그 기능을 제공하지는 않습니다. |
| 스키마 행 | #412가 스키마 결과 소비와 핸들 정리를 추적합니다. 반환된 프로토콜 패킷은 네이티브 스키마 행 반환 계약과 같지 않습니다. |
| 배치 파사드와 옵션 플래그 | `Cursor.executemany_batch(sql_list, auto_commit=None)`는 이미 임의 SQL을 배치 처리합니다. 연결 수준 파사드와 네이티브의 문장별 오류 레코드는 이 메서드의 튜플 결과·첫 오류 발생 방식과 다릅니다. 실행 플래그·쿼리 계획 옵션과 연결 멤버 설정자는 #438 아래의 집중된 후속 작업이 필요합니다. |

## 선택된 additive 계약 (#438)

2026-09-28 메인테이너가 선택한 설계는 기존 pycubrid를 유지하고 별도의
`pycubrid.compat.cubriddb`(래퍼)와 `pycubrid.compat.native`(네이티브)를 추가합니다.
#465에서 가져와 생성·종료할 수 있지만 아래의 다른 행은 아직 목표 계약입니다. 되돌릴 수 있는
이 additive 설계는 1.x 기본값 대체, 2.0 대체 도입, 릴리스 발행을 허가하지 않습니다.
전역 스위치, `CUBRIDdb`/`_cubrid` 이름 가리기, 기존
`Cursor.execute`의 오버로딩은 선택하지 않습니다. 래퍼와 네이티브의 실행 형태는
의미를 바꾸지 않고 통합할 수 없기 때문입니다. 두 표면은 별도 드라이버가 아니라
기존 순수 Python 전송 계층을 재사용해야 합니다.
후속 호환성 기능은 동일한 네이티브 연결 소유자를 확장해야 하며, 내부의 기존
드라이버 핸들을 새 공개 전송 API로 노출해서는 안 됩니다.

기존 connect/aio, user=`dba`, autocommit=False, threadsafety=1,
execute의 self 반환, 캐시된 문자열/None identity, None 크기 필드, Boolean null_ok,
정규화된 컬렉션 코드 16/17/18, raw/옵트인 타입 값, 명시적 오프셋의 bytes LOB API는
유지합니다. 기존 SQLAlchemy import와 현재 `pycubrid>=1.3.2,<2.0` 요구 범위도
유지합니다. 새 의존성이나 import 시점의 네이티브 드라이버는 필요하지 않습니다.

아래는 선택된 **목표 계약**입니다. `/`는 위치 전용 인자를 나타내며,
선택적 인자 생략과 명시적인 None은 서로 바꿔 쓸 수 없습니다.

| 표면 | 선택된 계약 / 구현 경계 |
| --- | --- |
| 팩터리 (#465) | 래퍼 `Connect/connect/connection(*args, **kwargs)`는 `Connection(dsn='', user='public', password='', charset='utf8')`에 위임합니다. 위치 인자 최대 세 개가 dsn/user/password 키워드를 덮어씁니다. 네이티브 `connect(url, user='public', passwd='')`와 소문자 connection 생성은 autocommit=True로 시작합니다. 래퍼 `.connection`은 정확히 그 호환성 네이티브 객체입니다. 생성·종료만 제공하며 초과 위치 인자·미지원 키워드/DSN 옵션을 거절합니다. 문자셋 선택·HA·커서 실행은 제공하지 않습니다. |
| 공유 / 전역 값 (향후) | 래퍼 apilevel='2.0', paramstyle='qmark', threadsafety=2에는 실제 커서와 연결별 요청·수명주기 직렬화 및 두 스레드 테스트가 먼저 필요합니다. 생성 전용 모듈은 이 전역 값을 내보내지 않습니다. 락 없는 기존 객체와 전역 threadsafety=1은 유지합니다. |
| 설정 | 네이티브 autocommit/isolation_level/lock_timeout/max_string_len 대입은 캐시 스냅샷만 바꿉니다. `set_autocommit(mode)` / `set_isolation_level(level)`은 서버 연산과 캐시 갱신을 수행하며 max_string_len은 소스의 조회 실패 시 0 폴백을 유지합니다. 래퍼 `.autocommit`은 서버에 반영되는 속성입니다. 스냅샷 멤버에 실제 서버 설정자를 만들어 붙이지 않습니다. |
| 래퍼 커서 | `cursor(dictCursor=None)`, `execute(query, args=None, set_type=None) -> int`, `executemany(query, args_list) -> None`; 튜플/dict fetch와 연결의 fetch-converter 콜백을 유지합니다. 모순되는 mapping 바인딩/default_cursor docstring은 작동하는 기능 약속이 아닙니다. |
| 네이티브 prepared 커서 | `prepare(sql) -> None`; 1부터 시작하는 인덱스의 `bind_param(index, value, bind_type=0, /) -> None`; `execute(option=0, max_col_size=0, /) -> int`; `fetch_row(how=0, /)`는 튜플/dict 또는 None입니다. 옵션 기본값은 docstring QUERY_ALL이 아니라 파싱된 0이며 #418/#439가 코어를 구현합니다. |
| Description | `(name, native_type, 0, 0, precision, scale, null_ok)`에서 null_ok는 정수 0/1이고 precision은 쿼리별 값이며 네이티브 플래그 타입을 유지합니다. 값과 Python 타입을 함께 검증하고 컬렉션 16→32를 무조건 변환하지 않습니다. |
| 확장 메타데이터 | `result_info(n=0, /)`는 15필드 튜플들의 튜플(n>=1도 바깥 항목 하나)을 반환하며 컬럼이 없으면 None입니다. 실제 순서는 type, not_null, scale, precision, name, attribute, class, default, auto_increment, unique, primary, foreign, reverse_index, reverse_unique, shared입니다. 빈 값과 없는 메타데이터를 구별하고 #445가 없는 필드를 만들어 내지 않아야 합니다. |
| 컬렉션 | 저장된 SET의 목표는 변경 가능한 set, MULTISET/SEQUENCE는 list입니다. 비NULL 요소는 검증된 타입별 텍스트 변환을 사용하고 중복·순서·빈 값을 보존합니다. 전체 SQL NULL과 NULL 요소는 아래 안전성 차이에 따라 None입니다. 중괄호 리터럴은 저장된 SET의 증거가 아니며 타입 지정 import/bind는 #440입니다. |
| Identity / 스키마 | 네이티브 `insert_id() -> int \| None`은 기존 INSERT 캐시의 형변환이 아니라 현재 브로커 identity를 조회합니다. `schema_info(schema_type, class_name, attr_name 생략, /)`는 키워드/플래그/명시적 None 없이 첫 행의 list 또는 None을 반환합니다. CLASS/VCLASS 플래그 1, ATTRIBUTE/CLASS_ATTRIBUTE 2, 나머지 0을 추론합니다. #456의 전체 소비·정리가 제공되면 재사용하며 기존 소비 API는 계속 모든 행을 반환합니다. |
| 네이티브 LOB | 처음에는 값이 없는 별도의 변경 가능한 바이트 위치 객체입니다. `write(string, type 생략, /) -> None`은 str/bytes(str은 UTF-8)를 받고 기본 BLOB 또는 요청된 B/C를 생성합니다. `read(len=0, /) -> str`은 생략/0에서 남은 바이트를 읽고 엄격한 UTF-8로 디코딩합니다. `seek(offset, whence=SEEK_CUR, /) -> int`의 SEEK_END는 size-offset입니다. #442/#443이 수명주기·짧은 전송·파일을 담당하며 기존 bytes 메서드를 대체하지 않습니다. |
| 예외 | 네임스페이스별 PEP 249 어댑터는 `(numeric_code, formatted_message)` args와 code/errno/SQLSTATE 증거를 유지하며 기존 예외 identity/args는 바꾸지 않습니다. 불안정한 메시지의 완전 일치나 네이티브 인자 파서 충돌은 목표가 아닙니다. |

[#418 타입 지정 CAS 설계](../PREPARED_BINDING_DESIGN.md)는 향후 동기 호환성
prepared 커서의 첫 스칼라 범위(#439), FC2/FC3/FC6 형식, 핸들·결과·트랜잭션 소유권과
statement pooling이 켜진 환경에서 측정한 경계를 명시합니다. 아직 실행 API나
공식 드라이버 전체 패리티의 증거가 아니며, 기존 1.x 리터럴 바인딩은 유지됩니다.

### 증거와 의도적인 안전성 차이

소스 계약은 모순되는 docstring보다 고정된 실제 구현을 따릅니다.
[래퍼 생성](https://github.com/CUBRID/cubrid-python/blob/e75ec36b2a92b8829a49a967a29a1fbb9d7c322b/CUBRIDdb/connections.py#L15),
[커서 어댑터](https://github.com/CUBRID/cubrid-python/blob/e75ec36b2a92b8829a49a967a29a1fbb9d7c322b/CUBRIDdb/cursors.py#L234),
[네이티브 구현](https://github.com/CUBRID/cubrid-python/blob/e75ec36b2a92b8829a49a967a29a1fbb9d7c322b/cubrid_ext/python_cubrid.c#L1323)을
참조하며 CCI gitlink는 `7d1eb8f40f04089b8218d08e36e2c24a2de11b24`입니다.

메인테이너 로컬 증거 `native-e75-oracle.y5UnxU`에는 README,
`observations.jsonl`, `verify_observations.py`가 있습니다. 정확한 e75+7d 빌드와
Python 3.10.12로 CUBRID 10.2.18.9024/11.4.6.1963의 정적 SELECT에서 타입을 기록한
33개 레코드입니다. 스칼라 튜플 행, 정수 null_ok, 메타데이터 컨테이너,
초기 autocommit=True를 측정했습니다. 컬렉션 리터럴은 문자열 list와 코드
104/96 또는 72/64였으며 저장된 SET이나 쿼리 독립적인 precision의 증거는 아닙니다.
저장된 컬럼·LOB·schema_info·문자셋/HA·실패 동작은 이 oracle이 검증하지 않았고,
#446이 더 넓고 이식 가능한 차분 증거를 추적합니다. 이 문서는 네이티브 실행을
재수행하거나 패리티를 인증하지 않습니다.

- 네이티브의 직접 `bind_param(None)`은 SystemError였습니다. 뒤에 바인딩되지 않은
  NULL 슬롯을 실행한 것은 명시적 NULL 바인딩 성공이 아닙니다. 목표 코어는 SQL NULL을
  안전하게 바인딩하며, 미완성 상태의 실행 전 명확한 거절도 기능 완성은 아닙니다.
- 네이티브 컬렉션 NULL은 ''가 되었습니다. None과 진짜 빈 텍스트를 구별하고,
  기존 디코딩 값의 `str()`로 네이티브 텍스트를 재구성하지 않습니다. 타입 지정 import는
  명시적인 요소 타입과 SQL NULL의 None을 사용하며 손실되는 문자열 sentinel은 안 됩니다.
- LOB 위치는 실제 전송량만큼 이동하며 짧은 write는 실패합니다. 요청 길이만큼
  이동하거나 위험한 짧은 read 버퍼 동작을 복제하지 않고 기존 #394 안전성을 유지합니다.
- 초과 팩터리 인자나 미지원 mapping 호출을 조용히 버리거나 키를 바인딩 값으로
  취급하지 말고 명확히 거절합니다.

이는 명시적 차분 분류이지 포괄적 면제나 "미지원이므로 완료" 항목이 아닙니다.
영향 없는 동작은 정확한 비교가, NULL·데이터 보존은 테스트가 필요합니다. 저장된
타입 증거가 없으면 그 영역을 인증할 수 없지만 독립적인 prepared 코어 작업은 가능합니다.

### 이행 목표와 작은 구현 단위의 수용 기준

실제 쿼리에는 기존 import를 유지하세요. 새 모듈은 아직 연결 생성·종료만
제공합니다. 후속 기능이 갖춰지면 래퍼 이행은
`import CUBRIDdb` → `from pycubrid.compat import cubriddb as CUBRIDdb`, 네이티브는
`import _cubrid` → `from pycubrid.compat import native as _cubrid`입니다.
수동 트랜잭션에서 래퍼는 `conn.autocommit = False`, 네이티브는
`conn.set_autocommit(False)`를 사용해야 합니다. 네이티브 멤버 대입은 스냅샷만
바꿉니다. NULL 사용자는 ''-as-NULL 비교를 `is None`으로 바꾸되 진짜 빈 문자열은
보존하세요. 바이너리 LOB에는 호환성 Unicode read가 아니라 기존 bytes API를 유지합니다.

| 잠정 구현 단위 | 수용 기준 / 디펜던시 |
| --- | --- |
| M 팩터리 / M 공유 | #465는 명시적 생성·종료, 별칭, DSN/user/autocommit 기본값과 래퍼-네이티브 관계를 제공하면서 기존 동작을 유지합니다. 공유·수명주기 직렬화는 별도이며 threadsafety=2를 게이팅합니다. prepared 엔진이나 전역 스위치는 제공하지 않습니다. |
| M prepared / 타입 바인딩 | #439는 #418과 이 계약 뒤에 count/반환/위치 인자/NULL/오류를 검증하고 #440 컬렉션과 #441 LOB 바인딩이 코어 뒤를 따릅니다. |
| M 변환 / 문자셋 / HA | 별도 dictCursor/converter 단위, #86의 실제 인코딩, 실제 failover 증거를 가진 HA/URL 옵션 단위입니다. 옵션 파싱이나 upstream default_cursor 스텁만으로는 미완료입니다. |
| S 배치 / 예외 / identity | 기존 임의 SQL 전송을 이용하는 네이티브 배치 레코드, 네임스페이스 예외/export 어댑터, fresh/트랜잭션/CALL/non-auto 제어를 갖춘 브로커 identity를 각각 분리합니다. 기존 첫 오류·캐시 문자열 동작은 유지합니다. |
| M 설정 / 탐색 | 실제 서버 연산과 네 캐시 멤버를 구별합니다. #444 seek/position과 별도 next_result/실행 옵션/쿼리 계획 단위이며 스텁·플래그만으로 완성하지 않습니다. |
| S/M 메타데이터 / 스키마 / LOB | #445 값·타입·15필드, #412/#455–457 소유한 스키마 재사용, #442 실제 바이트 위치·짧은 전송, #443 파일입니다. 하나의 거대한 파사드 PR이 아닌 집중된 작업입니다. |
| M 차분 증거 | #446/#351은 고정 네이티브 리비전에서 제공된 각 단위를 비교하며 저장된 컬렉션 NULL/빈 값/순서/중복, LOB UTF-8 실패와 기존 SQLAlchemy smoke를 포함합니다. 소스 목록은 패리티 통과가 아닙니다. |

#465 기반 구현은 서브모듈의 명시적 `__all__`, 기존 공개 API 검사기의
추적 모듈/클래스와 RELEASE_POLICY §1 확장, baseline 재생성을 함께 제공합니다.
후속 API 단위도 baseline을 갱신해야 합니다. 새 root 별칭이나 async 변경은
없습니다. 새 명시적 API는 MINOR 추가이고 기존 약속의 수정은 PATCH입니다.
#438은 설계를 선택했고 #465는 생성만 제공하며 나머지 하위 기능은 아직 없습니다.
#396은 범위 내 기능과 검증이 완료될 때까지 열어 둡니다.

## 소스의 불일치는 패리티 목표가 아님

카탈로그는 다음 불일치를 조용히 정규화하지 않고 보존합니다.

- 래퍼의 `callproc()`와 `nextset()`은 설명적인 docstring과 달리 스텁입니다.
- `result_info()`는 첫 필드들의 순서를 docstring과 다르게 구현합니다.
  기록된 순서는 고정 소스에서 튜플을 구성하는 방식을 따릅니다.
- 네이티브 execute는 옵션 기본값을 `0`으로 파싱하지만 docstring은 QUERY_ALL을
  설명합니다. `bind_param`은 문서의 `None`이 아니라 정수 기본값 `0`을 파싱합니다.
- 네이티브 schema_info는 class_name을, Set.imports는 type 인자를 요구합니다.
  문서의 선택적 인자·기본값 설명과 다릅니다.
- schema-info는 문서의 튜플 설명과 달리 행 리스트를 구성합니다.
  네이티브 insert_id는 pycubrid의 문자열 편의 API와 달리 정수를 반환합니다.
- 패키지 `__all__`은 패키지 할당이 확인되지 않은 이름도 광고합니다.
  카탈로그는 광고된 export와 실제 작동하는 연산을 구분합니다.
- 래퍼 문자셋 변환, 컬렉션 요소 추론, LOB의 짧은 전송량 처리·텍스트 디코딩,
  선택된 LOB 컬럼 처리는 독립적인 계약 증거가 필요합니다.
  소스 관찰은 새로 실행해 확인한 결함 주장과 같지 않습니다.

실제로 내부적인 세부 사항에는 명시적 제외 이유가 있습니다. 예를 들어
`_set_charset_name`은 내부용이며 사용자가 호출하면 안 된다고 명시합니다.
하지만 래퍼의 공개 문자셋 기능은 공백으로 계속 포함됩니다. 네이티브 ABI 배치,
CCI 포인터, 할당자·소멸자, 메모리 주소 형식은 복사하지 않습니다.
어떤 연산도 C로 구현되었다는 이유만으로 제외되지 않습니다.

## 검증과 출처

오프라인 일관성 검사는 다음과 같이 실행합니다.

```bash
python -m pytest tests/test_official_api_inventory.py -q
```

이 검사는 네이티브 드라이버를 가져오거나 데이터베이스에 연결하지 않고,
매핑을 패리티 증거로 바꾸지 않은 채 목록과 현재 명명된 대상을 검증합니다.
실제 소스 시나리오 목록화와 공식 드라이버 비교 증거는 별도의 산출물입니다
(#437/#446).

공식 드라이버의 메인테이너와 기여자에게 감사드립니다. 여기의 설명은 고정
소스에서 독립적으로 풀어 쓴 것으로, upstream 소스나 docstring을 통째로
복사하지 않았습니다. 기존 [NOTICE](https://github.com/cubrid-lab/pycubrid/blob/main/NOTICE)와
[제3자 참조 기록](https://github.com/cubrid-lab/pycubrid/blob/main/THIRD_PARTY_LICENSES.md#reference-test-suite)이
출처와 라이선스 한계의 기준으로 유지됩니다. 이 카탈로그는 새로운 라이선스
추론, 런타임 의존성 또는 upstream 소스 재사용 허가를 추가하지 않습니다.
