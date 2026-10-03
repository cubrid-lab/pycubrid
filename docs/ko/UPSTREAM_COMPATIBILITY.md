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
아래에 기록합니다. 명시적 네임스페이스는 아래의 제한된 구현 범위
(#465/#439/#440/#441/#442/#467)를 제공합니다. 이 카탈로그는 남은 API나
완전한 네이티브 동등성을 인증하지 않으며 기존 1.x
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
| 문자셋, dict 커서, 변환기 | #86부터 일반·비동기·호환 생성자가 선택한 코덱을 받아 EUC-KR 데이터베이스에서도 검증했습니다. 연결에 `charset` 속성은 유지하지 않습니다. #466은 기존 네이티브 스칼라 범위 위에 한정된 튜플/dict 커서와 연결별 변환기를 추가합니다. UTF-8 기본값은 바뀌지 않습니다. |
| Prepare/타입 지정 바인딩 | 일반 커서에는 공개 prepare/bind/execute가 없습니다. 명시적 `compat.native` 커서는 동기 스칼라 바인딩(#439)과 컬렉션 바인딩(#440), 조회한 LOB 핸들의 조회·바인딩(#441)을 제공합니다. |
| LOB 커서·파일 동작 | pycubrid는 명시적 오프셋으로 bytes를 읽고 쓰며, 공식 드라이버의 변경 가능한 위치·암묵적 생성 인터페이스와 다릅니다. seek와 읽기·쓰기 계약은 #442, 파일 가져오기·내보내기는 #443입니다. |
| 결과 탐색과 메타데이터 | 절대·상대 seek와 위치는 #444, 15필드 결과 메타데이터는 #398과 함께 #445에서 추적합니다. 네이티브 next_result는 존재하지만, 래퍼의 nextset 스텁과 pycubrid의 미지원 nextset이 그 기능을 제공하지는 않습니다. |
| 스키마 행 | #412가 스키마 결과 소비와 핸들 정리를 추적합니다. 반환된 프로토콜 패킷은 네이티브 스키마 행 반환 계약과 같지 않습니다. |
| 배치 파사드와 옵션 플래그 | `Cursor.executemany_batch(sql_list, auto_commit=None)`는 이미 임의 SQL을 배치 처리합니다. 연결 수준 파사드와 네이티브의 문장별 오류 레코드는 이 메서드의 튜플 결과·첫 오류 발생 방식과 다릅니다. 실행 플래그·쿼리 계획 옵션과 연결 멤버 설정자는 #438 아래의 집중된 후속 작업이 필요합니다. |

## 선택된 additive 계약 (#438)

2026-09-28 메인테이너가 선택한 설계는 기존 pycubrid를 유지하고 별도의
`pycubrid.compat.cubriddb`(래퍼)와 `pycubrid.compat.native`(네이티브)를 추가합니다.
#465의 생성·종료, 네이티브 prepared 스칼라(#439), 컬렉션 바인딩(#440),
LOB 핸들·스트림(#441/#442), 캐시와 실제 설정자(#467)를 제공하지만
아래의 다른 행은 아직 목표 계약입니다. 되돌릴 수 있는
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
| 팩터리 (#465) | 래퍼 `Connect/connect/connection(*args, **kwargs)`는 `Connection(dsn='', user='public', password='', charset='utf8')`에 위임합니다. 위치 인자 최대 세 개가 dsn/user/password 키워드를 덮어씁니다. 네이티브 `connect(url, user='public', passwd='')`와 소문자 connection 생성은 autocommit=True로 시작합니다. 래퍼 `.connection`은 정확히 그 호환성 네이티브 객체입니다. 생성·종료, #467 autocommit 접근, #86 문자셋과 #466 한정된 행 커서를 제공하며 HA·미지원 DSN 옵션은 거절합니다. |
| 공유 / 전역 값 (향후) | 래퍼 apilevel='2.0', paramstyle='qmark', threadsafety=2에는 실제 커서와 연결별 요청·수명주기 직렬화 및 두 스레드 테스트가 먼저 필요합니다. 현재의 제한된 호환 모듈은 이 전역 값을 내보내지 않습니다. 락 없는 기존 객체와 전역 threadsafety=1은 유지합니다. |
| 설정 (#467) | 네이티브의 `autocommit`, `isolation_level`, `lock_timeout`, `max_string_len`은 직접 대입이 브로커 요청을 보내지 않는 쓰기 가능한 캐시이며, 실제 설정은 별도 `set_autocommit(mode, /)` / `set_isolation_level(level, /)`로 바꿉니다. bool autocommit 설정자는 로컬 CCI 대응 모드를 바꾸며, 실제 모드가 바뀌고 활성 트랜잭션이 있을 때만 커밋합니다. 격리 수준 4/5/6의 SET은 커밋하지 않으며 같은 물리 세션의 실제 수준이 같으면 생략합니다. 래퍼 `.autocommit`의 세터는 bool을 검증·위임하고 getter는 원시 캐시를 읽습니다. 생성자는 lock/max/isolation의 실제 값을 읽고 max-string 서버의 완전한 오류에만 0을 적용하며, 숫자 수준 4의 초기 문자열은 공식 확장의 `CUBRID_TRAN_UNKNOWN_ISOLATION` 표기를 유지합니다. lock/max 실제 세터·일반 기본값 변경·위험한 네이티브 파서 패리티는 없습니다. |
| 래퍼 커서 (#466) | `pycubrid.compat.cursors.Cursor/DictCursor`와 `Connection.cursor(dictCursor=None)`를 제공합니다. `execute(query, args=None, set_type=None) -> int`는 기존 네이티브 INT32/문자열/NULL 스칼라만 위임하며 `set_type`의 non-None 값과 매핑은 I/O 전에 거부합니다. SELECT의 7필드 설명, 정확한 이름의 튜플/dict 행(중복 키는 마지막 값), 현재 연결별 변환기를 제공합니다. 거짓 변환 결과는 행을 소비한 뒤 bulk fetch를 멈추고 반복자는 None에서만 멈춥니다. `executemany`, 컬렉션/LOB 인자, mapping/default_cursor docstring은 미제공입니다. 비SELECT 설명은 공식 확장의 누락 가능성 대신 안정적인 None을 사용합니다. |
| 네이티브 prepared 커서 | `prepare(sql) -> None`; 1부터 시작하는 인덱스의 `bind_param(index, value, bind_type=0, /) -> None`; `execute(option=0, max_col_size=0, /) -> int`; `fetch_row(how=0, /)`는 튜플/dict 또는 None입니다. 옵션 기본값은 docstring QUERY_ALL이 아니라 파싱된 0이며 #418/#439가 코어를 구현합니다. |
| Description | `(name, native_type, 0, 0, precision, scale, null_ok)`에서 null_ok는 정수 0/1이고 precision은 쿼리별 값이며 네이티브 플래그 타입을 유지합니다. 값과 Python 타입을 함께 검증하고 컬렉션 16→32를 무조건 변환하지 않습니다. |
| 확장 메타데이터 (#445) | 네이티브 전용 `cursor.result_info([n])`는 성공한 실행의 캐시된 메타데이터를 읽어 15필드 튜플들의 튜플(1부터 시작하는 번호는 바깥 항목 하나)을 반환하며, 실행한 컬럼 없는 문장은 None을 반환합니다. 실제 CCI 타입·정수 플래그·전달된 문자열을 공식 구현 순서로 사용합니다. 지원 CUBRID 생산자는 없는 텍스트를 None이 아닌 빈 문자열로 보내며 속성·기본값을 추측하지 않습니다. 일반·래퍼 description과 행 위치는 바뀌지 않습니다. |
| 네이티브 컬렉션 바인딩 (#440) | 동기 전용으로 제공됩니다. `connection.set() -> set`, `set(connection, /)`, `set.imports(data, type, /, *, kind=SET) -> None`, `cursor.bind_set(index, set, /) -> None`. `data`는 tuple이어야 하고(아니면 InterfaceError) `type`은 BIT/VARBIT(NotSupportedError)을 제외한 모든 int 타입 코드이며, 공식 드라이버처럼 import의 표시일 뿐입니다. 공식 드라이버처럼 모든 원소는 `type`과 관계없이 STRING(2)으로 보내며, 기본 종류 SET은 공식 바이트를 보냅니다. `kind=MULTISET`은 중복을 유지하고(브로커가 MULTISET을 -454로 거부하므로 SEQUENCE로 보내며, 컬럼은 순서를 유지하지 않음) `kind=SEQUENCE`는 중복과 순서를 유지합니다. None이 NULL 원소이며, `'NULL'`, `''`, Python int 원소(INT 전용), NUL 거부, #439 오류 클래스는 의도적으로 분류된 차이입니다. import하지 않은 set은 SQL NULL을 바인딩합니다. 래퍼 `execute(..., set_type)`/`executemany` 컬렉션 형태는 제공하지 않습니다(#610). |
| 컬렉션 | 저장된 SET의 목표는 변경 가능한 set, MULTISET/SEQUENCE는 list입니다. 비NULL 요소는 검증된 타입별 텍스트 변환을 사용하고 중복·순서·빈 값을 보존합니다. 전체 SQL NULL과 NULL 요소는 아래 안전성 차이에 따라 None입니다. 중괄호 리터럴은 저장된 SET의 증거가 아니며 네이티브 타입 지정 import/bind는 위의 #440 행입니다. 일반 커서용 `pycubrid.types.Set`/`Multiset`/`Sequence` 파라미터(#567)에는 공식 대응물이 없습니다. 공식 wrapper `execute(query, args, set_type)`는 일반 list를 네이티브 prepared `bind_set`으로 바인딩하며 pycubrid는 이를 제공하지 않으므로, 이 파라미터에 대해서는 차등 비교 주장을 하지 않습니다. |
| Identity / 스키마 | 네이티브 `insert_id() -> int \| None`은 기존 INSERT 캐시의 형변환이 아니라 현재 브로커 identity를 조회합니다. `schema_info(schema_type, class_name, attr_name 생략, /)`는 키워드/플래그/명시적 None 없이 첫 행의 list 또는 None을 반환합니다. CLASS/VCLASS 플래그 1, ATTRIBUTE/CLASS_ATTRIBUTE 2, 나머지 0을 추론합니다. #456의 전체 소비·정리가 제공되면 재사용하며 기존 소비 API는 계속 모든 행을 반환합니다. |
| 네이티브 LOB 핸들 조회·바인딩 (#441) | 동기 전용으로 제공됩니다. `connection.lob() -> lob`, `lob(connection, /)`, `lob.close() -> None`(로컬 전용, 서버 해제 요청 없음, 그 전에 만든 바인딩은 유효), `cursor.fetch_lob(col, lob, /) -> None`, `cursor.bind_lob(index, lob, /) -> None`. `fetch_lob`은 `fetch_row`처럼 다음 행을 소비하고 BLOB/CLOB 타입을 `col`에서 정합니다(공식은 1번 컬럼을 읽지만 10.2/11.4에서 저장되는 복사본은 같음). int가 아닌 `col`은 공식 인자 파서처럼 먼저 TypeError를 내고, 결과 끝에서는 공식과 같이 컬럼 범위·타입이나 lob 상태를 검사하기 전에 None을 반환하며, 그 밖에는 LOB가 아니거나 범위를 벗어난 컬럼이 행을 소비하지 않고 ProgrammingError를 내며, NULL 셀은 공식과 같이 행을 소비하고 lob을 비웁니다. `bind_lob`은 공식과 같이 lob이 아니면 TypeError를 내고, 공식과 같이 커밋된 행에서 조회한 핸들을 다시, 다른 연결에서, 원래 연결이 닫히거나 재접속한 뒤에도 바인딩합니다(서버가 복사본 저장). 닫히거나 빈 lob은 요청 전에 InterfaceError를 내며(공식은 NULL 바인딩), 닫힌 lob이나 다른 연결의 lob으로 `fetch_lob`하면 InterfaceError입니다(공식은 채움). int가 아닌 인덱스는 공식과 같이 lob보다 먼저 TypeError를 내고, 범위를 벗어난 인덱스는 ProgrammingError입니다. 완전한 응답의 셀이 컬럼 LOB 타입의 핸들이 아니면 행을 소비하지 않고 DataError를 내며, 핸들 구조가 손상되었으면 OperationalError를 내고 세션을 폐기합니다. 각 lob은 출처(fetched/created)를 기록하므로 #442가 생성한 임시 핸들을 자기 세션에 묶을 수 있고, 실제 autocommit 모드에서 조회했는지도 기록합니다. 수동 모드에서 조회한 핸들은 나중에 커밋해도 다른 연결로 넘기지 않으며, autocommit 모드에서 다시 조회해야 합니다. 스트림 작업은 아래 #442에서 제공하며, 파일 입출력은 #443으로 남습니다. |
| 네이티브 LOB 스트림 (#442) | 동기 전용으로 `lob.write(data, type="B", /) -> None`, `read(length=0, /) -> str`, `seek(offset, whence=SEEK_CUR, /) -> int`와 `SEEK_*` 상수를 제공합니다. 위치와 크기는 바이트 단위이고 BLOB/CLOB 모두 UTF-8 텍스트를 반환하며 SEEK_END는 크기-offset입니다. 첫 쓰기는 BLOB/CLOB을 만들고 이후 쓰기는 끝에만 추가합니다. 안전상 중간 쓰기·유효하지 않은 위치는 요청 전에 거부하고 빈 값·EOF·초과 읽기는 공식 CCI의 위험한 동작 대신 가능한 문자열을 반환합니다. 네이티브 객체의 close는 계속 종료 상태이고, 생성 핸들은 원래 세션에 묶이며 첫 autocommit 바인딩이 임시 파일을 소비합니다. 일반 절대 오프셋 `Lob`과 파일 입출력(#443)은 변하지 않습니다. |
| 예외 | 네임스페이스별 PEP 249 어댑터는 `(numeric_code, formatted_message)` args와 code/errno/SQLSTATE 증거를 유지하며 기존 예외 identity/args는 바꾸지 않습니다. 제공된 result_info의 로컬 오류는 아래에 분류한 pycubrid 메시지 하나의 args를 유지하며 더 넓은 어댑터는 별도 작업입니다. 불안정한 메시지의 완전 일치나 네이티브 인자 파서 충돌은 목표가 아닙니다. |

#445 메타데이터 기능은 다른 docstring이 아닌 고정된
[튜플 생성 구현](https://github.com/CUBRID/cubrid-python/blob/e75ec36b2a92b8829a49a967a29a1fbb9d7c322b/cubrid_ext/python_cubrid.c#L2119-L2213)을
따릅니다. 새 커서·준비만 한 커서·잘못된 번호 오류의 `.code`는 -30006이고,
닫힌 커서는 -30019입니다. 기존 pycubrid의 메시지 하나인 `args`는 공식의
정수·메시지 쌍과 다르며 별도 차등 비교 주장에 기록합니다. 메시지에서 코드를
파싱하거나 errno를 만들지 않고 정수 변환·닫힘·키워드 우선순위를 측정합니다.
캐시된 메타데이터는 EOF와 동일 소유자 commit·rollback 이후에도 유지되며 행
무효화와 별개입니다. 실행 시도 실패 뒤 이전 메타데이터를 숨기고, 끊긴·다른·
오래된 소유자는 CCI 포인터의 위험 대신 안전하게 실패합니다. 텍스트는 연결
코덱을 유지하며 고정 UTF-8 비교로 비UTF-8 동등성을 주장하지 않습니다.
[API 설명](API_REFERENCE.md#확장-컬럼-메타데이터-result_info)을 참고하세요.

[#418 타입 지정 CAS 설계](../PREPARED_BINDING_DESIGN.md)는 향후 동기 호환성
prepared 커서의 첫 스칼라 범위(#439), FC2/FC3/FC6 형식, 핸들·결과·트랜잭션 소유권과
statement pooling이 켜진 환경에서 측정한 경계를 명시합니다. 아직 실행 API나
공식 드라이버 전체 패리티의 증거가 아니며, 기존 1.x 리터럴 바인딩은 유지됩니다.

#611의 준비 핸들 보정은 CCI의 같은 호출 내 invalid-plan 재시도보다
의도적으로 좁습니다. 완전한 브로커 실행 오류가 발생한 뒤 **다음 명시적인
사용자 execute**에서만 LOB이 없는 핸들을 원래 세션에서 다시 준비하고 FC3을
한 번 보냅니다. 원래 오류는 그대로 전달합니다. 이미 효과가 생겼을 수도
있는 문장을 실패한 호출 안에서 재실행하지 않으며 전송 오류·LOB 바인딩은
자동 재시도하지 않습니다. 구형 wire 오류 `-1024`는 CCI에서 정규화한
`CAS_ER_STMT_POOLING=-10024`이고 이 프로젝트의 오래된
`ER_STMT_POOLING=-15` 상수와 다릅니다. 정수 코드만으로 재실행하지 않습니다.
근거는 고정된 [CCI 재시도 경로](https://github.com/CUBRID/cubrid-cci/blob/7d1eb8f40f04089b8218d08e36e2c24a2de11b24/src/cci/cas_cci.c#L1411-L1424)와
실행 진입 뒤에도 pooling 오류를 낼 수 있는 [10.2](https://github.com/CUBRID/cubrid/blob/v10.2.13.8953/src/broker/cas_execute.c#L1265-L1275)·[11.4](https://github.com/CUBRID/cubrid/blob/v11.4.6.1963/src/broker/cas_execute.c#L1647-L1689)
브로커 경로입니다.

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

### 공식 드라이버 차분 게이트 (#446)

pycubrid가 주장하는 공식 동작은
[`tests/fixtures/official_differential_claims.json`](https://github.com/cubrid-lab/pycubrid/blob/main/tests/fixtures/official_differential_claims.json)에
기록합니다. 각 주장은 `match`(공식 드라이버와 Python 타입·값이 같음) 또는
`deviation`(이유, 담당 이슈, 두 드라이버의 정확한 관측값)이며, 검증하는 인벤토리
연산과 upstream 시나리오 id를 가집니다. 각 주장은 `tests/test_official_differential.py`의
라이브 케이스 하나와 연결되고, 이 케이스는 같은 서버에서 같은 SQL·값·autocommit
설정을 pycubrid와 공식 드라이버로 실행합니다. 불일치하면 실패하고, 분류된 차이도
어느 한쪽 관측값이 바뀌면 실패합니다.

**오라클 출처.** `scripts/build_official_oracle.py`는 cubrid-python
`e75ec36b2a92b8829a49a967a29a1fbb9d7c322b`를 가져와 `cci-src` gitlink가
`7d1eb8f40f04089b8218d08e36e2c24a2de11b24`인지 확인한 뒤 그 CCI 커밋을 가져옵니다.
CCI 정적 라이브러리는 CCI가 추적하는 번들 OpenSSL 1.1.1f 라이브러리로 CMake를
직접 사용해 빌드하고, 변경하지 않은 `cubrid_ext/python_cubrid.c`를 컴파일합니다.
이전 메인테이너 로컬 #439 빌드와 달리 upstream `setup.py`/`build_cci.sh` 래퍼를
실행하지 않고 upstream 파일을 패치하지 않습니다. upstream `version.h` 템플릿만
소스 트리 밖에서 생성합니다. 두 커밋, 도구 체인, 확장 모듈 SHA-256은 `oracle.json`에
기록됩니다. Linux x86_64만 지원하며 TLS는 이 오라클로 검증하지 않습니다.

**필수 레인.** 일반 CI의 `official-differential` 작업은 CI Gate에 포함되며 문서
전용 변경일 때만 건너뜁니다. Python 3.10으로 CUBRID 10.2와 11.4를 대상으로 실행하고,
두 고정값을 담은 빌드 스크립트 해시를 키로 오라클 빌드를 캐시합니다. 같은 작업이
nightly와 릴리스 전체 매트릭스도 막습니다. `PYCUBRID_OFFICIAL_ORACLE_REQUIRED=1`에서는
다음 경우 건너뛰지 않고 실패합니다.

- 드라이버나 매니페스트가 없거나, 확장 모듈 해시가 매니페스트와 다른 경우
- 케이스가 0개이거나, 어느 한 서버에서 결과가 없는 주장이 있는 경우
- 불일치하거나, 분류되지 않은 차이가 있는 경우

레인 감사(`check_integration_lanes.py --lane official`)는 모든 skip을 거부합니다.
케이스별 JSON Lines 증거와 요약은 `official-differential-evidence` 아티팩트로
업로드됩니다. 증거에는 Python·서버·드라이버 버전, pycubrid 커밋, 모든 관측값이
기록됩니다. `PYCUBRID_OFFICIAL_ORACLE_REQUIRED=1`이 없으면(오프라인, 다른 통합 레인,
로컬 실행) 다른 `CUBRIDdb` 빌드를 가져올 수 있더라도 모듈을 건너뛰며, 이런 실행은
주장을 인증하지 않습니다. 증거 검사는 기록된 결과 표시를 믿지 않고, 기록된
관측값과 원장으로 각 결과를 다시 계산합니다.

아래 수치는 원장에서 생성되므로 직접 수정하지 마세요. 주장을 바꾼 뒤
`python scripts/check_official_differential.py --write-docs`를 실행합니다. 오프라인
검사는 오래된 수치, 알 수 없는 인벤토리·시나리오 id, 케이스 없는 주장, 빌드
스크립트와 다른 오라클 고정값을 실패로 처리합니다.

<!-- official-differential-summary:start (generated by scripts/check_official_differential.py --write-docs) -->

| 표면 | 일치 | 분류된 차이 | 합계 |
| --- | ---: | ---: | ---: |
| 래퍼 (`CUBRIDdb`) | 13 | 2 | 15 |
| 네이티브 (`_cubrid`) | 27 | 12 | 39 |
| **합계** | **40** | **14** | **54** |

- 오라클: cubrid-python `e75ec36b2a92`, CCI `7d1eb8f40f04`, Python 3.10
- 필수 서버: CUBRID 10.2, CUBRID 11.4
- 분류된 차이: `fetch-monetary` (#344), `description-size-and-null-ok` (#438), `prepared-bind-null` (#439), `bind-multiset-duplicates` (#440), `bind-sequence-order` (#440), `bind-set-null-text` (#440), `bind-set-empty-string` (#440), `bind-set-python-int` (#440), `bind-set-nul-truncation` (#440), `bind-set-error-classes` (#440), `lob-bind-without-value` (#441), `lob-error-classes` (#441), `lob-fetch-into-closed-or-foreign` (#441), `native-result-info-error-args` (#445)

<!-- official-differential-summary:end -->

이 주장들은 측정한 제한된 범위입니다. 저장된 스칼라 조회, 정적 스칼라 행과
description, #439 prepared INT/문자열 부분집합, #440 네이티브 컬렉션 바인딩,
#441/#442 네이티브 LOB 핸들·스트림, #467 캐시와 안전한 실제 설정자 값을
다룹니다. 래퍼 컬렉션 형태, LOB 파일 입출력, 문자셋/HA,
실패 동작과 나머지 인벤토리 연산은 여기에 주장이 생길 때까지 인증되지 않습니다.
주장 수는 패리티 비율이 아닙니다.

### 이행 목표와 작은 구현 단위의 수용 기준

실제 쿼리에는 기존 import를 유지하세요. 명시적 네이티브 모듈은 제한된
동기 prepared·LOB 실행과 #467 설정을 지원하며, 래퍼는 연결 생성·종료와
autocommit 위임과 #466의 한정된 네이티브 스칼라 기반 행 커서를 제공합니다. 이 범위의 래퍼 이행은
`import CUBRIDdb` → `from pycubrid.compat import cubriddb as CUBRIDdb`, 네이티브는
`import _cubrid` → `from pycubrid.compat import native as _cubrid`입니다.
수동 트랜잭션에서 래퍼는 `conn.autocommit = False`, 네이티브는
`conn.set_autocommit(False)`를 사용해야 합니다. 네이티브 멤버 대입은 스냅샷만
바꿉니다. NULL 사용자는 ''-as-NULL 비교를 `is None`으로 바꾸되 진짜 빈 문자열은
보존하세요. 바이너리 LOB에는 호환성 Unicode read가 아니라 기존 bytes API를 유지합니다.

| 잠정 구현 단위 | 수용 기준 / 디펜던시 |
| --- | --- |
| M 팩터리 / M 공유 | #465는 명시적 생성·종료, 별칭, DSN/user/autocommit 기본값과 래퍼-네이티브 관계를 제공하면서 기존 동작을 유지합니다. 공유·수명주기 직렬화는 별도이며 threadsafety=2를 게이팅합니다. prepared 엔진이나 전역 스위치는 제공하지 않습니다. |
| M prepared / 타입 바인딩 | #439는 #418과 이 계약 뒤에 count/반환/위치 인자/NULL/오류를 검증하고, #440 네이티브 컬렉션 바인딩과 #441 네이티브 LOB 핸들 조회·바인딩은 라이브 차등 비교 주장과 함께 제공됩니다. |
| M 변환 / 문자셋 / HA | #466의 한정된 튜플/dict 행 및 연결별 변환기, #86의 문자셋 인코딩(래퍼의 `charset` 속성 유지는 아직 없음)을 제공합니다. HA/URL 옵션은 실제 failover 증거가 필요한 별도 단위입니다. 옵션 파싱이나 upstream default_cursor 스텁만으로는 미완료입니다. |
| S 배치 / 예외 / identity | 기존 임의 SQL 전송을 이용하는 네이티브 배치 레코드, 네임스페이스 예외/export 어댑터, fresh/트랜잭션/CALL/non-auto 제어를 갖춘 브로커 identity를 각각 분리합니다. 기존 첫 오류·캐시 문자열 동작은 유지합니다. |
| M 설정 / 탐색 | 실제 서버 연산과 네 캐시 멤버를 구별합니다. #444 seek/position과 별도 next_result/실행 옵션/쿼리 계획 단위이며 스텁·플래그만으로 완성하지 않습니다. |
| S/M 메타데이터 / 스키마 / LOB | #445 값·타입·15필드, #412/#455–457 소유한 스키마 재사용, #442 실제 바이트 위치·짧은 전송, #443 파일입니다. 하나의 거대한 파사드 PR이 아닌 집중된 작업입니다. |
| M 차분 증거 | #446/#351은 고정 네이티브 리비전에서 제공된 각 단위를 비교하며 저장된 컬렉션 NULL/빈 값/순서/중복, LOB UTF-8 실패와 기존 SQLAlchemy smoke를 포함합니다. 소스 목록은 패리티 통과가 아닙니다. |

#465 기반 구현은 서브모듈의 명시적 `__all__`, 기존 공개 API 검사기의
추적 모듈/클래스와 RELEASE_POLICY §1 확장, baseline 재생성을 함께 제공합니다.
후속 API 단위도 baseline을 갱신해야 합니다. 새 root 별칭이나 async 변경은
없습니다. 새 명시적 API는 MINOR 추가이고 기존 약속의 수정은 PATCH입니다.
#438은 설계를 선택했고 #465 생성 및 #466의 한정된 행 커서가 제공되며 나머지 하위 기능은 별도로 검증해야 합니다.
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
소스 시나리오 목록화(#437)는 별도의 산출물입니다. 공식 드라이버 비교 증거는
위에서 설명한 라이브 차분 게이트(#446)에서만 나옵니다.

```bash
python scripts/check_official_differential.py   # 오프라인 주장/문서 구조 검사
```

공식 드라이버의 메인테이너와 기여자에게 감사드립니다. 여기의 설명은 고정
소스에서 독립적으로 풀어 쓴 것으로, upstream 소스나 docstring을 통째로
복사하지 않았습니다. 기존 [NOTICE](https://github.com/cubrid-lab/pycubrid/blob/main/NOTICE)와
[제3자 참조 기록](https://github.com/cubrid-lab/pycubrid/blob/main/THIRD_PARTY_LICENSES.md#reference-test-suite)이
출처와 라이선스 한계의 기준으로 유지됩니다. 이 카탈로그는 새로운 라이선스
추론, 런타임 의존성 또는 upstream 소스 재사용 허가를 추가하지 않습니다.
