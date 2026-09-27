# 공식 드라이버 API 목록

[기계 판독 가능한 카탈로그](https://github.com/cubrid-lab/pycubrid/blob/main/tests/fixtures/official_api_inventory.json)는
공식 [CUBRID/cubrid-python 소스 스냅샷](https://github.com/CUBRID/cubrid-python/tree/e75ec36b2a92b8829a49a967a29a1fbb9d7c322b)에
드라이버가 선언한 공개 연산을 기록합니다. 소스 참조, 시그니처, 기본값,
반환값·오류 관찰 결과, 현재 pycubrid의 대응 항목과 명시적 공백을 담습니다.
이는 [#396](https://github.com/cubrid-lab/pycubrid/issues/396) 범위에서
[#436](https://github.com/cubrid-lab/pycubrid/issues/436)이 제공하는 목록 산출물입니다.

이 문서는 소스 항목을 목록화한 것입니다. 기능 패리티, 공식 드라이버의 성공적인
실행, 완전한 API 상위 집합을 인증하지 않습니다. 비슷한 이름이나 기존 패킷
클래스만으로는 충분한 증거가 되지 않습니다. 호환성·마이그레이션 계약은 여전히
[#438](https://github.com/cubrid-lab/pycubrid/issues/438)에서 추적합니다.
이 카탈로그는 1.x 기본값을 변경하거나 새로운 파사드를 선택하지 않습니다.

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
| 생성자, autocommit, 스레딩 | 네이티브 초기화는 autocommit을 켭니다. pycubrid는 기본적으로 수동 commit을 사용하며 `threadsafety=1`을 선언합니다. CCI URL/HA 옵션, 사용자 기본값, 래퍼 생성자 별칭은 #438의 결정이 필요합니다. |
| 문자셋, dict 커서, 변환기 | 공식 래퍼는 이 옵션들을 노출합니다. pycubrid에는 동등한 설정 가능 표면이 없습니다. 문자셋 제안 #86이 누락된 인코딩 옵션을 추적합니다. UTF-8 전용 동작을 문서화한 것은 구현 증거가 아닙니다. UTF-8 기본값은 바뀌지 않습니다. |
| Prepare/타입 지정 바인딩 | 패킷 클래스가 존재해도 공개 네이티브 prepare/bind/execute 기능은 없습니다. #418/#439, 타입 지정 컬렉션 핸들은 #440, LOB 핸들은 #441에서 추적합니다. |
| LOB 커서·파일 동작 | pycubrid는 명시적 오프셋으로 bytes를 읽고 쓰며, 공식 드라이버의 변경 가능한 위치·암묵적 생성 인터페이스와 다릅니다. seek와 읽기·쓰기 계약은 #442, 파일 가져오기·내보내기는 #443입니다. |
| 결과 탐색과 메타데이터 | 절대·상대 seek와 위치는 #444, 15필드 결과 메타데이터는 #398과 함께 #445에서 추적합니다. 네이티브 next_result는 존재하지만, 래퍼의 nextset 스텁과 pycubrid의 미지원 nextset이 그 기능을 제공하지는 않습니다. |
| 스키마 행 | #412가 스키마 결과 소비와 핸들 정리를 추적합니다. 반환된 프로토콜 패킷은 네이티브 스키마 행 반환 계약과 같지 않습니다. |
| 배치 파사드와 옵션 플래그 | `Cursor.executemany_batch(sql_list, auto_commit=None)`는 이미 임의 SQL을 배치 처리합니다. 연결 수준 파사드와 네이티브의 문장별 오류 레코드는 이 메서드의 튜플 결과·첫 오류 발생 방식과 다릅니다. 실행 플래그·쿼리 계획 옵션과 연결 멤버 설정자는 #438 아래의 집중된 후속 작업이 필요합니다. |

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
