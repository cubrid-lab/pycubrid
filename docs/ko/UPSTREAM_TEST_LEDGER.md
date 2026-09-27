# Upstream 시나리오 원장

[시나리오 원장](https://github.com/cubrid-lab/pycubrid/blob/main/tests/fixtures/upstream_scenarios.csv)은
[공개 기능 패리티 추적 이슈](https://github.com/cubrid-lab/pycubrid/issues/396) 아래의
[#437](https://github.com/cubrid-lab/pycubrid/issues/437)을 위한 초기 목록입니다.
기능 패리티 인증서나 Python 2 호환성 약속이 아닙니다.

## 목록화 범위

소스: [CUBRID/cubrid-python의 e75ec36 스냅샷](https://github.com/CUBRID/cubrid-python/tree/e75ec36b2a92b8829a49a967a29a1fbb9d7c322b).
목록은 **test 접두사를 가진 원시 함수 선언 450개**를 모두 보존합니다.

| 소스 폴더 | 원시 선언 수 |
|---|---:|
| `tests/` | 68 |
| `tests2/` | 198 |
| `tests3/` | 184 |

여기에는 `test_`라는 이름의 레거시 함수도 포함됩니다. 경로, 클래스, 함수,
소스 줄이 각 선언을 식별합니다. 별도로 이름을 붙인 assertion 하위 사례 8개를
더하면 초기 원장은 458행입니다. 이는 목록 항목 수이지 고유한 동작 시나리오
수가 아닙니다. 하위 사례 평가는 아직 완료되지 않았고, 헬퍼·설정 및 최상위
스크립트의 assertion 평가도 미완료입니다. 로컬 매개변수화 테스트 수를 이 선언
수로 나누어 패리티 백분율을 광고하지 마십시오.

레거시 선언도 원장에 남습니다. `duplicate_candidate_of`는 잠정적인 최신 소스
선언을 가리키는 **이름만 비교한 검토 힌트**입니다. 이 힌트나 대상이 assertion
동등성 또는 정식 고유 시나리오 수를 확정하지는 않습니다. 중복처럼 보인다는
이유로 선언을 제거하지 않습니다.

식별자와 독립적으로 풀어 쓴 기대값은 upstream을 참조합니다. upstream 테스트
본문을 가져오거나 복사하지 않습니다. 소스 출처 기록과
[미확정 테스트 파일 라이선스 조건](https://github.com/cubrid-lab/pycubrid/blob/main/THIRD_PARTY_LICENSES.md#reference-test-suite)을
유지하십시오. 소스를 그대로 재사용하기 전에 적용되는 조건을 확인해야 합니다.

## 매핑과 실행은 별개

`classification`의 의미는 다음과 같습니다.

| 값 | 의미 |
|---|---|
| `unknown` | Assertion과 하위 사례를 평가해야 합니다. |
| `duplicate_candidate` | 레거시 선언과 이름이 같지만 동작은 아직 평가하지 않았습니다. |
| `related` | 로컬 검증 범위는 있지만 assertion·API·설정의 일치 여부는 확정되지 않았습니다. |
| `assertion_equivalent` | 명명된 기대값만 검토된 로컬 assertion과 일치합니다. |
| `unsupported` | 검토된 필수 기능이 없으며 기능 구현 백로그로 남습니다. |

`local_nodes`는 assertion이 동등한 명시적 하위 사례 행에만 사용합니다. `related_nodes`는
동등한 항목으로 세지 않습니다. `local_revision`은 검토한 로컬 소스를 고정합니다.
모든 행에는 담당자·기능군, 기대 동작 또는 명시적 미확인 상태, 공백 이유·이슈가
있습니다. #437은 이 초기 목록을 제공합니다. 이 이슈를 닫아도 미평가 선언이나
중복 후보의 작업이 끝난 것은 아닙니다. 지속적인 기능군 평가는 열린 상위 이슈
#396 또는 명명된 기능 후속 이슈에서 이어지며, #437은 산출물의 출처로 유지됩니다.
미지원 시나리오는 패리티 작업에서 제외되는 항목이 아닙니다.

초기 검토는 `7e0aad8fe83a37f324c1c9efea0ff68de15680f2`에 이미 병합된
로컬 SQL·타입·데이터 노드 41개를 참조합니다. 정확한 assertion 하위 사례 8개만
로컬 노드 7개에 매핑됩니다. 선택된 ID·개수, 동일한 ENUM 순번 쌍, 트리거의
결과 행 두 개입니다. 상위 선언은 전체 fixture·API 동작을 동등하다고 선언하지
않고 관련 항목으로 유지합니다. 특히 다음 차이가 있습니다.

- INDEX 조인·서브셀렉트는 로컬에서 4, upstream에서 5를 assertion으로 확인합니다.
- PARTITION 스키마·데이터와 공백 패딩된 CHAR assertion이 다릅니다.
- ENUM 업데이트 데이터, 영향받은 행 수, 추가 메타데이터 검사가 다릅니다.
- 컬렉션 리터럴·디코딩 테스트는 prepared 컬렉션 바인딩(#440)을 구현하지 않으며,
  공식 드라이버의 문자열 값을 가진 컨테이너와도 다릅니다(#438).
- LOB 리터럴 왕복 테스트는 다른 데이터를 사용하며 공식 핸들 bind/fetch(#441),
  seek(#442), 파일 가져오기·내보내기(#443)의 동등성을 확정하지 않습니다.

초기 `evidence_status`는 모두 `not_run`입니다. 이 변경은 assertion을 검토하고
노드 ID를 수집하지만 어느 드라이버의 실제 서버 계약도 실행하지 않습니다.
매핑, 테스트 수집 또는 건너뛴 통합 테스트는 통과 증거가 아닙니다.

나중에 실행 관찰 결과를 기록할 때는 `verification_commit`, 서버 버전,
Python 버전, 모드, 결과, 불변 CI/JUnit 또는 보존된 결과물 참조를 기록하십시오.
실패했거나 이유를 설명하고 건너뛴 정직한 결과에는 `observed`를 사용합니다.
공백을 유지하고 이를 통과로 바꾸지 마십시오. 건너뛰지 않은 결과의 `skip_reason`은
비워 두어야 합니다. `verified_pass`에는 동등한
assertion 매핑, 통과 결과, 완전한 식별 정보가 필요합니다. 한 번의 관찰이 모든
서버·모드를 인증하지는 않습니다. 기존 레인·JUnit 방식이 런타임 증거를 제공하며,
[#446](https://github.com/cubrid-lab/pycubrid/issues/446)에서 이 작업을 추적합니다.

## 검사와 유지 관리

```bash
python -m pytest tests/test_upstream_scenario_ledger.py
```

집중 검사는 서버 없이 기존 pytest 수집 방식을 사용하여 잘못된 노드 ID,
중복 소스 ID, 누락된 담당자·공백 이유, 미지원·실패 항목의 통과 주장,
설명 없는 건너뛰기를 거부합니다. 합성 검증 입력은 이 거부 규칙을 테스트하며,
드라이버 실행 증거로 저장되지 않습니다.

다른 선언을 평가할 때는 소스 ID와 후보 링크를 보존하고 필요한 경우 명시적
assertion 하위 사례를 추가하십시오. `local_nodes`를 제공하기 전에 fixture,
인자, 타입과 정확한 기대값을 비교해야 합니다. 실행 필드는 독립적으로 유지하십시오.
이 작업은 테스트 목록 관리에 한정됩니다. 드라이버 동작, 지원 정책, 호환성
파사드, CI 스케줄러 또는 새로운 의존성을 변경하지 않습니다.
