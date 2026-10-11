# CI 실행 정책 (한국어)

> 🌐 [CI_POLICY.md](https://github.com/cubrid-lab/pycubrid/blob/main/docs/CI_POLICY.md)의 번역입니다. 영어 원문이 표준이며, CI가 영어 원문과의 구조 일치를 검사합니다.

일상적인 CI는 버전/OS의 전체 조합 매트릭스 대신 대표 조합을 사용합니다.

| 트리거 | 런타임 검증 |
| --- | --- |
| 문서만 변경한 PR | 문서 및 정책 검사만 수행. 런타임 스위트와 CUBRID 서버 생성 없음 |
| 일반 코드 PR | Ubuntu/Python 3.12 오프라인 스모크 레인 하나. 전체 커버리지를 주장하지 않음 |
| 고위험 PR | Python 3.11과 3.14에서 커버리지를 제외한 전체 오프라인 회귀와 Python 3.14/CUBRID 11.4. 필요하면 대상 레인 추가 |
| main으로의 코드 푸시 | Ubuntu/Python 3.11과 3.14에서 각각 기존 95% 커버리지 기준을 적용한 전체 오프라인 스위트. 최저·최신 라이브 엔드포인트 |
| 월요일 03:00 UTC | main 푸시와 같은 정책(Python 3.11/3.14 오프라인 스위트 포함)으로 최근 7일의 변경을 비교. 변경이 없거나 문서만 바뀐 이력은 런타임 테스트를 선택하지 않으며, 같은 SHA의 성공한 푸시 실행이 이미 실행한 레인은 반복하지 않음([이벤트 계층과 비용 근거](#이벤트-계층과-비용-근거) 참고) |
| 명시적 전체 실행 또는 릴리스 | 기존 전체 Python 3.11–3.14 × CUBRID 10.2/11.0/11.2/11.4 통합 워크플로와 필수 릴리스 레인, 그리고 95% 기준을 적용한 Python 3.11/3.14 오프라인 스위트, lint, 타입 검사, 공개 API 기준선, 저장소 도구 테스트 |

PR 스모크 스위트는 의도적으로 범위가 제한되어 있습니다. 기여자는 자신의 변경과
관련된 회귀 검사를 로컬에서 실행하고 명령과 결과를 PR에 기록해야 합니다.
스모크 통과는 전체 오프라인 스위트나 커버리지 기준이 실행됐다는 증거가 아닙니다.
`make test`와 `make integration`은 범위 변경 없이 그대로 사용할 수 있습니다.

변경 선택은 `ci.yml`의 `detect-changes` 잡에서 이루어집니다. 문서가 아닌 경로는
기본적으로 코드로 취급하므로, 새 소스/설정 파일이 조용히 문서로 분류되지
않습니다. 새 모듈을 포함한 모든 드라이버·테스트 경로와 의존성, 빌드, 스크립트 변경,
그리고 `ci.yml` 자체의 변경은 보수적으로 병합 전 대표 통합 검사와, 지원하는 가장 오래된
Python과 최신 Python에서의 기존 오프라인 회귀 스위트를 선택합니다([오프라인 엔드포인트 버전](#오프라인-엔드포인트-버전) 참고). 그 밖의 워크플로 변경은 대신 저장소 도구
레인을 선택합니다([워크플로 변경 영향](#워크플로-변경-영향) 참고). 그 밖의 코드 변경은 제한된 스모크
스위트를 유지합니다. 저장소 도구 테스트는 도구가 바뀔 때 Linux 한 레인에서
실행합니다. `tests/test_official_fixture_setup.py`, `tests/test_upstream_scenario_ledger.py`
또는 `tests/fixtures/upstream_scenarios.csv` 변경은 같은 도구 레인을 명시적으로
선택합니다. 이 마커 기반 검사는 드라이버 오프라인 레인에서 제외됩니다.
정적 린트 잡은 생성 문서 검사를 포함해 모든 이벤트에서 계속 실행됩니다.

집계 필수 검사의 이름은 그대로 유지되며 변경 감지를 포함합니다. 선택된 잡은
성공해야 합니다. 선택된 검사가 생략·실패·취소되면 게이트가 실패합니다.
의도적으로 선택하지 않은 잡만 생략될 수 있습니다. 브랜치 보호는 집계 게이트에
걸어 두고, 매트릭스를 줄인 뒤 예전 매트릭스 셀 이름을 하나하나 필수로 두지
마세요. 병합 전에 브랜치 보호 설정을 확인해야 합니다. 보호 설정 변경은 이
PR에 포함되지 않습니다.

전체 검증에는 자동 야간 일정이 없습니다. `integration-full.yml`은 수동 실행과,
변경 불가능한 후보 SHA를 넘기는 릴리스 `workflow_call`을 유지합니다. 두 수동
워크플로 모두 실행한 브랜치 커밋과 일치하는 전체 `sha` 입력을 요구합니다.
PR 증거를 수집할 때는 `pr_number`를 전달하세요. 사전 점검과 최종 게이트는 닫힌
PR, API 실패, 대체된 head를 거부합니다. 모든 체크아웃은 요청/실행의 변경 불가능한
SHA를 사용하며, 가드가 이를 요약에 보고합니다. 일상 CI의 수동 실행은 기본
브랜치와의 차이가 없어도 모든 대표 런타임·도구 레인을 강제로 실행하고, 전체
워크플로는 전체 매트릭스를 강제로 실행합니다. 릴리스 게시자/생성기는 변경하지
않습니다.

동시 실행은 이벤트/ref 그룹을 분리하며, 대체된 PR 실행은 여전히 취소합니다.
main 푸시와 PR 병합 ref의 SHA가 같다고 가정하지 않습니다. 주간 변경 선택은 저장된
마지막 성공 캐시가 아니라 최근 7일을 사용합니다. 실패한 주간 실행은 성공한
증거로 취급하지 말고 재실행하거나 수동 검증으로 이어 가야 합니다.
그런 다음 주간 실행은 정확히 같은 SHA의 성공한 푸시 실행이 이미 실행한 레인 묶음만
제외합니다(#750).

GitHub 요금이 어느 워크플로나 러너 SKU에서 발생하는지는 확인되지 않았습니다.
잡 수를 줄인 것은 반복 작업이 줄었다는 뜻이지, 측정된 비용 절감이 아닙니다.
비용을 주장하기 전에 이후의 Actions 잡/러너 분과 실제 과금 범주를 비교하세요.

심층 버그 탐색 워크플로는 Python 3.12/CUBRID 11.4 셀 하나로 매주(목요일 04:00 UTC) 실행하며,
변경이 없는 주는 건너뛰고 뮤테이션/성능/다운스트림 검사와 넓은 Hypothesis
프로필을 유지합니다. TLS, EUC-KR, 공식 드라이버 차분 잡은 PR에서 관련 경로에
따라 선택되며, main, 변경이 있는 주간 실행, 전체 릴리스 검증에서 계속 사용할 수
있습니다.

## 이벤트 계층과 비용 근거

#750은 비용이 큰 이벤트마다 목적을 하나씩 둡니다. 풀 리퀘스트는 빠르고 대표적인
검증을 유지합니다. `main` 푸시는 엔드포인트 증거를 맡습니다. 주간 일정은 같은 SHA에서
푸시 실행이 아직 증명하지 않은 것과 심층 버그 탐색을 맡습니다. 릴리스는 전체
매트릭스를 실행하고 실패 시 닫힙니다. 다른 레인이 같은 SHA에서 같은 단언을 하는
곳만 바꿨습니다.

### 측정

이 변경 전인 2026-10-09에 GitHub REST API(`actions/workflows/<file>/runs`와
`actions/runs/<id>/jobs?filter=latest`)로 측정했습니다.

- **러너 분**: 실행된 각 잡(최신 시도)의 `completed_at - started_at` 합계입니다.
- **올림 분**: 각 잡을 GitHub의 계량 단위인 1분 단위로 올린 값입니다. 이 저장소는
  표준 러너를 쓰는 공개 저장소이므로 두 값 모두 과금액이 아닙니다.
- **벽시계 분**: 첫 잡 시작부터 마지막 잡 종료까지입니다.
- **실행된 잡**: 결론이 `skipped`도 빈 값도 아닌 잡입니다. 실행된 잡이 없는 실행은
  제외합니다. 승인을 기다리는(`action_required`) PR 실행 120개와 시작 단계에서
  실패한 4개입니다.
- **실패 / 취소**: 실행 결론입니다. 표본의 취소된 실행은 모두 같은 동시 실행
  그룹의 새 실행이 대체한 것이므로 따로 셉니다.

기간은 `a65360a`가 현재의 대표 계층을 도입한 뒤인 2026-10-03 06:04 UTC부터입니다.
주간과 푸시 표본은 #745의 3.11·3.14 오프라인 셀(2026-10-09 02:06 UTC 반영)보다
앞선 것입니다. 2026-10-05 주간 실행에는 `offline-tests (3.12)` 셀 하나뿐이었고,
코드 푸시 51회 중 #745 이후는 2회뿐입니다. 릴리스 표본은 현재 워크플로 형태를 씁니다. `ci.yml` 실행은 다음처럼
분류합니다.

- **풀 리퀘스트**: `integration-tests`, `integration-tls`, `integration-charset`,
  `official-differential` 중 하나나 Python 3.11 `offline-tests` 셀을 실행했으면
  위험 PR입니다. 그렇지 않고 `offline-tests`를 실행했으면 일반 코드 PR입니다.
  나머지 PR은 문서 전용입니다.
- **푸시**: `offline-tests`를 실행했으면 코드 푸시입니다.
- **릴리스**: `Full compatibility matrix` 잡이 실행된 `publish-pypi.yml` 푸시
  실행입니다.

| 범주 | 기간 (실행 수) | 잡 수 중앙값 | 러너 분 중앙값 | 올림 분 중앙값 | 벽시계 분 중앙값 | 실패 / 취소 |
| --- | --- | --- | --- | --- | --- | --- |
| 문서 전용 PR | 2026-10-03..09 (24) | 8 | 1.0 | 8 | 0.7 | 0 / 0 |
| 일반 코드 PR | 2026-10-03..08 (10) | 11.5 | 2.3 | 11.5 | 1.0 | 3 (lint) / 1 |
| 위험 PR | 2026-10-03..09 (91) | 15 | 7.5 | 18 | 3.6 | 4 (lint, 그중 하나는 typecheck도) / 8 |
| main 푸시, 코드 | 2026-10-03..09 (50) | 17 | 10.7 | 23 | 4.0 | 0 / 5 |
| main 푸시, 코드 없음 | 2026-10-03..09 (17) | 8 | 1.0 | 8 | 0.7 | 0 / 0 |
| 주간 `ci.yml` (월요일 03:00) | 2026-10-05 (1) | 18 | 11.4 | 24 | 4.2 | 0 / 0 |
| 주간 `bug-hunt.yml` (월요일 04:00) | 2026-10-05 (1) | 7 | 149.4 | 152 | 83.0 | 1 (뮤테이션: 82.8분 뒤 러너 종료) / 0 |
| `bug-hunt.yml` 수동 실행 | 2026-10-04 (2) | 7 | 110.1 | 112.5 | 74.6 | 0 / 1 |
| `integration-full.yml` 수동 실행 | 2026-10-04 (3) | 27 | 40.6 | 53 | 8.4 | 0 / 0 |
| 릴리스 (`publish-pypi.yml`과 `integration-full.yml`) | 2026-10-04..09 (2) | 34 | 41.8 | 60.5 | 10.2 | 1 (게시 후 cookbook 검증) / 0 |

- **일정 레인에서 결함을 찾은 적은 없습니다.** 기간 중 푸시, 주간, 수동 실행,
  릴리스 실행에서 라이브 레인, 공식 차분, TLS, charset 잡이 실패한 적은 없습니다.
  PR 실패는 모두 lint였고 typecheck 실패가 하나 더 있었습니다.
- **주간 `bug-hunt.yml` 실패는 인프라 문제였습니다.** 유일한 실패는 뮤테이션
  테스트 중 러너가 종료된 것입니다.
- **이전 버그 탐색 형태.** 야간 15셀 형태(2026-09-18~10-02, 15회)의 러너 분
  중앙값은 739였습니다. 모든 실행이 다운스트림 도그푸딩과 mutmut 2.x 설정에서
  실패했으며, 둘 다 이후 고쳐졌습니다.
- **표본이 작습니다.** 두 워크플로가 10월 초에 주간 형태로 바뀌었으므로 주간
  표본은 각각 1회입니다. 릴리스 표본은 2회입니다.

성공한 코드 푸시의 잡별 러너 분 중앙값:

- `integration-tests`: 1.9 (3.14/11.4), 1.5 (3.11/10.2)
- `offline-tests`: 1.7 (3.12, #745 이전), 1.5 (3.11), 1.4 (3.14)
- `integration-tls`: 1.4
- 공식 차분: 1.2
- `integration-charset`: 0.9
- `repo-tooling-tests`: 0.7
- typecheck, packaging, lint: 각 0.3
- 공개 API 검사: 0.2

### 커버리지 담당

| 커버리지 | 담당 (워크플로 / 잡) | 이벤트 계층 |
| --- | --- | --- |
| 오프라인 스위트, 95% 기준 | `ci.yml` `offline-tests`, `integration-full.yml` `offline-endpoints` | 일반 PR: 3.12 스모크. 위험 PR: 커버리지 없이 3.11 + 3.14. main 푸시, 주간, 수동 실행: 3.11 + 3.14. 릴리스와 전체 수동 실행: 3.11 + 3.14 |
| 라이브 엔드포인트 | `ci.yml` `integration-tests`, `integration-full.yml` `integration-full` (4 × 4) | 위험 PR: 3.14/11.4. main 푸시, 주간, 수동 실행: 3.11/10.2 추가. 릴리스와 전체 수동 실행: 16셀 전부 |
| TLS | `ci.yml` `integration-tls` (3.14/11.4), `integration-full.yml` `integration-tls` (3.11 + 3.14) | TLS 경로 PR, PR이 아닌 코드 이벤트, 릴리스와 전체 수동 실행 |
| EUC-KR charset | `ci.yml`과 `integration-full.yml`의 `integration-charset` | charset 경로 PR, PR이 아닌 코드 이벤트, 릴리스와 전체 수동 실행 |
| 공식 CUBRIDdb 차분 | `ci.yml`과 `integration-full.yml`의 `official-differential` | 공식 경로 PR, PR이 아닌 코드 이벤트, 릴리스와 전체 수동 실행 |
| CUBRID 버전 차분 | `integration-full.yml` `version-differential` | 릴리스와 전체 수동 실행 |
| property, 프로토콜, 상태 기계, fault, chaos, soak (넓은 프로필) | `bug-hunt.yml` `property-and-fault` | 최근 7일에 변경이 있을 때 목요일 04:00 UTC, 그리고 수동 실행 |
| 뮤테이션 테스트 | `bug-hunt.yml` `mutation` | 버그 탐색과 같음 |
| 벤치마크 추세 | `bug-hunt.yml` `perf-trend` | 버그 탐색과 같음 |
| 다운스트림 코퍼스 (권고용) | `bug-hunt.yml` `downstream-corpus` | 버그 탐색과 같음 |
| lint, CHANGELOG 구조, 번역 구조, `llms-full.txt` 동기화 | `ci.yml` `lint`, `integration-full.yml` `lint` (같은 단계) | 모든 이벤트, 릴리스와 전체 수동 실행 |
| 타입 검사 | `ci.yml` `typecheck`, `integration-full.yml` `typecheck` (같은 단계) | 모든 코드 이벤트, 릴리스와 전체 수동 실행 |
| 공개 API 기준선 | `ci.yml` `compat-check`, `integration-full.yml` `compat-check` (같은 단계) | 모든 코드 이벤트, 릴리스와 전체 수동 실행 |
| 패키징 | `ci.yml` `packaging-smoke-test`, `publish-pypi.yml` `consistency`와 `build` | 위험 PR, PR이 아닌 코드 이벤트, 릴리스 |
| 저장소 도구 | `ci.yml` `repo-tooling-tests`, `integration-full.yml` `repo-tooling-tests` (같은 단계) | 도구 경로 이벤트, 릴리스와 전체 수동 실행 |
| Python 3.15 미리보기 (권고용) | `python-canary.yml` | 수동 실행만 |

### 통합

- **주간 `ci.yml` 실행은 같은 SHA의 푸시 증거를 재사용합니다.** 기간 중 유일한
  주간 실행(2026-10-05, `c2a4f1db`)은 다섯 시간 전 푸시 실행이 통과한 17개 잡에
  `repo-tooling-tests`를 더해 같은 SHA에서 다시 실행했습니다.
  - **두 실행이 같은 이유.** 둘 다 PR이 아닌 이벤트이므로 같은 `ci.yml`에서 같은
    매트릭스 셀을 고르고 같은 명령을 실행합니다.
  - **주간 실행이 하는 일.** `detect-changes` 잡이 읽기 전용 `actions: read`로
    정확히 같은 SHA의 푸시 실행을 찾습니다. 성공한 푸시 실행 하나가
    `offline-tests`를 실패나 생략된 셀 없이 실행했으면 코드 레인을, 같은 조건으로
    `repo-tooling-tests`를 실행했으면 저장소 도구 레인을 제외합니다.
  - **여전히 전부 실행하는 경우.** head 커밋이 문서 전용이거나, head 푸시 실행이
    실패했거나 취소됐거나, 조회가 실패하면 이전과 똑같이 레인을 선택합니다.
  - **안전망을 유지하는 이유.** 2026-09-01 이후 취소된 코드 푸시 실행 16개 중
    3개는 코드 레인을 선택하지 않은 실행으로 대체됐으므로, 7일 차이는 취소된 푸시의
    안전망으로 남습니다.
  - **드리프트는 이 레인의 일이 아닙니다.** 조용한 주에는 원래 아무것도 선택하지
    않으므로 주간 실행은 의존성이나 러너 이미지 드리프트를 맡은 적이 없습니다.
- **`bug-hunt.yml`을 월요일 04:00 UTC에서 목요일 04:00 UTC로 옮깁니다.** 이제
  월요일의 `ci.yml`, CodeQL, SBOM, 보안, 유지 관리 일정과 겹치지 않습니다. cron 시작이
  늦다고 앞선 실행이 끝났다는 뜻은 아닙니다.
  - **활동 확인.** 그대로입니다. 일정 실행은 최근 7일에 변경이 있을 때만 돕니다.
  - **제거할 중복이 없습니다.** `integration and not slow and not tls` 세션은
    Python 3.12와 넓은 Hypothesis 프로필을 씁니다. 따라서 `ci.yml`의 3.11/3.14
    셀과는 셀과 프로필이 달라 유지합니다.
- **릴리스 병합의 겹침은 유지합니다.**
  - **겹치는 부분.** 릴리스 커밋에서 `ci.yml` 푸시 실행의 라이브 레인
    (`integration-tests` 3.14/11.4와 3.11/10.2, TLS 3.14/11.4, charset, 공식 차분)은
    같은 SHA의 `integration-full.yml`과 명령이 같은 부분집합입니다. 릴리스마다 약
    6.9 러너 분입니다(기간 중 릴리스 2회).
  - **유지하는 이유.** 이를 건너뛰면 푸시 실행의 증거가 매트릭스 전에 멈출 수 있는
    릴리스 감지(`consistency` 실패)에 의존하게 됩니다. 그러면 코드 커밋에 라이브
    증거가 어디에도 남지 않습니다.
- **풀 리퀘스트와 main 푸시는 바뀌지 않습니다.**

위 측정 실행에서 계산한 예상 효과:

| 범주 | 잡 수 전 → 후 | 러너 분 전 → 후 | 올림 분 전 → 후 |
| --- | --- | --- | --- |
| 일반 코드 PR, 위험 PR, main 푸시 | 변경 없음 | 변경 없음 | 변경 없음 |
| 주간 `ci.yml`, head 푸시 실행이 코드 레인과 함께 성공 | 19 → 9 | 약 13 → 약 1.8 | 약 26 → 9 |
| 주간 `ci.yml`, head 푸시 실행이 성공하지 않았거나 문서 전용 | 19 → 19 | 약 13 → 약 13 | 약 26 → 약 26 |
| 주간 `bug-hunt.yml` | 7 → 7 (목요일) | 149.4 → 149.4 | 152 → 152 |
| 릴리스(lint, typecheck, 공개 API, 저장소 도구의 릴리스 실행 사본 포함) | 34 → 40 | 41.8 → 약 46.2 | 60.5 → 약 68.5 |

이 값은 예상치입니다. 절감을 주장하기 전에 이후 몇 주의 Actions 데이터와 비교하세요.

### 릴리스 증거

`publish-pypi.yml`은 릴리스 SHA로 `workflow_call`을 통해 `integration-full.yml`을
호출합니다. `build`와 `publish`는 그 `matrix` 잡을 필요로 하므로, 매트릭스가 실패하거나
취소되거나 생략되면 게시가 멈춥니다. `full-matrix-result`는 모든 의존 잡이
`success`를 보고하지 않으면 실패합니다.

릴리스는 어떤 `ci.yml` 실행도 읽지 않습니다. #750 이전에는 릴리스 커밋에서 95% 기준을
적용한 Python 3.11/3.14 오프라인 스위트(#745)가 `ci.yml` 푸시 실행에서만 나왔습니다.

- **그 실행을 요구한 곳이 없었습니다.** 릴리스 경로의 어떤 단계도 확인하지
  않았습니다.
- **나중 병합이 그 실행을 취소할 수 있었습니다.** `ci-push-refs/heads/main` 동시 실행
  그룹은 새 푸시가 오면 이전 푸시 실행을 취소합니다. 2026-09-01 이후 16개 푸시
  실행이 그렇게 취소됐습니다.

이제 `integration-full.yml`이 `offline-endpoints`를 실행합니다. 정확한 SHA에서
`ci.yml` `offline-tests`와 같은 설치와 pytest 명령으로 Python 3.11과 3.14를 씁니다.
`full-matrix-result`가 이를 요구합니다. 릴리스마다 약 2.9 러너 분이 늘어납니다.

같은 공백이 나머지 `main` 푸시 증거에도 있었고, 메인테이너는 이를 같은 방식으로
닫기로 결정했습니다(2026-10-11, sqlalchemy-cubrid #737과 같음). `integration-full.yml`은
`ci.yml` `lint`, `typecheck`, `compat-check`(공개 API 기준선), `repo-tooling-tests`의
사본도 정확한 SHA에서 같은 Python 버전, 타임아웃, 단계로 실행합니다. `ci.yml`과 다른
점은 세 가지이며, `tests/test_ci_policy.py`가 나머지를 같게 유지합니다.

- **변경 경로로 선택하지 않습니다.** 각 사본은 `validate-target`만 필요로 하므로 모든
  수동 실행과 릴리스 호출에서 실행됩니다. 사본이 건너뛰어지는 경우는 `validate-target`이
  실패했을 때뿐이며, 그때는 이미 게이트가 실패합니다.
- **체크아웃.** 다른 `integration-full.yml` 잡처럼 체크아웃에서
  `persist-credentials: false`를 설정합니다.
- **캐시 모드.** setup-uv는 이 워크플로의 `auto` 캐시 모드를 씁니다.

`full-matrix-result`는 네 잡을 모두 `needs`에 두고 `success`가 아닌 결과에는 실패하므로,
실패·취소·건너뜀·누락된 사본은 게시를 막습니다. `publish-pypi.yml`은 바뀌지 않습니다.
이 워크플로는 cubrid-lab 저장소 사이에서 동일하게 유지되기 때문입니다. 다른 방안인
`main` 푸시 실행을 취소 불가로 만들고 게시 워크플로가 이를 요구하게 하는 방식은
이 파일을 바꿔야 했습니다. 위의 잡별 중앙값으로 보면 사본은 릴리스나 전체 수동 실행마다
약 1.5 러너 분(반올림 4분)을 더합니다.

## 오프라인 엔드포인트 버전

`offline-tests` 잡은 이벤트에 따라 Python 매트릭스를 고릅니다(#745). 그래서 전체
오프라인 스위트가 이미 전부 실행되던 곳에서는 지원하는 가장 오래된(3.11) Python과
최신(3.14) Python에서 실행하고, 일반 PR은 비용이 적은 셀 하나를 유지합니다.

| 이벤트 | `offline-tests` 셀 | 스위트 |
| --- | --- | --- |
| 일반 코드 PR (`risk` 미선택) | Python 3.12 | 대표 스모크 테스트 |
| 위험 경로가 선택된 PR | Python 3.11과 3.14 | 커버리지 없는 전체 오프라인 스위트 |
| main 푸시, 월요일 일정, 수동 실행 | Python 3.11과 3.14 | 95% 커버리지 기준을 적용한 전체 오프라인 스위트 |
| 릴리스와 전체 수동 실행 (`integration-full.yml` `offline-endpoints`) | Python 3.11과 3.14 | 같은 설치와 명령. `full-matrix-result`가 요구 |

모든 셀은 `not integration and not repo_tooling` 선택, 15분 타임아웃, 불변
`inputs.sha || github.sha` 체크아웃을 그대로 유지합니다. `ci.yml`에서는 커버리지 보고서가 Python
버전별 파일(`coverage-py<version>.xml`)로 기록되고, `offline-coverage-py<version>`
아티팩트로 업로드되며, Codecov에는 `offline-py<version>` 플래그로 전송되므로 두 셀이
서로를 덮어쓰지 않습니다. `integration-full.yml`의 `offline-endpoints` 셀은 아무것도 업로드하지 않습니다.

주간 일정은 `ci.yml` 자체가 담당합니다. `integration-full.yml`에는 일정이 없고(릴리스나
전체 수동 실행에서만 두 셀을 반복합니다), `bug-hunt.yml`은 대상 property·fault 스위트만
실행하므로 다른 주간 레인이 이 증거를 중복하지 않습니다(#750). 여기서 Python 3.11은 `actions/setup-python`이
고르는 최신 3.11 패치 릴리스이며 3.11.0–3.11.2는 실행하지 않습니다. 해당 버전은 별도의
대상 회귀 검사를 유지합니다(#744).

`ci-gate`는 `offline-tests`의 집계 결과 하나를 봅니다. 셀 하나라도 실패하거나 취소되면
그 결과는 성공이 아니며, 생략은 코드 변경이 없거나, 주간 일정에서 같은 SHA의 성공한 푸시 실행이 이미 실행한 경우에만
허용되므로 누락되거나 실패한
엔드포인트 셀이 게이트를 통과시킬 수 없습니다.

## 워크플로 변경 영향

변경 경로 선택은 워크플로별 영향 표를 따릅니다(#761). 라이브 PR 레인(`risk`, `tls`,
`charset`, `official`)을 선택하는 것은 이를 정의하고 실행하는 `ci.yml`뿐입니다. `.github/`
아래의 다른 워크플로는 저장소 도구 레인을 선택하며, 그 정책·워크플로 테스트가 해당
워크플로를 파싱하고 검사합니다. `ci.yml`의 라이브 레인은 다른 워크플로의 잡을 실행하지
않으므로 이를 돌려도 검출력이 늘지 않습니다.

| 변경된 워크플로 | PR 검증 |
| --- | --- |
| `ci.yml` | 정의한 모든 레인과 도구 레인 |
| `integration-full.yml` | 도구 레인과, PR head에서 `integration-full.yml`을 수동 `workflow_dispatch`로 실행하고 PR에 링크 |
| `publish-pypi.yml`, `release-please.yml` | 도구 레인(릴리스 워크플로 테스트) |
| `bug-hunt.yml`, `python-canary.yml` | 도구 레인. 실행 자체가 바뀌면 수동 실행 |
| 그 밖의 워크플로 | 도구 레인. `pr-title.yml`과 `docs-sync.yml`은 PR에서 스스로도 실행됨 |

`tests/test_workflow_path_impact.py`가 모든 워크플로 파일과 대표적인 소스, 테스트, 문서
경로에 대해 필터를 평가합니다.

## 병렬 라이브 레인

`integration-tests`, `integration-charset`, `integration-tls`, `official-differential`은
`validate-target`과 `detect-changes`에만 의존하므로(#760), lint, 타입 검사, 오프라인
테스트가 끝난 뒤가 아니라 함께 시작합니다. `validate-target`은 수동 실행 SHA를 PR head와
대조하므로 의존성으로 유지합니다. `ci-gate`는 여전히 모든 잡을 요구하므로 라이브 레인이
통과해도 lint, 타입, 오프라인 실패가 있으면 게이트는 실패합니다. 대신 lint에 실패한 PR도
라이브 레인 러너 시간을 쓸 수 있습니다.

## 잡 타임아웃

실행되는 모든 잡은 정수 `timeout-minutes`를 지정합니다(GitHub 기본값은 360분).
따라서 멈춘 컨테이너, 소켓, 설치가 러너를 6시간 동안 붙잡지 않고 예산 안에서
실패합니다(#758). 짧은 잡은 Actions에서 관측된 최대 실행 시간의 약 3–5배이며
하한을 둡니다. 게이트와 작은 잡은 5분, lint/type/오프라인/도구 잡은 10–15분, 라이브
통합 레인은 20–30분입니다. 두 개의 긴 주간 잡은 더 작은 배수를 쓰는 명시적
예외입니다. property/fault/soak 잡은 120분(관측 최대 63분의 약 1.9배, soak 단계는
별도로 90분 상한)입니다. 뮤테이션 테스트는 뮤턴트 수가
거의 같은 네 개의 샤드로 실행됩니다(#750). 6,812개 뮤턴트 중 샤드마다 약 1,550–1,850개이므로,
단일 잡 120분 기준으로 샤드당 약 30분입니다. 샤드마다 90분을 주어 이전의 300분 예외를
없앴으며, 180분 상한을 넘는 잡은 없습니다. `tests/test_mutation_shards.py`가 모든
`only_mutate` 모듈이 정확히 한 샤드에 속하는지 검증합니다. 집계
게이트(`ci-gate`, `full-matrix-result`)는 `if: always()`와 짧은 타임아웃으로
실행됩니다. 타임아웃된 의존 잡은 성공이 아닌 결과(`cancelled`/`failure`)로
보고되며 게이트는 이를 실패로 처리합니다.

재사용 워크플로를 호출하는 잡에는 `timeout-minutes`를 지정할 수 없습니다. 저장소
내부 피호출 워크플로(`publish-pypi.yml` → `integration-full.yml`)는 그 워크플로의
잡에서 검증합니다. 외부 소유 피호출 워크플로는 `tests/test_workflow_timeouts.py`의
명시적 허용 목록입니다. `cubrid-lab/.github`의 공유 `doc-lint`, `codeql` 워크플로와
cookbook 스모크 테스트가 여기에 해당합니다. 이 테스트는 모든 워크플로를 파싱하여
실행 잡에 제한된 타임아웃이 없거나 새 외부 호출자가 허용 목록에 없으면 실패합니다.

## 의존성 설치

`ci.yml`과 `integration-full.yml`은 uv로 의존성을 설치합니다(#759). 각 Python 잡은
커밋 SHA로 고정한 `astral-sh/setup-uv`를 실행하고 uv 자체도 고정합니다
(`version: "0.12.17"`). 그다음 `uv pip install --system`으로 `actions/setup-python`
인터프리터에 설치하고, 결과를 `uv pip freeze --system`으로 기록합니다. setup-uv에는
setup-python과 같은 `python-version` 지정(예: `"3.12"` 또는 매트릭스 값)을 넘기므로,
캐시 키에는 러너 이미지마다 달라 키 불일치를 일으키는 `uv python find`의 패치 버전
대신 잡의 Python 마이너 버전이 들어갑니다. `pyproject.toml` 해시와 잡별
`cache-suffix`를 함께 써서 잡과 Python 버전마다 별도 캐시를 유지합니다. 같은 입력이
`UV_PYTHON`을 내보내며, `uv pip install --system`은 `PATH`에서 처음 일치하는
인터프리터, 즉 setup-python의 인터프리터를 고릅니다. 일상 CI는 항상 캐시하고(`enable-cache: true`),
`integration-full.yml`은 `auto`를 사용하며, 이는 태그 푸시, `release`,
`pull_request_target`, `workflow_run` 이벤트에서만 캐시를 끕니다. 따라서 릴리스 경로
(`main` 푸시에서 실행되는 `publish-pypi.yml` 또는 복구용 수동 실행)는 여전히 캐시를
복원합니다. uv 캐시에는 내려받거나 빌드한 wheel만 들어 있고 매 실행이 같은 제약에서
다시 해석하므로, 캐시는 설치를 빠르게 할 뿐 해석된 버전을 바꿀 수 없어 안전합니다. 바뀌는 것은 설치 도구뿐입니다. 해석은 같은
`pyproject.toml` 제약을 따릅니다. 전환 전에 Python 3.12에서 `.[dev]`를 pip와 uv로
해석한 결과는 같은 63개 패키지와 버전이었습니다(PEP 503 이름 정규화 후). 패키징
스모크 테스트는 일회용 가상 환경에서 일반 `pip`를 유지합니다. 빌드된 wheel과 sdist가
최종 사용자가 쓰는 도구로 설치되는지 증명하기 때문입니다. `python-canary.yml`도
프리뷰 인터프리터에서 `pip`를 유지합니다. `tests/test_workflow_installs.py`는 고정된
uv 설정, 버전 기록, 그리고 이 워크플로들에 다른 `pip install`이 남지 않았는지를
검증합니다.

## 고정된 문서 도구와 스캔 동시성

`docs.yml`은 `docs/requirements.txt`에서만 사이트 도구를 설치합니다. 이 파일은
`mkdocs`, `mkdocs-material`, `pymdown-extensions`를 정확한 버전으로 고정하므로(#782)
upstream 릴리스가 저장소 변경 없이 `mkdocs build --strict` 게이트를 깨뜨릴 수
없습니다. Dependabot(pip, `/docs`)이 갱신을 제안하며, `mkdocs.yml`은 이 파일을 게시
사이트에서 제외합니다.

`ci.yml`은 풀 리퀘스트에서도 사이트를 빌드합니다(#786). 따라서 `mkdocs build --strict`를
깨뜨리는 버전 갱신이나 문서 수정은 나중에 `main`에서가 아니라 병합 전에 실패합니다.
`docs-build` 작업은 `site` 경로 필터로 선택됩니다: `docs/**`(콘텐츠와
`docs/requirements.txt`), `mkdocs.yml`, `scripts/generate_llms_full.py`,
`.github/workflows/docs.yml`, `.github/workflows/ci.yml`. 루트 `*.md` 파일은 사이트
입력이 아니므로(변경 이력은 링크일 뿐 포함되지 않음) 이 작업을 선택하지 않으며,
`workflow_dispatch`는 다른 레인처럼 이 작업을 강제로 선택합니다. 이 작업은 `docs.yml`의
빌드 단계(`docs/requirements.txt` 설치, `scripts/generate_llms_full.py`,
`mkdocs build --strict`)를 읽기 전용 권한, 고정된 액션, 10분 타임아웃,
`persist-credentials: false`로 실행하며 Pages 아티팩트를 올리거나 배포하지 않습니다.
`CI Gate`는 `site`가 선택된 경우에만 `docs-build`의 성공을 요구하고 그 외에는 건너뜀을
허용하므로, 필수 `CI Gate` 검사가 깨진 문서 빌드를 막습니다.
`tests/test_ci_policy.py`와 `tests/test_workflow_path_impact.py`가 이를 검증합니다.

`codeql.yml`과 `security.yml`은 호출부 수준 `concurrency`를 선언합니다. 그룹은
`${{ github.workflow }}-${{ github.event_name == 'pull_request' && github.ref || github.run_id }}`, 설정은
`cancel-in-progress: ${{ github.event_name == 'pull_request' }}`입니다(#783). 풀
리퀘스트에 새로 푸시하면 대체된 스캔은 취소되지만, 그 외 이벤트는 실행마다 고유한 그룹을 받습니다. 같은 그룹에서는
`cancel-in-progress`가 false여도 대기 중인 실행이 새 실행으로 대체되므로, main 푸시와
예약 실행이 취소되거나 누락되지 않습니다. `security.yml`은 `pyproject.toml`에 고정된 `bandit[toml]==1.9.4`를
설치합니다. `tests/test_workflow_pins.py`가 세 가지를 모두 검증합니다.

## 아티팩트 보존 기간과 캐시

모든 `actions/upload-artifact` 단계는 `retention-days`를 지정하므로, 저장소 기본값인 90일로
남는 아티팩트가 없습니다. 디버깅용·증거용 아티팩트는 14일 또는 30일, 릴리스 아티팩트는 14일
보관합니다. `anchore/sbom-action`은 `upload-artifact: false`로 실행합니다. 워크플로가 SBOM을
직접 업로드하므로, 그대로 두면 액션이 기본 보존 기간으로 사본을 하나 더 저장하기 때문입니다.
`tests/test_workflow_pins.py::test_every_artifact_upload_sets_a_retention_period`가 두 규칙을
고정합니다.

2026-10-11 점검(#750): Actions 캐시는 항목 162개, 7.6 GB로 GitHub 저장소 한도 10 GB에
가까웠습니다. 그중 3.0 GB는 지금 어떤 워크플로도 만들지 않는 `setup-python` pip 캐시로,
uv 설치로 바꾸기 전에 남은 것입니다. 마지막 사용일이 2026-10-08이고 GitHub는 7일 동안 쓰이지
않은 캐시를 지우므로 저절로 정리됩니다. 나머지는 잡별 `setup-uv` 캐시입니다. 캐시 설정은 바꿀
필요가 없었습니다.

## Dependabot 그룹

이전에는 Dependabot이 pip(`/`), pip(`/docs`), GitHub Actions의 패키지마다 PR을 하나씩
열었고, PR마다 전체 PR 계층이 실행되었습니다. 이제 `.github/dependabot.yml`은 minor와
patch 버전 업데이트를 생태계와 디렉터리별로 PR 하나에 묶으므로(`dev-tools`,
`docs-tools`, `github-actions`), 주간 업데이트 묶음은 그룹당 CI 실행 한 번으로
끝납니다(#750). 다음은 여전히 별도 PR로 열립니다.

- **Major 업데이트**는 모든 그룹에서 제외되며(`update-types`는 `minor`와 `patch`뿐),
  `dependabot-auto-merge.yml`은 여전히 사람의 리뷰를 위해 보류합니다.
- **보안 업데이트**는 그룹이 `applies-to: version-updates`로 설정되어 있어 분리됩니다.
- **런타임 의존성 `tzdata`**는 `dev-tools`에서 제외됩니다.

그룹은 아무것도 우회하지 않습니다. 그룹 PR도 같은 필수 체크를 거치며, 체크가 통과해야
자동 병합이 완료됩니다. `dependabot/fetch-metadata`는 그룹 PR의 `update-type`으로 가장
높은 semver 변경을 보고하므로, 그룹이 major 업데이트를 자동 병합으로 가져갈 수 없습니다.
패키지 하나가 실패하면 그룹 전체가 막힙니다. 그룹 PR에서 고치거나, 별도 작업이 필요하면
리뷰된 PR로 해당 패키지를 그룹의 `exclude-patterns`에 추가해 Dependabot이 단독 PR로 열게 하세요. `tests/test_workflow_pins.py`가
그룹 설정을 검증합니다.

## Python 3.15 프리뷰 준비

`python-canary.yml`은 수동 전용입니다. 전체 SHA를 전달하고 그 커밋의 브랜치에서
실행하세요. Ubuntu/표준 GIL 레인 하나가 프리릴리스를 허용해 Python 3.15를
선택하고, 실제 인터프리터/의존성 버전을 출력하며, 전체 오프라인 회귀 검사를
실행하고 새로 설치한 wheel/sdist를 검증합니다. 설정/설치/테스트 실패는 평소처럼
실행을 실패시킵니다. 이 레인은 필수 PR 검사나 릴리스 게이트와 분리되어 있으며,
새 일정, PR 매트릭스 셀, CUBRID 서버 생성이 없습니다. 이 레인만으로는 공식 지원,
라이브 데이터베이스 호환성, free-threaded 호환성이 성립하지 않습니다.
