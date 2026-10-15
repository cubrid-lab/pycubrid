# pycubrid 기여 안내

> [CONTRIBUTING.md](https://github.com/cubrid-lab/pycubrid/blob/main/CONTRIBUTING.md)의 한국어 번역입니다. 영어 원문이 표준입니다.

`pycubrid`에 기여하려는 관심에 감사드립니다.

GitHub 이슈, PR과 댓글은 영어로 작성하세요. 현지화 문서 기여는 환영하며,
특정 번역 도구를 사용할 필요는 없습니다.

## 개발 환경 설정

### 사전 준비

- Python 3.11+
- Git
- Docker (통합 테스트용)

### 설치

```bash
git clone https://github.com/cubrid-lab/pycubrid.git
cd pycubrid

python3 -m venv venv
source venv/bin/activate

# 기여자 도구는 extra가 아니라 pyproject.toml의 PEP 735 의존성 그룹이므로
# `--group`에는 pip 25.1 이상이 필요합니다(`uv pip install -e . --group dev`도 됩니다).
python -m pip install --upgrade pip
pip install -e . --group dev
```

`dev` 그룹은 `test`, `lint`, `security`, `release` 그룹을 포함합니다. 특정 도구만
필요하면 해당 그룹만 설치하세요. 예를 들어 문서 사이트는 `pip install --group docs`,
뮤테이션 테스트는 `pip install -e . --group dev --group mutation`입니다. 이전의
`pip install -e ".[dev]"` extra는 더 이상 없으며 `pip install -e . --group dev`로
대체되었습니다(#784).

## 테스트 실행

### 오프라인 테스트

```bash
make test
```

### 통합 테스트

```bash
make integration                          # Docker broker on localhost:33000
make integration CUBRID_TEST_PORT=33522   # use a free port if 33000 is taken
```

`make integration`은 준비가 될 때까지 기다리며, 브로커가 준비되지 않거나 선택한
테스트가 모두 스킵되면 실패하고 항상 컨테이너를 제거합니다. 이미 실행 중인
서버를 검사하려면 엔드포인트를 명시하세요:

```bash
CUBRID_TEST_HOST=127.0.0.1 CUBRID_TEST_PORT=33522 \
  pytest tests/ -m "integration and not slow and not tls" -v
```

`CUBRID_TEST_URL` 또는 `CUBRID_TEST_HOST`가 통합 테스트를 *활성화*합니다.
엔드포인트는 필드별 `CUBRID_TEST_*` 변수, URL, 기본값 순서로 결정하며,
기본값은 사용자 `dba`의 `localhost:33000/testdb`입니다. 설정이 없으면 통합
테스트를 스킵하지만, 엔드포인트를 설정했는데 접속할 수 없으면 스킵 대신
**오류**를 냅니다. [통합 테스트](DEVELOPMENT.md#integration-tests)를 참고하세요.

### 비동기 TLS 통합 테스트 (선택)

전용 CI TLS 레인은 SSL을 활성화한 브로커에서 `integration and tls`를 선택합니다.
로컬 브로커·인증서 설정은 기존 [비동기 TLS 안내](DEVELOPMENT.md#async-tls-integration-tests)를
따르세요. 실행하지 못한 실서버 검사와 이유를 기록하세요. 연결·프로토콜 변경에서
빠진 브로커·버전 검증은 메인테이너가 조율합니다.

## 코드 스타일

이 프로젝트는 Ruff로 린트와 포맷 검사를 수행합니다.

```bash
make lint
```

자동 수정:

```bash
make format
```

Make나 pre-commit을 실행하기 전에 프로젝트 개발 환경
(`pip install -e . --group dev`)을 활성화하세요. Ruff와 Mypy pre-commit 훅은
`repo: local`, `language: system`이며, 같은 활성 환경에서
`python3 -m ruff`/`python3 -m mypy`를 실행합니다. 각 도구 버전의 단일 기준은
`pyproject.toml`에 고정된 정확한 버전입니다. 커밋할 때 훅을 실행하려면 이 환경이나
개발 패키지가 설치된 가상 환경을 활성화하세요. 그렇지 않으면 Ruff/Mypy가 없거나
고정 버전 대신 오래된 전역 버전이 조용히 실행될 수 있습니다.
`make tooling-check`는 설치된 Ruff/Mypy 버전과 고정값을 비교하고 훅이 활성
환경을 통해 실행되는지 확인합니다. Make와 CI는 명시적인 Python/pyi 검색과 훅
타입을 사용하며 `LINT_PATHS` (`pycubrid tests scripts demos
examples`)를 공유하므로 Markdown을 다시 포맷하지 않습니다. strict Mypy(패키지와
`make typecheck`의 `tests/typing_self_contract.py` 정적 계약)는 별도이며, pre-commit
훅도 명시적으로 `pycubrid/`를 검사합니다.

도구를 갱신하려면 `pyproject.toml`의 `lint` 의존성 그룹에 있는 버전 고정값을 변경하고
`dev` 그룹을 다시 설치한 뒤 `make check-all`과 `pre-commit run --all-files`를 실행하세요.
별도로 수정할 훅 revision은 없습니다. 드리프트 게이트는 누락·모호한 고정값,
고정값과 다른 설치 버전, 좁아진 검사 범위를 거부합니다. Dependabot의 `pip`
생태계는 `pyproject.toml` 고정값만 갱신할 수 있으며, pre-commit 훅이 항상 설치된
도구를 실행하므로 CI를 유지할 수 있습니다.

## PR 지침

1. 변경을 한 가지 목적에 집중하고 PR 설명에 동기를 적으세요.
2. 동작 변경에 맞는 테스트를 추가하거나 갱신하세요.
3. `make check-all`과 `make test`를 실행하고 명령·결과와 실행하지 못한 검사를 기록하세요.
4. 연결·프로토콜 관련 변경에는 통합 테스트를 실행하세요.
5. 사용자에게 보이는 변경은 `CHANGELOG.md`에 기록하세요.

메인테이너나 에이전트가 작성한 PR은 [AGENTS.md](../AGENTS.md#development-workflow-cubrid-lab-org-standard)의
도구별 리뷰 경로를 따릅니다. 중요한 OpenCode 작업은 문서화된 Oracle 설계 리뷰와 구현 후
리뷰를 거치고, 사소한 OpenCode 수정은 이유를 기록하면 이 리뷰를 생략할 수 있습니다.
Claude Code, Codex, ChatGPT 등 독립 에이전트 도구는 병합 전에 PR에 제출된 실질적인
GitHub Copilot 리뷰나 메인테이너의 리뷰를 기다립니다(메인테이너 결정, 2026-10-15). 구현자가
실행한 리뷰어 에이전트는 인정되지 않습니다. 리뷰 요청, 자체 리뷰, 통과한 CI, 자동 승인
제안만으로는 리뷰 경로가 완료되지 않습니다. 최종 결과, 반영한 지적과 해결되지 않은 스레드는
AGENTS.md에 따라 기록합니다. 사람의 기여는 일반 기여자 리뷰를 따르며, 어떤 에이전트나 내부
리뷰 도구도 설치하거나 사용할 필요가 없습니다.

`.github/workflows/agent-review.yml`의 `agent-review` 체크는 GitHub에 기록된 리뷰만 읽어 이
리뷰 경로가 충족됐는지 알려 주는 **참고용(advisory)** 체크입니다. 최종 head에 대해 두 차례 AI
리뷰 예산 안에서 제출된 실질적인 Copilot 리뷰, 또는 메인테이너의 `APPROVED` 리뷰나 인라인
스레드를 연 `COMMENTED` 리뷰만 인정합니다. PR 본문 체크박스, 대기 중인 리뷰 요청, 자체 리뷰,
다른 사람이나 다른 AI 에이전트의 리뷰, 이전 커밋에 대한 리뷰는 인정하지 않습니다. 바인딩,
프로토콜, 패킷, 연결/TLS, `compat` 모듈, 모든 워크플로, 릴리스 및 레인 점검 스크립트와 이
게이트 자체를 바꾸는 PR에는 메인테이너의 `APPROVED` 리뷰가 필요합니다. PR이 같은 이름의 체크를
게시할 수 있으므로, PR이 흉내 낼 수 없는 주체(GitHub App 등)가 결과를 게시할 때까지(#945)
필수 체크로 지정하지 않습니다. 자세한 규칙은
[CONTRIBUTING.md](../CONTRIBUTING.md#agent-review-evidence-gate)에 있습니다.

`.github/CODEOWNERS`는 릴리스, CI, 보안 정책, 와이어 프로토콜/연결 경로의 변경에
메인테이너 리뷰를 자동으로 요청합니다. 리뷰 라우팅만 담당하며 보안 경계가 아니고,
그 밖의 변경은 코드 오너를 기다리지 않습니다.

외부 기여자는 동기, 구현, 테스트와 관련 문서를 제공합니다. 메인테이너는 내부
Oracle/에이전트 검토, 통합 검증과 릴리스 분류를 조율합니다. 이런 프로젝트 도구의
설치는 외부 기여의 사전 조건이 아닙니다. 기여자 저작 정보를 보존하고, 실제로
그 도구가 커밋을 작성했을 때만 도구 출처를 추가하세요.

문서 변경이 필요 없으면 실제 이유를 `Docs: not needed -`로 시작하는 독립된
물리적 소스 줄에 적으세요. 빈 텍스트, `<reason>`, 인용문, 주석이나 fenced 예시는
예외가 아닙니다. 일반 문장과 붙어 있어도 되며 별도 문단일 필요는 없습니다.
기존 `docs-not-needed` 라벨은 메인테이너가 관리하는 별도 예외입니다. 어느 문서
예외도 코드·보안·릴리스 검사를 우회하지 않습니다. 앞에 공백 세 개까지 허용하지만,
탭·공백 네 개로 들여쓴 코드 예시와 원시 HTML `blockquote`/`pre`/`code` 블록은
예외를 부여하지 않습니다.

번역 도움이 필요하면 PR 본문에 빠진 언어와 제약 사유를 적으세요. 요청만으로
보류가 승인되지는 않습니다. 메인테이너가 기존 `translations-deferred` 라벨을
명시적으로 승인하고 기록된 후속 작업을 담당합니다. 한국어 README 동기화는
계속 필수이며, 다른 번역은 권고 사항입니다.

문서를 변경하면 `python scripts/generate_llms_full.py`로 `docs/llms-full.txt`를
다시 생성하세요. 이 명령은 표준 `docs/llms.txt` 인덱스도 루트 `llms.txt`에
복사합니다. `docs/llms.txt`만 편집하고, 생성 파일 중 하나라도 오래되면 CI가
실패합니다. 고정된 도구(`pip install --group docs`)를 설치한 뒤
기존 사이트 검사 `mkdocs build --strict`를 실행하세요.
`python scripts/check_docs_links.py`는 필수 `docs-check` CI 작업처럼 깨진 로컬 링크,
앵커, 닫히지 않은 코드 펜스와 없는 명령을 보고합니다. AI 검토 의견과 실제로
실행한 명령은 별개입니다. 빠진 검증과 기존 경고를 포함해 모두 정확히 보고하세요.

메인테이너는 검토한 upstream 커밋 SHA로 공유 워크플로 호출부를 갱신합니다.
그 커밋의 대상 워크플로와 `workflow_call` 입력을 확인하고 호출부를 함께 갱신한
뒤 필수 검사를 실행합니다. 공유 doc-lint 워크플로는 여전히 main 기반의 설정·
스캐너 자산을 가져오므로, 호출부 SHA 고정이 그 자산까지 고정하지는 않습니다.

## 저장소별 이슈 소유권

각 저장소는 자신의 코드·CI·문서 변경에 필요한 이슈, 우선순위, 담당자, 검증
조건 및 종료 여부를 독립적으로 관리합니다. 다른 저장소의 이슈를 읽지 않아도
작업 범위와 완료 조건을 이해할 수 있도록 작성하세요. 실제 기술적 의존성이나
호환성 계약이 있는 경우에만 `Refs owner/repo#N`으로 참조하며 다른 저장소의
우선순위나 종료 상태를 가져오지 않습니다. 완료한 현재 저장소의 이슈만
`Closes #N`으로 연결하세요. 연결할 현재 저장소 이슈가 없는 PR은 N/A와 그 이유를 적을
수 있습니다. 기존 필수 `PR title` 검사는 강조나 링크로 감싼 경우를 포함해 PR
본문, PR 제목과 커밋 메시지에서 다른 저장소 이슈의 직접 자동 종료 참조를
차단합니다. 제목과 커밋 메시지는 squash 커밋을 구성하므로 일반 텍스트로
읽으며, 병합할 때 squash 메시지를 직접 고친 내용은 검사하지 않습니다. 본문의
펜스·들여쓰기 코드 블록과 HTML 주석 블록은 Markdown 구조를 정확히 따라갈 수
있을 때만 제외하고, 확실하지 않은 부분은 검사합니다. 목록 항목·각주·인용문
안의 코드 블록과 주석 블록은 항상 검사합니다. 추적할 수 없는 펜스(목록
항목·인용문·원시 HTML 안의 펜스, 들여쓴 줄의 펜스, 펜스처럼 보이기만 하는 줄)
뒤에는 이후의 펜스와 주석을 검사합니다. 본문 문장이나 인라인 코드라도 HTML
태그를 시작할 수 있는 내용 뒤에는 이후의 주석을 검사하고, `textarea`나
`select` 같은 원시 텍스트 요소 이름 뒤에는 아무것도 제외하지 않습니다. 인라인
코드와 인라인 주석은 항상 검사합니다. 외부 기여자에게 내부 AI 도구 설치나 중복
작업을 요구하지 않습니다.

<a name="pull-request-and-commit-titles"></a>

## 이슈·PR·커밋 제목

모든 cubrid-lab 저장소의 이슈 제목, PR 제목과 커밋 첫 줄에 적용합니다.
PR은 squash-merge하며 PR 제목이 `main`의 커밋 제목이 되므로, PR 제목을
정확히 작성해야 합니다. `PR title` 검사가 이를 강제합니다.

```text
type: description
type(scope): description
type!: description
type(scope)!: description
```

- **type**: 소문자로 다음 중 정확히 하나를 사용합니다. `feat`, `fix`, `docs`,
  `test`, `perf`, `refactor`, `ci`, `build`, `chore`, `style`, `revert`.
- **scope**: 선택 사항이며 소문자·숫자·`-`·`_`를 사용합니다. 예: `compiler`,
  `aio`, `deps`, `release`.
- 콜론 앞의 **`!`**는 호환성을 깨는 변경입니다. 그런 변경에는 저장소 릴리스
  정책도 따르세요.
- 콜론 뒤에는 정확히 **공백 하나**를 둡니다.
- **description**: 영어로 구체적으로 적으세요. 변경한 함수·타입·동작을
  명시하고, 첫 단어가 API 이름·약어·고유명사가 아니면 소문자로 시작합니다.
  끝에 마침표를 붙이지 않습니다.
- 대괄호·상태·우선순위 접두사(`[Bug]`, `[WIP]`,
  `Track:`, `epic:`, `P1`)를 붙이지 않습니다. 미완성 작업은 draft PR로 열고,
  우선순위와 크기는 라벨로 표시합니다.
- 제목에 이슈·PR 번호를 넣지 마세요. PR 본문에 `Closes #123` 또는
  `Refs #123`을 적습니다. GitHub가 squash 커밋에 `(#456)` 같은 PR 번호를
  자동으로 붙입니다.

| 유형 | 용도 |
|------|------|
| `feat` | 사용자에게 보이는 새 기능 |
| `fix` | 보안 수정을 포함한 잘못된 동작의 수정 |
| `docs` | 문서만 변경 |
| `test` | 테스트만 변경 |
| `perf` | 동작 변화 없이 더 빠르거나 가볍게 변경 |
| `refactor` | 동작 변화 없는 구조 변경 |
| `ci` | CI 워크플로와 설정 |
| `build` | 패키징과 빌드 시스템 |
| `chore` | 릴리스·의존성 갱신·정리 등의 유지보수 |
| `style` | 포맷만 변경 |
| `revert` | 앞선 변경 되돌리기; 설명에 대상 변경을 명시 |

예시:

```text
fix(protocol): keep the CAS session after OUT_TRAN
feat(aio): add a charset connection option
docs: document JSON as_numeric() input limits
chore(deps): bump ruff from 0.16.8 to 0.16.9
chore: release v1.9.0
refactor(compiler)!: drop legacy LIMIT rendering
```

이슈 양식은 유형 접두사를 미리 채웁니다. 이를 유지하고 제목의 나머지도 같은
형식으로 쓰세요. 추적 이슈(epic)는 추적하는 작업의 유형을 사용합니다.

메인테이너는 **squash merge만** 사용하고 PR 제목을 커밋 제목으로 유지합니다.
브랜치 커밋은 커밋 본문으로 합쳐지므로 메시지를 의미 있게 쓰고
`Co-authored-by:` trailer를 그대로 보존하세요.

## 릴리스

기여자는 릴리스하지 않습니다. 사용자에게 보이는 변경은 `CHANGELOG.md`의
`## [Unreleased]` 아래에 적고, 일반 PR에서 `__version__`을 변경하거나 날짜가
있는 `## [X.Y.Z]` 섹션을 추가하지 마세요. 버전 변경이 병합되면 자동 릴리스가
시작됩니다. 메인테이너는 PR만 생성하는 `release-please.yml` 생성기를 사용하며,
선별한 Unreleased·Upgrade notes는 CHANGELOG에서 계속 검토합니다.
후보의 노트를 편집하기 전에 `autorelease: review`로 동결하고, 봇이 마지막으로
갱신한 head에서 CI를 시작하세요.
[`RELEASING.md`](https://github.com/cubrid-lab/pycubrid/blob/main/RELEASING.md)를 참고하세요.

## 이슈 보고

기존 이슈를 먼저 검색한 뒤 가장 가까운 이슈 양식을 사용하세요. 미리 채운 제목
접두사를 유지합니다. 직접 만든 이슈는 [이슈·PR·커밋 제목](#pull-request-and-commit-titles)의
유형을 선택하세요. 예: `fix(protocol): ...`.

제보자는 영향과 재현 방법을 설명하며, GitHub 라벨 권한이 없어도 보고할 수
있습니다. 메인테이너가 유형 라벨과 `priority: <value>`, `size: <value>`를
각각 하나씩 지정하고 필요하면 `area:`도 붙입니다. `testing` 같은 주제 라벨도
있을 수 있습니다. 사람이 CLI/API로 올린 이슈의 메타데이터가 불완전하면
`status: needs triage`가 붙습니다. 메인테이너가 메타데이터를 바로잡고 그 라벨을
제거합니다. `GITHUB_TOKEN`으로 이슈를 만드는 워크플로는 제목과 라벨을 직접
지정해야 합니다. GitHub는 그 이벤트로 다른 워크플로를 시작하지 않습니다.

이슈에 다음을 포함하세요:

- Python 버전
- CUBRID 서버 버전
- 최소 재현 코드
- 전체 traceback 또는 오류 출력

긴급성과 예상 작업량을 설명하세요. 메인테이너나 triager가 표준 `priority:`/
`size:` GitHub 라벨을 지정하므로 제보자에게 라벨 권한을 요구하지 않습니다.

## 버그 발견 → 회귀 절차

pycubrid는 적대적 "Bug Hunt" 검사 계층(property fuzzing, protocol fuzzing,
상태 기계, metamorphic parity, 결함 주입, 차등 검사, mutation, resource/soak)을
실행합니다. 여기서 발견하거나 수동 검토·downstream dogfooding으로 발견한 모든
결함은 수정이 반영되기 전에 반드시 다음 절차를 따라야 합니다:

```
Bug
 → Minimal reproduction
 → GitHub Issue (label: bug + area:)
 → Failing regression test (committed FIRST, red)
 → Fix
 → Permanent regression contract (the test is now green and kept)
```

규칙:

1. 기술적으로 테스트를 만들 수 없는 경우가 아니라면 **회귀 테스트 없이 버그를
   수정하지 않습니다**. 불가능하면 PR에 명시적으로 설명하세요.
2. **`xfail`에는 연결된 이슈가 필요합니다.** 알려졌지만 아직 고치지 않은 결함에는
   `@pytest.mark.xfail(strict=True,
   reason="issue #NNN: ...")`를 사용하세요. 수정되는 순간 가드가 강한 실패(XPASS)로
   바뀌므로 수정 PR에서 `xfail` 마커를 제거해야 합니다.
3. 테스트·CI 실패를 숨기는 포괄적인 **`|| true`**나 단순한 `except: pass`를
   사용하지 마세요.
4. **예상 밖 `XPASS`는 잡음이 아닌 신호입니다.** 연결된 버그가 수정되었으므로
   같은 PR에서 `xfail`을 제거하고 올바른 동작을 단언하세요.
5. **알려진 한계는 드라이버 / 서버 / upstream으로 분류**하여 차이가 pycubrid의
   책임인지 알 수 있게 하세요. 문서화한 서버 동작·구현 차이는 조용히 스킵하지
   않고 관련 테스트(예: CUBRIDdb 차등 스위트)에 고정합니다.

실제 예시: 이슈 #362(`Lob.read`의 조용한 절삭)는 LOB 적대적 스위트에서 발견하여
최소 재현과 함께 이슈로 만들고 strict `xfail` 회귀 검사로 보호한 뒤 수정했습니다.
수정 PR에서 `xfail`은 통과하는 단언으로 바뀌었습니다.

동작 변경 분류를 기록하는 위치는
[`RELEASE_POLICY.md`](https://github.com/cubrid-lab/pycubrid/blob/main/RELEASE_POLICY.md) §7을
참고하세요.

### CUBRID 버전 차등 검사

`tests/test_version_differential.py`(#351)는 같은 Hypothesis 생성 값·문장을
CUBRID 10.2, 11.0, 11.2, 11.4에서 함께 실행하고 pycubrid가 제공하는 오류 클래스/
`errno`/`sqlstate`, `rowcount`, `lastrowid`, `description`과 각 값의 Python 타입·
값을 비교합니다. `integration-full.yml`의 `version-differential` 잡에서 실행하며,
릴리스 `workflow_call`과 수동 `workflow_dispatch` 대상이지 PR마다 실행하지는
않습니다. 로컬에서는 버전별 컨테이너를 시작하고 다음처럼 지정하세요:

```bash
CUBRID_VERSION_MATRIX="10.2=127.0.0.1:33102,11.0=127.0.0.1:33110,11.2=127.0.0.1:33112,11.4=127.0.0.1:33114" \
CUBRID_TEST_HOST=127.0.0.1 CUBRID_TEST_PORT=33114 \
  python -m pytest tests/ -m "integration and version_matrix"
```

차이는 `tests/helpers/version_matrix.py`의 `VersionDifference`가 이유, CUBRID
변경 링크, 차이가 나는 버전, 달라도 되는 필드와 해당 차이에 도달할 수 있는
워크로드에 생성기가 붙이는 태그를 설명할 때만 통과합니다. 각 항목에는 차이가
재현되지 않으면 실패하는 결정적 probe도 있습니다. 다른 차이는 드라이버 버그
(수정하거나 이슈 등록) 또는 문서화되지 않은 서버 변경입니다. 후자는 pycubrid
밖에서 `csql` 같은 도구로 확인한 뒤 문서화하세요.

## 최소 PR 검증

[CI 실행 정책](CI_POLICY.md)을 따르세요. PR 스모크는 대표 검사이지 전체
스위트·커버리지 증거가 아닙니다. 관련 회귀 테스트를 로컬에서 실행하고 명령·
결과를 기록하며, 호환성상 필요하면 정확한 head의 전체 검증을 요청하세요.

## 처음 기여하기 좋은 이슈(good first issue) 참여

담당자가 지정되지 않은 `good first issue`는 **사람이 직접 주도하는 기여자**에게 열려 있습니다. 사람이 AI 도구의 도움을 받아 변경 내용을 검토하고 제출하는 경우도 포함됩니다. 이러한 기여자는 **메인테이너의 사전 허가, GitHub Assignee 지정 권한, 내부 에이전트 도구 없이** 작업을 시작하고 PR을 제출할 수 있습니다.

작업 전에는 기존 담당자, 댓글, 열린 PR을 확인하세요. 참여 의사를 알리는 댓글은 중복 작업을 줄이는 데 도움이 되지만 필수 승인 절차나 독점 예약은 아닙니다. 이미 누군가 맡았거나 PR이 진행 중이라면 먼저 조율하세요. Assignees와 라벨은 메인테이너가 관리하며, 스스로 담당자를 지정할 수 없어도 사람 기여자의 PR 제출은 제한되지 않습니다.

반면 **메인테이너를 대신해 자율적으로 작업하는 에이전트**는 신규 기여자에게 남겨 둔 `good first issue`를 구현하기 전에 해당 **특정 이슈에 대한 명시적 승인**과 구현자 지정이 필요합니다. 포괄적인 백로그 정리나 릴리스 준비 요청은 승인으로 간주하지 않습니다.

기존 테스트, 문서화, PR 리뷰 및 병합 요건은 그대로 유지됩니다. 병합 여부와 시점은 메인테이너가 결정합니다.

## 실행 가능한 이슈 설명 유지

코딩 전에 문제, 기대 동작, 범위, 완료 기준과 검증 방법을 합의하세요. 본문은
현재 명세로 유지하고 날짜가 있는 진행 상황은 댓글에 적습니다. 메인테이너는
관련 병합·인계 뒤에 닫힌 의존성과 완료된 체크리스트를 정리합니다. 원래 재현의
revision과 한계를 보존하세요. 오래된 증거는 현재 동작을 입증하지 않습니다.
우선순위·크기는 라벨에, 실행 순서는 backlog tracker에 둡니다. 연구는 문서화한
결정으로 종료하며 제안한 모든 선택지를 구현하겠다는 약속이 아닙니다.

사람이 직접 주도하는 외부 기여자(AI 보조 도구 사용 포함)는 담당자가 없는 이슈의 작업을 시작하거나 PR을 제출할 때 메인테이너의 사전 승인 또는 GitHub Assignees 지정 권한이 필요하지 않습니다. 기존 담당자·댓글·열린 PR을 확인하고, 중복 작업이나 인계가 필요하면 먼저 조율하세요. 참여 댓글은 권장하지만 필수 허가 절차는 아닙니다.

메인테이너가 운영하는 자율 에이전트와 내부 담당자는 작업 전 실제 구현자를 Assignees에 지정해야 합니다. 자율 에이전트가 `good first issue`를 맡으려면 그 이슈에 대한 명시적 승인이 별도로 필요합니다. 담당자 변경과 라벨 관리는 메인테이너가 진행하며 기존 기여자의 작업을 존중합니다. 검토자는 이슈 담당자일 필요가 없습니다.
