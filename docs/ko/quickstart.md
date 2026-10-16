# 처음으로 CUBRID에 연결하기 (한국어)

> 🌐 [quickstart.md](https://github.com/cubrid-lab/pycubrid/blob/main/docs/quickstart.md)의 번역입니다. 영어 원문이 표준이며, CI가 영어 원문과의 구조 일치를 검사합니다.

pycubrid는 Python 프로그램이 CUBRID 데이터베이스와 대화할 때 쓰는 **커넥터**(연결 라이브러리)입니다.
데이터베이스 자체인 CUBRID 서버는 별도의 프로그램이며, 어딘가에서 이미 실행되고 있어야 합니다.
pycubrid를 설치한다고 CUBRID가 설치되거나 데이터베이스가 만들어지지는 않습니다.

이 페이지는 다섯 개의 짧은 단원 중 첫 번째입니다. 빈 폴더에서 시작해 데이터베이스와 Python을
준비합니다. 다음 단원들은 한 단계씩 더합니다. [첫 쿼리 실행하기](first-query.md),
[행 저장하기](save-a-row.md), [커밋과 롤백](commit-and-rollback.md),
[안전하게 마치기](finish-safely.md)입니다. DB-API, CUBRID 통신 프로토콜, 이 프로젝트의 CI를 몰라도
따라 할 수 있으며, 그런 내용은 [참고 문서](#다음-단계)에 있습니다.

---

## 시작하기 전에

Python 3.11 이상과 연결할 수 있는 CUBRID 데이터베이스가 필요합니다. 상황에 맞는 쪽을 고르세요.

### 방법 A: 이미 CUBRID 데이터베이스가 있습니다

관리자에게 묻거나 직접 설정한 내용에서 다음 다섯 가지 값을 확인하세요.

- **host**(호스트): CUBRID가 실행되는 컴퓨터. 예: `db.example.com`, `localhost`
- **port**(포트): 연결을 받아 주는 입구인 CUBRID 브로커의 포트(기본값 `33000`)
- **database**(데이터베이스): 데이터베이스 이름. 예: `testdb`
- **user**(사용자)와 **password**(비밀번호): 그 데이터베이스의 계정

이 페이지 다음의 단원들은 작은 테이블 하나를 만들었다가 나중에 지웁니다. 연습용 데이터베이스나 그곳에
테이블을 만들 수 있는 계정을 사용하세요. 운영 중인 데이터베이스나 그 `dba` 계정으로 이 튜토리얼을 따라
하지 마세요. 각 단원 프로그램의 실습용 값을 여러분의 값으로 바꾼 뒤 1단계로 진행하세요.

### 방법 B: 버리고 다시 만들 수 있는 로컬 데이터베이스가 필요합니다

Docker로 `testdb`라는 데이터베이스가 있는 일회용 CUBRID 11.4 서버를 띄웁니다.
[Docker](https://docs.docker.com/get-started/get-docker/)와 bash 같은 셸이 필요합니다. 명령은
x86_64(amd64) Linux에서 검증했습니다. `cubrid/cubrid:11.4` 이미지는 amd64용으로만 배포되므로
다른 아키텍처(예: Apple silicon)와 Windows에서의 동작은 검증하지 않았습니다.

먼저 컨테이너를 시작합니다.

```bash
docker run -d --name pycubrid-quickstart -e CUBRID_DB=testdb -p 127.0.0.1:33000:33000 cubrid/cubrid:11.4
```

`127.0.0.1:`은 포트를 이 컴퓨터에만 공개하므로 다른 컴퓨터는 실습 환경에 접속할 수 없습니다.
데이터베이스가 시작되려면 시간이 조금 걸립니다. 다음을 같은 셸에 붙여 넣으세요. 최대 3분 동안 5초마다
데이터베이스가 응답하는지 확인합니다.

```bash
ready=no
for attempt in $(seq 36); do
  if docker exec pycubrid-quickstart csql -u dba testdb -c "SELECT 1" >/dev/null 2>&1; then
    ready=yes
    break
  fi
  sleep 5
done
if [ "$ready" = yes ]; then
  echo "CUBRID is ready"
else
  echo "CUBRID did not answer within 3 minutes; see: docker logs pycubrid-quickstart"
fi
```

보통 1분이 채 안 되어 `CUBRID is ready`를 출력합니다. 대신 `CUBRID did not answer within 3 minutes`가 출력되면
`docker logs pycubrid-quickstart`로 원인을 확인하세요. `docker run` 자체가 실패했다면(예: 포트
33000을 이미 다른 프로그램이 쓰는 경우) 그 문제를 먼저 해결하고 `docker rm -f pycubrid-quickstart`로
컨테이너를 지운 뒤 다시 시작하세요. 실습 환경은 비밀번호가 빈 `dba` 계정을 씁니다. 이것은 이 로컬
일회용 컨테이너에서만 허용되는 설정입니다. 다른 사람이 접속할 수 있는 서버에서는 절대 빈 비밀번호를
쓰지 마세요.

튜토리얼을 모두 마치면 [안전하게 마치기](finish-safely.md#docker-실습-환경-제거하기)에서 실습 환경을
지웁니다.

---

## 1단계: 가상 환경에 pycubrid 설치하기

가상 환경은 이 프로젝트의 패키지를 시스템의 나머지 부분과 분리해 둡니다. 먼저
`python3 --version`이 3.11 이상을 출력하는지 확인하세요. 더 오래된 버전이 나오면 첫 명령에서
더 새로운 인터프리터를 이름으로 지정하세요(예: `python3.12`). pip는 사용 중인 Python을 지원하는
pycubrid 버전만 설치하므로, 오래된 인터프리터에는 오래된 pycubrid가 설치됩니다. 그런 다음 빈
폴더에서 다음 명령을 실행하세요.

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install pycubrid
python -c "import pycubrid; print(pycubrid.__version__)"
```

마지막 명령은 설치된 버전(예: `1.13.0`)을 출력합니다. 아래의 모든 `python` 명령은 가상 환경이
활성화된 이 터미널에서 실행하세요. 다음으로 [첫 쿼리를 실행하세요](first-query.md).

---

## 2단계: 첫 쿼리 실행하기

첫 쿼리는 별도 단원에서 다룹니다. [첫 쿼리 실행하기](first-query.md)에서 `first_query.py`를 만들어
실행하고, 그 프로그램이 쓰는 연결과 커서를 설명합니다.

---

## 3단계: 행을 저장하고 다시 읽기

데이터 저장은 이제 두 개의 단원에서 다룹니다. [행을 저장하고 다시 읽기](save-a-row.md)는 튜토리얼
테이블을 만들고 행 하나를 커밋하며, [커밋과 롤백](commit-and-rollback.md)은 두 번째 행을 추가했다가
되돌립니다.

### 정리하기

튜토리얼 테이블과 Docker 실습 환경 제거, 그리고 중단되었거나 반복 실행된 경우의 복구는
[안전하게 마치기](finish-safely.md)에 있습니다.

---

## 첫 연결이 실패할 때

CUBRID 11.4 실습 환경에서 각 실수에 대해 pycubrid 1.13.0이 실제로 출력한 메시지(트레이스백의 마지막
줄)입니다.

| 메시지 | 뜻 | 확인할 것 |
|---|---|---|
| `ModuleNotFoundError: No module named 'pycubrid'` | 지금 실행한 Python이 pycubrid를 설치한 Python이 아닙니다. | 1단계의 가상 환경을 활성화하고(`source .venv/bin/activate`) 시스템 `python3`가 아닌 `python`으로 실행하세요. |
| `OperationalError: failed to connect to CUBRID broker` | 그 호스트와 포트에서 아무도 응답하지 않았습니다. | 서버가 실행 중인지(실습 환경이면 `docker ps`에 `pycubrid-quickstart`가 보이는지), 호스트와 포트가 맞는지, 방화벽이 포트를 막지 않는지 확인하세요. |
| `DatabaseError: Failed to connect to database server, 'nosuchdb', ...` | 브로커는 응답했지만 그 이름으로 실행 중인 데이터베이스가 없습니다. | 데이터베이스 이름을 확인하고, 관리자에게 그 데이터베이스가 시작되어 있는지 물어보세요. |
| `OperationalError: User "nobody" is invalid.` | 그 데이터베이스에 해당 사용자가 없습니다. | 사용자 이름을 확인하세요. |
| `OperationalError: Incorrect or missing password.` | 사용자는 있지만 비밀번호가 틀렸습니다. | 비밀번호를 확인하세요. |

오류를 피해 가지 말고 값을 고치세요. 운영 환경의 `dba` 계정으로 바꾸거나 TLS 인증서 검사를 끄는
방법으로 오류를 없애지 마세요. 더 많은 경우는 [문제 해결](TROUBLESHOOTING.md#연결-문제)에 있습니다.

---

## 실제 실행 영상

<video controls preload="metadata" playsinline width="100%">
  <!-- docs-check: ignore --> <!-- MkDocs serves this page one level down and does not rewrite raw HTML, so this path is relative to the site URL, not the file. -->
  <source src="../../assets/videos/pycubrid-demo.mp4" type="video/mp4">
  브라우저가 영상을 지원하지 않으면 아래 다운로드 링크를 사용하세요.
</video>

[동기·비동기 CRUD 실행 영상 다운로드](../assets/videos/pycubrid-demo.mp4).
영상은 PyPI 배포본이 아닌 로컬 소스 빌드 wheel과 격리된 CUBRID 서버를 사용합니다.
쿼리 결과, 파라미터 CRUD, 별도 연결에서 확인한 컨텍스트 매니저의 커밋,
비동기 쿼리와 자원 정리를 검증합니다.
[편집 가능한 소스와 녹화 근거](https://github.com/cubrid-lab/pycubrid/tree/main/demos)를 참고하세요.
이 영상은 실행 예제이며 전체 호환성이나 성능을 보장하지 않습니다.

---

## 다음 단계

- [예제](EXAMPLES.md): 수정, 삭제, `with` 블록, 비동기 코드, LOB
- [연결 옵션](CONNECTION.md): 모든 연결 옵션, URL, 타임아웃, TLS
- [문제 해결](TROUBLESHOOTING.md)과 [자주 묻는 질문](faq.md): 연결, 쿼리, 트랜잭션의 더 많은 문제
- 참고: [API 참조](API_REFERENCE.md), [타입 시스템](TYPES.md),
  [값을 안전하게 전달하기(파라미터 바인딩)](PARAMETER_BINDING.md),
  [지원 매트릭스](SUPPORT_MATRIX.md). 기여자를 위한 통신 프로토콜 자체는 [CAS 프로토콜](PROTOCOL.md)에
  있습니다.
