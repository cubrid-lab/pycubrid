# 처음으로 CUBRID에 연결하기 (한국어)

> 🌐 [quickstart.md](https://github.com/cubrid-lab/pycubrid/blob/main/docs/quickstart.md)의 번역입니다. 영어 원문이 표준이며, CI가 영어 원문과의 구조 일치를 검사합니다.

pycubrid는 Python 프로그램이 CUBRID 데이터베이스와 대화할 때 쓰는 **커넥터**(연결 라이브러리)입니다.
데이터베이스 자체인 CUBRID 서버는 별도의 프로그램이며, 어딘가에서 이미 실행되고 있어야 합니다.
pycubrid를 설치한다고 CUBRID가 설치되거나 데이터베이스가 만들어지지는 않습니다.

이 페이지는 빈 폴더에서 시작해 첫 쿼리를 실행하고 첫 행을 저장하기까지 안내합니다. DB-API,
CUBRID 통신 프로토콜, 이 프로젝트의 CI를 몰라도 따라 할 수 있으며, 그런 내용은
[참고 문서](#다음-단계)에 있습니다.

---

## 시작하기 전에

Python 3.11 이상과 연결할 수 있는 CUBRID 데이터베이스가 필요합니다. 상황에 맞는 쪽을 고르세요.

### 방법 A: 이미 CUBRID 데이터베이스가 있습니다

관리자에게 묻거나 직접 설정한 내용에서 다음 다섯 가지 값을 확인하세요.

- **host**(호스트): CUBRID가 실행되는 컴퓨터. 예: `db.example.com`, `localhost`
- **port**(포트): 연결을 받아 주는 입구인 CUBRID 브로커의 포트(기본값 `33000`)
- **database**(데이터베이스): 데이터베이스 이름. 예: `testdb`
- **user**(사용자)와 **password**(비밀번호): 그 데이터베이스의 계정

3단계에서는 작은 테이블 하나를 만들었다가 나중에 지웁니다. 연습용 데이터베이스나 그곳에 테이블을
만들 수 있는 계정을 사용하세요. 운영 중인 데이터베이스나 그 `dba` 계정으로 이 튜토리얼을 따라 하지
마세요. 2단계와 3단계의 실습용 값을 여러분의 값으로 바꾼 뒤 1단계로 진행하세요.

### 방법 B: 버리고 다시 만들 수 있는 로컬 데이터베이스가 필요합니다

Docker로 `testdb`라는 데이터베이스가 있는 일회용 CUBRID 11.4 서버를 띄웁니다.
[Docker](https://docs.docker.com/get-started/get-docker/)와 bash 같은 셸이 필요합니다. 명령은
x86_64(amd64) Linux에서 검증했습니다. `cubrid/cubrid:11.4` 이미지는 amd64용으로만 배포되므로
다른 아키텍처(예: Apple silicon)와 Windows에서의 동작은 검증하지 않았습니다.

```bash
docker run -d --name pycubrid-quickstart -e CUBRID_DB=testdb -p 127.0.0.1:33000:33000 cubrid/cubrid:11.4
ready=no; for i in $(seq 36); do if docker exec pycubrid-quickstart csql -u dba testdb -c "SELECT 1" >/dev/null 2>&1; then ready=yes; break; fi; sleep 5; done; if [ "$ready" = yes ]; then echo "CUBRID is ready"; else echo "CUBRID did not answer within 3 minutes; see: docker logs pycubrid-quickstart"; fi
```

`127.0.0.1:`은 포트를 이 컴퓨터에만 공개하므로 다른 컴퓨터는 실습 환경에 접속할 수 없습니다. 두 번째
명령은 데이터베이스가 응답할 때까지 최대 3분 기다린 뒤(보통 1분이 채 안 걸립니다) `CUBRID is ready`를
출력합니다. 대신 `CUBRID did not answer within 3 minutes`가 출력되면
`docker logs pycubrid-quickstart`로 원인을 확인하세요. `docker run` 자체가 실패했다면(예: 포트
33000을 이미 다른 프로그램이 쓰는 경우) 그 문제를 먼저 해결하고 `docker rm -f pycubrid-quickstart`로
컨테이너를 지운 뒤 다시 시작하세요. 실습 환경은 비밀번호가 빈 `dba` 계정을 씁니다. 이것은 이 로컬
일회용 컨테이너에서만 허용되는 설정입니다. 다른 사람이 접속할 수 있는 서버에서는 절대 빈 비밀번호를
쓰지 마세요.

튜토리얼을 모두 마치면 `docker rm -f pycubrid-quickstart`로 실습 환경을 지웁니다.

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
활성화된 이 터미널에서 실행하세요.

---

## 2단계: 첫 쿼리 실행하기

같은 폴더에 `first_query.py` 파일을 만듭니다.

```python
import pycubrid

# Connection settings for the disposable local lab. With your own database,
# replace them with the values from your administrator.
conn = pycubrid.connect(
    host="localhost",  # the machine that runs the CUBRID broker
    port=33000,  # the broker port (33000 is the CUBRID default)
    database="testdb",  # the database name
    user="dba",
    password="",
)

cur = conn.cursor()  # a cursor sends SQL and reads results
cur.execute("SELECT 1 + 1")  # execute sends one SQL statement
print(cur.fetchone())  # fetchone reads one result row

cur.close()
conn.close()
```

**연결**(connection)은 데이터베이스와의 세션입니다. **커서**(cursor)는 연결 안에서 일하며,
`execute`는 SQL 문 하나를 보내고 `fetchone`은 결과에서 한 행을 읽습니다. 파일을 실행하세요.

```bash
python first_query.py
```

값 하나가 든 행 하나가 출력됩니다.

```text
(2,)
```

오류가 나면 [첫 연결이 실패할 때](#첫-연결이-실패할-때)를 보세요.

---

## 3단계: 행을 저장하고 다시 읽기

이 단계에는 2단계에서 성공한 연결 값이 필요합니다. 같은 다섯 가지 연결 값으로
`first_saved_row.py`를 만듭니다.

```python
import pycubrid

SETTINGS = {"host": "localhost", "port": 33000, "database": "testdb", "user": "dba", "password": ""}

conn = pycubrid.connect(**SETTINGS)
cur = conn.cursor()

# 1. Create a table that belongs to this tutorial. The comment marks it, so the
#    cleanup script only ever drops this tutorial's own table.
cur.execute(
    "CREATE TABLE pycubrid_quickstart_notes (id INT PRIMARY KEY, note VARCHAR(100))"
    " COMMENT = 'created by the pycubrid quickstart'"
)
conn.commit()

# 2. Insert one row. The ? marks are filled with the values that follow, in order.
cur.execute(
    "INSERT INTO pycubrid_quickstart_notes (id, note) VALUES (?, ?)",
    (1, "my first saved row"),
)
conn.commit()  # commit makes the change permanent and visible to others

# 3. Insert a second row, then undo it with rollback.
cur.execute(
    "INSERT INTO pycubrid_quickstart_notes (id, note) VALUES (?, ?)",
    (2, "this row is rolled back"),
)
conn.rollback()  # rollback discards everything since the last commit

cur.execute("SELECT id, note FROM pycubrid_quickstart_notes ORDER BY id")
print("Rows in this session:", cur.fetchall())
cur.close()
conn.close()

# 4. Check from a brand-new connection that the committed row was saved.
check = pycubrid.connect(**SETTINGS)
check_cur = check.cursor()
check_cur.execute("SELECT id, note FROM pycubrid_quickstart_notes ORDER BY id")
print("Rows seen by a new connection:", check_cur.fetchall())
check_cur.close()
check.close()
```

변경 사항은 **커밋**(commit)하기 전까지 확정되지 않으며, **롤백**(rollback)은 마지막 커밋
이후의 모든 변경을 버립니다. `?` 표시는 자리 표시자입니다. pycubrid는 SQL 문을 보내기 전에 각
`?`를 튜플의 해당 값으로 안전하게 이스케이프해 바꿔 넣습니다. 값을 SQL 문자열에 직접 붙여 넣지
말고 항상 이 방식으로 전달하세요. (값이 어떻게 변환되는지는 [파라미터 바인딩](PARAMETER_BINDING.md)에
설명되어 있습니다.)

파일을 실행하세요.

```bash
python first_saved_row.py
```

롤백한 행은 사라졌고, 새 연결에서도 저장된 행이 보입니다.

```text
Rows in this session: [(1, 'my first saved row')]
Rows seen by a new connection: [(1, 'my first saved row')]
```

이 파일을 두 번째로 실행하거나, 중간에 끊긴 뒤 다시 실행하면 첫 문장에서
`Class dba.pycubrid_quickstart_notes already exists`로 멈춥니다(소유자 이름 `dba` 자리에는 여러분의
사용자가 표시됩니다). 이전 실행에서 만든 테이블이 남아 있기 때문입니다. 아래의 정리를 실행한 뒤
`first_saved_row.py`를 다시 실행하세요.

### 정리하기

`cleanup_quickstart.py`는 여러분의 사용자가 소유한 그 이름의 테이블에 튜토리얼의 주석이 붙어 있을 때만
튜토리얼 테이블을 지웁니다. 같은 이름으로 여러분이 직접 만든 테이블이나 다른 사용자의 테이블은 그대로
둡니다.

```python
import pycubrid

SETTINGS = {"host": "localhost", "port": 33000, "database": "testdb", "user": "dba", "password": ""}
MARK = "created by the pycubrid quickstart"

conn = pycubrid.connect(**SETTINGS)
cur = conn.cursor()
cur.execute(
    "SELECT comment FROM db_class WHERE class_name = ? AND owner_name = CURRENT_USER",
    ("pycubrid_quickstart_notes",),
)
row = cur.fetchone()
if row is None:
    print("Nothing to clean up: pycubrid_quickstart_notes does not exist.")
elif row[0] != MARK:
    print("Not dropped: pycubrid_quickstart_notes was not created by this quickstart.")
else:
    cur.execute("DROP TABLE pycubrid_quickstart_notes")
    conn.commit()
    print("Dropped pycubrid_quickstart_notes.")
cur.close()
conn.close()
```

```bash
python cleanup_quickstart.py
```

`Dropped pycubrid_quickstart_notes.`를 출력하며, 지울 것이 없으면
`Nothing to clean up: pycubrid_quickstart_notes does not exist.`를 출력합니다. Docker 실습 환경을
썼다면 `docker rm -f pycubrid-quickstart`로 실습 환경 전체도 지울 수 있습니다.

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
- [연결 가이드](CONNECTION.md): 모든 연결 옵션, URL, 타임아웃, TLS
- [문제 해결](TROUBLESHOOTING.md): 연결, 쿼리, 트랜잭션의 더 많은 문제
- 참고: [API 참조](API_REFERENCE.md), [타입 시스템](TYPES.md),
  [파라미터 바인딩](PARAMETER_BINDING.md), [지원 매트릭스](SUPPORT_MATRIX.md),
  그리고 통신 프로토콜 자체는 [CAS 프로토콜](PROTOCOL.md)
