# 안전하게 마치기 (한국어)

> 🌐 [finish-safely.md](https://github.com/cubrid-lab/pycubrid/blob/main/docs/finish-safely.md)의 번역입니다. 영어 원문이 표준이며, CI가 영어 원문과의 구조 일치를 검사합니다.

이 단원에서는 튜토리얼이 만든 것, 즉 튜토리얼 테이블과 사용했다면 Docker 실습 환경을 제거합니다.
단원이 중단되었거나 두 번 실행되었을 때 복구하는 방법도 안내합니다.
[커밋과 롤백](commit-and-rollback.md) 다음에 이어지는 단원입니다.

---

## 시작하기 전에

같은 폴더, 같은 다섯 가지 연결 값, 활성화된 가상 환경이 필요합니다(새 터미널이면
`source .venv/bin/activate`를 다시 실행). [행을 저장하고 다시 읽기](save-a-row.md) 이후라면 언제든,
단원이 도중에 실패했을 때도 이 단원을 실행할 수 있습니다.

---

## 튜토리얼 테이블 정리하기

`cleanup_quickstart.py`는 튜토리얼 테이블을 제거하되, 내 사용자가 소유한 그 이름의 테이블에 튜토리얼의
주석이 있을 때만 제거합니다. 직접 만든 같은 이름의 테이블이나 다른 사용자의 테이블은 그대로 둡니다.
같은 폴더에 같은 `SETTINGS`로 만드세요.

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

실행하세요.

```bash
python cleanup_quickstart.py
```

다음 세 줄 중 하나를 출력합니다.

| 출력 | 뜻 |
|---|---|
| `Dropped pycubrid_quickstart_notes.` | 튜토리얼 테이블을 제거했습니다. |
| `Nothing to clean up: pycubrid_quickstart_notes does not exist.` | 내 사용자에게 그 이름의 테이블이 없으며, 아무것도 바꾸지 않았습니다. |
| `Not dropped: pycubrid_quickstart_notes was not created by this quickstart.` | 내 사용자에게 튜토리얼 주석이 없는 같은 이름의 테이블이 있습니다. 그대로 둡니다. |

### 확인 방식

`db_class`는 CUBRID의 테이블 카탈로그입니다. 쿼리는 소유자가 `CURRENT_USER`(접속한 사용자)인
튜토리얼 이름의 테이블을 찾아 그 주석을 읽습니다. 스크립트는 주석이
[행을 저장하고 다시 읽기](save-a-row.md)에서 쓴 주석일 때만 테이블을 삭제합니다. 테이블 이름도 다른
값처럼 `?` 자리 표시자로 전달합니다.

`Not dropped`가 나오면 그 테이블은 튜토리얼의 것이 아닙니다. 단원을 진행하려고 삭제하지 마세요. 다른
연습용 데이터베이스나 계정을 쓰거나, 그 테이블을 만든 사람에게 물어보세요.

---

## Docker 실습 환경 제거하기

[방법 B](quickstart.md#방법-b-버리고-다시-만들-수-있는-로컬-데이터베이스가-필요합니다)의 일회용 Docker
실습 환경을 썼다면 데이터베이스와 함께 컨테이너 전체를 제거하세요.

```bash
docker rm -f pycubrid-quickstart
```

이 명령은 `pycubrid-quickstart`라는 이름의 컨테이너만 제거합니다. 직접 쓰는 데이터베이스라면 이 단계를
건너뛰세요.

---

## 단원이 중단되었거나 두 번 실행되었을 때

| 보이는 것 | 할 일 |
|---|---|
| `first_saved_row.py`가 `Class dba.pycubrid_quickstart_notes already exists`(사용자 이름 포함)로 멈춤 | 이전 실행의 테이블이 남아 있습니다. `cleanup_quickstart.py`를 실행한 뒤 `first_saved_row.py`를 다시 실행하세요. |
| `first_rollback.py`가 `After rollback`을 출력하기 전에 중단됨 | 정리할 것이 없습니다. 연결이 끝날 때 커밋하지 않은 행은 버려집니다. 다시 실행하세요. |
| `first_rollback.py`가 `Unknown class "dba.pycubrid_quickstart_notes"`(사용자 이름 포함)로 멈춤 | 먼저 [행을 저장하고 다시 읽기](save-a-row.md)의 `first_saved_row.py`를 실행하세요. |

---

## 다음 단계

단원을 모두 마쳤습니다. 여기서부터는 다음을 보세요.

- [예제](EXAMPLES.md): 수정, 삭제, `with` 블록, 비동기 코드, LOB
- [연결 옵션](CONNECTION.md): 모든 연결 옵션, URL, 타임아웃, TLS
- 문제가 생기면 [문제 해결](TROUBLESHOOTING.md)과 [자주 묻는 질문](faq.md)
