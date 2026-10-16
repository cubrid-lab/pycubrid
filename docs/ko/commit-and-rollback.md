# 커밋과 롤백 (한국어)

> 🌐 [commit-and-rollback.md](https://github.com/cubrid-lab/pycubrid/blob/main/docs/commit-and-rollback.md)의 번역입니다. 영어 원문이 표준이며, CI가 영어 원문과의 구조 일치를 검사합니다.

이 단원에서는 튜토리얼 테이블에 두 번째 행을 추가한 뒤 롤백으로 되돌립니다. 트랜잭션이 무엇을 남기고
무엇을 버리는지 볼 수 있습니다. [행을 저장하고 다시 읽기](save-a-row.md) 다음에 이어지는 단원입니다.

---

## 시작하기 전에

앞 단원에서 마련한 것이 필요합니다.

- [행을 저장하고 다시 읽기](save-a-row.md)의 `first_saved_row.py`가
  `Rows seen by a new connection: [(1, 'my first saved row')]`를 출력해, `pycubrid_quickstart_notes`
  테이블에 행 하나가 있어야 합니다.
- 같은 폴더와 활성화된 가상 환경(새 터미널이면 `source .venv/bin/activate`를 다시 실행)

---

## 프로그램 작성하기

같은 폴더에 같은 `SETTINGS`로 `first_rollback.py`를 만듭니다.

```python
import pycubrid

SETTINGS = {"host": "localhost", "port": 33000, "database": "testdb", "user": "dba", "password": ""}

conn = pycubrid.connect(**SETTINGS)
cur = conn.cursor()

# 1. Insert a second row, but do not commit it.
cur.execute(
    "INSERT INTO pycubrid_quickstart_notes (id, note) VALUES (?, ?)",
    (2, "this row is rolled back"),
)
cur.execute("SELECT id, note FROM pycubrid_quickstart_notes ORDER BY id")
print("Before rollback:", cur.fetchall())

# 2. Undo it.
conn.rollback()  # rollback discards everything since the last commit

cur.execute("SELECT id, note FROM pycubrid_quickstart_notes ORDER BY id")
print("After rollback:", cur.fetchall())
cur.close()
conn.close()
```

---

## 실행하기

```bash
python first_rollback.py
```

두 번째 행은 트랜잭션 안에서는 보이고, 롤백한 뒤에는 사라집니다.

```text
Before rollback: [(1, 'my first saved row'), (2, 'this row is rolled back')]
After rollback: [(1, 'my first saved row')]
```

---

## 프로그램이 한 일

- **트랜잭션**은 끝낼 때까지 변경을 하나로 묶습니다. pycubrid는 트랜잭션을 알아서 시작합니다. 자동
  커밋이 기본적으로 꺼져 있으므로 `commit()`을 호출하기 전에는 아무것도 영구적이지 않습니다.
- 트랜잭션 안에서는 내 연결에 커밋하지 않은 행이 이미 보이므로, 첫 `SELECT`는 두 행을 보여 줍니다.
- `rollback()`은 트랜잭션을 끝내고 마지막 `commit()` 이후의 모든 변경을 버립니다. 앞 단원에서 저장한
  행은 커밋되었으므로 남아 있습니다.
- `commit()` 없이 연결을 닫아도 변경은 저장되지 않습니다. 남기고 싶은 것은 닫기 전에 커밋하세요.

`first_rollback.py`는 다시 실행해도 됩니다. 실행할 때마다 같은 두 번째 행을 추가하고 지우므로 같은 두
줄을 출력합니다. 롤백 전에 프로그램이 중단되면, 연결이 끝날 때 서버가 커밋하지 않은 행을 버립니다.

---

## 다음 단계

변경을 저장하고 되돌려 보았습니다. 마지막으로 [안전하게 마치기](finish-safely.md)에서 튜토리얼
테이블과, 사용했다면 Docker 실습 환경을 제거합니다.
