# 행을 저장하고 다시 읽기 (한국어)

> 🌐 [save-a-row.md](https://github.com/cubrid-lab/pycubrid/blob/main/docs/save-a-row.md)의 번역입니다. 영어 원문이 표준이며, CI가 영어 원문과의 구조 일치를 검사합니다.

이 단원에서는 작은 테이블을 만들어 행 하나를 저장하고, 새 연결에서 그 행을 읽습니다.
[첫 쿼리 실행하기](first-query.md) 다음에 이어지는 단원입니다.

---

## 시작하기 전에

앞 단원에서 마련한 것이 필요합니다.

- [첫 쿼리 실행하기](first-query.md)의 `first_query.py`가 `(2,)`를 출력해, 다섯 가지 연결 값이
  동작하는 것을 확인했어야 합니다.
- 같은 폴더와 활성화된 가상 환경(새 터미널이면 `source .venv/bin/activate`를 다시 실행)

이 단원은 사용자 계정에 `pycubrid_quickstart_notes`라는 테이블을 만듭니다.
[시작하기 전에](quickstart.md#시작하기-전에)에서 정한 연습용 데이터베이스나 계정을 쓰고, 운영
데이터베이스는 쓰지 마세요.

---

## 프로그램 작성하기

같은 폴더에 `first_saved_row.py`를 만듭니다. Docker 실습 환경을 쓰지 않는다면 `SETTINGS`에 직접 쓸
다섯 가지 값을 넣으세요.

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
cur.close()
conn.close()

# 3. Check from a brand-new connection that the committed row was saved.
check = pycubrid.connect(**SETTINGS)
check_cur = check.cursor()
check_cur.execute("SELECT id, note FROM pycubrid_quickstart_notes ORDER BY id")
print("Rows seen by a new connection:", check_cur.fetchall())
check_cur.close()
check.close()
```

---

## 실행하기

```bash
python first_saved_row.py
```

두 번째 연결에서도 저장한 행이 보입니다.

```text
Rows seen by a new connection: [(1, 'my first saved row')]
```

---

## 프로그램이 한 일

- `SETTINGS`는 다섯 가지 연결 값을 한 번만 적어 두고, `connect(**SETTINGS)`는 그 값을 키워드 인자로
  넘깁니다. 그래서 두 연결이 같은 값을 씁니다.
- `CREATE TABLE`이 튜토리얼 테이블을 만들었습니다. 테이블의 주석은 이 테이블이 튜토리얼의 것임을
  표시하며, [안전하게 마치기](finish-safely.md)는 무언가를 삭제하기 전에 이 주석을 확인합니다.
- `?` 표시는 자리 표시자입니다. pycubrid는 문장을 보내기 전에 각 `?`를 튜플의 해당 값으로 안전하게
  이스케이프해 바꿉니다. 값을 SQL 문자열에 직접 붙여 넣지 말고 항상 이 방법으로 전달하세요. 각 Python
  타입을 어떻게 보내는지는 [파라미터 바인딩](PARAMETER_BINDING.md)에 있습니다.
- `commit()`은 변경을 영구적으로 만듭니다. 커밋하기 전에는 다른 연결에 변경이 보이지 않습니다. 새 연결
  `check`가 행이 저장되었음을 보여 줍니다.
- `fetchall()`은 결과에 남은 모든 행을 튜플의 리스트로 읽습니다.

---

## 다시 실행하면

파일을 두 번 실행하거나 중단된 뒤 다시 실행하면 첫 문장에서
`Class dba.pycubrid_quickstart_notes already exists`로 멈춥니다(소유자 이름 `dba`는 사용자 이름입니다).
이전 실행에서 만든 테이블이 아직 남아 있다는 뜻입니다. 직접 삭제하지 말고
[안전하게 마치기](finish-safely.md#튜토리얼-테이블-정리하기)의 정리 스크립트를 실행한 뒤
`first_saved_row.py`를 다시 실행하세요.

---

## 다음 단계

행이 저장되었습니다. 다음으로 [커밋과 롤백](commit-and-rollback.md)에서 두 번째 행을 추가하고, 저장되기
전에 되돌려 봅니다.
