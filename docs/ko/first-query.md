# 첫 쿼리 실행하기 (한국어)

> 🌐 [first-query.md](https://github.com/cubrid-lab/pycubrid/blob/main/docs/first-query.md)의 번역입니다. 영어 원문이 표준이며, CI가 영어 원문과의 구조 일치를 검사합니다.

이 단원에서는 CUBRID에 연결해 SQL 문 하나를 보내고, 그 결과로 돌아온 행 하나를 읽습니다.
데이터베이스와 Python을 준비하는 [처음으로 CUBRID에 연결하기](quickstart.md) 다음에 이어지는
단원입니다.

---

## 시작하기 전에

준비 단원에서 마련한 것이 필요합니다.

- 응답하는 CUBRID 데이터베이스: Docker 실습 환경이 `CUBRID is ready`를 출력했거나
  ([시작하기 전에](quickstart.md#시작하기-전에)), 직접 쓸 데이터베이스의 다섯 가지 값을 알고 있어야
  합니다.
- 가상 환경에 설치한 pycubrid([1단계](quickstart.md#1단계-가상-환경에-pycubrid-설치하기))

1단계와 같은 폴더에서 작업하세요. 새 터미널을 열었다면 `source .venv/bin/activate`로 가상 환경을
다시 활성화하세요. 각 단계는 터미널에서 실행하는 파일이므로, 앞서 열어 둔 Python 세션에서 이어지는
것은 없습니다.

---

## 프로그램 작성하기

그 폴더에 `first_query.py` 파일을 만듭니다.

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

---

## 실행하기

```bash
python first_query.py
```

값 하나가 든 행 하나가 출력됩니다.

```text
(2,)
```

오류가 나면 마지막 줄을 [첫 연결이 실패할 때](quickstart.md#첫-연결이-실패할-때)에서 찾아보세요.

---

## 프로그램이 한 일

- `pycubrid.connect()`는 데이터베이스와의 세션인 **연결**(connection)을 엽니다. 호스트, 포트,
  데이터베이스, 사용자, 비밀번호의 다섯 가지 값이 필요합니다.
- `conn.cursor()`는 **커서**(cursor)를 만듭니다. 커서는 연결 안에서 일하며 SQL 문을 보내고 그
  결과를 읽습니다.
- `cur.execute()`는 SQL 문 하나를 서버로 보냅니다.
- `cur.fetchone()`은 결과에서 한 행을 읽습니다. 행은 튜플이므로 값 `2` 하나는 `(2,)`로
  출력됩니다.
- `close()`는 커서를, 그다음 연결을 닫습니다. 서버가 쓰지 않는 세션을 계속 붙잡고 있지 않도록,
  연 것은 닫으세요.

`connect()`는 기본적으로 서버에 닿는 데 자체 시간 제한을 두지 않습니다. 방화벽이 연결을 소리 없이
버리면 운영 체제가 포기할 때까지 기다립니다. 다른 컴퓨터의 데이터베이스라면 `connect()` 호출에 예를
들어 `connect_timeout=10`을 추가하세요. [연결 가이드](CONNECTION.md#커스텀-포트와-타임아웃)에서 이
옵션과 다른 타임아웃을 설명합니다.

---

## 다음 단계

첫 쿼리는 계산된 값만 읽었습니다. 다음으로 [행을 저장하고 다시 읽어 보세요](save-a-row.md). 작은
테이블을 만들어 행 하나를 저장하고, 새 연결에서 그 행을 읽습니다.
