# pycubrid.compat에서 벗어나기 (한국어)

> 🌐 [COMPAT_MIGRATION.md](https://github.com/cubrid-lab/pycubrid/blob/main/docs/COMPAT_MIGRATION.md)의 번역입니다. 영어 원문이 표준이며, CI가 영어 원문과의 구조 일치를 검사합니다.

옵트인 `pycubrid.compat` 네임스페이스(`pycubrid.compat.native`,
`pycubrid.compat.cubriddb`, `pycubrid.compat.cursors`)는 deprecated되었습니다.
이 네임스페이스는 공식 `CUBRIDdb`와 `_cubrid` 드라이버를 흉내 냅니다. 이 가이드는
무엇으로 대체해야 하는지, 그리고 Compat 기능 중 일반 API로 대체할 수 없는 것이
무엇인지 설명합니다.

## 무슨 일이 일어나고 있나

이제 Compat 연결을 만들면 이 페이지를 가리키는 `FutureWarning`이 발생합니다.
Compat은 계속 동작합니다. 이후의 1.x MINOR 릴리스에서 제거되며, 경고를 처음
내보내는 릴리스가 PyPI에 올라온 뒤 최소 28일(달력 기준)이 지난 다음에 제거됩니다.
제거 버전과 날짜는 아직 정해지지 않았습니다. 규칙은
[`RELEASE_POLICY.md`](https://github.com/cubrid-lab/pycubrid/blob/main/RELEASE_POLICY.md)
(2~4절)에 있습니다.

이것이 여러분에게 중요한 이유는 다음과 같습니다.

- `pycubrid`나 `pycubrid.aio`를 import할 때는 경고가 나오지 않습니다. 경고는
  Compat 사용자에게만 보입니다.
- 1.9.0 Upgrade note는 1.8.0에서 릴리스된 이름이 1.x가 끝날 때까지 유지된다고
  약속했습니다. 이 약속은 철회되었습니다. `RELEASE_POLICY.md` 4절이 이를
  공개적으로 밝힙니다.
- 마이그레이션하지 않은 코드는 제거 릴리스에서 import 오류로 실패합니다.

## 사용 위치 찾기

경고 문구는 다음과 같습니다.

```text
pycubrid.compat is deprecated and will be removed in a later 1.x MINOR release.
Use pycubrid or pycubrid.aio instead; see https://cubrid-lab.github.io/pycubrid/COMPAT_MIGRATION/
```

경고는 연결을 만드는 여러분의 코드 줄에 귀속됩니다. 기본적으로 Python은 그런 줄마다
한 번씩 경고를 보여 줍니다. 모든 사용 위치를 찾으려면 경고를 오류로 바꾸세요.

```bash
python -W error::FutureWarning your_app.py
pytest -W error::FutureWarning
```

자체 테스트가 이미 경고를 오류로 취급한다면, 이제 Compat 연결에서 테스트가
실패합니다. 마이그레이션하는 동안 이 경고만 잠시 끄려면 범위를 좁힌 필터를 쓰고,
끝나면 제거하세요.

```python
import warnings

warnings.filterwarnings(
    "ignore",
    message="pycubrid.compat is deprecated",
    category=FutureWarning,
)
```

`FutureWarning` 전체에 대한 무차별 `ignore`는 쓰지 마세요.

### 버전 고정하기

`pycubrid>=1.8.0,<2.0` 같은 범위는 코드를 바꾸지 않아도 제거 릴리스를 자동으로
설치할 수 있습니다. 제거 버전이 `CHANGELOG.md`에 공지되면, 마이그레이션을 마칠
때까지 그 버전 아래로 고정하세요. 예: `pycubrid>=1.8.0,<X.Y` (`X.Y`는 공지된 제거
버전).

## 이름만 바꾸면 틀립니다

아래 차이는 오류 없이 동작을 바꿉니다. 코드에서 하나씩 확인하세요.

| 주제 | Compat | `pycubrid.connect()` |
| --- | --- | --- |
| 트랜잭션 | 오토커밋으로 시작 | 수동 커밋으로 시작. `commit()`을 호출하지 않는 코드는 `autocommit=True`를 넘기지 않으면 쓰기를 잃음 |
| 기본 사용자 | `public` | `dba`. 항상 `user`를 지정하세요 |
| 바인딩 | 서버 측 타입 지정 바인드 | SQL 텍스트에 클라이언트 측 리터럴로 삽입. [파라미터 바인딩](PARAMETER_BINDING.md) 참고 |
| 오류 | 브로커 텍스트를 가림("prepared statement failed on server") | 오류와 `executemany(..., prepared=True)` 행 결과에 브로커 텍스트가 담기며, 여기에 바인드된 값이나 키가 들어 있을 수 있음. 로깅을 점검하세요 |
| 완전한 응답의 디코딩 불가 값 | `OperationalError`, 세션 종료 | `DataError`, 세션 유지 |
| 문장 풀링 | 브로커 풀링이 없으면 `native`의 `prepare()`가 `NotSupportedError`를 발생 | 필요 없음 |

## 연결

### 연결

`native.connect`, `native.connection`, 그리고 `cubriddb.Connect`,
`cubriddb.connect`, `cubriddb.connection`, `cubriddb.Connection`을 다룹니다.

```python
# before
from pycubrid.compat import native, cubriddb
n = native.connect("CUBRID:db.example:33000:testdb:::", "app", "secret")
w = cubriddb.connect("CUBRID:db.example:33000:testdb:::", "app", "secret", charset="euckr")
# after
import pycubrid
conn = pycubrid.connect(host="db.example", port=33000, database="testdb",
                        user="app", password="secret", autocommit=True, charset="euckr")
```

DSN 문자열은 없습니다. `autocommit=True`는 Compat의 기본값을 유지합니다. 수동
모드를 쓰려면 빼세요. Compat은 HA와 TLS URL 옵션을 거부합니다. 일반 연결은
`ssl`, 타임아웃, `fetch_size`를 받습니다([연결 옵션](CONNECTION.md) 참고). 네이티브
연결은 연결 시점에 서버 파라미터 세 개를 읽었지만 일반 연결은 읽지 않으므로,
`lock_timeout`, `max_string_len`, `isolation_level`은 사라집니다("격리 수준과 락 타임아웃" 참고).

### `cubriddb.Connection.connection`

대체 API가 없고 필요도 없습니다. 연결 객체는 하나뿐입니다.

## 트랜잭션과 세션 설정

### close, commit, rollback

이름은 같고 `None`을 반환합니다. 동작 하나가 다릅니다. 수동 모드에서 holdable로
선언된 Compat 결과는 `commit()` 뒤에도 계속 동작합니다. 일반 커서는 그렇지
않습니다.

```python
# before: manual mode, the result survives commit
n.set_autocommit(False); c = n.cursor(); c.prepare("SELECT id FROM big"); c.execute()
c.fetch_row(); n.commit(); c.fetch_row()      # continues across pages
# after: finish reading first
cur = conn.cursor(); cur.execute("SELECT id FROM big")
rows = cur.fetchall(); conn.commit()
```

30,000행으로 실제 실행한 결과, 네이티브 커서는 `commit()` 뒤에도 모든 행을
읽었습니다. 일반 커서는 버퍼에 있던 행이 소진되자 `InterfaceError: result set
invalidated before all rows were fetched`를 발생시켰습니다. 짧은 결과는 첫 응답에
다 들어가 문제를 가리므로, 작은 테스트는 통과할 수 있습니다. `autocommit=True`에서는
일반 `fetchall()`이 30,000행을 모두 읽었습니다.

### 오토커밋

`set_autocommit`, `get_autocommit`, `autocommit` 속성을 다룹니다.

```python
# before
n.set_autocommit(False); w.autocommit = False; w.get_autocommit()
# after
conn.autocommit = False; conn.autocommit
```

차이점은 다음과 같습니다.

- 네이티브의 대입은 캐시된 값만 바꿨습니다. 일반 연결의 대입은 서버에 실제로
  적용됩니다.
- 일반 연결의 대입은 `bool(value)`를 적용합니다. 래퍼는 bool이 아닌 값에
  `ValueError`를 발생시켰습니다.
- 일반 연결의 대입은 항상 SET과 COMMIT을 보내므로, 같은 값을 대입해도 열려 있는
  작업이 커밋됩니다. 네이티브는 모드가 실제로 바뀔 때만 커밋했습니다.
- 비동기: `await conn.set_autocommit(False)`.

### 격리 수준과 락 타임아웃

`set_isolation_level`, `isolation_level`, `lock_timeout`, `max_string_len`을
다룹니다.

```python
# before
n.set_isolation_level(5); n.isolation_level; n.lock_timeout; n.max_string_len
# after
cur.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
cur.execute("SET TRANSACTION LOCK TIMEOUT 5")
cur.execute("GET TRANSACTION ISOLATION LEVEL TO iso")
cur.execute("SELECT iso")
cur.fetchone()                            # (5,)
cur.execute("GET TRANSACTION LOCK TIMEOUT TO lk")
cur.execute("SELECT lk")
cur.fetchone()                            # (5.0,)
```

값 하나를 읽는 데 두 문장이 필요하며, 변수는 `@` 없이 선택합니다. `TO`를 빼면
`GET TRANSACTION` 문은 결과 집합을 돌려주지 않습니다. 네이티브 속성과 달리 매번 현재
세션을 읽습니다.

락 타임아웃은 `SET TRANSACTION LOCK TIMEOUT`과 같은 초 단위 `float`로 돌아오며,
`-1.0`은 무한 대기입니다. 네이티브 `lock_timeout`은 밀리초였습니다. `max_string_len`에는
대체 수단이 없습니다.

SQL로 지정한 설정은 드라이버가 세션을 교체하면 사라집니다. 다시 적용하세요([연결
옵션](CONNECTION.md) 참고). 네이티브는 설정을 "커밋하지 않고" 적용했습니다.
`SET TRANSACTION`도 커밋하지 않습니다. 열린 트랜잭션은 그대로 유지되며 롤백할 수
있습니다. 세션이 교체되면 위 읽기는 서버 기본값을 보여 주므로, 설정이 사라졌는지 확인할 수
있습니다.

### 버전과 ping

`server_version`, `client_version`, `ping`을 다룹니다.

```python
# before
n.server_version(); n.client_version(); n.ping()
# after
conn.get_server_version(); pycubrid.__version__; conn.ping(reconnect=False)
```

`ping`은 `1`을 반환했습니다. 일반 `ping`은 `True`를 반환하므로 값을 비교한다면
`int(...)`로 감싸세요. 이는 `select 1+1` 쿼리가 아니라 CHECK_CAS를 보냅니다. 기본값
`reconnect=True`는 세션을 교체할 수 있으며, 네이티브는 그러지 않았습니다. 쿼리로
확인하려면 다음과 같이 합니다.

```python
cur.execute("select 1+1 from db_root")
ok = cur.fetchone()[0] == 2
```

## 스키마 정보

### `native.connection.schema_info`

```python
# before
row = n.schema_info(4, "t", "s")                   # first row as a list, or None
# after: schema types 1 CLASS and 2 VCLASS use flag 1; 4 ATTRIBUTE and 5 CLASS_ATTRIBUTE use flag 2
packet = conn.get_schema_info(4, "t", 2, arg2="s")
try:
    rows = conn.fetch_schema_info(packet)
finally:
    conn.close_schema_info(packet)
row = list(rows[0]) if rows else None
if row is not None:
    row[1] = row[1] & 0xFFFF & ~0xE000             # DOMAIN, as native returned it
```

일반 호출은 모든 행을 튜플로 반환하며, 없으면 `[]`를 반환합니다. `SET(INT)`
컬럼으로 실제 실행한 결과 DOMAIN은 네이티브에서 `8`, 일반에서 `-24568`이었습니다.
위의 마스크를 적용하면 `8`이 됩니다. 스키마 요청이 실패했을 때 네이티브는 완전한
서버 오류라면 세션을 유지했습니다. 일반 `get_schema_info`는 세션을 버립니다. 일반
API는 더 많은 스키마 타입을 지원합니다.

## 커서와 문장

### 커서 만들기

`connection.cursor()`, `Connection.cursor(dictCursor)`, 직접 `Cursor(conn)`,
`native.cursor(owner)`를 다룹니다.

`conn.cursor()`를 쓰세요. 커서를 직접 생성하는 공개 대체 API는 없습니다.
`with conn.cursor() as cur:`를 쓰고 커서를 명시적으로 닫으세요.

### prepare, bind, execute, fetch

`prepare`, `bind_param`, `execute`, `fetch_row`를 다룹니다.

```python
# before
c = n.cursor(); c.prepare("SELECT id, v FROM t WHERE v = ?"); c.bind_param(1, "a")
count = c.execute(); row = c.fetch_row()
# after
cur = conn.cursor()
cur.execute("SELECT id, v FROM t WHERE v = ?", ("a",))   # returns the cursor
rows = cur.fetchall(); count = len(rows); row = rows[0] if rows else None
```

네이티브 `execute()`는 SELECT에서도 정수 개수를 반환했습니다. 일반 `execute()`는
커서를 반환하고, SELECT의 `rowcount`는 `-1`입니다. 재사용할 수 있는 핸들은
없습니다. SQL은 호출할 때마다 다시 렌더링됩니다. 서버 측 prepared SELECT에 해당하는
일반 API는 없습니다.

반복되는 DML의 경우:

```python
# before
c.prepare("INSERT INTO t(id, v) VALUES (?, ?)")
for a, b in rows:
    c.bind_param(1, a); c.bind_param(2, b); c.execute()
# after: server-side typed binds
cur.executemany("INSERT INTO t(id, v) VALUES (?, ?)", rows, prepared=True); conn.commit()
```

`prepared=True`는 마커가 하나 이상인 INSERT, UPDATE, DELETE, MERGE를 받습니다. 행
하나도 허용됩니다. 실패 시 동작은 "래퍼 `executemany`"를 보세요.

### 래퍼 `execute(query, args, set_type)`

```python
# before
n_rows = cur.execute("SELECT id FROM t WHERE id = ?", 1)
cur.execute("INSERT INTO t VALUES (?, ?)", (1, [1, 2]))
# after
from pycubrid import Set
cur.execute("SELECT id FROM t WHERE id = ?", (1,))
cur.execute("INSERT INTO t VALUES (?, ?)", (1, Set([1, 2])))
```

값이 하나여도 튜플로 감싸세요. 실제 실행에서 스칼라를 그대로 넘기면
`ProgrammingError: parameters must be a sequence`가 발생했습니다. 일반 `list`,
`tuple`, `set`도 오류를 냅니다. `Set`, `Multiset`, `Sequence` 중에서 고르세요.
`set_type`에 대응하는 기능은 없습니다. 렌더링된 리터럴의 요소 타입이 적용됩니다.
일반 API는 래퍼가 거부하던 `Decimal`, `float`, 날짜를 받습니다.

### 래퍼 `executemany`

```python
# before: returns None, stops at the first failing group
cur.executemany("INSERT INTO t(id, v) VALUES (?, ?)", rows)
# after, closest to stop-at-first-failure
for row in rows:
    cur.execute("INSERT INTO t(id, v) VALUES (?, ?)", row)
# after, batch: every row runs and failures are reported at the end
cur.executemany("INSERT INTO t(id, v) VALUES (?, ?)", rows, prepared=True)
```

세 행 중 가운데에 중복이 있는 경우로 실제 실행했습니다. 래퍼는 오토커밋에서 첫
행을 유지하고, 세 번째 행은 실행하지 않았으며, `IntegrityError`를 발생시켰습니다.
수동 모드의 `prepared=True`는 1행과 3행을 실행하고 2행을 되돌린 뒤
`IntegrityError`를 발생시켰으며, 커밋 여부는 호출자에게 맡겼습니다. 일반 호출은
커서를 반환합니다. 일반 `executemany`는 리터럴 배치를 보냅니다. 이 경우의 실패
동작은 래퍼와 비교하지 않았습니다.

## 행 읽기

### fetch와 결과 메타데이터

`fetchone`, `fetchmany`, `fetchall`, 반복, `next`, `close`, `description`,
`rowcount`, `arraysize`, `con`을 다룹니다.

fetch 이름은 같습니다. `cur.next()`는 `next(cur)`로 바꾸세요. `cur.con`은
없어졌으므로 연결에 대한 참조는 직접 보관하세요. `description`은 다릅니다. 실제
실행 결과는 래퍼가 `('id', 8, 0, 0, 10, 0, 0)`, 일반이
`('id', 8, None, None, 10, 0, False)`였습니다. 크기 필드는 `None`이고 `null_ok`는
`bool`입니다. 읽기 전용입니다. `fetchmany(0)`은 일반 커서에서 경고를 내고 래퍼에서는
내지 않습니다.

### 딕셔너리 행

```python
# before
cur = w.cursor(True); cur.execute(sql); cur.fetchone()
# after
cur = conn.cursor(); cur.execute(sql)
names = [d[0] for d in cur.description]
rows = [dict(zip(names, r)) for r in cur.fetchall()]
```

실제 실행 결과는 동일했습니다. 컬럼 이름이 중복되면 양쪽 모두 마지막 것이
우선합니다.

### `set_fetch_value_converter`

대체 API가 없습니다. 호출하는 쪽에서 변환하세요.

```python
rows = [convert(r, cur.description) for r in cur.fetchall()]
```

두 번째 인자는 "fetch와 결과 메타데이터"에서 설명한 형태이므로, 그 인덱스 2, 3, 6을 읽는 변환기는 다른
값을 보게 됩니다.

## 컬렉션

### SET, MULTISET, SEQUENCE

`connection.set`, `native.set`, `set.imports`, `bind_set`을 다룹니다.

```python
# before
s = n.set(); s.imports((1, 2, 2), 8)
c.prepare("INSERT INTO t VALUES (?, ?)"); c.bind_param(1, 1); c.bind_set(2, s); c.execute()
# after, literal
from pycubrid import Set, Multiset, Sequence
cur.execute("INSERT INTO t VALUES (?, ?)", (1, Set([1, 2, 2])))
# after, typed
cur.executemany("INSERT INTO t VALUES (?, ?)", [(1, Set([1, 2]))], prepared=True)
```

`kind=` 인자는 선택한 클래스에 대응합니다. 타입 지정 경로에서 `Multiset`은 네이티브와
같이 SEQUENCE로 전송됩니다. 리터럴 경로는 `MULTISET{...}`로 렌더링합니다. 실제
실행에서 저장된 세 행은 모두 같게 디코딩되었습니다. 두 드라이버 모두 컬렉션
컬럼을 기본적으로 원시 `bytes`로 반환합니다. `frozenset`과 `list` 값을 받으려면
`pycubrid.connect(decode_collections=True)`를 쓰세요. 네이티브에는 이런 옵션이
없었습니다. 한 번도 import하지 않은 네이티브 set은 NULL로 바인드되었습니다. 대신
`None`을 넘기세요.

## 대용량 객체(LOB)

### BLOB와 CLOB

`connection.lob`, `native.lob`, `bind_lob`, `fetch_lob`,
`lob.write/read/seek/imports/export/close`, `SEEK_*` 상수를 다룹니다.

```python
# before: write
l = n.lob(); l.write(b"data", "B")
c.prepare("INSERT INTO t(id, b) VALUES (?, ?)"); c.bind_param(1, 1); c.bind_lob(2, l); c.execute()
# after: autocommit must be off
lob = conn.create_lob(23)                 # 23 is BLOB, 24 is CLOB
offset = lob.write(b"data")
offset += lob.write(b"more", offset)      # the offset is explicit and append-only
cur.executemany("INSERT INTO t(id, b) VALUES (?, ?)", [(1, lob)], prepared=True)
conn.commit()

# before: read
l = n.lob(); c.prepare("SELECT c FROM t WHERE id = ?"); c.bind_param(1, 2); c.execute()
c.fetch_lob(1, l); text = l.read(); l.seek(0, native.SEEK_SET)
# after
cur.execute("SELECT c FROM t WHERE id = ?", (2,))
cell = cur.fetchone()[0]                  # None for SQL NULL
lob = pycubrid.Lob(conn, cell["lob_type"], cell["packed_lob_handle"])
text = lob.read(cell["lob_length"]).decode("utf-8")

# before: files
l.imports("in.bin", "B"); l.export("out.bin")
# after: not atomic; write to a temporary file and os.replace() for the same guarantee
file_lob = conn.create_lob(23)            # 새 LOB: 쓰기는 이어 쓰기만 가능
with open("in.bin", "rb") as fh:
    offset = 0
    while chunk := fh.read(65536):
        offset += file_lob.write(chunk, offset)
with open("out.bin", "wb") as fh:
    fh.write(file_lob.read(offset))
```

| 주제 | 네이티브 | 일반 (실제 실행) |
| --- | --- | --- |
| 단일 행 바인드 | `bind_lob`과 `execute` | `execute(sql, (lob,))`는 `ProgrammingError`를 발생. `executemany(..., prepared=True)`를 쓰세요 |
| 오토커밋 | 허용. 첫 문장이 임시 핸들을 소비함 | `NotSupportedError`로 거부 |
| 읽기 타입 | `str`, UTF-8로 디코딩. UTF-8이 아닌 BLOB은 `UnicodeDecodeError` | `bytes` |
| 위치 | 상태를 가진 `seek` | 오프셋을 직접 보관. `SEEK_*`는 쓸 곳이 없음. 자체 코드에서 `os.SEEK_*`의 의미를 쓰세요 |
| 같은 연결에서 가져온 핸들 재바인드 | 가능 | 가능(셀로 만든 `Lob`) |
| 다른 연결에서 재바인드 | 커밋된 행에 한해 가능 | 바이트를 읽어 새 LOB에 쓰세요. 다른 연결의 `Lob`은 `InterfaceError`를 발생시킵니다. `Lob(target, ...)`으로 핸들을 다시 감싸면 거부되지 않지만 다른 트랜잭션이 커밋하지 않은 데이터가 저장될 수 있으니 이 방식에 의존하지 마세요 |
| 추가(append)가 아닌 오프셋에 쓰기 | I/O 전에 `NotSupportedError` | 서버가 오류 -1016 반환 |
| 비동기 | 없음 | 없음 |

## 일반 대체 API가 없는 기능

일반 API가 없는 Compat 기능입니다. 우회 방법이 있으면 함께 적었습니다.

| Compat 기능 | 일반 API 상태 | 할 일 |
| --- | --- | --- |
| `cubriddb.Connection.connection` | 대체 API 없음, 필요 없음 | 하나뿐인 연결 객체를 사용 |
| `max_string_len` | 대체 수단 없음 | 없음 |
| `set_fetch_value_converter` | 대체 API 없음 | 호출하는 쪽에서 행을 변환 |
| 래퍼 `execute`의 `set_type` 인자 | 대체 수단 없음 | 리터럴의 요소 타입이 적용됨 |
| `result_info` | 대체 API 없음 | 아래 참고 |
| `data_seek`, `row_seek`, `row_tell` | 대체 API 없음 | 아래 참고 |
| `commit()` 뒤에도 살아 있는 결과 | 큰 결과에서는 지원 안 함 | commit 전에 다 읽거나 두 번째 연결을 사용 |
| 오토커밋에서의 LOB 바인드 | 거부됨 | 수동 트랜잭션 사용 |
| 가져온 LOB을 다른 연결에서 재바인드 | 대체 수단 없음 | 바이트를 읽어 새 LOB에 쓰기 |
| 서버 측 prepared SELECT, 재사용 문장 핸들 | 대체 수단 없음 | 리터럴을 렌더링하는 `execute()` 사용 |

### `result_info`

대체 API가 없습니다. `description`은 이름, 타입, 정밀도, 스케일, null 허용 여부를
제공합니다. 실제 컬럼 이름, 테이블 이름, 기본값, auto-increment, unique, 기본 키,
외래 키, reverse index, shared 플래그는 일반 커서에서 공개되지 않습니다. 알려진
테이블이라면 `get_schema_info`가 일부를 제공합니다. 임의의 쿼리 컬럼에는
대응시킬 수 없습니다.

### `data_seek`, `row_seek`, `row_tell`

대체 API가 없습니다. 행을 모두 메모리에 올리거나 SQL로 페이징하세요.

```python
rows = cur.fetchall(); row = rows[n - 1]
cur.execute("SELECT id FROM t ORDER BY id LIMIT ?, ?", (offset, size))
```

모두 올리면 결과 크기에 비례해 메모리를 씁니다. `LIMIT` 형식은 이 가이드를 위해
실행해 보지 않았습니다.

## 확인한 것과 확인하지 않은 것

- "실제 실행" 결과는 CUBRID 11.4.6과 Python 3.12에서 한 번 관찰한 것입니다. 10.2,
  11.0, 11.2는 확인하지 않았습니다.
- `SET TRANSACTION`과 `GET TRANSACTION` 결과는 10.2, 11.0, 11.2, 11.4의 동기와 비동기에서
  실제로 확인했습니다.
- 확인하지 않은 것: `max_string_len`을 읽는 방법, 일반 `executemany`와 래퍼의 실패 동작
  비교, `LIMIT ?, ?` 형식.
- Compat의 비공개 사용은 셀 수 없으므로, 아무도 의존하지 않는다고 프로젝트가 말할
  수는 없습니다.
