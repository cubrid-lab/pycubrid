# pycubrid (한국어)

> 🌐 [index.md](https://github.com/cubrid-lab/pycubrid/blob/main/docs/index.md)의 번역입니다. 영어 원문이 표준이며, CI가 영어 원문과의 구조 일치를 검사합니다.

C 확장 없이 순수 Python으로 작성된 CUBRID 데이터베이스용 DB-API 2.0 드라이버입니다.

> English: [documentation home](../index.md)

## 주요 기능

- PEP 249 (DB-API 2.0)를 완전히 준수하는 연결 및 커서 인터페이스
- 순수 Python으로 직접 구현한 CUBRID CAS 와이어 프로토콜
- 최신 IDE와 정적 분석을 위한 타입 패키지 지원 (`py.typed`)
- CLOB 및 BLOB 작업을 위한 LOB 지원

## 여기서 시작하세요

pycubrid는 Python 프로그램이 CUBRID 데이터베이스와 대화하게 해 줍니다. CUBRID는 이미 실행 중이어야
하는 별도의 서버이며, pycubrid를 설치해도 CUBRID가 설치되지는 않습니다.
[처음으로 CUBRID에 연결하기](quickstart.md)는 데이터베이스 준비(이미 있는 데이터베이스 또는 Docker로
띄우는 일회용 로컬 데이터베이스)부터 첫 쿼리와 첫 행 저장까지 안내합니다.

## 빠른 설치

```bash
pip install pycubrid
```

## 최소 예제

```python
import pycubrid

conn = pycubrid.connect(host="localhost", port=33000, database="testdb", user="dba", password="")
cur = conn.cursor()
cur.execute("SELECT 1")
print(cur.fetchone())
conn.close()
```

## 문서

- [처음으로 CUBRID에 연결하기](quickstart.md)
- [연결 가이드](CONNECTION.md)
- [타입 시스템](TYPES.md)
- [파라미터 바인딩 계약](PARAMETER_BINDING.md)
- [API 참조](API_REFERENCE.md)

## 프로젝트 링크

- [GitHub](https://github.com/cubrid-lab/pycubrid)
- [PyPI](https://pypi.org/project/pycubrid/)
- [변경 이력](https://github.com/cubrid-lab/pycubrid/blob/main/CHANGELOG.md)
- [기여하기](../CONTRIBUTING.ko.md)

## 생태계

cubrid-lab Python 생태계의 일부입니다:

- **pycubrid** — CUBRID용 순수 Python DB-API 2.0 드라이버 (동기 + 네이티브 asyncio)
- sqlalchemy-cubrid — SQLAlchemy 2.0–2.1 다이얼렉트 + Alembic
- cubrid-cookbook-python — 실행 가능한 예제 68개와 애플리케이션 템플릿
- cubrid-mcp-server — MCP 서버 — LLM 클라이언트를 위한 자연어 접근
