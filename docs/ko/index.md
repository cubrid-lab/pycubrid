# pycubrid

C 확장 없이 순수 Python으로 작성된 CUBRID 데이터베이스용 DB-API 2.0 드라이버입니다.

> English: [documentation home](../index.md)

## 주요 기능

- PEP 249 (DB-API 2.0)를 완전히 준수하는 연결 및 커서 인터페이스
- 순수 Python으로 직접 구현한 CUBRID CAS 와이어 프로토콜
- 최신 IDE와 정적 분석을 위한 타입 패키지 지원 (`py.typed`)
- CLOB 및 BLOB 작업을 위한 LOB 지원

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

- [시작하기](CONNECTION.md)
- [사용자 가이드](TYPES.md)
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
- sqlalchemy-cubrid — SQLAlchemy 2.0–2.2 다이얼렉트 + Alembic
- cubrid-cookbook-python — 실행 가능한 예제 68개와 애플리케이션 템플릿
- cubrid-mcp-server — MCP 서버 — LLM 클라이언트를 위한 자연어 접근
