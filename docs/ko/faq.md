# 자주 묻는 질문 (한국어)

> 🌐 [faq.md](https://github.com/cubrid-lab/pycubrid/blob/main/docs/faq.md)의 번역입니다. 영어 원문이 표준이며, 페이지 번역은 경고 수준의 동기화 규칙을 따릅니다.

pycubrid 사용 관련 자주 묻는 질문과 실용적인 답변.

---

??? "원격 CUBRID 서버에는 어떻게 연결하나요?"
    `connect()`에 원격 호스트와 브로커 포트를 직접 사용하세요.

    ```python
    from __future__ import annotations

    import pycubrid

    conn = pycubrid.connect(
        host="db.example.com",
        port=33000,
        database="production",
        user="app_user",
        password="secret",
        connect_timeout=15.0,
    )
    ```

    !!! warning
        배포 환경이 브로커→CAS 포트 리다이렉션을 사용한다면, 리다이렉트되는 CAS 포트가 방화벽/네트워크 규칙에서 허용되는지 확인하세요.

??? "어떤 Python 버전을 지원하나요?"
    pycubrid는 패키지 지원 매트릭스와 CI에서 테스트된 버전을 따릅니다.

    !!! note
        최신 호환성 표는 [지원 매트릭스](SUPPORT_MATRIX.md)를 확인하세요.

??? "LOB 데이터는 어떻게 다루나요?"
    대부분의 애플리케이션에서는 파라미터화 SQL에 `str`/`bytes`를 직접 전달하면 됩니다.

    ```python
    cur.execute(
        "INSERT INTO docs (title, content) VALUES (?, ?)",
        ["Guide", "Large CLOB text..."],
    )
    ```

    세밀한 제어가 필요하면 `conn.create_lob(...)`과 반환되는 `Lob` 객체를 사용하세요.

    !!! tip
        LOB 메타데이터와 수명 주기에 대한 자세한 내용은 [예제](EXAMPLES.md#lob-handling)를 읽어 보세요.

??? "커넥션 풀링은 어떻게 쓰나요?"
    pycubrid 자체에는 내장 풀이 없습니다. 권장 옵션:

    1. SQLAlchemy 엔진 풀링 (`pool_size`, `max_overflow`, `pool_pre_ping`)
    2. 간단한 스크립트용 가벼운 애플리케이션 수준 큐 기반 풀

    ```python
    from sqlalchemy import create_engine

    engine = create_engine(
        "cubrid+pycubrid://dba@localhost:33000/testdb",
        pool_size=5,
        pool_pre_ping=True,
    )
    ```

??? "어떤 문자 인코딩을 지원하나요?"
    pycubrid는 표준 CUBRID 문자 타입과 Python `str` 값을 지원합니다.

    !!! note
        다국어 텍스트 워크로드(NCHAR/VARNCHAR)에서는 데이터베이스 콜레이션과 서버/클라이언트 문자셋 설정을 함께 확인하세요.

??? "에러는 어떻게 제대로 처리하나요?"
    구체적인 DB-API 예외(`IntegrityError`, `ProgrammingError`, `OperationalError`)를 먼저 잡고, 그다음 `Error`로 폴백하세요.

    ```python
    import pycubrid

    try:
        cur.execute("INSERT INTO users (email) VALUES (?)", ["duplicate@example.com"])
        conn.commit()
    except pycubrid.IntegrityError:
        conn.rollback()
        raise
    except pycubrid.Error:
        conn.rollback()
        raise
    ```

    분류별 에러 진단은 [문제 해결](TROUBLESHOOTING.md)을 참고하세요.
