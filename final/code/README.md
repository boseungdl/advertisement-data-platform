# code — 데이터 파이프라인 코드

3개 하위 폴더로 분리:

- [`ddl/`](ddl/) — Bronze/Silver/Gold 테이블 정의 (SQL)
- [`pipelines/`](pipelines/) — Kafka→Bronze, Bronze→Silver, Silver→Gold 처리 로직 (Python)
- [`health-queries/`](health-queries/) — 운영 헬스 체크 SQL 쿼리

## 채워질 시점

- Epic 2 (Bronze 파이프라인) → `pipelines/kafka_to_bronze.py`
- Epic 3 (Silver 파이프라인) → `ddl/silver.sql`, `pipelines/bronze_to_silver.py`
- Epic 4 (Gold 파이프라인) → `ddl/gold.sql`, `pipelines/silver_to_gold.py`
- Epic 6 (헬스 + 대시보드) → `health-queries/*.sql`
