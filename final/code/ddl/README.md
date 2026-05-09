# ddl — 테이블 정의

🔄 **Epic 3, 4**에서 채워질 폴더.

## 예정 파일

- `silver.sql` — `iceberg.ads.processed_events` 정의
  - 파티션: `event_date` (Iceberg hidden partitioning, 일 단위)
  - MERGE 키: `event_id`
- `gold.sql` — `iceberg.ads.campaign_summary_daily` 정의
  - 파티션: `summary_date` (월 단위)
  - PK: (`summary_date`, `campaign_id`)

> Bronze는 Iceberg가 아니라 Parquet 파일이므로 별도 DDL 없음. 컬럼 구조만 [docs/DATA_MODEL.md](../../../docs/DATA_MODEL.md) §2 참고.

## 상세 스키마

컬럼·타입·변환 규칙: [docs/DATA_MODEL.md](../../../docs/DATA_MODEL.md)
