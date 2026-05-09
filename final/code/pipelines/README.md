# pipelines — 처리 로직

🔄 **Epic 2~4**에서 채워질 폴더.

## 예정 파일

| 파일 | 역할 | 주기 | Epic |
|---|---|---|---|
| `kafka_to_bronze.py` | Kafka → 로컬 raw parquet 적재 | 5분 micro-batch (streaming) | 2 |
| `bronze_to_silver.py` | raw → Iceberg `processed_events` MERGE | 15분 주기 batch | 3 |
| `silver_to_gold.py` | Silver → `campaign_summary_daily` 7일 rebuild | 1시간 주기 batch | 4 |

## 작업 베이스

외부 데모 [`iceberg-lakehouse-lab/jobs/`](../../../iceberg-lakehouse-lab/jobs/)의 3개 잡 파일을 광고 도메인 + Iceberg 카탈로그(Glue) 설정에 맞춰 수정.

## 핵심 보장

- **멱등성**: `bronze_to_silver.py`의 MERGE는 `s.ingested_at > t.ingested_at` 조건으로 같은 윈도우 두 번 돌려도 결과 동일 ([docs/DATA_MODEL.md §5](../../../docs/DATA_MODEL.md#5-merge-멱등성-보장-핵심-설계))
- **체크포인트**: streaming은 Kafka offset 체크포인트로 새벽 재시작 안전
