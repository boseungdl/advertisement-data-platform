# orchestration — Airflow DAG

🔄 **Epic 5**(매니지먼트 자동화) + **Epic 1.2** (Airflow 컨테이너 셋업)에서 채워질 폴더.

## 예정 DAG 5개

| DAG 파일 | 주기 | 역할 |
|---|---|---|
| `ads_silver_merge_15min.py` | `*/15 * * * *` | Bronze → Silver MERGE |
| `ads_gold_summary_hourly.py` | `5 * * * *` | Silver → Gold 7일 rebuild |
| `ads_iceberg_compaction_daily.py` | `0 3 * * *` (03:00 KST) | 작은 파일 합치기 |
| `ads_iceberg_expire_daily.py` | `0 4 * * *` (04:00 KST) | 30일 초과 옛 스냅샷 만료 |
| `ads_iceberg_orphan_weekly.py` | `0 5 * * 0` (일 05:00 KST) | 고아 파일 청소 |

> Spark Streaming(Kafka → Bronze)은 별도 detached 컨테이너로 항상 가동. Airflow가 관리하지 않음.

## 스케줄 충돌 회피

- 03:00~05:00 KST 매니지먼트 시간대에는 streaming MERGE를 일시 정지(또는 시간대 분리)
- 이유: 컴팩션과 streaming MERGE가 같은 파티션을 동시에 건드리면 OCC 충돌 (Iceberg가 감지·중재하지만 운영적으로 분리하는 게 안전)

## 상세 의존성 매트릭스

[docs/ARCHITECTURE.md §7](../../docs/ARCHITECTURE.md#7-스케줄--오케스트레이션)
