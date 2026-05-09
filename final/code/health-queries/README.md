# health-queries — 운영 헬스 체크 쿼리

🔄 **Epic 6**에서 채워질 폴더. final-project 평가 기준 ① "운영자 5분 헬스체크"의 핵심.

## 예정 쿼리 5~10개

| 파일 | 무엇을 검사 | Iceberg 메타 활용 |
|---|---|---|
| `freshness.sql` | 최근 이벤트 시각 vs 현재 시각 (지연 측정) | `processed_events` MAX(event_time) |
| `row_count_consistency.sql` | Bronze/Silver/Gold 일자별 행 수 일치 | 각 단계 COUNT 비교 |
| `iceberg_files.sql` | 파티션당 파일 수 + 평균 크기 | `*.files` 메타 테이블 |
| `iceberg_snapshots.sql` | 스냅샷 누적 수 + Expire 작동 확인 | `*.snapshots` 메타 테이블 |
| `iceberg_history.sql` | 최근 snapshot 작업 이력 (어느 잡이 무엇을) | `*.history` 메타 테이블 |
| `iceberg_orphan.sql` | 카탈로그에서 참조 없는 고아 파일 검출 | manifests 비교 |
| `idempotency_check.sql` | 같은 event_id 중복 행 검출 (멱등성 위반 감지) | DISTINCT vs COUNT 차이 |
| `merge_lag.sql` | Bronze 도착 → Silver 반영 지연 (P50/P95/P99) | `ingested_at - kafka_timestamp` |
| `attribution_diff.sql` | Last-click vs Even-credit 결과 차이 분석 | 두 컬럼 합계 비교 |

## 사용 위치

- Athena 콘솔에서 직접 실행 (운영자 5분 체크)
- QuickSight 대시보드 운영 탭의 패널 데이터 소스
