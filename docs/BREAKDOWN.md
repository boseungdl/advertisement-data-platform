# BREAKDOWN — 작업 분해 (Epic / Story)

> 워크플로우 가이드 §4 작업 분해. 사전 계획은 **Story까지만**. Task(=PR=머지 커밋 1개)는 작업 시작 직전 5분 투자해서 쪼갠다.
> 작성일: 2026-05-09 / 단계: 3단계 작업 분해

---

## 계층 구조

```
프로젝트 (광고 캠페인 분석 + 운영 모니터링)
  └─ Epic         (큰 기능 묶음, 며칠~1주 단위)
      └─ Story    (Epic 안의 의미 있는 단위, 0.5~2일)
          └─ Task = PR = 머지 커밋 1개  ← 시작 직전에 5~7개로 쪼갬
```

**Task = PR = 머지 커밋 1개** (Squash Merge 기준)
- 변경 파일 1~10개, 줄수 50~400, 리뷰 30분 안에 가능
- 한 가지 일만 함, 독립 롤백 가능

---

## MVP 경계

PRD §3 MoSCoW 기준:
- **Must (MVP, 먼저 끝낼 것)**: M1 파이프라인 3계층 / M2 MERGE 멱등성 / M3 Iceberg 매니지먼트 자동화 / M4 헬스 쿼리 / M5 대시보드 탭 2개
- **Should (MVP 끝나면)**: S1 어트리뷰션 2종 / S2 100x ADR / S3 RUNBOOK / S4 README 평가 답변
- **Could (여유 있을 때)**: C1 봇 필터 / C2 백필 자동화

---

## Epic 7개와 진행 순서

```
[Epic 1] 환경 셋업
   ↓
[Epic 2] Bronze 파이프라인 (M1)
   ↓
[Epic 3] Silver 파이프라인 (M1, M2)  ──┐
   ↓                                    │
[Epic 4] Gold 파이프라인 (M1)           │
   ↓                                    │
[Epic 5] Iceberg 매니지먼트 (M3)        ├── 병렬: [Epic 7] 문서화
   ↓                                    │       (ADR / README / RUNBOOK)
[Epic 6] 헬스 쿼리 + 대시보드 (M4, M5)─┘
```

---

## Epic 1 — 환경 셋업

> 목적: 로컬 도커(Kafka/Spark/Airflow) + AWS(S3/Glue) 인프라 띄우기. 코드 작성 전 준비.

| Story | 내용 | 예상 PR 수 |
|---|---|---|
| S1.1 | docker-compose 골격 (spark-iceberg + kafka + zookeeper) — `iceberg-lakehouse-lab` 베이스 활용 | 1 |
| S1.2 | Airflow 컨테이너 추가 (LocalExecutor) + DAG 폴더 마운트 | 1~2 |
| S1.3 | AWS 자격증명 + S3 버킷 + Glue DB 생성 | 1 |
| S1.4 | Spark Iceberg 카탈로그 연결 (로컬 + Glue 둘 다) | 1 |
| S1.5 | Criteo 데이터 다운로드 + 샘플 100K 추출 | 1 |

---

## Epic 2 — Bronze 파이프라인 (M1)

> 목적: Criteo CSV → Kafka → 로컬 raw parquet 적재. `iceberg-lakehouse-lab` `kafka_producer.py` + `kafka_to_raw_files.py` 베이스.

| Story | 내용 | 예상 PR 수 |
|---|---|---|
| S2.1 | Kafka topic 생성 + producer로 샘플 1K 발행 검증 | 1 |
| S2.2 | Spark Structured Streaming → 로컬 raw parquet 적재 (5분 micro-batch) | 1~2 |
| S2.3 | 체크포인트 동작 검증 (재시작 → 이어 읽기) | 1 |
| S2.4 | Bronze 파티션 구조 (`raw_date/raw_hour`) 확정 | 1 |

---

## Epic 3 — Silver 파이프라인 (M1, M2 — 차별화 핵심)

> 목적: raw → Iceberg `processed_events` MERGE. **late-arriving conversion 멱등성이 본 게임**.

| Story | 내용 | 예상 PR 수 |
|---|---|---|
| S3.1 | `iceberg.ads.processed_events` DDL (`code/ddl/silver.sql`) | 1 |
| S3.2 | raw → Silver MERGE 로직 (`raw_to_processed_iceberg.py` 베이스, `ingested_at` 비교) | 2 |
| S3.3 | 멱등성 자동 테스트 (같은 윈도우 2회 → 결과 동일) | 1 |
| S3.4 | late-arriving conversion 시뮬레이션 + 검증 (5일 늦은 conv 흘려보내기) | 1 |
| S3.5 | Last-click / Even-credit 두 어트리뷰션 컬럼 채우기 (S1) | 1 |
| S3.6 | Airflow DAG `ads_silver_merge_hourly` 등록 | 1 |

---

## Epic 4 — Gold 파이프라인 (M1)

> 목적: Silver → `campaign_summary_daily` 일 단위 집계. 7일 윈도우 rebuild.

| Story | 내용 | 예상 PR 수 |
|---|---|---|
| S4.1 | `iceberg.ads.campaign_summary_daily` DDL (`code/ddl/gold.sql`) | 1 |
| S4.2 | Silver → Gold rebuild 로직 (`processed_to_campaign_summary.py` 베이스) | 2 |
| S4.3 | KPI 계산 검증 (CTR/CVR/ROI/평균 전환 지연) | 1 |
| S4.4 | Airflow DAG `ads_gold_summary_hourly` 등록 | 1 |

---

## Epic 5 — Iceberg 매니지먼트 자동화 (M3)

> 목적: 운영 사고력 어필. Compaction + Expire Snapshots + Orphan Cleanup 3종 모두 Airflow DAG로.

| Story | 내용 | 예상 PR 수 |
|---|---|---|
| S5.1 | Compaction DAG `ads_iceberg_compaction_daily` (03:00 KST) | 1~2 |
| S5.2 | Expire Snapshots DAG `ads_iceberg_expire_daily` (04:00 KST, 30일 초과) | 1 |
| S5.3 | Orphan Cleanup DAG `ads_iceberg_orphan_weekly` (일 05:00 KST) | 1 |
| S5.4 | 매니지먼트 시간대 분리 검증 (streaming MERGE 충돌 회피) | 1 |
| S5.5 | 매니지먼트 효과 측정 쿼리 (파일 수 N→M, 저장 비용 변화) | 1 |

---

## Epic 6 — 헬스 쿼리 + 대시보드 (M4, M5)

> 목적: "운영자 5분 헬스체크" 시연 + 캠페인 매니저 비즈니스 화면.

| Story | 내용 | 예상 PR 수 |
|---|---|---|
| S6.1 | Iceberg 메타 테이블 헬스 쿼리 5~10개 (`code/health-queries/*.sql`) | 1~2 |
| S6.2 | QuickSight 데이터셋 등록 (Athena 연결) | 1 |
| S6.3 | 비즈니스 탭: 캠페인별 ROI/CTR/CVR, 어트리뷰션 비교 | 1~2 |
| S6.4 | 운영 탭: 신선도, 행 수, 파일 수, 스냅샷 수, 매니지먼트 실행 이력 | 1~2 |
| S6.5 | 대시보드 스크린샷 캡처 + `dashboard/` 폴더에 저장 | 1 |

---

## Epic 7 — 문서화 (Epic 1~6과 병렬, S2~S4)

> 목적: 협업·지속 가능성 어필. 결정 박제.

| Story | 내용 | 예상 PR 수 |
|---|---|---|
| S7.1 | ADR-001~006 본문 (각 1~2 페이지) — Bronze Parquet / Silver Iceberg / Glue / Local Spark / Airflow / QuickSight | 6 |
| S7.2 | README 9개 평가 섹션 작성 (`final-project-guide.md` §5) | 1~2 |
| S7.3 | RUNBOOK 1건 — Spark OOM 재시작 절차 | 1 |
| S7.4 | ADR-007 — 100x 스케일 시나리오 (구현 X, 설계만) | 1 |
| S7.5 | 발표 슬라이드 5~9 작성 (매니지먼트, 헬스, 100x, 결과, 회고) | 1 |

---

## 진행 원칙

1. **한 번에 하나의 Story**만 진행. 끝나면 다음.
2. Story 시작 시 **4단계 Pre-flight 5분 투자**: 5줄 스펙 → AI 비판 → 커밋 5~7개 분해.
3. 각 Task = PR. Squash Merge.
4. 매 PR마다 보안 체크리스트 통과 (시크릿 / IAM / PII / 비용).

---

## 갱신 이력

- 2026-05-09: 1차 분해 (Epic 7개 / 총 Story 31개 추정)
- 2026-05-09: `final/` 산출물 디렉토리 골격 추가 (S0.1 — 6개 폴더 + README들 + 루트 README + 평가 답변 9개 섹션 골격)
- 🔄 진행 중 Story 쪼개기/합치기 발생하면 갱신
