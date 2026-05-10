# 최종 프로젝트 — 광고 캠페인 분석 + 어트리뷰션 + 운영 모니터링 데이터 레이크하우스

Criteo Attribution Dataset(30일·700+ 캠페인·16.4M 이벤트)을 광고대행사 fleet의 실시간 click/impression/conversion 스트림으로 가정한 **Iceberg 기반 메달리온 데이터 레이크하우스**. 광고 도메인의 본질인 **late-arriving conversion**(클릭은 오늘, 전환은 며칠 뒤)을 안전하게 처리하면서, 운영자가 **5분 안에 헬스체크** 가능한 가시성을 함께 제공.

> 셋업·실행 가이드(디렉토리 구조 / AWS 셋업 / 데이터 준비 / Bootstrap)는 [docs/SETUP.md](../docs/SETUP.md) 참고.

---

## 1. 도메인 정의 + 핵심 KPI 3개

### 도메인 분야

**광고 도메인의 클릭·노출·전환 이벤트를 메달리온 아키텍처로 관리하는 데이터 레이크하우스.**

> 데이터 본질·22 컬럼 흐름은 [docs/SETUP.md — 데이터 본질·구조](../docs/SETUP.md#데이터-본질구조-criteo-attribution-dataset).
> 사용자 페르소나(P1 캠페인 매니저 / P2 파이프라인 운영자)는 [docs/PRD.md](../docs/PRD.md).

### 데이터 규모와 패턴

가이드 §1 정형 템플릿 (광고 도메인 기준 직접 정의):

- **현재 규모**: 일 **100만 이벤트** (시간당 ~4만, 초당 ~12)의 작은 환경 가정
- **이벤트 크기**: 평균 ~1KB → **일 ~1GB raw, 월 ~30GB, 연 ~365GB**
- **트래픽 패턴 (광고 도메인 직접 정의)**: 피크 시간대(12~18시 + 21~23시) 평소의 **2~3배**, 주말 **1.5배** (시즌 스파이크 — 블프/광군제 3~5배 등 — 은 §7에서 깊이)
- **6개월 내 10x 성장 예상** — 일 **1,000만 이벤트**가 현실적 목표
- **장기 100x 가능성** — 1~2년 내 일 **1억 이벤트(일 ~100GB)** 까지 갈 수 있다고 가정. **스케일 아웃 설계 사고의 근거 (§7)** — 모든 아키텍처·스키마 결정이 이 가정을 통과해야 함

#### 처리량 환산 — 단계별 단단한 숫자

| 단계 | 일 이벤트 | 시간당 | **초당** | 일 raw | 월 raw |
|---|---|---|---|---|---|
| 현재 | 100만 | 4만 | **~12** | 1GB | 30GB |
| 10x (6개월) | 1,000만 | 40만 | **~120** | 10GB | 300GB |
| **100x (1~2년)** | **1억** | 400만 | **~1,200** | **100GB** | **3TB** |

피크 시 2~3배 가정 → 100x 피크 **초당 ~3,600**. 단일 Kafka broker(보통 1~2만 msg/s 한계)는 여유가 적음 → **다중 broker 필수**가 §7 결정의 핵심 근거.

### 핵심 KPI 3개

| 지표 | 목표 숫자 | 만족 못 하면 어떤 문제? |
|---|---|---|
| **처리량** | 일 100만 이벤트 처리 가능 | 광고 이벤트가 들어오는 속도를 못 따라가서 큐 적체·데이터 누락 |
| **신선도** | Gold까지 반영 지연 **P95 1시간 이내** *(Bronze = streaming, Silver/Gold = 1시간 주기)* | 분석가가 시간 지난 데이터로 잘못 판단. 어제 캠페인 결과가 늦게까지 안 보임 |
| **멱등성** | 같은 데이터 두 번 처리 → 결과 동일 (**`event_id` 중복 = 0건**) | 장애 후 재실행 시 데이터가 두 번 들어가 KPI 부풀려짐. 사람이 손으로 정리해야 함 |

> **P95** = "100번 측정했을 때 95번"이라는 뜻. 거의 항상 1시간 안에 반영됨.
> **100x 처리량 환산** (초당 1,200, 일 100GB)은 [§1-B 처리량 환산 표](#처리량-환산--단계별-단단한-숫자) / **알람 임계 디테일**은 [§5 운영 가시성](#5-운영-헬스-체크-쿼리-모음) / **장애 시 자동 복구 시연**은 [§8 RUNBOOK](#8-장애운영-시나리오) 참고.

> 어트리뷰션 모델 정의·ROI 임계값 같은 **분석 영역 결정은 분석가의 일**. 우리는 **데이터 엔지니어**로서 분석가가 그런 분석을 안전하게 할 수 있도록 **신선도·멱등성을 보장**하는 데 집중합니다. (어트리뷰션 두 모델 비교 같은 Silver 추가 컬럼은 메인 단단해진 후 Phase 2에서 가능.)

---

## 2. 전체 아키텍처

### 왜 이 컴포넌트 조합인가 — 페르소나 요구가 만든 결정

처음 든 의문: **"광고 데이터 처리에 굳이 레이크하우스가 필요한가? 그냥 Glue ETL로 batch 변환해서 RDS·웨어하우스에 넣으면 안 되나?"**

답은 페르소나 요구에 있습니다. P1·P2의 진짜 요구를 분해해 컴포넌트로 매핑하면:

| **P1 캠페인 매니저** 요구 | 필요한 능력 | 컴포넌트 |
|---|---|---|
| 700+ 캠페인 ROI/어트리뷰션을 SQL로 ad-hoc 조회 | 정제 표를 SQL 엔진이 직접 조회 | **Athena + Iceberg** |
| 며칠 뒤 도착한 전환으로 어제 표 자동 갱신 | **MERGE 멱등성** | **Iceberg(오픈테이블포맷)** |
| 직관적 시각 대시보드 | BI 도구 | **QuickSight** |

| **P2 파이프라인 운영자** 요구 | 필요한 능력 | 컴포넌트 |
|---|---|---|
| 광고 이벤트 stream 즉시 적재 | 메시지 큐 + 스트리밍 처리 | **Kafka + Spark Structured Streaming** |
| 90일 raw 보존 + 비용 효율 | 저비용 객체 스토리지 | **S3 (Bronze)** |
| 백필·시간여행 안전 | ACID + snapshot | **Iceberg** |
| 5분 안에 헬스체크 가능 | 표 메타 + 자동화 | **Iceberg 메타 테이블 + Airflow** |

→ 이 요구를 **한 번에** 만족시키는 패턴이 **레이크하우스**. raw부터 분석까지 한 곳에, ACID·시간여행·MERGE 멱등성이 보장되는 표 형식 위에서.

#### 다른 패턴은 왜 안 되나

| 대안 | 부족한 점 |
|---|---|
| **Glue ETL만** (batch 변환 → RDS) | 스트리밍 X, 시간여행 X, MERGE 멱등성 X, 메타 기반 운영 가시성 X — **처리 도구지 데이터 플랫폼이 아님**. P2 요구의 절반도 못 만족. |
| **Data Warehouse 단독** (Redshift/Snowflake) | 90일 raw 보존 비용 폭증, 스트리밍 통합 약함. 광고 도메인의 raw 90일 + 5분 streaming 요구와 안 맞음. |
| **Data Lake 단독** (S3 + Glue Catalog만, Iceberg X) | ACID/MERGE 멱등성 X — 늦게 도착하는 전환을 안전하게 갱신 불가. P1 요구의 핵심을 못 풀음. |

→ **레이크하우스가 두 페르소나 요구를 동시에 만족시키는 가장 단순한 답**. 컴포넌트 조합이 자연스럽게 결정됨.

### 메달리온 3계층 결정 — 레이크하우스 안에서 어떻게 쪼갰나

레이크하우스 안에서도 **3계층 분리**가 핵심입니다. 한 가지 고민에서 세 가지 결정이 나왔고, 각 결정이 계층 선택의 근거:

| 결정 | 근거 |
|---|---|
| **Bronze는 plain Parquet, Iceberg 아님** | raw는 백필 안전선이라 절대 손상되면 안 됨. **append-only면 충분** — MERGE/스냅샷 가치 없음. Iceberg 관리 부담만 늘 뿐. |
| **Silver는 Iceberg(오픈테이블포맷)** | 며칠 뒤 도착하는 전환을 어제 표에 안전하게 갱신하려면 **MERGE 멱등성이 필수**. plain Parquet으로는 불가능 — Iceberg 채택의 가장 큰 근거. |
| **Gold는 두 종류로 분리** | **운영자 헬스 메트릭**과 **매니저 비즈니스 KPI**는 갱신 주기·집계 단위·소비자 페르소나가 모두 다름. 한 테이블에 섞으면 어느 쪽도 만족 못함. |

### 흐름

![architecture](../docs/img/architecture.png)

```
[Criteo TSV] → [kafka_producer.py] → [Kafka 단일 broker]
                                            │
                                            ▼ Spark Structured Streaming (5분 micro-batch)
                                   ┌─────────────────────────┐
                                   │ Bronze (S3 parquet)      │  ← raw 보존, append-only, 90일
                                   │   raw/ad-events/         │
                                   └────────────┬────────────┘
                                                │ Spark Batch MERGE (15분)
                                                ▼
                                   ┌─────────────────────────┐
                                   │ Silver (Iceberg + Glue)  │  ← processed_events,
                                   │   processed_events       │     event_id MERGE 키
                                   └────────────┬────────────┘
                                                │ Spark Batch (1h, 7d rebuild)
                                                ▼
                                   ┌─────────────────────────┐
                                   │ Gold (Iceberg + Glue)    │  ← campaign_summary_daily,
                                   │   campaign_summary_daily │     attribution 2종
                                   └────────────┬────────────┘
                                                │ Athena
                                                ▼
                                       QuickSight (비즈니스/운영 탭)

[병렬] Airflow on Docker
   ├─ ads_silver_merge_15min       (15분 주기)
   ├─ ads_gold_summary_hourly      (1시간 주기)
   ├─ ads_iceberg_compaction_daily (03:00 KST)
   ├─ ads_iceberg_expire_daily     (04:00 KST, 30일 초과 만료)
   └─ ads_iceberg_orphan_weekly    (일 05:00 KST)
```

**환경 분리**: 처리(Kafka·Spark·Airflow)는 **로컬 도커**, 저장(Bronze parquet·Silver/Gold Iceberg·Glue 카탈로그)과 분석(Athena·QuickSight)은 **AWS S3**. 비용 절충은 [ADR-008](../docs/adr/0008-bronze-on-s3.md)에 박제.

**카탈로그 결정**: AWS Glue Data Catalog 채택 — Athena 네이티브, 매니지드, 비용 거의 0(월 100만 객체 무료). REST 카탈로그는 100x 시점에 검토.

**100x 시 AWS 매핑**: Local Spark → EMR/EKS, 단일 Glue → REST 또는 도메인별 분리, 단일 Kafka broker → MSK 다중 broker.

빠른 시작은 [docs/SETUP.md](../docs/SETUP.md). 12개 표준 섹션 상세는 [docs/ARCHITECTURE.md](../docs/ARCHITECTURE.md).

---

## 3. 메달리온 3계층 의사결정

### 3-1. Bronze (raw)
- **위치**: `s3://<bucket>/lakehouse/ads/raw/ad-events/` (Parquet, **Iceberg 아님** — [ADR-008](../docs/adr/0008-bronze-on-s3.md))
- **컬럼**: `kafka_offset`, `kafka_partition`, `kafka_timestamp`, `payload`(JSON 원본), `raw_date`(파티션), `raw_hour`(파티션)
- **파티션**: `raw_date=YYYY-MM-DD/raw_hour=HH/` — Kafka 도착 시각 기준. 시간대별 백필·디버깅 빠름.
- **변환 규칙**: 없음. Kafka payload 그대로 보존.
- **이유**: raw는 보존·재처리·감사 용도. append-only면 충분 → MERGE/스냅샷 가치 없음. Iceberg 관리 부담만 늘어 비효율.
- **보존**: 90일 (백필 안전 윈도우, 90일 후 Glacier transition 검토).
- **카탈로그 등록**: 미등록 (선택적). Spark에서 경로로 직접 read.

### 3-2. Silver (processed)
- **테이블**: `iceberg.ads.processed_events`
- **위치**: `s3://<bucket>/lakehouse/ads/processed_events/` (Glue 카탈로그)
- **입력 흐름**: `kafka_producer.py --realistic`이 1 Criteo row → 3 토픽(`ad-impressions` / `ad-clicks` / `ad-conversions`)으로 시간차 분리 발행. Bronze는 3 토픽을 통합 적재. Silver는 `event_id`로 다시 결합 (자세한 흐름은 [SETUP.md](../docs/SETUP.md#데이터-본질구조-criteo-attribution-dataset))
- **변환 규칙**:
  - 3 토픽 JSON payload → 컬럼 분해, `event_type` ∈ {`impression`, `click`, `conversion`} 보존
  - timestamp UTC 표준화 (ms 단위)
  - **late-arriving conversion 결합** — 며칠 뒤 도착한 conversion이 기존 impression의 `event_id`에 MERGE되며 attribution 갱신
  - **두 어트리뷰션 결과 동시 계산**: `attribution_lastclick`(Boolean), `attribution_evencredit`(Double, 0~1)
  - 이상치 마킹: `is_valid` 플래그 (event_time 미래·conversion 30일 초과 등)
- **파티션**: `event_date` (Iceberg **hidden partitioning**, 일 단위) — 사용자가 `WHERE event_time BETWEEN ...`만 적어도 자동으로 파티션 잘림. 분석가 실수 방지.
- **MERGE 키**: `event_id` (UUID)
- **갱신 가드**: `WHEN MATCHED AND s.ingested_at > t.ingested_at THEN UPDATE` — 옛 데이터로 새 데이터를 덮어쓰지 못하게.
- **재현성**: 같은 (입력 + base_datetime + seed) → 동일 Silver. 어트리뷰션 두 결과는 deterministic 룰 기반 → 동일 입력 → 동일 결과.

### 3-3. Gold (summary)

| 테이블 | 내용 | 파티션 | MERGE 키 |
|---|---|---|---|
| `iceberg.ads.campaign_summary_daily` | 캠페인×일자 KPI (impressions, clicks, conversions×2, CTR, CVR, ROI×2, 평균 전환 지연) | `summary_date` (월 단위) | (`summary_date`, `campaign_id`) |
| `iceberg.ads.attribution_diff_daily` | (선택, Phase 2) Last-click vs Even-credit 결과 차이가 큰 캠페인 일배치 | `summary_date` | (`summary_date`, `campaign_id`) |

- **갱신**: 1시간 주기, **최근 7일 rebuild** — late-arriving conversion이 7일 안에 들어와 어제·그제 표가 자동 갱신됨.
- **파티션 결정 근거**: `summary_date`를 **월 단위**로 둔 이유 — 캠페인 700개 × 일 30일 = 21K rows / 월. 일 단위 파티션은 폭증, 월 단위가 효율.
- **활용**: QuickSight 직접 쿼리, time-travel로 어제 시점 결과 재현 가능.

상세 컬럼: [docs/DATA_MODEL.md](../docs/DATA_MODEL.md)

---

## 4. 이 도메인에서 Iceberg가 가장 가치 있는 지점

광고 도메인은 데이터 엔지니어링 관점에서 **5가지 본질적 어려움**이 있고, Iceberg가 그 중 다섯을 동시에 해결.

1. **Late-arriving conversion MERGE 멱등성** — 클릭은 오늘, 전환은 5일 뒤. 어제 만든 표의 attribution 컬럼이 오늘 갱신돼야 하고, 같은 윈도우 두 번 돌려도 결과가 같아야 함. Iceberg `MERGE INTO` + `ingested_at` 가드 한 번에.

2. **3개월 백필 시 어제 결과 보존** — 정제 로직 버그 발견 → 3개월 데이터 재처리. 백필 직전 `ALTER TABLE ... CREATE TAG snap_before_backfill`로 박제, 실패 시 `rollback_to_snapshot`. **time-travel이 없으면 사실상 불가능**.

3. **컴팩션 vs streaming MERGE 동시성 안전 (Snapshot Isolation)** — 작은 파일 청소 잡과 15분 MERGE가 같은 파티션을 동시에 건드려도 OCC가 충돌 감지·재시도. Plain Parquet+Hive는 락 직접 관리.

4. **고속 micro-batch의 작은 파일 폭증 자동화** — 15분 주기 배치가 만드는 작은 parquet들을 `rewrite_data_files` procedure로 컴팩션. **운영자 개입 없이** 평균 파일 크기 유지.

5. **어트리뷰션 모델 추가 시 스키마 진화** — `attribution_evencredit` 같은 신규 컬럼을 `ALTER TABLE ADD COLUMN` 한 줄로 추가. 옛 데이터는 NULL로 두고 백필. **비-Iceberg는 호환성 수동 관리**.

→ Plain Parquet + Glue로는 (1)(2)(3)(5)가 사실상 불가능. **광고 도메인의 운영 본질이 Iceberg에 정확히 매핑**.

---

## 5. 운영 헬스 체크 쿼리 모음

운영 가시성의 목표: **운영자가 매일 5분 안에 전체 시스템을 한눈에 진단할 수 있는가**. 단순히 쿼리 8개를 던지는 게 아니라, **3계층으로 분리**해서 단단하게:

1. **한눈 진단 5개** — 매일 5분 헬스체크 시 보는 메인 위젯 (사람 1회)
2. **헬스 쿼리 8개** — 진단 5개의 데이터 소스 (Iceberg 메타 활용)
3. **알람 임계** — 24/7 자동 감시 (위반 시 Slack/이메일)

### ① 한눈 진단 5개 (대시보드 운영 탭 메인)

운영자가 출근해 대시보드 한 번 열었을 때 이 5개 위젯으로 정상/이상 판단:

| 진단 | 메트릭 | 목표 |
|---|---|---|
| **1. 신선도** (시스템이 살아있나) | Gold까지 반영 지연 | P95 1시간 이내 |
| **2. 처리량 흐름** (큐 적체 없나) | Bronze 적재 속도 vs Kafka 도착 속도 | 적체 0 |
| **3. 데이터 무결성** (깨졌나) | `event_id` 중복 행 수 | **0건** |
| **4. 운영 자동화 상태** (매니지먼트 정상?) | 어제 컴팩션·Expire·Orphan DAG 성공 여부 | 3/3 success |
| **5. 저장 효율** (작은 파일 누적?) | Silver/Gold 파티션당 평균 파일 크기 | **≥ 128MB** |

→ 이 5개가 한 화면에 있으면 운영자가 5분 안에 "정상" 또는 "어디 들여다볼지"를 판단.

### ② 헬스 쿼리 8개 (진단의 데이터 소스)

(TBD — 실제 SQL은 Epic 6에서 [final/code/health-queries/](code/health-queries/)로 작성)

| # | 쿼리 | 점검 항목 | Iceberg 메타 | 매핑 진단 |
|---|---|---|---|---|
| 1 | `freshness.sql` | Bronze/Silver/Gold 최신 `event_time` vs 현재 (반영 지연) | `MAX(event_time)` | 1·2 |
| 2 | `row_count_consistency.sql` | 일자별 행 수 Bronze=Silver=Gold ±0.1% 일치 | 각 단계 COUNT | 2 |
| 3 | `iceberg_files.sql` | 파티션당 파일 수 + 평균 크기 | `<table>.files` | 5 |
| 4 | `iceberg_snapshots.sql` | 누적 스냅샷 + 30일 초과 자동 만료 작동 | `<table>.snapshots` | 4 |
| 5 | `iceberg_history.sql` | 최근 snapshot 작업 이력 (어느 잡이 무엇을 변경) | `<table>.history` | 4 |
| 6 | `merge_idempotency_check.sql` | 같은 `event_id` 중복 행 검출 | `COUNT(*) vs COUNT(DISTINCT event_id)` | 3 |
| 7 | `merge_lag.sql` | Bronze→Silver 반영 지연 P50/P95/P99 | `ingested_at - kafka_timestamp` | 1 |
| 8 | `attribution_diff.sql` | Last-click vs Even-credit 차이 (분석가 영역, Phase 2) | Gold 두 컬럼 비교 | — |

### ③ 알람 임계 (24/7 자동 감시)

진단 5개의 KPI 위반 위험을 미리 감지하기 위한 트리거. 위반 시 Slack/이메일 자동 알람:

| 메트릭 | 알람 임계 | 위반이 의미 | 매핑 진단 | 대응 쿼리 |
|---|---|---|---|---|
| Bronze 신선도 | `now() - 10분` 초과 | streaming 2 배치 지연 | 1 | #1 |
| Silver MERGE lag P95 | 15분 초과 | 배치 잡 정체 | 1·2 | #7 |
| 멱등성 위반 | `event_id` 중복 1건 이상 | 데이터 깨짐 | 3 | #6 |
| 스냅샷 누적 | 1,000개 초과 | Expire 작동 안 함 | 4 | #4 |
| 작은 파일 비율 | 파티션당 평균 < 128MB | 컴팩션 필요 | 5 | #3 |

→ **알람 = 24/7 자동, 매일 5분 헬스체크 = 사람 1회**. 둘이 다른 도구 — 알람은 사고 방지 트리거, 헬스체크는 사람의 정기 점검.

---

## 6. 대시보드 (스크린샷 + 운영 메트릭)

(TBD — Epic 6에서 QuickSight 구축 후 [final/dashboard/](dashboard/)에 스크린샷·데이터셋 export 첨부. 아래는 위젯 명세)

### 비즈니스 탭 (P1 민혁)
| 위젯 | 시각화 | 데이터 소스 | 페르소나 페인 매핑 |
|---|---|---|---|
| 캠페인 ROI 표 | 정렬 가능한 표, 음수는 빨강 | `campaign_summary_daily` 최근 7일 | "어제 광고비 손해 본 캠페인 어디?" |
| Last-click vs Even-credit | 산점도(x=last, y=even) | Gold 두 컬럼 | "어트리뷰션 모델에 따라 결과가 얼마나 갈리나?" |
| 일자별 광고비 vs 매출 | 라인 차트 (이중 Y축) | Gold | "이번 주 추세는?" |
| 평균 전환 지연 분포 | 히스토그램 (시간 단위) | Silver `conv_delay_seconds` | "내 캠페인의 전환은 즉시인가 며칠 후인가?" |

### 운영 탭 (P2 지수)
| 위젯 | 시각화 | 데이터 소스 | 페르소나 페인 매핑 |
|---|---|---|---|
| 신선도 게이지 | 게이지 (목표 P95 1시간) | 헬스 쿼리 #1 | "지금 데이터 늦었나?" |
| Bronze/Silver/Gold 행 수 | 라인 차트 (3개 시리즈) | 헬스 쿼리 #2 | "단계별 누락 없이 흐르고 있나?" |
| Iceberg 파일 수 추이 | 라인 차트 (컴팩션 직전·직후 마커) | 헬스 쿼리 #3 | "컴팩션 효과 시각화" |
| 매니지먼트 DAG 실행 이력 | Airflow 캘린더 뷰 | Airflow API | "어제 새벽 잡이 죽었나?" |
| MERGE 멱등성 검증 | 단일 숫자(중복 행 수, 0 목표) | 헬스 쿼리 #6 | "데이터 깨졌나?" |

→ 두 탭 모두 페르소나의 **첫 30초 시선**을 KPI에 박는 구성.

---

## 7. 100x 스케일 아웃 시나리오 (설계만, 구현 X)

현재 일 100만 → 단기 10x(일 1,000만) → 장기 100x(일 1억). [ARCHITECTURE.md §12](../docs/ARCHITECTURE.md#12-알려진-제약--리스크--향후-계획) 참고.

| 깨지는 지점 | 100x 대응 | 비용/리스크 추정 |
|---|---|---|
| Kafka single broker (현재) | MSK 다중 broker + 파티션 N배 (3 → 30) | MSK m5.large × 3 ≈ $300/월 |
| Spark Local Mode | EMR Cluster Mode (코드 그대로) | EMR m5.xlarge × 4 (15분만 spot) ≈ $200/월 |
| Bronze S3 단일 prefix throttling | 도메인별 prefix 분리 (`raw/<advertiser>/...`) | 동일 비용, 무료 |
| Iceberg 컴팩션 단일 잡 시간 폭증 | 분산 매니지먼트 + `partial-progress.enabled=true` | 동일 비용 |
| Glue Catalog 단일 부하 | REST Catalog(Tabular) 또는 도메인별 Glue 분리 | REST 컨테이너 ≈ $50/월 (셀프호스트) |
| Bronze S3 비용 | 9TB × $0.023 = $200/월 → **90일 후 Glacier transition** | Glacier 1/4 비용 → $50/월 절감 |

**광고 도메인 특유의 트래픽 스파이크**:
- 블랙프라이데이/광군제(11월) — 평소 대비 **3~5배 트래픽**
- 크리스마스 직전(12월 둘째 주) — **2~3배**
- 한국 명절(설/추석) — 광고주에 따라 **0.5~2배**

→ Auto-scaling 정책은 **요일 + 월 + 광고주 카테고리** 기반으로 구성 필요. (TBD — ADR-007 별도 작성)

---

## 8. 장애·운영 시나리오

(TBD — 정식 RUNBOOK은 Epic 7에서. 아래는 발표용 골격, 증상→진단→조치→검증 형식)

### 시나리오 1: Spark Streaming Job이 새벽 OOM으로 다운

- **증상**: 알람 발생 (Kafka consumer lag 5분 초과). 운영자 핸드폰 울림.
- **진단**: `docker compose logs spark-ads | grep OOM`. 메모리 사용량 그래프(헬스 쿼리 #1로 신선도 갭 확인).
- **조치**:
  1. 컨테이너 재기동 — `docker compose restart spark-ads`
  2. Kafka consumer가 **체크포인트**(`s3://.../checkpoints/raw-ad-events/`)에서 마지막 offset 자동 복구
  3. 재처리되는 마이크로배치는 Bronze에 **append**되며, Silver MERGE는 `event_id` 키 + `ingested_at` 가드로 **중복 없이 갱신**
- **검증**: 헬스 쿼리 #1(신선도 정상화 확인) + #6(멱등성 — 중복 행 0)

→ **Iceberg가 어떻게 도와주나**: Silver `processed_events`의 MERGE 멱등성 가드 덕분에 사람 개입 없이 자동 복구. 새벽 호출 → 출근해서 로그만 확인.

### 시나리오 2: 3개월 백필 (정제 로직 버그 발견)

- **증상**: 어제 분석가가 "최근 데이터의 캠페인 KPI 패턴이 3개월 전과 정합성 안 맞는다" 보고.
- **진단**: 변경 이력(`<table>.history`)에서 정제 로직 버그 PR 시점 확인. 영향 범위 = 3개월 × 700 캠페인.
- **조치**:
  1. **백필 직전 스냅샷 박제**:
     ```sql
     ALTER TABLE iceberg.ads.processed_events CREATE TAG snap_before_backfill;
     ALTER TABLE iceberg.ads.campaign_summary_daily CREATE TAG snap_before_backfill;
     ```
  2. **Expire/Orphan DAG 일시 정지** — `airflow dags pause ads_iceberg_expire_daily` (백필 윈도우 보호, Bronze 90일 보존이 받쳐줌)
  3. Silver→Gold 백필 실행 (`bronze_to_silver.py --from-date 2026-02-01 --to-date 2026-04-30`)
  4. 백필 완료 후 DAG 재가동
- **검증**: 일자별 행 수 일치(쿼리 #2), Gold 합계 ±0.1% 일치, 어트리뷰션 결과 안정성 확인(쿼리 #8)
- **실패 시 롤백**:
  ```sql
  CALL iceberg.system.rollback_to_snapshot('ads.processed_events', <snap_before_backfill의 snapshot_id>);
  ```

→ **Iceberg가 어떻게 도와주나**: TAG + rollback으로 안전망. **time-travel 없는 환경이라면 백필 시도 자체가 도박**.

### 시나리오 3: 컴팩션과 streaming MERGE가 같은 파티션 동시 변경

- **증상**: 컴팩션 잡 로그에 OCC 충돌 경고. MERGE 잡 일시 실패율 증가.
- **진단**: `<table>.history`에서 충돌 시점 작업 ID 확인.
- **조치**:
  1. **시간대 분리** — 매니지먼트 03:00~05:00 KST 사이에만 (`ads_silver_merge_15min`은 그 시간 자동 일시 정지하도록 Airflow `schedule_interval` 조건 추가)
  2. `partial-progress.enabled=true` — 컴팩션 일부 성공도 commit, 충돌 부분만 재시도
- **검증**: 컴팩션 후 평균 파일 크기 회복(쿼리 #3), MERGE 잡 실패율 0% 회복

→ **Iceberg가 어떻게 도와주나**: OCC가 자동 감지·재시도. 사람 개입 없이 안전.

---

## 9. 멱등성 / 재처리 가능성 설계

### 멱등성 4가지 레이어

| 레이어 | 멱등성 키 / 가드 | 보장 방식 |
|---|---|---|
| Kafka → Bronze | (`kafka_offset`, `kafka_partition`) 자연키 | Spark Streaming 체크포인트 + append-only |
| Bronze → Silver | `event_id` (MERGE 키) + `ingested_at`(갱신 가드) | `WHEN MATCHED AND s.ingested_at > t.ingested_at` |
| Silver → Gold | (`summary_date`, `campaign_id`) | 7일 윈도우 rebuild (재계산 안전) |
| 백필 운영 | TAG 박제 + rollback 절차 | `ALTER TABLE ... CREATE TAG` + `rollback_to_snapshot` |

### 재처리 가능성

- **Bronze 90일 보존** = 백필 안전 윈도우. 모든 재처리의 베이스라인.
- **Silver MERGE는 멱등** = 같은 윈도우 N번 실행 → bit-identical.
- **Gold 7일 윈도우 rebuild** = late-arriving conversion이 자연스럽게 어제·그제 표 갱신.
- **백필 인자**: `bronze_to_silver.py --from-date YYYY-MM-DD --to-date YYYY-MM-DD`로 임의 윈도우 재처리.

### 광고 도메인 특유의 함정

- **재시뮬 시 자연키 충돌**: 같은 Criteo 입력에 다른 `--base-datetime`으로 재시뮬하면 같은 `event_id` 생성됨 → MERGE가 UPDATE로 `event_time`만 덮어쓰는 현상. **시계열 보존하려면 시뮬 전 TAG 박제 또는 prefix 분리**.
- **어트리뷰션 모델 변경**: `attribution_evencredit` 로직 변경 시 옛 결과 재현 위해 `silver_snapshot_id`를 Gold에 함께 기록. time-travel로 옛 모델 결과 복원 가능.

상세 SQL 패턴: [docs/DATA_MODEL.md §5](../docs/DATA_MODEL.md#5-merge-멱등성-보장-핵심-설계)

---

## 디렉토리 구조

```
final/
├── README.md           ← 본 문서 (평가 답변 9개 섹션)
├── infra/              ✅ Spark + Kafka + Iceberg JAR + .env.example
├── code/
│   ├── ddl/            (TBD - Epic 3, 4) silver.sql, gold.sql
│   ├── pipelines/      ✅ prepare_criteo_data.py / (TBD) kafka_to_bronze, bronze_to_silver, silver_to_gold
│   └── health-queries/ (TBD - Epic 6) 8개 SQL
├── orchestration/      (TBD - Epic 5) Airflow DAG 5개
└── dashboard/          (TBD - Epic 6) QuickSight 스크린샷 + 데이터셋 export
```

빈 폴더는 `.gitkeep`. 셋업·실행은 [docs/SETUP.md](../docs/SETUP.md). 워크플로우 진행은 [docs/BREAKDOWN.md](../docs/BREAKDOWN.md).

---

## 갱신 이력

- 2026-05-09 v1: 9개 섹션 골격. §1~§4, §7, §9는 PRD/ARCHITECTURE/DATA_MODEL에서 가져와 채움.
- 2026-05-10 v2: 운영 절차 4개 섹션을 [docs/SETUP.md](../docs/SETUP.md)로 분리.
- 2026-05-10 v3: 도메인 컨셉 재정렬(어트리뷰션·late-arrival 강조), §0 데이터 섹션 신설, §1 KPI 광고 도메인 특화(어트리뷰션 일관성/음의 ROI 조기 경보/신선도+멱등성), §3 테이블·컬럼·MERGE 키 구체 명시, §4 Iceberg 가치 5개로 확장, §5 헬스 쿼리 8개 항목 명세, §6 위젯 명세표, §7 비용 추정 칸 + 광고 트래픽 스파이크, §8 RUNBOOK 3건 증상→진단→조치→검증 형식, §9 멱등성 4레이어 + 광고 도메인 함정.
- 2026-05-10 v4: §0 데이터 섹션을 [docs/SETUP.md](../docs/SETUP.md)로 이전(데이터 본질·22컬럼·realistic 모드 3토픽 분리 흐름). README §1 위에 한 줄 링크. §3 Silver 변환 규칙에 "1 Criteo row → 3 토픽 → Bronze 통합 → Silver event_id 결합" 흐름 명시.
- 2026-05-10 v5: §1 도메인 정의를 가이드 형태대로 재구성 — "도메인 분야(광고) + 데이터 규모/패턴(일 100만 이벤트, 광고 시즌 스파이크) + 성장 가정(10x/100x)" 중심. 페르소나는 [docs/PRD.md](../docs/PRD.md) 한 줄 링크로 분리.
