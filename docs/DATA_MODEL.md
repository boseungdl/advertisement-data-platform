# DATA MODEL — 광고 캠페인 분석 플랫폼

> 살아있는 문서 — 1차 뼈대 / 작성일: 2026-05-09 (~65% 채움)

## 1. 스테이지 구조

```
Criteo CSV → Kafka → Bronze → Silver → Gold
                     (raw)    (정제)   (집계)
                     append   MERGE    rebuild
```

**왜 3단계로 나누나**
- **Bronze**: 원본 보존 — 스키마 바뀌어도 다시 처리 가능. append-only.
- **Silver**: 정제 표준 — 비즈니스 로직 변경 시 Gold만 다시 만들면 됨. MERGE.
- **Gold**: 분석/서빙 전용 — rebuild 자유로움.

이 분리가 운영 시 재처리 시간을 1/10로 줄여줍니다.

---

## 2. 테이블 명세

### 🟫 Bronze — `raw_files` (Parquet, Iceberg 아님) — [ADR-008](adr/0008-bronze-on-s3.md)

- **위치**: AWS S3 `s3://<bucket>/lakehouse/ads/raw/ad-events/`
- **파티션**: `raw_date=YYYY-MM-DD/raw_hour=HH/`
- **보존**: 90일 (백필 안전 윈도우, 90일 후 Glacier transition 검토)
- **변환**: 없음. Kafka payload 그대로 보존.
- **카탈로그**: 미등록(선택적). Spark에서 경로로 직접 read.

| 컬럼 | 타입 | 설명 |
|---|---|---|
| kafka_offset | Long | Kafka 오프셋 |
| kafka_partition | Int | Kafka 파티션 |
| kafka_timestamp | Long | Kafka 도착 시각 (ms) |
| payload | String | JSON 원본 |
| raw_date | Date | 파티션 (Kafka 도착일) |
| raw_hour | Int | 파티션 (Kafka 도착 시) |

### 🥈 Silver — `iceberg.ads.processed_events` (Iceberg)

- **카탈로그**: AWS Glue, 위치: `s3://<bucket>/lakehouse/ads/processed_events/`
- **파티션**: `event_date` (Iceberg hidden partitioning, 일 단위)
- **MERGE 키**: `event_id`
- **변환 규칙**:
  - JSON payload → 컬럼 분해
  - timestamp 표준화 (UTC, ms)
  - 이상치 마킹 (`is_valid` 플래그)
  - **late-arriving conversion** → 같은 `event_id`에 conversion 정보 갱신

| 컬럼 | 타입 | 설명 |
|---|---|---|
| event_id | String | UUID, MERGE 키 |
| event_time | Timestamp | 이벤트 발생 시각 (UTC) |
| event_date | Date | 파티션 (event_time에서 파생) |
| campaign_id | Int | Criteo 캠페인 ID |
| user_id | String | 익명 사용자 해시 |
| event_type | String | click / impression / conversion |
| click_id | String | 연관 클릭 ID (conversion일 때) |
| conversion_value | Double | 전환 금액 |
| conv_delay_seconds | Long | 클릭→전환 지연 |
| cost | Double | 노출/클릭 비용 |
| cat1..cat9 | Int | Criteo 익명 카테고리 |
| attribution_lastclick | Boolean | Last-click 모델 결과 |
| attribution_evencredit | Double | Even-credit 모델 결과 (0~1) |
| ingested_at | Timestamp | 적재 시각 (멱등성 추적용) |
| is_valid | Boolean | 데이터 품질 검증 통과 |

### 🥇 Gold — `iceberg.ads.campaign_summary_daily` (Iceberg)

- **카탈로그**: AWS Glue, 위치: `s3://<bucket>/lakehouse/ads/campaign_summary_daily/`
- **파티션**: `summary_date` (월 단위 — 일 단위는 행 수 적어 과함)
- **갱신**: 1시간 주기, 최근 7일 윈도우 rebuild
- **PK**: (`summary_date`, `campaign_id`)

| 컬럼 | 타입 | 설명 |
|---|---|---|
| summary_date | Date | 일 단위 |
| campaign_id | Int | 캠페인 |
| impressions | Long | 노출 수 |
| clicks | Long | 클릭 수 |
| conversions_lastclick | Long | Last-click 기준 전환 |
| conversions_evencredit | Double | Even-credit 기준 전환 (소수) |
| ctr | Double | clicks/impressions |
| cvr_lastclick | Double | conversions_lastclick/clicks |
| total_cost | Double | 광고비 합 |
| total_revenue_lastclick | Double | Last-click 기준 매출 합 |
| roi_lastclick | Double | (revenue - cost) / cost |
| avg_conv_delay_hours | Double | 평균 전환 지연 |
| computed_at | Timestamp | 집계 시각 |

---

## 3. 파티셔닝 전략

| 계층 | 파티션 키 | 단위 | 이유 |
|---|---|---|---|
| Bronze | `raw_date` + `raw_hour` | 시간 | 시간대별 백필 빠름 |
| Silver | `event_date` (hidden) | 일 | 사용자가 `event_time` 기준 쿼리하면 자동 적용 |
| Gold | `summary_date` | 월 | 일별 행 수 적음 → 월 단위가 효율 |

> **Iceberg hidden partitioning**: 일반 Hive 파티션은 사용자가 `WHERE event_date = ...`라고 적어야 잘리는데, Iceberg는 `WHERE event_time BETWEEN ...`만 적어도 알아서 파티션 자름. 쿼리 짤 때 실수 방지.

---

## 4. 데이터 계약 (Data Contract)

### Producer 보장 (Kafka → Bronze 입력)
- `event_id`, `event_time`, `campaign_id`는 NULL 불가
- `event_time`은 최근 30일 이내
- `event_type` ∈ {`click`, `impression`, `conversion`}

### Consumer 보장 (Gold 출력)
- `summary_date`별로 활성 캠페인 누락 없음
- 일별 `clicks` 합계가 Silver의 `clicks` 합계와 ±0.1% 일치
- 컬럼 추가는 가능, **삭제·타입 변경은 ADR 필수**

> **데이터 계약**(Data Contract) = 데이터 생산자와 소비자가 약속하는 입출력 보장. 이게 깨지면 다운스트림 다 깨짐.

---

## 5. MERGE 멱등성 보장 (핵심 설계)

```sql
MERGE INTO iceberg.ads.processed_events t
USING (
  SELECT * FROM new_batch_window
) s
ON t.event_id = s.event_id
WHEN MATCHED AND s.ingested_at > t.ingested_at
  THEN UPDATE SET *
WHEN NOT MATCHED
  THEN INSERT *;
```

**무엇을 보장하나**
- 같은 시간 윈도우를 두 번 돌려도 결과 동일 (`ingested_at` 비교로 옛 갱신은 무시)
- 늦게 도착한 conversion → 기존 click 이벤트의 attribution 컬럼만 갱신
- 새벽에 잡 죽었다 재시작해도 안전

이게 발표에서 "왜 Iceberg인가"의 단단한 답이 됩니다.

---

## 6. 갱신 이력

- 2026-05-09: 1차 뼈대 작성
- 🔄 구현 중 컬럼 누락 발견 시 추가 (예: device_type, geo)
- 🔄 인덱스 결정 (Iceberg z-ordering) — 5단계 구현 시
- 🔄 백필 전용 컬럼(`backfill_run_id`) 검토
