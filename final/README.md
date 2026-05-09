# 최종 프로젝트 — 광고 캠페인 분석 + 운영 모니터링 플랫폼

> 메타코드 Lakehouse 강의 8회차 최종 프로젝트 산출물.
> 작성일: 2026-05-09 / 진행 단계: 4단계 Pre-flight (구현 전 1차 뼈대)

> 이 README는 **살아있는 문서**. Epic 진행하면서 🔄 TBD 표시된 섹션이 채워집니다.

---

## 1. 도메인 정의 + 핵심 KPI 3개

### 도메인
**광고 캠페인 매니저(P1 민혁)** — 700+ 캠페인의 성과를 매일 추적해 광고비 배분 결정.
**데이터 파이프라인 운영자(P2 지수)** — 매일 5분 안에 데이터 신선도와 헬스 점검.
두 사용자가 같은 대시보드의 **비즈니스 탭 / 운영 탭**으로 분리되어 본다.

상세: [docs/PRD.md](../docs/PRD.md)

### 핵심 KPI 3개
| KPI | 목표 | 의미 |
|---|---|---|
| 처리량 | 일 100만 이벤트 (10x 성장 시 1,000만) | 광고 도메인 표준 규모 |
| 신선도 | P95 1시간 안에 Gold 반영 | 늦게 도착한 전환이 자동으로 어제·그제 표에 반영 |
| 멱등성 | 같은 윈도우 두 번 처리 → 결과 동일 | 백필 안전, 새벽 호출 시 재실행 안전 |

---

## 2. 전체 아키텍처

![architecture](../docs/img/architecture.png)

광고 데이터 흐름: **Criteo CSV → Kafka → Bronze raw → Silver Iceberg(MERGE) → Gold Iceberg(집계) → Athena → QuickSight**.
처리(Kafka/Spark)는 **로컬 도커**, 저장(Iceberg+Glue)과 분석(Athena+QuickSight)은 **AWS**.

상세 12개 섹션: [docs/ARCHITECTURE.md](../docs/ARCHITECTURE.md)

---

## 3. 메달리온 3계층 의사결정

### 3-1. Bronze (raw)
- **저장 형식**: Parquet append 파일 (Iceberg 아님)
- **이유**: raw는 보존·재처리·디버깅 용도. append-only면 충분. MERGE/스냅샷 가치 적음.

### 3-2. Silver (processed)
- **저장 형식**: Iceberg
- **이유**: 늦게 도착하는 전환을 MERGE로 안전하게 처리. **이게 광고 도메인의 본질**.

### 3-3. Gold (summary)
- **저장 형식**: Iceberg
- **이유**: 7일 윈도우 rebuild 빈번 → snapshot 격리 유리. 분석/서빙 전용.

상세 스키마·컬럼: [docs/DATA_MODEL.md](../docs/DATA_MODEL.md)

---

## 4. 이 도메인에서 Iceberg가 가장 가치 있는 지점

**핵심**: 광고 데이터의 본질은 **클릭은 오늘, 전환은 5일 뒤** — 늦게 도착함.
어제 만든 캠페인 리포트의 숫자가 오늘 자동으로 갱신돼야 하고, 새벽 잡이 죽어 재실행해도 데이터가 망가지면 안 된다.

**Iceberg가 답하는 3가지**
- **MERGE 멱등성** — 같은 시간 윈도우를 두 번 돌려도 결과 동일 (`ingested_at` 비교로 옛 갱신 무시)
- **Snapshot Isolation** — 컴팩션 도중에도 streaming MERGE가 같은 파티션을 안전하게 갱신
- **Hidden Partitioning** — 사용자가 파티션 컬럼 신경 안 써도 자동 적용 → 분석가 실수 방지

→ Plain Parquet + Glue로는 이 3가지를 **모두** 만족 못 함.

---

## 5. 운영 헬스 체크 쿼리 모음

🔄 **TBD** — Epic 6 진행 시 [code/health-queries/](code/health-queries/)에 5~10개 쿼리 추가.

예정 항목:
- 데이터 신선도 (최근 이벤트 시각 vs 현재 시각)
- 일자별 행 수 (Bronze vs Silver vs Gold 일치)
- Iceberg 파일 수 + 평균 크기 (컴팩션 효과)
- 스냅샷 누적 수 (Expire 작동 확인)
- MERGE 멱등성 검증 (중복 행 검출)

---

## 6. 대시보드 (스크린샷 + 운영 메트릭)

🔄 **TBD** — Epic 6에서 QuickSight 대시보드 구축 후 [dashboard/](dashboard/)에 스크린샷 첨부.

탭 구성:
- **비즈니스 탭**: 캠페인별 ROI/CTR/CVR, Last-click vs Even-credit 어트리뷰션 비교
- **운영 탭**: 신선도, 행 수, 파일 수, 스냅샷 수, 매니지먼트 DAG 실행 이력

---

## 7. 100x 스케일 아웃 시나리오 (설계만, 구현 X)

현재 일 100만 → 단기 10x → 장기 100x(일 1억) 가정.

**병목과 대응**

| 컴포넌트 | 100x 시 깨질 곳 | 대응 |
|---|---|---|
| Kafka | 단일 브로커 처리량 한계 | 다중 브로커 + 파티션 N배 |
| Spark | Local Mode 메모리 한계 | EMR Cluster Mode (코드 변경 X) |
| Bronze 저장 | 로컬 디스크 IOPS | S3 직접 적재 |
| Iceberg 컴팩션 | 단일 잡 시간 폭증 | 분산 매니지먼트 (`partial-progress.enabled`) |
| Glue Catalog | 단일 카탈로그 부하 | 도메인별 분리 또는 REST 카탈로그 이전 |

🔄 ADR-007 본문(Epic 7)에 상세 설계.

---

## 8. 장애·운영 시나리오

🔄 **TBD** — Epic 7에서 RUNBOOK 1건 작성 (Spark OOM 재시작 시나리오).

대상 시나리오 (가이드 §4):
- **Spark Streaming 새벽 OOM** — 체크포인트 복구 + 멱등성으로 재처리 안전
- **3개월 백필** — Expire/Orphan 정책이 백필 윈도우를 막지 않도록 보존 90일 설정
- **컴팩션과 streaming MERGE 동시 실행** — Iceberg OCC가 충돌 감지·중재, 시간대 분리 운영

---

## 9. 멱등성 / 재처리 가능성 설계

### 멱등성 보장
- **MERGE 키**: `event_id` (UUID)
- **갱신 가드**: `s.ingested_at > t.ingested_at` 조건. 옛 데이터로 새 데이터를 덮어쓰지 못함.
- **Bronze 보존 90일**: 모든 윈도우 재처리 안전 윈도우.

### 재처리 가능성
- Bronze는 append-only 파일이라 언제든 다시 읽어 Silver MERGE 재실행 가능.
- Gold는 7일 윈도우 rebuild — 어제·그제 데이터 변경되어도 자동 반영.
- 백필: `--from-date YYYY-MM-DD --to-date YYYY-MM-DD` 인자로 임의 윈도우 재처리.

상세 SQL 패턴: [docs/DATA_MODEL.md §5](../docs/DATA_MODEL.md#5-merge-멱등성-보장-핵심-설계)

---

## 갱신 이력

- 2026-05-09 v1: 9개 섹션 골격. §1~§4, §7, §9는 PRD/ARCHITECTURE/DATA_MODEL에서 가져와 채움. §5, §6, §8은 Epic 진행 후 채움.
