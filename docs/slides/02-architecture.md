# 발표 슬라이드 3·4 — 아키텍처 + 메달리온 의사결정

> 1차 뼈대 / 5단계 구현 후 슬라이드 5~9 추가 작성

---

## 슬라이드 3 — 전체 아키텍처

**제목**: 전체 데이터 흐름

**비주얼**: `docs/img/architecture.png` 그대로 PPT에 삽입. 원본은 `docs/diagrams/architecture.mmd` ([생성 명령](../diagrams/README.md))
> ⚠️ mermaid 코드 텍스트를 슬라이드에 그대로 붙이지 말 것 — 가이드 §3 절대 규칙. PNG export로만.

**환경 분리 색상**:
- 🟡 **노란색 (로컬 도커)**: Kafka, Spark, Airflow, Bronze raw
- 🔵 **파란색 (AWS)**: Iceberg processed/summary, Glue, Athena, QuickSight

**스피커 노트** (말로 할 것):
- 좌→우 한 줄: "Criteo CSV → Kafka → Spark Streaming → Bronze raw → Spark Batch MERGE → Silver Iceberg → Spark 집계 → Gold Iceberg → Athena → QuickSight"
- 환경 분리 강조: "AWS만 가면 비용 폭증, 로컬만 가면 청중 의심. 절충"
- 100x 시나리오 한 줄 미리 깔기: "Spark Local → EMR, 단일 Glue → 카탈로그 분산"

**예상 청중 질문**
- Q: "왜 Bronze는 로컬?" → A: "raw는 90일 보존하는데 S3 비용 부담. 광고 도메인은 raw 재처리 자주 하지만 압축 못해서 로컬이 효율"
- Q: "왜 Glue?" → A: "AWS 네이티브, 매니지드, 비용 거의 0(월 100만 객체 무료). REST 카탈로그도 후보였으나 운영 부담 큼"

---

## 슬라이드 4 — 메달리온 3계층 의사결정

**제목**: 왜 Bronze는 Iceberg가 아닌가? 왜 Silver부터 Iceberg인가?

| 계층 | 저장 형식 | 이유 |
|---|---|---|
| Bronze | Parquet 파일 | append-only면 충분. MERGE/스냅샷 가치 적음. 관리 비용 절약 |
| Silver | **Iceberg** | late-arriving conversion MERGE가 본질. 멱등성. 시간 여행 |
| Gold | **Iceberg** | 7일 윈도우 rebuild 빈번 → snapshot 격리 유리 |

**스피커 노트**
- "강의 데모와 달리 raw도 Iceberg로 두지 않은 이유는, 광고 raw는 append-only 보관용이지 MERGE할 일이 없어서"
- "Iceberg의 본질적 가치는 **MERGE 멱등성**과 **스냅샷 격리**. 그게 필요한 곳에만 적용한 것"
- "Silver에 광고 도메인 본질(늦게 도착하는 전환)이 있어서 Iceberg가 가장 빛나는 자리"

**예상 청중 질문**
- Q: "Iceberg의 어떤 기능이 Silver에서 가장 중요한가?" → A: "**copy-on-write 기반 MERGE + snapshot isolation**. 컴팩션 도중에도 streaming MERGE가 안전하게 진행됨"
- Q: "Delta Lake / Hudi와 비교하면?" → A: "셋 다 비슷한 표 형식. Iceberg는 catalog-agnostic + 엔진 중립이라 Spark/Trino/Athena 다 같이 쓰기 좋음. AWS Athena 네이티브 지원도 결정 요인"

**용어**
- **copy-on-write** = 변경 시 옛 파일은 그대로 두고 새 파일을 만드는 방식. 안전한 동시 작업 보장
- **snapshot isolation** = 작업 중 다른 작업이 데이터 바꿔도 내 작업은 시작 시점 데이터로 보임

---

## 다음 슬라이드 예고

- 🔄 **슬라이드 5**: Iceberg 매니지먼트 자동화 (Compaction / Expire Snapshots / Orphan Cleanup) — 5단계 구현 후
- 🔄 **슬라이드 6**: 운영 헬스 쿼리 5~10개 시연 — 5단계 구현 후
- 🔄 **슬라이드 7**: 100x 스케일 시나리오 (설계만)
- 🔄 **슬라이드 8**: 비즈니스 KPI 결과 (페르소나 성공 기준 달성 여부)
- 🔄 **슬라이드 9**: 회고 — 잘 된 것 / 다시 하면 / 배운 것
