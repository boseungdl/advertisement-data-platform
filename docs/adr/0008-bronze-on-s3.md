# ADR-008. Bronze raw zone을 로컬 디스크에서 AWS S3로 이전

> Status: Accepted
> Date: 2026-05-10
> 결정자: 본인 (1인 프로젝트)

## Context (상황)

- 초기 설계(2026-05-09)는 비용 절감 + 단순성 위해 Bronze raw zone을 **로컬 도커 named volume**(`/warehouse/raw/ad-events/`)에 두었음.
- Silver/Gold는 처음부터 AWS S3 + Glue Iceberg.
- 일 100만 이벤트 가정 → 일 ~1GB raw, 90일 보존 = ~90GB.

## Options (검토한 옵션)

- **A. Bronze 로컬 유지** (초기 설계)
- **B. Bronze를 S3에 Parquet으로 이전** (이번 결정)
- **C. Bronze를 S3에 Iceberg로 이전**

## Decision (결정)

**옵션 B** — Bronze 위치를 S3로 이전, 형식은 **Parquet 유지**(Iceberg 아님).

## Rationale (이유)

- **일관성**: 모든 데이터가 S3에 있어 백업·재해복구 자동. 로컬 디스크 의존성 제거.
- **통합 조회**: Athena를 통해 Bronze도 조회 가능 (Glue partition table로 등록 시 — 선택적).
- **운영 단순성**: 로컬 I/O와 S3 transfer를 분리 운영하던 부담 제거. 한 곳에서 권한·암호화·라이프사이클 관리.
- **Bronze는 여전히 Parquet**: append-only 특성 + Iceberg의 핵심 가치(MERGE/스냅샷)가 Bronze에서는 의미 작음. 카탈로그 등록도 선택적.
- **C(Bronze=Iceberg) 미선택 이유**: Compaction/Expire를 Bronze에도 적용하면 관리 포인트만 늘고 효익 작음. final-project 가이드의 "raw는 append-only" 원칙과 일치.

## Consequences (트레이드오프)

### 즉시
- **비용 증가**: 90GB × $0.023/GB ≈ **$2/월** (월 $40 한도 내, 5% 미만)
- **S3 transfer 의존**: 네트워크 장애 시 Bronze 적재 일시 실패 가능 → Spark Streaming 체크포인트로 자동 복구
- **로컬 디스크 의존성 제거**: docker-compose의 `warehouse` named volume 사용 안 함

### 장기 (100x 가정)
- 9TB × $0.023 = **약 $200/월** — 90일 후 Glacier transition 정책 필요
- S3 transfer가 단일 prefix에 집중되면 throttling 위험 → 도메인별 prefix 분리 검토

### 갱신된 문서
- [docs/ARCHITECTURE.md](../ARCHITECTURE.md) §3, §4, §5, §6, §12
- [docs/DATA_MODEL.md](../DATA_MODEL.md) Bronze 섹션
- [docs/diagrams/architecture.mmd](../diagrams/architecture.mmd) → PNG 재생성
- [final/infra/docker-compose.yml](../../final/infra/docker-compose.yml) — `warehouse` volume 제거
- [final/README.md](../../final/README.md) — 디렉토리 구조 + 실행 절차 + AWS 셋업 경로 안내

## 향후 재검토 시점

- 100x 트래픽이 현실화될 때 → Glacier transition + 도메인 prefix 분리 ADR 추가 작성
- S3 비용이 월 $10 초과할 때 → Bronze 보존 기간 90일 → 30일 단축 검토
