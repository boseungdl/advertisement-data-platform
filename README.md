# 광고 캠페인 분석 + 운영 모니터링 플랫폼

> Iceberg 기반 광고 데이터 메달리온 파이프라인 + 캠페인 매니저/파이프라인 운영자용 대시보드.
> 메타코드 Lakehouse 강의 8회차 최종 프로젝트.

## 디렉토리 구조

```
.
├── final/             # 8회차 최종 프로젝트 산출물 (평가 대상)
│   ├── README.md          → 평가 답변 9개 섹션
│   ├── infra/             → 환경 구성 (docker-compose, AWS)
│   ├── code/
│   │   ├── ddl/           → Bronze/Silver/Gold DDL
│   │   ├── pipelines/     → 정제·집계·MERGE 로직
│   │   └── health-queries/ → 운영 헬스 쿼리 5~10개
│   ├── orchestration/     → Airflow DAG
│   └── dashboard/         → BI 스크린샷 + 정의
│
├── docs/              # 작업 문서 (워크플로우 9단계 추적)
│   ├── PRD.md             → 1단계 기획
│   ├── ARCHITECTURE.md    → 2단계 설계 - 시스템
│   ├── DATA_MODEL.md      → 2단계 설계 - 스키마
│   ├── BREAKDOWN.md       → 3단계 작업 분해 (Epic/Story)
│   ├── adr/               → 결정 기록 (Epic 7에서 채움)
│   ├── diagrams/          → mermaid 원본
│   ├── img/               → 다이어그램 PNG
│   └── slides/            → 발표 슬라이드 메모
│
├── CLAUDE.md          # 프로젝트 컨텍스트 (스택, 컨벤션, AI 행동 지침)
└── README.md          # ← 지금 이 파일
```

## 시작하기

- 평가용 산출물 보기 → [final/README.md](final/README.md)
- 작업 흐름 보기 → [docs/](docs/)

🔄 실행 가이드(`docker compose up ...`)는 5단계 구현 끝나면 [final/infra/README.md](final/infra/README.md)에 정리됨.
