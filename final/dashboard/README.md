# dashboard — BI 스크린샷 + 정의

🔄 **Epic 6**에서 채워질 폴더.

## 예정 파일

```
dashboard/
├── screenshots/
│   ├── business-tab.png        → 캠페인 KPI 화면 (P1 민혁용)
│   └── ops-tab.png             → 파이프라인 헬스 화면 (P2 지수용)
├── quicksight-dataset.json     → Athena 연결 데이터셋 정의 (export)
└── dashboard-spec.md           → 위젯 구성 + 메트릭 매핑
```

## 탭 구성

### 비즈니스 탭 (P1 민혁 — 캠페인 매니저)
- 캠페인별 ROI / CTR / CVR 표 (정렬 가능)
- 어트리뷰션 비교 차트: Last-click vs Even-credit
- 일자별 광고비 vs 매출 라인 차트
- 평균 전환 지연 분포 히스토그램

### 운영 탭 (P2 지수 — 파이프라인 운영자)
- 데이터 신선도 게이지 (목표 P95 1시간)
- 일자별 행 수 비교 (Bronze/Silver/Gold)
- Iceberg 파일 수 추이 (컴팩션 효과)
- 스냅샷 누적 수 (Expire 작동 확인)
- 매니지먼트 DAG 실행 이력 + 실패율

## BI 도구

AWS QuickSight (Standard Edition, 1인 월 약 $24).
데이터 소스: Athena → `iceberg_ads_db.processed_events` / `iceberg_ads_db.campaign_summary_daily`.
