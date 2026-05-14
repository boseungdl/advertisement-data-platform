# Criteo Attribution Dataset — 실제 헤더 확인 결과

> 확인 파일: `final/infra/data/pcb_dataset_final.tsv.gz` (623MB, Criteo 미러본)
> 확인일: 2026-05-12

## 컬럼 수: **22개**

| # | 컬럼 | 의미 |
|---|---|---|
| 1 | `timestamp` | 노출 시각 (상대시간, 초 단위 / 0부터 시작) |
| 2 | `uid` | 익명 유저 ID |
| 3 | `campaign` | 캠페인 ID |
| 4 | `conversion` | 전환 여부 (0/1) |
| 5 | `conversion_timestamp` | 전환 시각 (없으면 -1) |
| 6 | `conversion_id` | 전환 ID (없으면 -1) |
| 7 | `attribution` | **어트리뷰션 플래그** (이 클릭이 전환에 기여했는가) |
| 8 | `click` | 클릭 여부 (0/1) |
| 9 | `click_pos` | 그 유저의 *클릭 순번* |
| 10 | `click_nb` | 전환까지의 *총 클릭 수* |
| 11 | `cost` | 입찰 비용 |
| 12 | `cpo` | per-order cost (CPA의 또 다른 표현) |
| 13 | `time_since_last_click` | 직전 클릭 이후 경과 시간 |
| 14-22 | `cat1` ~ `cat9` | 익명화된 카테고리 변수 9개 |

## 샘플 5행 (해석)

- **row1**: 노출만 (click=0, conversion=0) — 가장 흔한 케이스
- **row4**: 클릭됨(click=1, click_pos=0, click_nb=7) → 7번의 클릭 끝에 전환(conversion=1)
- 대다수 행은 *노출만* — 클릭/전환은 매우 희박

## 실습 스크립트의 슬림화

`prepare_criteo_data.py` 기본 출력은 **7개**:
`timestamp / uid / campaign / click / conversion / conversion_timestamp / cost`

버려지는 컬럼 (15개):
- `attribution`, `conversion_id`, `click_pos`, `click_nb`, `cpo`, `time_since_last_click`
- `cat1` ~ `cat9`

`--keep-cats` 옵션 주면 `cat1`~`cat9` 살려서 **16개**.

## 주제 결정에 의미하는 것

원본에 이미 있는 *살릴 가치 있는* 컬럼:
- `attribution` — 어트리뷰션 기여도 분석 (가이드 §1에서 "attribution dataset"이라 강조한 이유)
- `click_pos`, `click_nb`, `time_since_last_click` — 유저 클릭 시퀀스/여정 분석
- `cat1` ~ `cat9` — 익명 카테고리 차원 (광고/매체/유저 특성 추정)

→ **추가 데이터 수집 0** 으로 어트리뷰션·세그먼트·시퀀스 분석까지 가능한 데이터셋.
