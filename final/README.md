# 최종 프로젝트 — 광고 캠페인 분석 + 운영 모니터링 플랫폼

> 메타코드 Lakehouse 강의 8회차 최종 프로젝트 산출물.
> 작성일: 2026-05-09 / 진행 단계: 4단계 Pre-flight (구현 전 1차 뼈대)

> 이 README는 **살아있는 문서**. Epic 진행하면서 🔄 TBD 표시된 섹션이 채워집니다.

---

## 디렉토리 구조

```
final/
├── README.md           ← 지금 이 파일 (평가 답변 9개 섹션)
├── infra/              ✅ 환경 구성 (S1.1 완료) — docker-compose.yml, Dockerfile, requirements.txt, .env.example
├── code/
│   ├── ddl/            🔄 테이블 정의 (Epic 3, 4) — silver.sql, gold.sql
│   ├── pipelines/      🔄 처리 로직 (Epic 2~4) — kafka_to_bronze.py, bronze_to_silver.py, silver_to_gold.py
│   └── health-queries/ 🔄 운영 헬스 쿼리 (Epic 6) — 5~10개 SQL
├── orchestration/      🔄 Airflow DAG (Epic 5) — silver merge / gold summary / iceberg compaction·expire·orphan
└── dashboard/          🔄 BI 산출물 (Epic 6) — QuickSight 스크린샷 + 데이터셋 정의
```

빈 폴더는 `.gitkeep`만 둔 상태. 실제 파일은 해당 Epic에서 채워집니다.

---

## AWS 셋업 (S1.3 — 최초 1회)

> Iceberg/Glue/Athena/QuickSight를 위한 AWS 자원 준비.
> 비용: S3 5GB + Glue 100만 객체까지 무료. 보수적으로 **월 5만원(약 $40) 한도 알람** 권장.
> ⚠️ Access key는 절대 git에 들어가면 안 됨 (`.env`는 `.gitignore`에 등록됨).

### 1. IAM 사용자 생성 (실습용 별도 권장)

- AWS 콘솔 → **IAM → Users → Create user**
- Name: `iceberg-lab` (CLI 프로필명과 일치)
- Console access: 비활성 (CLI만 사용)
- Permissions: `AdministratorAccess` 직접 부여 (실습 한정 — 운영은 최소권한 원칙)
- 생성 후 **Security credentials → Create access key → CLI**
- Access Key ID / Secret을 **즉시 복사** (Secret은 이 화면 벗어나면 다시 못 봄)

### 2. AWS CLI 프로필 `iceberg-lab`

```bash
aws configure --profile iceberg-lab
# AWS Access Key ID     [None]: <위에서 복사>
# AWS Secret Access Key [None]: <위에서 복사>
# Default region name   [None]: ap-northeast-2
# Default output format [None]: json

# 동작 확인
aws sts get-caller-identity --profile iceberg-lab
```

### 3. S3 버킷 + prefix 생성

```bash
# 버킷 이름은 전 세계 유일 — 본인 이름으로 교체
export BUCKET=meta-ads-lakehouse-<random>
export PREFIX=lakehouse/ads

aws s3 mb s3://$BUCKET --profile iceberg-lab --region ap-northeast-2

# prefix placeholder (콘솔에서 폴더처럼 보이게)
aws s3api put-object --bucket $BUCKET --key $PREFIX/ \
  --profile iceberg-lab --region ap-northeast-2
```

> 사용 경로 (ADR-008 — 모든 데이터를 S3에):
> - `s3://$BUCKET/$PREFIX/raw/ad-events/` ← **Bronze** (raw parquet, 90일 보존)
> - `s3://$BUCKET/$PREFIX/checkpoints/raw-ad-events/` ← Spark Streaming 체크포인트
> - `s3://$BUCKET/$PREFIX/processed_events/` ← Silver (Iceberg)
> - `s3://$BUCKET/$PREFIX/campaign_summary_daily/` ← Gold (Iceberg)

### 4. Glue Data Catalog DB

```bash
aws glue create-database \
  --database-input '{"Name":"iceberg_ads_db","Description":"Meta Ads Lakehouse Iceberg tables"}' \
  --profile iceberg-lab --region ap-northeast-2

aws glue get-database --name iceberg_ads_db \
  --profile iceberg-lab --region ap-northeast-2
```

### 5. 비용 알람 (월 약 $40 한도)

AWS 콘솔 → **Billing → Budgets → Create budget**

- Budget type: Cost budget
- Name: `meta-ads-lakehouse-monthly`
- Amount: **$40 USD / month**
- Alert: 80%(≈$32) 도달 시 본인 이메일로 자동

### 6. `.env` 채우기

```bash
cd final/infra
cp .env.example .env

# .env 열어 본인 값으로 교체:
#   AWS_PROFILE=iceberg-lab        (기본값 OK)
#   S3_BUCKET=<위에서 만든 버킷>
#   S3_PREFIX=lakehouse/ads        (기본값 OK)
#   ICEBERG_DB=iceberg_ads_db      (기본값 OK)
```

→ AWS 셋업 끝. 다음은 [실행 (Bootstrap)](#실행-bootstrap).

### 정리 (실습 종료 후, 비용 막기)

```bash
# S3 버킷 비우고 삭제
aws s3 rm s3://$BUCKET --recursive --profile iceberg-lab
aws s3 rb s3://$BUCKET --profile iceberg-lab

# Glue DB 삭제
aws glue delete-database --name iceberg_ads_db --profile iceberg-lab

# IAM 사용자 Access key Deactivate → Delete (또는 사용자 자체 삭제)
```

---

## 데이터 준비 (S1.5 — 최초 1회)

> Criteo Attribution Dataset (16.4M 이벤트 / 30일 / 700+ 캠페인) → Bronze 입력 CSV 생성.
> 풀스케일은 디스크 1~2GB. 빠르게 시작하려면 샘플 100K부터.

### 1. 원본 다운로드

다음 중 한 곳에서 `criteo_attribution_dataset.tsv.gz` (653MB) 받아 `final/infra/data/`에 저장:

- **HuggingFace 미러** (가장 쉬움): https://huggingface.co/datasets/criteo/criteo-attribution-dataset
- 공식: https://ailab.criteo.com/criteo-attribution-modeling-bidding-dataset/ (신청 폼)
- Kaggle 미러: https://www.kaggle.com/datasets/sharatsachin/criteo-attribution-modeling

```bash
# 다운로드 후
mkdir -p final/infra/data
mv ~/Downloads/criteo_attribution_dataset.tsv.gz final/infra/data/
```

### 2. CSV 변환 (호스트에서 실행)

```bash
# 권장: 100만건 샘플
python final/code/pipelines/prepare_criteo_data.py \
  --input final/infra/data/criteo_attribution_dataset.tsv.gz \
  --output final/infra/data \
  --sample 1000000

# 빠른 테스트: 10만건
python final/code/pipelines/prepare_criteo_data.py \
  --input final/infra/data/criteo_attribution_dataset.tsv.gz \
  --output final/infra/data \
  --sample 100000

# 풀스케일 (디스크 여유 있을 때)
python final/code/pipelines/prepare_criteo_data.py \
  --input final/infra/data/criteo_attribution_dataset.tsv.gz \
  --output final/infra/data \
  --sample 16400000
```

### 3. 검증

스크립트가 마지막에 자동 출력:

```
[전체]
  총 이벤트:       1,000,000건
  클릭:               XX,XXX건  (CTR X.XX%)
  전환:                X,XXX건  (CVR X.XX% of clicks)
  총 비용:       $XX,XXX.XX
  평균 전환지연:   XX.X시간
  캠페인 수:        ~700
  기간:           2026-04-01 00:00 ~ 2026-04-30 23:59
```

컨테이너 안에서 확인:
```bash
docker exec -it spark-ads ls -lh /home/jovyan/data/
# ad_events.csv, ad_events_sample.csv, ad_events_batch2.csv 보이면 OK
```

> `final/infra/data/`는 `.gitignore`에 등록되어 있어 데이터 자체는 git에 포함되지 않음.

---

## 실행 (Bootstrap)

> 첫 빌드는 10~20분 소요 (Iceberg/AWS JAR 4개 다운로드). 두 번째부터는 캐시되어 빠름.

### 1. 컨테이너 띄우기

```bash
cd final/infra

# (최초 1회) 환경 변수 파일 준비 — 본인 값으로 수정
cp .env.example .env

# (Windows에서 ~/.aws 폴더 없으면 1회 생성)
mkdir -p ~/.aws

# 컨테이너 빌드 + 실행
docker compose up -d --build              # spark-ads 빌드 + 실행
docker compose --profile streaming up -d  # kafka + zookeeper + kafka-ui 추가
docker compose ps                         # 4개 모두 running/healthy 확인
```

### 2. 접속 확인

| 항목 | 주소 | 비고 |
|---|---|---|
| Jupyter Notebook | http://localhost:8888 | 토큰: `iceberg` |
| Kafka UI | http://localhost:8090 | 좌측 cluster `local` 보이면 OK |
| Spark UI | http://localhost:4040 | Spark 잡 실행 중에만 활성 |

### 3. Iceberg 패키지 로드 검증

```bash
docker exec -it spark-ads pyspark
# 셸이 뜨면 종료 (Ctrl+D). 에러 없으면 OK.
```

### 가능한 문제

| 증상 | 원인 / 해결 |
|---|---|
| 포트 충돌 (8888/8090/9092) | 다른 프로세스 점유 → `docker-compose.yml`의 host 포트 변경 |
| Docker Desktop 안 켜짐 | 실행 후 재시도 |
| `~/.aws/` mount 실패 | AWS 자격증명은 S1.4에서 셋업. 임시: `mkdir ~/.aws` 빈 폴더 또는 해당 mount 라인 주석 |
| 첫 빌드 매우 느림 | 정상 (JAR 다운로드 진행). `docker compose logs -f spark-iceberg`로 진행 확인 |

### 종료

```bash
docker compose --profile streaming down
docker compose down
```

> ✅ S1.1 완료 — 컨테이너 이름 `spark-ads`, network `meta-ads`, mount 경로는 `final/code/`와 직접 연결됨.

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
