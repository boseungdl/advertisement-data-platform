# SETUP — 운영자/개발자용 셋업 가이드

> 이 문서는 프로젝트를 **처음 띄우는 사람**을 위한 절차서입니다.
> 평가 답변(도메인/아키텍처/메달리온/Iceberg 가치/헬스/대시보드/스케일/장애/멱등성)은 [final/README.md](../final/README.md)를 참고하세요.

---

## 디렉토리 구조

```
final/
├── README.md           ← 평가 답변 9개 섹션 (발표용)
├── infra/              ✅ 환경 구성 (S1.1 완료) — docker-compose.yml, Dockerfile, requirements.txt, .env.example
├── code/
│   ├── ddl/            (TBD - Epic 3, 4) — silver.sql, gold.sql
│   ├── pipelines/      (TBD - Epic 2~4) — kafka_to_bronze.py, bronze_to_silver.py, silver_to_gold.py
│   └── health-queries/ (TBD - Epic 6) — 5~10개 SQL
├── orchestration/      (TBD - Epic 5) — Airflow DAG: silver merge / gold summary / iceberg compaction·expire·orphan
└── dashboard/          (TBD - Epic 6) — QuickSight 스크린샷 + 데이터셋 정의
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

## 데이터 본질·구조 (Criteo Attribution Dataset)

> 데이터 자체가 무엇이고, 우리 파이프라인에서 어떻게 쪼개져 흐르는지 한 페이지로 설명.

### Criteo Attribution이 뭔가 한 줄로

> **30일치 실제 광고 운영 로그.** 사용자가 어느 광고를 봤고(노출), 클릭했고, 그 후 며칠 안에 사이트에서 구매·가입(전환)했는지를 한 행에 다 담아둔 데이터. 광고 도메인 학술 표준 데이터셋 (Diemert et al., 2017).

규모: **16,468,027 행 × 22 컬럼 / 30일 / 약 700개 캠페인** / TSV.GZ 약 653MB.

### 22 컬럼이 무엇인지 — 5그룹으로 쪼개서 보기

| 그룹 | 컬럼 | 의미 |
|---|---|---|
| **시간** | `timestamp`, `conversion_timestamp` | 광고 노출 시각, 전환 시각 (둘 다 상대 초) |
| **사용자/캠페인** | `uid`, `campaign` | 사용자 ID(해시), 캠페인 ID |
| **이벤트 결과** | `click` (0/1), `conversion` (0/1), `attribution` (0/1) | 이 노출이 클릭됐나, 전환됐나, attribution 받았나 |
| **클릭 행동** | `click_pos`, `click_nb`, `time_since_last_click` | 클릭 위치, 누적 클릭 수, 직전 클릭과의 간격 |
| **비용** | `cost`, `cpo` | 광고비, CPO(Cost Per Order) |
| **익명 카테고리** | `cat1` ~ `cat9` | 광고/사용자 익명화 분류 (광고주·매체 등) |

### ⚠️ 핵심 — 1 row의 의미

**Criteo 원본은 1 row = 1 노출(impression) 이벤트**. 그 노출이:
- 클릭됐으면 `click=1` (안 됐으면 0)
- 그 사용자가 전환했으면 `conversion=1`, `conversion_timestamp`에 전환 시각 기록

즉 **노출/클릭/전환이 행으로 쪼개진 게 아니라 한 행에 컬럼으로 들어 있음**. 실제 광고 운영에서는 노출은 즉시, 클릭은 몇 초 뒤, 전환은 며칠 뒤 도착하는데 — Criteo는 이걸 사후 결합해서 wide format으로 제공.

### 우리 파이프라인은 이걸 어떻게 다시 쪼개나

**`kafka_producer.py --realistic`** (default 권장)

```
Criteo 1 row (wide)
   │
   ▼ kafka_producer.py가 시간차 분리 발행
┌──────────────┬──────────┬──────────────┐
│ ad-impressions│ ad-clicks│ ad-conversions│
│ 즉시         │ +1~30초  │ +1초~며칠     │
└──────────────┴──────────┴──────────────┘
   │              │             │
   ▼              ▼             ▼
       Bronze (S3 raw parquet, 3 topic 통합)
                  │
                  ▼ Spark MERGE (event_id 키)
       Silver (Iceberg processed_events)
                  │ event_type 컬럼 (impression/click/conversion)
                  │ + attribution_lastclick / attribution_evencredit
                  ▼
       Gold (Iceberg campaign_summary_daily)
```

**왜 시간차로 쪼개나** — 광고 운영의 실제 모습은 노출과 전환이 다른 시간에 도착함. Criteo가 사후 결합한 wide format을 우리가 "다시 흩뿌려서" 운영 환경을 시뮬. 이게 **late-arriving conversion 멱등성·MERGE 시연**의 본질이고, Iceberg를 채택한 이유.

**Basic 모드(`--realistic` 빼고)**도 가능 — 1 row → 1 토픽(`ad-events`)에 그대로. click/conversion 컬럼 그대로 wide. 빠른 데모 용도.

### 라이센스

CC BY-NC-SA 4.0 — 비상업·동일조건·저작자 표시. 발표·포트폴리오 OK, 사업화 X.

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
| `~/.aws/` mount 실패 | AWS 자격증명은 S1.3에서 셋업. 임시: `mkdir ~/.aws` 빈 폴더 또는 해당 mount 라인 주석 |
| 첫 빌드 매우 느림 | 정상 (JAR 다운로드 진행). `docker compose logs -f spark-ads`로 진행 확인 |

### 종료

```bash
docker compose --profile streaming down
docker compose down
```

> ✅ S1.1 완료 — 컨테이너 이름 `spark-ads`, network `meta-ads`, mount 경로는 `final/code/`와 직접 연결됨.
