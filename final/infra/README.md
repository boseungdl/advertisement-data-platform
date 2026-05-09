# infra — 환경 구성

🔄 **Epic 1 (환경 셋업)**에서 채워질 폴더.

## 예정 파일

- `docker-compose.yml` — Spark + Iceberg + Kafka + Jupyter 컨테이너 정의
- `Dockerfile` — Spark 이미지 (Iceberg JAR 포함)
- `requirements.txt` — Python 의존성
- `.env.example` — AWS 자격증명 등 환경 변수 샘플
- `aws-setup.md` — S3 버킷 + Glue DB 생성 가이드

## 작업 베이스

외부 데모 [`iceberg-lakehouse-lab/`](../../iceberg-lakehouse-lab/)을 시작점으로 광고 도메인 맞춤 수정 (서비스 이름, 토픽, 경로 등).

## 실행 순서

🔄 5단계 구현 끝나면 `docker compose up -d --build` 등 명령어 정리.
