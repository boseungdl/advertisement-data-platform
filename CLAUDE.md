


# CLAUDE.md

Behavioral guidelines to reduce common LLM coding mistakes. Merge with project-specific instructions as needed.

**Tradeoff:** These guidelines bias toward caution over speed. For trivial tasks, use judgment.

## 1. Think Before Coding

**Don't assume. Don't hide confusion. Surface tradeoffs.**

Before implementing:
- State your assumptions explicitly. If uncertain, ask.
- If multiple interpretations exist, present them - don't pick silently.
- If a simpler approach exists, say so. Push back when warranted.
- If something is unclear, stop. Name what's confusing. Ask.

## 2. Simplicity First

**Minimum code that solves the problem. Nothing speculative.**

- No features beyond what was asked.
- No abstractions for single-use code.
- No "flexibility" or "configurability" that wasn't requested.
- No error handling for impossible scenarios.
- If you write 200 lines and it could be 50, rewrite it.

Ask yourself: "Would a senior engineer say this is overcomplicated?" If yes, simplify.

## 3. Surgical Changes

**Touch only what you must. Clean up only your own mess.**

When editing existing code:
- Don't "improve" adjacent code, comments, or formatting.
- Don't refactor things that aren't broken.
- Match existing style, even if you'd do it differently.
- If you notice unrelated dead code, mention it - don't delete it.

When your changes create orphans:
- Remove imports/variables/functions that YOUR changes made unused.
- Don't remove pre-existing dead code unless asked.

The test: Every changed line should trace directly to the user's request.

## 4. Goal-Driven Execution

**Define success criteria. Loop until verified.**

Transform tasks into verifiable goals:
- "Add validation" → "Write tests for invalid inputs, then make them pass"
- "Fix the bug" → "Write a test that reproduces it, then make it pass"
- "Refactor X" → "Ensure tests pass before and after"

For multi-step tasks, state a brief plan:
```
1. [Step] → verify: [check]
2. [Step] → verify: [check]
3. [Step] → verify: [check]
```

Strong success criteria let you loop independently. Weak criteria ("make it work") require constant clarification.

## 5. Communication Style (가독성 우선)

**쉬운 말로, 전문용어는 풀어서.**

설명할 때:
- 한국어로 답한다 (기본).
- 전문용어/약어가 나오면 처음 등장 시 **괄호로 한 줄 풀이**를 같이 적는다.
  예) `MERGE (이미 있는 행은 갱신, 없으면 새로 넣는 SQL 명령)`
- 비유를 적극 사용한다. "Iceberg 스냅샷"보다 "사진 찍어두기 — 옛날 상태로 되돌릴 수 있음"이 먼저.
- 결론을 먼저, 이유는 짧게. 큰 표·중첩 리스트는 가능하면 평문 문단으로.
- 한 메시지에 새로운 결정 요청은 **3개 이내**. 그 이상이면 다음 단계로 미룬다.
- 영어 전문용어를 한국어로 의역할 수 있으면 의역 우선, 원어는 괄호로 보조.

체크: 비전공자가 읽어도 큰 그림이 잡히는가? 아니면 다시 풀어쓴다.

## 6. 프로젝트 컨텍스트 (Stack & Conventions)

광고 캠페인 분석 + 운영 모니터링 플랫폼. 상세 문서:
- 기획: [docs/PRD.md](docs/PRD.md)
- 시스템: [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)
- 스키마: [docs/DATA_MODEL.md](docs/DATA_MODEL.md)
- 결정: docs/adr/ (3단계부터 누적)
- 발표: docs/slides/

### Stack

- Language: Python 3.10+
- Streaming/Batch: Apache Spark 3.5.3 (Local Mode in Docker)
- Streaming Source: Kafka (single broker, Docker)
- Iceberg: 1.6.x via spark-sql-iceberg
- Orchestration: Airflow 2.9 (Docker)
- Storage (Bronze): 로컬 `/warehouse/raw` (Parquet)
- Storage (Silver/Gold): AWS S3 + Glue Catalog
- Query: AWS Athena
- BI: AWS QuickSight
- Container: Docker / docker-compose

### Conventions

**Code**
- Format: black, ruff (line 100)
- Type hints: required (mypy lenient)
- Tests: pytest

**Naming**
- Iceberg table: `iceberg.ads.<purpose>` (예: `ads.processed_events`)
- Airflow DAG: `ads_<purpose>_<freq>` (예: `ads_silver_merge_hourly`)
- AWS resource: `<env>-ads-<purpose>` (예: `dev-ads-warehouse`)
- 필수 태그: `env`, `owner`, `project=meta-ads`

**Git**
- Branch: `feat/`, `fix/`, `docs/`, `chore/`
- Commits: Conventional Commits
- PRs: Squash Merge only

### Working Style (must follow)

- prod 직접 배포 금지 → 반드시 PR + dev 검증
- AWS 비용 영향 큰 변경은 추정치 동봉
- 데이터 변경은 dry-run 우선 (`LIMIT`, `--empty`)
- Iceberg 매니지먼트(Compaction/Expire/Orphan)는 streaming MERGE와 시간대 분리

### When You Help Me (AI 행동 지침)

- 작은 단위로 코드 생성 (한 PR = 한 가지 일)
- "왜?" 물으면 트레이드오프 설명
- 모르는 라이브러리 버전은 짐작 X, 확인 요청
- 보안/비용 위험은 코드 생성 전 먼저 경고
- 결정 못한 부분은 옵션 2~3개로 비교 제시

🔄 이 섹션은 5단계 구현 중 추가 룰 발견 시 갱신.


