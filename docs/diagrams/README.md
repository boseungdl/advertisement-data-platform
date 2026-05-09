# Diagrams

다이어그램 원본(`*.mmd`, `*.dot` 등)과 PNG export 안내.

## PNG 생성 (Mermaid)

```bash
# Mermaid CLI 설치 (1회만)
npm install -g @mermaid-js/mermaid-cli

# architecture.mmd → architecture.png
mmdc -i docs/diagrams/architecture.mmd \
     -o docs/img/architecture.png \
     -w 2000 -H 900 -b white

# Node CLI 안 깔고 한 번만 돌리려면 npx로:
# npx -y -p @mermaid-js/mermaid-cli mmdc -i docs/diagrams/architecture.mmd -o docs/img/architecture.png -w 2000 -H 900 -b white
```

## 왜 PNG로 export하는가

- ARCHITECTURE.md는 GitHub 외에도 Word/PDF/Notion/Confluence/사내 위키 등에서 열림
- mermaid 코드 텍스트를 그대로 박으면 90% 환경에서 깨진 텍스트로 보임
- 면접관이 PDF로 받으면 그냥 텍스트 뭉치를 보게 됨
- → 항상 PNG/SVG로 export 후 이미지 참조

## 갱신 흐름

1. `docs/diagrams/<name>.mmd` 수정
2. 위 `mmdc` 명령으로 `docs/img/<name>.png` 재생성
3. ARCHITECTURE.md는 이미지 참조라 자동 반영
4. 둘 다 git commit
