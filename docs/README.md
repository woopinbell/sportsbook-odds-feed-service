# odds-feed-service docs

이 repo 문서의 진입점. 사용자(본인)의 학습·면접 준비 자료이며 한국어로 쓴다
(코드 식별자·주석·commit·API 메시지는 영어 — ADR-0016).

## 구성

```
docs/
├── README.md            ← 이 파일 (진입점)
├── commits/             dev 커밋별 기록 (1 커밋 = 1 페이지, 9단 구조)
│   ├── README.md        목차 + L3/L2 빠른 참조 색인
│   └── 000.md ~ 010.md
└── reflection/          빌드 후 회고
    ├── retrospective.md 5단 회고 (무엇을/가설/가설 vs 실제/다시 한다면/한계)
    └── change-cost.md   6~12개월 변경 시나리오 비용 시뮬레이션
```

> `docs/notes/`는 **없다.** 독립 토픽 reference는 Phase 1(shared-protocol)에서만
> 작성했고 Phase 2부터 중단(2026-05-29 결정). 학습 내용은 각 `commits/NNN.md`
> 본문(개발 맥락 속 기술 설명, "Java for C++ devs" 톤)과 "기억·설명 Level"
> (L1/L2/L3) 색인으로 흡수했다.

## 읽는 순서 추천

1. **면접 직전 5분** → [`commits/README.md`](./commits/README.md)의 "L3 빠른
   참조". "왜 그렇게 했는가"의 핵심만.
2. **이 서비스를 처음 보는 사람** → 상위 [`../README.md`](../README.md)(영문
   요약 + 시스템 위치 + 빌드/실행) → `commits/000.md`부터 순서대로.
3. **회고·설계 trade-off** → [`reflection/retrospective.md`](./reflection/retrospective.md)
   → [`reflection/change-cost.md`](./reflection/change-cost.md).

## 이 서비스 한 줄 요약

odds 데이터 단방향 진입점. 외부 공급자(mock/real)의 odds·마켓·lifecycle·결과를
Kafka publish + Redis write-through. 다른 서비스는 Kafka subscribe 또는 Redis
read로 소비. 베팅 접수·odds 산정·정산은 안 함. (전체 맥락: 상위 README와
orchestration ADR.)
