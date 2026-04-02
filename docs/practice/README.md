# docs/practice — odds-feed-service 재구현 문제지

`docs/commits/NNN.md`(답지)에서 파생한 문제지. 골격·계약(시그니처·공개 API·필드
이름)은 남기고 본문·알고리즘·핵심 값·검증 로직을 `// TODO: <책임>`으로 비웠다.

> **직접 수정 금지.** 답지가 정본이고 문제지는 파생물이다 — 답지가 바뀌면 재파생한다.

## 사용법

1. `practice/NNN.md`의 `## 개요`·`## 작업 순서`·`### 논리 단위`를 읽는다.
2. 각 `// TODO`와 `**책임:**`만 보고 코드를 재유도한다(답지를 안 보고).
3. 막히면 `commits/NNN.md`(답지)로 확인.
4. `## 검증`의 명령을 **실제로 돌려** green을 확인한다.

## 목차 / 스킵 사유

| # | 문제지 | 재유도 초점 |
|---|---|---|
| [000](./000.md) | ✅ | config — pom 의존성 선택 / profile 값 / logback 이중모드 |
| [001](./001.md) | ✅ (얇게) | 계약 커밋 — **sealed `ProviderEvent` union만** blank, 나머지 노출 |
| [002](./002.md) | ✅ | 평균회귀 random walk + lifecycle 상태머신(`advance`) |
| [003](./003.md) | ✅ | 네 시나리오 `apply` + rotator |
| [004](./004.md) | ✅ | poll/diff + sliding-window RateLimiter + 결정론 UUID 유도 |
| [005](./005.md) | ✅ | 캐시 직렬화 포맷(decimal/JSON/enum 이름) |
| [006](./006.md) | ✅ | Avro codec(Instant 변환) + 1% threshold + partition key |
| [007](./007.md) | ✅ | provider→cache+publisher 조립 + instanceof dispatch |
| [008](./008.md) | ✅ | EventCatalog + cursor pagination + admin 전이 |
| [009](./009.md) | ✅ | publisher→Kafka throughput 측정 |
| 010 | ⬜ **스킵** | 순수 문서(README 성능 표 채우기) — 재유도할 구현 없음 |
| 011 | ⬜ **스킵** | 의존 1줄(commons-pool2) — 로직 무변경, 버전은 BOM 관리 |
| [012](./012.md) | ✅ | (Phase 5 fix) 마켓 OPEN/CLOSE 양방향 캐시 |
| [013](./013.md) | ✅ | (Phase 5 fix) resultDetail 1X2 채점 + outcome-before-emit |

> 스킵은 sportsbook/CLAUDE.md의 **실체성 게이트** — "재유도할 실체가 없는
> 문제지는 공회전"(마커·순수 문서 커밋). 답지(`commits/010.md`·`011.md`)는 그대로 둔다.
