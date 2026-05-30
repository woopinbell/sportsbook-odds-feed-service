# docs/commits — odds-feed-service 개발 기록

dev 커밋 1개 = 페이지 1개(`NNN.md`). 9단 구조(제목 / 개요 / 작업 순서 / 작업
내역 / 결과 / 요약 / 다음 작업 / 핵심 확인 / 기억·설명 Level). retrospective
커밋(docs(reflection)/docs(commits)/docs(readme finalize))은 문서화 대상이 아니다.

## 목차

| # | 커밋 | 주제 | 일자 |
|---|---|---|---|
| [000](./000.md) | chore(project): initialize layout | 빌드·profile·관측성 골격, application(parent 상속) | 2026-05-28 |
| [001](./001.md) | feat(provider): OddsProvider abstraction | 인터페이스 + sealed `ProviderEvent` + DTO | 2026-05-28 |
| [002](./002.md) | feat(provider): MockOddsProvider | 결정론 시뮬레이터, 평균회귀 random walk | 2026-05-28 |
| [003](./003.md) | feat(provider): MockScenario library | 4 edge-case 시나리오 + rotator | 2026-05-28 |
| [004](./004.md) | feat(provider): TheOddsApiProvider | real adapter, rate limit + 월 quota | 2026-05-28 |
| [005](./005.md) | feat(cache): RedisOddsCache | write-through, 3 key 구조 | 2026-05-28 |
| [006](./006.md) | feat(publisher): Avro Kafka publisher | 1% threshold, partition key=eventId | 2026-05-28 |
| [007](./007.md) | feat(orchestrator): wire together | provider→cache+publisher 조립 | 2026-05-28 |
| [008](./008.md) | feat(api): REST read + trader admin | cursor pagination, suspend/close/reopen | 2026-05-28 |
| [009](./009.md) | test(load): kafka throughput baseline | ~290k events/sec 측정 | 2026-05-28 |
| [010](./010.md) | docs(readme): performance section | 측정값 박제 (좁은 diff) | 2026-05-28 |
| [011](./011.md) | build(deps): commons-pool2 | pooled Lettuce 부팅 의존 (Phase 5 통합) | 2026-05-30 |
| [012](./012.md) | fix(orchestrator): market status both ways | odds tick에 OPEN + terminal에 CLOSE (Phase 5 통합) | 2026-05-30 |
| [013](./013.md) | fix(mock): fully graded result | 1X2 채점 resultDetail + outcome 순서 (Phase 5 통합) | 2026-05-30 |

> [011]~[013]은 dev 단계가 아니라 **Phase 5 전체 스택 통합**에서 드러난 결함의 수정이다(단위 테스트 통과, e2e에서만 드러남).

## L3 빠른 참조 (외워서 설명 — 면접 직전 5분)

면접에서 직접 마주칠 가능성이 높은, "왜 그렇게 했는가"를 외워둘 항목.

- **library vs application** (000) — shared-protocol은 BOM import, odds-feed는
  `spring-boot-starter-parent` 상속. application은 fat jar + spring-boot:run +
  plugin 관리가 필요.
- **`List` vs `Stream` 반환** (001) — Stream은 일회용. orchestrator가 스냅샷을
  여러 번 훑으므로 List.
- **sealed interface + record로 변경 이벤트** (001) — `permits`로 컴파일 타임
  봉인, 단일 `Flux`로 발생 순서 보존.
- **평균회귀(mean reversion)** (002) — 순수 random walk는 발산·`Odds<1.0`
  예외. fair 쪽 10% 당기고 [1.01,100] clamp.
- **추상화 기준(YAGNI)** (003 vs 004) — `MockScenario`는 추상 context를 안 만듦
  (단일 mock). `QuotaCounter`는 인터페이스로 뽑음(테스트 in-memory + 운영 Redis,
  실제 두 구현 존재). "두 번째 구현이 생기면 추출".
- **결정론 UUID 유도** (004) — `UUID.nameUUIDFromBytes`로 opaque 외부 id를 lookup
  테이블 없이 안정 매핑.
- **write-through non-atomic인데 왜 OK** (005) — odds 이벤트 멱등성(다음 변경이
  정정). 돈 다루는 wallet/betting만 outbox(ADR-0006).
- **enum을 이름으로 저장** (005) — ordinal은 순서 변경에 깨짐.
- **Avro Instant 변환 NPE** (006) — `TimestampMillisConversion` 전역 등록 필수
  (Avro 기본 Joda).
- **`BigDecimal` 경계 정밀도** (006) — double는 0.01 부정확. divide에 scale +
  RoundingMode 명시. 정확히 1%는 `>= 0`으로 통과.
- **partition key = eventId** (006) — 한 경기 모든 변경이 같은 partition →
  전순서(ADR-0006).
- **cache 무조건 / publish만 threshold** (007) — threshold는 Kafka 노이즈용이지
  "현재 odds"가 아님. cache가 canonical.
- **Java 17엔 switch 패턴 없음** (007) — instanceof 체인 (switch pattern은 21+).
- **cursor pagination** (008) — offset 아닌 "마지막 본 키" seek, kickoff+eventId
  tie-break, base64 불투명 커서.
- **in-process broker = JVM ceiling** (009) — ~290k/s, 실배포는 더 낮음.
  acks=1은 직렬화/배치 경로 측정용(acks=all은 내구성 게이트).
- **commons-pool2 부팅 의존** (011, Phase 5) — pooled Lettuce는 commons-pool2를
  런타임에 요구. 풀을 안 켜는 단위 테스트는 못 잡고 compose 부팅 크래시로만 드러남.
- **마켓 상태 양방향 캐시** (012, Phase 5) — betting의 `market:{e}:{m}==OPEN`
  게이트 충족을 위해 odds tick에 OPEN, terminal lifecycle에 CLOSE. 단방향
  (suspend/close)만이라 정상 OPEN 누락→전건 거절, 죽은 이벤트 OPEN 잔존이 거울상.
- **mock 결과 채점** (013, Phase 5) — 빈 resultDetail이면 settlement이 stamp할
  계약이 없어 미정산. score로 1X2를 WON/LOST 채점 + outcome을 FINISHED emit 전에
  저장(orchestrator가 동기로 getMatchResult를 부르는 순서 의존성).

## L2 빠른 참조 (문서 보며 설명)

- 의존성 선택(web vs webflux 역할 분리), logback 이중 모드 (000)
- shared-protocol 타입 재사용, `MatchOutcome` 네이밍 충돌 (001)
- `Clock` 주입 + package-private `tick` 테스트 전략, 시간 scale (002)
- package-private 접근점 노출, auto-rotate 결정론 제어, POSTPONED 정지 (003)
- snake_case `@JsonNaming`, poll+diff 필터, sliding-window RateLimiter (004)
- 저장 포맷별 선택(decimal/JSON/enum 이름), key 중앙화 (005)
- 타입드 `KafkaTemplate` 교체, boolean 반환 용도 (006)
- 이벤트당 1구독(`computeIfAbsent`), lifecycle status 갱신 (007)
- EventCatalog vs Redis SCAN, `/api` vs `/internal` 분리, @MockBean Clock (008)
- producer 튜닝, @Tag로 CI 제외 (009)
- in-memory `marketsByEvent` 추적, 멱등 CLOSE, `MarketStatusChanged` publish (012, Phase 5)
- `gradeSelections`의 1X2 매핑, outcome-before-emit 순서 의존성 (013, Phase 5)
