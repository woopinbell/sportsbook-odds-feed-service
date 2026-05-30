# odds-feed-service

> Single-direction ingress for sports odds data in an online sportsbook
> microservice system. **Java 17 · Spring Boot 3.2 · Maven · Kafka · Redis · Avro.**

## English overview

`odds-feed-service` is the **one-way entry point for odds data** in a
nine-repository sportsbook system. It pulls odds from an external provider,
publishes change events to Kafka, and maintains a Redis write-through cache that
other services read. It does **not** accept bets, price odds, or settle — those
belong to other services.

### Architecture

Odds flow through one seam, the `OddsProvider` interface (ADR-0010), behind which
two adapters sit, selected by Spring profile:

- **`MockOddsProvider`** (`mock`, default) — a deterministic in-memory simulator.
  It seeds fixtures, drives the lifecycle `SCHEDULED → IN_PLAY → FINISHED` on a
  scheduled tick, and moves prices with a mean-reverting random walk. A
  `MockScenario` library injects edge cases (late goal, postponement, market
  suspension, odds crash). Used for development, integration tests, and load
  tests.
- **`TheOddsApiProvider`** (`real`, optional) — polls The Odds API v4 over
  WebClient, gated by a sliding-window rate limiter and a Redis-backed monthly
  quota counter so the free tier (500 req/month) survives. Needs
  `THE_ODDS_API_KEY`.

A single `FeedOrchestrator` is the only component wired to all three sides:
it subscribes to the active provider's per-event `Flux<ProviderEvent>` stream
and fans each event out to the cache and the publisher. Profile choice never
propagates past the orchestrator.

### Features

- **Kafka publish** to four Avro topics, all partitioned by `eventId` so a
  per-event consumer sees a totally ordered stream (ADR-0006): `odds.changed`,
  `market.status.changed`, `event.lifecycle`, `match.result`.
- **1% change threshold** — an `OddsChanged` is published only when the relative
  price move is ≥ 1% (noise reduction). The cache is always updated; the
  threshold gates Kafka, not truth.
- **Redis write-through cache** — `odds:{eventId}:{marketId}:{selectionId}` (raw
  decimal), `event:{eventId}` (JSON), `market:{eventId}:{marketId}` (status),
  24h TTL. betting-service reads this during slip validation.
- **Read HTTP API** — `GET /api/v1/events` (cursor-paginated by kickoff),
  `GET /api/v1/events/{id}`, `GET /api/v1/odds/{eventId}/{marketId}/{selectionId}`.
- **Trader admin API** (called by admin-api) — `POST /internal/v1/events/{eventId}/markets/{marketId}/{suspend,close,reopen}`.

### Tech stack

Java 17 (records, sealed interfaces) · Spring Boot 3.2.11 (Web + WebFlux
WebClient + Data Redis + Kafka + Actuator) · Apache Avro (no Schema Registry in
V1 — the shared `shared-protocol` artifact is the single source of truth,
ADR-0014) · Project Reactor · Lettuce · Micrometer + OpenTelemetry + Logstash
JSON logs (ADR-0007) · JUnit 5 + AssertJ + Mockito + Testcontainers + WireMock ·
Spotless + Checkstyle.

### Build & run

```sh
( cd ../shared-protocol && ./mvnw -DskipTests install )   # once, into mavenLocal
./mvnw verify                                             # build + test
./mvnw spring-boot:run -Dspring-boot.run.profiles=mock    # mock mode (no key)
THE_ODDS_API_KEY=xxx ./mvnw spring-boot:run -Dspring-boot.run.profiles=real
```

Mock mode still needs Redis and Kafka reachable (see below).

### Performance

Publisher → Kafka throughput baseline: **~290,000 events/sec** (single thread,
in-process broker), about 3× the 100k events/sec target. See
[`load-test/results/BEST.md`](./load-test/results/BEST.md). This is a JVM-side
ceiling; a networked production broker will be lower.

### Limitations (V1)

Real-mode `getMatchResult` is a stub (V2 `/scores`); no Schema Registry;
single-instance (multi-instance would double-publish); end-to-end and HTTP
latency not yet measured; only the `MATCH_RESULT_1X2` market is generated.
Details below and in [`docs/reflection/`](./docs/reflection/).

---

> 아래는 한국어 본문이다. 코드 식별자·주석·commit·API 메시지는 영어(ADR-0016).
> 학습·면접 준비 기록은 [`docs/`](./docs/) — 커밋별 [`docs/commits/`](./docs/commits/),
> 회고 [`docs/reflection/`](./docs/reflection/).

## 시스템에서의 위치

[sportsbook](../) 9 repo 중 Phase 2 leaf. `shared-protocol`만 의존하며, 다른 sportsbook 서비스는 의존하지 않는다 (`wallet-service`, `risk-service`와 병렬 진행 가능).

- **의존**: `shared-protocol` (Avro 이벤트 스키마 + 도메인 value object)
- **의존받음**: `betting-service` (베팅 슬립 검증 시 Redis odds read), `gateway` (Kafka subscribe → WebSocket fan-out), `settlement-service` (`MatchResult` 이벤트 consume)

전체 시스템 컨텍스트와 cross-cutting 결정은 [../CLAUDE.md](../CLAUDE.md) 및 [orchestration/docs/architecture/decisions/](../orchestration/docs/architecture/decisions/) 참고.

## 책임 범위

**DO**:
- 외부 odds API 폴링 또는 WebSocket 수신 (mock 기본, real 옵션)
- Kafka publish:
  - `odds.changed` — selection별 odds 변경 (1% 이상 변동 시만 publish, noise reduction)
  - `market.status.changed` — open/suspended/closed
  - `event.lifecycle` — scheduled / in_play / finished / cancelled / postponed
  - `match.result` — 경기 종료 시 결과 (settlement-service가 consume)
- Redis 캐시 write-through 유지 (`odds:{eventId}:{marketId}:{selectionId}`, `event:{eventId}`, `market:{eventId}:{marketId}`)
- HTTP read API: `/api/v1/events`, `/api/v1/events/{id}`, `/api/v1/odds/{eventId}/{marketId}/{selectionId}`
- Trader 운영 endpoint (admin-api 호출용): `POST /internal/v1/events/{eventId}/markets/{marketId}/{suspend,close,reopen}`

**DO NOT**:
- 베팅 접수 → `betting-service`
- odds 자체 산정 (트레이더/북메이커 영역, 외부 공급자가 제공)
- 정산 → `settlement-service`

> odds margin / overround 계산·검증은 책임 범위에 속하지만 V1 시뮬레이터는 base
> 확률 합이 1.0(margin 0)이라 실제 margin 주입은 미구현 — 제한사항 참조.

## 기술 스택

| 항목 | 결정 |
|---|---|
| 언어 / JVM | Java 17 LTS (records / sealed interfaces) |
| Build | Maven (`spring-boot-starter-parent` 상속 — application repo) |
| Framework | Spring Boot 3.2.11 — Web + WebFlux(WebClient) + Data Redis + Kafka + Actuator |
| Reactive | Project Reactor (`Mono` / `Flux`) — Real provider WebClient + provider 스트림 |
| Format / Lint | Spotless (google-java-format) + Checkstyle (semantic rules) |
| Test | JUnit 5 + AssertJ + Mockito + Spring Boot Test + Testcontainers(Redis) + WireMock + EmbeddedKafka |
| Cache | Redis (Lettuce client) — write-through |
| Broker | Apache Kafka — Avro 직렬화, partition key `eventId`, V1 Schema Registry 없음 |
| Observability | logstash-logback-encoder (JSON logs) + Micrometer / Prometheus + OpenTelemetry tracing |
| 외부 API client | Spring WebClient (Reactor) — `THE_ODDS_API_KEY` 환경변수로 활성 |

근거: [ADR-0015](../orchestration/docs/architecture/decisions/0015-stack-pivot-to-java.md), [ADR-0010](../orchestration/docs/architecture/decisions/0010-data-source-strategy.md), [ADR-0006](../orchestration/docs/architecture/decisions/0006-messaging-and-saga.md).

## 빌드 / 실행 / 테스트

```sh
# shared-protocol을 먼저 mavenLocal에 설치 (이미 설치돼 있다면 skip)
( cd ../shared-protocol && ./mvnw -DskipTests install )

# 빌드 + 단위/통합 테스트 (load 태그 테스트는 기본 포함)
./mvnw verify

# Mock 모드 실행 (default, 외부 API 키 불필요)
./mvnw spring-boot:run -Dspring-boot.run.profiles=mock

# Real 모드 실행 (The Odds API 키 필요)
THE_ODDS_API_KEY=xxx ./mvnw spring-boot:run -Dspring-boot.run.profiles=real

# 포맷 자동 적용 / Lint 검사만
./mvnw spotless:apply
./mvnw spotless:check checkstyle:check
```

### 외부 의존 (Mock 모드도 필요)

- Redis: `docker run -p 6379:6379 redis:7-alpine`
- Kafka: `docker run -p 9092:9092 confluentinc/cp-kafka:7.6.0` (또는 `bitnami/kafka`)

Real 모드는 추가로 `THE_ODDS_API_KEY` 환경변수 필요 (무료 tier 500 req/month).

## 구조

```
src/main/java/com/sportsbook/oddsfeed/
├── OddsFeedApplication.java        Spring Boot entrypoint
├── provider/                       OddsProvider + ProviderEvent (sealed) + DTO
│   ├── mock/                       MockOddsProvider, OddsSimulator, MockScenario 4종, ScenarioRotator
│   └── real/                       TheOddsApiProvider, RateLimiter, QuotaCounter
├── cache/                          RedisOddsCache, CacheKeys (write-through)
├── publisher/                      OddsFeedPublisher (Avro, 1% threshold)
├── kafka/                          AvroSerializer/Deserializer, KafkaConfig
├── orchestrator/                   FeedOrchestrator (provider → cache + publish)
├── api/                            EventReadController, OddsReadController, MarketAdminController, EventCatalog
└── config/                         properties 바인딩 + ApplicationConfig (Clock, @EnableScheduling)
```

## 결정 사항 참조

이 repo에 특히 적용되는 결정 (전체 결정은 [../CLAUDE.md](../CLAUDE.md) 참조):

- [ADR-0010 — Data Source Strategy](../orchestration/docs/architecture/decisions/0010-data-source-strategy.md) — Mock + The Odds API duo, `OddsProvider` 인터페이스 격리
- [ADR-0006 — Messaging and Saga](../orchestration/docs/architecture/decisions/0006-messaging-and-saga.md) — Kafka + Avro, partition key `eventId`, Saga + Outbox
- [ADR-0014 — Avro Schema Tooling](../orchestration/docs/architecture/decisions/0014-avro-schema-tooling.md) — `.avsc` 위치, V1 Schema Registry 없음
- [ADR-0007 — Observability](../orchestration/docs/architecture/decisions/0007-observability.md) — JSON logs / OTel / Prometheus
- [ADR-0004 — API Conventions](../orchestration/docs/architecture/decisions/0004-api-conventions.md) — camelCase, RFC 7807, cursor pagination
- [ADR-0013 — Domain Enums](../orchestration/docs/architecture/decisions/0013-domain-enums.md) — `MarketType` V1 4종

### 이 repo 고유 결정 (CLAUDE.md 참조)

- Redis 갱신 패턴: **write-through** (Kafka publish와 동시 Redis update, non-atomic — odds 이벤트 멱등성으로 정당화)
- odds 변경 thresholding: **1% 이상 변경 시만 publish** (cache는 무조건 갱신)
- Mock 시간 scale: 1초 = mock 1분 기본 (부하 테스트 시 조정)
- Mock 시나리오 라이브러리: `LateGoal`, `MatchPostponed`, `SuddenMarketSuspend`, `OddsCrash` (정상 경기는 시뮬레이터 기본 동작)
- Redis key 구조: `odds:{eventId}:{marketId}:{selectionId}` (TTL 24h), `event:{eventId}`, `market:{eventId}:{marketId}`

## 성능

| 시나리오 | 목표 | 측정값 | 측정 환경 |
|---|---|---|---|
| Publisher → Kafka throughput (single thread) | 10만 events/sec | **약 290,000 events/sec** (3회 평균 ≈ 290k) | EmbeddedKafkaZKBroker, ARM macOS, acks=1 + linger.ms=5 + lz4 |
| Mock → Kafka end-to-end latency p99 | < 100ms | 미측정 | Phase 5 e2e suite (orchestration repo)에서 측정 |
| 1% threshold filter 정확성 | 100% | **100%** (`OddsFeedPublisherTest` 7 케이스) | `mvn test` |
| HTTP read p99 (`/api/v1/events`, `/api/v1/odds/...`) | p99 < 50ms | 미측정 | `load-test/scenarios/odds_read.js` (k6) |

세부 측정 기록: [`load-test/results/2026-05-28/BASELINE.md`](./load-test/results/2026-05-28/BASELINE.md). 누적 최고치: [`load-test/results/BEST.md`](./load-test/results/BEST.md). 셋업·재현은 [`load-test/README.md`](./load-test/README.md).

### 측정 caveats

- **단일 노드 in-process broker** — 실제 운영(TCP / 별도 호스트 / 다중 replica)은 더 낮은 throughput이 예상된다. 위 수치는 publisher 측 코드 효율 ceiling.
- **Testcontainers cp-kafka 이미지가 ARM macOS에서 기동 실패**해 EmbeddedKafkaZKBroker로 대체. publisher 측 측정은 유효하나 broker 실측치는 별도 환경에서 재확인 필요.
- **end-to-end latency**는 mock 생성 시점과 broker commit 시점을 상관시켜야 의미가 있어 (별도 프로세스 필요) orchestration phase로 미룸.

## 제한사항 (V1)

- **Real 모드 `getMatchResult`는 stub** (`Optional.empty()`) — The Odds API `/scores` 연동은 V2. V1 정산은 mock 결과로 검증.
- **Schema Registry 없음** — producer/consumer가 같은 `shared-protocol` 버전을 의존해 schema 일치 보장 (ADR-0014). V2에서 Apicurio 검토.
- **single-instance 가정** — orchestrator 인스턴스가 둘이면 double-publish. leader election / event-id 샤딩은 Phase 5 (k8s).
- **MarketType 1종만 생성** — mock은 `MATCH_RESULT_1X2`만. 나머지 3종(ADR-0013)은 publisher/orchestrator per-market 패턴 확립 후 확장.
- **odds margin / overround 미구현** — mock base 확률 합 1.0 (margin 0). 실 margin 주입은 시나리오 확장 후보.
- The Odds API 무료 tier 500 req/month — Real 모드는 시연·통합 검증용. 부하 테스트는 Mock만.
- WebSocket / SSE odds push는 `gateway` 책임. 본 서비스는 Kafka publish만.
