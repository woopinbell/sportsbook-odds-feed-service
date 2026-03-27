# odds-feed-service

> **English summary** — Single-direction ingress for sports odds data in an
> online sportsbook microservice system. Pulls odds from an external
> provider behind an `OddsProvider` interface — **mock-first** (a
> deterministic simulator used for development, load testing, and chaos
> scenarios) with **The Odds API** as the optional real-mode adapter
> (ADR-0010). Publishes change events to Kafka (`odds.changed`,
> `market.status.changed`, `event.lifecycle`, `match.result`) partitioned by
> `eventId` so downstream consumers (betting / gateway / settlement) see a
> per-event ordered stream. Maintains a Redis write-through cache that
> betting-service reads during slip validation. Read-only HTTP API for
> clients and a trader admin endpoint for manual market suspension.
> **Java 17 + Spring Boot 3.2.x + Maven + Kafka + Redis + Avro.**

**구현 진행 중.**

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
  - `event.lifecycle` — created / scheduled / in_play / finished / cancelled / postponed
  - `match.result` — 경기 종료 시 결과 (settlement-service가 consume)
- Redis 캐시 write-through 유지 (`odds:{eventId}:{marketId}:{selectionId}`, `event:{eventId}`, `market:{eventId}:{marketId}`)
- HTTP read API: `/api/v1/events`, `/api/v1/events/{id}/markets`, `/api/v1/markets/{id}`, `/api/v1/markets/{id}/odds`
- Trader 운영 endpoint (admin-api 호출용): `POST /internal/v1/markets/{id}/{suspend,close,reopen}`
- odds margin 계산 및 검증 (suspicious margin 경고)

**DO NOT**:
- 베팅 접수 → `betting-service`
- odds 자체 산정 (트레이더/북메이커 영역, 외부 공급자가 제공)
- 정산 → `settlement-service`

## 기술 스택

| 항목 | 결정 |
|---|---|
| 언어 / JVM | Java 17 LTS (records / sealed interfaces / pattern matching) |
| Build | Maven (`spring-boot-starter-parent` 상속) |
| Framework | Spring Boot 3.2.11 — Web + WebFlux(WebClient) + Data Redis + Kafka + Actuator |
| Reactive | Project Reactor (`Mono` / `Flux`) — Real provider WebClient만 사용, 본체는 servlet 기반 |
| Format | Spotless + google-java-format |
| Lint | Checkstyle (semantic rules — magic numbers / param count / unused imports) |
| Test | JUnit 5 + AssertJ + Mockito + Spring Boot Test + Testcontainers (Redis / Kafka) |
| Cache | Redis (Lettuce client) — write-through |
| Broker | Apache Kafka — Avro serialized events, partition key `eventId` |
| Observability | logstash-logback-encoder (JSON logs) + Micrometer / Prometheus + OpenTelemetry tracing |
| 외부 API client | Spring WebClient (Reactor) — `THE_ODDS_API_KEY` 환경변수로 활성 |

근거: [ADR-0015](../orchestration/docs/architecture/decisions/0015-stack-pivot-to-java.md), [ADR-0010](../orchestration/docs/architecture/decisions/0010-data-source-strategy.md), [ADR-0006](../orchestration/docs/architecture/decisions/0006-messaging-and-saga.md).

## 빌드 / 실행 / 테스트

```sh
# shared-protocol을 먼저 mavenLocal에 설치 (이미 설치돼 있다면 skip)
( cd ../shared-protocol && ./mvnw -DskipTests install )

# 빌드 + 단위/통합 테스트
./mvnw verify

# 테스트만
./mvnw test

# Mock 모드 실행 (default, 외부 API 키 불필요)
./mvnw spring-boot:run -Dspring-boot.run.profiles=mock

# Real 모드 실행 (The Odds API 키 필요)
THE_ODDS_API_KEY=xxx ./mvnw spring-boot:run -Dspring-boot.run.profiles=real

# 포맷 자동 적용
./mvnw spotless:apply

# Lint 검사만
./mvnw spotless:check checkstyle:check
```

### 외부 의존 (Mock 모드도 필요)

- Redis: `docker run -p 6379:6379 redis:7-alpine`
- Kafka: `docker run -p 9092:9092 confluentinc/cp-kafka:7.6.0` (또는 `bitnami/kafka`)

Real 모드는 추가로 `THE_ODDS_API_KEY` 환경변수 필요 (무료 tier 500 req/month).

## 구조

```
src/
├── main/java/com/sportsbook/oddsfeed/
│   ├── OddsFeedApplication.java        Spring Boot entrypoint
│   ├── provider/                       OddsProvider interface + Mock / Real adapters
│   ├── cache/                          Redis write-through cache
│   ├── publisher/                      Kafka publisher (Avro)
│   ├── api/                            REST controllers (read + trader admin)
│   └── config/                         Spring config beans, properties classes
├── main/resources/
│   ├── application.yml                 공통 기본
│   ├── application-mock.yml            Mock 모드 (default)
│   ├── application-real.yml            Real 모드 (The Odds API)
│   └── logback-spring.xml              JSON 구조화 로깅
└── test/java/com/sportsbook/oddsfeed/  단위 + 통합 테스트
```

세부 파일은 첫 dev 커밋부터 점진적으로 추가된다.

## 결정 사항 참조

이 repo에 특히 적용되는 결정 (전체 결정은 [../CLAUDE.md](../CLAUDE.md) 참조):

- [ADR-0010 — Data Source Strategy](../orchestration/docs/architecture/decisions/0010-data-source-strategy.md) — Mock + The Odds API duo, `OddsProvider` 인터페이스 격리
- [ADR-0006 — Messaging and Saga](../orchestration/docs/architecture/decisions/0006-messaging-and-saga.md) — Kafka + Avro, partition key `eventId`, Saga + Outbox
- [ADR-0014 — Avro Schema Tooling](../orchestration/docs/architecture/decisions/0014-avro-schema-tooling.md) — `.avsc` 위치, V1 Schema Registry 없음
- [ADR-0007 — Observability](../orchestration/docs/architecture/decisions/0007-observability.md) — JSON logs / OTel / Prometheus
- [ADR-0004 — API Conventions](../orchestration/docs/architecture/decisions/0004-api-conventions.md) — camelCase, RFC 7807, cursor pagination
- [ADR-0013 — Domain Enums](../orchestration/docs/architecture/decisions/0013-domain-enums.md) — `MarketType` V1 4종

### 이 repo 고유 결정 (CLAUDE.md 참조)

- Redis 갱신 패턴: **write-through** (Kafka publish와 동시 Redis update)
- odds 변경 thresholding: **1% 이상 변경 시만 publish** (noise reduction)
- Mock 시간 scale: 1초 = mock 1분 기본 (부하 테스트 시 조정 가능)
- Mock 시나리오 라이브러리: `NormalMatch`, `LateGoal`, `MatchPostponed`, `SuddenMarketSuspend`, `OddsCrash`
- Redis key 구조: `odds:{eventId}:{marketId}:{selectionId}` (TTL 24h), `event:{eventId}`, `market:{eventId}:{marketId}`

## 성능

| 시나리오 | 목표 | 측정값 | 측정 환경 |
|---|---|---|---|
| Publisher → Kafka throughput (single thread) | 10만 events/sec | **약 290,000 events/sec** (3회 평균 ≈ 290k) | EmbeddedKafkaZKBroker, ARM macOS, acks=1 + linger.ms=5 + lz4 |
| Mock → Kafka end-to-end latency p99 | < 100ms | TBD | Phase 5 e2e suite (orchestration repo)에서 측정 |
| 1% threshold filter 정확성 | 100% | **100%** (`OddsFeedPublisherTest.oddsChangeBelowThresholdIsNotPublished` 등 7 케이스) | `mvn test` |
| HTTP read p99 (`/api/v1/events`, `/api/v1/odds/...`) | p99 < 50ms | TBD | `load-test/scenarios/odds_read.js` (k6) |

세부 측정 기록: [`load-test/results/2026-05-28/BASELINE.md`](./load-test/results/2026-05-28/BASELINE.md). 누적 최고치: [`load-test/results/BEST.md`](./load-test/results/BEST.md). 부하 테스트 셋업과 재현 방법은 [`load-test/README.md`](./load-test/README.md) 참조.

### 측정 caveats

- **단일 노드 in-process broker** — 실제 운영 환경(TCP / 별도 호스트 / 다중 replica)에서는 더 낮은 throughput이 예상된다. 위 수치는 publisher 측의 코드 효율 ceiling으로 해석해야 한다.
- **Testcontainers cp-kafka 이미지가 ARM macOS에서 기동 실패**해 EmbeddedKafkaZKBroker로 대체. 같은 Kafka 클라이언트 코드를 사용하므로 publisher 측 측정은 유효하나 broker 실측치는 별도 환경에서 다시 확인 필요.
- **end-to-end latency**는 mock 생성 시점부터 broker에 record가 commit되는 시점까지인데, MockOddsProvider와 broker가 별도 프로세스에 있어야 의미 있는 숫자가 나온다 → orchestration phase로 미룸.

## 제한사항 (V1)

- 본 구현 진행 중.
- The Odds API 무료 tier 500 req/month — Real 모드는 시연 / 통합 검증용. 부하 테스트는 Mock 모드만.
- V1 Schema Registry 없음 — producer/consumer는 같은 `shared-protocol` 버전을 의존해 schema 일치 보장 (ADR-0014). V2에서 Apicurio 도입 검토.
- Mock 시나리오는 코드 기반 → DB 기반 시나리오 편집 UI는 V1 외.
- WebSocket / SSE odds push는 `gateway` 책임. 본 서비스는 Kafka publish만.
