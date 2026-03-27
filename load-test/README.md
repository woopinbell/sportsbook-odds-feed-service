# odds-feed-service load tests

Two levels of measurement live here:

1. **`src/test/java/com/sportsbook/oddsfeed/load/`** — JVM-side throughput
   tests against an in-process Kafka broker. These run as part of
   `./mvnw test` (tagged so they can be excluded with
   `-DexcludedGroups=load`). They measure the publisher → Kafka path
   without HTTP, which is the right shape for the 10만 events/sec
   target.

2. **`scenarios/`** — k6 scripts exercising the read HTTP surface
   (`/api/v1/events`, `/api/v1/odds/...`) at sustained throughput. Run
   from a separate terminal against a `./mvnw spring-boot:run` instance.

## Why EmbeddedKafkaBroker, not Testcontainers Kafka

The Testcontainers `confluentinc/cp-kafka:7.5.0` image fails to start
on ARM macOS in this environment (the issue is documented in commit
`9c3bd8f`). EmbeddedKafkaBroker comes with `spring-kafka-test`, runs an
in-process broker via the same Kafka client code, and gives us a real
producer-side measurement. The throughput numbers it produces should be
read as a **best-case figure for this JVM**: container/network overhead
adds latency in real deployments. The 10만/sec target is a publisher
ceiling, not a single-node guarantee.

## Running the JVM throughput test

```sh
./mvnw test -Dtest=KafkaPublishThroughputTest
```

Results print to stdout. Capture the latest into `results/BEST.md`.

## Running the k6 scenarios

```sh
# In one shell — boot the service against local Redis + Kafka:
./mvnw spring-boot:run -Dspring-boot.run.profiles=mock

# In another shell — drive HTTP load:
k6 run --vus 200 --duration 1m load-test/scenarios/odds_read.js
```

k6 binary install: `brew install k6` (macOS).

## Targets (sportsbook/CLAUDE.md)

| Scenario                                  | Target              |
|-------------------------------------------|---------------------|
| Kafka publish throughput (publisher side) | 10만 events/sec     |
| Mock → Kafka end-to-end latency           | p99 < 100ms         |
| HTTP read p99 (`/api/v1/odds/...`)        | p99 < 50ms          |
| Threshold filter accuracy                 | 100% (no leakage)   |

Measured baselines live in `results/BEST.md`; per-run snapshots in
`results/YYYY-MM-DD/`.
