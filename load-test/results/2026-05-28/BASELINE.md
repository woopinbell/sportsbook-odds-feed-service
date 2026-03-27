# Phase 2-C baseline — 2026-05-28

## Setup

- **Host**: macOS, Apple Silicon (ARM), Docker Desktop available but
  unused for this run (Testcontainers' confluentinc/cp-kafka:7.5.0
  failed to start on this host — see `load-test/README.md`).
- **Test driver**: `KafkaPublishThroughputTest` (JUnit 5, tagged
  `load`).
- **Broker**: `EmbeddedKafkaZKBroker` (Spring-Kafka in-process broker,
  ZooKeeper mode).
- **Producer config**: acks=1, linger.ms=5, batch.size=32K,
  compression=lz4.
- **Serializer**: `AvroSerializer` + `StringSerializer`.
- **Payload**: `OddsChanged` Avro records, decimal odds 2.0000 → 2.1000
  (well above the 1% threshold so every send actually publishes).
- **Method**: 1-event warm-up, then 100,000 sequential `publishOddsChanged`
  calls followed by `producerFactory.createProducer().flush()`. Wall time
  between the first post-warm-up send and the flush completion.

## Results

| Run | Events  | Wall (ms) | Throughput (events/sec) |
|-----|---------|-----------|-------------------------|
| 1   | 100 000 | 339       | **294 985**             |
| 2   | 100 000 | 341       | **293 255**             |
| 3   | 100 000 | 352       | **284 091**             |

Stable around **≈ 290k events/sec** — roughly 3× the
sportsbook/CLAUDE.md target of 10만 events/sec.

## Interpretation

- The figure is a JVM-side ceiling against an in-process broker. Real
  Kafka over TCP on a separate host pays network and replica-fanout
  costs; expect a single-broker production figure of ~50-150k/sec
  depending on hardware and replication factor.
- The 1% threshold filter is on the publish path and contributes to the
  cost of every call. A "no-threshold" raw template.send baseline would
  be a touch higher but not dramatically — Avro encoding and Kafka
  client batch logic dominate.
- Single thread, sequential calls. The orchestrator's real publish
  pattern would be driven by the @Scheduled tick and the @Profile-active
  provider's emit rate, which is bounded by tick frequency × selections
  per tick, not by publisher throughput.

## Mock → Kafka end-to-end latency

Not measured in this run. The target (p99 < 100ms) requires a clock
correlation between the MockOddsProvider's tick and the broker's
appended-record timestamp. Deferred to the orchestration phase's e2e
suite.

## Failures observed

None — all three runs passed the `tps > 10_000` floor assertion.
