# odds-feed-service — best measured baselines

| Metric                                        | Best       | Date       | Notes                                      |
|-----------------------------------------------|------------|------------|--------------------------------------------|
| Publisher → Kafka throughput (single thread)  | **295k/s** | 2026-05-28 | EmbeddedKafkaZKBroker, ARM macOS, see [2026-05-28/BASELINE.md](./2026-05-28/BASELINE.md) |
| Mock → Kafka end-to-end p99 latency            | TBD        |            | Pending orchestration e2e instrumentation |
| `GET /api/v1/events` HTTP p99                  | TBD        |            | Pending k6 run against running instance    |
| `GET /api/v1/odds/...` HTTP p99                | TBD        |            | Pending k6 run                             |

## How to update

After each `KafkaPublishThroughputTest` run that beats the existing
best, drop a fresh `YYYY-MM-DD/BASELINE.md` next to this file and
update the row above.
