package com.sportsbook.oddsfeed.load;

import static org.assertj.core.api.Assertions.assertThat;

import com.sportsbook.oddsfeed.config.KafkaTopicsProperties;
import com.sportsbook.oddsfeed.config.PublishProperties;
import com.sportsbook.oddsfeed.kafka.AvroSerializer;
import com.sportsbook.oddsfeed.publisher.OddsFeedPublisher;
import com.sportsbook.protocol.value.EventId;
import com.sportsbook.protocol.value.MarketId;
import com.sportsbook.protocol.value.Odds;
import com.sportsbook.protocol.value.SelectionId;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.EmbeddedKafkaZKBroker;

/**
 * Sustained publisher → Kafka throughput baseline. Tagged {@code load} so a CI run can opt out with
 * {@code -DexcludedGroups=load}; on a developer laptop this completes in a few seconds.
 *
 * <p>EmbeddedKafkaBroker rather than Testcontainers Kafka — the confluentinc/cp-kafka image fails
 * to start on ARM macOS in this environment (rationale in load-test/README.md). The in-process
 * broker still exercises the real Kafka producer code path; the resulting throughput is therefore a
 * fair JVM-side ceiling.
 */
@Tag("load")
class KafkaPublishThroughputTest {

  private static final int EVENT_COUNT = 100_000;
  private static final KafkaTopicsProperties TOPICS =
      new KafkaTopicsProperties(
          "odds.changed", "market.status.changed", "event.lifecycle", "match.result");

  private static EmbeddedKafkaBroker broker;
  private static DefaultKafkaProducerFactory<String, SpecificRecord> producerFactory;
  private static OddsFeedPublisher publisher;

  @BeforeAll
  static void start() {
    broker =
        new EmbeddedKafkaZKBroker(
            1,
            true,
            3,
            TOPICS.oddsChanged(),
            TOPICS.marketStatusChanged(),
            TOPICS.eventLifecycle(),
            TOPICS.matchResult());
    broker.afterPropertiesSet();

    Map<String, Object> producerProps = new HashMap<>();
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker.getBrokersAsString());
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, AvroSerializer.class);
    producerProps.put(ProducerConfig.LINGER_MS_CONFIG, 5);
    producerProps.put(ProducerConfig.BATCH_SIZE_CONFIG, 32 * 1024);
    producerProps.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
    producerProps.put(ProducerConfig.ACKS_CONFIG, "1");
    producerFactory = new DefaultKafkaProducerFactory<>(producerProps);
    KafkaTemplate<String, SpecificRecord> template = new KafkaTemplate<>(producerFactory);
    publisher =
        new OddsFeedPublisher(template, TOPICS, new PublishProperties(new BigDecimal("0.01")));
  }

  @AfterAll
  static void stop() {
    producerFactory.destroy();
    broker.destroy();
  }

  @Test
  void publishesAtLeastTenKEventsPerSecond() {
    EventId eventId = new EventId(UUID.randomUUID());
    MarketId marketId = new MarketId(UUID.randomUUID());
    SelectionId selectionId = new SelectionId(UUID.randomUUID());
    Odds previous = Odds.ofDecimal("2.00");
    Odds next = Odds.ofDecimal("2.10");
    Instant when = Instant.parse("2026-05-28T10:00:00Z");

    // Warm-up: first send pays the producer-init cost. Discard it.
    publisher.publishOddsChanged(eventId, marketId, selectionId, previous, next, when);

    Instant start = Instant.now();
    for (int i = 0; i < EVENT_COUNT; i++) {
      publisher.publishOddsChanged(eventId, marketId, selectionId, previous, next, when);
    }
    producerFactory.createProducer().flush();
    Duration elapsed = Duration.between(start, Instant.now());

    double tps = EVENT_COUNT / (elapsed.toMillis() / 1000.0);
    System.out.printf(
        "[KafkaPublishThroughputTest] %d events in %d ms → %.0f events/sec%n",
        EVENT_COUNT, elapsed.toMillis(), tps);

    // Hard floor that catches accidental regressions (e.g. flushing per
    // message). The actual figure goes to load-test/results/BEST.md.
    assertThat(tps).isGreaterThan(10_000.0);
  }
}
