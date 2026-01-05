package com.sportsbook.oddsfeed.publisher;

import com.sportsbook.oddsfeed.config.KafkaTopicsProperties;
import com.sportsbook.oddsfeed.config.PublishProperties;
import com.sportsbook.protocol.event.EventLifecycle;
import com.sportsbook.protocol.event.EventLifecycleStatus;
import com.sportsbook.protocol.event.MarketStatus;
import com.sportsbook.protocol.event.MarketStatusChanged;
import com.sportsbook.protocol.event.MatchFinalStatus;
import com.sportsbook.protocol.event.MatchResult;
import com.sportsbook.protocol.event.OddsChanged;
import com.sportsbook.protocol.value.EventId;
import com.sportsbook.protocol.value.MarketId;
import com.sportsbook.protocol.value.Odds;
import com.sportsbook.protocol.value.SelectionId;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.Map;
import org.apache.avro.specific.SpecificRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * High-level Kafka publish API for the four topics this service writes to. Carries the 1% threshold
 * filter for {@link #publishOddsChanged} (project threshold contract) and pins the partition key to
 * {@code eventId} on every send so a per-event consumer sees a totally ordered stream (ADR-0006).
 */
@Component
public class OddsFeedPublisher {

  private static final Logger log = LoggerFactory.getLogger(OddsFeedPublisher.class);
  private static final int RELATIVE_SCALE = 6;

  private final KafkaTemplate<String, SpecificRecord> template;
  private final KafkaTopicsProperties topics;
  private final BigDecimal threshold;

  public OddsFeedPublisher(
      KafkaTemplate<String, SpecificRecord> avroKafkaTemplate,
      KafkaTopicsProperties topics,
      PublishProperties publishProps) {
    this.template = avroKafkaTemplate;
    this.topics = topics;
    this.threshold = publishProps.oddsChangeThreshold();
  }

  /**
   * Publish a price movement. Returns {@code false} when the relative move is below the configured
   * threshold and nothing was sent. Returning a boolean lets the orchestrator surface filter ratios
   * in metrics without re-running the math.
   */
  public boolean publishOddsChanged(
      EventId eventId,
      MarketId marketId,
      SelectionId selectionId,
      Odds previous,
      Odds next,
      Instant changedAt) {
    if (!isSignificantChange(previous, next)) {
      log.trace(
          "skipping odds change for {} below threshold ({} -> {})",
          selectionId.value(),
          previous,
          next);
      return false;
    }
    OddsChanged event =
        new OddsChanged(
            eventId.value().toString(),
            marketId.value().toString(),
            selectionId.value().toString(),
            previous.decimal().toPlainString(),
            next.decimal().toPlainString(),
            changedAt);
    send(topics.oddsChanged(), eventId, event);
    return true;
  }

  public void publishMarketStatusChanged(
      EventId eventId,
      MarketId marketId,
      MarketStatus previous,
      MarketStatus next,
      String reason,
      Instant occurredAt) {
    MarketStatusChanged event =
        new MarketStatusChanged(
            eventId.value().toString(),
            marketId.value().toString(),
            previous,
            next,
            reason,
            occurredAt);
    send(topics.marketStatusChanged(), eventId, event);
  }

  public void publishEventLifecycle(
      EventId eventId, EventLifecycleStatus status, Instant scheduledStartAt, Instant occurredAt) {
    EventLifecycle event =
        new EventLifecycle(eventId.value().toString(), status, occurredAt, scheduledStartAt);
    send(topics.eventLifecycle(), eventId, event);
  }

  public void publishMatchResult(
      EventId eventId,
      String score,
      MatchFinalStatus finalStatus,
      Map<String, String> detail,
      Instant settledAt) {
    MatchResult event =
        new MatchResult(eventId.value().toString(), score, finalStatus, detail, settledAt);
    send(topics.matchResult(), eventId, event);
  }

  boolean isSignificantChange(Odds previous, Odds next) {
    BigDecimal prev = previous.decimal();
    BigDecimal absDiff = next.decimal().subtract(prev).abs();
    BigDecimal relative = absDiff.divide(prev, RELATIVE_SCALE, RoundingMode.HALF_EVEN);
    return relative.compareTo(threshold) >= 0;
  }

  private void send(String topic, EventId eventId, SpecificRecord payload) {
    template.send(topic, eventId.value().toString(), payload);
  }
}
