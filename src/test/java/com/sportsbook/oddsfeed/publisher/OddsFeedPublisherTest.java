package com.sportsbook.oddsfeed.publisher;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

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
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import org.apache.avro.specific.SpecificRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaTemplate;

class OddsFeedPublisherTest {

  private static final KafkaTopicsProperties TOPICS =
      new KafkaTopicsProperties(
          "odds.changed", "market.status.changed", "event.lifecycle", "match.result");
  private static final BigDecimal ONE_PERCENT = new BigDecimal("0.01");

  private KafkaTemplate<String, SpecificRecord> kafkaTemplate;
  private OddsFeedPublisher publisher;
  private EventId eventId;
  private MarketId marketId;
  private SelectionId selectionId;

  @BeforeEach
  @SuppressWarnings("unchecked")
  void setUp() {
    kafkaTemplate = (KafkaTemplate<String, SpecificRecord>) mock(KafkaTemplate.class);
    publisher = new OddsFeedPublisher(kafkaTemplate, TOPICS, new PublishProperties(ONE_PERCENT));
    eventId = new EventId(UUID.randomUUID());
    marketId = new MarketId(UUID.randomUUID());
    selectionId = new SelectionId(UUID.randomUUID());
  }

  @Test
  void oddsChangeBelowThresholdIsNotPublished() {
    boolean published =
        publisher.publishOddsChanged(
            eventId,
            marketId,
            selectionId,
            Odds.ofDecimal("2.00"),
            Odds.ofDecimal("2.01"),
            Instant.parse("2026-05-28T10:00:00Z"));
    assertThat(published).isFalse();
    verify(kafkaTemplate, never())
        .send(
            eq("odds.changed"),
            eq(eventId.value().toString()),
            org.mockito.ArgumentMatchers.<SpecificRecord>any());
  }

  @Test
  void oddsChangeAboveThresholdEmitsOddsChangedToTheCorrectTopic() {
    Instant when = Instant.parse("2026-05-28T10:00:00Z");
    boolean published =
        publisher.publishOddsChanged(
            eventId, marketId, selectionId, Odds.ofDecimal("2.00"), Odds.ofDecimal("2.05"), when);

    assertThat(published).isTrue();
    OddsChanged captured = captureSend("odds.changed", OddsChanged.class);
    assertThat(captured.getEventId()).isEqualTo(eventId.value().toString());
    assertThat(captured.getMarketId()).isEqualTo(marketId.value().toString());
    assertThat(captured.getSelectionId()).isEqualTo(selectionId.value().toString());
    assertThat(captured.getPreviousOdds()).isEqualTo("2.0000");
    assertThat(captured.getNewOdds()).isEqualTo("2.0500");
    assertThat(captured.getChangedAt()).isEqualTo(when);
  }

  @Test
  void oddsChangeAtExactlyThresholdIsPublished() {
    boolean published =
        publisher.publishOddsChanged(
            eventId,
            marketId,
            selectionId,
            Odds.ofDecimal("2.00"),
            Odds.ofDecimal("2.02"),
            Instant.now());
    assertThat(published).isTrue();
  }

  @Test
  void partitionKeyIsEventIdForEveryTopic() {
    Instant now = Instant.parse("2026-05-28T10:00:00Z");
    publisher.publishOddsChanged(
        eventId, marketId, selectionId, Odds.ofDecimal("2.00"), Odds.ofDecimal("2.10"), now);
    publisher.publishMarketStatusChanged(
        eventId, marketId, MarketStatus.OPEN, MarketStatus.SUSPENDED, "VAR", now);
    publisher.publishEventLifecycle(
        eventId, EventLifecycleStatus.IN_PLAY, now.plusSeconds(60), now);
    publisher.publishMatchResult(eventId, "2-1", MatchFinalStatus.COMPLETED, Map.of(), now);

    ArgumentCaptor<String> keys = ArgumentCaptor.forClass(String.class);
    verify(kafkaTemplate, org.mockito.Mockito.times(4))
        .send(
            org.mockito.ArgumentMatchers.anyString(),
            keys.capture(),
            org.mockito.ArgumentMatchers.<SpecificRecord>any());
    assertThat(keys.getAllValues())
        .as("eventId must drive the partition key on every topic per ADR-0006")
        .containsOnly(eventId.value().toString());
  }

  @Test
  void marketStatusChangedCarriesReasonField() {
    Instant when = Instant.parse("2026-05-28T10:00:00Z");
    publisher.publishMarketStatusChanged(
        eventId, marketId, MarketStatus.OPEN, MarketStatus.SUSPENDED, "goal scored", when);
    MarketStatusChanged captured = captureSend("market.status.changed", MarketStatusChanged.class);
    assertThat(captured.getReason()).isEqualTo("goal scored");
    assertThat(captured.getPreviousStatus()).isEqualTo(MarketStatus.OPEN);
    assertThat(captured.getNewStatus()).isEqualTo(MarketStatus.SUSPENDED);
    assertThat(captured.getOccurredAt()).isEqualTo(when);
  }

  @Test
  void eventLifecycleCarriesScheduledStart() {
    Instant when = Instant.parse("2026-05-28T10:00:00Z");
    Instant kickoff = when.plusSeconds(3600);
    publisher.publishEventLifecycle(eventId, EventLifecycleStatus.SCHEDULED, kickoff, when);
    EventLifecycle captured = captureSend("event.lifecycle", EventLifecycle.class);
    assertThat(captured.getStatus()).isEqualTo(EventLifecycleStatus.SCHEDULED);
    assertThat(captured.getScheduledStartAt()).isEqualTo(kickoff);
    assertThat(captured.getOccurredAt()).isEqualTo(when);
  }

  @Test
  void matchResultCarriesScoreAndDetail() {
    Instant when = Instant.parse("2026-05-28T12:00:00Z");
    publisher.publishMatchResult(
        eventId, "2-1", MatchFinalStatus.COMPLETED, Map.of("homeGoals", "2"), when);
    MatchResult captured = captureSend("match.result", MatchResult.class);
    assertThat(captured.getScore()).isEqualTo("2-1");
    assertThat(captured.getFinalStatus()).isEqualTo(MatchFinalStatus.COMPLETED);
    assertThat(captured.getSettledAt()).isEqualTo(when);
    assertThat(captured.getResultDetail()).containsEntry("homeGoals", "2");
  }

  @SuppressWarnings("unchecked")
  private <T extends SpecificRecord> T captureSend(String expectedTopic, Class<T> type) {
    ArgumentCaptor<SpecificRecord> captor = ArgumentCaptor.forClass(SpecificRecord.class);
    verify(kafkaTemplate).send(eq(expectedTopic), eq(eventId.value().toString()), captor.capture());
    assertThat(captor.getValue()).isInstanceOf(type);
    return (T) captor.getValue();
  }
}
