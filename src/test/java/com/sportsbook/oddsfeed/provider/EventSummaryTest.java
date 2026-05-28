package com.sportsbook.oddsfeed.provider;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import com.sportsbook.protocol.event.EventLifecycleStatus;
import com.sportsbook.protocol.value.EventId;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class EventSummaryTest {

  @Test
  void rejectsNullRequiredFields() {
    EventId eventId = new EventId(UUID.randomUUID());
    Instant kickoff = Instant.parse("2026-06-01T18:00:00Z");
    assertThatNullPointerException()
        .isThrownBy(
            () ->
                new EventSummary(
                    null,
                    Sport.FOOTBALL,
                    "Premier League",
                    "Manchester United",
                    "Chelsea",
                    kickoff,
                    EventLifecycleStatus.SCHEDULED))
        .withMessageContaining("eventId");
    assertThatNullPointerException()
        .isThrownBy(
            () ->
                new EventSummary(
                    eventId,
                    Sport.FOOTBALL,
                    "Premier League",
                    "Manchester United",
                    "Chelsea",
                    null,
                    EventLifecycleStatus.SCHEDULED))
        .withMessageContaining("scheduledStartAt");
  }

  @Test
  void carriesAllFieldsExactly() {
    EventId eventId = new EventId(UUID.randomUUID());
    Instant kickoff = Instant.parse("2026-06-01T18:00:00Z");
    EventSummary summary =
        new EventSummary(
            eventId,
            Sport.FOOTBALL,
            "Premier League",
            "Manchester United",
            "Chelsea",
            kickoff,
            EventLifecycleStatus.SCHEDULED);
    assertThat(summary.eventId()).isEqualTo(eventId);
    assertThat(summary.sport()).isEqualTo(Sport.FOOTBALL);
    assertThat(summary.competition()).isEqualTo("Premier League");
    assertThat(summary.homeTeam()).isEqualTo("Manchester United");
    assertThat(summary.awayTeam()).isEqualTo("Chelsea");
    assertThat(summary.scheduledStartAt()).isEqualTo(kickoff);
    assertThat(summary.status()).isEqualTo(EventLifecycleStatus.SCHEDULED);
  }
}
