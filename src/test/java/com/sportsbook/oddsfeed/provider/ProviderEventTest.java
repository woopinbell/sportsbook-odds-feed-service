package com.sportsbook.oddsfeed.provider;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import com.sportsbook.protocol.event.EventLifecycleStatus;
import com.sportsbook.protocol.event.MarketStatus;
import com.sportsbook.protocol.value.EventId;
import com.sportsbook.protocol.value.MarketId;
import com.sportsbook.protocol.value.Odds;
import com.sportsbook.protocol.value.SelectionId;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class ProviderEventTest {

  private static final EventId EVENT = new EventId(UUID.randomUUID());
  private static final MarketId MARKET = new MarketId(UUID.randomUUID());
  private static final SelectionId SELECTION = new SelectionId(UUID.randomUUID());
  private static final Instant NOW = Instant.parse("2026-05-28T10:00:00Z");

  @Test
  void sealedHierarchyExposesExactlyThreePermittedTypes() {
    assertThat(ProviderEvent.class.getPermittedSubclasses())
        .extracting(Class::getSimpleName)
        .containsExactlyInAnyOrder("OddsUpdated", "MarketStatusUpdated", "LifecycleUpdated");
  }

  @Test
  void oddsUpdatedRejectsNullFields() {
    Odds o1 = Odds.ofDecimal("1.85");
    Odds o2 = Odds.ofDecimal("1.90");
    assertThatNullPointerException()
        .isThrownBy(() -> new ProviderEvent.OddsUpdated(null, MARKET, SELECTION, o1, o2, NOW))
        .withMessageContaining("eventId");
    assertThatNullPointerException()
        .isThrownBy(() -> new ProviderEvent.OddsUpdated(EVENT, MARKET, SELECTION, null, o2, NOW))
        .withMessageContaining("previousOdds");
  }

  @Test
  void marketStatusUpdatedAllowsNullReason() {
    ProviderEvent.MarketStatusUpdated event =
        new ProviderEvent.MarketStatusUpdated(
            EVENT, MARKET, MarketStatus.OPEN, MarketStatus.SUSPENDED, null, NOW);
    assertThat(event.reason()).isNull();
    assertThat(event.previousStatus()).isEqualTo(MarketStatus.OPEN);
    assertThat(event.newStatus()).isEqualTo(MarketStatus.SUSPENDED);
  }

  @Test
  void lifecycleUpdatedExposesScheduledStart() {
    Instant kickoff = NOW.plusSeconds(3600);
    ProviderEvent.LifecycleUpdated event =
        new ProviderEvent.LifecycleUpdated(EVENT, EventLifecycleStatus.SCHEDULED, kickoff, NOW);
    assertThat(event.scheduledStartAt()).isEqualTo(kickoff);
    assertThat(event.status()).isEqualTo(EventLifecycleStatus.SCHEDULED);
  }
}
