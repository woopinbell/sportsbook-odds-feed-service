package com.sportsbook.oddsfeed.provider.mock;

import static org.assertj.core.api.Assertions.assertThat;

import com.sportsbook.oddsfeed.config.MockProperties;
import com.sportsbook.oddsfeed.provider.EventSummary;
import com.sportsbook.oddsfeed.provider.MatchOutcome;
import com.sportsbook.oddsfeed.provider.ProviderEvent;
import com.sportsbook.oddsfeed.provider.Sport;
import com.sportsbook.protocol.event.EventLifecycleStatus;
import com.sportsbook.protocol.event.MatchFinalStatus;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.Disposable;

class MockOddsProviderTest {

  private static final Instant T0 = Instant.parse("2026-05-28T10:00:00Z");
  private static final MockProperties PROPS =
      new MockProperties(
          1.0,
          new MockProperties.Scenarios(false, 60),
          0.45,
          0.25,
          0.30,
          /* randomSeed */ 42L,
          /* tickIntervalMs */ 500);

  private MockOddsProvider provider;

  @BeforeEach
  void setUp() {
    provider = new MockOddsProvider(PROPS, Clock.fixed(T0, ZoneOffset.UTC));
    provider.seed();
  }

  @Test
  void seedsThreeFootballEvents() {
    List<EventSummary> events = provider.listEvents(Sport.FOOTBALL);
    assertThat(events).hasSize(MockOddsProvider.INITIAL_EVENT_COUNT);
    assertThat(events).allSatisfy(e -> assertThat(e.sport()).isEqualTo(Sport.FOOTBALL));
    assertThat(events)
        .allSatisfy(e -> assertThat(e.status()).isEqualTo(EventLifecycleStatus.SCHEDULED));
  }

  @Test
  void listEventsForUntrackedSportReturnsEmpty() {
    assertThat(provider.listEvents(Sport.BASKETBALL)).isEmpty();
  }

  @Test
  void streamEventsReturnsEmptyForUnknownId() {
    com.sportsbook.protocol.value.EventId unknown =
        new com.sportsbook.protocol.value.EventId(java.util.UUID.randomUUID());
    assertThat(provider.streamEvents(unknown).collectList().block(Duration.ofSeconds(1))).isEmpty();
  }

  @Test
  void tickProgressesEventThroughFullLifecycle() {
    EventSummary first = provider.listEvents(Sport.FOOTBALL).get(0);
    List<ProviderEvent> received = new ArrayList<>();
    Disposable subscription = provider.streamEvents(first.eventId()).subscribe(received::add);

    provider.tick(T0.plusSeconds(2));
    provider.tick(T0.plusSeconds(120));

    subscription.dispose();

    assertThat(received)
        .filteredOn(e -> e instanceof ProviderEvent.LifecycleUpdated)
        .extracting(e -> ((ProviderEvent.LifecycleUpdated) e).status())
        .containsExactly(EventLifecycleStatus.IN_PLAY, EventLifecycleStatus.FINISHED);
  }

  @Test
  void finishedEventPopulatesMatchOutcome() {
    EventSummary first = provider.listEvents(Sport.FOOTBALL).get(0);
    provider.tick(T0.plusSeconds(120));
    java.util.Optional<MatchOutcome> outcome = provider.getMatchResult(first.eventId());
    assertThat(outcome).isPresent();
    assertThat(outcome.get().finalStatus()).isEqualTo(MatchFinalStatus.COMPLETED);
    assertThat(outcome.get().score()).isIn("2-1", "1-1", "0-1");
  }

  @Test
  void oddsUpdatesEmitWhilePriceMoves() {
    EventSummary first = provider.listEvents(Sport.FOOTBALL).get(0);
    List<ProviderEvent.OddsUpdated> oddsEvents = new ArrayList<>();
    Disposable subscription =
        provider
            .streamEvents(first.eventId())
            .ofType(ProviderEvent.OddsUpdated.class)
            .subscribe(oddsEvents::add);

    for (int i = 0; i < 20; i++) {
      provider.tick(T0.plus(Duration.ofMillis(500L * i)));
    }
    subscription.dispose();

    assertThat(oddsEvents).isNotEmpty();
    assertThat(oddsEvents).allSatisfy(o -> assertThat(o.eventId()).isEqualTo(first.eventId()));
  }
}
