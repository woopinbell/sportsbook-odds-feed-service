package com.sportsbook.oddsfeed.provider.mock;

import static org.assertj.core.api.Assertions.assertThat;

import com.sportsbook.oddsfeed.config.MockProperties;
import com.sportsbook.oddsfeed.provider.EventSummary;
import com.sportsbook.oddsfeed.provider.ProviderEvent;
import com.sportsbook.protocol.event.EventLifecycleStatus;
import com.sportsbook.protocol.event.MarketStatus;
import com.sportsbook.protocol.value.Odds;
import java.math.BigDecimal;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.Disposable;

class MockScenarioTest {

  private static final Instant T0 = Instant.parse("2026-05-28T10:00:00Z");
  private static final MockProperties PROPS =
      new MockProperties(1.0, new MockProperties.Scenarios(false, 60), 0.45, 0.25, 0.30, 42L, 500);

  private MockOddsProvider provider;
  private MockOddsProvider.MockEvent firstEvent;

  @BeforeEach
  void setUp() {
    provider = new MockOddsProvider(PROPS, Clock.fixed(T0, ZoneOffset.UTC));
    provider.seed();
    firstEvent = provider.activeEvents().iterator().next();
  }

  @Test
  void sealedHierarchyExposesExactlyFourScenarios() {
    assertThat(MockScenario.class.getPermittedSubclasses())
        .extracting(Class::getSimpleName)
        .containsExactlyInAnyOrder(
            "LateGoal", "MatchPostponed", "SuddenMarketSuspend", "OddsCrash");
  }

  @Test
  void matchPostponedTransitionsScheduledEvent() {
    EventSummary before = firstEvent.summary;
    new MatchPostponed().apply(firstEvent, T0, new Random(0), provider);
    assertThat(firstEvent.status).isEqualTo(EventLifecycleStatus.POSTPONED);
    assertThat(firstEvent.summary.eventId()).isEqualTo(before.eventId());
  }

  @Test
  void matchPostponedRejectsInPlayEvent() {
    provider.tick(T0.plusSeconds(120));
    boolean canApply = new MatchPostponed().canApply(firstEvent, T0.plusSeconds(120));
    assertThat(canApply).isFalse();
  }

  @Test
  void postponedEventStopsAdvancing() {
    new MatchPostponed().apply(firstEvent, T0, new Random(0), provider);
    provider.tick(T0.plusSeconds(3600));
    assertThat(firstEvent.status).isEqualTo(EventLifecycleStatus.POSTPONED);
  }

  @Test
  void suddenMarketSuspendEmitsStatusChangeAndMutatesState() {
    List<ProviderEvent> received = new ArrayList<>();
    Disposable sub = provider.streamEvents(firstEvent.summary.eventId()).subscribe(received::add);

    new SuddenMarketSuspend().apply(firstEvent, T0, new Random(0), provider);
    sub.dispose();

    MarketStatus marketStatus = firstEvent.markets.values().iterator().next().status;
    assertThat(marketStatus).isEqualTo(MarketStatus.SUSPENDED);
    assertThat(received)
        .filteredOn(e -> e instanceof ProviderEvent.MarketStatusUpdated)
        .singleElement()
        .satisfies(
            e -> {
              ProviderEvent.MarketStatusUpdated msu = (ProviderEvent.MarketStatusUpdated) e;
              assertThat(msu.previousStatus()).isEqualTo(MarketStatus.OPEN);
              assertThat(msu.newStatus()).isEqualTo(MarketStatus.SUSPENDED);
              assertThat(msu.reason()).isNotBlank();
            });
  }

  @Test
  void oddsCrashCrashesOneSelectionByMoreThanThirty() {
    Odds before =
        firstEvent
            .markets
            .values()
            .iterator()
            .next()
            .selections
            .values()
            .iterator()
            .next()
            .currentOdds;
    List<ProviderEvent.OddsUpdated> oddsEvents = new ArrayList<>();
    Disposable sub =
        provider
            .streamEvents(firstEvent.summary.eventId())
            .ofType(ProviderEvent.OddsUpdated.class)
            .subscribe(oddsEvents::add);

    new OddsCrash().apply(firstEvent, T0, new Random(0), provider);
    sub.dispose();

    assertThat(oddsEvents).hasSize(1);
    ProviderEvent.OddsUpdated event = oddsEvents.get(0);
    BigDecimal ratio =
        event
            .newOdds()
            .decimal()
            .divide(event.previousOdds().decimal(), 4, java.math.RoundingMode.HALF_EVEN);
    assertThat(ratio.doubleValue()).isLessThan(0.5);
    assertThat(before).isNotNull();
  }

  @Test
  void lateGoalRequiresInPlayWithinFinalWindow() {
    LateGoal scenario = new LateGoal();
    assertThat(scenario.canApply(firstEvent, T0)).isFalse();

    provider.tick(T0.plusSeconds(2));

    Duration matchDurationReal = Duration.ofSeconds(90);
    Instant nearEnd = firstEvent.endAt.minus(Duration.ofSeconds(5));
    assertThat(scenario.canApply(firstEvent, nearEnd)).isTrue();
    assertThat(matchDurationReal).isNotNull();
  }

  @Test
  void rotatorDispatchesOneScenarioPerTick() {
    ScenarioRotator rotator = new ScenarioRotator(PROPS, provider, Clock.fixed(T0, ZoneOffset.UTC));
    assertThat(rotator.scenarios()).hasSize(4);
    rotator.rotateOnce(T0);
    boolean someChange =
        provider.activeEvents().stream()
            .anyMatch(
                e ->
                    e.status != EventLifecycleStatus.SCHEDULED
                        || e.markets.values().stream()
                            .anyMatch(m -> m.status != MarketStatus.OPEN));
    assertThat(someChange).isTrue();
  }
}
