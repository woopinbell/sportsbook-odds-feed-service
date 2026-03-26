package com.sportsbook.oddsfeed.orchestrator;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.sportsbook.oddsfeed.cache.RedisOddsCache;
import com.sportsbook.oddsfeed.provider.EventSummary;
import com.sportsbook.oddsfeed.provider.MatchOutcome;
import com.sportsbook.oddsfeed.provider.OddsProvider;
import com.sportsbook.oddsfeed.provider.ProviderEvent;
import com.sportsbook.oddsfeed.provider.Sport;
import com.sportsbook.oddsfeed.publisher.OddsFeedPublisher;
import com.sportsbook.protocol.event.EventLifecycleStatus;
import com.sportsbook.protocol.event.MarketStatus;
import com.sportsbook.protocol.event.MatchFinalStatus;
import com.sportsbook.protocol.value.EventId;
import com.sportsbook.protocol.value.MarketId;
import com.sportsbook.protocol.value.Odds;
import com.sportsbook.protocol.value.SelectionId;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

class FeedOrchestratorTest {

  private OddsProvider provider;
  private RedisOddsCache cache;
  private OddsFeedPublisher publisher;
  private FeedOrchestrator orchestrator;

  private EventId eventId;
  private MarketId marketId;
  private SelectionId selectionId;
  private EventSummary summary;

  @BeforeEach
  void setUp() {
    provider = mock(OddsProvider.class);
    cache = mock(RedisOddsCache.class);
    publisher = mock(OddsFeedPublisher.class);
    orchestrator = new FeedOrchestrator(provider, cache, publisher);

    eventId = new EventId(UUID.randomUUID());
    marketId = new MarketId(UUID.randomUUID());
    selectionId = new SelectionId(UUID.randomUUID());
    summary =
        new EventSummary(
            eventId,
            Sport.FOOTBALL,
            "Premier League",
            "Manchester United",
            "Chelsea",
            Instant.parse("2026-06-01T18:00:00Z"),
            EventLifecycleStatus.SCHEDULED);
  }

  @Test
  void refreshStoresEventsAndSubscribesOnce() {
    when(provider.listEvents(Sport.FOOTBALL)).thenReturn(List.of(summary));
    when(provider.listEvents(Sport.BASKETBALL)).thenReturn(List.of());
    when(provider.streamEvents(eventId)).thenReturn(Flux.empty());

    orchestrator.refresh();
    orchestrator.refresh();

    verify(cache, times(2)).storeEvent(summary);
    verify(provider, times(1)).streamEvents(eventId);
  }

  @Test
  void oddsUpdatedPublishesAndCaches() {
    Instant when = Instant.parse("2026-05-28T10:00:00Z");
    ProviderEvent.OddsUpdated event =
        new ProviderEvent.OddsUpdated(
            eventId, marketId, selectionId, Odds.ofDecimal("2.00"), Odds.ofDecimal("2.10"), when);

    orchestrator.dispatch(eventId, event);

    verify(publisher)
        .publishOddsChanged(
            eq(eventId),
            eq(marketId),
            eq(selectionId),
            eq(Odds.ofDecimal("2.00")),
            eq(Odds.ofDecimal("2.10")),
            eq(when));
    verify(cache).storeOdds(eventId, marketId, selectionId, Odds.ofDecimal("2.10"));
  }

  @Test
  void marketStatusUpdatedPublishesAndCachesStatus() {
    Instant when = Instant.parse("2026-05-28T10:00:00Z");
    ProviderEvent.MarketStatusUpdated event =
        new ProviderEvent.MarketStatusUpdated(
            eventId, marketId, MarketStatus.OPEN, MarketStatus.SUSPENDED, "VAR", when);

    orchestrator.dispatch(eventId, event);

    verify(publisher)
        .publishMarketStatusChanged(
            eq(eventId),
            eq(marketId),
            eq(MarketStatus.OPEN),
            eq(MarketStatus.SUSPENDED),
            eq("VAR"),
            eq(when));
    verify(cache).storeMarketStatus(eventId, marketId, MarketStatus.SUSPENDED);
  }

  @Test
  void lifecycleUpdatedRewritesCachedSummary() {
    when(cache.getEvent(eventId)).thenReturn(Optional.of(summary));
    Instant when = Instant.parse("2026-05-28T10:00:00Z");
    ProviderEvent.LifecycleUpdated event =
        new ProviderEvent.LifecycleUpdated(
            eventId, EventLifecycleStatus.IN_PLAY, summary.scheduledStartAt(), when);

    orchestrator.dispatch(eventId, event);

    verify(publisher)
        .publishEventLifecycle(
            eq(eventId),
            eq(EventLifecycleStatus.IN_PLAY),
            eq(summary.scheduledStartAt()),
            eq(when));
    verify(cache)
        .storeEvent(
            new EventSummary(
                eventId,
                Sport.FOOTBALL,
                "Premier League",
                "Manchester United",
                "Chelsea",
                summary.scheduledStartAt(),
                EventLifecycleStatus.IN_PLAY));
  }

  @Test
  void lifecycleFinishedFetchesAndPublishesMatchResult() {
    when(cache.getEvent(eventId)).thenReturn(Optional.of(summary));
    Instant when = Instant.parse("2026-05-28T12:00:00Z");
    MatchOutcome outcome =
        new MatchOutcome(eventId, "2-1", MatchFinalStatus.COMPLETED, Map.of(), when);
    when(provider.getMatchResult(eventId)).thenReturn(Optional.of(outcome));

    orchestrator.dispatch(
        eventId,
        new ProviderEvent.LifecycleUpdated(
            eventId, EventLifecycleStatus.FINISHED, summary.scheduledStartAt(), when));

    verify(publisher)
        .publishMatchResult(
            eq(eventId), eq("2-1"), eq(MatchFinalStatus.COMPLETED), any(), eq(when));
  }

  @Test
  void lifecycleNonFinishedDoesNotPublishMatchResult() {
    when(cache.getEvent(eventId)).thenReturn(Optional.of(summary));
    Instant when = Instant.parse("2026-05-28T10:00:00Z");

    orchestrator.dispatch(
        eventId,
        new ProviderEvent.LifecycleUpdated(
            eventId, EventLifecycleStatus.IN_PLAY, summary.scheduledStartAt(), when));

    verify(provider, never()).getMatchResult(any());
    verify(publisher, never()).publishMatchResult(any(), any(), any(), any(), any());
  }

  @Test
  void streamSubscriptionForwardsEventsToDispatch() {
    Sinks.Many<ProviderEvent> sink = Sinks.many().multicast().onBackpressureBuffer();
    when(provider.listEvents(Sport.FOOTBALL)).thenReturn(List.of(summary));
    when(provider.listEvents(Sport.BASKETBALL)).thenReturn(List.of());
    when(provider.streamEvents(eventId)).thenReturn(sink.asFlux());

    orchestrator.refresh();

    Instant when = Instant.parse("2026-05-28T10:00:00Z");
    sink.tryEmitNext(
        new ProviderEvent.OddsUpdated(
            eventId, marketId, selectionId, Odds.ofDecimal("2.00"), Odds.ofDecimal("2.10"), when));

    verify(publisher)
        .publishOddsChanged(
            eq(eventId), eq(marketId), eq(selectionId), any(Odds.class), any(Odds.class), eq(when));
  }
}
