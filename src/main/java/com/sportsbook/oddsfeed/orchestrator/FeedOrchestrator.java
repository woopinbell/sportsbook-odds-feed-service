package com.sportsbook.oddsfeed.orchestrator;

import com.sportsbook.oddsfeed.api.EventCatalog;
import com.sportsbook.oddsfeed.cache.RedisOddsCache;
import com.sportsbook.oddsfeed.provider.EventSummary;
import com.sportsbook.oddsfeed.provider.MatchOutcome;
import com.sportsbook.oddsfeed.provider.OddsProvider;
import com.sportsbook.oddsfeed.provider.ProviderEvent;
import com.sportsbook.oddsfeed.provider.Sport;
import com.sportsbook.oddsfeed.publisher.OddsFeedPublisher;
import com.sportsbook.protocol.event.EventLifecycleStatus;
import com.sportsbook.protocol.event.MarketStatus;
import com.sportsbook.protocol.value.EventId;
import com.sportsbook.protocol.value.MarketId;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import reactor.core.Disposable;

/**
 * Wires the three sides of this service together: pulls from {@link OddsProvider}, writes to the
 * {@link RedisOddsCache}, and forwards to the {@link OddsFeedPublisher}.
 *
 * <p>Discovery loop: on {@code @PostConstruct} and on every refresh tick, list events per sport.
 * Each newly-seen event picks up a per-event subscription to {@code provider.streamEvents}, which
 * dispatches each {@link ProviderEvent} to the right cache + publish path.
 *
 * <p>Cache vs publish split: the cache is updated unconditionally on every odds update — the
 * threshold filter is about Kafka noise, not truth. Betting-service reading Redis must see the
 * freshest odds available, even when the change wasn't significant enough to publish. The publisher
 * decides the publish; the cache stays canonical.
 *
 * <p>This component is single-instance by design (one process publishes the wire stream). A
 * multi-instance deployment would double-publish; that's a Phase-5 orchestration concern (leader
 * election or a partitioned-by-event sharding strategy) and is out of V1 scope.
 */
@Component
public class FeedOrchestrator {

  private static final Logger log = LoggerFactory.getLogger(FeedOrchestrator.class);

  private final OddsProvider provider;
  private final RedisOddsCache cache;
  private final OddsFeedPublisher publisher;
  private final EventCatalog catalog;
  private final Map<EventId, Disposable> subscriptions = new ConcurrentHashMap<>();

  // Markets seen per event, so a terminal lifecycle (FINISHED / CANCELLED / POSTPONED) can close
  // them. Populated wherever a (event, market) pair flows through, mirroring the OPEN write — the
  // symmetric half of caching OPEN on odds ticks: without it the market:{e}:{m} key stays OPEN
  // after the event ends and betting-service would accept a slip on a dead event.
  private final Map<EventId, Set<MarketId>> marketsByEvent = new ConcurrentHashMap<>();

  public FeedOrchestrator(
      OddsProvider provider,
      RedisOddsCache cache,
      OddsFeedPublisher publisher,
      EventCatalog catalog) {
    this.provider = provider;
    this.cache = cache;
    this.publisher = publisher;
    this.catalog = catalog;
  }

  @PostConstruct
  void start() {
    refresh();
  }

  @PreDestroy
  void stop() {
    subscriptions.values().forEach(Disposable::dispose);
    subscriptions.clear();
  }

  @Scheduled(
      fixedRateString = "${oddsfeed.orchestrator.refresh-interval-seconds:30}",
      timeUnit = java.util.concurrent.TimeUnit.SECONDS)
  void refresh() {
    for (Sport sport : Sport.values()) {
      for (EventSummary summary : provider.listEvents(sport)) {
        cache.storeEvent(summary);
        catalog.put(summary);
        subscriptions.computeIfAbsent(summary.eventId(), this::subscribe);
      }
    }
  }

  private Disposable subscribe(EventId eventId) {
    log.info("subscribing to provider stream for event {}", eventId.value());
    return provider.streamEvents(eventId).subscribe(event -> dispatch(eventId, event));
  }

  /** Visible for testing — push a ProviderEvent through the cache + publish split. */
  void dispatch(EventId eventId, ProviderEvent event) {
    if (event instanceof ProviderEvent.OddsUpdated odds) {
      handleOdds(odds);
    } else if (event instanceof ProviderEvent.MarketStatusUpdated status) {
      handleMarketStatus(status);
    } else if (event instanceof ProviderEvent.LifecycleUpdated lifecycle) {
      handleLifecycle(lifecycle);
    }
  }

  private void handleOdds(ProviderEvent.OddsUpdated event) {
    publisher.publishOddsChanged(
        event.eventId(),
        event.marketId(),
        event.selectionId(),
        event.previousOdds(),
        event.newOdds(),
        event.occurredAt());
    cache.storeOdds(event.eventId(), event.marketId(), event.selectionId(), event.newOdds());
    // Odds only flow for OPEN markets (the provider skips ticks when status != OPEN), so an odds
    // update implies the market is open. betting-service reads market:{e}:{m} == OPEN before
    // accepting a slip; previously this key was never written (only suspend/close emitted a
    // MarketStatusUpdated), so every bet was rejected as "market not open". Surfaced only in the
    // full-stack Phase-5 e2e — unit tests seeded the market key by hand.
    cache.storeMarketStatus(event.eventId(), event.marketId(), MarketStatus.OPEN);
    trackMarket(event.eventId(), event.marketId());
  }

  private void handleMarketStatus(ProviderEvent.MarketStatusUpdated event) {
    publisher.publishMarketStatusChanged(
        event.eventId(),
        event.marketId(),
        event.previousStatus(),
        event.newStatus(),
        event.reason(),
        event.occurredAt());
    cache.storeMarketStatus(event.eventId(), event.marketId(), event.newStatus());
    trackMarket(event.eventId(), event.marketId());
  }

  private void handleLifecycle(ProviderEvent.LifecycleUpdated event) {
    publisher.publishEventLifecycle(
        event.eventId(), event.status(), event.scheduledStartAt(), event.occurredAt());
    cache
        .getEvent(event.eventId())
        .ifPresent(
            current -> {
              EventSummary updated =
                  new EventSummary(
                      current.eventId(),
                      current.sport(),
                      current.competition(),
                      current.homeTeam(),
                      current.awayTeam(),
                      current.scheduledStartAt(),
                      event.status());
              cache.storeEvent(updated);
              catalog.put(updated);
            });

    // A terminal lifecycle closes the event's markets so no slip is accepted on a dead event. The
    // symmetric half of caching OPEN on odds ticks (bug 3): OPEN was written but never cleared, so
    // a postponed/cancelled/finished event left market:{e}:{m} == OPEN and betting-service would
    // still accept a slip. CLOSE here flips the cache and publishes MarketStatusChanged.
    if (isTerminal(event.status())) {
      closeMarkets(event.eventId(), event.status(), event.occurredAt());
    }

    if (event.status() == EventLifecycleStatus.FINISHED) {
      Optional<MatchOutcome> outcome = provider.getMatchResult(event.eventId());
      outcome.ifPresent(
          o ->
              publisher.publishMatchResult(
                  o.eventId(), o.score(), o.finalStatus(), o.detail(), o.settledAt()));
    }
  }

  private static boolean isTerminal(EventLifecycleStatus status) {
    return status == EventLifecycleStatus.FINISHED
        || status == EventLifecycleStatus.CANCELLED
        || status == EventLifecycleStatus.POSTPONED;
  }

  private void trackMarket(EventId eventId, MarketId marketId) {
    marketsByEvent.computeIfAbsent(eventId, k -> ConcurrentHashMap.newKeySet()).add(marketId);
  }

  /**
   * Flips every known market of {@code eventId} to CLOSED (cache + MarketStatusChanged), reading the
   * prior status so the published transition is accurate. Idempotent: a re-delivered terminal
   * lifecycle skips markets already CLOSED.
   */
  private void closeMarkets(EventId eventId, EventLifecycleStatus status, Instant occurredAt) {
    for (MarketId marketId : marketsByEvent.getOrDefault(eventId, Set.of())) {
      MarketStatus previous = cache.getMarketStatus(eventId, marketId).orElse(MarketStatus.OPEN);
      if (previous == MarketStatus.CLOSED) {
        continue;
      }
      publisher.publishMarketStatusChanged(
          eventId, marketId, previous, MarketStatus.CLOSED, "EVENT_" + status, occurredAt);
      cache.storeMarketStatus(eventId, marketId, MarketStatus.CLOSED);
    }
  }
}
