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
import com.sportsbook.protocol.value.EventId;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.util.Map;
import java.util.Optional;
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

    if (event.status() == EventLifecycleStatus.FINISHED) {
      Optional<MatchOutcome> outcome = provider.getMatchResult(event.eventId());
      outcome.ifPresent(
          o ->
              publisher.publishMatchResult(
                  o.eventId(), o.score(), o.finalStatus(), o.detail(), o.settledAt()));
    }
  }
}
