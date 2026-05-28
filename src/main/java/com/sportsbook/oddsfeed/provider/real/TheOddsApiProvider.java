package com.sportsbook.oddsfeed.provider.real;

import com.sportsbook.oddsfeed.config.RealProperties;
import com.sportsbook.oddsfeed.provider.EventSummary;
import com.sportsbook.oddsfeed.provider.MatchOutcome;
import com.sportsbook.oddsfeed.provider.OddsProvider;
import com.sportsbook.oddsfeed.provider.ProviderEvent;
import com.sportsbook.oddsfeed.provider.Sport;
import com.sportsbook.protocol.event.EventLifecycleStatus;
import com.sportsbook.protocol.value.EventId;
import com.sportsbook.protocol.value.MarketId;
import com.sportsbook.protocol.value.Odds;
import com.sportsbook.protocol.value.SelectionId;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

/**
 * {@link OddsProvider} backed by The Odds API v4. Active under the {@code real} profile. Polls the
 * upstream on a fixed cadence, maps each response to {@link EventSummary} and emits {@link
 * ProviderEvent.OddsUpdated} for each selection price that changed since the previous poll. The
 * abstraction matches {@link com.sportsbook.oddsfeed.provider.mock.MockOddsProvider}, so the
 * orchestrator added in the next commit doesn't branch on profile.
 *
 * <p>V1 limitations made explicit so the publisher commit doesn't have to discover them:
 *
 * <ul>
 *   <li>No upstream websocket. We poll {@code GET /sports/{key}/odds} and diff snapshots.
 *   <li>{@link #getMatchResult(EventId)} returns {@link Optional#empty()}. The {@code /scores}
 *       endpoint integration is a V2 enhancement; settlement runs against the mock for V1.
 *   <li>Only the first bookmaker's {@code h2h} (head-to-head, equivalent to MATCH_RESULT_1X2) is
 *       read. Multi-bookmaker aggregation is out of V1 scope.
 * </ul>
 *
 * <p>Both the per-minute {@link RateLimiter} and the per-month {@link QuotaCounter} gate outbound
 * calls. A blocked rate limit short-circuits the poll cycle; a blown monthly quota logs and stops
 * polling for the rest of the month.
 */
@Component
@Profile("real")
public class TheOddsApiProvider implements OddsProvider {

  private static final Logger log = LoggerFactory.getLogger(TheOddsApiProvider.class);
  private static final String H2H_MARKET_KEY = "h2h";
  private static final Duration FETCH_TIMEOUT = Duration.ofSeconds(10);

  private final WebClient client;
  private final RealProperties props;
  private final RateLimiter rateLimiter;
  private final QuotaCounter quotaCounter;

  private final Map<EventId, Sinks.Many<ProviderEvent>> streams = new ConcurrentHashMap<>();
  private final Map<EventId, Snapshot> lastSeen = new ConcurrentHashMap<>();

  public TheOddsApiProvider(
      WebClient theOddsWebClient,
      RealProperties props,
      RateLimiter rateLimiter,
      QuotaCounter quotaCounter) {
    this.client = theOddsWebClient;
    this.props = props;
    this.rateLimiter = rateLimiter;
    this.quotaCounter = quotaCounter;
  }

  @Override
  public List<EventSummary> listEvents(Sport sport) {
    String sportKey = sportToApiKey(sport);
    if (sportKey == null) {
      return List.of();
    }
    List<TheOddsApiDtos.Event> raw = fetchEvents(sportKey);
    if (raw == null) {
      return List.of();
    }
    List<EventSummary> summaries = new ArrayList<>(raw.size());
    for (TheOddsApiDtos.Event dto : raw) {
      summaries.add(toSummary(dto, sport));
    }
    return List.copyOf(summaries);
  }

  @Override
  public Flux<ProviderEvent> streamEvents(EventId eventId) {
    return streams
        .computeIfAbsent(eventId, k -> Sinks.many().multicast().onBackpressureBuffer())
        .asFlux();
  }

  @Override
  public Optional<MatchOutcome> getMatchResult(EventId eventId) {
    // V2 enhancement — see class doc.
    return Optional.empty();
  }

  @Scheduled(
      fixedRateString = "${oddsfeed.real.poll-interval-seconds:60}",
      timeUnit = java.util.concurrent.TimeUnit.SECONDS)
  void scheduledPoll() {
    for (Sport sport : Sport.values()) {
      pollSport(sport);
    }
  }

  /**
   * Single poll cycle for one sport. Package-private so unit tests can step polling without the
   * Spring scheduler.
   */
  void pollSport(Sport sport) {
    String sportKey = sportToApiKey(sport);
    if (sportKey == null) {
      return;
    }
    List<TheOddsApiDtos.Event> raw = fetchEvents(sportKey);
    if (raw == null) {
      return;
    }
    for (TheOddsApiDtos.Event dto : raw) {
      EventId eventId = deriveEventId(dto.id());
      Snapshot next = toSnapshot(dto, eventId);
      Snapshot previous = lastSeen.get(eventId);
      if (previous != null) {
        emitDiff(eventId, previous, next);
      }
      lastSeen.put(eventId, next);
    }
  }

  private List<TheOddsApiDtos.Event> fetchEvents(String sportKey) {
    if (!rateLimiter.tryAcquire()) {
      log.debug("rate-limited; skipping fetch for {}", sportKey);
      return null;
    }
    long used = quotaCounter.increment();
    if (used > props.monthlyQuota()) {
      log.warn("monthly quota exhausted ({} > {}); skipping fetch", used, props.monthlyQuota());
      return null;
    }
    try {
      TheOddsApiDtos.Event[] response =
          client
              .get()
              .uri(
                  uriBuilder ->
                      uriBuilder
                          .path("/sports/{key}/odds")
                          .queryParam("apiKey", props.apiKey())
                          .queryParam("regions", "uk")
                          .queryParam("markets", H2H_MARKET_KEY)
                          .queryParam("oddsFormat", "decimal")
                          .build(sportKey))
              .retrieve()
              .bodyToMono(TheOddsApiDtos.Event[].class)
              .block(FETCH_TIMEOUT);
      return response == null ? List.of() : List.of(response);
    } catch (RuntimeException ex) {
      log.warn("failed to fetch odds for {}: {}", sportKey, ex.toString());
      return null;
    }
  }

  private EventSummary toSummary(TheOddsApiDtos.Event dto, Sport sport) {
    EventId eventId = deriveEventId(dto.id());
    return new EventSummary(
        eventId,
        sport,
        dto.sportTitle(),
        dto.homeTeam(),
        dto.awayTeam(),
        dto.commenceTime(),
        EventLifecycleStatus.SCHEDULED);
  }

  private Snapshot toSnapshot(TheOddsApiDtos.Event dto, EventId eventId) {
    Map<SelectionKey, Odds> prices = new HashMap<>();
    MarketId marketId = deriveMarketId(eventId, H2H_MARKET_KEY);
    if (dto.bookmakers() == null || dto.bookmakers().isEmpty()) {
      return new Snapshot(prices, marketId, dto.commenceTime());
    }
    TheOddsApiDtos.Bookmaker book = dto.bookmakers().get(0);
    for (TheOddsApiDtos.Market market : book.markets()) {
      if (!H2H_MARKET_KEY.equals(market.key())) {
        continue;
      }
      for (TheOddsApiDtos.Outcome outcome : market.outcomes()) {
        prices.put(new SelectionKey(market.key(), outcome.name()), Odds.ofDecimal(outcome.price()));
      }
    }
    return new Snapshot(prices, marketId, book.lastUpdate());
  }

  private void emitDiff(EventId eventId, Snapshot previous, Snapshot next) {
    Sinks.Many<ProviderEvent> sink = streams.get(eventId);
    if (sink == null) {
      return;
    }
    Comparator<Map.Entry<SelectionKey, Odds>> stableOrder =
        Comparator.comparing(e -> e.getKey().outcomeName());
    next.prices().entrySet().stream()
        .sorted(stableOrder)
        .forEach(
            entry -> {
              SelectionKey key = entry.getKey();
              Odds newOdds = entry.getValue();
              Odds prevOdds = previous.prices().get(key);
              if (prevOdds == null || !prevOdds.equals(newOdds)) {
                Odds emittedPrevious = prevOdds == null ? newOdds : prevOdds;
                SelectionId selectionId = deriveSelectionId(eventId, key);
                sink.tryEmitNext(
                    new ProviderEvent.OddsUpdated(
                        eventId,
                        next.marketId(),
                        selectionId,
                        emittedPrevious,
                        newOdds,
                        next.observedAt()));
              }
            });
  }

  private String sportToApiKey(Sport sport) {
    return switch (sport) {
      case FOOTBALL -> matchConfiguredKey("soccer_epl");
      case BASKETBALL -> matchConfiguredKey("basketball_nba");
    };
  }

  private String matchConfiguredKey(String preferred) {
    if (props.sportKeys() == null || props.sportKeys().isEmpty()) {
      return preferred;
    }
    return props.sportKeys().stream().filter(s -> s.equals(preferred)).findFirst().orElse(null);
  }

  /**
   * Deterministically derive a UUID from the upstream event ID. The Odds API uses opaque non-UUID
   * strings; using {@link UUID#nameUUIDFromBytes(byte[])} gives us a stable typed ID without a
   * separate lookup table.
   */
  static EventId deriveEventId(String upstreamId) {
    return new EventId(UUID.nameUUIDFromBytes(upstreamId.getBytes(StandardCharsets.UTF_8)));
  }

  static MarketId deriveMarketId(EventId eventId, String marketKey) {
    return new MarketId(
        UUID.nameUUIDFromBytes(
            (eventId.value() + ":" + marketKey).getBytes(StandardCharsets.UTF_8)));
  }

  static SelectionId deriveSelectionId(EventId eventId, SelectionKey key) {
    return new SelectionId(
        UUID.nameUUIDFromBytes(
            (eventId.value() + ":" + key.marketKey() + ":" + key.outcomeName())
                .getBytes(StandardCharsets.UTF_8)));
  }

  /** Internal snapshot of one event's prices, keyed by (marketKey, outcomeName). */
  record Snapshot(Map<SelectionKey, Odds> prices, MarketId marketId, Instant observedAt) {}

  /** Logical pointer to a selection within an event, independent of UUID derivation. */
  record SelectionKey(String marketKey, String outcomeName) {}
}
