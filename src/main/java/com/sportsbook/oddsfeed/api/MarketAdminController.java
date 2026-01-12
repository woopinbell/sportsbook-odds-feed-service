package com.sportsbook.oddsfeed.api;

import com.sportsbook.oddsfeed.cache.RedisOddsCache;
import com.sportsbook.oddsfeed.publisher.OddsFeedPublisher;
import com.sportsbook.protocol.event.MarketStatus;
import com.sportsbook.protocol.value.EventId;
import com.sportsbook.protocol.value.MarketId;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import java.time.Clock;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

/**
 * Trader admin endpoints called from {@code admin-api}. Suspends, closes, or reopens a market; each
 * action publishes a {@code MarketStatusChanged} event and updates the Redis cache so downstream
 * services see the override on the next read.
 *
 * <p>Mounted at {@code /internal/v1} so a future API gateway can route {@code /api/v1/*}
 * (customer-facing) and {@code /internal/v1/*} (admin-facing) to different security policies
 * without per-endpoint annotation.
 */
@RestController
@RequestMapping("/internal/v1/events/{eventId}/markets/{marketId}")
public class MarketAdminController {

  private final RedisOddsCache cache;
  private final OddsFeedPublisher publisher;
  private final Clock clock;

  public MarketAdminController(RedisOddsCache cache, OddsFeedPublisher publisher, Clock clock) {
    this.cache = cache;
    this.publisher = publisher;
    this.clock = clock;
  }

  @PostMapping("/suspend")
  @ResponseStatus(HttpStatus.ACCEPTED)
  public void suspend(
      @PathVariable("eventId") UUID eventId,
      @PathVariable("marketId") UUID marketId,
      @Valid @RequestBody MarketStatusChangeRequest body) {
    transition(eventId, marketId, MarketStatus.SUSPENDED, body.reason());
  }

  @PostMapping("/close")
  @ResponseStatus(HttpStatus.ACCEPTED)
  public void close(
      @PathVariable("eventId") UUID eventId,
      @PathVariable("marketId") UUID marketId,
      @Valid @RequestBody MarketStatusChangeRequest body) {
    transition(eventId, marketId, MarketStatus.CLOSED, body.reason());
  }

  @PostMapping("/reopen")
  @ResponseStatus(HttpStatus.ACCEPTED)
  public void reopen(
      @PathVariable("eventId") UUID eventId,
      @PathVariable("marketId") UUID marketId,
      @Valid @RequestBody MarketStatusChangeRequest body) {
    transition(eventId, marketId, MarketStatus.OPEN, body.reason());
  }

  private void transition(UUID eventUuid, UUID marketUuid, MarketStatus next, String reason) {
    EventId eventId = new EventId(eventUuid);
    MarketId marketId = new MarketId(marketUuid);
    MarketStatus previous = cache.getMarketStatus(eventId, marketId).orElse(MarketStatus.OPEN);
    publisher.publishMarketStatusChanged(
        eventId, marketId, previous, next, reason, clock.instant());
    cache.storeMarketStatus(eventId, marketId, next);
  }

  public record MarketStatusChangeRequest(@NotBlank String reason) {}
}
