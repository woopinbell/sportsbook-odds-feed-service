package com.sportsbook.oddsfeed.api;

import com.sportsbook.oddsfeed.cache.RedisOddsCache;
import com.sportsbook.protocol.value.EventId;
import com.sportsbook.protocol.value.MarketId;
import com.sportsbook.protocol.value.Odds;
import com.sportsbook.protocol.value.SelectionId;
import java.math.BigDecimal;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

/**
 * Read-only odds lookup. The URL carries eventId in addition to marketId / selectionId because we
 * don't maintain a marketId → eventId reverse index in V1 (Redis is keyed on the composite tuple).
 * Adding the reverse index is a Phase 5 optimization once the call pattern justifies it.
 */
@RestController
@RequestMapping("/api/v1/odds")
public class OddsReadController {

  private final RedisOddsCache cache;

  public OddsReadController(RedisOddsCache cache) {
    this.cache = cache;
  }

  @GetMapping("/{eventId}/{marketId}/{selectionId}")
  public OddsResponse getOdds(
      @PathVariable("eventId") UUID eventId,
      @PathVariable("marketId") UUID marketId,
      @PathVariable("selectionId") UUID selectionId) {
    EventId e = new EventId(eventId);
    MarketId m = new MarketId(marketId);
    SelectionId s = new SelectionId(selectionId);
    Odds odds =
        cache
            .getOdds(e, m, s)
            .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Odds not found"));
    return new OddsResponse(eventId, marketId, selectionId, odds.decimal());
  }

  public record OddsResponse(UUID eventId, UUID marketId, UUID selectionId, BigDecimal odds) {}
}
