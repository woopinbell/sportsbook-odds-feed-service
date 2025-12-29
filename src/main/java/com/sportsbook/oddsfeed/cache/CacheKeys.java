package com.sportsbook.oddsfeed.cache;

import com.sportsbook.protocol.value.EventId;
import com.sportsbook.protocol.value.MarketId;
import com.sportsbook.protocol.value.SelectionId;

/**
 * Centralized Redis key construction. Format matches the project key contract:
 *
 * <ul>
 *   <li>{@code odds:{eventId}:{marketId}:{selectionId}} — current decimal odds (string)
 *   <li>{@code event:{eventId}} — event summary (JSON)
 *   <li>{@code market:{eventId}:{marketId}} — market status (enum name)
 * </ul>
 *
 * <p>Keys live in a single namespace so the {@code KEYS odds:*} debug pattern works without scoping
 * by application name. When this service runs alongside others on a shared Redis, a cluster-level
 * {@code odds-feed-service:} prefix becomes a candidate — left out for V1 because each service gets
 * its own DB index in docker-compose.
 */
public final class CacheKeys {

  private CacheKeys() {}

  public static String odds(EventId eventId, MarketId marketId, SelectionId selectionId) {
    return "odds:" + eventId.value() + ":" + marketId.value() + ":" + selectionId.value();
  }

  public static String event(EventId eventId) {
    return "event:" + eventId.value();
  }

  public static String market(EventId eventId, MarketId marketId) {
    return "market:" + eventId.value() + ":" + marketId.value();
  }
}
