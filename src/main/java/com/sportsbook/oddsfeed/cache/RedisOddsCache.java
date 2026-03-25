package com.sportsbook.oddsfeed.cache;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sportsbook.oddsfeed.config.CacheProperties;
import com.sportsbook.oddsfeed.provider.EventSummary;
import com.sportsbook.protocol.event.MarketStatus;
import com.sportsbook.protocol.value.EventId;
import com.sportsbook.protocol.value.MarketId;
import com.sportsbook.protocol.value.Odds;
import com.sportsbook.protocol.value.SelectionId;
import java.time.Duration;
import java.util.Optional;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

/**
 * Write-through cache for the data downstream services read between Kafka updates. Writes happen
 * synchronously alongside the Kafka publish (the orchestrator calls publisher first, then cache —
 * if either side fails, the other has already committed, but the next provider event will rewrite
 * both). True atomic dual-writes need a transactional outbox; that pattern is reserved for the
 * services that own money (wallet, betting) per ADR-0006.
 *
 * <p>Storage format is deliberately denormalised for fast point reads from betting-service slip
 * validation: betting-service does {@code GET odds:{eventId}:{marketId}:{selectionId}} once per
 * selection in a slip and never needs to deserialize anything more elaborate than a decimal string.
 */
@Component
public class RedisOddsCache {

  private final StringRedisTemplate redis;
  private final ObjectMapper objectMapper;
  private final Duration ttl;

  public RedisOddsCache(
      StringRedisTemplate redis, ObjectMapper objectMapper, CacheProperties props) {
    this.redis = redis;
    this.objectMapper = objectMapper;
    this.ttl = props.ttl();
  }

  public void storeOdds(EventId eventId, MarketId marketId, SelectionId selectionId, Odds odds) {
    redis
        .opsForValue()
        .set(CacheKeys.odds(eventId, marketId, selectionId), odds.decimal().toPlainString(), ttl);
  }

  public Optional<Odds> getOdds(EventId eventId, MarketId marketId, SelectionId selectionId) {
    String value = redis.opsForValue().get(CacheKeys.odds(eventId, marketId, selectionId));
    return value == null ? Optional.empty() : Optional.of(Odds.ofDecimal(value));
  }

  public void storeEvent(EventSummary summary) {
    try {
      redis
          .opsForValue()
          .set(CacheKeys.event(summary.eventId()), objectMapper.writeValueAsString(summary), ttl);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("Failed to serialize EventSummary", e);
    }
  }

  public Optional<EventSummary> getEvent(EventId eventId) {
    String json = redis.opsForValue().get(CacheKeys.event(eventId));
    if (json == null) {
      return Optional.empty();
    }
    try {
      return Optional.of(objectMapper.readValue(json, EventSummary.class));
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("Failed to deserialize EventSummary", e);
    }
  }

  public void storeMarketStatus(EventId eventId, MarketId marketId, MarketStatus status) {
    redis.opsForValue().set(CacheKeys.market(eventId, marketId), status.name(), ttl);
  }

  public Optional<MarketStatus> getMarketStatus(EventId eventId, MarketId marketId) {
    String value = redis.opsForValue().get(CacheKeys.market(eventId, marketId));
    return value == null ? Optional.empty() : Optional.of(MarketStatus.valueOf(value));
  }
}
