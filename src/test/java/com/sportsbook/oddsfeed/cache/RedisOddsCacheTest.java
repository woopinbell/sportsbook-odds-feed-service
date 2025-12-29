package com.sportsbook.oddsfeed.cache;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.sportsbook.oddsfeed.config.CacheProperties;
import com.sportsbook.oddsfeed.provider.EventSummary;
import com.sportsbook.oddsfeed.provider.Sport;
import com.sportsbook.protocol.event.EventLifecycleStatus;
import com.sportsbook.protocol.event.MarketStatus;
import com.sportsbook.protocol.value.EventId;
import com.sportsbook.protocol.value.MarketId;
import com.sportsbook.protocol.value.Odds;
import com.sportsbook.protocol.value.SelectionId;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

class RedisOddsCacheTest {

  private static final int REDIS_PORT = 6379;

  @SuppressWarnings("resource")
  private static final GenericContainer<?> REDIS =
      new GenericContainer<>(DockerImageName.parse("redis:7-alpine")).withExposedPorts(REDIS_PORT);

  private static LettuceConnectionFactory connectionFactory;
  private static StringRedisTemplate redisTemplate;
  private static RedisOddsCache cache;
  private static ObjectMapper objectMapper;

  @BeforeAll
  static void startContainer() {
    REDIS.start();
    connectionFactory =
        new LettuceConnectionFactory(REDIS.getHost(), REDIS.getMappedPort(REDIS_PORT));
    connectionFactory.afterPropertiesSet();
    redisTemplate = new StringRedisTemplate(connectionFactory);
    objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
    cache =
        new RedisOddsCache(redisTemplate, objectMapper, new CacheProperties(Duration.ofHours(24)));
  }

  @AfterAll
  static void stopContainer() {
    connectionFactory.destroy();
    REDIS.stop();
  }

  @BeforeEach
  void flush() {
    redisTemplate.getConnectionFactory().getConnection().serverCommands().flushDb();
  }

  @Test
  void roundTripsOdds() {
    EventId eventId = new EventId(UUID.randomUUID());
    MarketId marketId = new MarketId(UUID.randomUUID());
    SelectionId selectionId = new SelectionId(UUID.randomUUID());
    Odds price = Odds.ofDecimal("1.85");

    cache.storeOdds(eventId, marketId, selectionId, price);

    assertThat(cache.getOdds(eventId, marketId, selectionId)).contains(price);
    assertThat(redisTemplate.opsForValue().get(CacheKeys.odds(eventId, marketId, selectionId)))
        .isEqualTo("1.8500");
  }

  @Test
  void returnsEmptyForMissingKey() {
    EventId eventId = new EventId(UUID.randomUUID());
    MarketId marketId = new MarketId(UUID.randomUUID());
    SelectionId selectionId = new SelectionId(UUID.randomUUID());
    assertThat(cache.getOdds(eventId, marketId, selectionId)).isEmpty();
    assertThat(cache.getEvent(eventId)).isEmpty();
    assertThat(cache.getMarketStatus(eventId, marketId)).isEmpty();
  }

  @Test
  void roundTripsEventSummary() {
    EventId eventId = new EventId(UUID.randomUUID());
    EventSummary summary =
        new EventSummary(
            eventId,
            Sport.FOOTBALL,
            "Premier League",
            "Manchester United",
            "Chelsea",
            Instant.parse("2026-06-01T18:00:00Z"),
            EventLifecycleStatus.SCHEDULED);

    cache.storeEvent(summary);

    assertThat(cache.getEvent(eventId)).contains(summary);
  }

  @Test
  void roundTripsMarketStatus() {
    EventId eventId = new EventId(UUID.randomUUID());
    MarketId marketId = new MarketId(UUID.randomUUID());
    cache.storeMarketStatus(eventId, marketId, MarketStatus.SUSPENDED);
    assertThat(cache.getMarketStatus(eventId, marketId)).contains(MarketStatus.SUSPENDED);
  }

  @Test
  void cacheKeysMatchDocumentedFormat() {
    EventId eventId = new EventId(UUID.fromString("00000000-0000-0000-0000-000000000001"));
    MarketId marketId = new MarketId(UUID.fromString("00000000-0000-0000-0000-000000000002"));
    SelectionId selectionId =
        new SelectionId(UUID.fromString("00000000-0000-0000-0000-000000000003"));
    assertThat(CacheKeys.odds(eventId, marketId, selectionId))
        .isEqualTo(
            "odds:00000000-0000-0000-0000-000000000001"
                + ":00000000-0000-0000-0000-000000000002"
                + ":00000000-0000-0000-0000-000000000003");
    assertThat(CacheKeys.event(eventId)).isEqualTo("event:00000000-0000-0000-0000-000000000001");
    assertThat(CacheKeys.market(eventId, marketId))
        .isEqualTo(
            "market:00000000-0000-0000-0000-000000000001"
                + ":00000000-0000-0000-0000-000000000002");
  }
}
