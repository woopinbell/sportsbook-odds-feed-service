package com.sportsbook.oddsfeed.provider.real;

import java.time.Clock;
import java.time.Duration;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import org.springframework.context.annotation.Profile;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

/**
 * Redis-backed {@link QuotaCounter}: one key per calendar month ({@code oddsfeed:thequota:YYYY-MM})
 * with a 35-day TTL so old months expire automatically. INCR is atomic and matches the user-prompt
 * requirement for the monthly self-counter.
 */
@Component
@Profile("real")
public class RedisQuotaCounter implements QuotaCounter {

  private static final String KEY_PREFIX = "oddsfeed:thequota:";
  private static final DateTimeFormatter MONTH_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM");
  private static final Duration TTL = Duration.ofDays(35);

  private final StringRedisTemplate redis;
  private final Clock clock;

  public RedisQuotaCounter(StringRedisTemplate redis, Clock clock) {
    this.redis = redis;
    this.clock = clock;
  }

  @Override
  public long increment() {
    String key = currentKey();
    Long incremented = redis.opsForValue().increment(key);
    redis.expire(key, TTL);
    return incremented == null ? 0L : incremented;
  }

  @Override
  public long current() {
    String value = redis.opsForValue().get(currentKey());
    return value == null ? 0L : Long.parseLong(value);
  }

  private String currentKey() {
    return KEY_PREFIX + MONTH_FORMAT.format(clock.instant().atZone(ZoneOffset.UTC));
  }
}
