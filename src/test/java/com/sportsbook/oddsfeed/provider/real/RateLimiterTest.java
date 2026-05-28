package com.sportsbook.oddsfeed.provider.real;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class RateLimiterTest {

  @Test
  void allowsUpToCapWithinWindow() {
    RateLimiter limiter =
        new RateLimiter(3, Clock.fixed(Instant.parse("2026-05-28T10:00:00Z"), ZoneOffset.UTC));
    assertThat(limiter.tryAcquire()).isTrue();
    assertThat(limiter.tryAcquire()).isTrue();
    assertThat(limiter.tryAcquire()).isTrue();
    assertThat(limiter.tryAcquire()).isFalse();
    assertThat(limiter.currentUsage()).isEqualTo(3);
  }

  @Test
  void evictsTimestampsOlderThanWindow() {
    AtomicReference<Instant> nowRef = new AtomicReference<>(Instant.parse("2026-05-28T10:00:00Z"));
    Clock movingClock =
        new Clock() {
          @Override
          public Instant instant() {
            return nowRef.get();
          }

          @Override
          public ZoneOffset getZone() {
            return ZoneOffset.UTC;
          }

          @Override
          public Clock withZone(java.time.ZoneId zone) {
            return this;
          }
        };
    RateLimiter limiter = new RateLimiter(2, movingClock);
    assertThat(limiter.tryAcquire()).isTrue();
    assertThat(limiter.tryAcquire()).isTrue();
    assertThat(limiter.tryAcquire()).isFalse();

    nowRef.set(nowRef.get().plusSeconds(61));
    assertThat(limiter.tryAcquire()).isTrue();
    assertThat(limiter.currentUsage()).isEqualTo(1);
  }
}
