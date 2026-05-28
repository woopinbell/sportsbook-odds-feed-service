package com.sportsbook.oddsfeed.config;

import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Binding for {@code oddsfeed.real.*}. Active under the {@code real} profile, which activates the
 * {@code TheOddsApiProvider} per ADR-0010.
 *
 * <p>{@code apiKey} resolves from the {@code THE_ODDS_API_KEY} environment variable in
 * application-real.yml; Spring fails fast at context init if it's missing under the real profile.
 *
 * <p>{@code monthlyQuota} is enforced client-side via a Redis counter — The Odds API doesn't
 * surface remaining budget cheaply, and we want to stop polling well before the upstream rejects.
 */
@ConfigurationProperties(prefix = "oddsfeed.real")
public record RealProperties(
    String apiKey,
    String baseUrl,
    List<String> sportKeys,
    RateLimit rateLimit,
    int monthlyQuota,
    int pollIntervalSeconds) {

  public record RateLimit(int maxRequestsPerMinute) {}
}
