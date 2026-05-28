package com.sportsbook.oddsfeed.provider.real;

import com.fasterxml.jackson.databind.PropertyNamingStrategies.SnakeCaseStrategy;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

/**
 * Wire model for The Odds API v4 {@code /sports/{key}/odds} response. Kept in one file because none
 * of these types escapes the {@code real} package — the provider maps them to {@link
 * com.sportsbook.oddsfeed.provider.EventSummary} and {@link
 * com.sportsbook.oddsfeed.provider.ProviderEvent.OddsUpdated} before returning to callers.
 *
 * <p>The class-level {@code @JsonNaming} lets Java fields stay camelCase while Jackson reads the
 * snake_case keys the upstream actually sends ({@code sport_key}, {@code commence_time}).
 */
public final class TheOddsApiDtos {

  private TheOddsApiDtos() {}

  @JsonNaming(SnakeCaseStrategy.class)
  public record Event(
      String id,
      String sportKey,
      String sportTitle,
      Instant commenceTime,
      String homeTeam,
      String awayTeam,
      List<Bookmaker> bookmakers) {}

  @JsonNaming(SnakeCaseStrategy.class)
  public record Bookmaker(String key, String title, Instant lastUpdate, List<Market> markets) {}

  @JsonNaming(SnakeCaseStrategy.class)
  public record Market(String key, Instant lastUpdate, List<Outcome> outcomes) {}

  @JsonNaming(SnakeCaseStrategy.class)
  public record Outcome(String name, BigDecimal price) {}
}
