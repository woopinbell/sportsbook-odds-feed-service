package com.sportsbook.oddsfeed.provider;

import com.sportsbook.protocol.event.EventLifecycleStatus;
import com.sportsbook.protocol.value.EventId;
import java.time.Instant;
import java.util.Objects;

/**
 * Snapshot of a single event the provider knows about. Carried by {@link
 * OddsProvider#listEvents(Sport)} so the orchestrator can discover newly scheduled fixtures and
 * seed the Redis event/market metadata.
 *
 * <p>{@code competition} is a free-form display label (e.g. "Premier League"); we don't model
 * leagues / tournaments as first-class entities in V1 since none of the downstream services
 * dispatch on it.
 */
public record EventSummary(
    EventId eventId,
    Sport sport,
    String competition,
    String homeTeam,
    String awayTeam,
    Instant scheduledStartAt,
    EventLifecycleStatus status) {

  public EventSummary {
    Objects.requireNonNull(eventId, "eventId");
    Objects.requireNonNull(sport, "sport");
    Objects.requireNonNull(competition, "competition");
    Objects.requireNonNull(homeTeam, "homeTeam");
    Objects.requireNonNull(awayTeam, "awayTeam");
    Objects.requireNonNull(scheduledStartAt, "scheduledStartAt");
    Objects.requireNonNull(status, "status");
  }
}
