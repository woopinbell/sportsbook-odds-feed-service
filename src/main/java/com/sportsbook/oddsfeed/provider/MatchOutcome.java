package com.sportsbook.oddsfeed.provider;

import com.sportsbook.protocol.event.MatchFinalStatus;
import com.sportsbook.protocol.value.EventId;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;

/**
 * Final result of a completed event, returned by {@link OddsProvider#getMatchResult(EventId)}.
 *
 * <p>Named {@code MatchOutcome} (not {@code MatchResult}) to avoid the collision with the Avro
 * generated {@code com.sportsbook.protocol.event.MatchResult} wire type. The publisher converts
 * between the two at the Kafka boundary.
 *
 * <p>{@code detail} is intentionally schema-less so a new sport (yellow cards, possession, tennis
 * set scores) doesn't trigger an Avro evolution.
 */
public record MatchOutcome(
    EventId eventId,
    String score,
    MatchFinalStatus finalStatus,
    Map<String, String> detail,
    Instant settledAt) {

  public MatchOutcome {
    Objects.requireNonNull(eventId, "eventId");
    Objects.requireNonNull(score, "score");
    Objects.requireNonNull(finalStatus, "finalStatus");
    Objects.requireNonNull(detail, "detail");
    Objects.requireNonNull(settledAt, "settledAt");
    detail = Map.copyOf(detail);
  }
}
