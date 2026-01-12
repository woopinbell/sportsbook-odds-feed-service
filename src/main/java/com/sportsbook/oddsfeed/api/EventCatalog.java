package com.sportsbook.oddsfeed.api;

import com.sportsbook.oddsfeed.provider.EventSummary;
import com.sportsbook.protocol.value.EventId;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.stereotype.Component;

/**
 * In-memory index of currently known events. The orchestrator writes to it on every discovery cycle
 * and on every lifecycle transition; the REST read controller reads from it instead of scanning
 * Redis (a SCAN over {@code event:*} would be O(N) and would block other Redis commands).
 *
 * <p>Strictly local to this service — other services read the same data from Redis via the
 * canonical {@code event:{eventId}} key.
 */
@Component
public class EventCatalog {

  private final Map<EventId, EventSummary> events = new ConcurrentHashMap<>();

  public void put(EventSummary summary) {
    events.put(summary.eventId(), summary);
  }

  public Optional<EventSummary> get(EventId eventId) {
    return Optional.ofNullable(events.get(eventId));
  }

  /** Snapshot ordered by kickoff ascending, with eventId as a tie-breaker for cursor stability. */
  public List<EventSummary> orderedByKickoff() {
    return events.values().stream()
        .sorted(
            Comparator.comparing(EventSummary::scheduledStartAt)
                .thenComparing(e -> e.eventId().value()))
        .toList();
  }

  public int size() {
    return events.size();
  }
}
