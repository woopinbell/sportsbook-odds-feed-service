package com.sportsbook.oddsfeed.provider;

import com.sportsbook.protocol.value.EventId;
import java.util.List;
import java.util.Optional;
import reactor.core.publisher.Flux;

/**
 * Single point of contact between this service and the outside world of odds data. Two
 * implementations exist: {@code MockOddsProvider} (default, deterministic simulator) and {@code
 * TheOddsApiProvider} (real upstream behind an HTTP API). Selected by Spring profile per ADR-0010.
 *
 * <p>Deliberate deviations from the ADR-0010 sketch:
 *
 * <ul>
 *   <li>{@code listEvents} returns {@code List} rather than {@code Stream} — callers typically
 *       iterate the snapshot more than once (log + subscribe + diff against Redis) and a {@code
 *       Stream} is single-use.
 *   <li>{@code streamEvents} (was {@code streamOddsChanges}) returns {@code Flux<ProviderEvent>}.
 *       The sealed {@link ProviderEvent} interleaves odds / market-status / lifecycle changes; a
 *       single ordered stream per event keeps occurrence order intact.
 * </ul>
 *
 * <p>Implementations are free to be cold or hot, but consumers should assume {@link
 * #streamEvents(EventId)} is hot — subscribing late may miss events.
 */
public interface OddsProvider {

  /**
   * Snapshot of events the provider currently knows about for the given sport. Includes scheduled,
   * in-play, and recently finished events; orchestrator filters by {@code status} as needed.
   */
  List<EventSummary> listEvents(Sport sport);

  /**
   * Continuous stream of changes for a specific event. Returns {@link Flux#empty()} when the
   * provider isn't tracking the event. Lifecycle, market-status, and odds updates are interleaved
   * in occurrence order.
   */
  Flux<ProviderEvent> streamEvents(EventId eventId);

  /**
   * Final outcome for a finished event, or {@link Optional#empty()} if the event is still active or
   * unknown. Polled by the orchestrator once a {@link ProviderEvent.LifecycleUpdated} carries
   * {@code FINISHED}.
   */
  Optional<MatchOutcome> getMatchResult(EventId eventId);
}
