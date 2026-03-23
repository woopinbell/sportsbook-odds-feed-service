package com.sportsbook.oddsfeed.provider;

import com.sportsbook.protocol.event.EventLifecycleStatus;
import com.sportsbook.protocol.event.MarketStatus;
import com.sportsbook.protocol.value.EventId;
import com.sportsbook.protocol.value.MarketId;
import com.sportsbook.protocol.value.Odds;
import com.sportsbook.protocol.value.SelectionId;
import java.time.Instant;
import java.util.Objects;

/**
 * Sealed view of every change the provider can emit for a single event. Modeled as one stream
 * rather than three because real providers (and our mock) interleave these notifications in
 * occurrence order — splitting into three Flux endpoints would force the consumer to reassemble
 * ordering.
 *
 * <p>The Avro wire schemas in {@code shared-protocol} are intentionally split per topic ({@code
 * OddsChanged}, {@code MarketStatusChanged}, {@code EventLifecycle}); the publisher pattern-matches
 * on this sealed type to dispatch to the correct topic.
 */
public sealed interface ProviderEvent
    permits ProviderEvent.OddsUpdated,
        ProviderEvent.MarketStatusUpdated,
        ProviderEvent.LifecycleUpdated {

  EventId eventId();

  Instant occurredAt();

  /** A selection's price moved. Subject to the 1% noise-reduction threshold at publish time. */
  record OddsUpdated(
      EventId eventId,
      MarketId marketId,
      SelectionId selectionId,
      Odds previousOdds,
      Odds newOdds,
      Instant occurredAt)
      implements ProviderEvent {
    public OddsUpdated {
      Objects.requireNonNull(eventId, "eventId");
      Objects.requireNonNull(marketId, "marketId");
      Objects.requireNonNull(selectionId, "selectionId");
      Objects.requireNonNull(previousOdds, "previousOdds");
      Objects.requireNonNull(newOdds, "newOdds");
      Objects.requireNonNull(occurredAt, "occurredAt");
    }
  }

  /** A market opened, suspended, or closed. {@code reason} is optional and may be null. */
  record MarketStatusUpdated(
      EventId eventId,
      MarketId marketId,
      MarketStatus previousStatus,
      MarketStatus newStatus,
      String reason,
      Instant occurredAt)
      implements ProviderEvent {
    public MarketStatusUpdated {
      Objects.requireNonNull(eventId, "eventId");
      Objects.requireNonNull(marketId, "marketId");
      Objects.requireNonNull(previousStatus, "previousStatus");
      Objects.requireNonNull(newStatus, "newStatus");
      Objects.requireNonNull(occurredAt, "occurredAt");
    }
  }

  /** Lifecycle phase transition for the event itself (kickoff, end, cancellation). */
  record LifecycleUpdated(
      EventId eventId, EventLifecycleStatus status, Instant scheduledStartAt, Instant occurredAt)
      implements ProviderEvent {
    public LifecycleUpdated {
      Objects.requireNonNull(eventId, "eventId");
      Objects.requireNonNull(status, "status");
      Objects.requireNonNull(scheduledStartAt, "scheduledStartAt");
      Objects.requireNonNull(occurredAt, "occurredAt");
    }
  }
}
