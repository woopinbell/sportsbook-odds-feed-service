package com.sportsbook.oddsfeed.provider.mock;

import com.sportsbook.oddsfeed.provider.ProviderEvent;
import com.sportsbook.protocol.event.EventLifecycleStatus;
import com.sportsbook.protocol.event.MarketStatus;
import java.time.Instant;
import java.util.Random;

/**
 * Briefly suspend an OPEN market (simulating a VAR check or other in-play pause). The auto-reopen
 * is modeled as a separate scenario tick; for V1 this emits the suspend, mutates state, and relies
 * on the rotator (or the next scenario invocation) to flip it back.
 */
public final class SuddenMarketSuspend implements MockScenario {

  private static final String REASON = "simulated in-play pause (VAR check)";

  @Override
  public String id() {
    return "SuddenMarketSuspend";
  }

  @Override
  public boolean canApply(MockOddsProvider.MockEvent event, Instant now) {
    if (event.status != EventLifecycleStatus.IN_PLAY
        && event.status != EventLifecycleStatus.SCHEDULED) {
      return false;
    }
    return event.markets.values().stream().anyMatch(m -> m.status == MarketStatus.OPEN);
  }

  @Override
  public void apply(
      MockOddsProvider.MockEvent event, Instant now, Random rng, MockOddsProvider provider) {
    MockOddsProvider.MockMarket target =
        event.markets.values().stream()
            .filter(m -> m.status == MarketStatus.OPEN)
            .findFirst()
            .orElseThrow();
    MarketStatus previous = target.status;
    target.status = MarketStatus.SUSPENDED;
    provider.emit(
        event.summary.eventId(),
        new ProviderEvent.MarketStatusUpdated(
            event.summary.eventId(),
            target.marketId,
            previous,
            MarketStatus.SUSPENDED,
            REASON,
            now));
  }
}
