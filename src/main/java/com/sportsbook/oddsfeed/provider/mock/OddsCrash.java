package com.sportsbook.oddsfeed.provider.mock;

import com.sportsbook.oddsfeed.provider.ProviderEvent;
import com.sportsbook.protocol.event.EventLifecycleStatus;
import com.sportsbook.protocol.value.Odds;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.List;
import java.util.Random;

/**
 * A single-tick crash: an arbitrary selection's odds drop to a fraction of their previous value.
 * Distinct from {@link LateGoal} in that it can fire at any phase (not just the final window) and
 * represents non-goal news (red card, injury, withdrawal). The aggressive multiplier exercises the
 * 1% threshold filter in the publisher — every crash must publish.
 */
public final class OddsCrash implements MockScenario {

  private static final double CRASH_MULTIPLIER = 0.3;

  @Override
  public String id() {
    return "OddsCrash";
  }

  @Override
  public boolean canApply(MockOddsProvider.MockEvent event, Instant now) {
    return event.status == EventLifecycleStatus.IN_PLAY
        || event.status == EventLifecycleStatus.SCHEDULED;
  }

  @Override
  public void apply(
      MockOddsProvider.MockEvent event, Instant now, Random rng, MockOddsProvider provider) {
    MockOddsProvider.MockMarket market = event.markets.values().iterator().next();
    List<MockOddsProvider.MockSelection> selections = List.copyOf(market.selections.values());
    MockOddsProvider.MockSelection target = selections.get(rng.nextInt(selections.size()));

    Odds previous = target.currentOdds;
    BigDecimal nextDecimal =
        previous
            .decimal()
            .multiply(BigDecimal.valueOf(CRASH_MULTIPLIER))
            .max(BigDecimal.valueOf(OddsSimulator.MIN_ODDS))
            .setScale(Odds.SCALE, RoundingMode.HALF_EVEN);
    Odds next = Odds.ofDecimal(nextDecimal);
    target.currentOdds = next;

    provider.emit(
        event.summary.eventId(),
        new ProviderEvent.OddsUpdated(
            event.summary.eventId(), market.marketId, target.selectionId, previous, next, now));
  }
}
