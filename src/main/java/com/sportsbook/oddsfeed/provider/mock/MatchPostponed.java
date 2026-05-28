package com.sportsbook.oddsfeed.provider.mock;

import com.sportsbook.protocol.event.EventLifecycleStatus;
import java.time.Instant;
import java.util.Random;

/**
 * The fixture is postponed before kickoff. Drives downstream settlement-service to void all
 * existing slips touching this event (ADR-0012: postponed/cancelled events refund bets in full).
 */
public final class MatchPostponed implements MockScenario {

  @Override
  public String id() {
    return "MatchPostponed";
  }

  @Override
  public boolean canApply(MockOddsProvider.MockEvent event, Instant now) {
    return event.status == EventLifecycleStatus.SCHEDULED;
  }

  @Override
  public void apply(
      MockOddsProvider.MockEvent event, Instant now, Random rng, MockOddsProvider provider) {
    provider.transitionTo(event, EventLifecycleStatus.POSTPONED, now);
  }
}
