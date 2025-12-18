package com.sportsbook.oddsfeed.provider.mock;

import java.time.Instant;
import java.util.Random;

/**
 * Disruption applied periodically to an in-flight mock event so the system exercises edge cases the
 * baseline random walk never produces (a sudden suspended market, a late goal, a postponed fixture,
 * an odds crash). Selected by {@link ScenarioRotator} on a configurable cadence.
 *
 * <p>Scenarios deliberately mutate {@link MockOddsProvider}'s internal state directly. The
 * indirection of a "scenario context" interface was considered and rejected — the mock is test-only
 * code, the rotator and scenarios live in the same package, and adding an abstraction here would
 * obscure rather than clarify the simulation.
 */
public sealed interface MockScenario
    permits LateGoal, MatchPostponed, SuddenMarketSuspend, OddsCrash {

  /** Stable identifier used in logs, metrics, and load-test driver scripts. */
  String id();

  /**
   * Whether this scenario can meaningfully apply to {@code event} at {@code now}. The rotator skips
   * scenarios that return false; it does not consider them blocking.
   */
  boolean canApply(MockOddsProvider.MockEvent event, Instant now);

  /** Apply the disruption: mutate event state and emit any resulting {@code ProviderEvent}s. */
  void apply(MockOddsProvider.MockEvent event, Instant now, Random rng, MockOddsProvider provider);
}
