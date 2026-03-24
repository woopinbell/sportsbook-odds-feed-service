package com.sportsbook.oddsfeed.provider.mock;

import com.sportsbook.oddsfeed.config.MockProperties;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Periodically injects a {@link MockScenario} into a random eligible event so the simulator stops
 * looking like an ever-smoothing random walk. Driven by {@code
 * oddsfeed.mock.scenarios.rotation-interval-seconds}; honors the {@code auto-rotate} flag so load
 * tests can disable rotation and drive scenarios deterministically from the test harness.
 */
@Component
@Profile("mock")
public class ScenarioRotator {

  private static final Logger log = LoggerFactory.getLogger(ScenarioRotator.class);

  private final MockProperties props;
  private final MockOddsProvider provider;
  private final Clock clock;
  private final List<MockScenario> scenarios;
  private final Random rng;

  public ScenarioRotator(MockProperties props, MockOddsProvider provider, Clock clock) {
    this.props = props;
    this.provider = provider;
    this.clock = clock;
    this.scenarios =
        List.of(new LateGoal(), new MatchPostponed(), new SuddenMarketSuspend(), new OddsCrash());
    this.rng = props.randomSeed() == 0 ? new Random() : new Random(props.randomSeed() + 1);
  }

  @Scheduled(
      fixedRateString = "${oddsfeed.mock.scenarios.rotation-interval-seconds:60}",
      timeUnit = java.util.concurrent.TimeUnit.SECONDS)
  void scheduledRotate() {
    if (!props.scenarios().autoRotate()) {
      return;
    }
    rotateOnce(clock.instant());
  }

  /**
   * Drive a single scenario application. Package-private so unit tests can step rotation without
   * the Spring scheduler.
   */
  void rotateOnce(Instant now) {
    MockScenario scenario = scenarios.get(rng.nextInt(scenarios.size()));
    Optional<MockOddsProvider.MockEvent> target =
        provider.activeEvents().stream().filter(e -> scenario.canApply(e, now)).findFirst();
    target.ifPresent(
        event -> {
          log.info(
              "Applying scenario {} to event {}", scenario.id(), event.summary.eventId().value());
          scenario.apply(event, now, rng, provider);
        });
  }

  /** All available scenarios in stable order; exposed for tests and debugging. */
  List<MockScenario> scenarios() {
    return scenarios;
  }
}
