package com.sportsbook.oddsfeed.provider.mock;

import static org.assertj.core.api.Assertions.assertThat;

import com.sportsbook.protocol.value.Odds;
import java.util.Random;
import org.junit.jupiter.api.Test;

class OddsSimulatorTest {

  @Test
  void initialOddsAreFairValueForImpliedProbability() {
    Odds homeWin = OddsSimulator.initialOdds(0.5);
    assertThat(homeWin.decimal().doubleValue())
        .isCloseTo(2.0, org.assertj.core.data.Offset.offset(1e-4));
  }

  @Test
  void nextOddsStaysWithinClampedRange() {
    Random rng = new Random(42L);
    Odds current = OddsSimulator.initialOdds(0.45);
    for (int i = 0; i < 1000; i++) {
      current = OddsSimulator.nextOdds(current, 0.45, rng);
      assertThat(current.decimal().doubleValue())
          .isBetween(OddsSimulator.MIN_ODDS, OddsSimulator.MAX_ODDS);
    }
  }

  @Test
  void nextOddsRevertsTowardFairOverTime() {
    Random rng = new Random(7L);
    double implied = 0.40;
    double fair = 1.0 / implied;
    Odds current = Odds.ofDecimal("5.0000");
    for (int i = 0; i < 5000; i++) {
      current = OddsSimulator.nextOdds(current, implied, rng);
    }
    assertThat(current.decimal().doubleValue())
        .isCloseTo(fair, org.assertj.core.data.Offset.offset(0.5));
  }

  @Test
  void deterministicGivenSameSeed() {
    Random rng1 = new Random(123L);
    Random rng2 = new Random(123L);
    Odds o1 = Odds.ofDecimal("2.0000");
    Odds o2 = Odds.ofDecimal("2.0000");
    for (int i = 0; i < 50; i++) {
      o1 = OddsSimulator.nextOdds(o1, 0.5, rng1);
      o2 = OddsSimulator.nextOdds(o2, 0.5, rng2);
    }
    assertThat(o1).isEqualTo(o2);
  }
}
