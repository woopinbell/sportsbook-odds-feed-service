package com.sportsbook.oddsfeed.provider.mock;

import com.sportsbook.protocol.value.Odds;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Random;

/**
 * Pure random-walk pricing for the mock provider. Extracted from {@link MockOddsProvider} so it can
 * be unit-tested without Spring or thread scheduling.
 *
 * <p>Model: each selection has a target implied probability (e.g. home-win 0.45). Fair odds are
 * {@code 1 / p}. At each step the current odds drift around fair with a small Gaussian noise, then
 * a fraction of the move is pulled back toward fair so the walk doesn't run away. Output is clamped
 * to a sensible band so the simulator can't accidentally produce {@code Odds < 1.0} (which the
 * {@link Odds} value object rejects).
 */
public final class OddsSimulator {

  static final double NOISE_STDDEV = 0.02;
  static final double MEAN_REVERSION = 0.10;
  static final double MIN_ODDS = 1.01;
  static final double MAX_ODDS = 100.0;

  private OddsSimulator() {}

  /** Initial odds for a fresh selection: exactly fair value (no margin baked in for V1). */
  public static Odds initialOdds(double impliedProbability) {
    double fair = 1.0 / impliedProbability;
    return Odds.ofDecimal(BigDecimal.valueOf(fair).setScale(Odds.SCALE, RoundingMode.HALF_EVEN));
  }

  /**
   * One step of the random walk. The {@code current} value is perturbed by multiplicative Gaussian
   * noise, then mean-reverted a fraction of the way toward fair odds.
   */
  public static Odds nextOdds(Odds current, double impliedProbability, Random rng) {
    double fair = 1.0 / impliedProbability;
    double cur = current.decimal().doubleValue();
    double noisy = cur * (1.0 + rng.nextGaussian() * NOISE_STDDEV);
    double next = noisy * (1.0 - MEAN_REVERSION) + fair * MEAN_REVERSION;
    double clamped = Math.max(MIN_ODDS, Math.min(MAX_ODDS, next));
    return Odds.ofDecimal(BigDecimal.valueOf(clamped).setScale(Odds.SCALE, RoundingMode.HALF_EVEN));
  }
}
