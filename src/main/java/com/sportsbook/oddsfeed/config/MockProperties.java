package com.sportsbook.oddsfeed.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Binding for {@code oddsfeed.mock.*}. Active under the {@code mock} profile only; the real profile
 * uses a separate properties class added with the Real adapter commit.
 *
 * <p>{@code minutesPerSecond} is the simulation compression factor: one wall second represents N
 * mock minutes, so a 90-minute fixture completes in {@code 90 / N} wall seconds. Default 1 keeps
 * the wall ≈ mock-minute mapping suitable for manual testing; load tests push this to 60+ to
 * recycle the event roster every few seconds.
 */
@ConfigurationProperties(prefix = "oddsfeed.mock")
public record MockProperties(
    double minutesPerSecond,
    Scenarios scenarios,
    double baseHomeWinProbability,
    double baseDrawProbability,
    double baseAwayWinProbability,
    long randomSeed,
    int tickIntervalMs) {

  public record Scenarios(boolean autoRotate, int rotationIntervalSeconds) {}
}
