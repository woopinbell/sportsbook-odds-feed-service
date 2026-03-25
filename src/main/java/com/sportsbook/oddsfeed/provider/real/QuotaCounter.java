package com.sportsbook.oddsfeed.provider.real;

/**
 * Tracks consumed-month requests against The Odds API monthly budget. Abstracted so tests can pass
 * an in-memory counter without a Redis instance.
 */
public interface QuotaCounter {

  /** Atomically increment the current month's counter and return the new value. */
  long increment();

  /** Read the current month's counter without mutating it. */
  long current();
}
