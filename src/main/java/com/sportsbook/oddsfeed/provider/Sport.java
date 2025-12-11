package com.sportsbook.oddsfeed.provider;

/**
 * V1 sport categories carried by the provider abstraction. Kept local to odds-feed-service because
 * only this service surfaces it; events on the wire identify themselves by {@code eventId} rather
 * than sport, so other services don't depend on this enum. Migrates to shared-protocol the moment a
 * second service needs to discriminate by sport.
 */
public enum Sport {
  FOOTBALL,
  BASKETBALL
}
