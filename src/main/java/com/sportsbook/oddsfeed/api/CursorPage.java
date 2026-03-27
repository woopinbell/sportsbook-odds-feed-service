package com.sportsbook.oddsfeed.api;

import java.util.List;

/**
 * Cursor-paginated response wrapper for time-series listings (ADR-0004). {@code nextCursor} is
 * {@code null} when the current page exhausted the stream.
 */
public record CursorPage<T>(List<T> items, String nextCursor) {}
