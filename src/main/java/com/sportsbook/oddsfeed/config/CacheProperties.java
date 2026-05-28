package com.sportsbook.oddsfeed.config;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Binding for {@code oddsfeed.cache.*}. Currently a single TTL applied uniformly to odds, event,
 * and market keys (24h per the repo CLAUDE.md). Split into per-key TTLs when the operational cost
 * of cache misses on long-lived metadata starts to matter.
 */
@ConfigurationProperties(prefix = "oddsfeed.cache")
public record CacheProperties(Duration ttl) {}
