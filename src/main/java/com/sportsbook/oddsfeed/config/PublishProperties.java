package com.sportsbook.oddsfeed.config;

import java.math.BigDecimal;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Binding for {@code oddsfeed.publish.*}. {@code oddsChangeThreshold} is the minimum absolute
 * relative change (|new - prev| / prev) required for a price movement to escape onto Kafka. The
 * project contract fixes the default at 0.01 (1%); load tests use the same default unless a
 * scenario specifically wants every tick.
 */
@ConfigurationProperties(prefix = "oddsfeed.publish")
public record PublishProperties(BigDecimal oddsChangeThreshold) {}
