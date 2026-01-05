package com.sportsbook.oddsfeed.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Binding for {@code oddsfeed.kafka.topics.*}. Every topic the publisher writes to is bound here
 * rather than scattered as constants — the docker-compose and k8s manifests reuse the same
 * application.yml so topic renames stay a single-file change.
 */
@ConfigurationProperties(prefix = "oddsfeed.kafka.topics")
public record KafkaTopicsProperties(
    String oddsChanged, String marketStatusChanged, String eventLifecycle, String matchResult) {}
