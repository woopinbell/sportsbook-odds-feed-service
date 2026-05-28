package com.sportsbook.oddsfeed.config;

import java.time.Clock;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Cross-cutting wiring: an injectable {@link Clock} so simulator and provider code can be unit-
 * tested against fixed instants, and {@link EnableScheduling} so the mock provider's tick can run
 * on the Spring scheduler.
 */
@Configuration
@EnableScheduling
@EnableConfigurationProperties({MockProperties.class, RealProperties.class})
public class ApplicationConfig {

  @Bean
  public Clock systemClock() {
    return Clock.systemUTC();
  }
}
