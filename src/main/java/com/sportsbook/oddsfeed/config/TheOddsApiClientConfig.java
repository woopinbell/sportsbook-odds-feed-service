package com.sportsbook.oddsfeed.config;

import com.sportsbook.oddsfeed.provider.real.RateLimiter;
import java.time.Clock;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.web.reactive.function.client.WebClient;

/**
 * Wires the {@code real}-profile beans that need explicit configuration: the {@link WebClient}
 * pointed at The Odds API base URL and the {@link RateLimiter} sized from properties. The
 * Redis-backed quota counter discovers itself via component scan.
 */
@Configuration
@Profile("real")
public class TheOddsApiClientConfig {

  @Bean
  public WebClient theOddsWebClient(RealProperties props) {
    return WebClient.builder().baseUrl(props.baseUrl()).build();
  }

  @Bean
  public RateLimiter theOddsRateLimiter(RealProperties props, Clock clock) {
    return new RateLimiter(props.rateLimit().maxRequestsPerMinute(), clock);
  }
}
