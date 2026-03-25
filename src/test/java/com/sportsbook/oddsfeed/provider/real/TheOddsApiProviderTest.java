package com.sportsbook.oddsfeed.provider.real;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.assertj.core.api.Assertions.assertThat;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.sportsbook.oddsfeed.config.RealProperties;
import com.sportsbook.oddsfeed.provider.EventSummary;
import com.sportsbook.oddsfeed.provider.ProviderEvent;
import com.sportsbook.oddsfeed.provider.Sport;
import com.sportsbook.protocol.event.EventLifecycleStatus;
import com.sportsbook.protocol.value.EventId;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.Disposable;

class TheOddsApiProviderTest {

  private static final String SAMPLE_RESPONSE =
      """
      [
        {
          "id": "abc123",
          "sport_key": "soccer_epl",
          "sport_title": "EPL",
          "commence_time": "2026-06-01T18:00:00Z",
          "home_team": "Manchester United",
          "away_team": "Chelsea",
          "bookmakers": [
            {
              "key": "draftkings",
              "title": "DraftKings",
              "last_update": "2026-05-28T10:00:00Z",
              "markets": [
                {
                  "key": "h2h",
                  "last_update": "2026-05-28T10:00:00Z",
                  "outcomes": [
                    {"name": "Manchester United", "price": 1.85},
                    {"name": "Draw", "price": 3.60},
                    {"name": "Chelsea", "price": 4.20}
                  ]
                }
              ]
            }
          ]
        }
      ]
      """;

  private static final String UPDATED_RESPONSE =
      """
      [
        {
          "id": "abc123",
          "sport_key": "soccer_epl",
          "sport_title": "EPL",
          "commence_time": "2026-06-01T18:00:00Z",
          "home_team": "Manchester United",
          "away_team": "Chelsea",
          "bookmakers": [
            {
              "key": "draftkings",
              "title": "DraftKings",
              "last_update": "2026-05-28T10:05:00Z",
              "markets": [
                {
                  "key": "h2h",
                  "last_update": "2026-05-28T10:05:00Z",
                  "outcomes": [
                    {"name": "Manchester United", "price": 1.90},
                    {"name": "Draw", "price": 3.60},
                    {"name": "Chelsea", "price": 4.00}
                  ]
                }
              ]
            }
          ]
        }
      ]
      """;

  private WireMockServer wireMock;
  private TheOddsApiProvider provider;
  private RecordingQuotaCounter quotaCounter;

  @BeforeEach
  void setUp() {
    wireMock = new WireMockServer(wireMockConfig().dynamicPort());
    wireMock.start();
    RealProperties props =
        new RealProperties(
            "test-api-key",
            "http://localhost:" + wireMock.port(),
            List.of("soccer_epl"),
            new RealProperties.RateLimit(10),
            500,
            60);
    WebClient client = WebClient.builder().baseUrl(props.baseUrl()).build();
    Clock clock = Clock.fixed(Instant.parse("2026-05-28T10:00:00Z"), ZoneOffset.UTC);
    quotaCounter = new RecordingQuotaCounter();
    provider = new TheOddsApiProvider(client, props, new RateLimiter(10, clock), quotaCounter);
  }

  @AfterEach
  void tearDown() {
    wireMock.stop();
  }

  @Test
  void listEventsParsesEventSummary() {
    stubOdds(SAMPLE_RESPONSE);

    List<EventSummary> events = provider.listEvents(Sport.FOOTBALL);

    assertThat(events).hasSize(1);
    EventSummary first = events.get(0);
    assertThat(first.competition()).isEqualTo("EPL");
    assertThat(first.homeTeam()).isEqualTo("Manchester United");
    assertThat(first.awayTeam()).isEqualTo("Chelsea");
    assertThat(first.scheduledStartAt()).isEqualTo(Instant.parse("2026-06-01T18:00:00Z"));
    assertThat(first.status()).isEqualTo(EventLifecycleStatus.SCHEDULED);
  }

  @Test
  void listEventsIncrementsQuotaCounter() {
    stubOdds(SAMPLE_RESPONSE);
    provider.listEvents(Sport.FOOTBALL);
    assertThat(quotaCounter.value).isEqualTo(1L);
  }

  @Test
  void pollSportEmitsOddsUpdatedForChangedPricesAndNotForUnchanged() {
    stubOdds(SAMPLE_RESPONSE);
    provider.pollSport(Sport.FOOTBALL);

    EventId eventId = TheOddsApiProvider.deriveEventId("abc123");
    List<ProviderEvent.OddsUpdated> received = new ArrayList<>();
    Disposable subscription =
        provider
            .streamEvents(eventId)
            .ofType(ProviderEvent.OddsUpdated.class)
            .subscribe(received::add);

    stubOdds(UPDATED_RESPONSE);
    provider.pollSport(Sport.FOOTBALL);
    subscription.dispose();

    assertThat(received)
        .as("Manchester United and Chelsea prices moved; Draw stayed at 3.60")
        .hasSize(2);
    assertThat(received)
        .extracting(o -> o.newOdds().decimal().toPlainString())
        .containsExactlyInAnyOrder("1.9000", "4.0000");
  }

  @Test
  void pollSportSkipsWhenRateLimited() {
    stubOdds(SAMPLE_RESPONSE);
    RateLimiter exhausted =
        new RateLimiter(0, Clock.fixed(Instant.parse("2026-05-28T10:00:00Z"), ZoneOffset.UTC));
    TheOddsApiProvider limited =
        new TheOddsApiProvider(
            WebClient.builder().baseUrl("http://localhost:" + wireMock.port()).build(),
            new RealProperties(
                "k",
                "http://localhost:" + wireMock.port(),
                List.of("soccer_epl"),
                new RealProperties.RateLimit(0),
                500,
                60),
            exhausted,
            quotaCounter);
    limited.pollSport(Sport.FOOTBALL);
    assertThat(quotaCounter.value).isZero();
  }

  private void stubOdds(String body) {
    wireMock.resetMappings();
    wireMock.stubFor(
        get(urlPathEqualTo("/sports/soccer_epl/odds"))
            .willReturn(
                aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "application/json")
                    .withBody(body)));
  }

  private static final class RecordingQuotaCounter implements QuotaCounter {
    private final AtomicLong counter = new AtomicLong();
    long value = 0L;

    @Override
    public long increment() {
      value = counter.incrementAndGet();
      return value;
    }

    @Override
    public long current() {
      return counter.get();
    }
  }
}
