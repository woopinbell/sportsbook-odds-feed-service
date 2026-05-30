package com.sportsbook.oddsfeed.provider.mock;

import com.sportsbook.oddsfeed.config.MockProperties;
import com.sportsbook.oddsfeed.provider.EventSummary;
import com.sportsbook.oddsfeed.provider.MatchOutcome;
import com.sportsbook.oddsfeed.provider.OddsProvider;
import com.sportsbook.oddsfeed.provider.ProviderEvent;
import com.sportsbook.oddsfeed.provider.Sport;
import com.sportsbook.protocol.domain.MarketType;
import com.sportsbook.protocol.domain.SettlementResult;
import com.sportsbook.protocol.event.EventLifecycleStatus;
import com.sportsbook.protocol.event.MarketStatus;
import com.sportsbook.protocol.event.MatchFinalStatus;
import com.sportsbook.protocol.value.EventId;
import com.sportsbook.protocol.value.MarketId;
import com.sportsbook.protocol.value.Odds;
import com.sportsbook.protocol.value.SelectionId;
import jakarta.annotation.PostConstruct;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

/**
 * Default {@link OddsProvider}: a deterministic, in-memory simulator. Drives the full event
 * lifecycle (scheduled → in-play → finished), emits odds changes via a random walk centred on
 * configured implied probabilities, and synthesizes a final score at finish. See ADR-0010.
 *
 * <p>Internal state is kept in three concurrent maps keyed by {@link EventId}: the per-event
 * mutable {@link MockEvent}, a multicast {@link Sinks.Many} the orchestrator subscribes to, and the
 * synthesized {@link MatchOutcome} populated once the event reaches {@code FINISHED}. Mutations
 * happen on the scheduler thread only; reads are safe via {@link ConcurrentHashMap}.
 *
 * <p>V1 scope kept deliberately narrow: each event carries a single MATCH_RESULT_1X2 market with
 * three selections (HOME / DRAW / AWAY). Additional market types (TOTAL_OVER_UNDER, BTTS,
 * DOUBLE_CHANCE per ADR-0013) follow once the orchestrator and publisher are in place.
 */
@Component
@Profile("mock")
public class MockOddsProvider implements OddsProvider {

  static final int INITIAL_EVENT_COUNT = 3;
  static final Duration MATCH_DURATION_MOCK = Duration.ofMinutes(90);
  static final Duration KICKOFF_SPACING_MOCK = Duration.ofMinutes(1);
  private static final double SECONDS_PER_MINUTE = 60.0;

  private static final Logger log = LoggerFactory.getLogger(MockOddsProvider.class);

  private static final String[] FOOTBALL_TEAMS = {
    "Manchester United",
    "Chelsea",
    "Liverpool",
    "Arsenal",
    "Tottenham",
    "Manchester City",
    "Newcastle",
    "Brighton"
  };

  private final MockProperties props;
  private final Clock clock;
  private final Map<EventId, MockEvent> events = new ConcurrentHashMap<>();
  private final Map<EventId, Sinks.Many<ProviderEvent>> streams = new ConcurrentHashMap<>();
  private final Map<EventId, MatchOutcome> outcomes = new ConcurrentHashMap<>();
  private Random rng;

  public MockOddsProvider(MockProperties props, Clock clock) {
    this.props = props;
    this.clock = clock;
  }

  @PostConstruct
  void seed() {
    rng = props.randomSeed() == 0 ? new Random() : new Random(props.randomSeed());
    Instant now = clock.instant();
    for (int i = 0; i < INITIAL_EVENT_COUNT; i++) {
      Duration mockOffset = KICKOFF_SPACING_MOCK.multipliedBy(i);
      Instant kickoff = now.plus(toRealDuration(mockOffset));
      Instant end = kickoff.plus(toRealDuration(MATCH_DURATION_MOCK));
      MockEvent event = buildEvent(kickoff, end, i, now);
      events.put(event.summary.eventId(), event);
      streams.put(event.summary.eventId(), Sinks.many().multicast().onBackpressureBuffer());
    }
    log.info(
        "Mock provider seeded with {} events, minutesPerSecond={}",
        events.size(),
        props.minutesPerSecond());
  }

  /**
   * Scheduled tick driven by Spring. Read the tick interval from properties so load tests can
   * tighten it; the simulator is otherwise idempotent w.r.t. tick frequency.
   */
  @Scheduled(fixedRateString = "${oddsfeed.mock.tick-interval-ms:500}")
  void scheduledTick() {
    tick(clock.instant());
  }

  /**
   * Single simulator step. Package-private so unit tests can drive the simulator directly without
   * the Spring scheduler.
   */
  void tick(Instant now) {
    for (MockEvent event : events.values()) {
      advance(event, now);
    }
  }

  private void advance(MockEvent event, Instant now) {
    if (event.status == EventLifecycleStatus.FINISHED
        || event.status == EventLifecycleStatus.CANCELLED
        || event.status == EventLifecycleStatus.POSTPONED) {
      return;
    }

    if (event.status == EventLifecycleStatus.SCHEDULED && !now.isBefore(event.kickoffAt)) {
      transitionTo(event, EventLifecycleStatus.IN_PLAY, now);
    }
    if (event.status == EventLifecycleStatus.IN_PLAY && !now.isBefore(event.endAt)) {
      // Outcome MUST be stored before transitionTo: transitionTo emits LifecycleUpdated(FINISHED),
      // which the orchestrator handles synchronously and immediately calls getMatchResult(). If the
      // outcome isn't in the map yet, getMatchResult returns empty and MatchResult is never
      // published — settlement then never triggers. (Unit tests called getMatchResult separately,
      // so this ordering dependency only surfaced in the full-stack Phase-5 e2e.)
      outcomes.put(event.summary.eventId(), synthesizeOutcome(event, now));
      transitionTo(event, EventLifecycleStatus.FINISHED, now);
      return;
    }

    for (MockMarket market : event.markets.values()) {
      if (market.status != MarketStatus.OPEN) {
        continue;
      }
      for (MockSelection selection : market.selections.values()) {
        Odds previous = selection.currentOdds;
        Odds next = OddsSimulator.nextOdds(previous, selection.impliedProbability, rng);
        if (!previous.equals(next)) {
          selection.currentOdds = next;
          emit(
              event.summary.eventId(),
              new ProviderEvent.OddsUpdated(
                  event.summary.eventId(),
                  market.marketId,
                  selection.selectionId,
                  previous,
                  next,
                  now));
        }
      }
    }
  }

  Collection<MockEvent> activeEvents() {
    return events.values();
  }

  void transitionTo(MockEvent event, EventLifecycleStatus next, Instant now) {
    log.debug("Event {} {} -> {}", event.summary.eventId().value(), event.status, next);
    event.status = next;
    event.summary =
        new EventSummary(
            event.summary.eventId(),
            event.summary.sport(),
            event.summary.competition(),
            event.summary.homeTeam(),
            event.summary.awayTeam(),
            event.summary.scheduledStartAt(),
            next);
    emit(
        event.summary.eventId(),
        new ProviderEvent.LifecycleUpdated(
            event.summary.eventId(), next, event.summary.scheduledStartAt(), now));
  }

  void emit(EventId eventId, ProviderEvent event) {
    Sinks.Many<ProviderEvent> sink = streams.get(eventId);
    if (sink != null) {
      sink.tryEmitNext(event);
    }
  }

  private MockEvent buildEvent(Instant kickoff, Instant end, int index, Instant now) {
    EventId eventId = new EventId(UUID.randomUUID());
    MarketId marketId = new MarketId(UUID.randomUUID());

    MockSelection home =
        new MockSelection(
            new SelectionId(UUID.randomUUID()),
            "HOME",
            props.baseHomeWinProbability(),
            OddsSimulator.initialOdds(props.baseHomeWinProbability()));
    MockSelection draw =
        new MockSelection(
            new SelectionId(UUID.randomUUID()),
            "DRAW",
            props.baseDrawProbability(),
            OddsSimulator.initialOdds(props.baseDrawProbability()));
    MockSelection away =
        new MockSelection(
            new SelectionId(UUID.randomUUID()),
            "AWAY",
            props.baseAwayWinProbability(),
            OddsSimulator.initialOdds(props.baseAwayWinProbability()));

    Map<SelectionId, MockSelection> selections = new LinkedHashMap<>();
    selections.put(home.selectionId, home);
    selections.put(draw.selectionId, draw);
    selections.put(away.selectionId, away);

    MockMarket market = new MockMarket(marketId, MarketType.MATCH_RESULT_1X2, selections);

    int teamA = (index * 2) % FOOTBALL_TEAMS.length;
    int teamB = (teamA + 1) % FOOTBALL_TEAMS.length;
    EventSummary summary =
        new EventSummary(
            eventId,
            Sport.FOOTBALL,
            "Premier League",
            FOOTBALL_TEAMS[teamA],
            FOOTBALL_TEAMS[teamB],
            kickoff,
            EventLifecycleStatus.SCHEDULED);

    MockEvent event = new MockEvent();
    event.summary = summary;
    event.markets.put(marketId, market);
    event.kickoffAt = kickoff;
    event.endAt = end;
    event.status = EventLifecycleStatus.SCHEDULED;
    log.debug("Seeded mock event {} kickoff={} end={}", eventId.value(), kickoff, end);
    return event;
  }

  private MatchOutcome synthesizeOutcome(MockEvent event, Instant now) {
    double pHome = props.baseHomeWinProbability();
    double pDraw = props.baseDrawProbability();
    double roll = rng.nextDouble();
    String score;
    String winningSelection;
    if (roll < pHome) {
      score = "2-1";
      winningSelection = "HOME";
    } else if (roll < pHome + pDraw) {
      score = "1-1";
      winningSelection = "DRAW";
    } else {
      score = "0-1";
      winningSelection = "AWAY";
    }
    return new MatchOutcome(
        event.summary.eventId(),
        score,
        MatchFinalStatus.COMPLETED,
        gradeSelections(event, winningSelection),
        now);
  }

  /**
   * Per-selection result contract settlement consumes: {@code selectionId -> SettlementResult}
   * name. For the single 1X2 market the winning outcome's selection is WON and the other two LOST.
   * Without this map the MatchResult carries an empty {@code resultDetail}, so settlement stamps no
   * outcome and a completed event never settles — the defect the placeholder {@code Map.of()} hid
   * until the full-stack Phase-5 e2e (unit tests asserted score/status, never the per-leg grading).
   */
  private Map<String, String> gradeSelections(MockEvent event, String winningSelection) {
    Map<String, String> detail = new LinkedHashMap<>();
    for (MockMarket market : event.markets.values()) {
      for (MockSelection selection : market.selections.values()) {
        SettlementResult result =
            selection.name.equals(winningSelection) ? SettlementResult.WON : SettlementResult.LOST;
        detail.put(selection.selectionId.value().toString(), result.name());
      }
    }
    return detail;
  }

  private Duration toRealDuration(Duration mockDuration) {
    double mockMinutes = mockDuration.toSeconds() / SECONDS_PER_MINUTE;
    long realSeconds = Math.max(1L, (long) (mockMinutes / props.minutesPerSecond()));
    return Duration.ofSeconds(realSeconds);
  }

  @Override
  public List<EventSummary> listEvents(Sport sport) {
    List<EventSummary> result = new ArrayList<>();
    for (MockEvent event : events.values()) {
      if (event.summary.sport() == sport) {
        result.add(event.summary);
      }
    }
    return List.copyOf(result);
  }

  @Override
  public Flux<ProviderEvent> streamEvents(EventId eventId) {
    Sinks.Many<ProviderEvent> sink = streams.get(eventId);
    return sink == null ? Flux.empty() : sink.asFlux();
  }

  @Override
  public Optional<MatchOutcome> getMatchResult(EventId eventId) {
    return Optional.ofNullable(outcomes.get(eventId));
  }

  static final class MockEvent {
    EventSummary summary;
    final Map<MarketId, MockMarket> markets = new ConcurrentHashMap<>();
    Instant kickoffAt;
    Instant endAt;
    EventLifecycleStatus status;
  }

  static final class MockMarket {
    final MarketId marketId;
    final MarketType type;
    MarketStatus status;
    final Map<SelectionId, MockSelection> selections;

    MockMarket(MarketId marketId, MarketType type, Map<SelectionId, MockSelection> selections) {
      this.marketId = marketId;
      this.type = type;
      this.status = MarketStatus.OPEN;
      this.selections = selections;
    }
  }

  static final class MockSelection {
    final SelectionId selectionId;
    final String name;
    final double impliedProbability;
    Odds currentOdds;

    MockSelection(SelectionId id, String name, double impliedProbability, Odds initial) {
      this.selectionId = id;
      this.name = name;
      this.impliedProbability = impliedProbability;
      this.currentOdds = initial;
    }
  }
}
