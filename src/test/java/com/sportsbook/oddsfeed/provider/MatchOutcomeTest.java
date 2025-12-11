package com.sportsbook.oddsfeed.provider;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.sportsbook.protocol.event.MatchFinalStatus;
import com.sportsbook.protocol.value.EventId;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class MatchOutcomeTest {

  @Test
  void rejectsNullRequiredFields() {
    assertThatNullPointerException()
        .isThrownBy(
            () ->
                new MatchOutcome(null, "2-1", MatchFinalStatus.COMPLETED, Map.of(), Instant.now()))
        .withMessageContaining("eventId");
  }

  @Test
  void copiesDetailMapDefensively() {
    Map<String, String> mutable = new HashMap<>();
    mutable.put("homeGoals", "2");
    MatchOutcome outcome =
        new MatchOutcome(
            new EventId(UUID.randomUUID()),
            "2-1",
            MatchFinalStatus.COMPLETED,
            mutable,
            Instant.now());

    mutable.put("awayGoals", "1");

    assertThat(outcome.detail()).containsOnlyKeys("homeGoals");
    assertThatThrownBy(() -> outcome.detail().put("k", "v"))
        .isInstanceOf(UnsupportedOperationException.class);
  }
}
