package com.sportsbook.oddsfeed.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.sportsbook.oddsfeed.provider.EventSummary;
import com.sportsbook.oddsfeed.provider.Sport;
import com.sportsbook.protocol.event.EventLifecycleStatus;
import com.sportsbook.protocol.value.EventId;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.web.servlet.MockMvc;

@WebMvcTest(controllers = EventReadController.class)
class EventReadControllerTest {

  @Autowired private MockMvc mockMvc;
  @MockBean private EventCatalog catalog;

  @Test
  void listsEventsOnDefaultPageSize() throws Exception {
    EventSummary one = summary(1, "2026-06-01T18:00:00Z");
    EventSummary two = summary(2, "2026-06-02T18:00:00Z");
    when(catalog.orderedByKickoff()).thenReturn(List.of(one, two));

    mockMvc
        .perform(get("/api/v1/events"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.items").isArray())
        .andExpect(jsonPath("$.items.length()").value(2))
        .andExpect(jsonPath("$.items[0].eventId").value(one.eventId().value().toString()))
        .andExpect(jsonPath("$.items[0].sport").value("FOOTBALL"))
        .andExpect(jsonPath("$.items[0].scheduledStartAt").value("2026-06-01T18:00:00Z"))
        .andExpect(jsonPath("$.nextCursor").doesNotExist());
  }

  @Test
  void cursorPaginatesToSecondPage() throws Exception {
    EventSummary one = summary(1, "2026-06-01T18:00:00Z");
    EventSummary two = summary(2, "2026-06-02T18:00:00Z");
    EventSummary three = summary(3, "2026-06-03T18:00:00Z");
    when(catalog.orderedByKickoff()).thenReturn(List.of(one, two, three));

    String body =
        mockMvc
            .perform(get("/api/v1/events").param("size", "2"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.items.length()").value(2))
            .andExpect(jsonPath("$.nextCursor").exists())
            .andReturn()
            .getResponse()
            .getContentAsString();
    String cursor = extractJsonValue(body, "nextCursor");

    mockMvc
        .perform(get("/api/v1/events").param("size", "2").param("cursor", cursor))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.items.length()").value(1))
        .andExpect(jsonPath("$.items[0].eventId").value(three.eventId().value().toString()))
        .andExpect(jsonPath("$.nextCursor").doesNotExist());
  }

  @Test
  void invalidCursorReturns400() throws Exception {
    when(catalog.orderedByKickoff()).thenReturn(List.of());
    mockMvc
        .perform(get("/api/v1/events").param("cursor", "not-a-valid-cursor"))
        .andExpect(status().isBadRequest());
  }

  @Test
  void getReturnsEventWhenPresent() throws Exception {
    EventSummary s = summary(7, "2026-06-01T18:00:00Z");
    when(catalog.get(s.eventId())).thenReturn(java.util.Optional.of(s));

    mockMvc
        .perform(get("/api/v1/events/{id}", s.eventId().value()))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.eventId").value(s.eventId().value().toString()));
  }

  @Test
  void getReturns404WhenMissing() throws Exception {
    UUID id = UUID.randomUUID();
    when(catalog.get(new EventId(id))).thenReturn(java.util.Optional.empty());
    mockMvc.perform(get("/api/v1/events/{id}", id)).andExpect(status().isNotFound());
  }

  @Test
  void cursorEncodeDecodeRoundTrips() {
    EventSummary s = summary(11, "2026-06-01T18:00:00Z");
    String encoded = EventReadController.encodeCursor(s);
    EventReadController.Cursor decoded = EventReadController.decodeCursor(encoded);
    assertThat(decoded.kickoff()).isEqualTo(s.scheduledStartAt());
    assertThat(decoded.eventId()).isEqualTo(s.eventId().value());
  }

  private static EventSummary summary(int seed, String kickoff) {
    return new EventSummary(
        new EventId(new UUID(0L, seed)),
        Sport.FOOTBALL,
        "Premier League",
        "Home" + seed,
        "Away" + seed,
        Instant.parse(kickoff),
        EventLifecycleStatus.SCHEDULED);
  }

  private static String extractJsonValue(String body, String key) {
    int idx = body.indexOf("\"" + key + "\":");
    int start = body.indexOf("\"", idx + key.length() + 3) + 1;
    int end = body.indexOf("\"", start);
    return body.substring(start, end);
  }
}
