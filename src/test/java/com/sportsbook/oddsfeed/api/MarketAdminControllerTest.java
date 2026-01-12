package com.sportsbook.oddsfeed.api;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sportsbook.oddsfeed.cache.RedisOddsCache;
import com.sportsbook.oddsfeed.publisher.OddsFeedPublisher;
import com.sportsbook.protocol.event.MarketStatus;
import com.sportsbook.protocol.value.EventId;
import com.sportsbook.protocol.value.MarketId;
import java.time.Clock;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

@WebMvcTest(controllers = MarketAdminController.class)
class MarketAdminControllerTest {

  static final Instant FIXED_NOW = Instant.parse("2026-05-28T10:00:00Z");

  @Autowired private MockMvc mockMvc;
  @Autowired private ObjectMapper objectMapper;
  @MockBean private RedisOddsCache cache;
  @MockBean private OddsFeedPublisher publisher;
  @MockBean private Clock clock;

  @BeforeEach
  void setUpClock() {
    when(clock.instant()).thenReturn(FIXED_NOW);
  }

  @Test
  void suspendPublishesAndCaches() throws Exception {
    UUID eventId = UUID.randomUUID();
    UUID marketId = UUID.randomUUID();
    when(cache.getMarketStatus(new EventId(eventId), new MarketId(marketId)))
        .thenReturn(Optional.of(MarketStatus.OPEN));

    mockMvc
        .perform(
            post("/internal/v1/events/{e}/markets/{m}/suspend", eventId, marketId)
                .contentType(MediaType.APPLICATION_JSON)
                .content(
                    objectMapper.writeValueAsString(
                        new MarketAdminController.MarketStatusChangeRequest("goal scored"))))
        .andExpect(status().isAccepted());

    verify(publisher)
        .publishMarketStatusChanged(
            eq(new EventId(eventId)),
            eq(new MarketId(marketId)),
            eq(MarketStatus.OPEN),
            eq(MarketStatus.SUSPENDED),
            eq("goal scored"),
            eq(FIXED_NOW));
    verify(cache)
        .storeMarketStatus(new EventId(eventId), new MarketId(marketId), MarketStatus.SUSPENDED);
  }

  @Test
  void closeUsesUnknownPreviousAsOpenDefault() throws Exception {
    UUID eventId = UUID.randomUUID();
    UUID marketId = UUID.randomUUID();
    when(cache.getMarketStatus(any(), any())).thenReturn(Optional.empty());

    mockMvc
        .perform(
            post("/internal/v1/events/{e}/markets/{m}/close", eventId, marketId)
                .contentType(MediaType.APPLICATION_JSON)
                .content(
                    objectMapper.writeValueAsString(
                        new MarketAdminController.MarketStatusChangeRequest("scheduled close"))))
        .andExpect(status().isAccepted());

    verify(publisher)
        .publishMarketStatusChanged(
            any(), any(), eq(MarketStatus.OPEN), eq(MarketStatus.CLOSED), any(), any());
  }

  @Test
  void reopenSetsStatusBackToOpen() throws Exception {
    UUID eventId = UUID.randomUUID();
    UUID marketId = UUID.randomUUID();
    when(cache.getMarketStatus(any(), any())).thenReturn(Optional.of(MarketStatus.SUSPENDED));

    mockMvc
        .perform(
            post("/internal/v1/events/{e}/markets/{m}/reopen", eventId, marketId)
                .contentType(MediaType.APPLICATION_JSON)
                .content(
                    objectMapper.writeValueAsString(
                        new MarketAdminController.MarketStatusChangeRequest("VAR cleared"))))
        .andExpect(status().isAccepted());

    verify(publisher)
        .publishMarketStatusChanged(
            any(), any(), eq(MarketStatus.SUSPENDED), eq(MarketStatus.OPEN), any(), any());
  }

  @Test
  void rejectsBlankReason() throws Exception {
    UUID eventId = UUID.randomUUID();
    UUID marketId = UUID.randomUUID();

    mockMvc
        .perform(
            post("/internal/v1/events/{e}/markets/{m}/suspend", eventId, marketId)
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"reason\":\"\"}"))
        .andExpect(status().isBadRequest());
  }
}
