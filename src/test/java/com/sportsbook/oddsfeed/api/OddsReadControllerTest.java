package com.sportsbook.oddsfeed.api;

import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.sportsbook.oddsfeed.cache.RedisOddsCache;
import com.sportsbook.protocol.value.EventId;
import com.sportsbook.protocol.value.MarketId;
import com.sportsbook.protocol.value.Odds;
import com.sportsbook.protocol.value.SelectionId;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.web.servlet.MockMvc;

@WebMvcTest(controllers = OddsReadController.class)
class OddsReadControllerTest {

  @Autowired private MockMvc mockMvc;
  @MockBean private RedisOddsCache cache;

  @Test
  void returnsOddsWhenPresent() throws Exception {
    UUID eventId = UUID.randomUUID();
    UUID marketId = UUID.randomUUID();
    UUID selectionId = UUID.randomUUID();
    when(cache.getOdds(new EventId(eventId), new MarketId(marketId), new SelectionId(selectionId)))
        .thenReturn(Optional.of(Odds.ofDecimal("1.85")));

    mockMvc
        .perform(get("/api/v1/odds/{e}/{m}/{s}", eventId, marketId, selectionId))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.eventId").value(eventId.toString()))
        .andExpect(jsonPath("$.marketId").value(marketId.toString()))
        .andExpect(jsonPath("$.selectionId").value(selectionId.toString()))
        .andExpect(jsonPath("$.odds").value(1.85));
  }

  @Test
  void returns404WhenMissing() throws Exception {
    UUID eventId = UUID.randomUUID();
    UUID marketId = UUID.randomUUID();
    UUID selectionId = UUID.randomUUID();
    when(cache.getOdds(new EventId(eventId), new MarketId(marketId), new SelectionId(selectionId)))
        .thenReturn(Optional.empty());

    mockMvc
        .perform(get("/api/v1/odds/{e}/{m}/{s}", eventId, marketId, selectionId))
        .andExpect(status().isNotFound());
  }
}
