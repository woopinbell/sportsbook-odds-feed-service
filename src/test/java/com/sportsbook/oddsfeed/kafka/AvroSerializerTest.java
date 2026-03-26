package com.sportsbook.oddsfeed.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import com.sportsbook.protocol.event.EventLifecycle;
import com.sportsbook.protocol.event.EventLifecycleStatus;
import com.sportsbook.protocol.event.MarketStatus;
import com.sportsbook.protocol.event.MarketStatusChanged;
import com.sportsbook.protocol.event.OddsChanged;
import java.time.Instant;
import org.junit.jupiter.api.Test;

/**
 * Exercises the Avro encode/decode pair without spinning up Kafka. Verifies the timestamp-millis
 * logical-type conversion (Instant fields), the Avro-generated enum encoding (MarketStatus /
 * EventLifecycleStatus), and that scale-4 decimal strings survive untouched.
 */
class AvroSerializerTest {

  @Test
  void oddsChangedRoundTripsThroughBinaryEncoding() {
    OddsChanged original =
        new OddsChanged(
            "00000000-0000-0000-0000-000000000001",
            "00000000-0000-0000-0000-000000000002",
            "00000000-0000-0000-0000-000000000003",
            "1.8500",
            "1.9000",
            Instant.parse("2026-05-28T10:00:00Z"));

    AvroSerializer<OddsChanged> serializer = new AvroSerializer<>();
    byte[] bytes = serializer.serialize("odds.changed", original);

    AvroDeserializer<OddsChanged> deserializer =
        new AvroDeserializer<>(OddsChanged.getClassSchema());
    OddsChanged decoded = deserializer.deserialize("odds.changed", bytes);

    assertThat(decoded.getEventId()).isEqualTo(original.getEventId());
    assertThat(decoded.getMarketId()).isEqualTo(original.getMarketId());
    assertThat(decoded.getSelectionId()).isEqualTo(original.getSelectionId());
    assertThat(decoded.getPreviousOdds()).isEqualTo(original.getPreviousOdds());
    assertThat(decoded.getNewOdds()).isEqualTo(original.getNewOdds());
    assertThat(decoded.getChangedAt()).isEqualTo(original.getChangedAt());
  }

  @Test
  void marketStatusChangedRoundTripsEnumsAndOptionalReason() {
    MarketStatusChanged original =
        new MarketStatusChanged(
            "00000000-0000-0000-0000-000000000001",
            "00000000-0000-0000-0000-000000000002",
            MarketStatus.OPEN,
            MarketStatus.SUSPENDED,
            "goal scored",
            Instant.parse("2026-05-28T10:00:00Z"));

    byte[] bytes =
        new AvroSerializer<MarketStatusChanged>().serialize("market.status.changed", original);
    MarketStatusChanged decoded =
        new AvroDeserializer<MarketStatusChanged>(MarketStatusChanged.getClassSchema())
            .deserialize("market.status.changed", bytes);

    assertThat(decoded.getPreviousStatus()).isEqualTo(MarketStatus.OPEN);
    assertThat(decoded.getNewStatus()).isEqualTo(MarketStatus.SUSPENDED);
    assertThat(decoded.getReason()).isEqualTo("goal scored");
  }

  @Test
  void eventLifecycleRoundTripsTwoTimestamps() {
    Instant kickoff = Instant.parse("2026-06-01T18:00:00Z");
    Instant occurredAt = Instant.parse("2026-05-28T10:00:00Z");
    EventLifecycle original =
        new EventLifecycle(
            "00000000-0000-0000-0000-000000000001",
            EventLifecycleStatus.SCHEDULED,
            occurredAt,
            kickoff);

    byte[] bytes = new AvroSerializer<EventLifecycle>().serialize("event.lifecycle", original);
    EventLifecycle decoded =
        new AvroDeserializer<EventLifecycle>(EventLifecycle.getClassSchema())
            .deserialize("event.lifecycle", bytes);

    assertThat(decoded.getStatus()).isEqualTo(EventLifecycleStatus.SCHEDULED);
    assertThat(decoded.getOccurredAt()).isEqualTo(occurredAt);
    assertThat(decoded.getScheduledStartAt()).isEqualTo(kickoff);
  }

  @Test
  void serializerReturnsNullForNullInput() {
    assertThat(new AvroSerializer<OddsChanged>().serialize("odds.changed", null)).isNull();
  }
}
