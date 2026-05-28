package com.sportsbook.oddsfeed.kafka;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.apache.avro.data.TimeConversions;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

/**
 * Schema-Registry-free Avro Serializer for Kafka. ADR-0014 keeps the V1 build registry-less: the
 * single {@code shared-protocol} artifact is the schema source of truth for every producer and
 * consumer in the system, so we can serialize records as raw Avro binary without the Confluent
 * "magic byte + schema id" framing.
 *
 * <p>Registers the timestamp-millis logical-type conversion globally so generated classes whose
 * {@code Instant} fields would otherwise fail to encode (Avro defaults to Joda) round-trip cleanly
 * through {@code java.time.Instant}.
 */
public class AvroSerializer<T extends SpecificRecord> implements Serializer<T> {

  static {
    SpecificData.get().addLogicalTypeConversion(new TimeConversions.TimestampMillisConversion());
  }

  @Override
  public byte[] serialize(String topic, T data) {
    if (data == null) {
      return null;
    }
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try {
      SpecificDatumWriter<T> writer = new SpecificDatumWriter<>(data.getSchema());
      BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
      writer.write(data, encoder);
      encoder.flush();
    } catch (IOException e) {
      throw new SerializationException("Failed to serialize Avro record on topic " + topic, e);
    }
    return out.toByteArray();
  }
}
