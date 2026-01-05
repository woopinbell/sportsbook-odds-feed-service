package com.sportsbook.oddsfeed.kafka;

import java.util.HashMap;
import java.util.Map;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

/**
 * Wires the Kafka producer side for Avro records. Spring Boot's auto-configuration would give us a
 * {@code KafkaTemplate<Object, Object>} backed by ByteArraySerializer (what application.yml
 * configures), which is the right default for a service that doesn't yet know its payload type.
 * Once we commit to Avro, we replace it with a typed template against the StringSerializer +
 * AvroSerializer pair so any non-Avro send is a compile-time error.
 */
@Configuration
public class KafkaConfig {

  @Bean
  public ProducerFactory<String, SpecificRecord> avroProducerFactory(KafkaProperties properties) {
    Map<String, Object> configs = new HashMap<>(properties.buildProducerProperties());
    configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, AvroSerializer.class);
    return new DefaultKafkaProducerFactory<>(configs);
  }

  @Bean
  public KafkaTemplate<String, SpecificRecord> avroKafkaTemplate(
      ProducerFactory<String, SpecificRecord> avroProducerFactory) {
    return new KafkaTemplate<>(avroProducerFactory);
  }
}
