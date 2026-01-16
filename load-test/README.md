# 발행 처리량 검사

`KafkaPublishThroughputTest`는 배당 발행기가 Avro 이벤트를 직렬화해 내장 Kafka에
보내는 처리량을 측정합니다.

```sh
./mvnw test -Dtest=KafkaPublishThroughputTest
```

목표는 단일 스레드에서 초당 100,000건입니다. 이 검사는 발행 코드의 비교 기준이며,
별도 호스트의 운영 Kafka 처리량을 대신하지 않습니다. 결과를 기록할 때는 CPU, JVM,
브로커 종류와 발행 설정을 함께 남겨야 합니다.

