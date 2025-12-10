# 배당 피드 서비스

외부 배당 데이터를 스포츠북 내부 형식으로 바꾸고 Kafka와 Redis에 전달하는
Spring Boot 서비스입니다.

## 기술 구성

- Java 17, Spring Boot 3.2, Maven
- Kafka, Redis, Avro
- JUnit 5, Testcontainers

## 빌드

공통 계약을 먼저 설치한 다음 검증을 실행합니다.

```sh
(cd ../sportsbook-shared-protocol-fix && ./mvnw -DskipTests install)
./mvnw verify
```

