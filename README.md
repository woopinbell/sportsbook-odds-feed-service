# 배당 피드 서비스

외부 배당 데이터를 스포츠북 내부 형식으로 정규화하고 Kafka와 Redis에 전달합니다.
베팅 접수나 정산은 담당하지 않으며, 다른 서비스가 사용할 배당과 경기 상태를 한
방향으로 공급합니다.

## 처리 흐름

```text
MockOddsProvider ─┐
                  ├─▶ FeedOrchestrator ─▶ Redis
TheOddsApiProvider┘                    └─▶ Kafka
                                           ├─ odds.changed
                                           ├─ market.status.changed
                                           ├─ event.lifecycle
                                           └─ match.result
```

`OddsProvider` 인터페이스 뒤에 두 공급자를 둡니다.

- 기본 `mock` 프로필은 시드가 고정된 경기와 배당 변화를 만들어 로컬 개발과 통합
  테스트를 재현할 수 있게 합니다.
- 선택 사항인 `real` 프로필은 The Odds API를 호출합니다. 호출 빈도와 월간 사용량을
  제한하며 `THE_ODDS_API_KEY`가 필요합니다.

`FeedOrchestrator`는 선택된 공급자의 이벤트를 Redis 캐시와 Kafka 발행기로 전달합니다.
배당이 1% 이상 변했을 때만 `odds.changed`를 발행하지만, Redis에는 모든 최신 값을
기록합니다. 같은 경기의 이벤트는 `eventId`를 파티션 키로 사용해 순서를 유지합니다.

## 제공 API

| 메서드와 경로 | 용도 |
|---|---|
| `GET /api/v1/events` | 시작 시각 기준 경기 목록 조회 |
| `GET /api/v1/events/{id}` | 경기 상세 조회 |
| `GET /api/v1/odds/{eventId}/{marketId}/{selectionId}` | 한 선택지의 현재 배당 조회 |
| `POST /internal/v1/events/{eventId}/markets/{marketId}/suspend` | 마켓 일시 중지 |
| `POST /internal/v1/events/{eventId}/markets/{marketId}/close` | 마켓 종료 |
| `POST /internal/v1/events/{eventId}/markets/{marketId}/reopen` | 마켓 재개 |

## 기술 구성

- Java 17, Spring Boot 3.2, Maven
- Spring MVC, WebClient, Project Reactor
- Kafka, Avro, Redis(Lettuce)
- Micrometer, OpenTelemetry, JSON 로그
- JUnit 5, Mockito, WireMock, Testcontainers, Embedded Kafka

## 빌드와 실행

공통 계약을 먼저 로컬 Maven 저장소에 설치합니다.

```sh
(cd ../sportsbook-shared-protocol-fix && ./mvnw -DskipTests install)
./mvnw verify
```

모의 공급자로 실행하려면 Redis와 Kafka를 준비한 뒤 다음 명령을 사용합니다.

```sh
./mvnw spring-boot:run -Dspring-boot.run.profiles=mock
```

실제 공급자는 API 키를 환경 변수로 전달합니다.

```sh
THE_ODDS_API_KEY=your-key \
  ./mvnw spring-boot:run -Dspring-boot.run.profiles=real
```

## 주요 설정

- Redis 배당 키: `odds:{eventId}:{marketId}:{selectionId}`
- Redis 경기 키: `event:{eventId}`
- Redis 마켓 키: `market:{eventId}:{marketId}`
- 기본 캐시 보존 기간: 24시간
- 기본 배당 발행 임계값: 1%

## 현재 범위

모의 공급자는 `MATCH_RESULT_1X2` 마켓을 생성합니다. 실제 공급자의 경기 결과 조회,
여러 인스턴스의 중복 발행 방지, Schema Registry 연동은 포함하지 않습니다.
전체 시스템 결정은
[통합 저장소의 ADR](https://github.com/woopinbell/sportsbook-orchestration-fix/tree/main/docs/architecture/decisions)을
참고해 주세요.

