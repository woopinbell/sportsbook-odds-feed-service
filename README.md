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

`OddsProvider` 인터페이스 뒤에 재현 가능한 모의 공급자와 선택형 실제 공급자를 둡니다.
`FeedOrchestrator`는 선택된 공급자의 이벤트를 Redis 캐시와 Kafka 발행기로 전달합니다.
배당이 1% 이상 변했을 때만 `odds.changed`를 발행하지만, Redis에는 모든 최신 값을
기록합니다. 같은 경기의 이벤트는 `eventId`를 파티션 키로 사용해 순서를 유지합니다.

## 제공 API

| 메서드와 경로 | 용도 |
|---|---|
| `GET /api/v1/events` | 시작 시각 기준 경기 목록 조회 |
| `GET /api/v1/events/{id}` | 경기 상세 조회 |
| `GET /api/v1/odds/{eventId}/{marketId}/{selectionId}` | 한 선택지의 현재 배당 조회 |
| `POST /internal/v1/events/{eventId}/markets/{marketId}/{suspend,close,reopen}` | 마켓 상태 변경 |

## 빌드와 실행

```sh
(cd ../sportsbook-shared-protocol-fix && ./mvnw -DskipTests install)
./mvnw verify
./mvnw spring-boot:run -Dspring-boot.run.profiles=mock
```

실제 공급자를 사용하려면 `THE_ODDS_API_KEY`를 지정하고 `real` 프로필로 실행합니다.
모의 프로필도 Redis와 Kafka가 필요합니다.

## 발행 처리량 확인

```sh
./mvnw test -Dtest=KafkaPublishThroughputTest
```

이 검사는 단일 JVM에서 발행기가 이벤트를 직렬화하고 내장 브로커에 보내는 처리량을
측정합니다. 목표는 초당 100,000건입니다. 네트워크를 거치는 운영 Kafka의 수용량을
뜻하지 않으며, 브로커와 호스트 조건을 결과와 함께 기록해야 합니다.

자세한 실행 조건은 [부하 검증 문서](load-test/README.md)를 참고해 주세요.

## 현재 범위

모의 공급자는 `MATCH_RESULT_1X2` 마켓을 생성합니다. 실제 공급자의 경기 결과 조회,
여러 인스턴스의 중복 발행 방지, Schema Registry 연동은 포함하지 않습니다.
전체 시스템 결정은
[통합 저장소의 ADR](https://github.com/woopinbell/sportsbook-orchestration-fix/tree/main/docs/architecture/decisions)을
참고해 주세요.

