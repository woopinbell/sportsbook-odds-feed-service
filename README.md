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
| `POST /internal/v1/events/{eventId}/markets/{marketId}/{suspend,close,reopen}` | 마켓 상태 변경 |

## 빌드와 실행

```sh
(cd ../sportsbook-shared-protocol-fix && ./mvnw -DskipTests install)
./mvnw verify
./mvnw spring-boot:run -Dspring-boot.run.profiles=mock
```

실제 공급자를 사용하려면 `THE_ODDS_API_KEY`를 지정하고 `real` 프로필로 실행합니다.
모의 프로필도 Redis와 Kafka가 필요합니다.

## 성능 검증

발행기 검사는 단일 JVM과 내장 Kafka에서 초당 약 290,000건을 처리해 초당 100,000건
목표를 넘었습니다. 이 값은 발행 코드의 상한을 보는 기준이며 네트워크 브로커의 운영
처리량을 뜻하지 않습니다.

2026-07-13에 독립된 로컬 환경에서 HTTP 읽기 경로를 각각 초당 1,000회로 검증했습니다.
각 경로는 60초 예열 뒤 60초 측정을 다섯 번 수행했습니다.

| 경로 | 다섯 번의 p99 | 결과 |
|---|---|---|
| 경기 목록 | 9.503, 1.743, 2.440070, 3.335, 1.535ms | 모두 50ms 미만 |
| 한 배당 조회 | 5.961, 8.969, 8.874070, 17.883, 17.289ms | 모두 50ms 미만 |

모든 측정에서 HTTP 오류와 누락된 반복이 없었습니다. 두 경로를 동시에 초당
1,000회씩 호출한 값은 참고용으로만 기록하며 합격 조건에는 포함하지 않습니다.
OpenTelemetry 내보내기는 로컬 수집기 부재가 결과를 왜곡하지 않도록 부하 실행에서
비활성화했습니다.

재현 절차와 제한은 [부하 검증 문서](load-test/README.md)에 정리되어 있습니다.

## 현재 범위

모의 공급자는 `MATCH_RESULT_1X2` 마켓을 생성합니다. 실제 공급자의 경기 결과 조회,
여러 인스턴스의 중복 발행 방지, Schema Registry 연동은 포함하지 않습니다.
전체 시스템 결정은
[통합 저장소의 ADR](https://github.com/woopinbell/sportsbook-orchestration-fix/tree/main/docs/architecture/decisions)을
참고해 주세요.

