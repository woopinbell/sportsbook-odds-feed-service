# odds-feed-service 변경 비용 시뮬레이션 (change-cost)

> 6~12개월 안에 현실적으로 들어올 만한 변경 요청과, 그때 **어디가 깨지고
> 어떻게 복구하며 비용이 얼마나 드는지**를 미리 적어둔 문서. 설계가 어떤 변경에
> 강하고 어떤 변경에 약한지를 정직하게 드러내는 게 목적이다.
> 비용 추정은 상대적 규모(S/M/L)로 표기한다.

---

## 변경 시나리오

### 시나리오 1 — 새 MarketType 추가 (예: TOTAL_OVER_UNDER)

| 항목 | 내용 |
|---|---|
| **변경 요청** | mock이 1X2뿐 아니라 Over/Under 2.5 마켓도 생성·publish하도록. |
| **깨질 위치** | `MockOddsProvider.buildEvent` (이벤트당 마켓 1개·selection 3개 하드코딩), `OddsSimulator` (1X2 확률 모델만 있음 — O/U는 라인 + 양방향 확률 필요), mock의 `MATCH_RESULT_1X2` 상수 가정. |
| **안 깨지는 곳** | `OddsFeedPublisher` (marketId·selectionId를 불투명하게 다룸 — 마켓 종류 모름), Avro 스키마(`OddsChanged`는 id만 들고 감), `RedisOddsCache`(key 구조 동일), REST API(generic). |
| **복구 동선** | `MockMarket`을 marketType별로 빌드하는 팩토리로 일반화 → `OddsSimulator`에 O/U용 라인·확률 추가 → `buildEvent`가 이벤트당 여러 마켓 생성. publisher·cache·API는 무수정. |
| **비용** | **M.** provider 내부에 갇힘. 추상화 덕에 하류는 안 건드림. |

### 시나리오 2 — Schema Registry (Apicurio) 도입

| 항목 | 내용 |
|---|---|
| **변경 요청** | 다중 환경 배포·외부 partner producer 대비, Avro 직렬화를 registry 기반으로 (ADR-0014 V2 트리거). |
| **깨질 위치** | `AvroSerializer` / `AvroDeserializer` (지금은 raw binary — registry는 "magic byte + schema id" framing 필요), `KafkaConfig`(serializer 교체), orchestration docker-compose(컨테이너 추가), 모든 consumer 서비스(역호환). |
| **안 깨지는 곳** | `OddsFeedPublisher` (직렬화는 KafkaTemplate에 위임 — serializer 교체만으로 흡수), `.avsc` 정의(`shared-protocol`에 그대로), 도메인 코드 전부. |
| **복구 동선** | Confluent/Apicurio serializer 의존성 추가 → `KafkaConfig`의 value-serializer를 registry-aware로 교체 → registry URL을 application.yml에 → docker-compose에 Apicurio + DB. producer/consumer 단계적 롤아웃. |
| **비용** | **L.** 코드 변경은 좁지만(serializer 2개) 인프라·운영·전 서비스 호환 검증이 큼. ADR-0014가 "V1 미도입"을 명시적으로 정당화해둔 이유. |

### 시나리오 3 — 세 번째 데이터 소스 추가 (예: Sportradar)

| 항목 | 내용 |
|---|---|
| **변경 요청** | The Odds API 무료 tier가 부족해, 유료 Sportradar adapter를 추가. |
| **깨질 위치** | (신규) `SportradarProvider implements OddsProvider`, (신규) `@Profile("sportradar")`, (신규) `SportradarProperties`, `application-sportradar.yml`. |
| **안 깨지는 곳** | `OddsProvider` 인터페이스, `FeedOrchestrator`(profile 모름 — 활성 provider 빈 하나만 주입받음), cache·publisher·API 전부. |
| **복구 동선** | `OddsProvider` 3메서드 구현 + profile 1개 추가. orchestrator는 무수정 — 이게 adapter 패턴(ADR-0010)의 정확한 배당금. |
| **비용** | **S~M.** 인터페이스가 처음부터 이걸 노렸다. 새 클래스 1개 + 설정 + 매핑 로직. |

### 시나리오 4 — 수평 확장 (multi-instance 배포)

| 항목 | 내용 |
|---|---|
| **변경 요청** | 트래픽 증가로 odds-feed를 2+ 인스턴스로. |
| **깨질 위치** | `FeedOrchestrator` — 인스턴스마다 `@PostConstruct`/`@Scheduled refresh()`가 돌아 **모든 토픽을 double-publish**. 클래스 Javadoc에 "single-instance by design"이라 명시해둔 바로 그 가정이 깨진다. |
| **안 깨지는 곳** | provider·publisher·cache·API의 로직 자체(인스턴스당으로는 정상). Kafka partition key=eventId 덕에 순서는 유지되지만 **중복**이 문제. |
| **복구 동선** | (a) leader election(예: ShedLock·k8s Lease)으로 한 인스턴스만 publish, 또는 (b) sport/event-id로 인스턴스 간 샤딩. consumer 멱등 처리도 병행 검토. |
| **비용** | **L.** 분산 조정이 새로 들어온다. V1이 의도적으로 안 산 비용 (Phase 5 k8s). |

### 시나리오 5 — 경기 결과 실연동 (`getMatchResult`)

| 항목 | 내용 |
|---|---|
| **변경 요청** | settlement-service가 실 데이터 결과로 정산하도록 The Odds API `/scores` 연동. |
| **깨질 위치** | `TheOddsApiProvider.getMatchResult` (지금 `Optional.empty()` 고정), RateLimiter·QuotaCounter(추가 호출이 월 quota를 더 먹음). |
| **안 깨지는 곳** | `MatchOutcome` 레코드·`FeedOrchestrator`의 FINISHED → publishMatchResult 경로(이미 mock으로 동작 검증됨), `match.result` 토픽. |
| **복구 동선** | `/scores` 응답 DTO 추가 → `getMatchResult`가 그걸 `MatchOutcome`으로 매핑 → quota 예산 재배분(odds polling과 results polling 분리). |
| **비용** | **S~M.** orchestrator 경로가 이미 mock으로 닦여 있어 provider 한 메서드 채우는 일에 가깝다. |

---

## 의도적으로 미룬 진화

- **outbox 패턴을 안 썼다.** Kafka publish와 Redis write가 원자적이지 않다.
  odds-feed 이벤트는 멱등적(같은 selection의 다음 변경이 누락분을 덮어씀)이라
  "돈을 다루는" wallet/betting에서 요구되는 transactional outbox(ADR-0006)의
  세금을 여기선 안 냈다. **임계점**: 누락된 캐시 갱신이 실제 베팅 분쟁으로
  이어진 사례가 관측되면 outbox 도입.
- **margin / overround 미구현.** mock의 확률 합이 1.0이라 북메이커 margin이
  없다. 실 운영 시뮬레이션 충실도를 높이려면 `OddsSimulator`에 overround 주입.
  지금은 학습 가치 대비 우선순위가 낮아 보류.
- **per-market threshold 미지원.** `PublishProperties.oddsChangeThreshold`가
  전역 1% 단일값. 마켓별로 noise 특성이 다르면 map 기반으로 확장. 현재는 단일
  값으로 충분.

## 재설계가 합리적인 임계점

지금 구조를 **갈아엎는 게 더 싼** 지점은 다음 중 하나가 오는 때다:

1. **다중 인스턴스가 필수가 되는 순간** (시나리오 4). 그때는 orchestrator를
   "단일 프로세스 조립기"에서 "샤딩·리더 election을 아는 분산 컴포넌트"로
   재설계하는 게, 기존 구조에 조정 로직을 덧대는 것보다 깨끗하다.
2. **provider가 push 기반(WebSocket/SSE)으로 바뀌는 순간.** 현재 real
   provider는 poll + diff다. 실시간 push 소스가 들어오면 `streamEvents`의
   계약(이미 `Flux`라 push 친화적)은 유지되지만, RateLimiter·QuotaCounter·
   pollSport 전체가 무의미해지고 backpressure 전략이 새로 필요하다. 이때는
   real adapter를 새로 쓰는 게 맞다.
3. **Schema 진화가 빈번해지는 순간** (시나리오 2). 단일 `shared-protocol`
   버전 강제가 배포 병목이 되면 registry 기반으로 전환. AvroSerializer 2개가
   교체 지점이라 코드 비용은 작지만, 그 시점이 곧 운영 모델 전환점이다.

세 경우 모두 **인터페이스(`OddsProvider`, `ProviderEvent`)는 살아남고 구현만
교체**되도록 경계를 그어둔 게 V1 설계의 핵심 베팅이다.
