# odds-feed-service 회고 (retrospective)

> Phase 2-C. `shared-protocol` 직후, `betting-service`보다 먼저 진행한 leaf
> 서비스. dev 11 커밋 + 부하 baseline 시점의 회고에, Phase 5 전체 스택 통합 3 커밋을 반영해 갱신했다.
> 1인칭 기록이며, 모든 서술은 git log·commit body·코드 diff에 근거한다.

---

## 1. 무엇을 만들었나

odds 데이터의 **단방향 진입점 (single-direction ingress)** 을 만들었다. 외부
공급자(mock 또는 The Odds API)로부터 odds 변경·마켓 상태·이벤트 lifecycle·경기
결과를 받아 **Kafka에 publish**하고 동시에 **Redis 캐시를 write-through**로
갱신한다. 다른 서비스는 Kafka를 subscribe하거나 Redis를 read해서 odds를 받는다.
이 서비스 자체는 베팅 접수도, odds 산정도, 정산도 하지 않는다 (책임 경계는
repo CLAUDE.md의 DO / DO NOT).

구체적으로 11개의 dev 커밋으로 쌓은 것:

1. **scaffold** — Spring Boot 3.2.11 application (`spring-boot-starter-parent`
   상속), mock/real profile 분리, 관측성 wiring(logback JSON, Micrometer,
   OTel), Spotless + Checkstyle.
2. **`OddsProvider` 추상화** — `listEvents` / `streamEvents` / `getMatchResult`
   세 메서드. mock과 real이 같은 인터페이스 뒤에 선다 (ADR-0010 adapter 패턴).
   변경 이벤트는 sealed interface `ProviderEvent` (OddsUpdated /
   MarketStatusUpdated / LifecycleUpdated) 하나의 `Flux`로 흐른다.
3. **`MockOddsProvider`** — 결정론적 시뮬레이터. 이벤트 lifecycle
   (SCHEDULED → IN_PLAY → FINISHED)을 `@Scheduled` tick으로 진행시키고, odds는
   `OddsSimulator`의 평균회귀 random walk로 움직인다.
4. **`MockScenario` 라이브러리** — LateGoal / MatchPostponed /
   SuddenMarketSuspend / OddsCrash 네 가지 교란 + `ScenarioRotator`.
5. **`TheOddsApiProvider`** — real 모드. WebClient + sliding-window
   RateLimiter + Redis 월간 quota 카운터.
6. **`RedisOddsCache`** — write-through 캐시. 세 가지 key 구조.
7. **`OddsFeedPublisher`** — Avro Kafka publisher. partition key = `eventId`,
   1% threshold 필터.
8. **`FeedOrchestrator`** — provider → cache + publisher를 잇는 유일한 조립
   지점.
9. **REST read API + trader admin** — events 목록(cursor pagination) /
   단건 / odds 조회 + suspend·close·reopen.
10. **부하 baseline** — `KafkaPublishThroughputTest`로 publisher → Kafka
    처리량 측정.
11. **README 성능 섹션** — 측정값 박제.

검증: `./mvnw verify` 기준 **72개 테스트 통과**, Spotless + Checkstyle 0
violation. 부하 baseline은 in-process broker 기준 **약 290,000 events/sec**
(목표 10만의 약 3배).

Phase 5 전체 스택 통합에서 세 가지가 더해졌다: pooled Lettuce 부팅 의존
(commons-pool2, [011](../commits/011.md)), 마켓 상태를 lifecycle 양방향으로
캐시(odds tick에 OPEN + terminal에 CLOSE, [012](../commits/012.md)), mock이 1X2
selection을 채점한 `resultDetail`을 발행([013](../commits/013.md)). 셋 다 단위
테스트를 통과하고 full-stack e2e에서만 드러난 결함이었다(§3-6).

---

## 2. 시작 시점의 가설

세션을 시작할 때 머릿속에 있던 가정들:

- **"leaf 서비스니까 단순할 것이다."** `shared-protocol`만 의존하는 Phase 2
  leaf라, wallet/risk와 함께 "가볍게 끝나는" 묶음으로 분류돼 있었다
  (SESSION_STARTUP.md). 실제로 도메인 로직은 가볍지만, **외부 인프라(Kafka,
  Redis)와 두 개의 데이터 소스 모드**를 다뤄야 해서 표면적이 좁지 않았다.
- **"mock은 그냥 랜덤이면 된다."** 처음엔 odds를 단순 난수로 흔들 생각이었다.
  하지만 그러면 odds가 발산하거나 1.0 밑으로 내려가 `Odds` value object가
  던지는 예외를 맞는다. 평균회귀(mean reversion) + clamping이 필요하다는 걸
  구현하면서 깨달았다.
- **"Testcontainers로 Kafka·Redis 통합 테스트를 다 돌리면 된다."**
  sportsbook/CLAUDE.md Level 1 전략이 "실제 컨테이너를 띄워라"였으므로 당연히
  Kafka도 Testcontainers로 갈 생각이었다.
- **"Avro는 shared-protocol이 다 해줬으니 publish는 `template.send` 한 줄."**
  스키마는 이미 있으니 직렬화는 거저일 줄 알았다.
- **"Java 17이면 최신 문법(switch pattern matching)을 다 쓸 수 있다."**
  sealed interface를 쓰면 당연히 `switch` 패턴 매칭으로 dispatch할 생각이었다.

---

## 3. 가설 vs 실제 — 어디서 실제로 시간을 잃었나

솔직하게, 도메인 로직보다 **빌드·테스트·인프라 마찰**에서 시간을 더 썼다.

### 3-1. Testcontainers Kafka가 ARM macOS에서 안 떴다 (가장 큰 마찰)

가장 많이 헤맨 지점. `confluentinc/cp-kafka` 이미지가 이 환경(Apple Silicon
macOS)에서 컨테이너 기동에 실패했다. publisher 커밋에서 end-to-end round-trip
통합 테스트를 짜뒀다가 **통째로 들어내고**, Avro codec 자체를 직접 round-trip
하는 단위 테스트(`AvroSerializerTest`)로 대체했다. 부하 테스트(작업 6)에서도
같은 벽에 부딪혀, 결국 `spring-kafka-test`의 **`EmbeddedKafkaZKBroker`** (in-process
broker)로 갔다. 여기서도 함정이 하나 더 있었다 — `EmbeddedKafkaBroker`는
abstract라서 직접 `new` 할 수 없고, 구상 클래스인 `EmbeddedKafkaZKBroker`를
찾아야 했다 (jar 안을 grep해서 확인).

→ 교훈: "실제 컨테이너" 원칙은 좋지만, **호스트 아키텍처가 이미지 호환성을
좌우한다.** Redis(`redis:7-alpine`)는 멀쩡히 떴는데 Kafka 이미지만 안 떴다.
in-process broker는 같은 Kafka client 코드 경로를 쓰므로 publisher 측 측정엔
유효하지만, broker 실측치는 별도 환경에서 다시 봐야 한다는 한계를 README에
명시했다.

### 3-2. Spotless / Checkstyle 가 거의 매 커밋 한 번씩 막았다

C/C++ 배경에서 온 입장에서 가장 낯설었던 부분. 빌드가 **포맷·린트 위반으로
fail**하는 경험이 반복됐다:

- `google-java-format`이 주석 줄바꿈(comment reflow)을 강제해서, 내가 쓴
  Javadoc 줄 길이가 미묘하게 어긋나면 `spotless:check`가 fail. → `mvn
  spotless:apply`로 자동 정리하는 습관이 들었다.
- Checkstyle `MagicNumber` — `60.0`(분→초), WebClient timeout `10` 같은 숫자
  리터럴을 상수로 추출하라고 막았다 (`SECONDS_PER_MINUTE`, `FETCH_TIMEOUT`).
- Checkstyle `HideUtilityClassConstructor` — Spring Boot 메인 클래스
  (`OddsFeedApplication`)를 utility class로 오인해서 막았다.
  `@SuppressWarnings("checkstyle:HideUtilityClassConstructor")`로 해결.

→ C/C++엔 빌드 자체를 막는 표준 포매터가 거의 없다. Java 생태계에서는 **포맷이
빌드 게이트의 일부**라는 걸 몸으로 익혔다. 한 번 흐름을 타니 "코드 짠 뒤
`spotless:apply` 먼저, 그다음 `verify`"가 루틴이 됐다.

### 3-3. Java 17 ≠ Java 21 — switch pattern matching이 없었다

`FeedOrchestrator.dispatch`에서 sealed `ProviderEvent`를 `switch` 패턴 매칭으로
처리하려다 컴파일 에러를 맞았다 — switch pattern matching은 **Java 21+**.
ADR-0015가 못박은 건 JDK 17 LTS다. `if (event instanceof ... x)` 체인으로
내려야 했다. sealed interface의 exhaustiveness 이점을 컴파일 타임에 100% 강제할
수는 없는 절충(default 없는 if-else 체인 + 리뷰 시점 검증)을 commit body에
적어뒀다.

### 3-4. mock 시간 스케일 산수 버그 — 테스트가 잡았다

`MockOddsProvider.toRealDuration`에서 mock분 → 실초 변환을 처음에 60을 한 번 더
곱해서, 90 mock분짜리 경기가 5400 실초(90분)가 되도록 잘못 짰다. lifecycle
transition 테스트(`tickProgressesEventThroughFullLifecycle`)가 FINISHED에
도달 못 해서 빨갛게 떴고, 그 덕에 발견했다. → 단위 테스트가 "산수 실수"를 즉시
잡아준 정확한 사례.

### 3-5. 테스트 하네스의 자잘한 API 마찰

- `@WebMvcTest`에서 `Clock`을 주입하려고 inner `@Configuration` +
  `@Import`를 썼더니 컨텍스트가 안정적으로 안 잡혀서 admin 컨트롤러 테스트가
  404로 떨어졌다. → `@MockBean Clock`으로 단순화하니 깔끔히 통과.
- WireMock 3.x에서 stub 초기화 API(`removeStubsByMetadata`)의 시그니처를 잘못
  써서 컴파일 에러. → `resetMappings()`로 교체.

→ 이런 건 "도메인"이 아니라 "도구 사용법"이고, 처음 쓰는 스택에서는 이게 시간의
상당 부분을 먹는다는 걸 인정하게 됐다.

### 무엇이 가설대로 쉬웠나

- **`OddsProvider` 추상화**는 처음 설계대로 흘렀다. mock을 먼저 짜고 real을
  같은 인터페이스에 끼우는 데 인터페이스 변경이 0이었다. adapter 패턴이
  값을 한 정확한 사례.
- **Redis write-through**는 단순했다. `StringRedisTemplate` + 평문 decimal
  문자열로 충분했고 Testcontainers Redis가 한 번에 떴다.
- **1% threshold 필터**는 `BigDecimal` 산수 한 메서드(`isSignificantChange`)로
  끝났고, 경계값(정확히 1%) 테스트까지 한 번에 통과.

### 3-6. (Phase 5 통합) full-stack에서만 드러난 세 결함

dev의 단위 테스트는 Redis 키를 손으로 seed하고, unpooled 클라이언트로 캐시를
돌리고, `getMatchResult`를 별도로 호출해 score/status만 단언했다. 그래서 다음
셋이 숨어 있다가 orchestration compose/e2e에서 실제 betting·settlement과
붙어서야 드러났다.

- **pooled Lettuce 부팅 의존**([011](../commits/011.md)). `lettuce.pool.enabled=true`
  인데 `commons-pool2`가 POM에 없어 compose 부팅이 크래시했다. 풀을 안 켜는 단위
  테스트는 못 잡는다. 의존 1줄로 해결.
- **마켓 상태가 한 방향만 캐시됐다**([012](../commits/012.md)). betting은
  `market:{e}:{m}==OPEN`을 보고 슬립을 받는데, odds-feed는 suspend/close
  이벤트에서만 상태를 써서 정상 OPEN 마켓의 키가 없었다 → 전건 거절. 거울상으로
  끝난 이벤트는 OPEN으로 잔존 → 죽은 이벤트에 베팅 수락. odds tick에 OPEN을 쓰고
  terminal lifecycle에 CLOSE해 양방향을 닫았다.
- **mock이 채점되지 않은 결과를 발행했다**([013](../commits/013.md)). MatchResult의
  `resultDetail`이 빈 맵이라 settlement이 stamp할 selection별 WON/LOST 계약이 없어
  완료 경기가 정산되지 않았다. score로 1X2를 채점해 맵을 채웠다(+ outcome을
  FINISHED emit 전에 저장하는 순서 버그도 함께). 이로써 settlement이 문서로만
  합의했던 결과 계약을 odds-feed가 실제로 채웠다.

공통 교훈: leaf 서비스의 단위 테스트는 *경계 너머의 소비자*를 흉내 내느라 키를
seed하고 의존을 mock하는데, 바로 그 흉내가 **producer/consumer 계약의 공백(빈
맵·없는 키·빠진 런타임 의존)**을 가린다. 이건 전체 스택 e2e의 몫이다.

---

## 4. 다시 한다면

- **Kafka 통합 테스트 전략을 처음부터 in-process로 잡는다.** ARM macOS가
  타깃이라는 걸 알았으니, publisher 커밋에서 Testcontainers Kafka로 돌아가지
  않고 곧장 `EmbeddedKafkaZKBroker`를 썼을 것이다. 들어냈다 다시 짜는 왕복을
  아꼈을 것. (Redis는 Testcontainers로 멀쩡하니 그대로 둔다.)
- **`mvn spotless:apply`를 pre-commit hook으로** 걸었을 것이다. 매 커밋마다
  포맷 위반으로 한 번씩 fail하는 대신, 커밋 직전 자동 정리되게.
- **mock 시간 변환을 처음부터 테스트 우선(test-first)으로** 짰을 것이다.
  `toRealDuration(90 mock분) == 90 실초`를 먼저 단언하고 구현했으면 60 곱셈
  버그를 애초에 안 냈다.
- **`OddsReadController`의 URL을 더 고민**했을 것이다. 지금은 reverse index가
  없어서 `(eventId, marketId, selectionId)` 세 개를 다 path에 받는다. 호출자
  (betting-service)가 실제로 어떤 키를 들고 오는지 인터페이스를 먼저 합의했으면
  더 자연스러운 URL이 나왔을 수 있다. 다만 이건 betting-service를 짜기 전이라
  알 수 없었던 정보라, "다시 해도 똑같이 미뤘을" 항목에 가깝다.
- **반대로 그대로 둘 결정들**: provider 추상화, sealed `ProviderEvent` 단일
  스트림, write-through(outbox 안 씀), partition key = eventId. 전부 ADR이나
  도메인 특성에 근거가 있었고 구현하면서 후회가 없었다.

---

## 5. 남은 한계 (의도적으로 닫지 않은 범위)

ADR-0012(V1 scope)와 repo CLAUDE.md 결정에 따라 **일부러 열어둔** 것들. "못 한
게 아니라 안 한 것"을 면접에서 구분해 말하기 위해 기록한다.

- **`TheOddsApiProvider.getMatchResult`는 `Optional.empty()` 고정.** The Odds
  API의 `/scores` endpoint 연동은 V2. V1 정산 흐름은 mock 결과로 검증한다.
  (실 데이터 결과는 무료 tier에서 신뢰도가 낮아 후순위.)
- **Schema Registry 없음** (ADR-0014). producer/consumer가 같은
  `shared-protocol` 버전을 의존해 스키마 불일치가 원천 차단되므로 V1에선
  registry가 오버킬. V2(다중 환경 배포 + 외부 partner producer)에서 Apicurio.
- **end-to-end latency(mock → Kafka p99 < 100ms) 미측정.** mock 생성 시점과
  broker commit 시점을 상관시키려면 둘이 별도 프로세스여야 의미가 있어서,
  orchestration repo의 Phase 5 e2e로 미뤘다.
- **single-instance 가정.** orchestrator 인스턴스가 둘이면 모든 토픽에
  double-publish한다. leader election이나 event-id 샤딩은 Phase 5 k8s 범위.
- **마켓 추적이 in-memory.** [012](../commits/012.md)의 `marketsByEvent`
  (orchestrator 프로세스 메모리)는 odds tick에서 채워지므로, 한 이벤트의 odds
  tick과 terminal lifecycle 사이에 orchestrator가 재시작되면 그 이벤트의 마켓을
  CLOSE하지 못한다(마켓 상태 키의 TTL이 backstop). 위 single-instance 가정과 같은
  결의 한계로, 다중 인스턴스·재시작 복원력은 Phase 5 k8s 범위다.
- **MarketType 1종만 실제 생성.** mock은 `MATCH_RESULT_1X2`만 만든다.
  ADR-0013의 나머지 3종(TOTAL_OVER_UNDER, BTTS, DOUBLE_CHANCE)은 publisher와
  orchestrator의 per-market 처리 패턴이 자리잡은 뒤 확장.
- **HTTP read p99 미측정.** k6 스크립트(`odds_read.js`)는 골격만 있고, 실행은
  서비스 인스턴스를 띄운 별도 세션 + k6 바이너리가 필요해 미실행.
- **margin / overround 계산 미구현.** repo CLAUDE.md의 DO 목록에 "odds margin
  계산 및 검증(suspicious margin 경고)"이 있지만, mock의 base 확률 합이 1.0
  (margin 0)이라 V1 시뮬레이터엔 margin 개념이 없다. 실제 북메이커 margin
  주입은 mock 시나리오 확장 후보.

이 한계들은 전부 commit body 또는 클래스 Javadoc에 "왜 안 했는지"가 적혀 있어,
나중에 누가 봐도 TODO를 깜빡한 게 아니라 의도적 결정임이 드러나도록 했다.
