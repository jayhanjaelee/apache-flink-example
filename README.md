# apache-flink-example

[kafka-example](https://github.com/jayhanjaelee/kafka-example)에서 수집되는 데이터를 Apache Flink로 실시간 처리하는 데모 프로젝트입니다.

## 개요

kafka-example의 producer(`POST /send`)로 `demo-topic`에 넣은 사용자 이벤트(JSON)를 Flink Job이 구독해서, `userId` 기준 15초 tumbling window로 이벤트 수와 amount 합계를 집계합니다. 집계 결과는 stdout(print sink)과 새 Kafka 토픽 `demo-topic-processed`(KafkaSink)에 함께 출력됩니다. JSON 파싱에 실패하는 메시지(스키마가 다르거나 plain string인 경우)는 로그를 남기고 건너뜁니다.

```
kafka-example (producer) --> demo-topic --> Flink Job --> stdout
                                                       \-> demo-topic-processed (Kafka)
```

## 사전 요구사항

- Docker / Docker Compose
- [kafka-example](https://github.com/jayhanjaelee/kafka-example)이 로컬에 클론되어 있고 먼저 기동 가능한 상태
- 로컬에 Maven이 없어도 무방 (Docker 컨테이너로 빌드)

## 아키텍처

- **소스**: `KafkaSource`로 `demo-topic` 구독, consumer group은 `flink-demo-group`(kafka-example의 Node.js consumer가 쓰는 `demo-group`과 충돌하지 않도록 분리), `OffsetsInitializer.earliest()`로 시작
- **파싱**: `UserEventParser`(`FlatMapFunction`)가 Jackson으로 JSON을 `UserEvent{userId, action, amount}`로 역직렬화. 파싱 실패/필수 필드 누락 시 로그만 남기고 skip
- **집계**: `keyBy(userId)` → `TumblingProcessingTimeWindows(15s)` → `AggregateFunction`(증분 카운트/합계) + `ProcessWindowFunction`(윈도우 시작/끝 메타 부착) → `AggregatedResult{userId, windowStart, windowEnd, eventCount, totalAmount}`
- **싱크**: `print()`(stdout)과 `KafkaSink`(`demo-topic-processed`, key=userId, value=JSON, `DeliveryGuarantee.AT_LEAST_ONCE`) 병행

소스 코드 구조:
```
src/main/java/com/example/flinkdemo/
├── FlinkKafkaDemoJob.java              # main, 파이프라인 조립
├── model/
│   ├── UserEvent.java                  # 입력 POJO
│   └── AggregatedResult.java           # 출력 POJO
├── parser/
│   └── UserEventParser.java            # JSON 파싱 + skip
└── aggregate/
    ├── EventCountAmountAggregator.java # 증분 집계
    └── WindowResultFunction.java       # 윈도우 메타 부착
```

## 네트워크 연결 (kafka-example과의 관계)

kafka-example의 브로커(`kafka-1`, `kafka-2`)는 `KAFKA_ADVERTISED_LISTENERS`가 컨테이너 네트워크 호스트명(`kafka-1:9092`, `kafka-2:9092`)으로 고정되어 있어 호스트 포트 매핑(`localhost:9092`)으로는 접근할 수 없습니다. 그래서 이 프로젝트의 `docker-compose.yml`은 kafka-example 프로젝트 파일을 전혀 수정하지 않고, kafka-example이 만드는 기본 네트워크(`kafka-example_default`)를 `external` 네트워크로 참조해서 join합니다:

```yaml
networks:
  kafka-net:
    external: true
    name: ${KAFKA_NETWORK_NAME:-kafka-example_default}
```

디렉토리명/프로젝트명이 달라 네트워크 이름이 다르면 `KAFKA_NETWORK_NAME` 환경변수로 오버라이드할 수 있습니다.

**따라서 kafka-example 스택이 먼저 떠 있어야 합니다.**

## 실행 방법

```bash
# 1) kafka-example 먼저 기동 (네트워크가 먼저 존재해야 함)
cd /path/to/kafka-example
docker compose up -d

# 2) Job 빌드 (로컬에 mvn이 없다면 Docker로)
cd /path/to/apache-flink-example
docker run --rm -v "$PWD":/app -v flink-demo-m2:/root/.m2 -w /app maven:3.9-eclipse-temurin-11 mvn package
# 로컬에 mvn(Java 11+)이 있다면: mvn package

# 3) Flink 클러스터 기동
docker compose up -d
open http://localhost:8081   # Task Managers 탭에서 slot 2개 등록 확인

# 4) Job 제출
docker compose exec jobmanager flink run -d /opt/flink/usrlib/flink-kafka-demo.jar
docker compose exec jobmanager flink list

# 5) 데이터 투입 (kafka-example producer, 호스트 포트 9091)
curl -X POST localhost:9091/send -H "Content-Type: application/json" \
  -d '{"key":"userA","message":{"userId":"userA","action":"click","amount":10}}'

# 6) 15초 이상 대기 후 결과 확인
docker compose logs -f taskmanager | grep AggregatedResult

# 7) kafka-ui(http://localhost:9080)에서 demo-topic-processed 토픽 확인
```

## 정리

```bash
# 순서 중요: flink 먼저 down (반대로 하면 kafka-example이 네트워크 삭제 시도하다 "active endpoints" 에러 발생)
cd /path/to/apache-flink-example && docker compose down
cd /path/to/kafka-example && docker compose down
```

## Gotchas

- **Jackson 버전 충돌**: `flink-connector-base`가 전이적으로 더 낮은 `jackson-core` 버전을 끌어와 직접 선언한 `jackson-databind`와 버전이 어긋나면 `NoSuchMethodError: BufferRecycler.releaseToPool()`가 발생합니다. `pom.xml`에서 `jackson-core`를 `jackson-databind`와 같은 버전으로 명시적으로 고정해 해결했습니다. 혹시 모를 클래스로더 충돌(예: `flink-csv`/`flink-json`이 클러스터 lib에 번들한 unshaded Jackson)에도 안전하도록 `maven-shade-plugin`에서 `com.fasterxml.jackson` 패키지를 relocation 처리했습니다.
- **Docker 바인드 마운트가 비어 보이는 현상**: `mvn clean package`는 `target/` 디렉토리를 삭제 후 재생성합니다. 이미 그 디렉토리를 바인드 마운트한 컨테이너가 떠 있는 상태에서 이 작업을 하면, 컨테이너 쪽 마운트가 옛 inode를 붙잡고 있어 `/opt/flink/usrlib`가 빈 디렉토리로 보일 수 있습니다. 빌드 후에는 `docker compose up -d --force-recreate jobmanager`로 마운트를 다시 잡아주세요.
- `demo-topic`은 두 프로젝트가 함께 사용하므로, Flink Job의 consumer group(`flink-demo-group`)은 kafka-example의 Node.js consumer(`demo-group`)와 겹치지 않게 분리되어 있습니다.
