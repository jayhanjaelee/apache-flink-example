package com.example.flinkdemo;

import com.example.flinkdemo.aggregate.EventCountAmountAggregator;
import com.example.flinkdemo.aggregate.WindowResultFunction;
import com.example.flinkdemo.model.AggregatedResult;
import com.example.flinkdemo.model.UserEvent;
import com.example.flinkdemo.parser.UserEventParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;

import java.nio.charset.StandardCharsets;
import java.time.Duration;

public class FlinkKafkaDemoJob {

    private static final String BOOTSTRAP_SERVERS = "kafka-1:9092,kafka-2:9092";
    private static final String SOURCE_TOPIC = "demo-topic";
    private static final String SINK_TOPIC = "demo-topic-processed";
    private static final String CONSUMER_GROUP = "flink-demo-group"; // 기존 demo-group과 충돌 방지
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2); // taskmanager.numberOfTaskSlots: 2 와 일치
        env.enableCheckpointing(10_000); // KafkaSink AT_LEAST_ONCE 보장을 위해 필요

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(BOOTSTRAP_SERVERS)
                .setTopics(SOURCE_TOPIC)
                .setGroupId(CONSUMER_GROUP)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> rawStream = env.fromSource(
                source, WatermarkStrategy.noWatermarks(), "kafka-source");

        DataStream<UserEvent> events = rawStream
                .flatMap(new UserEventParser())
                .name("parse-json");

        DataStream<AggregatedResult> aggregated = events
                .keyBy(event -> event.userId)
                .window(TumblingProcessingTimeWindows.of(Duration.ofSeconds(15)))
                .aggregate(new EventCountAmountAggregator(), new WindowResultFunction())
                .name("windowed-aggregation");

        aggregated.print().name("print-sink");

        KafkaSink<AggregatedResult> sink = KafkaSink.<AggregatedResult>builder()
                .setBootstrapServers(BOOTSTRAP_SERVERS)
                .setRecordSerializer(KafkaRecordSerializationSchema.<AggregatedResult>builder()
                        .setTopic(SINK_TOPIC)
                        .setKeySerializationSchema(result -> result.userId.getBytes(StandardCharsets.UTF_8))
                        .setValueSerializationSchema(result -> {
                            try {
                                return OBJECT_MAPPER.writeValueAsBytes(result);
                            } catch (Exception e) {
                                throw new RuntimeException("Failed to serialize AggregatedResult", e);
                            }
                        })
                        .build())
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        aggregated.sinkTo(sink).name("kafka-sink");

        env.execute("Flink Kafka Demo - User Event Aggregation");
    }
}
